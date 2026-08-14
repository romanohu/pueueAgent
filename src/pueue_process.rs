//! Verified, bounded execution for one Pueue operation.
//!
//! The native launch protocol and process-group ownership live in `process`.
//! This adapter only assembles the Pueue argv, consumes the core policy
//! anchors, bounds the two output streams, and turns every abnormal exit into
//! a bounded control error.

use std::{
    ffi::OsString,
    fmt,
    io,
    process::ExitStatus,
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncReadExt},
    task::JoinHandle,
};

use crate::{
    environment::SanitizedEnvironment,
    execution_policy::ResolvedExecutionPolicy,
    pueue::{PueueError, PUEUE_TIMEOUT},
    AppError,
};

pub use crate::pueue_security::MAX_PUEUE_OUTPUT_BYTES;

/// The result of a successful, bounded Pueue process collection.
pub struct BoundedOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl fmt::Debug for BoundedOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedOutput")
            .field("status", &self.status)
            .field("stdout_len", &self.stdout.len())
            .field("stderr_len", &self.stderr.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PueueProcessRunner {
    timeout: Duration,
    output_limit: usize,
}

impl Default for PueueProcessRunner {
    fn default() -> Self {
        Self {
            timeout: PUEUE_TIMEOUT,
            output_limit: MAX_PUEUE_OUTPUT_BYTES,
        }
    }
}

impl PueueProcessRunner {
    pub const fn new() -> Self {
        Self {
            timeout: PUEUE_TIMEOUT,
            output_limit: MAX_PUEUE_OUTPUT_BYTES,
        }
    }

    /// Debug-only limit seam used by native integration fixtures. Optimized
    /// builds do not expose a configurable lifetime or output cap.
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub const fn with_limits(timeout: Duration, output_limit: usize) -> Self {
        Self {
            timeout,
            output_limit,
        }
    }

    /// Run exactly one operation using the core's pinned executable, launcher,
    /// config descriptor, environment, and process-group lifecycle.
    ///
    /// `operation_argv` begins with the operation name (`status`, `add`,
    /// `kill`, `remove`, or `group`) and then contains its direct argv.
    #[cfg(unix)]
    pub async fn run(
        &self,
        policy: &ResolvedExecutionPolicy,
        operation_argv: &[OsString],
    ) -> Result<BoundedOutput, AppError> {
        let operation_arg = operation_argv.first().ok_or(AppError::Validation {
            field: "pueue_operation",
            message: "must not be empty",
        })?;
        let operation = operation_name(operation_arg).ok_or(AppError::Validation {
            field: "pueue_operation",
            message: "is not supported",
        })?;
        let runner = *self;
        let policy = policy.clone();
        let operation_argv = operation_argv.to_vec();
        // Tokio retains ownership of a spawned task when its JoinHandle is
        // dropped. Keep every destructive process resource in this task so
        // cancelling the caller cannot bypass the bounded cleanup path.
        tokio::spawn(async move { runner.run_owned(&policy, &operation_argv).await })
            .await
            .map_err(|_| {
                AppError::Pueue(PueueError::Cleanup {
                    operation,
                    stage: "supervisor",
                })
            })?
    }

    #[cfg(unix)]
    async fn run_owned(
        &self,
        policy: &ResolvedExecutionPolicy,
        operation_argv: &[OsString],
    ) -> Result<BoundedOutput, AppError> {
        use crate::process::{
            spawn_verified_command, terminate_process_group, ProcessGroupRequirement,
            VerifiedChildIo, VerifiedCommandSpec,
        };

        let (operation_arg, operation_args) = operation_argv
            .split_first()
            .ok_or(AppError::Validation {
                field: "pueue_operation",
                message: "must not be empty",
            })?;
        let operation = operation_name(operation_arg).ok_or(AppError::Validation {
            field: "pueue_operation",
            message: "is not supported",
        })?;

        // This is deliberately the final path/config check before the core
        // launcher call.  Do not replace it with a path-based argument.
        let verified_config = policy
            .pueue_config_anchor
            .verify_identity(&policy.project_roots)?;
        let environment = SanitizedEnvironment::for_pueue(policy)?;

        let mut argv = Vec::with_capacity(operation_args.len() + 3);
        argv.push(OsString::from("--config"));
        argv.push(OsString::from("/dev/fd/9"));
        argv.push(OsString::from(operation));
        argv.extend_from_slice(operation_args);

        let mut verified = match spawn_verified_command(VerifiedCommandSpec {
            launcher: policy.launcher_anchor.clone(),
            executable: policy.pueue_anchor.clone(),
            argv,
            environment,
            cwd: None,
            process_group: ProcessGroupRequirement::Required,
            start_suspended: true,
            project_root: None,
            pueue_config: Some(verified_config),
            child_io: VerifiedChildIo::Capture,
        }) {
            Ok(child) => child,
            Err(error) => return Err(map_spawn_error(operation, error)),
        };

        if let Err(error) = verified.release() {
            return Err(cleanup_after_failure(&mut verified, operation, error).await);
        }
        if let Err(error) = verified.confirm_exec().await {
            return Err(cleanup_after_failure(&mut verified, operation, error).await);
        }
        if let Err(error) = verified.wait_for_release_ack().await {
            return Err(cleanup_after_failure(&mut verified, operation, error).await);
        }

        let stdout = match verified.take_stdout() {
            Ok(stream) => stream,
            Err(error) => {
                return Err(cleanup_after_failure(&mut verified, operation, error).await)
            }
        };
        let stderr = match verified.take_stderr() {
            Ok(stream) => stream,
            Err(error) => {
                return Err(cleanup_after_failure(&mut verified, operation, error).await)
            }
        };

        let stdout_task = tokio::spawn(read_bounded(stdout, self.output_limit));
        let stderr_task = tokio::spawn(read_bounded(stderr, self.output_limit));
        let outcome = collect_until_terminal(
            &mut verified,
            stdout_task,
            stderr_task,
            self.timeout,
        )
        .await;

        match outcome {
            Ok(output) if output.status.success() => Ok(output),
            Ok(output) => Err(AppError::Pueue(PueueError::CommandFailed {
                operation,
                exit_code: output.status.code(),
                stdout: output.stdout,
                stderr: output.stderr,
            })),
            Err(mut pending) => {
                // `collect_until_terminal` drops its wait future before
                // returning the pending readers.  Keep those readers owned
                // while core terminates and reaps the group, then collect or
                // abort every reader handle.
                let cleanup = terminate_process_group(&mut verified).await;
                let readers = pending.finish_readers().await;
                if let Err(error) = cleanup {
                    return Err(AppError::Pueue(PueueError::Cleanup {
                        operation,
                        stage: cleanup_stage(&error),
                    }));
                }
                if let Err(stage) = readers {
                    return Err(AppError::Pueue(PueueError::Cleanup { operation, stage }));
                }
                Err(pending.failure.into_app_error(operation))
            }
        }
    }

    #[cfg(not(unix))]
    pub async fn run(
        &self,
        _policy: &ResolvedExecutionPolicy,
        operation_argv: &[OsString],
    ) -> Result<BoundedOutput, AppError> {
        let operation_arg = operation_argv.first().ok_or(AppError::Validation {
            field: "pueue_operation",
            message: "must not be empty",
        })?;
        let operation = operation_name(operation_arg).ok_or(AppError::Validation {
            field: "pueue_operation",
            message: "is not supported",
        })?;
        Err(AppError::Pueue(PueueError::Spawn {
            operation,
            source_kind: io::ErrorKind::Unsupported,
        }))
    }
}

fn operation_name(value: &OsString) -> Option<&'static str> {
    match value.to_str()? {
        "status" => Some("status"),
        "add" => Some("add"),
        "kill" => Some("kill"),
        "remove" => Some("remove"),
        "group" => Some("group"),
        _ => None,
    }
}

#[cfg(unix)]
#[derive(Debug)]
enum CollectionFailure {
    Timeout,
    OutputLimit(&'static str),
    Reader(&'static str),
    Wait,
}

#[cfg(unix)]
struct PendingCollection {
    failure: CollectionFailure,
    stdout_task: Option<JoinHandle<Result<Vec<u8>, ReadFailure>>>,
    stderr_task: Option<JoinHandle<Result<Vec<u8>, ReadFailure>>>,
}

#[cfg(unix)]
impl PendingCollection {
    async fn finish_readers(&mut self) -> Result<(), &'static str> {
        let mut failure = None;
        for (stream, task) in [
            ("stdout", self.stdout_task.take()),
            ("stderr", self.stderr_task.take()),
        ] {
            let Some(mut task) = task else {
                continue;
            };
            let reader_failed =
                match tokio::time::timeout(Duration::from_secs(1), &mut task).await {
                    Ok(Ok(_)) => false,
                    Ok(Err(_)) => true,
                    Err(_) => {
                        task.abort();
                        let _ = task.await;
                        true
                    }
                };
            if reader_failed {
                failure.get_or_insert(if stream == "stdout" {
                    "read stdout"
                } else {
                    "read stderr"
                });
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

#[cfg(unix)]
impl CollectionFailure {
    fn into_app_error(self, operation: &'static str) -> AppError {
        let error = match self {
            Self::Timeout => PueueError::Timeout { operation },
            Self::OutputLimit(stream) => PueueError::OutputLimit { operation, stream },
            Self::Reader(stream) => PueueError::Cleanup {
                operation,
                stage: if stream == "stdout" {
                    "read stdout"
                } else {
                    "read stderr"
                },
            },
            Self::Wait => PueueError::Cleanup {
                operation,
                stage: "wait",
            },
        };
        AppError::Pueue(error)
    }
}

#[cfg(unix)]
async fn read_bounded<R>(mut reader: R, limit: usize) -> Result<Vec<u8>, ReadFailure>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::with_capacity(limit.min(8192));
    let mut buffer = [0u8; 8192];
    loop {
        let count = reader
            .read(&mut buffer)
            .await
            .map_err(|_| ReadFailure::Io)?;
        if count == 0 {
            return Ok(bytes);
        }
        if bytes.len().saturating_add(count) > limit {
            return Err(ReadFailure::Limit);
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
}

#[cfg(unix)]
#[derive(Debug)]
enum ReadFailure {
    Io,
    Limit,
}

#[cfg(unix)]
async fn collect_until_terminal(
    child: &mut crate::process::VerifiedChild,
    mut stdout_task: JoinHandle<Result<Vec<u8>, ReadFailure>>,
    mut stderr_task: JoinHandle<Result<Vec<u8>, ReadFailure>>,
    timeout: Duration,
) -> Result<BoundedOutput, PendingCollection> {
    let mut wait = Box::pin(child.wait());
    let mut deadline = Box::pin(tokio::time::sleep(timeout));
    let mut status = None;
    let mut stdout = None;
    let mut stderr = None;
    let mut stdout_joined = false;
    let mut stderr_joined = false;

    let result = loop {
        tokio::select! {
            result = &mut wait, if status.is_none() => {
                match result {
                    Ok(value) => status = Some(value),
                    Err(_) => break Err(CollectionFailure::Wait),
                }
            }
            result = &mut stdout_task, if stdout.is_none() => {
                stdout_joined = true;
                match result {
                    Ok(Ok(value)) => stdout = Some(value),
                    Ok(Err(ReadFailure::Limit)) => break Err(CollectionFailure::OutputLimit("stdout")),
                    Ok(Err(ReadFailure::Io)) | Err(_) => break Err(CollectionFailure::Reader("stdout")),
                }
            }
            result = &mut stderr_task, if stderr.is_none() => {
                stderr_joined = true;
                match result {
                    Ok(Ok(value)) => stderr = Some(value),
                    Ok(Err(ReadFailure::Limit)) => break Err(CollectionFailure::OutputLimit("stderr")),
                    Ok(Err(ReadFailure::Io)) | Err(_) => break Err(CollectionFailure::Reader("stderr")),
                }
            }
            _ = &mut deadline => break Err(CollectionFailure::Timeout),
        }

        if status.is_some() && stdout.is_some() && stderr.is_some() {
            break Ok(BoundedOutput {
                status: status.take().expect("status was checked"),
                stdout: stdout.take().expect("stdout was checked"),
                stderr: stderr.take().expect("stderr was checked"),
            });
        }
    };

    match result {
        Ok(output) => Ok(output),
        Err(failure) => Err(PendingCollection {
            failure,
            stdout_task: (!stdout_joined).then_some(stdout_task),
            stderr_task: (!stderr_joined).then_some(stderr_task),
        }),
    }
}

#[cfg(unix)]
async fn cleanup_after_failure(
    child: &mut crate::process::VerifiedChild,
    operation: &'static str,
    original: AppError,
) -> AppError {
    match crate::process::terminate_process_group(child).await {
        Ok(()) => original,
        Err(error) => AppError::Pueue(PueueError::Cleanup {
            operation,
            stage: cleanup_stage(&error),
        }),
    }
}

#[cfg(unix)]
fn cleanup_stage(error: &AppError) -> &'static str {
    match error {
        AppError::Io {
            operation: "reap terminated verified child",
            ..
        }
        | AppError::Runtime {
            operation: "reap terminated verified child before timeout",
        } => "reap",
        _ => "terminate",
    }
}

#[cfg(unix)]
fn map_spawn_error(operation: &'static str, error: AppError) -> AppError {
    if matches!(error, AppError::PolicyViolation { .. }) {
        return error;
    }
    let source_kind = match &error {
        AppError::Io { source, .. } => source.kind(),
        _ => io::ErrorKind::Other,
    };
    AppError::Pueue(PueueError::Spawn {
        operation,
        source_kind,
    })
}

#[cfg(all(test, unix))]
mod tests {
    use super::{cleanup_stage, read_bounded, BoundedOutput, ReadFailure};
    use crate::AppError;
    use std::os::unix::process::ExitStatusExt;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn bounded_reader_caps_stdout_and_stderr_independently() {
        let (mut stdout_writer, stdout_reader) = tokio::io::duplex(128);
        let (mut stderr_writer, stderr_reader) = tokio::io::duplex(128);
        let stdout_task = tokio::spawn(async move {
            stdout_writer.write_all(&[0u8; 65]).await.unwrap();
        });
        let stderr_task = tokio::spawn(async move {
            stderr_writer.write_all(&[0u8; 64]).await.unwrap();
        });

        assert!(matches!(read_bounded(stdout_reader, 64).await, Err(ReadFailure::Limit)));
        assert_eq!(read_bounded(stderr_reader, 64).await.unwrap().len(), 64);
        stdout_task.await.unwrap();
        stderr_task.await.unwrap();
    }

    #[test]
    fn bounded_output_debug_reports_lengths_without_captured_bytes() {
        const SENTINEL: &[u8] = b"captured-output-SENTINEL";
        let output = BoundedOutput {
            status: std::process::ExitStatus::from_raw(0),
            stdout: SENTINEL.to_vec(),
            stderr: SENTINEL.to_vec(),
        };

        let debug = format!("{output:?}");
        assert!(!debug.contains("captured-output-SENTINEL"));
        assert!(debug.contains(&format!("stdout_len: {}", SENTINEL.len())));
        assert!(debug.contains(&format!("stderr_len: {}", SENTINEL.len())));
    }

    #[test]
    fn cleanup_stage_only_classifies_core_reap_operations_as_reap() {
        let reap_io = AppError::Io {
            operation: "reap terminated verified child",
            source: std::io::Error::other("fixture"),
        };
        let reap_timeout = AppError::Runtime {
            operation: "reap terminated verified child before timeout",
        };
        let terminate = AppError::Io {
            operation: "signal verified process group",
            source: std::io::Error::other("fixture"),
        };
        let unrelated_runtime = AppError::Runtime {
            operation: "unrelated runtime failure",
        };

        assert_eq!(cleanup_stage(&reap_io), "reap");
        assert_eq!(cleanup_stage(&reap_timeout), "reap");
        assert_eq!(cleanup_stage(&terminate), "terminate");
        assert_eq!(cleanup_stage(&unrelated_runtime), "terminate");
    }
}
