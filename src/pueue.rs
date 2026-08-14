use std::{
    collections::BTreeMap,
    ffi::OsString,
    fmt,
    io,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{
    environment::SanitizedEnvironment,
    execution_policy::ResolvedExecutionPolicy,
    pueue_process::{BoundedOutput, PueueProcessRunner},
    pueue_security::validate_group,
    AppError,
};

pub const PUEUE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Error)]
pub enum PueueError {
    #[error("failed to start Pueue `{operation}`")]
    Spawn {
        operation: &'static str,
        source_kind: io::ErrorKind,
    },

    #[error("Pueue `{operation}` timed out")]
    Timeout { operation: &'static str },

    #[error("Pueue `{operation}` exceeded the {stream} output limit")]
    OutputLimit {
        operation: &'static str,
        stream: &'static str,
    },

    #[error("Pueue `{operation}` cleanup failed during {stage}")]
    Cleanup {
        operation: &'static str,
        stage: &'static str,
    },

    #[error("Pueue `{operation}` failed with exit code {exit_code:?}; inspect captured output")]
    CommandFailed {
        operation: &'static str,
        exit_code: Option<i32>,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },

    #[error("Pueue status JSON is invalid: {source}")]
    InvalidStatusJson {
        #[source]
        source: serde_json::Error,
    },

    #[error("Pueue group JSON is invalid: {source}")]
    InvalidGroupJson {
        #[source]
        source: serde_json::Error,
    },

    #[error("Pueue status JSON has an invalid task shape: {reason}")]
    InvalidStatusTask { reason: &'static str },

    #[error("Pueue add returned an invalid task ID; inspect captured output")]
    InvalidTaskId { stdout: Vec<u8> },
}

impl fmt::Debug for PueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Spawn {
                operation,
                source_kind,
            } => formatter
                .debug_struct("Spawn")
                .field("operation", operation)
                .field("source_kind", source_kind)
                .finish(),
            Self::Timeout { operation } => formatter
                .debug_struct("Timeout")
                .field("operation", operation)
                .finish(),
            Self::OutputLimit { operation, stream } => formatter
                .debug_struct("OutputLimit")
                .field("operation", operation)
                .field("stream", stream)
                .finish(),
            Self::Cleanup { operation, stage } => formatter
                .debug_struct("Cleanup")
                .field("operation", operation)
                .field("stage", stage)
                .finish(),
            Self::CommandFailed {
                operation,
                exit_code,
                stdout,
                stderr,
            } => formatter
                .debug_struct("CommandFailed")
                .field("operation", operation)
                .field("exit_code", exit_code)
                .field("stdout_len", &stdout.len())
                .field("stderr_len", &stderr.len())
                .finish(),
            Self::InvalidStatusJson { .. } => {
                formatter.write_str("InvalidStatusJson { source: <redacted> }")
            }
            Self::InvalidGroupJson { .. } => {
                formatter.write_str("InvalidGroupJson { source: <redacted> }")
            }
            Self::InvalidStatusTask { reason } => formatter
                .debug_struct("InvalidStatusTask")
                .field("reason", reason)
                .finish(),
            Self::InvalidTaskId { stdout } => formatter
                .debug_struct("InvalidTaskId")
                .field("stdout_len", &stdout.len())
                .finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PueueTask {
    pub id: i64,
    pub group: String,
    pub command: String,
    pub state: String,
    pub enqueued_at: Option<String>,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    pub result: Option<Value>,
}

impl PueueTask {
    pub fn is_running(&self) -> bool {
        self.state.eq_ignore_ascii_case("running")
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state.to_ascii_lowercase().as_str(),
            "done" | "failed" | "killed" | "finished" | "success"
        )
    }
}

#[async_trait]
pub trait PueueApi: Send + Sync {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError>;

    async fn add(&self, args: &[OsString]) -> Result<i64, AppError>;

    async fn kill(&self, task_id: i64) -> Result<(), AppError>;

    async fn remove(&self, task_id: i64) -> Result<(), AppError>;

    async fn ensure_group(&self, group: &str) -> Result<(), AppError>;
}

#[derive(Debug, Clone)]
pub struct CommandPueue {
    backend: CommandPueueBackend,
}

#[derive(Debug, Clone)]
enum CommandPueueBackend {
    Verified {
        policy: Arc<ResolvedExecutionPolicy>,
        environment: SanitizedEnvironment,
        runner: PueueProcessRunner,
    },
    #[cfg(test)]
    Unconfigured,
}

pub fn configured_pueue(
    policy: Arc<ResolvedExecutionPolicy>,
) -> Result<CommandPueue, AppError> {
    let environment = SanitizedEnvironment::for_pueue(&policy)?;
    let _ = policy.pueue_anchor.verify_identity()?;
    let _ = policy.launcher_anchor.verify_identity()?;
    let _ = policy
        .pueue_config_anchor
        .verify_identity(&policy.project_roots)?;
    Ok(CommandPueue {
        backend: CommandPueueBackend::Verified {
            policy,
            environment,
            runner: PueueProcessRunner::new(),
        },
    })
}

impl CommandPueue {
    async fn execute(
        &self,
        operation: &'static str,
        operation_args: &[OsString],
    ) -> Result<BoundedOutput, AppError> {
        match &self.backend {
            CommandPueueBackend::Verified {
                policy,
                environment,
                runner,
            } => {
                let mut argv = Vec::with_capacity(operation_args.len() + 1);
                argv.push(OsString::from(operation));
                argv.extend_from_slice(operation_args);
                runner
                    .run_with_environment(policy, environment, &argv)
                    .await
            }
            #[cfg(test)]
            CommandPueueBackend::Unconfigured => Err(AppError::Configuration {
                field: "pueue_test_policy",
            }),
        }
    }
}

#[cfg(test)]
impl CommandPueue {
    pub fn new(policy: Arc<ResolvedExecutionPolicy>) -> Result<Self, AppError> {
        configured_pueue(policy)
    }

    #[cfg(debug_assertions)]
    pub fn with_test_limits(
        policy: Arc<ResolvedExecutionPolicy>,
        timeout: Duration,
        output_limit: usize,
    ) -> Result<Self, AppError> {
        let environment = SanitizedEnvironment::for_pueue(&policy)?;
        Ok(Self {
            backend: CommandPueueBackend::Verified {
                policy,
                environment,
                runner: PueueProcessRunner::with_limits(timeout, output_limit),
            },
        })
    }
}

#[cfg(test)]
impl Default for CommandPueue {
    fn default() -> Self {
        Self {
            backend: CommandPueueBackend::Unconfigured,
        }
    }
}

#[async_trait]
impl PueueApi for CommandPueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        let output = self.execute("status", &[OsString::from("--json")]).await?;
        let status: RawStatus = serde_json::from_slice(&output.stdout)
            .map_err(|source| PueueError::InvalidStatusJson { source })?;
        let mut tasks = status
            .tasks
            .into_values()
            .map(PueueTask::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        tasks.sort_unstable_by_key(|task| task.id);
        Ok(tasks)
    }

    async fn add(&self, args: &[OsString]) -> Result<i64, AppError> {
        let mut operation_args = Vec::with_capacity(args.len() + 2);
        operation_args.push(OsString::from("--print-task-id"));
        if let Some(separator_index) = args.iter().position(|argument| argument == "--") {
            operation_args.extend_from_slice(&args[..separator_index]);
            operation_args.push(OsString::from("--escape"));
            operation_args.extend_from_slice(&args[separator_index..]);
        } else {
            operation_args.push(OsString::from("--escape"));
            operation_args.extend_from_slice(args);
        }
        let output = self.execute("add", &operation_args).await?;
        let task_id = std::str::from_utf8(&output.stdout)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .filter(|task_id| *task_id >= 0)
            .ok_or(PueueError::InvalidTaskId {
                stdout: output.stdout,
            })?;
        Ok(task_id)
    }

    async fn kill(&self, task_id: i64) -> Result<(), AppError> {
        self.execute("kill", &[OsString::from(task_id.to_string())])
            .await?;
        Ok(())
    }

    async fn remove(&self, task_id: i64) -> Result<(), AppError> {
        self.execute("remove", &[OsString::from(task_id.to_string())])
            .await?;
        Ok(())
    }

    async fn ensure_group(&self, group: &str) -> Result<(), AppError> {
        validate_group(group)?;
        if self.group_exists(group).await? {
            return Ok(());
        }

        let add_result = self
            .execute("group", &[OsString::from("add"), OsString::from(group)])
            .await;
        if let Err(error) = add_result {
            if matches!(
                &error,
                AppError::Pueue(PueueError::CommandFailed {
                    operation: "group",
                    ..
                })
            ) && matches!(self.group_exists(group).await, Ok(true))
            {
                return Ok(());
            }

            return Err(error);
        }

        Ok(())
    }
}

impl CommandPueue {
    async fn group_exists(&self, group: &str) -> Result<bool, AppError> {
        let output = self.execute("group", &[OsString::from("-j")]).await?;
        let groups: Value = serde_json::from_slice(&output.stdout)
            .map_err(|source| PueueError::InvalidGroupJson { source })?;
        Ok(groups
            .as_object()
            .is_some_and(|groups| groups.contains_key(group)))
    }
}

#[derive(Debug, Deserialize)]
struct RawStatus {
    tasks: BTreeMap<String, RawTask>,
}

#[derive(Debug, Deserialize)]
struct RawTask {
    id: RawTaskId,
    group: String,
    command: String,
    status: Value,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawTaskId {
    Integer(i64),
    String(String),
}

impl RawTaskId {
    fn into_i64(self) -> Result<i64, PueueError> {
        let task_id = match self {
            Self::Integer(task_id) => Some(task_id),
            Self::String(task_id) => task_id.parse().ok(),
        };
        task_id
            .filter(|task_id| *task_id >= 0)
            .ok_or(PueueError::InvalidStatusTask {
                reason: "task ID must be a non-negative integer",
            })
    }
}

impl TryFrom<RawTask> for PueueTask {
    type Error = PueueError;

    fn try_from(raw: RawTask) -> Result<Self, Self::Error> {
        let statuses = raw
            .status
            .as_object()
            .ok_or(PueueError::InvalidStatusTask {
                reason: "status must be an object",
            })?;
        if statuses.len() != 1 {
            return Err(PueueError::InvalidStatusTask {
                reason: "status must contain exactly one state",
            });
        }
        let (state, details) = statuses
            .iter()
            .next()
            .ok_or(PueueError::InvalidStatusTask {
                reason: "status must contain a state",
            })?;
        let details = details.as_object().ok_or(PueueError::InvalidStatusTask {
            reason: "state details must be an object",
        })?;

        Ok(Self {
            id: raw.id.into_i64()?,
            group: raw.group,
            command: raw.command,
            state: state.clone(),
            enqueued_at: timestamp_field(
                details,
                "enqueued_at",
                "enqueued_at timestamp must be a string when present",
            )?,
            started_at: timestamp_field(
                details,
                "start",
                "start timestamp must be a string when present",
            )?,
            ended_at: timestamp_field(
                details,
                "end",
                "end timestamp must be a string when present",
            )?,
            result: details
                .get("result")
                .filter(|result| !result.is_null())
                .cloned(),
        })
    }
}

fn timestamp_field(
    object: &serde_json::Map<String, Value>,
    field: &str,
    invalid_reason: &'static str,
) -> Result<Option<String>, PueueError> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(PueueError::InvalidStatusTask {
            reason: invalid_reason,
        }),
    }
}
