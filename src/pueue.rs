use std::{
    collections::BTreeMap,
    ffi::OsString,
    fmt,
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    environment::SanitizedEnvironment,
    execution_policy::ResolvedExecutionPolicy,
    pueue_process::{
        validate_native_pueue_argv, validate_pueue_execution_contract, BoundedOutput,
        PueueProcessRunner,
    },
    pueue_security::validate_group,
    AppError,
};

pub const PUEUE_TIMEOUT: Duration = Duration::from_secs(30);

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PueueControlBoundary {
    TargetNotReleased,
    TargetReleasedClientQuiescent,
    CleanupUncertain,
}

#[doc(hidden)]
#[derive(Debug)]
pub struct ControlledPueueFailure {
    error: AppError,
    boundary: PueueControlBoundary,
}

impl ControlledPueueFailure {
    pub fn target_not_released(error: AppError) -> Self {
        Self {
            error,
            boundary: PueueControlBoundary::TargetNotReleased,
        }
    }

    pub fn target_released_client_quiescent(error: AppError) -> Self {
        Self {
            error,
            boundary: PueueControlBoundary::TargetReleasedClientQuiescent,
        }
    }

    pub fn cleanup_uncertain(error: AppError) -> Self {
        Self {
            error,
            boundary: PueueControlBoundary::CleanupUncertain,
        }
    }

    pub fn boundary(&self) -> PueueControlBoundary {
        self.boundary
    }

    pub fn into_error(self) -> AppError {
        self.error
    }
}

#[doc(hidden)]
pub type ControlledPueueResult<T> = Result<T, ControlledPueueFailure>;

fn controlled_operation_unsupported() -> ControlledPueueFailure {
    ControlledPueueFailure::target_not_released(AppError::Configuration {
        field: "pueue_controlled_operation",
    })
}

fn group_already_exists() -> AppError {
    AppError::Validation {
        field: "pueue_group",
        message: "already exists and is not owned by this operation",
    }
}

fn group_missing_after_creation() -> AppError {
    AppError::Validation {
        field: "pueue_group",
        message: "was absent after creation",
    }
}

/// Validate the complete native Pueue argv that `CommandPueue::add` will
/// eventually launch. This keeps durable submission intent from referring to
/// a request the native control protocol cannot carry.
pub fn validate_add_argv(args: &[OsString]) -> Result<(), AppError> {
    let mut argv = Vec::with_capacity(args.len() + 9);
    argv.extend([
        OsString::from("pueue"),
        OsString::from("--config"),
        OsString::from("/dev/fd/9"),
        OsString::from("add"),
    ]);
    argv.extend(add_operation_args(args));
    validate_native_pueue_argv(&argv)
}

fn add_operation_args(args: &[OsString]) -> Vec<OsString> {
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
    operation_args
}

#[derive(Error)]
pub enum PueueError {
    #[error("failed to start Pueue `{operation}`")]
    Spawn {
        operation: &'static str,
        source_kind: io::ErrorKind,
    },

    #[error("Pueue {operation} timed out")]
    Timeout { operation: &'static str },

    #[error("Pueue {operation} output limit exceeded for {stream}")]
    OutputLimit {
        operation: &'static str,
        stream: &'static str,
    },

    #[error("Pueue `{operation}` cleanup failed during {stage}")]
    Cleanup {
        operation: &'static str,
        stage: &'static str,
    },

    #[error("Pueue {operation} failed with exit code {exit_code:?}")]
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

    #[error("Pueue add returned an invalid task ID")]
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

    async fn group_exists(&self, _group: &str) -> Result<bool, AppError> {
        Err(AppError::Configuration {
            field: "pueue_group_api",
        })
    }

    async fn create_group_exclusive(&self, _group: &str) -> Result<(), AppError> {
        Err(AppError::Configuration {
            field: "pueue_group_api",
        })
    }

    async fn remove_group(&self, _group: &str) -> Result<(), AppError> {
        Err(AppError::Configuration {
            field: "pueue_group_api",
        })
    }

    #[doc(hidden)]
    async fn status_json_controlled_before(
        &self,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> ControlledPueueResult<Vec<PueueTask>> {
        Err(controlled_operation_unsupported())
    }

    #[doc(hidden)]
    async fn add_controlled_before(
        &self,
        _args: &[OsString],
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> ControlledPueueResult<i64> {
        Err(controlled_operation_unsupported())
    }

    #[doc(hidden)]
    async fn kill_controlled_before(
        &self,
        _task_id: i64,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> ControlledPueueResult<()> {
        Err(controlled_operation_unsupported())
    }

    #[doc(hidden)]
    async fn remove_controlled_before(
        &self,
        _task_id: i64,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> ControlledPueueResult<()> {
        Err(controlled_operation_unsupported())
    }

    #[doc(hidden)]
    async fn group_exists_controlled_before(
        &self,
        _group: &str,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> ControlledPueueResult<bool> {
        Err(controlled_operation_unsupported())
    }

    #[doc(hidden)]
    async fn create_group_exclusive_controlled_before(
        &self,
        _group: &str,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> ControlledPueueResult<()> {
        Err(controlled_operation_unsupported())
    }

    #[doc(hidden)]
    async fn remove_group_controlled_before(
        &self,
        _group: &str,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> ControlledPueueResult<()> {
        Err(controlled_operation_unsupported())
    }
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
    validate_pueue_execution_contract(&policy, &environment)?;
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

    async fn execute_controlled_before(
        &self,
        operation: &'static str,
        operation_args: &[OsString],
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> ControlledPueueResult<BoundedOutput> {
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
                    .run_with_environment_controlled_before(
                        policy,
                        environment,
                        &argv,
                        deadline,
                        cancellation,
                    )
                    .await
            }
            #[cfg(test)]
            CommandPueueBackend::Unconfigured => Err(ControlledPueueFailure::target_not_released(
                AppError::Configuration {
                    field: "pueue_test_policy",
                },
            )),
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
        validate_add_argv(args)?;
        let operation_args = add_operation_args(args);
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
        if self.group_exists_legacy(group).await? {
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
            ) && matches!(self.group_exists_legacy(group).await, Ok(true))
            {
                return Ok(());
            }

            return Err(error);
        }

        Ok(())
    }

    async fn group_exists(&self, group: &str) -> Result<bool, AppError> {
        validate_group(group)?;
        self.group_exists_legacy(group).await
    }

    async fn create_group_exclusive(&self, group: &str) -> Result<(), AppError> {
        validate_group(group)?;
        if self.group_exists_legacy(group).await? {
            return Err(group_already_exists());
        }
        self.execute("group", &[OsString::from("add"), OsString::from(group)])
            .await?;
        if self.group_exists_legacy(group).await? {
            Ok(())
        } else {
            Err(group_missing_after_creation())
        }
    }

    async fn remove_group(&self, group: &str) -> Result<(), AppError> {
        validate_group(group)?;
        self.execute("group", &[OsString::from("remove"), OsString::from(group)])
            .await?;
        Ok(())
    }

    async fn status_json_controlled_before(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> ControlledPueueResult<Vec<PueueTask>> {
        let output = self
            .execute_controlled_before(
                "status",
                &[OsString::from("--json")],
                deadline,
                cancellation,
            )
            .await?;
        let status: RawStatus = serde_json::from_slice(&output.stdout).map_err(|source| {
            ControlledPueueFailure::target_released_client_quiescent(
                PueueError::InvalidStatusJson { source }.into(),
            )
        })?;
        let mut tasks = status
            .tasks
            .into_values()
            .map(PueueTask::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                ControlledPueueFailure::target_released_client_quiescent(error.into())
            })?;
        tasks.sort_unstable_by_key(|task| task.id);
        Ok(tasks)
    }

    async fn add_controlled_before(
        &self,
        args: &[OsString],
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> ControlledPueueResult<i64> {
        validate_add_argv(args).map_err(ControlledPueueFailure::target_not_released)?;
        let operation_args = add_operation_args(args);
        let output = self
            .execute_controlled_before("add", &operation_args, deadline, cancellation)
            .await?;
        std::str::from_utf8(&output.stdout)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .filter(|task_id| *task_id >= 0)
            .ok_or_else(|| {
                ControlledPueueFailure::target_released_client_quiescent(
                    PueueError::InvalidTaskId {
                        stdout: output.stdout,
                    }
                    .into(),
                )
            })
    }

    async fn kill_controlled_before(
        &self,
        task_id: i64,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> ControlledPueueResult<()> {
        self.execute_controlled_before(
            "kill",
            &[OsString::from(task_id.to_string())],
            deadline,
            cancellation,
        )
        .await?;
        Ok(())
    }

    async fn remove_controlled_before(
        &self,
        task_id: i64,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> ControlledPueueResult<()> {
        self.execute_controlled_before(
            "remove",
            &[OsString::from(task_id.to_string())],
            deadline,
            cancellation,
        )
        .await?;
        Ok(())
    }

    async fn group_exists_controlled_before(
        &self,
        group: &str,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> ControlledPueueResult<bool> {
        validate_group(group).map_err(ControlledPueueFailure::target_not_released)?;
        let output = self
            .execute_controlled_before("group", &[OsString::from("-j")], deadline, cancellation)
            .await?;
        let groups: BTreeMap<String, Value> =
            serde_json::from_slice(&output.stdout).map_err(|source| {
                ControlledPueueFailure::target_released_client_quiescent(
                    PueueError::InvalidGroupJson { source }.into(),
                )
            })?;
        Ok(groups.contains_key(group))
    }

    async fn create_group_exclusive_controlled_before(
        &self,
        group: &str,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> ControlledPueueResult<()> {
        validate_group(group).map_err(ControlledPueueFailure::target_not_released)?;
        if self
            .group_exists_controlled_before(group, deadline, cancellation.clone())
            .await?
        {
            return Err(ControlledPueueFailure::target_released_client_quiescent(
                group_already_exists(),
            ));
        }

        self.execute_controlled_before(
            "group",
            &[OsString::from("add"), OsString::from(group)],
            deadline,
            cancellation.clone(),
        )
        .await?;

        let exists = self
            .group_exists_controlled_before(group, deadline, cancellation)
            .await
            .map_err(|failure| {
                let boundary = failure.boundary();
                let error = failure.into_error();
                if boundary == PueueControlBoundary::CleanupUncertain {
                    ControlledPueueFailure::cleanup_uncertain(error)
                } else {
                    ControlledPueueFailure::target_released_client_quiescent(error)
                }
            })?;
        if exists {
            Ok(())
        } else {
            Err(ControlledPueueFailure::target_released_client_quiescent(
                group_missing_after_creation(),
            ))
        }
    }

    async fn remove_group_controlled_before(
        &self,
        group: &str,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> ControlledPueueResult<()> {
        validate_group(group).map_err(ControlledPueueFailure::target_not_released)?;
        self.execute_controlled_before(
            "group",
            &[OsString::from("remove"), OsString::from(group)],
            deadline,
            cancellation,
        )
        .await?;
        Ok(())
    }
}

impl CommandPueue {
    async fn group_exists_legacy(&self, group: &str) -> Result<bool, AppError> {
        let output = self.execute("group", &[OsString::from("-j")]).await?;
        let groups: BTreeMap<String, Value> = serde_json::from_slice(&output.stdout)
            .map_err(|source| PueueError::InvalidGroupJson { source })?;
        Ok(groups.contains_key(group))
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
