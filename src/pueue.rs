use std::{collections::BTreeMap, ffi::OsString, io, path::PathBuf, process::Output};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::process::Command;

use crate::AppError;

#[derive(Debug, Error)]
pub enum PueueError {
    #[error("failed to start Pueue `{operation}`: {source}")]
    Spawn {
        operation: &'static str,
        #[source]
        source: io::Error,
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

    #[error("Pueue status JSON has an invalid task shape: {reason}")]
    InvalidStatusTask { reason: &'static str },

    #[error("Pueue add returned an invalid task ID; inspect captured output")]
    InvalidTaskId { stdout: Vec<u8> },
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
}

#[derive(Debug, Clone)]
pub struct CommandPueue {
    executable: PathBuf,
    fixed_args: Vec<OsString>,
}

impl CommandPueue {
    pub fn new<I, S>(executable: impl Into<PathBuf>, fixed_args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        Self {
            executable: executable.into(),
            fixed_args: fixed_args.into_iter().map(Into::into).collect(),
        }
    }

    pub fn executable(&self) -> &std::path::Path {
        &self.executable
    }

    pub fn fixed_args(&self) -> &[OsString] {
        &self.fixed_args
    }

    async fn execute(
        &self,
        operation: &'static str,
        operation_args: &[OsString],
    ) -> Result<Output, AppError> {
        let output = Command::new(&self.executable)
            .args(&self.fixed_args)
            .arg(operation)
            .args(operation_args)
            .output()
            .await
            .map_err(|source| PueueError::Spawn { operation, source })?;

        if !output.status.success() {
            return Err(PueueError::CommandFailed {
                operation,
                exit_code: output.status.code(),
                stdout: output.stdout,
                stderr: output.stderr,
            }
            .into());
        }

        Ok(output)
    }
}

impl Default for CommandPueue {
    fn default() -> Self {
        Self::new("pueue", Vec::<OsString>::new())
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
