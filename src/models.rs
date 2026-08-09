use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    path::{Path, PathBuf},
    str::FromStr,
};

use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug)]
pub struct ModelEnumParseError {
    enum_name: &'static str,
    value: String,
}

impl Display for ModelEnumParseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unknown {} database value {:?}",
            self.enum_name, self.value
        )
    }
}

impl Error for ModelEnumParseError {}

macro_rules! database_enum {
    ($name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = ModelEnumParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(ModelEnumParseError {
                        enum_name: stringify!($name),
                        value: value.to_owned(),
                    }),
                }
            }
        }

        impl ToSql for $name {
            fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
                Ok(ToSqlOutput::Borrowed(ValueRef::Text(
                    self.as_str().as_bytes(),
                )))
            }
        }

        impl FromSql for $name {
            fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                value
                    .as_str()?
                    .parse()
                    .map_err(|error| FromSqlError::Other(Box::new(error)))
            }
        }
    };
}

database_enum!(EventKind {
    TaskFinished => "task_finished",
    TaskFailed => "task_failed",
    Crash => "crash",
    Stalled => "stalled",
    DeepCheck => "deep_check",
    AutoKilled => "auto_killed",
    TerminationFailed => "termination_failed",
});

database_enum!(EventStatus {
    Pending => "pending",
    Claimed => "claimed",
    Completed => "completed",
    RetryWait => "retry_wait",
    Failed => "failed",
});

database_enum!(IntegrationEventKind {
    UnknownCallbackGroup => "unknown_callback_group",
});

database_enum!(IncidentTransition {
    Opened => "opened",
    Updated => "updated",
    Unchanged => "unchanged",
    Resolved => "resolved",
});

database_enum!(IncidentStatus {
    Open => "open",
    Acknowledged => "acknowledged",
    Resolved => "resolved",
});

database_enum!(SubmissionStatus {
    Pending => "pending",
    Accepted => "accepted",
    Adopted => "adopted",
    Unreconciled => "unreconciled",
    Failed => "failed",
});

database_enum!(AgentRunStatus {
    Starting => "starting",
    Running => "running",
    Completed => "completed",
    Failed => "failed",
    TimedOut => "timed_out",
    Cancelled => "cancelled",
});

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum AgentContextMode {
    Fresh,
    Resume { session_id: String },
    ResumeLatest,
}

impl AgentContextMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Resume { .. } => "resume",
            Self::ResumeLatest => "resume_latest",
        }
    }

    pub fn session_id(&self) -> Option<&str> {
        match self {
            Self::Resume { session_id } => Some(session_id),
            Self::Fresh | Self::ResumeLatest => None,
        }
    }

    pub fn from_db_parts(
        mode: &str,
        session_id: Option<String>,
    ) -> Result<Self, ModelEnumParseError> {
        match mode {
            "fresh" => Ok(Self::Fresh),
            "resume" => session_id
                .filter(|value| !value.trim().is_empty())
                .map(|session_id| Self::Resume { session_id })
                .ok_or_else(|| ModelEnumParseError {
                    enum_name: "AgentContextMode",
                    value: "resume without session_id".to_owned(),
                }),
            "resume_latest" => Ok(Self::ResumeLatest),
            _ => Err(ModelEnumParseError {
                enum_name: "AgentContextMode",
                value: mode.to_owned(),
            }),
        }
    }
}

database_enum!(TerminationRequestStatus {
    Requested => "requested",
    Sent => "sent",
    Confirmed => "confirmed",
    TimedOut => "timed_out",
    Failed => "failed",
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Project {
    pub project_id: String,
    pub root_path: PathBuf,
    pub pueue_group: String,
    pub config_path: PathBuf,
    pub enabled: bool,
    pub paused: bool,
    pub halted_reason: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewProject {
    pub project_id: String,
    pub root_path: PathBuf,
    pub pueue_group: String,
    pub config_path: PathBuf,
    pub enabled: bool,
    pub paused: bool,
    pub created_at: i64,
}

impl NewProject {
    pub fn new(
        project_id: impl Into<String>,
        root_path: impl Into<PathBuf>,
        pueue_group: impl Into<String>,
        config_path: impl Into<PathBuf>,
        created_at: i64,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            root_path: root_path.into(),
            pueue_group: pueue_group.into(),
            config_path: config_path.into(),
            enabled: true,
            paused: false,
            created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub event_id: i64,
    pub project_id: String,
    pub kind: EventKind,
    pub dedup_key: String,
    pub payload: Value,
    pub status: EventStatus,
    pub attempts: i64,
    pub not_before: i64,
    pub lease_until: Option<i64>,
    pub created_at: i64,
    pub completed_at: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewEvent {
    pub project_id: String,
    pub kind: EventKind,
    pub dedup_key: String,
    pub payload: Value,
    pub not_before: i64,
    pub created_at: i64,
}

impl NewEvent {
    pub fn new(
        project_id: impl Into<String>,
        kind: EventKind,
        dedup_key: impl Into<String>,
        payload: Value,
        not_before: i64,
        created_at: i64,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            kind,
            dedup_key: dedup_key.into(),
            payload,
            not_before,
            created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IntegrationEvent {
    pub integration_event_id: i64,
    pub kind: IntegrationEventKind,
    pub dedup_key: String,
    pub payload: Value,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewIntegrationEvent {
    pub kind: IntegrationEventKind,
    pub dedup_key: String,
    pub payload: Value,
    pub created_at: i64,
}

impl NewIntegrationEvent {
    pub fn new(
        kind: IntegrationEventKind,
        dedup_key: impl Into<String>,
        payload: Value,
        created_at: i64,
    ) -> Self {
        Self {
            kind,
            dedup_key: dedup_key.into(),
            payload,
            created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Incident {
    pub incident_id: i64,
    pub project_id: String,
    pub kind: String,
    pub task_key: Option<String>,
    pub fingerprint: String,
    pub status: IncidentStatus,
    pub first_seen_at: i64,
    pub last_seen_at: i64,
    pub acknowledged_at: Option<i64>,
    pub resolved_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewIncident {
    pub project_id: String,
    pub kind: String,
    pub task_key: Option<String>,
    pub fingerprint: String,
    pub seen_at: i64,
}

impl NewIncident {
    pub fn new(
        project_id: impl Into<String>,
        kind: impl Into<String>,
        task_key: Option<impl Into<String>>,
        fingerprint: impl Into<String>,
        seen_at: i64,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            kind: kind.into(),
            task_key: task_key.map(Into::into),
            fingerprint: fingerprint.into(),
            seen_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncidentUpdate {
    pub incident: Incident,
    pub transition: IncidentTransition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submission {
    pub submission_id: String,
    pub project_id: String,
    pub argv: Vec<String>,
    pub created_at: i64,
    pub pueue_task_id: Option<i64>,
    pub task_signature: Option<String>,
    pub status: SubmissionStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSubmission {
    pub submission_id: String,
    pub project_id: String,
    pub argv: Vec<String>,
    pub created_at: i64,
    pub status: SubmissionStatus,
}

impl NewSubmission {
    pub fn new(
        submission_id: impl Into<String>,
        project_id: impl Into<String>,
        argv: Vec<String>,
        created_at: i64,
    ) -> Self {
        Self {
            submission_id: submission_id.into(),
            project_id: project_id.into(),
            argv,
            created_at,
            status: SubmissionStatus::Pending,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRun {
    pub run_id: i64,
    pub project_id: String,
    pub primary_event_id: i64,
    pub pid: Option<i64>,
    pub status: AgentRunStatus,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub exit_code: Option<i64>,
    pub log_path: PathBuf,
    pub last_error: Option<String>,
    pub context_mode: AgentContextMode,
    pub context_session_id: Option<String>,
    pub context_lineage: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAgentRun {
    pub project_id: String,
    pub primary_event_id: i64,
    pub pid: Option<i64>,
    pub status: AgentRunStatus,
    pub started_at: i64,
    pub log_path: PathBuf,
    pub context_mode: AgentContextMode,
    pub context_session_id: Option<String>,
    pub context_lineage: Vec<String>,
}

impl NewAgentRun {
    pub fn new(
        project_id: impl Into<String>,
        primary_event_id: i64,
        pid: Option<i64>,
        status: AgentRunStatus,
        started_at: i64,
        log_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            primary_event_id,
            pid,
            status,
            started_at,
            log_path: log_path.into(),
            context_mode: AgentContextMode::Fresh,
            context_session_id: None,
            context_lineage: Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_context(
        project_id: impl Into<String>,
        primary_event_id: i64,
        pid: Option<i64>,
        status: AgentRunStatus,
        started_at: i64,
        log_path: impl Into<PathBuf>,
        context_mode: AgentContextMode,
        context_session_id: Option<String>,
        context_lineage: Vec<String>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            primary_event_id,
            pid,
            status,
            started_at,
            log_path: log_path.into(),
            context_mode,
            context_session_id,
            context_lineage,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRunEvent {
    pub project_id: String,
    pub run_id: i64,
    pub event_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminationRequest {
    pub request_id: i64,
    pub incident_id: i64,
    pub project_id: String,
    pub task_signature: String,
    pub reason: String,
    pub status: TerminationRequestStatus,
    pub requested_at: i64,
    pub grace_until: Option<i64>,
    pub confirmed_at: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTerminationRequest {
    pub incident_id: i64,
    pub project_id: String,
    pub task_signature: String,
    pub reason: String,
    pub status: TerminationRequestStatus,
    pub requested_at: i64,
    pub grace_until: Option<i64>,
}

impl NewTerminationRequest {
    pub fn new(
        incident_id: i64,
        project_id: impl Into<String>,
        task_signature: impl Into<String>,
        reason: impl Into<String>,
        requested_at: i64,
        grace_until: Option<i64>,
    ) -> Self {
        Self {
            incident_id,
            project_id: project_id.into(),
            task_signature: task_signature.into(),
            reason: reason.into(),
            status: TerminationRequestStatus::Requested,
            requested_at,
            grace_until,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskObservation {
    pub project_id: String,
    pub task_signature: String,
    pub pueue_task_id: i64,
    pub pueue_group: String,
    pub command: Vec<String>,
    pub state: String,
    pub enqueued_at: Option<i64>,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub result: Option<String>,
    pub observed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTaskObservation {
    pub project_id: String,
    pub task_signature: String,
    pub pueue_task_id: i64,
    pub pueue_group: String,
    pub command: Vec<String>,
    pub state: String,
    pub enqueued_at: Option<i64>,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub result: Option<String>,
    pub observed_at: i64,
}

impl NewTaskObservation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        project_id: impl Into<String>,
        task_signature: impl Into<String>,
        pueue_task_id: i64,
        pueue_group: impl Into<String>,
        command: Vec<String>,
        state: impl Into<String>,
        enqueued_at: Option<i64>,
        started_at: Option<i64>,
        ended_at: Option<i64>,
        result: Option<String>,
        observed_at: i64,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            task_signature: task_signature.into(),
            pueue_task_id,
            pueue_group: pueue_group.into(),
            command,
            state: state.into(),
            enqueued_at,
            started_at,
            ended_at,
            result,
            observed_at,
        }
    }
}

pub(crate) fn path_text<'path>(
    path: &'path Path,
    field: &'static str,
) -> Result<&'path str, crate::AppError> {
    path.to_str()
        .ok_or(crate::AppError::Configuration { field })
}
