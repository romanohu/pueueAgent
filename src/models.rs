use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    path::{Path, PathBuf},
    str::FromStr,
};

use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::AppError;

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
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
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
    OperatorWake => "operator_wake",
});

database_enum!(EventStatus {
    Pending => "pending",
    Claimed => "claimed",
    InFlight => "in_flight",
    Dispatched => "dispatched",
    Completed => "completed",
    RetryWait => "retry_wait",
    Failed => "failed",
    DeadLetter => "dead_letter",
});

database_enum!(InterventionStatus {
    Pending => "pending",
    Reserved => "reserved",
    Applied => "applied",
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

database_enum!(SubmissionKind {
    Experiment => "experiment",
    Control => "control",
});

database_enum!(BatchStatus {
    Pending => "pending",
    Dispatching => "dispatching",
    Accepted => "accepted",
    Partial => "partial",
    Failed => "failed",
    Completed => "completed",
});

database_enum!(BatchJobStatus {
    Pending => "pending",
    Dispatching => "dispatching",
    Accepted => "accepted",
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

database_enum!(CampaignState {
    Active => "active",
    BudgetWaiting => "budget_waiting",
    GoalReachedPendingReview => "goal_reached_pending_review",
    Paused => "paused",
    Degraded => "degraded",
    Halted => "halted",
    Retired => "retired",
});

database_enum!(ProposalKind {
    Experiment => "experiment",
    Repair => "repair",
    BroaderSearch => "broader_search",
    Recipe => "recipe",
    CodeChange => "code_change",
    DataEvaluation => "data_evaluation",
});

database_enum!(ProposalStatus {
    Pending => "pending",
    Accepted => "accepted",
    Rejected => "rejected",
});

database_enum!(ExperimentStatus {
    Reserved => "reserved",
    Submitting => "submitting",
    Accepted => "accepted",
    Unreconciled => "unreconciled",
    Succeeded => "succeeded",
    Failed => "failed",
    Cancelled => "cancelled",
});

database_enum!(BudgetDimension {
    Experiment => "experiment",
    AgentRun => "agent_run",
    CodeChange => "code_change",
});

database_enum!(BudgetReservationStatus {
    Reserved => "reserved",
    Consumed => "consumed",
    Released => "released",
});

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExperimentTerminalOutcome<'a> {
    Succeeded,
    Failed {
        failure_code: &'a str,
        failure_fingerprint: &'a str,
    },
    Cancelled,
}

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
    Dispatching => "dispatching",
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
    pub kind: SubmissionKind,
    pub metadata: Value,
    pub origin_agent_run_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Campaign {
    pub campaign_id: String,
    pub project_id: String,
    pub objective_text: String,
    pub objective_digest: String,
    pub initial_argv: Vec<String>,
    pub state: CampaignState,
    pub state_reason: Option<String>,
    pub baseline_experiment_id: Option<String>,
    pub next_eligible_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub proposal_id: String,
    pub campaign_id: String,
    pub kind: ProposalKind,
    pub status: ProposalStatus,
    pub hypothesis: String,
    pub source_experiment_id: Option<String>,
    pub argv: Vec<String>,
    pub working_directory: String,
    pub expected_evidence: Vec<String>,
    pub canonical_digest: String,
    pub reject_reason: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Experiment {
    pub experiment_id: String,
    pub campaign_id: String,
    pub proposal_id: String,
    pub submission_id: String,
    pub parent_experiment_id: Option<String>,
    pub attempt: i64,
    pub status: ExperimentStatus,
    pub pueue_task_id: Option<i64>,
    pub task_signature: Option<String>,
    pub failure_code: Option<String>,
    pub failure_fingerprint: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub finished_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetReservation {
    pub reservation_id: String,
    pub campaign_id: String,
    pub experiment_id: Option<String>,
    pub dimension: BudgetDimension,
    pub subject_key: String,
    pub status: BudgetReservationStatus,
    pub window_started_at: i64,
    pub window_ends_at: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSubmission {
    pub submission_id: String,
    pub project_id: String,
    pub argv: Vec<String>,
    pub created_at: i64,
    pub status: SubmissionStatus,
    pub kind: SubmissionKind,
    pub metadata: Value,
    pub origin_agent_run_id: Option<i64>,
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
            kind: SubmissionKind::Experiment,
            metadata: Value::Object(Default::default()),
            origin_agent_run_id: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_kind_metadata(
        submission_id: impl Into<String>,
        project_id: impl Into<String>,
        argv: Vec<String>,
        created_at: i64,
        kind: SubmissionKind,
        metadata: Value,
        origin_agent_run_id: Option<i64>,
    ) -> Self {
        Self {
            submission_id: submission_id.into(),
            project_id: project_id.into(),
            argv,
            created_at,
            status: SubmissionStatus::Pending,
            kind,
            metadata,
            origin_agent_run_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchJob {
    pub request_id: String,
    pub job_id: String,
    pub ordinal: i64,
    pub kind: SubmissionKind,
    pub argv: Vec<String>,
    pub metadata: Value,
    pub status: BatchJobStatus,
    pub pueue_task_id: Option<i64>,
    pub submission_id: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewBatchJob {
    pub job_id: String,
    pub ordinal: i64,
    pub kind: SubmissionKind,
    pub argv: Vec<String>,
    pub metadata: Value,
}

impl NewBatchJob {
    pub fn new(
        job_id: impl Into<String>,
        ordinal: i64,
        kind: SubmissionKind,
        argv: Vec<String>,
        metadata: Value,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            ordinal,
            kind,
            argv,
            metadata,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchRequest {
    pub request_id: String,
    pub project_id: String,
    pub manifest_hash: String,
    pub status: BatchStatus,
    pub lease_until: Option<i64>,
    pub lease_token: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub last_error: Option<String>,
    pub jobs: Vec<BatchJob>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewBatchRequest {
    pub request_id: String,
    pub project_id: String,
    pub manifest_hash: String,
    pub jobs: Vec<NewBatchJob>,
    pub created_at: i64,
}

impl NewBatchRequest {
    pub fn new(
        request_id: impl Into<String>,
        project_id: impl Into<String>,
        manifest_hash: impl Into<String>,
        jobs: Vec<NewBatchJob>,
        created_at: i64,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            project_id: project_id.into(),
            manifest_hash: manifest_hash.into(),
            jobs,
            created_at,
        }
    }
}

/// The bounded, non-secret execution facts retained for an agent run.
///
/// This projection deliberately contains no command line, prompt, or
/// environment data. The database stores it as three nullable text columns so
/// runs created by older callers remain readable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionProjection {
    execution_kind: String,
    executable_path: String,
    executable_identity: String,
}

impl ExecutionProjection {
    pub fn new(
        execution_kind: impl AsRef<str>,
        executable_path: impl AsRef<str>,
        executable_identity: impl AsRef<str>,
    ) -> Result<Self, AppError> {
        let execution_kind = execution_kind.as_ref();
        if !matches!(execution_kind, "codex" | "custom") {
            return Err(AppError::Validation {
                field: "execution_kind",
                message: "must be codex or custom",
            });
        }

        let executable_path = executable_path.as_ref();
        validate_execution_fact(
            "executable_path",
            executable_path,
            MAX_EXECUTABLE_PATH_BYTES,
        )?;
        if !Path::new(executable_path).is_absolute() {
            return Err(AppError::Validation {
                field: "executable_path",
                message: "must be an absolute path",
            });
        }

        let executable_identity = executable_identity.as_ref();
        validate_execution_fact(
            "executable_identity",
            executable_identity,
            MAX_EXECUTABLE_IDENTITY_BYTES,
        )?;

        Ok(Self {
            execution_kind: execution_kind.to_owned(),
            executable_path: executable_path.to_owned(),
            executable_identity: executable_identity.to_owned(),
        })
    }

    pub fn execution_kind(&self) -> &str {
        &self.execution_kind
    }

    pub fn executable_path(&self) -> &str {
        &self.executable_path
    }

    pub fn executable_identity(&self) -> &str {
        &self.executable_identity
    }
}

pub const MAX_EXECUTABLE_PATH_BYTES: usize = 4096;
pub const MAX_EXECUTABLE_IDENTITY_BYTES: usize = 256;

fn validate_execution_fact(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), AppError> {
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(AppError::Validation {
            field,
            message: "must be non-empty, bounded UTF-8 without control characters",
        });
    }
    Ok(())
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
    pub launch_gate_state: String,
    pub context_mode: AgentContextMode,
    pub context_session_id: Option<String>,
    pub context_lineage: Vec<String>,
    pub execution_kind: Option<String>,
    pub executable_path: Option<String>,
    pub executable_identity: Option<String>,
    pub policy_code: Option<String>,
    pub failure_stage: Option<String>,
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
    pub execution: Option<ExecutionProjection>,
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
            execution: None,
        }
    }

    /// Attach the bounded non-secret execution projection used by the native
    /// binding path.  The legacy constructor intentionally leaves it empty.
    pub fn with_execution(mut self, execution: ExecutionProjection) -> Self {
        self.execution = Some(execution);
        self
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
            execution: None,
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
    pub dispatch_lease_until: Option<i64>,
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

pub(crate) fn launch_gate_marker_path(log_path: &Path) -> PathBuf {
    let mut marker = log_path.as_os_str().to_os_string();
    marker.push(".gate-started");
    PathBuf::from(marker)
}
