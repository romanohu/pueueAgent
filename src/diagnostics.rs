use std::{cmp::Ordering, collections::BTreeMap};

use rusqlite::params;
use serde::Serialize;

use crate::{
    db::{
        AgentRunRepository, Db, EventRepository, IncidentRepository, TerminationRequestRepository,
    },
    models::{
        AgentRun, AgentRunStatus, Event, EventKind, EventStatus, Incident, IncidentStatus, Project,
        TerminationRequest, TerminationRequestStatus,
    },
    pueue::PueueTask,
    service::ServiceStatus,
    status::{PueueSnapshot, StatusInput},
    AppError,
};

pub const JSON_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_SUMMARY_LIMIT: usize = 8;
pub const DEFAULT_EVENT_LIST_LIMIT: usize = 100;
pub const MAX_EVENT_LIST_LIMIT: usize = 1_000;
pub const MAX_TASK_SUMMARY_LIMIT: usize = MAX_EVENT_LIST_LIMIT;

const MAX_SUMMARY_TEXT_BYTES: usize = 240;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventFilter {
    pub kind: Option<EventKind>,
    pub status: Option<EventStatus>,
    pub limit: usize,
}

impl EventFilter {
    pub fn new(kind: Option<EventKind>, status: Option<EventStatus>, limit: usize) -> Self {
        Self {
            kind,
            status,
            limit,
        }
    }
}

pub fn render_project_status_json(
    db: &Db,
    project: &Project,
    input: &StatusInput,
) -> Result<String, AppError> {
    let events = EventRepository::new(db).list_filtered(
        &project.project_id,
        &EventFilter::new(None, None, DEFAULT_SUMMARY_LIMIT),
    )?;
    let incidents =
        IncidentRepository::new(db).list_by_project(&project.project_id, DEFAULT_SUMMARY_LIMIT)?;
    let terminations = TerminationRequestRepository::new(db)
        .list_by_project(&project.project_id, DEFAULT_SUMMARY_LIMIT)?;
    let agent_runs =
        AgentRunRepository::new(db).list_by_project(&project.project_id, DEFAULT_SUMMARY_LIMIT)?;

    let report = ProjectStatusReport {
        schema_version: JSON_SCHEMA_VERSION,
        project: ProjectSummary::from(project),
        daemon: DaemonSummary {
            status: service_status_label(input.daemon_health),
        },
        pueue: pueue_summary(project, &input.pueue),
        events: EventSection {
            counts: event_counts(db, &project.project_id)?,
            recent: events.iter().map(EventSummary::from).collect(),
        },
        incidents: IncidentSection {
            counts: incident_counts(db, &project.project_id)?,
            recent: incidents.iter().map(IncidentSummary::from).collect(),
        },
        termination: TerminationSection {
            counts: termination_counts(db, &project.project_id)?,
            recent: terminations.iter().map(TerminationSummary::from).collect(),
        },
        agent_runs: AgentRunSection {
            counts: agent_run_counts(db, &project.project_id)?,
            recent: agent_runs.iter().map(AgentRunSummary::from).collect(),
        },
        policy: FutureSection::default(),
        resource: FutureSection::default(),
    };

    serde_json::to_string(&report).map_err(|source| AppError::Serialization {
        operation: "serialize JSON project status",
        source,
    })
}

#[derive(Serialize)]
struct ProjectStatusReport {
    schema_version: u32,
    project: ProjectSummary,
    daemon: DaemonSummary,
    pueue: PueueSummary,
    events: EventSection,
    incidents: IncidentSection,
    termination: TerminationSection,
    agent_runs: AgentRunSection,
    policy: FutureSection,
    resource: FutureSection,
}

#[derive(Serialize)]
struct ProjectSummary {
    project_id: String,
    root_path: String,
    pueue_group: String,
    enabled: bool,
    paused: bool,
    halted: bool,
}

impl From<&Project> for ProjectSummary {
    fn from(project: &Project) -> Self {
        Self {
            project_id: project.project_id.clone(),
            root_path: project.root_path.to_string_lossy().into_owned(),
            pueue_group: project.pueue_group.clone(),
            enabled: project.enabled,
            paused: project.paused,
            halted: project.halted_reason.is_some(),
        }
    }
}

#[derive(Serialize)]
struct DaemonSummary {
    status: &'static str,
}

#[derive(Serialize)]
struct PueueSummary {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_task_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    returned_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    truncated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_tasks: Option<Vec<PueueTaskSummary>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_category: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_summary: Option<String>,
}

#[derive(Serialize)]
struct PueueTaskSummary {
    task_id: i64,
    state: String,
    command_summary: String,
    enqueued_at: Option<String>,
    started_at: Option<String>,
}

#[derive(Serialize)]
struct EventSection {
    counts: EventCounts,
    recent: Vec<EventSummary>,
}

#[derive(Default, Serialize)]
struct EventCounts {
    pending: i64,
    claimed: i64,
    completed: i64,
    retry_wait: i64,
    failed: i64,
}

#[derive(Serialize)]
struct EventSummary {
    event_id: i64,
    kind: EventKind,
    status: EventStatus,
    attempts: i64,
    lease_until: Option<i64>,
    created_at: i64,
    completed_at: Option<i64>,
    error_category: Option<&'static str>,
    error_summary: Option<String>,
}

impl From<&Event> for EventSummary {
    fn from(event: &Event) -> Self {
        Self {
            event_id: event.event_id,
            kind: event.kind,
            status: event.status,
            attempts: event.attempts,
            lease_until: event.lease_until,
            created_at: event.created_at,
            completed_at: event.completed_at,
            error_category: event.last_error.as_ref().map(|_| "event_processing"),
            error_summary: event
                .last_error
                .as_ref()
                .map(|_| safe_error_summary("event_processing")),
        }
    }
}

#[derive(Serialize)]
struct IncidentSection {
    counts: IncidentCounts,
    recent: Vec<IncidentSummary>,
}

#[derive(Default, Serialize)]
struct IncidentCounts {
    total: i64,
    open: i64,
    acknowledged: i64,
    resolved: i64,
}

#[derive(Serialize)]
struct IncidentSummary {
    incident_id: i64,
    kind: String,
    task_key: Option<String>,
    status: IncidentStatus,
    first_seen_at: i64,
    last_seen_at: i64,
    acknowledged_at: Option<i64>,
    resolved_at: Option<i64>,
}

impl From<&Incident> for IncidentSummary {
    fn from(incident: &Incident) -> Self {
        Self {
            incident_id: incident.incident_id,
            kind: bounded_summary(&incident.kind),
            task_key: incident.task_key.as_deref().map(bounded_summary),
            status: incident.status,
            first_seen_at: incident.first_seen_at,
            last_seen_at: incident.last_seen_at,
            acknowledged_at: incident.acknowledged_at,
            resolved_at: incident.resolved_at,
        }
    }
}

#[derive(Serialize)]
struct TerminationSection {
    counts: TerminationCounts,
    recent: Vec<TerminationSummary>,
}

#[derive(Default, Serialize)]
struct TerminationCounts {
    requested: i64,
    dispatching: i64,
    sent: i64,
    confirmed: i64,
    timed_out: i64,
    failed: i64,
}

#[derive(Serialize)]
struct TerminationSummary {
    request_id: i64,
    incident_id: i64,
    task_signature: String,
    status: TerminationRequestStatus,
    requested_at: i64,
    confirmed_at: Option<i64>,
    error_category: Option<&'static str>,
    error_summary: Option<String>,
}

impl From<&TerminationRequest> for TerminationSummary {
    fn from(request: &TerminationRequest) -> Self {
        Self {
            request_id: request.request_id,
            incident_id: request.incident_id,
            task_signature: bounded_summary(&request.task_signature),
            status: request.status,
            requested_at: request.requested_at,
            confirmed_at: request.confirmed_at,
            error_category: request.last_error.as_ref().map(|_| "termination_dispatch"),
            error_summary: request
                .last_error
                .as_ref()
                .map(|_| safe_error_summary("termination_dispatch")),
        }
    }
}

#[derive(Serialize)]
struct AgentRunSection {
    counts: AgentRunCounts,
    recent: Vec<AgentRunSummary>,
}

#[derive(Default, Serialize)]
struct AgentRunCounts {
    total: i64,
    starting: i64,
    running: i64,
    completed: i64,
    failed: i64,
    timed_out: i64,
    cancelled: i64,
    active: i64,
}

#[derive(Serialize)]
struct AgentRunSummary {
    run_id: i64,
    primary_event_id: i64,
    pid: Option<i64>,
    status: AgentRunStatus,
    started_at: i64,
    finished_at: Option<i64>,
    exit_code: Option<i64>,
    error_category: Option<&'static str>,
    error_summary: Option<String>,
}

impl From<&AgentRun> for AgentRunSummary {
    fn from(run: &AgentRun) -> Self {
        Self {
            run_id: run.run_id,
            primary_event_id: run.primary_event_id,
            pid: run.pid,
            status: run.status,
            started_at: run.started_at,
            finished_at: run.finished_at,
            exit_code: run.exit_code,
            error_category: run.last_error.as_ref().map(|_| "agent_run"),
            error_summary: run
                .last_error
                .as_ref()
                .map(|_| safe_error_summary("agent_run")),
        }
    }
}

#[derive(Default, Serialize)]
struct FutureSection {}

fn pueue_summary(project: &Project, snapshot: &PueueSnapshot) -> PueueSummary {
    match snapshot {
        PueueSnapshot::Tasks(tasks) => {
            let mut active = tasks
                .iter()
                .filter(|task| {
                    task.group == project.pueue_group
                        && !task.is_terminal()
                        && !task.state.eq_ignore_ascii_case("queued")
                })
                .collect::<Vec<_>>();
            active.sort_unstable_by(|left, right| compare_pueue_tasks(left, right));
            let active_task_count = active.len();
            let task_limit = DEFAULT_SUMMARY_LIMIT.min(MAX_TASK_SUMMARY_LIMIT);
            active.truncate(task_limit);
            let returned_count = active.len();
            PueueSummary {
                status: "ok",
                active_task_count: Some(active_task_count),
                returned_count: Some(returned_count),
                truncated: Some(returned_count < active_task_count),
                active_tasks: Some(active.into_iter().map(PueueTaskSummary::from).collect()),
                error_category: None,
                error_summary: None,
            }
        }
        PueueSnapshot::Error(_error) => PueueSummary {
            status: "error",
            active_task_count: None,
            returned_count: None,
            truncated: None,
            active_tasks: None,
            error_category: Some("pueue_status"),
            error_summary: Some(safe_error_summary("pueue_status")),
        },
    }
}

impl From<&PueueTask> for PueueTaskSummary {
    fn from(task: &PueueTask) -> Self {
        Self {
            task_id: task.id,
            state: task.state.to_ascii_lowercase(),
            command_summary: executable_summary(&task.command),
            enqueued_at: task.enqueued_at.clone(),
            started_at: task.started_at.clone(),
        }
    }
}

fn compare_pueue_tasks(left: &&PueueTask, right: &&PueueTask) -> Ordering {
    let left_time = task_time(left);
    let right_time = task_time(right);
    match (left_time.parse::<i64>(), right_time.parse::<i64>()) {
        (Ok(left_time), Ok(right_time)) => right_time.cmp(&left_time),
        _ => right_time.cmp(left_time),
    }
    .then_with(|| right.id.cmp(&left.id))
}

fn task_time(task: &PueueTask) -> &str {
    task.started_at
        .as_deref()
        .or(task.enqueued_at.as_deref())
        .unwrap_or_default()
}

fn service_status_label(status: ServiceStatus) -> &'static str {
    match status {
        ServiceStatus::Running => "running",
        ServiceStatus::Stopped => "stopped",
        ServiceStatus::NotInstalled => "not_installed",
    }
}

fn event_counts(db: &Db, project_id: &str) -> Result<EventCounts, AppError> {
    let counts = grouped_counts(db, "events", project_id, "query JSON event counts")?;
    Ok(EventCounts {
        pending: count(&counts, "pending"),
        claimed: count(&counts, "claimed"),
        completed: count(&counts, "completed"),
        retry_wait: count(&counts, "retry_wait"),
        failed: count(&counts, "failed"),
    })
}

fn incident_counts(db: &Db, project_id: &str) -> Result<IncidentCounts, AppError> {
    let counts = grouped_counts(db, "incidents", project_id, "query JSON incident counts")?;
    Ok(IncidentCounts {
        total: counts.values().sum(),
        open: count(&counts, "open"),
        acknowledged: count(&counts, "acknowledged"),
        resolved: count(&counts, "resolved"),
    })
}

fn termination_counts(db: &Db, project_id: &str) -> Result<TerminationCounts, AppError> {
    let counts = grouped_counts(
        db,
        "termination_requests",
        project_id,
        "query JSON termination counts",
    )?;
    Ok(TerminationCounts {
        requested: count(&counts, "requested"),
        dispatching: count(&counts, "dispatching"),
        sent: count(&counts, "sent"),
        confirmed: count(&counts, "confirmed"),
        timed_out: count(&counts, "timed_out"),
        failed: count(&counts, "failed"),
    })
}

fn agent_run_counts(db: &Db, project_id: &str) -> Result<AgentRunCounts, AppError> {
    let counts = grouped_counts(db, "agent_runs", project_id, "query JSON agent run counts")?;
    let starting = count(&counts, "starting");
    let running = count(&counts, "running");
    Ok(AgentRunCounts {
        total: counts.values().sum(),
        starting,
        running,
        completed: count(&counts, "completed"),
        failed: count(&counts, "failed"),
        timed_out: count(&counts, "timed_out"),
        cancelled: count(&counts, "cancelled"),
        active: starting + running,
    })
}

fn grouped_counts(
    db: &Db,
    table: &'static str,
    project_id: &str,
    operation: &'static str,
) -> Result<BTreeMap<String, i64>, AppError> {
    let connection = db.connect()?;
    let sql = match table {
        "events" => "SELECT status, COUNT(*) FROM events WHERE project_id = ?1 GROUP BY status",
        "incidents" => {
            "SELECT status, COUNT(*) FROM incidents WHERE project_id = ?1 GROUP BY status"
        }
        "termination_requests" => {
            "SELECT status, COUNT(*) FROM termination_requests WHERE project_id = ?1 GROUP BY status"
        }
        "agent_runs" => {
            "SELECT status, COUNT(*) FROM agent_runs WHERE project_id = ?1 GROUP BY status"
        }
        _ => unreachable!("only fixed status count tables are supported"),
    };
    let mut statement = connection
        .prepare(sql)
        .map_err(|source| AppError::Database { operation, source })?;
    let rows = statement
        .query_map(params![project_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(|source| AppError::Database { operation, source })?;
    rows.collect::<Result<BTreeMap<_, _>, _>>()
        .map_err(|source| AppError::Database { operation, source })
}

fn count(counts: &BTreeMap<String, i64>, status: &str) -> i64 {
    counts.get(status).copied().unwrap_or(0)
}

fn bounded_summary(value: &str) -> String {
    let redacted = value
        .split_whitespace()
        .map(redact_sensitive_token)
        .collect::<Vec<_>>()
        .join(" ");
    bounded_text(&redacted)
}

fn safe_error_summary(category: &'static str) -> String {
    let summary = match category {
        "pueue_status" => "Pueue status command failed",
        "event_processing" => "event processing failed",
        "termination_dispatch" => "termination dispatch failed",
        "agent_run" => "agent run failed",
        _ => "diagnostic operation failed",
    };
    bounded_text(summary)
}

fn bounded_text(value: &str) -> String {
    let normalized = value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    let normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.len() <= MAX_SUMMARY_TEXT_BYTES {
        normalized
    } else {
        let mut prefix = String::new();
        for character in normalized.chars() {
            if prefix.len() + character.len_utf8() > MAX_SUMMARY_TEXT_BYTES - 3 {
                break;
            }
            prefix.push(character);
        }
        format!("{prefix}...")
    }
}

fn redact_sensitive_token(token: &str) -> String {
    let lower = token.to_ascii_lowercase();
    if is_sensitive_token(&lower) {
        "[redacted]".to_owned()
    } else if is_path_token(token) {
        "[path]".to_owned()
    } else {
        token.to_owned()
    }
}

fn is_sensitive_token(lower: &str) -> bool {
    [
        "token",
        "secret",
        "password",
        "passwd",
        "prompt",
        "transcript",
        "log_path",
        "apikey",
        "api_key",
        "authorization",
        "bearer",
        "credential",
        "cookie",
        "private_key",
        "session_id",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

fn is_path_token(token: &str) -> bool {
    token.starts_with('/')
        || token.starts_with("~/")
        || token.starts_with("./")
        || token.starts_with("../")
        || token.contains('/')
        || token.contains('\\')
        || token
            .as_bytes()
            .get(1)
            .is_some_and(|character| *character == b':')
}

fn executable_summary(command: &str) -> String {
    let token = command
        .split_whitespace()
        .find(|token| !is_environment_assignment(token));
    let Some(token) = token else {
        return "unknown".to_owned();
    };
    let executable = token.rsplit('/').next().unwrap_or("unknown");
    let executable = executable.rsplit('\\').next().unwrap_or("unknown");
    let lower = executable.to_ascii_lowercase();
    if token.starts_with('-')
        || executable.contains('=')
        || is_shell_wrapper(&lower)
        || executable.is_empty()
        || executable.len() > 64
        || !executable.chars().all(is_safe_executable_character)
    {
        "unknown".to_owned()
    } else {
        executable.to_owned()
    }
}

fn is_environment_assignment(token: &str) -> bool {
    let Some((name, _value)) = token.split_once('=') else {
        return false;
    };
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

fn is_shell_wrapper(executable: &str) -> bool {
    matches!(
        executable,
        "ash"
            | "bash"
            | "busybox"
            | "cmd"
            | "command"
            | "csh"
            | "dash"
            | "doas"
            | "env"
            | "exec"
            | "fish"
            | "ksh"
            | "powershell"
            | "pwsh"
            | "sh"
            | "sudo"
            | "tcsh"
            | "zsh"
    )
}

fn is_safe_executable_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '+' | '-')
}
