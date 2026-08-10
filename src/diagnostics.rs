use std::{cmp::Ordering, collections::BTreeMap};

use rusqlite::params;
use serde::Serialize;

use crate::{
    config,
    db::{
        AgentRunRepository, Db, EventRepository, IncidentRepository, InterventionRepository,
        SubmissionRepository, TaskObservationRepository, TerminationRequestRepository,
    },
    models::{
        AgentRun, AgentRunStatus, Event, EventKind, EventStatus, Incident, IncidentStatus, Project,
        Submission, TaskObservation, TerminationRequest, TerminationRequestStatus,
    },
    output::{bounded_redacted_text, render_id},
    pueue::PueueTask,
    service::{callback_command, ServicePaths, ServiceStatus},
    status::{PueueSnapshot, StatusInput},
    AppError,
};

pub const JSON_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_SUMMARY_LIMIT: usize = 8;
pub const DEFAULT_EVENT_LIST_LIMIT: usize = 100;
pub const MAX_EVENT_LIST_LIMIT: usize = 1_000;
pub const MAX_TASK_SUMMARY_LIMIT: usize = MAX_EVENT_LIST_LIMIT;

const MAX_TASK_AGENT_RUNS: usize = 64;

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

#[derive(Serialize)]
struct EventListReport {
    schema_version: u32,
    project_id: String,
    events: Vec<EventSummary>,
}

pub fn render_events(
    db: &Db,
    project: &Project,
    filter: &EventFilter,
    json: bool,
) -> Result<String, AppError> {
    let events = EventRepository::new(db).list_filtered(&project.project_id, filter)?;
    if json {
        return serde_json::to_string(&EventListReport {
            schema_version: JSON_SCHEMA_VERSION,
            project_id: project.project_id.clone(),
            events: events.iter().map(EventSummary::from).collect(),
        })
        .map_err(|source| AppError::Serialization {
            operation: "serialize event diagnostics",
            source,
        });
    }

    Ok(events
        .iter()
        .map(|event| {
            format!(
                "{} kind={} status={} attempts={} lease={} created_at={} completed_at={} error={}",
                render_id("event", event.event_id),
                event.kind,
                event.status,
                event.attempts,
                event
                    .lease_until
                    .map_or_else(|| "none".to_owned(), |value| value.to_string()),
                event.created_at,
                event
                    .completed_at
                    .map_or_else(|| "none".to_owned(), |value| value.to_string()),
                event
                    .last_error
                    .as_deref()
                    .map(bounded_summary)
                    .unwrap_or_else(|| "none".to_owned())
            )
        })
        .collect::<Vec<_>>()
        .join("\n"))
}

#[derive(Serialize)]
struct SubmissionSummary {
    submission_id: String,
    status: crate::models::SubmissionStatus,
    created_at: i64,
    pueue_task_id: Option<i64>,
    task_signature: Option<String>,
}

impl From<&Submission> for SubmissionSummary {
    fn from(submission: &Submission) -> Self {
        Self {
            submission_id: bounded_summary(&submission.submission_id),
            status: submission.status,
            created_at: submission.created_at,
            pueue_task_id: submission.pueue_task_id,
            task_signature: submission.task_signature.as_deref().map(bounded_summary),
        }
    }
}

#[derive(Serialize)]
struct TaskObservationSummary {
    task_signature: String,
    pueue_task_id: i64,
    pueue_group: String,
    state: String,
    command_summary: String,
    enqueued_at: Option<i64>,
    started_at: Option<i64>,
    ended_at: Option<i64>,
    observed_at: i64,
}

impl From<&TaskObservation> for TaskObservationSummary {
    fn from(observation: &TaskObservation) -> Self {
        Self {
            task_signature: bounded_summary(&observation.task_signature),
            pueue_task_id: observation.pueue_task_id,
            pueue_group: bounded_summary(&observation.pueue_group),
            state: bounded_redacted_text(&observation.state.to_ascii_lowercase()),
            command_summary: executable_summary(&observation.command.join(" ")),
            enqueued_at: observation.enqueued_at,
            started_at: observation.started_at,
            ended_at: observation.ended_at,
            observed_at: observation.observed_at,
        }
    }
}

#[derive(Serialize)]
struct TaskInspectionReport {
    schema_version: u32,
    project_id: String,
    task_id: i64,
    latest: TaskObservationSummary,
    history: Vec<TaskObservationSummary>,
    submissions: Vec<SubmissionSummary>,
    incidents: Vec<IncidentSummary>,
    events: Vec<EventSummary>,
    terminations: Vec<TerminationSummary>,
    agent_runs: Vec<AgentRunSummary>,
}

pub fn render_task_inspection(
    db: &Db,
    project: &Project,
    task_id: i64,
    json: bool,
) -> Result<String, AppError> {
    let observations = TaskObservationRepository::new(db).find_by_pueue_task(
        &project.project_id,
        task_id,
        MAX_TASK_SUMMARY_LIMIT,
    )?;
    let latest = observations.first().ok_or(AppError::Runtime {
        operation: "find project task observation",
    })?;
    let stable_signature = latest.task_signature.clone();
    let history = observations
        .iter()
        .filter(|observation| observation.task_signature == stable_signature)
        .map(TaskObservationSummary::from)
        .collect::<Vec<_>>();
    let events = EventRepository::new(db).find_by_task_signature(
        &project.project_id,
        &stable_signature,
        MAX_TASK_SUMMARY_LIMIT,
    )?;
    let agent_runs_repository = AgentRunRepository::new(db);
    let mut agent_runs = Vec::new();
    for event in &events {
        let remaining = MAX_TASK_AGENT_RUNS.saturating_sub(agent_runs.len());
        if remaining == 0 {
            break;
        }
        agent_runs.extend(agent_runs_repository.find_by_event(
            &project.project_id,
            event.event_id,
            remaining,
        )?);
    }
    agent_runs.sort_unstable_by(|left, right| {
        right
            .started_at
            .cmp(&left.started_at)
            .then_with(|| right.run_id.cmp(&left.run_id))
    });
    agent_runs.dedup_by_key(|run| run.run_id);
    agent_runs.truncate(MAX_TASK_SUMMARY_LIMIT);
    let report = TaskInspectionReport {
        schema_version: JSON_SCHEMA_VERSION,
        project_id: project.project_id.clone(),
        task_id,
        latest: TaskObservationSummary::from(latest),
        history,
        submissions: SubmissionRepository::new(db)
            .find_by_task_signature(
                &project.project_id,
                &stable_signature,
                MAX_TASK_SUMMARY_LIMIT,
            )?
            .iter()
            .map(SubmissionSummary::from)
            .collect(),
        incidents: IncidentRepository::new(db)
            .find_by_task_key(
                &project.project_id,
                &stable_signature,
                MAX_TASK_SUMMARY_LIMIT,
            )?
            .iter()
            .map(IncidentSummary::from)
            .collect(),
        events: events.iter().map(EventSummary::from).collect(),
        terminations: TerminationRequestRepository::new(db)
            .find_by_task_signature(
                &project.project_id,
                &stable_signature,
                MAX_TASK_SUMMARY_LIMIT,
            )?
            .iter()
            .map(TerminationSummary::from)
            .collect(),
        agent_runs: agent_runs.iter().map(AgentRunSummary::from).collect(),
    };
    if json {
        return serde_json::to_string(&report).map_err(|source| AppError::Serialization {
            operation: "serialize task diagnostics",
            source,
        });
    }
    Ok(format_task_inspection_text(&report))
}

fn format_task_inspection_text(report: &TaskInspectionReport) -> String {
    format!(
        "{} latest_signature={} state={} observed_at={} history={} submissions={} incidents={} events={} terminations={} agent_runs={}",
        render_id("task", report.task_id),
        report.latest.task_signature,
        report.latest.state,
        report.latest.observed_at,
        report.history.len(),
        report.submissions.len(),
        report.incidents.len(),
        report.events.len(),
        report.terminations.len(),
        report.agent_runs.len(),
    )
}

#[derive(Serialize)]
struct ExplanationReport {
    schema_version: u32,
    project_id: String,
    incident: IncidentSummary,
    event: Option<EventSummary>,
    policy: NotConfigured,
    approval: NotConfigured,
    pueue_action: Vec<TerminationSummary>,
    agent_runs: Vec<AgentRunSummary>,
    chain: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct NotConfigured {
    status: &'static str,
}

pub fn render_incident_explanation(
    db: &Db,
    project: &Project,
    incident_id: i64,
    json: bool,
) -> Result<String, AppError> {
    let incident = IncidentRepository::new(db)
        .find_by_project_and_id(&project.project_id, incident_id)?
        .ok_or(AppError::Runtime {
            operation: "find project incident",
        })?;
    let event = EventRepository::new(db).find_by_dedup_key(
        &project.project_id,
        &format!("incident-wake:v1:incident={incident_id}"),
    )?;
    let task_signature = event
        .as_ref()
        .and_then(|event| event.payload.get("task_signature"))
        .and_then(serde_json::Value::as_str)
        .or(incident.task_key.as_deref());
    let terminations = task_signature
        .map(|signature| {
            TerminationRequestRepository::new(db).find_by_task_signature(
                &project.project_id,
                signature,
                MAX_TASK_SUMMARY_LIMIT,
            )
        })
        .transpose()?
        .unwrap_or_default();
    let agent_runs = event
        .as_ref()
        .map(|event| {
            AgentRunRepository::new(db).find_by_event(
                &project.project_id,
                event.event_id,
                MAX_TASK_SUMMARY_LIMIT,
            )
        })
        .transpose()?
        .unwrap_or_default();
    let policy = NotConfigured {
        status: "not_configured",
    };
    let approval = NotConfigured {
        status: "not_configured",
    };
    let pueue_action = terminations
        .iter()
        .map(TerminationSummary::from)
        .collect::<Vec<_>>();
    let mut chain = vec![serde_json::json!({
        "stage": "observation",
        "task_signature": task_signature.map(bounded_summary),
    })];
    chain.push(serde_json::json!({
        "stage": "incident_transition",
        "incident_id": incident.incident_id,
        "status": incident.status,
        "first_seen_at": incident.first_seen_at,
        "last_seen_at": incident.last_seen_at,
    }));
    chain.push(serde_json::json!({
        "stage": "event",
        "event": event.as_ref().map(EventSummary::from),
    }));
    chain.push(serde_json::json!({"stage": "policy", "status": policy.status}));
    chain.push(serde_json::json!({"stage": "approval", "status": approval.status}));
    chain.push(serde_json::json!({
        "stage": "pueue_action",
        "request_ids": terminations.iter().map(|request| request.request_id).collect::<Vec<_>>(),
    }));
    let report = ExplanationReport {
        schema_version: JSON_SCHEMA_VERSION,
        project_id: project.project_id.clone(),
        incident: IncidentSummary::from(&incident),
        event: event.as_ref().map(EventSummary::from),
        policy,
        approval,
        pueue_action,
        agent_runs: agent_runs.iter().map(AgentRunSummary::from).collect(),
        chain,
    };
    if json {
        return serde_json::to_string(&report).map_err(|source| AppError::Serialization {
            operation: "serialize incident explanation",
            source,
        });
    }
    Ok(report
        .chain
        .iter()
        .filter_map(|step| step.get("stage").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>()
        .join(" -> "))
}

#[derive(Debug, Clone)]
pub struct DoctorExternal {
    pub pueue: Result<Vec<PueueTask>, String>,
    pub service: Result<ServiceStatus, String>,
    pub callback: Result<Option<String>, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DoctorCheckStatus {
    Ok,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DoctorCheck {
    pub name: String,
    pub status: DoctorCheckStatus,
    pub summary: String,
    pub remediation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DoctorReport {
    pub schema_version: u32,
    pub status: DoctorCheckStatus,
    pub checks: Vec<DoctorCheck>,
}

impl DoctorReport {
    pub fn has_errors(&self) -> bool {
        self.status == DoctorCheckStatus::Error
    }
}

pub fn render_doctor_report(
    db: &Db,
    project: &Project,
    paths: &ServicePaths,
    external: DoctorExternal,
    now: i64,
    json: bool,
) -> Result<String, AppError> {
    let report = build_doctor_report(db, project, paths, external, now)?;
    render_doctor_report_value(&report, json)
}

pub fn render_doctor_report_value(report: &DoctorReport, json: bool) -> Result<String, AppError> {
    if json {
        return serde_json::to_string(report).map_err(|source| AppError::Serialization {
            operation: "serialize doctor diagnostics",
            source,
        });
    }
    let mut lines = vec![format!("doctor: {}", doctor_status_label(report.status))];
    lines.extend(report.checks.iter().map(|check| {
        format!(
            "{}: {} — {} [{}]",
            check.name,
            doctor_status_label(check.status),
            check.summary,
            check.remediation
        )
    }));
    Ok(lines.join("\n"))
}

pub fn build_doctor_report(
    db: &Db,
    project: &Project,
    paths: &ServicePaths,
    external: DoctorExternal,
    now: i64,
) -> Result<DoctorReport, AppError> {
    let connection = db.connect()?;
    let mut checks = Vec::new();
    let user_version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|source| AppError::Database {
            operation: "query doctor SQLite schema version",
            source,
        })?;
    checks.push(if user_version == 6 {
        doctor_ok("schema.version", "SQLite schema version is 6", "none")
    } else {
        doctor_error(
            "schema.version",
            &format!("SQLite schema version is {user_version}"),
            "run the supported database migration before starting the daemon",
        )
    });

    let required_tables = [
        "projects",
        "events",
        "integration_events",
        "incidents",
        "agent_runs",
        "agent_run_events",
        "submissions",
        "termination_requests",
        "task_observations",
        "operator_logs",
        "interventions",
    ];
    let table_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN (
                 'projects','events','integration_events','incidents','agent_runs',
                 'agent_run_events','submissions','termination_requests','task_observations',
                 'operator_logs','interventions'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(|source| AppError::Database {
            operation: "query doctor SQLite tables",
            source,
        })?;
    checks.push(if table_count == required_tables.len() as i64 {
        doctor_ok(
            "schema.tables",
            "required SQLite tables are present",
            "none",
        )
    } else {
        doctor_error(
            "schema.tables",
            "one or more required SQLite tables are missing",
            "reopen the database with the matching pueue-agent release",
        )
    });
    let required_indexes = [
        "events_claimable_idx",
        "events_project_status_idx",
        "integration_events_kind_created_idx",
        "incidents_active_fingerprint_idx",
        "incidents_project_status_idx",
        "agent_runs_project_status_idx",
        "agent_runs_one_active_per_project_idx",
        "agent_run_events_event_idx",
        "submissions_project_status_idx",
        "termination_requests_project_status_idx",
        "task_observations_group_state_idx",
        "operator_logs_project_created_idx",
        "interventions_project_sequence_idx",
        "interventions_project_status_created_idx",
        "interventions_reservation_lease_idx",
    ];
    let index_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name IN (
                 'events_claimable_idx','events_project_status_idx',
                 'integration_events_kind_created_idx','incidents_active_fingerprint_idx',
                 'incidents_project_status_idx','agent_runs_project_status_idx',
                 'agent_runs_one_active_per_project_idx','agent_run_events_event_idx',
                 'submissions_project_status_idx','termination_requests_project_status_idx',
                 'task_observations_group_state_idx','operator_logs_project_created_idx',
                 'interventions_project_sequence_idx','interventions_project_status_created_idx',
                 'interventions_reservation_lease_idx'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(|source| AppError::Database {
            operation: "query doctor SQLite indexes",
            source,
        })?;
    checks.push(if index_count == required_indexes.len() as i64 {
        doctor_ok(
            "schema.indexes",
            "required SQLite indexes are present",
            "none",
        )
    } else {
        doctor_error(
            "schema.indexes",
            "one or more required SQLite indexes are missing",
            "reopen the database with the matching pueue-agent release",
        )
    });
    let foreign_keys: i64 = connection
        .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
        .map_err(|source| AppError::Database {
            operation: "query doctor SQLite foreign keys",
            source,
        })?;
    checks.push(if foreign_keys == 1 {
        doctor_ok(
            "sqlite.foreign_keys",
            "SQLite foreign keys are enabled",
            "none",
        )
    } else {
        doctor_error(
            "sqlite.foreign_keys",
            "SQLite foreign keys are disabled",
            "enable foreign keys on every database connection",
        )
    });
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .map_err(|source| AppError::Database {
            operation: "query doctor SQLite journal mode",
            source,
        })?;
    checks.push(if journal_mode.eq_ignore_ascii_case("wal") {
        doctor_ok("sqlite.wal", "SQLite WAL mode is enabled", "none")
    } else {
        doctor_warning(
            "sqlite.wal",
            &format!(
                "SQLite journal mode is {}",
                bounded_redacted_text(&journal_mode)
            ),
            "use WAL mode for the daemon database",
        )
    });
    let busy_timeout: i64 = connection
        .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
        .map_err(|source| AppError::Database {
            operation: "query doctor SQLite busy timeout",
            source,
        })?;
    checks.push(if busy_timeout >= 5_000 {
        doctor_ok(
            "sqlite.busy_timeout",
            "SQLite busy timeout is at least 5 seconds",
            "none",
        )
    } else {
        doctor_warning(
            "sqlite.busy_timeout",
            &format!("SQLite busy timeout is {busy_timeout} ms"),
            "configure a busy timeout of at least 5000 ms",
        )
    });

    checks.push(match config::load(&project.config_path) {
        Ok(project_config)
            if project_config.project_id == project.project_id
                && project_config.pueue_group == project.pueue_group =>
        {
            doctor_ok("project.config", "project configuration is valid", "none")
        }
        Ok(_) => doctor_error(
            "project.config",
            "project configuration identity does not match SQLite",
            "align project_id and pueue_group in config.toml and the registered project",
        ),
        Err(error) => doctor_error(
            "project.config",
            &bounded_redacted_text(&error.to_string()),
            "repair .pueue-agent/config.toml and validate it before retrying",
        ),
    });
    checks.push(match &external.pueue {
        Ok(tasks) if tasks.iter().any(|task| task.group == project.pueue_group) => doctor_ok(
            "pueue.status",
            "Pueue status is available and the project group is observed",
            "none",
        ),
        Ok(_) => doctor_warning(
            "pueue.status",
            "Pueue status is available but the project group is not observed",
            "verify that Pueue is running and the dedicated project group exists",
        ),
        Err(error) => doctor_error(
            "pueue.status",
            &bounded_redacted_text(error),
            "start Pueue and verify the configured Pueue profile",
        ),
    });
    checks.push(match &external.callback {
        Ok(Some(callback)) if callback == &callback_command(paths) => doctor_ok(
            "pueue.callback",
            "the daemon callback is registered",
            "none",
        ),
        Ok(Some(_)) => doctor_warning(
            "pueue.callback",
            "a different callback is registered",
            "review the Pueue callback before enabling automatic processing",
        ),
        Ok(None) => doctor_warning(
            "pueue.callback",
            "the daemon callback is not registered",
            "run enable for this project after reviewing the Pueue configuration",
        ),
        Err(error) => doctor_error(
            "pueue.callback",
            &bounded_redacted_text(error),
            "make the Pueue configuration readable and inspect its callback",
        ),
    });
    checks.push(match &external.service {
        Ok(ServiceStatus::Running) => {
            doctor_ok("service.state", "the daemon service is running", "none")
        }
        Ok(ServiceStatus::Stopped) => doctor_warning(
            "service.state",
            "the daemon service is stopped",
            "start the configured user service",
        ),
        Ok(ServiceStatus::NotInstalled) => doctor_warning(
            "service.state",
            "the daemon service is not installed",
            "run enable to install the supported user service",
        ),
        Err(error) => doctor_error(
            "service.state",
            &bounded_redacted_text(error),
            "verify the supported service manager and user session",
        ),
    });
    checks.push(if paths.release_binary.is_file() {
        doctor_ok(
            "service.path",
            "the configured release binary exists",
            "none",
        )
    } else {
        doctor_error(
            "service.path",
            "the configured release binary is missing",
            "build or install the release binary referenced by the service",
        )
    });
    let expired_events: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND status = 'claimed' AND lease_until <= ?2",
            params![&project.project_id, now],
            |row| row.get(0),
        )
        .map_err(|source| AppError::Database {
            operation: "query doctor expired event leases",
            source,
        })?;
    let expired_interventions: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM interventions
             WHERE project_id = ?1 AND status = 'reserved' AND lease_expires_at <= ?2",
            params![&project.project_id, now],
            |row| row.get(0),
        )
        .map_err(|source| AppError::Database {
            operation: "query doctor expired intervention leases",
            source,
        })?;
    let expired_terminations: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM termination_requests
             WHERE project_id = ?1
               AND dispatch_lease_until IS NOT NULL AND dispatch_lease_until <= ?2",
            params![&project.project_id, now],
            |row| row.get(0),
        )
        .map_err(|source| AppError::Database {
            operation: "query doctor expired termination leases",
            source,
        })?;
    let expired_total = expired_events + expired_interventions + expired_terminations;
    checks.push(if expired_total == 0 {
        doctor_ok(
            "leases.expired",
            "no expired event, intervention, or termination leases",
            "none",
        )
    } else {
        doctor_warning(
            "leases.expired",
            &format!("{expired_total} expired lease(s) require scheduler recovery"),
            "keep the daemon running and inspect the affected project records",
        )
    });
    let starting_without_pid: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM agent_runs WHERE project_id = ?1 AND status = 'starting' AND pid IS NULL",
            [&project.project_id],
            |row| row.get(0),
        )
        .map_err(|source| AppError::Database {
            operation: "query doctor starting agent runs",
            source,
        })?;
    checks.push(if starting_without_pid == 0 {
        doctor_ok(
            "agent_runs.starting",
            "no agent run is stuck before PID assignment",
            "none",
        )
    } else {
        doctor_warning(
            "agent_runs.starting",
            &format!("{starting_without_pid} starting agent run(s) have no PID"),
            "inspect the scheduler log and allow startup recovery to release reservations",
        )
    });

    checks.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    let status = if checks
        .iter()
        .any(|check| check.status == DoctorCheckStatus::Error)
    {
        DoctorCheckStatus::Error
    } else if checks
        .iter()
        .any(|check| check.status == DoctorCheckStatus::Warning)
    {
        DoctorCheckStatus::Warning
    } else {
        DoctorCheckStatus::Ok
    };
    Ok(DoctorReport {
        schema_version: JSON_SCHEMA_VERSION,
        status,
        checks,
    })
}

fn doctor_ok(name: &str, summary: &str, remediation: &str) -> DoctorCheck {
    DoctorCheck {
        name: name.to_owned(),
        status: DoctorCheckStatus::Ok,
        summary: bounded_redacted_text(summary),
        remediation: bounded_redacted_text(remediation),
    }
}

fn doctor_warning(name: &str, summary: &str, remediation: &str) -> DoctorCheck {
    DoctorCheck {
        name: name.to_owned(),
        status: DoctorCheckStatus::Warning,
        summary: bounded_redacted_text(summary),
        remediation: bounded_redacted_text(remediation),
    }
}

fn doctor_error(name: &str, summary: &str, remediation: &str) -> DoctorCheck {
    DoctorCheck {
        name: name.to_owned(),
        status: DoctorCheckStatus::Error,
        summary: bounded_redacted_text(summary),
        remediation: bounded_redacted_text(remediation),
    }
}

fn doctor_status_label(status: DoctorCheckStatus) -> &'static str {
    match status {
        DoctorCheckStatus::Ok => "ok",
        DoctorCheckStatus::Warning => "warning",
        DoctorCheckStatus::Error => "error",
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
        interventions: InterventionStatusProjection {
            counts: intervention_counts(db, &project.project_id)?,
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
    interventions: InterventionStatusProjection,
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

#[derive(Serialize)]
struct InterventionStatusProjection {
    counts: InterventionCountsProjection,
}

#[derive(Serialize)]
struct InterventionCountsProjection {
    pending: i64,
    reserved: i64,
    applied: i64,
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
            active.sort_unstable_by(compare_pueue_tasks);
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
            state: bounded_redacted_text(&task.state.to_ascii_lowercase()),
            command_summary: executable_summary(&task.command),
            enqueued_at: task.enqueued_at.as_deref().map(bounded_redacted_text),
            started_at: task.started_at.as_deref().map(bounded_redacted_text),
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

fn intervention_counts(
    db: &Db,
    project_id: &str,
) -> Result<InterventionCountsProjection, AppError> {
    let counts = InterventionRepository::new(db).count_by_project(project_id)?;
    Ok(InterventionCountsProjection {
        pending: counts.pending,
        reserved: counts.reserved,
        applied: counts.applied,
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
    bounded_redacted_text(value)
}

fn safe_error_summary(category: &'static str) -> String {
    let summary = match category {
        "pueue_status" => "Pueue status command failed",
        "event_processing" => "event processing failed",
        "termination_dispatch" => "termination dispatch failed",
        "agent_run" => "agent run failed",
        _ => "diagnostic operation failed",
    };
    bounded_redacted_text(summary)
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
