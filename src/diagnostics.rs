use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use rusqlite::{params, OptionalExtension};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    config,
    db::{
        inferred_pre_binding_policy_code, AgentRunRepository, CampaignRepository,
        CampaignStatusProjection, Db, EventExecutionProjection, EventRepository,
        HealthRepository, IncidentRepository, InterventionRepository, ProjectRepository,
        SubmissionRepository, TaskObservationRepository,
        TerminationRequestRepository,
        LATEST_SCHEMA_VERSION,
    },
    execution_policy::{
        inspect_pueue_config_path, load_existing_policy, resolve_project_policy, CampaignLimits,
        LogUnsafeReason, PolicyViolation, PolicyViolationCode, PolicyViolationDetail,
        ResolvedExecutionPolicy,
    },
    environment::MAX_PRIVATE_TEMP_RUN_ID,
    models::{
        AgentRun, AgentRunStatus, Event, EventKind, EventStatus, Incident, IncidentStatus, Project,
        Submission, TaskObservation, TerminationRequest, TerminationRequestStatus,
    },
    output::{
        bounded_execution_path, bounded_redacted_text, bounded_typed_text, format_state,
        human_header, human_summary, render_id, DecisionStatusProjection,
    },
    pueue::{PueueTask, PUEUE_TIMEOUT},
    pueue_security::MAX_PUEUE_OUTPUT_BYTES,
    project_logs::{inspect_agent_log_dir, ProjectRootLogReader},
    service::{callback_command, ServicePaths, ServiceStatus},
    state,
    status::{current_decision_projection, PueueSnapshot, StatusInput},
    AppError,
};

pub const JSON_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_SUMMARY_LIMIT: usize = 8;
pub const DEFAULT_EVENT_LIST_LIMIT: usize = 100;
pub const MAX_EVENT_LIST_LIMIT: usize = 1_000;
pub const MAX_TASK_SUMMARY_LIMIT: usize = MAX_EVENT_LIST_LIMIT;
pub const MAX_HEALTH_ROW_LIMIT: usize = 50;
pub const MAX_METRICS_ROW_LIMIT: usize = 50;

const MAX_TASK_AGENT_RUNS: usize = 64;
const MAX_RESTART_UNCERTAIN_SAMPLES: i64 = 3;
const MAX_DECISION_DOCTOR_PAYLOAD_BYTES: i64 = 128 * 1024;
const MAX_DECISION_DOCTOR_DIGEST_BYTES: i64 = 256;

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
    root_path: String,
    events: Vec<EventSummary>,
}

pub fn render_events(
    db: &Db,
    project: &Project,
    filter: &EventFilter,
    json: bool,
) -> Result<String, AppError> {
    let event_repository = EventRepository::new(db);
    let events = event_repository.list_filtered(&project.project_id, filter)?;
    let event_ids = events.iter().map(|event| event.event_id).collect::<Vec<_>>();
    let execution = event_repository.latest_execution_projections(&project.project_id, &event_ids)?;
    if json {
        let summaries = events
            .iter()
            .map(|event| {
                EventSummary::from_event(event, execution.get(&event.event_id))
            })
            .collect::<Vec<_>>();
        return serde_json::to_string(&EventListReport {
            schema_version: JSON_SCHEMA_VERSION,
            project_id: project.project_id.clone(),
            root_path: bounded_execution_path(&project.root_path.to_string_lossy())
                .unwrap_or_else(|| "[invalid]".to_owned()),
            events: summaries,
        })
        .map_err(|source| AppError::Serialization {
            operation: "serialize event diagnostics",
            source,
        });
    }

    let mut lines = vec![human_header("events", &project.project_id)];
    lines.push(format!(
        "root: {}",
        bounded_execution_path(&project.root_path.to_string_lossy())
            .unwrap_or_else(|| "[invalid]".to_owned())
    ));
    lines.push("EVENT STATE KIND ATTEMPTS NOT_BEFORE LEASE CREATED COMPLETED RUN EXECUTION PATH POLICY STAGE ERROR".to_owned());
    lines.extend(events.iter().map(|event| {
        let projection = EventSummary::from_event(event, execution.get(&event.event_id));
        format!(
            "{} state={} kind={} attempts={} not_before={} lease={} created_at={} completed_at={} run_id={} execution_kind={} executable_path={} policy_code={} failure_stage={} error={}",
            render_id("event", event.event_id),
            format_state(event.status.as_str()),
            event.kind,
            event.attempts,
            event.not_before,
            event
                .lease_until
                .map_or_else(|| "none".to_owned(), |value| value.to_string()),
            event.created_at,
            event
                .completed_at
                .map_or_else(|| "none".to_owned(), |value| value.to_string()),
            execution
                .get(&event.event_id)
                .map(|projection| projection.run_id)
                .map_or_else(|| "none".to_owned(), |value| value.to_string()),
            projection.execution_kind.unwrap_or_else(|| "none".to_owned()),
            projection.executable_path.unwrap_or_else(|| "none".to_owned()),
            projection.policy_code.unwrap_or_else(|| "none".to_owned()),
            projection.failure_stage.unwrap_or_else(|| "none".to_owned()),
            event
                .last_error
                .as_deref()
                .map(bounded_summary)
                .unwrap_or_else(|| "none".to_owned()),
        )
    }));
    lines.push(human_summary(format!("{} event(s) shown", events.len())));
    Ok(lines.join("\n"))
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
    pub project_id: String,
    pub root_path: String,
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
    lines.push(format!("project: {}", bounded_redacted_text(&report.project_id)));
    lines.push(format!("root: {}", report.root_path));
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
    let project_roots = registered_project_roots(db)?;
    let policy = load_existing_policy(&paths.policy_load_input(
        project_roots.clone(),
        paths.release_binary.clone(),
    ));
    build_doctor_report_with_policy_and_roots(
        db,
        project,
        paths,
        external,
        now,
        &policy,
        &project_roots,
    )
}

pub fn build_doctor_report_with_policy(
    db: &Db,
    project: &Project,
    paths: &ServicePaths,
    external: DoctorExternal,
    now: i64,
    policy: &Result<ResolvedExecutionPolicy, PolicyViolation>,
) -> Result<DoctorReport, AppError> {
    let project_roots = registered_project_roots(db)?;
    build_doctor_report_with_policy_and_roots(
        db,
        project,
        paths,
        external,
        now,
        policy,
        &project_roots,
    )
}

/// Build doctor projections with the exact registered-root inventory used to
/// load policy, while retaining the non-mutating diagnostics boundary.
pub fn build_doctor_report_with_policy_and_roots(
    db: &Db,
    project: &Project,
    paths: &ServicePaths,
    external: DoctorExternal,
    now: i64,
    policy: &Result<ResolvedExecutionPolicy, PolicyViolation>,
    project_roots: &[std::path::PathBuf],
) -> Result<DoctorReport, AppError> {
    let connection = db.connect()?;
    let mut checks = Vec::new();
    let user_version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|source| AppError::Database {
            operation: "query doctor SQLite schema version",
            source,
        })?;
    checks.push(if user_version == LATEST_SCHEMA_VERSION {
        doctor_ok(
            "schema.version",
            &format!("SQLite schema version is {LATEST_SCHEMA_VERSION}"),
            "none",
        )
    } else {
        doctor_error(
            "schema.version",
            &format!("SQLite schema version is {user_version}"),
            "run the supported database migration before starting the daemon",
        )
    });

    let canonical_state_path = state::path(&project.root_path);
    let canonical_state = match state::load_if_present(&canonical_state_path) {
        Ok(Some(canonical_state)) => {
            checks.push(doctor_ok(
                "state.schema",
                &format!("canonical state is valid ({})", canonical_state.summary()),
                "none",
            ));
            Some(canonical_state)
        }
        Ok(None) => {
            checks.push(doctor_warning(
                "state.schema",
                "canonical state.json is missing",
                "create state.json with init for a new project; existing STATE.md is not rewritten",
            ));
            None
        }
        Err(error) => {
            checks.push(doctor_error(
                "state.schema",
                &bounded_redacted_text(&error.to_string()),
                "repair state.json using the supported schema without rewriting STATE.md",
            ));
            None
        }
    };
    if let Some(canonical_state) = canonical_state {
        let state_markdown_path = project.root_path.join(".pueue-agent/STATE.md");
        if state_markdown_path.is_file() {
            match state::load_state_markdown(&state_markdown_path) {
                Ok(state_markdown) => {
                    let warnings = state::check_consistency(&canonical_state, &state_markdown);
                    if warnings.is_empty() {
                        checks.push(doctor_ok(
                            "state.consistency",
                            &format!(
                                "STATE.md has no canonical contradiction ({})",
                                canonical_state.summary()
                            ),
                            "none",
                        ));
                    } else {
                        let summary = warnings
                            .iter()
                            .map(|warning| warning.summary.as_str())
                            .collect::<Vec<_>>()
                            .join("; ");
                        checks.push(doctor_warning(
                            "state.consistency",
                            &summary,
                            "treat state.json as canonical and review STATE.md without automatic repair",
                        ));
                    }
                }
                Err(error) => checks.push(doctor_warning(
                    "state.consistency",
                    &bounded_redacted_text(&error.to_string()),
                    "keep state.json canonical and inspect supplementary STATE.md manually",
                )),
            }
        } else {
            checks.push(doctor_warning(
                "state.consistency",
                &format!(
                    "supplementary STATE.md is missing ({})",
                    canonical_state.summary()
                ),
                "review the human-readable project context; state.json remains canonical",
            ));
        }
    }

    let campaign = CampaignRepository::new(db).doctor_projection_for_project(&project.project_id)?;
    checks.push(if campaign.live_campaign_count <= 1 {
        doctor_ok(
            "campaign.live_count",
            if campaign.live_campaign_count == 0 {
                "no live campaign is registered"
            } else {
                "exactly one live campaign is registered"
            },
            "none",
        )
    } else {
        doctor_error(
            "campaign.live_count",
            "more than one live campaign is registered",
            "inspect campaign rows without mutating them from doctor",
        )
    });
    if campaign.live_campaign_count != 0 {
        checks.push(if campaign.baseline_linkage_errors == 0 {
            doctor_ok(
                "campaign.baseline_linkage",
                "the live campaign baseline links to its initial experiment",
                "none",
            )
        } else {
            doctor_error(
                "campaign.baseline_linkage",
                &format!(
                    "{} live campaign baseline linkage error(s)",
                    campaign.baseline_linkage_errors
                ),
                "inspect campaign, proposal, and experiment linkage without automatic repair",
            )
        });
        checks.push(if campaign.orphan_reservations == 0 {
            doctor_ok(
                "campaign.orphan_reservations",
                "campaign reservations have valid scoped owners",
                "none",
            )
        } else {
            doctor_error(
                "campaign.orphan_reservations",
                &format!(
                    "{} campaign reservation(s) have no valid scoped owner",
                    campaign.orphan_reservations
                ),
                "inspect reservation linkage without deleting records from doctor",
            )
        });
        checks.push(if campaign.task_identity_disagreements == 0 {
            doctor_ok(
                "campaign.task_identity",
                "experiment and submission task identities agree",
                "none",
            )
        } else {
            doctor_error(
                "campaign.task_identity",
                &format!(
                    "{} experiment/submission task identity disagreement(s)",
                    campaign.task_identity_disagreements
                ),
                "quarantine conflicting task identities; doctor does not reconcile them",
            )
        });
        checks.push(if campaign.submission_boundary_count == 0 {
            doctor_ok(
                "campaign.submission_boundaries",
                "no submitting or unreconciled campaign intent is present",
                "none",
            )
        } else {
            doctor_warning(
                "campaign.submission_boundaries",
                &format!(
                    "{} submitting or unreconciled campaign intent(s) require recovery review",
                    campaign.submission_boundary_count
                ),
                "keep unreconciled intents quarantined and inspect exact task identity",
            )
        });
        checks.push(if campaign.budget_wake_errors == 0 {
            doctor_ok(
                "campaign.budget_wake",
                "every budget-waiting campaign has a finite wake time",
                "none",
            )
        } else {
            doctor_error(
                "campaign.budget_wake",
                &format!(
                    "{} budget-waiting campaign(s) have no finite wake time",
                    campaign.budget_wake_errors
                ),
                "inspect rolling reservation windows; doctor does not wake campaigns",
            )
        });
        checks.push(match (
            campaign.objective_digest.as_deref(),
            state::load_objective(&project.root_path),
        ) {
            (Some(expected), Ok(on_disk)) if expected == on_disk.digest => doctor_ok(
                "campaign.objective_digest",
                "STATE.md matches the immutable campaign objective digest",
                "none",
            ),
            (Some(_), Ok(_)) => doctor_warning(
                "campaign.objective_digest",
                "STATE.md differs from the immutable campaign objective digest",
                "restore or review STATE.md; doctor never rewrites the active snapshot",
            ),
            (Some(_), Err(_)) => doctor_warning(
                "campaign.objective_digest",
                "STATE.md cannot be validated against the immutable campaign objective digest",
                "inspect STATE.md without changing the active SQLite snapshot",
            ),
            (None, _) => doctor_error(
                "campaign.objective_digest",
                "the live campaign objective digest is unavailable",
                "inspect the campaign row without rewriting it from doctor",
            ),
        });
    }
    checks.extend(decision_doctor_checks(db, project, &connection, now, policy)?);

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
        "agent_run_id_sequence",
    ];
    let table_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN (
                 'projects','events','integration_events','incidents','agent_runs',
                 'agent_run_events','submissions','termination_requests','task_observations',
                 'operator_logs','interventions','agent_run_id_sequence'
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
    let sequence_count = connection
        .query_row(
            "SELECT COUNT(*) FROM agent_run_id_sequence",
            [],
            |row| row.get::<_, i64>(0),
        )
        .ok();
    let sequence_row = connection
        .query_row(
            "SELECT sequence_id, last_run_id FROM agent_run_id_sequence
             WHERE sequence_id = 1",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional();
    let sequence_valid = match (sequence_count, sequence_row) {
        (Some(1), Ok(Some((1, last_run_id))))
            if (0..=MAX_PRIVATE_TEMP_RUN_ID).contains(&last_run_id) => connection
            .query_row(
                "SELECT COALESCE(MAX(run_id), 0) FROM agent_runs",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|max_run_id| {
                max_run_id <= MAX_PRIVATE_TEMP_RUN_ID && last_run_id >= max_run_id
            })
            .unwrap_or(false),
        _ => false,
    };
    checks.push(if sequence_valid {
        doctor_ok(
            "schema.agent_run_id_sequence",
            "agent run ID sequence singleton is valid",
            "none",
        )
    } else {
        doctor_error(
            "schema.agent_run_id_sequence",
            "agent run ID sequence singleton is missing or invalid",
            "reopen the database with the matching pueue-agent release",
        )
    });
    let required_indexes = [
        "events_claimable_idx",
        "events_project_status_idx",
        "events_project_status_not_before_idx",
        "integration_events_kind_created_idx",
        "incidents_active_fingerprint_idx",
        "incidents_project_status_idx",
        "agent_runs_project_status_idx",
        "agent_runs_one_active_per_project_idx",
        "agent_run_events_event_idx",
        "submissions_project_status_idx",
        "submissions_project_kind_status_idx",
        "submissions_project_origin_agent_run_idx",
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
                 'events_project_status_not_before_idx',
                 'integration_events_kind_created_idx','incidents_active_fingerprint_idx',
                 'incidents_project_status_idx','agent_runs_project_status_idx',
                 'agent_runs_one_active_per_project_idx','agent_run_events_event_idx',
                 'submissions_project_status_idx','submissions_project_kind_status_idx',
                 'submissions_project_origin_agent_run_idx','termination_requests_project_status_idx',
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
    let pueue_bounds_summary = format!(
        "Pueue commands use timeout={}s and independent stdout/stderr caps={} bytes",
        PUEUE_TIMEOUT.as_secs(),
        MAX_PUEUE_OUTPUT_BYTES,
    );
    checks.push(doctor_ok_with_typed_summary(
        "pueue.bounds",
        &pueue_bounds_summary,
        "none",
    ));
    checks.push(match inspect_pueue_config_path(
        &paths.pueue_config,
        project_roots,
    ) {
        Ok(()) => doctor_ok(
            "pueue.config",
            "the configured lexical Pueue profile passes no-follow anchoring checks",
            "none",
        ),
        Err(_) => doctor_error(
            "pueue.config",
            "the configured lexical Pueue profile is unavailable or unsafe",
            "restore the configured Pueue profile; doctor does not create or replace it",
        ),
    });
    checks.extend(execution_doctor_checks(db, project, policy));
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

    let unlinked_ack_events: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1
               AND status IN ('in_flight', 'dispatched')
               AND NOT EXISTS (
                   SELECT 1
                   FROM agent_run_events
                   JOIN agent_runs
                     ON agent_runs.project_id = agent_run_events.project_id
                    AND agent_runs.run_id = agent_run_events.run_id
                   WHERE agent_run_events.project_id = events.project_id
                     AND agent_run_events.event_id = events.event_id
               )",
            [&project.project_id],
            |row| row.get(0),
        )
        .map_err(|source| AppError::Database {
            operation: "query doctor event ack consistency",
            source,
        })?;
    checks.push(if unlinked_ack_events == 0 {
        doctor_ok(
            "events.ack_consistency",
            "all in-flight and dispatched events have a same-project agent run link",
            "none",
        )
    } else {
        doctor_error(
            "events.ack_consistency",
            &format!(
                "{unlinked_ack_events} in-flight or dispatched event(s) have no same-project agent run link"
            ),
            "inspect the affected event and agent-run records without repairing them from doctor",
        )
    });

    let dead_letter_events: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND status = 'dead_letter'",
            [&project.project_id],
            |row| row.get(0),
        )
        .map_err(|source| AppError::Database {
            operation: "query doctor dead-letter events",
            source,
        })?;
    checks.push(if dead_letter_events == 0 {
        doctor_ok(
            "events.dead_letter",
            "no dead-letter events are present",
            "none",
        )
    } else {
        doctor_warning(
            "events.dead_letter",
            &format!("{dead_letter_events} dead-letter event(s) are present"),
            "inspect bounded dead-letter details with events --status dead-letter",
        )
    });

    let policy_blocked = EventRepository::new(db).policy_blocked_counts(&project.project_id)?;
    let policy_stages = AgentRunRepository::new(db).policy_failure_stage_counts(&project.project_id)?;
    checks.push(if policy_blocked.is_empty() && policy_stages.is_empty() {
        doctor_ok(
            "execution.policy_blocked",
            "no policy-blocked events or run failures are present",
            "none",
        )
    } else {
        let event_summary = policy_blocked
            .iter()
            .map(|(code, count)| format!("{code}/pre_binding={count}"))
            .collect::<Vec<_>>()
            .join(", ");
        let stage_summary = policy_stages
            .iter()
            .map(|((code, stage), count)| format!("{code}/{stage}={count}"))
            .collect::<Vec<_>>()
            .join(", ");
        let summary = [event_summary, stage_summary]
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join(", ");
        doctor_warning_with_typed_summary(
            "execution.policy_blocked",
            &format!("policy-blocked events: {summary}"),
            "inspect bounded policy code and stage diagnostics; doctor does not retry or repair events",
        )
    });

    checks.push(if unlinked_ack_events == 0 {
        doctor_ok(
            "execution.ack_consistency",
            "all in-flight and dispatched events have a same-project agent run link",
            "none",
        )
    } else {
        doctor_error(
            "execution.ack_consistency",
            &format!(
                "{unlinked_ack_events} in-flight or dispatched event(s) have no same-project agent run link"
            ),
            "inspect the affected event and agent-run records without repairing them from doctor",
        )
    });

    let restart_uncertain_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1
               AND status = 'dead_letter'
               AND last_error LIKE 'restart_interruption: execution outcome unknown%'",
            [&project.project_id],
            |row| row.get(0),
        )
        .map_err(|source| AppError::Database {
            operation: "query doctor restart-uncertain events",
            source,
        })?;
    let restart_samples = if restart_uncertain_count == 0 {
        Vec::new()
    } else {
        let mut statement = connection
            .prepare(
                "SELECT last_error FROM events
                 WHERE project_id = ?1
                   AND status = 'dead_letter'
                   AND last_error LIKE 'restart_interruption: execution outcome unknown%'
                 ORDER BY event_id DESC LIMIT ?2",
            )
            .map_err(|source| AppError::Database {
                operation: "prepare doctor restart-uncertain samples",
                source,
            })?;
        let rows = statement
            .query_map(
                params![&project.project_id, MAX_RESTART_UNCERTAIN_SAMPLES],
                |row| row.get::<_, Option<String>>(0),
            )
            .map_err(|source| AppError::Database {
                operation: "query doctor restart-uncertain samples",
                source,
            })?;
        rows.collect::<Result<Vec<Option<String>>, _>>()
            .map_err(|source| AppError::Database {
                operation: "read doctor restart-uncertain samples",
                source,
            })?
            .into_iter()
            .flatten()
            .map(|sample| bounded_redacted_text(&sample))
            .collect::<Vec<_>>()
    };
    checks.push(if restart_uncertain_count == 0 {
        doctor_ok(
            "events.restart_uncertain",
            "no restart-uncertain event reasons are recorded",
            "none",
        )
    } else {
        let sample_summary = restart_samples.join("; ");
        doctor_warning(
            "events.restart_uncertain",
            &format!(
                "{restart_uncertain_count} restart-uncertain event(s): {sample_summary}"
            ),
            "review bounded restart-interruption details and decide whether to re-submit manually",
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
        project_id: project.project_id.clone(),
        root_path: bounded_execution_path(&project.root_path.to_string_lossy())
            .unwrap_or_else(|| "[invalid]".to_owned()),
        status,
        checks,
    })
}

fn decision_doctor_checks(
    db: &Db,
    project: &Project,
    connection: &rusqlite::Connection,
    now: i64,
    policy: &Result<ResolvedExecutionPolicy, PolicyViolation>,
) -> Result<Vec<DoctorCheck>, AppError> {
    let current_campaign_id = connection
        .query_row(
            "SELECT campaign_id
             FROM campaigns
             WHERE project_id = ?1 AND state <> 'retired'
             LIMIT 1",
            [&project.project_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|source| AppError::Database {
            operation: "read live decision campaign for doctor",
            source,
        })?;
    let limits = policy
        .as_ref()
        .map(|policy| policy.campaign_limits)
        .unwrap_or_else(|_| CampaignLimits::default());
    let cycle_policy_limit = limits.max_parallel_experiments as usize;
    let attempt_policy_limit = limits.max_decision_attempts_per_cycle as usize;
    let mut cycles = Vec::new();
    let mut unknown_cycle_state = false;
    if let Some(campaign_id) = current_campaign_id.as_deref() {
        for state in ["pending", "analyzing", "waiting", "degraded"] {
            cycles.extend(decision_doctor_cycle_probe(
                connection,
                campaign_id,
                state,
                i64::from(limits.max_parallel_experiments) + 1,
            )?);
        }
        unknown_cycle_state = decision_doctor_unknown_state_exists(connection, campaign_id)?;
    }
    let cycle_overflow = cycles.len() > cycle_policy_limit;
    cycles.truncate(cycle_policy_limit.saturating_add(1));
    if !cycle_overflow {
        if let Some(campaign_id) = current_campaign_id.as_deref() {
            cycles.extend(decision_doctor_cycle_probe(
                connection,
                campaign_id,
                "completed",
                1,
            )?);
        }
    }

    let decision_cycle_count = cycles.len();
    let mut malformed_rows = i64::from(cycle_overflow) + i64::from(unknown_cycle_state);
    let mut lineage_errors = 0_i64;
    let mut active_attempt_count = 0_i64;
    let mut duplicate_agent_bindings = 0_i64;
    let mut agent_run_ids = BTreeSet::new();
    let mut active_running_attempts = 0_i64;
    let mut running_errors = 0_i64;
    let mut waiting_without_wake = 0_i64;
    let mut degraded_without_diagnostics = 0_i64;
    let timeout_seconds = config::load(&project.config_path)
        .ok()
        .map(|config| i64::from(config.agent.timeout_minutes) * 60);

    for cycle in &cycles {
        malformed_rows += i64::from(!cycle.row_valid);
        lineage_errors += i64::from(!cycle.lineage_valid);
        waiting_without_wake +=
            i64::from(cycle.state.as_deref() == Some("waiting") && cycle.next_wake_at.is_none());
        degraded_without_diagnostics += i64::from(
            cycle.state.as_deref() == Some("degraded") && !cycle.degraded_diagnostics_valid,
        );
        let Some(cycle_id) = cycle.cycle_id.as_deref() else {
            continue;
        };
        let attempts = decision_doctor_attempt_probe(
            connection,
            cycle_id,
            &project.project_id,
            i64::from(limits.max_decision_attempts_per_cycle) + 1,
        )?;
        malformed_rows += i64::from(attempts.len() > attempt_policy_limit);
        for attempt in attempts {
            malformed_rows += i64::from(!attempt.row_valid);
            if let Some(agent_run_id) = attempt.agent_run_id {
                if !agent_run_ids.insert(agent_run_id) {
                    duplicate_agent_bindings += 1;
                }
            }
            let active = cycle.state.as_deref() == Some("analyzing")
                && matches!(
                    attempt.state.as_deref(),
                    Some("reserved" | "evidence_ready" | "running" | "decided")
                );
            active_attempt_count += i64::from(active);
            if attempt.state.as_deref() == Some("running") {
                active_running_attempts += i64::from(active);
                let structurally_valid = active
                    && attempt.started_at.is_some()
                    && attempt.agent_run_id.is_some()
                    && attempt.agent_run_exists
                    && attempt.agent_run_project_matches
                    && attempt.agent_run_active;
                let overdue = timeout_seconds.is_some_and(|timeout| {
                    attempt
                        .started_at
                        .is_some_and(|started_at| started_at <= now - timeout)
                });
                running_errors += i64::from(!structurally_valid || overdue);
            }
        }
    }

    let active_conflicts = i64::from(active_attempt_count > 1) + duplicate_agent_bindings;
    let mut checks = vec![if malformed_rows == 0 {
        doctor_ok(
            "decision.rows",
            if decision_cycle_count == 0 {
                "no live managed decision rows are present"
            } else {
                "bounded live decision rows satisfy policy and typed field constraints"
            },
            "none",
        )
    } else {
        doctor_error(
            "decision.rows",
            &format!("{malformed_rows} malformed or policy-unbounded decision row set(s)"),
            "inspect decision rows without migrating, deleting, or repairing them from doctor",
        )
    }];
    checks.push(if lineage_errors == 0 {
        doctor_ok(
            "decision.lineage",
            if decision_cycle_count == 0 {
                "no live managed decision cycle lineage is present"
            } else {
                "bounded live decision cycles have terminal same-campaign source lineage"
            },
            "none",
        )
    } else {
        doctor_error(
            "decision.lineage",
            &format!("{lineage_errors} orphan or cross-campaign decision cycle(s)"),
            "inspect campaign and terminal experiment lineage without doctor repair",
        )
    });
    checks.push(if active_conflicts == 0 {
        doctor_ok(
            "decision.active_attempts",
            if decision_cycle_count == 0 {
                "no active managed decision attempt is present"
            } else {
                "campaign decision attempts and agent bindings are single-owner"
            },
            "none",
        )
    } else {
        doctor_error(
            "decision.active_attempts",
            &format!("{active_conflicts} duplicate active attempt or agent binding conflict(s)"),
            "pause the campaign and inspect attempt/run ownership without doctor repair",
        )
    });
    checks.push(if running_errors != 0 {
        doctor_error(
            "decision.running_attempts",
            &format!("{running_errors} stale or overdue running decision attempt(s)"),
            "keep the daemon running for bounded recovery; do not rebind or edit attempts manually",
        )
    } else if active_running_attempts != 0 && timeout_seconds.is_none() {
        doctor_warning(
            "decision.running_attempts",
            "running decision attempts cannot be checked against an unavailable timeout",
            "repair config.toml, then rerun doctor without changing decision rows",
        )
    } else {
        doctor_ok(
            "decision.running_attempts",
            if decision_cycle_count == 0 {
                "no running managed decision attempt is present"
            } else {
                "running decision attempts retain active run ownership and timeout bounds"
            },
            "none",
        )
    });
    checks.push(if waiting_without_wake == 0 {
        doctor_ok(
            "decision.wait_wake",
            if decision_cycle_count == 0 {
                "no managed decision wait is present"
            } else {
                "every bounded waiting decision cycle has a finite wake timestamp"
            },
            "none",
        )
    } else {
        doctor_error(
            "decision.wait_wake",
            &format!("{waiting_without_wake} waiting decision cycle(s) have no wake timestamp"),
            "pause the campaign and inspect the persisted wait; doctor does not synthesize a wake",
        )
    });

    let current_decision = if malformed_rows == 0 {
        current_campaign_id
            .as_deref()
            .map(|campaign_id| current_decision_projection(db, campaign_id, now))
            .transpose()?
            .flatten()
    } else {
        None
    };
    let digest_errors = if let Some(current_decision) = current_decision.as_ref() {
        decision_doctor_digest_errors(
            connection,
            &current_decision.cycle.cycle_id,
            i64::from(limits.max_decision_attempts_per_cycle) + 1,
        )?
    } else {
        0
    };
    checks.push(if malformed_rows != 0 {
        doctor_error(
            "decision.digests",
            "decision digest inspection is blocked by malformed bounded rows",
            "inspect the typed row error without exposing or rewriting stored payloads",
        )
    } else if digest_errors == 0 {
        doctor_ok(
            "decision.digests",
            if decision_cycle_count == 0 {
                "no managed decision payload digest is present"
            } else {
                "current decision context and output digests match stored bytes"
            },
            "none",
        )
    } else {
        doctor_error(
            "decision.digests",
            &format!("{digest_errors} current decision payload digest mismatch(es)"),
            "pause the campaign and inspect bounded digest facts without printing or repairing payloads",
        )
    });
    checks.push(if degraded_without_diagnostics == 0 {
        doctor_ok(
            "decision.degraded_diagnostics",
            if decision_cycle_count == 0 {
                "no degraded managed decision cycle is present"
            } else {
                "degraded decision cycles retain bounded failure diagnostics"
            },
            "none",
        )
    } else {
        doctor_error(
            "decision.degraded_diagnostics",
            &format!(
                "{degraded_without_diagnostics} degraded decision cycle(s) lack failure diagnostics"
            ),
            "pause the campaign and inspect the originating attempt; doctor does not invent diagnostics",
        )
    });

    Ok(checks)
}

struct DecisionDoctorCycleProbe {
    cycle_id: Option<String>,
    state: Option<String>,
    next_wake_at: Option<i64>,
    row_valid: bool,
    lineage_valid: bool,
    degraded_diagnostics_valid: bool,
}

const DECISION_DOCTOR_CYCLE_PROBE_SELECT: &str =
    "SELECT CASE WHEN typeof(dc.cycle_id) = 'text' THEN dc.cycle_id END,
            CASE WHEN typeof(dc.state) = 'text' THEN dc.state END,
            CASE WHEN typeof(dc.next_wake_at) = 'integer' THEN dc.next_wake_at END,
            typeof(dc.cycle_id) = 'text'
              AND typeof(dc.campaign_id) = 'text'
              AND typeof(dc.source_experiment_id) = 'text'
              AND typeof(dc.source_terminal_at) = 'integer'
              AND dc.source_terminal_at > 0
              AND typeof(dc.state) = 'text'
              AND dc.state IN ('pending','analyzing','waiting','completed','degraded')
              AND typeof(dc.next_wake_at) IN ('null','integer')
              AND typeof(dc.consecutive_failed_attempts) = 'integer'
              AND dc.consecutive_failed_attempts >= 0
              AND typeof(dc.last_decision_kind) IN ('null','text')
              AND (dc.last_decision_kind IS NULL
                   OR dc.last_decision_kind IN ('proposal','wait'))
              AND COALESCE(length(CAST(dc.last_decision_kind AS BLOB)), 0) <= 128
              AND typeof(dc.last_failure_code) IN ('null','text')
              AND COALESCE(length(CAST(dc.last_failure_code AS BLOB)), 0) <= 128
              AND typeof(dc.last_failure_summary) IN ('null','text')
              AND COALESCE(length(CAST(dc.last_failure_summary AS BLOB)), 0) <= 2048
              AND typeof(dc.created_at) = 'integer'
              AND typeof(dc.updated_at) = 'integer',
            e.experiment_id IS NOT NULL
              AND typeof(e.experiment_id) = 'text'
              AND typeof(e.campaign_id) = 'text'
              AND e.campaign_id = dc.campaign_id
              AND typeof(e.status) = 'text'
              AND e.status IN ('succeeded','failed','cancelled')
              AND typeof(COALESCE(e.finished_at, e.updated_at)) = 'integer'
              AND dc.source_terminal_at = COALESCE(e.finished_at, e.updated_at),
            dc.state IN ('pending','analyzing','waiting','completed')
              OR (typeof(dc.last_failure_code) = 'text'
                  AND length(CAST(dc.last_failure_code AS BLOB)) BETWEEN 1 AND 128
                  AND typeof(dc.last_failure_summary) = 'text'
                  AND length(CAST(dc.last_failure_summary AS BLOB)) BETWEEN 1 AND 2048)
     FROM decision_cycles dc INDEXED BY decision_cycles_campaign_state_source_order_idx
     LEFT JOIN experiments e ON e.experiment_id = dc.source_experiment_id";

fn decision_doctor_cycle_probe(
    connection: &rusqlite::Connection,
    campaign_id: &str,
    state: &'static str,
    limit: i64,
) -> Result<Vec<DecisionDoctorCycleProbe>, AppError> {
    let sql = decision_doctor_cycle_probe_sql(state);
    let mut statement = connection
        .prepare(&sql)
        .map_err(|source| AppError::Database {
            operation: "prepare bounded live decision cycle probe",
            source,
        })?;
    let rows = statement
        .query_map(params![campaign_id, limit], |row| {
            Ok(DecisionDoctorCycleProbe {
                cycle_id: row.get(0)?,
                state: row.get(1)?,
                next_wake_at: row.get(2)?,
                row_valid: row.get(3)?,
                lineage_valid: row.get(4)?,
                degraded_diagnostics_valid: row.get(5)?,
            })
        })
        .map_err(|source| AppError::Database {
            operation: "query bounded live decision cycle probe",
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| AppError::Database {
            operation: "read bounded live decision cycle probe",
            source,
        })?;
    Ok(rows)
}

fn decision_doctor_cycle_probe_sql(state: &'static str) -> String {
    assert!(matches!(
        state,
        "pending" | "analyzing" | "waiting" | "completed" | "degraded"
    ));
    let direction = if state == "completed" { "DESC" } else { "ASC" };
    format!(
        "{DECISION_DOCTOR_CYCLE_PROBE_SELECT}
         WHERE dc.campaign_id = ?1 AND dc.state = '{state}'
         ORDER BY dc.source_terminal_at {direction},
                  dc.source_experiment_id {direction}, dc.cycle_id {direction}
         LIMIT ?2"
    )
}

const DECISION_UNKNOWN_STATE_RANGES: [&str; 6] = [
    "dc.state < 'analyzing'",
    "dc.state > 'analyzing' AND dc.state < 'completed'",
    "dc.state > 'completed' AND dc.state < 'degraded'",
    "dc.state > 'degraded' AND dc.state < 'pending'",
    "dc.state > 'pending' AND dc.state < 'waiting'",
    "dc.state > 'waiting'",
];

fn decision_doctor_unknown_state_exists(
    connection: &rusqlite::Connection,
    campaign_id: &str,
) -> Result<bool, AppError> {
    for predicate in DECISION_UNKNOWN_STATE_RANGES {
        let exists = connection
            .query_row(
                &format!(
                    "SELECT 1
                     FROM decision_cycles dc
                          INDEXED BY decision_cycles_campaign_state_source_order_idx
                     WHERE dc.campaign_id = ?1 AND ({predicate})
                     LIMIT 1"
                ),
                [campaign_id],
                |_| Ok(true),
            )
            .optional()
            .map_err(|source| AppError::Database {
                operation: "probe malformed decision cycle state",
                source,
            })?
            .unwrap_or(false);
        if exists {
            return Ok(true);
        }
    }
    Ok(false)
}

struct DecisionDoctorAttemptProbe {
    state: Option<String>,
    agent_run_id: Option<i64>,
    started_at: Option<i64>,
    row_valid: bool,
    agent_run_exists: bool,
    agent_run_project_matches: bool,
    agent_run_active: bool,
}

fn decision_doctor_attempt_probe(
    connection: &rusqlite::Connection,
    cycle_id: &str,
    project_id: &str,
    limit: i64,
) -> Result<Vec<DecisionDoctorAttemptProbe>, AppError> {
    let mut statement = connection
        .prepare(
            "SELECT CASE WHEN typeof(da.state) = 'text' THEN da.state END,
                    CASE WHEN typeof(da.agent_run_id) = 'integer' THEN da.agent_run_id END,
                    CASE WHEN typeof(da.started_at) = 'integer' THEN da.started_at END,
                    typeof(da.cycle_id) = 'text'
                      AND typeof(da.attempt_number) = 'integer' AND da.attempt_number > 0
                      AND typeof(da.state) = 'text'
                      AND da.state IN ('reserved','evidence_ready','running','decided','failed')
                      AND typeof(da.context_json) IN ('null','text')
                      AND typeof(da.context_digest) IN ('null','text')
                      AND (da.context_json IS NULL) = (da.context_digest IS NULL)
                      AND typeof(da.context_schema_version) IN ('null','integer')
                      AND (da.context_json IS NULL) = (da.context_schema_version IS NULL)
                      AND (da.context_json IS NULL OR da.context_schema_version = 1)
                      AND COALESCE(length(CAST(da.context_json AS BLOB)), 0) <= ?2
                      AND COALESCE(length(CAST(da.context_digest AS BLOB)), 0) <= ?3
                      AND typeof(da.decision_json) IN ('null','text')
                      AND typeof(da.decision_digest) IN ('null','text')
                      AND typeof(da.decision_kind) IN ('null','text')
                      AND (da.decision_json IS NULL) = (da.decision_digest IS NULL)
                      AND (da.decision_json IS NULL) = (da.decision_kind IS NULL)
                      AND (da.decision_kind IS NULL OR da.decision_kind IN ('proposal','wait'))
                      AND COALESCE(length(CAST(da.decision_json AS BLOB)), 0) <= ?2
                      AND COALESCE(length(CAST(da.decision_digest AS BLOB)), 0) <= ?3
                      AND typeof(da.failure_code) IN ('null','text')
                      AND COALESCE(length(CAST(da.failure_code AS BLOB)), 0) <= 128
                      AND typeof(da.failure_summary) IN ('null','text')
                      AND COALESCE(length(CAST(da.failure_summary AS BLOB)), 0) <= 2048
                      AND typeof(da.agent_run_id) IN ('null','integer')
                      AND typeof(da.created_at) = 'integer'
                      AND typeof(da.started_at) IN ('null','integer')
                      AND typeof(da.finished_at) IN ('null','integer'),
                    ar.run_id IS NOT NULL,
                    typeof(ar.project_id) = 'text' AND ar.project_id = ?4,
                    typeof(ar.status) = 'text' AND ar.status IN ('starting','running')
             FROM decision_attempts da
             LEFT JOIN agent_runs ar ON ar.run_id = da.agent_run_id
             WHERE da.cycle_id = ?1
             ORDER BY da.attempt_number
             LIMIT ?5",
        )
        .map_err(|source| AppError::Database {
            operation: "prepare bounded live decision attempt probe",
            source,
        })?;
    let rows = statement
        .query_map(
            params![
                cycle_id,
                MAX_DECISION_DOCTOR_PAYLOAD_BYTES,
                MAX_DECISION_DOCTOR_DIGEST_BYTES,
                project_id,
                limit,
            ],
            |row| {
                Ok(DecisionDoctorAttemptProbe {
                    state: row.get(0)?,
                    agent_run_id: row.get(1)?,
                    started_at: row.get(2)?,
                    row_valid: row.get(3)?,
                    agent_run_exists: row.get(4)?,
                    agent_run_project_matches: row.get(5)?,
                    agent_run_active: row.get(6)?,
                })
            },
        )
        .map_err(|source| AppError::Database {
            operation: "query bounded live decision attempt probe",
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| AppError::Database {
            operation: "read bounded live decision attempt probe",
            source,
        })?;
    Ok(rows)
}

fn decision_doctor_digest_errors(
    connection: &rusqlite::Connection,
    cycle_id: &str,
    limit: i64,
) -> Result<i64, AppError> {
    let mut statement = connection
        .prepare(
            "SELECT typeof(context_json),
                    CASE WHEN typeof(context_json) = 'text' THEN context_json END,
                    typeof(context_digest),
                    CASE WHEN typeof(context_digest) = 'text' THEN context_digest END,
                    typeof(decision_json),
                    CASE WHEN typeof(decision_json) = 'text' THEN decision_json END,
                    typeof(decision_digest),
                    CASE WHEN typeof(decision_digest) = 'text' THEN decision_digest END
             FROM decision_attempts
             WHERE cycle_id = ?1
             ORDER BY attempt_number DESC
             LIMIT ?2",
        )
        .map_err(|source| AppError::Database {
            operation: "prepare bounded decision digest inspection",
            source,
        })?;
    let rows = statement
        .query_map(params![cycle_id, limit], |row| {
            Ok((
                (row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?),
                (row.get::<_, String>(2)?, row.get::<_, Option<String>>(3)?),
                (row.get::<_, String>(4)?, row.get::<_, Option<String>>(5)?),
                (row.get::<_, String>(6)?, row.get::<_, Option<String>>(7)?),
            ))
        })
        .map_err(|source| AppError::Database {
            operation: "query bounded decision digest inspection",
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| AppError::Database {
            operation: "read bounded decision digest inspection",
            source,
        })?;
    Ok(rows
        .into_iter()
        .map(
            |(context_json, context_digest, decision_json, decision_digest)| {
                let context_error = digest_pair_is_invalid(context_json, context_digest);
                let decision_error = digest_pair_is_invalid(decision_json, decision_digest);
                i64::from(context_error) + i64::from(decision_error)
            },
        )
        .sum())
}

fn digest_pair_is_invalid(
    payload: (String, Option<String>),
    digest: (String, Option<String>),
) -> bool {
    match (payload, digest) {
        ((payload_type, None), (digest_type, None))
            if payload_type == "null" && digest_type == "null" =>
        {
            false
        }
        ((payload_type, Some(payload)), (digest_type, Some(digest)))
            if payload_type == "text" && digest_type == "text" =>
        {
            format!("{:x}", Sha256::digest(payload.as_bytes())) != digest
        }
        _ => true,
    }
}

#[cfg(test)]
mod decision_doctor_query_plan_tests {
    use rusqlite::params;
    use tempfile::TempDir;

    use super::{decision_doctor_cycle_probe_sql, DECISION_UNKNOWN_STATE_RANGES};
    use crate::db::Db;

    #[test]
    fn every_cycle_state_probe_uses_the_canonical_index_without_a_scan_or_sort() {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        for state in ["pending", "analyzing", "waiting", "completed", "degraded"] {
            let sql = decision_doctor_cycle_probe_sql(state);
            assert!(!sql.contains("state <>"), "{state}: {sql}");
            let connection = db.connect().unwrap();
            let mut statement = connection
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap();
            let details = statement
                .query_map(params!["campaign-a", 2_i64], |row| {
                    row.get::<_, String>(3)
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(
                details.iter().any(|detail| {
                    detail.contains("decision_cycles_campaign_state_source_order_idx")
                }),
                "{state}: {details:?}"
            );
            assert!(
                details.iter().all(|detail| {
                    !detail.starts_with("SCAN dc") && !detail.contains("TEMP B-TREE")
                }),
                "{state}: {details:?}"
            );
        }
        for predicate in DECISION_UNKNOWN_STATE_RANGES {
            let connection = db.connect().unwrap();
            let sql = format!(
                "EXPLAIN QUERY PLAN
                 SELECT 1
                 FROM decision_cycles dc
                      INDEXED BY decision_cycles_campaign_state_source_order_idx
                 WHERE dc.campaign_id = ?1 AND ({predicate})
                 LIMIT 1"
            );
            let mut statement = connection.prepare(&sql).unwrap();
            let details = statement
                .query_map(["campaign-a"], |row| row.get::<_, String>(3))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(
                details.iter().any(|detail| {
                    detail.contains("decision_cycles_campaign_state_source_order_idx")
                }),
                "{predicate}: {details:?}"
            );
            assert!(
                details.iter().all(|detail| {
                    !detail.starts_with("SCAN dc") && !detail.contains("TEMP B-TREE")
                }),
                "{predicate}: {details:?}"
            );
        }
    }
}

fn registered_project_roots(db: &Db) -> Result<Vec<std::path::PathBuf>, AppError> {
    Ok(ProjectRepository::new(db)
        .list_all()?
        .into_iter()
        .map(|project| project.root_path)
        .collect())
}

/// Check the immutable execution boundary without creating policy files,
/// enrolling executables, repairing logs, or changing database state.
fn execution_doctor_checks(
    db: &Db,
    project: &Project,
    policy: &Result<ResolvedExecutionPolicy, PolicyViolation>,
) -> Vec<DoctorCheck> {
    let (policy, mut checks) = match policy {
        Ok(policy) => {
            let mut checks = vec![doctor_ok(
                "execution.policy",
                "immutable execution policy is readable",
                "none",
            )];
            let anchors_valid = policy.codex_anchor.verify_identity().is_ok()
                && policy.launcher_anchor.verify_identity().is_ok()
                && policy.pueue_anchor.verify_identity().is_ok()
                && policy
                    .pueue_config_anchor
                    .verify_identity(&policy.project_roots)
                    .is_ok();
            checks.push(if anchors_valid {
                doctor_ok(
                    "execution.anchors",
                    "Codex, launcher, Pueue executable, and Pueue config anchors retain their pinned identity",
                    "none",
                )
            } else {
                doctor_error(
                    "execution.anchors",
                    "an execution anchor is missing or replaced",
                    "restore the pinned executable and restart the daemon; doctor does not enroll replacements",
                )
            });
            (Some(policy), checks)
        }
        Err(violation) => {
            (None, vec![
                doctor_error(
                    "execution.policy",
                    &format!("immutable execution policy is unavailable ({})", violation.code.as_str()),
                    "restore the service-owned policy; doctor does not create or repair policy files",
                ),
                doctor_warning(
                    "execution.anchors",
                    "execution anchors were not inspected because policy is unavailable",
                    "restore the immutable execution policy before inspecting anchors",
                ),
                doctor_warning(
                    "execution.project_root",
                    "project root anchor was not inspected because policy is unavailable",
                    "restore the immutable execution policy before inspecting the project root",
                ),
                doctor_warning(
                    "execution.log_contract",
                    "agent log contract was not inspected because policy is unavailable",
                    "restore the immutable execution policy before inspecting agent logs",
                ),
            ])
        }
    };
    let root = if let Some(policy) = policy.as_ref() {
        match config::load(&project.config_path)
            .and_then(|config| resolve_project_policy(policy, project, &config).map_err(AppError::from))
            .and_then(|project_policy| project_policy.root_anchor.verify_identity().map_err(AppError::from))
        {
            Ok(root) => {
                checks.push(doctor_ok(
                    "execution.project_root",
                    "project root retains its pinned identity",
                    "none",
                ));
                Some(root)
            }
            Err(_) => {
                checks.push(doctor_error(
                    "execution.project_root",
                    "project root is missing, replaced, or not admitted by execution policy",
                    "restore the registered root and policy configuration; doctor does not re-enroll it",
                ));
                None
            }
        }
    } else {
        None
    };
    let recent_runs = AgentRunRepository::new(db).list_by_project(
        &project.project_id,
        DEFAULT_SUMMARY_LIMIT,
    );
    checks.push(match &recent_runs {
        Ok(runs) => {
            let summaries = runs
                .iter()
                .filter_map(|run| {
                    let (Some(kind), Some(path), Some(code), Some(stage)) = (
                        run.execution_kind.as_deref(),
                        run.executable_path.as_deref(),
                        run.policy_code.as_deref(),
                        run.failure_stage.as_deref(),
                    ) else {
                        return None;
                    };
                    Some(format!(
                        "run={} kind={} path={} policy={}/{}",
                        run.run_id,
                        bounded_summary(kind),
                        bounded_execution_path(path).unwrap_or_else(|| "[invalid]".to_owned()),
                        bounded_summary(code),
                        bounded_summary(stage),
                    ))
                })
                .collect::<Vec<_>>();
            if summaries.is_empty() {
                doctor_ok(
                    "execution.recent",
                    "no recent run carries policy failure execution evidence",
                    "none",
                )
            } else {
                doctor_warning_with_typed_summary(
                    "execution.recent",
                    &format!("recent bounded execution evidence: {}", summaries.join("; ")),
                    "inspect the associated run without exposing command inputs or log output",
                )
            }
        }
        Err(_) => doctor_warning(
            "execution.recent",
            "recent execution evidence could not be queried",
            "restore database access before inspecting bounded execution diagnostics",
        ),
    });
    if policy.is_some() {
        checks.push(match root {
        Some(root) => match inspect_agent_log_dir(&ProjectRootLogReader::from_verified(root)) {
            Ok(_) => doctor_ok(
                "execution.log_contract",
                "agent log directory satisfies the read-only secure log contract",
                "none",
            ),
            Err(AppError::PolicyViolation { violation })
                if violation.code == PolicyViolationCode::LogUnsafe
                    && violation.detail == PolicyViolationDetail::LogUnsafe(LogUnsafeReason::Missing) => doctor_warning(
                "execution.log_contract",
                "agent log directory is not present yet",
                "the first verified agent run creates .pueue-agent/logs; doctor does not create it",
            ),
            Err(_) => doctor_error(
                "execution.log_contract",
                "agent log directory violates the secure log contract",
                "restore secure .pueue-agent/logs ownership and permissions without doctor repair",
            ),
        },
        None => doctor_warning(
            "execution.log_contract",
            "agent log contract could not be inspected",
            "restore the project root before inspecting agent logs",
        ),
        });
    }
    checks
}

fn doctor_ok(name: &str, summary: &str, remediation: &str) -> DoctorCheck {
    DoctorCheck {
        name: name.to_owned(),
        status: DoctorCheckStatus::Ok,
        summary: bounded_redacted_text(summary),
        remediation: bounded_redacted_text(remediation),
    }
}

fn doctor_ok_with_typed_summary(name: &str, summary: &str, remediation: &str) -> DoctorCheck {
    DoctorCheck {
        name: name.to_owned(),
        status: DoctorCheckStatus::Ok,
        summary: bounded_typed_text(summary),
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

fn doctor_warning_with_typed_summary(
    name: &str,
    summary: &str,
    remediation: &str,
) -> DoctorCheck {
    DoctorCheck {
        name: name.to_owned(),
        status: DoctorCheckStatus::Warning,
        summary: bounded_typed_text(summary),
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
    let campaign = CampaignRepository::new(db)
        .status_projection_for_project(&project.project_id, crate::status::status_timestamp()?)?;
    let campaign = match campaign {
        Some(campaign) => {
            let decision = current_decision_projection(
                db,
                &campaign.campaign_id,
                crate::status::status_timestamp()?,
            )?
                .as_ref()
                .map(DecisionStatusProjection::from);
            Some(CampaignStatusSummary::new(campaign, decision))
        }
        None => None,
    };

    let report = ProjectStatusReport {
        schema_version: JSON_SCHEMA_VERSION,
        project: ProjectSummary::from(project),
        daemon: DaemonSummary {
            status: service_status_label(input.daemon_health),
        },
        service: service_status_label(input.daemon_health),
        automation: automation_status_label(project),
        pueue: pueue_summary(project, &input.pueue),
        events: EventSection {
            counts: event_counts(db, &project.project_id)?,
            recent: event_summaries(db, &project.project_id, &events)?,
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
        health: HealthSection {
            recent: HealthRepository::list_for_project(
                db,
                &project.project_id,
                MAX_HEALTH_ROW_LIMIT,
            )?
            .iter()
            .map(RunningHealthSummary::from)
            .collect(),
        },
        evaluation: {
            let recent = evaluation_metrics(db, &project.project_id, MAX_METRICS_ROW_LIMIT)?;
            if recent.is_empty() {
                None
            } else {
                Some(EvaluationSection { recent })
            }
        },
        campaign,
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
    service: &'static str,
    automation: &'static str,
    pueue: PueueSummary,
    events: EventSection,
    incidents: IncidentSection,
    termination: TerminationSection,
    agent_runs: AgentRunSection,
    interventions: InterventionStatusProjection,
    health: HealthSection,
    #[serde(skip_serializing_if = "Option::is_none")]
    evaluation: Option<EvaluationSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    campaign: Option<CampaignStatusSummary>,
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
            root_path: bounded_execution_path(&project.root_path.to_string_lossy())
                .unwrap_or_else(|| "[invalid]".to_owned()),
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
    in_flight: i64,
    dispatched: i64,
    completed: i64,
    retry_wait: i64,
    failed: i64,
    dead_letter: i64,
}

#[derive(Serialize)]
struct EventSummary {
    event_id: i64,
    kind: EventKind,
    status: EventStatus,
    attempts: i64,
    not_before: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_id: Option<i64>,
    lease_until: Option<i64>,
    created_at: i64,
    completed_at: Option<i64>,
    error_category: Option<&'static str>,
    error_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    executable_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    executable_identity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    policy_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure_stage: Option<String>,
}

impl From<&Event> for EventSummary {
    fn from(event: &Event) -> Self {
        Self::from_event(event, None)
    }
}

impl EventSummary {
    fn from_event(event: &Event, execution: Option<&EventExecutionProjection>) -> Self {
        Self {
            event_id: event.event_id,
            kind: event.kind,
            status: event.status,
            attempts: event.attempts,
            not_before: event.not_before,
            run_id: execution.map(|projection| projection.run_id),
            lease_until: event.lease_until,
            created_at: event.created_at,
            completed_at: event.completed_at,
            error_category: event.last_error.as_ref().map(|_| "event_processing"),
            error_summary: event
                .last_error
                .as_ref()
                .map(|_| safe_error_summary("event_processing")),
            last_error: event.last_error.as_deref().map(bounded_summary),
            execution_kind: execution
                .and_then(|projection| projection.execution_kind.as_deref())
                .map(bounded_summary),
            executable_path: execution
                .and_then(|projection| projection.executable_path.as_deref())
                .and_then(bounded_execution_path),
            executable_identity: execution
                .and_then(|projection| projection.executable_identity.as_deref())
                .map(bounded_summary),
            policy_code: execution
                .and_then(|projection| projection.policy_code.as_deref())
                .map(bounded_summary)
                .or_else(|| inferred_pre_binding_policy_code(event, execution.is_some())),
            failure_stage: execution
                .and_then(|projection| projection.failure_stage.as_deref())
                .map(bounded_summary)
                .or_else(|| {
                    inferred_pre_binding_policy_code(event, execution.is_some())
                        .map(|_| "pre_binding".to_owned())
                }),
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
    execution_kind: Option<String>,
    executable_path: Option<String>,
    executable_identity: Option<String>,
    policy_code: Option<String>,
    failure_stage: Option<String>,
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

#[derive(Serialize)]
struct HealthSection {
    recent: Vec<RunningHealthSummary>,
}

#[derive(Serialize)]
struct RunningHealthSummary {
    experiment_id: String,
    campaign_id: String,
    pueue_task_id: i64,
    state: crate::models::HealthState,
    observation_count: i64,
    last_observed_at: i64,
    updated_at: i64,
    signals: Vec<crate::models::SignalSummaryEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recommended_action: Option<String>,
}

impl From<&crate::models::RunningHealthRow> for RunningHealthSummary {
    fn from(row: &crate::models::RunningHealthRow) -> Self {
        // A malformed persisted signal summary degrades to an explicit empty
        // list so one bad row cannot fail the whole bounded listing.
        let signals: Vec<crate::models::SignalSummaryEntry> =
            serde_json::from_str(&row.signal_summary_json).unwrap_or_default();
        Self {
            experiment_id: bounded_summary(&row.experiment_id),
            campaign_id: bounded_summary(&row.campaign_id),
            pueue_task_id: row.pueue_task_id,
            state: row.state,
            observation_count: row.observation_count,
            last_observed_at: row.last_observed_at,
            updated_at: row.updated_at,
            signals,
            recommended_action: running_health_recommended_action(&row.diagnosis_json),
        }
    }
}

fn running_health_recommended_action(diagnosis_json: &Option<String>) -> Option<String> {
    let diagnosis = diagnosis_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())?;
    diagnosis
        .get("recommended_action")?
        .as_str()
        .map(bounded_summary)
}

#[derive(Serialize)]
struct EvaluationSection {
    recent: Vec<MetricsSummary>,
}

#[derive(Serialize)]
struct MetricsSummary {
    experiment_id: String,
    source: String,
    primary_metric_name: Option<String>,
    primary_metric_value: Option<f64>,
    artifact_defect: Option<String>,
    created_at: i64,
    updated_at: i64,
}

fn evaluation_metrics(
    db: &crate::db::Db,
    project_id: &str,
    limit: usize,
) -> Result<Vec<MetricsSummary>, AppError> {
    let connection = db.connect()?;
    let mut statement = connection
        .prepare(
            "SELECT em.experiment_id, em.source, em.primary_metric_name, em.primary_metric_value, em.artifact_defect, em.created_at, em.updated_at
             FROM experiment_metrics em
             JOIN experiments e ON e.experiment_id = em.experiment_id
             JOIN campaigns c ON c.campaign_id = e.campaign_id
             WHERE c.project_id = ?1
             ORDER BY em.updated_at DESC, em.experiment_id DESC
             LIMIT ?2",
        )
        .map_err(|source| AppError::Database {
            operation: "prepare evaluation metrics listing",
            source,
        })?;
    let rows = statement
        .query_map(
            rusqlite::params![project_id, limit as i64],
            |row| {
                Ok(MetricsSummary {
                    experiment_id: bounded_summary(&row.get::<_, String>(0)?),
                    source: bounded_summary(&row.get::<_, String>(1)?),
                    primary_metric_name: row
                        .get::<_, Option<String>>(2)?
                        .map(|v| bounded_summary(&v)),
                    primary_metric_value: row.get::<_, Option<f64>>(3)?,
                    artifact_defect: row
                        .get::<_, Option<String>>(4)?
                        .map(|v| bounded_summary(&v)),
                    created_at: row.get::<_, i64>(5)?,
                    updated_at: row.get::<_, i64>(6)?,
                })
            },
        )
        .map_err(|source| AppError::Database {
            operation: "query evaluation metrics listing",
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| AppError::Database {
            operation: "read evaluation metrics listing",
            source,
        })?;
    Ok(rows)
}

#[derive(Serialize)]
struct CampaignStatusSummary {
    campaign_id: String,
    state: crate::models::CampaignState,
    state_reason: Option<String>,
    objective_digest: String,
    next_eligible_at: Option<i64>,
    experiment_counts: BTreeMap<String, i64>,
    rolling_usage: BTreeMap<String, i64>,
    unreconciled_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_experiment_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    plateau_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_metric_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_metric_value: Option<f64>,
    decision: Option<DecisionStatusProjection>,
}

impl CampaignStatusSummary {
    fn new(
        campaign: CampaignStatusProjection,
        decision: Option<DecisionStatusProjection>,
    ) -> Self {
        let has_objective = campaign.has_objective;
        let best_experiment_id = if has_objective {
            campaign
                .current_best_experiment_id
                .as_deref()
                .map(bounded_summary)
        } else {
            None
        };
        Self {
            campaign_id: bounded_summary(&campaign.campaign_id),
            state: campaign.state,
            state_reason: campaign.state_reason.as_deref().map(bounded_summary),
            objective_digest: bounded_summary(&campaign.objective_digest),
            next_eligible_at: campaign.next_eligible_at,
            experiment_counts: campaign.experiment_counts,
            rolling_usage: campaign.rolling_usage,
            unreconciled_count: campaign.unreconciled_count,
            best_experiment_id,
            plateau_count: if has_objective {
                Some(campaign.plateau_count)
            } else {
                None
            },
            best_metric_name: if has_objective {
                campaign.primary_metric_name.as_deref().map(bounded_summary)
            } else {
                None
            },
            best_metric_value: if has_objective {
                campaign.primary_metric_value
            } else {
                None
            },
            decision,
        }
    }
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
            execution_kind: run.execution_kind.as_deref().map(bounded_summary),
            executable_path: run.executable_path.as_deref().and_then(bounded_execution_path),
            executable_identity: run.executable_identity.as_deref().map(bounded_summary),
            policy_code: run.policy_code.as_deref().map(bounded_summary),
            failure_stage: run.failure_stage.as_deref().map(bounded_summary),
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

fn automation_status_label(project: &Project) -> &'static str {
    if !project.enabled {
        "disabled"
    } else if project.halted_reason.is_some() {
        "halted"
    } else if project.paused {
        "paused"
    } else {
        "active"
    }
}

fn event_counts(db: &Db, project_id: &str) -> Result<EventCounts, AppError> {
    let counts = grouped_counts(db, "events", project_id, "query JSON event counts")?;
    Ok(EventCounts {
        pending: count(&counts, "pending"),
        claimed: count(&counts, "claimed"),
        in_flight: count(&counts, "in_flight"),
        dispatched: count(&counts, "dispatched"),
        completed: count(&counts, "completed"),
        retry_wait: count(&counts, "retry_wait"),
        failed: count(&counts, "failed"),
        dead_letter: count(&counts, "dead_letter"),
    })
}

fn event_summaries(
    db: &Db,
    project_id: &str,
    events: &[Event],
) -> Result<Vec<EventSummary>, AppError> {
    let repository = EventRepository::new(db);
    let event_ids = events.iter().map(|event| event.event_id).collect::<Vec<_>>();
    let execution = repository.latest_execution_projections(project_id, &event_ids)?;
    Ok(events
        .iter()
        .map(|event| {
            EventSummary::from_event(event, execution.get(&event.event_id))
        })
        .collect())
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::to_value;

    use crate::models::{AgentContextMode, AgentRun, AgentRunStatus};

    use super::AgentRunSummary;

    #[test]
    fn agent_run_summary_serializes_only_execution_audit_facts() {
        let run = AgentRun {
            run_id: 7,
            project_id: "project-a".to_owned(),
            primary_event_id: 11,
            pid: Some(42),
            status: AgentRunStatus::Failed,
            started_at: 100,
            finished_at: Some(110),
            exit_code: Some(1),
            log_path: PathBuf::from("/tmp/agent.log"),
            last_error: Some("policy_blocked:unsafe_codex_argument".to_owned()),
            launch_gate_state: "failed".to_owned(),
            context_mode: AgentContextMode::Fresh,
            context_session_id: None,
            context_lineage: Vec::new(),
            execution_kind: Some("codex".to_owned()),
            executable_path: Some("/trusted/bin/codex".to_owned()),
            executable_identity: Some("device=1;inode=2".to_owned()),
            policy_code: Some("unsafe_codex_argument".to_owned()),
            failure_stage: Some("run_bound_pre_marker".to_owned()),
        };

        let serialized = to_value(AgentRunSummary::from(&run)).unwrap();
        assert_eq!(serialized["execution_kind"], "codex");
        assert_eq!(serialized["executable_path"], "/trusted/bin/codex");
        assert_eq!(serialized["executable_identity"], "device=1;inode=2");
        assert_eq!(serialized["policy_code"], "unsafe_codex_argument");
        assert_eq!(serialized["failure_stage"], "run_bound_pre_marker");
        for absent in ["prompt", "argv", "environment", "credentials"] {
            assert!(serialized.get(absent).is_none(), "unexpected {absent}");
        }
    }
}
