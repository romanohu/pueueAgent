use std::{collections::BTreeMap, time::SystemTime};

use rusqlite::OptionalExtension;

use crate::{
    config,
    db::{
        inferred_pre_binding_policy_code, AgentRunRepository, CampaignRepository,
        CampaignStatusProjection, Db, DecisionDoctorProjection, EventRepository,
        ProjectRepository, SubmissionRepository,
    },
    models::{DecisionAttemptState, DecisionCycle, Event, Project},
    output::{
        bounded_execution_path, bounded_redacted_text, bounded_typed_text, format_state,
        human_header, human_summary, render_decision_status_line, render_id,
        DecisionStatusProjection,
    },
    pueue::PueueTask,
    service::ServiceStatus,
    AppError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PueueSnapshot {
    Tasks(Vec<PueueTask>),
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusInput {
    pub daemon_health: ServiceStatus,
    pub pueue: PueueSnapshot,
    pub now_override: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisableMode {
    KeepReservation,
    Remove,
}

pub fn render_project_status(
    db: &Db,
    project: &Project,
    input: &StatusInput,
) -> Result<String, AppError> {
    let mut lines = vec![human_header("status", &project.project_id)];
    lines.push(format!(
        "daemon: {}",
        service_status_label(input.daemon_health)
    ));
    lines.push(format!(
        "service: {}",
        service_status_label(input.daemon_health)
    ));
    lines.push(format!("automation: {}", automation_status_label(project)));
    lines.push(format!(
        "project: {}",
        bounded_redacted_text(&project.project_id)
    ));
    lines.push(project_lifecycle_line(project));
    let root_path = project.root_path.to_string_lossy();
    let root_path = bounded_execution_path(&root_path).unwrap_or_else(|| "[invalid]".to_owned());
    lines.push(format!(
        "root: {}",
        bounded_typed_text(&root_path)
    ));
    lines.push(format!(
        "group: {}",
        bounded_redacted_text(&project.pueue_group)
    ));
    lines.push(format!("enabled: {}", project.enabled));
    lines.push(format!("paused: {}", project.paused));
    lines.push(format!(
        "halted: {}",
        bounded_redacted_text(project.halted_reason.as_deref().unwrap_or("no"))
    ));

    let active_task_count = match &input.pueue {
        PueueSnapshot::Tasks(tasks) => {
            let project_tasks = tasks
                .iter()
                .filter(|task| task.group == project.pueue_group)
                .collect::<Vec<_>>();
            let active = project_tasks
                .iter()
                .filter(|task| !task.is_terminal() && !task.state.eq_ignore_ascii_case("queued"))
                .copied()
                .collect::<Vec<_>>();
            let active_count = active.len();
            let queued = project_tasks
                .iter()
                .filter(|task| task.state.eq_ignore_ascii_case("queued"))
                .count();
            lines.push(format!(
                "pueue: total={} active={active_count} queued={queued}",
                project_tasks.len()
            ));
            lines.push(format!("active_tasks: {active_count}"));
            for task in active {
                lines.push(format!(
                    "{} state={} {}",
                    render_id("task", task.id),
                    format_state(&bounded_redacted_text(&task.state)),
                    bounded_redacted_text(&task.command)
                ));
            }
            active_count
        }
        PueueSnapshot::Error(message) => {
            lines.push(format!("pueue: error: {}", bounded_redacted_text(message)));
            0
        }
    };

    let event_counts = event_status_counts(db, &project.project_id)?;
    lines.push(event_counts_line(&event_counts));
    let event_repository = EventRepository::new(db);
    let recent_events = event_repository.recent_events(&project.project_id, 8)?;
    let recent_event_ids = recent_events.iter().map(|event| event.event_id).collect::<Vec<_>>();
    let linked_recent_events = event_repository
        .latest_execution_projections(&project.project_id, &recent_event_ids)?;
    if !recent_events.is_empty() {
        lines.push(format!(
            "recent_events: {}",
            recent_events
                .iter()
                .map(|event| event_summary(event, linked_recent_events.contains_key(&event.event_id)))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let integration_errors = integration_error_count(db)?;
    lines.push(format!("integration_errors: {integration_errors}"));

    let open_incidents = open_incident_count(db, &project.project_id)?;
    lines.push(format!("open_incidents: {open_incidents}"));

    let termination_counts = termination_status_counts(db, &project.project_id)?;
    lines.push(format!(
        "termination_requests: requested={} sent={} confirmed={} timed_out={} failed={}",
        count(&termination_counts, "requested"),
        count(&termination_counts, "sent"),
        count(&termination_counts, "confirmed"),
        count(&termination_counts, "timed_out"),
        count(&termination_counts, "failed")
    ));
    let failed_termination_errors = failed_termination_errors(db, &project.project_id)?;
    if !failed_termination_errors.is_empty() {
        lines.push(format!(
            "termination_errors: {}",
            bounded_redacted_text(&failed_termination_errors.join("; "))
        ));
    }

    let agent_counts = agent_run_status_counts(db, &project.project_id)?;
    let active_agent_runs = count(&agent_counts, "starting") + count(&agent_counts, "running");
    lines.push(format!(
        "agent_runs: active={} failed={}",
        active_agent_runs,
        count(&agent_counts, "failed")
    ));
    let recent_agent_runs = AgentRunRepository::new(db).list_by_project(&project.project_id, 3)?;
    if !recent_agent_runs.is_empty() {
        lines.push(format!(
            "recent_agent_runs: {}",
            recent_agent_runs
                .iter()
                .map(|run| {
                    let execution = [
                        run.execution_kind
                            .as_deref()
                            .map(|value| format!("kind={}", bounded_redacted_text(value))),
                        run.executable_path
                            .as_deref()
                            .and_then(bounded_execution_path)
                            .map(|value| format!("path={value}")),
                        run.policy_code
                            .as_deref()
                            .map(|value| format!("policy={}", bounded_redacted_text(value))),
                        run.failure_stage
                            .as_deref()
                            .map(|value| format!("stage={}", bounded_redacted_text(value))),
                    ]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .join(" ");
                    format!("run={} state={} {execution}", run.run_id, format_state(run.status.as_str()))
                })
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let now = match input.now_override {
        Some(now) => now,
        None => status_timestamp()?,
    };
    if let Some(campaign) = CampaignRepository::new(db)
        .status_projection_for_project(&project.project_id, now)?
    {
        lines.push(campaign_status_line(&campaign));
        if campaign.has_objective {
            lines.push(best_status_line(&campaign));
            lines.push(plateau_status_line(&campaign));
        }
        if let Some(decision) = current_decision_projection(db, &campaign.campaign_id, now)? {
            lines.push(render_decision_status_line(
                &DecisionStatusProjection::from(&decision),
            ));
        }
    }
    lines.extend(running_health_lines(db, &project.project_id, now)?);

    lines.extend(guardrail_lines(db, project)?);
    lines.extend(context_lines(db, project)?);
    lines.push(human_summary(format!(
        "{active_task_count} active task(s), {} pending event(s), {active_agent_runs} active agent run(s)",
        count(&event_counts, "pending")
    )));

    Ok(lines.join("\n"))
}

const MAX_HEALTH_LINES: usize = 8;

fn running_health_lines(db: &Db, project_id: &str, now: i64) -> Result<Vec<String>, AppError> {
    let limit = i64::try_from(MAX_HEALTH_LINES).unwrap_or(1);
    let connection = db.connect()?;
    let mut statement = connection
        .prepare(
            "SELECT rh.experiment_id, rh.state, rh.signal_summary_json,
                    rh.diagnosis_json, rh.last_observed_at
             FROM running_health AS rh
             JOIN campaigns AS c ON c.campaign_id = rh.campaign_id
             WHERE rh.project_id = ?1 AND c.state = 'active'
             ORDER BY rh.last_observed_at DESC, rh.experiment_id DESC
             LIMIT ?2",
        )
        .map_err(|source| AppError::Database {
            operation: "prepare running health status query",
            source,
        })?;
    let rows = statement
        .query_map(rusqlite::params![project_id, limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .map_err(|source| AppError::Database {
            operation: "query running health status rows",
            source,
        })?;
    let collected = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| AppError::Database {
            operation: "read running health status rows",
            source,
        })?;
    Ok(collected
        .into_iter()
        .map(|(experiment_id, state, signal_summary_json, diagnosis_json, last_observed_at)| {
            running_health_line(
                &experiment_id,
                &state,
                &signal_summary_json,
                diagnosis_json.as_deref(),
                last_observed_at,
                now,
            )
        })
        .collect())
}

fn running_health_line(
    experiment_id: &str,
    state: &str,
    signal_summary_json: &str,
    diagnosis_json: Option<&str>,
    last_observed_at: i64,
    now: i64,
) -> String {
    format!(
        "health: id={} state={} signals={} age={} action={}",
        bounded_redacted_text(experiment_id),
        bounded_redacted_text(state),
        top_signal_label(signal_summary_json),
        now.saturating_sub(last_observed_at),
        last_recommended_action(diagnosis_json),
    )
}

fn top_signal_label(signal_summary_json: &str) -> String {
    let Ok(entries) =
        serde_json::from_str::<Vec<crate::models::SignalSummaryEntry>>(signal_summary_json)
    else {
        return "none".to_owned();
    };
    let mut counts: Vec<(String, i64)> = Vec::new();
    for entry in entries {
        if let Some(counted) = counts.iter_mut().find(|(class, _)| class == entry.class.as_str()) {
            counted.1 += 1;
        } else {
            counts.push((entry.class, 1));
        }
    }
    counts.sort_unstable_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    counts
        .first()
        .map(|(class, count)| {
            bounded_redacted_text(&format!("{}x{count}", bounded_redacted_text(class)))
        })
        .unwrap_or_else(|| "none".to_owned())
}

fn last_recommended_action(diagnosis_json: Option<&str>) -> String {
    let diagnosis = diagnosis_json.and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok());
    let action = diagnosis
        .as_ref()
        .and_then(|value| value.get("recommended_action"))
        .and_then(serde_json::Value::as_str);
    action
        .map(bounded_redacted_text)
        .unwrap_or_else(|| "none".to_owned())
}

pub fn render_project_status_compact(
    db: &Db,
    project: &Project,
    input: &StatusInput,
) -> Result<String, AppError> {
    let mut lines = vec![human_header("status", &project.project_id)];
    lines.push(format!(
        "daemon: {}",
        service_status_label(input.daemon_health)
    ));
    lines.push(format!(
        "service: {}",
        service_status_label(input.daemon_health)
    ));
    lines.push(format!("automation: {}", automation_status_label(project)));
    lines.push(project_lifecycle_line(project));

    match &input.pueue {
        PueueSnapshot::Tasks(tasks) => {
            let project_tasks = tasks
                .iter()
                .filter(|task| task.group == project.pueue_group)
                .collect::<Vec<_>>();
            let active = project_tasks
                .iter()
                .filter(|task| !task.is_terminal() && !task.state.eq_ignore_ascii_case("queued"))
                .count();
            let queued = project_tasks
                .iter()
                .filter(|task| task.state.eq_ignore_ascii_case("queued"))
                .count();
            lines.push(format!(
                "pueue: total={} active={} queued={}",
                project_tasks.len(),
                active,
                queued
            ));
        }
        PueueSnapshot::Error(_) => lines.push("pueue: error".to_owned()),
    }

    let experiments =
        SubmissionRepository::new(db).count_started_or_accepted(&project.project_id)?;
    lines.push(format!("experiments: {experiments}"));

    let agent_counts = agent_run_status_counts(db, &project.project_id)?;
    lines.push(format!(
        "agent_runs: active={} failed={}",
        count(&agent_counts, "starting") + count(&agent_counts, "running"),
        count(&agent_counts, "failed")
    ));
    let recent_agent_runs = AgentRunRepository::new(db).list_by_project(&project.project_id, 1)?;
    if let Some(run) = recent_agent_runs.first() {
        let execution = [
            run.execution_kind
                .as_deref()
                .map(|value| format!("kind={}", bounded_redacted_text(value))),
            run.executable_path
                .as_deref()
                .and_then(bounded_execution_path)
                .map(|value| format!("path={value}")),
            run.policy_code
                .as_deref()
                .map(|value| format!("policy={}", bounded_redacted_text(value))),
            run.failure_stage
                .as_deref()
                .map(|value| format!("stage={}", bounded_redacted_text(value))),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
        if !execution.is_empty() {
            lines.push(format!("recent_agent_run: run={} {execution}", run.run_id));
        }
    }

    let now = status_timestamp()?;
    if let Some(campaign) = CampaignRepository::new(db)
        .status_projection_for_project(&project.project_id, now)?
    {
        lines.push(campaign_status_line(&campaign));
        if campaign.has_objective {
            lines.push(best_status_line(&campaign));
            lines.push(plateau_status_line(&campaign));
        }
        if let Some(decision) = current_decision_projection(db, &campaign.campaign_id, now)? {
            lines.push(render_decision_status_line(
                &DecisionStatusProjection::from(&decision),
            ));
        }
    }

    let event_counts = event_status_counts(db, &project.project_id)?;
    lines.push(event_counts_line(&event_counts));

    let guardrails = guardrail_lines(db, project)?;
    lines.extend(guardrails);
    lines.push(human_summary(format!(
        "enabled={} paused={} halted={}",
        project.enabled,
        project.paused,
        project.halted_reason.is_some()
    )));

    Ok(lines.join("\n"))
}

fn campaign_status_line(campaign: &CampaignStatusProjection) -> String {
    format!(
        "campaign: id={} state={} reason={} experiments={} rolling_usage={} next_eligible_at={} unreconciled={} objective_digest={}",
        bounded_redacted_text(&campaign.campaign_id),
        campaign.state,
        bounded_redacted_text(campaign.state_reason.as_deref().unwrap_or("none")),
        render_counts(&campaign.experiment_counts),
        render_counts(&campaign.rolling_usage),
        campaign
            .next_eligible_at
            .map_or_else(|| "none".to_owned(), |value| value.to_string()),
        campaign.unreconciled_count,
        bounded_redacted_text(&campaign.objective_digest),
    )
}

fn best_status_line(campaign: &CampaignStatusProjection) -> String {
    match &campaign.current_best_experiment_id {
        None => "best: none".to_owned(),
        Some(best_id) => {
            let short_raw = if best_id.len() >= 8 {
                &best_id[..8]
            } else {
                best_id.as_str()
            };
            let id = bounded_redacted_text(short_raw);
            match campaign.primary_metric_value {
                Some(value) => {
                    let metric = campaign
                        .primary_metric_name
                        .as_deref()
                        .map(bounded_redacted_text)
                        .unwrap_or_else(|| "none".to_owned());
                    format!("best: id={id} value={value} metric={metric}")
                }
                None => format!("best: id={id}"),
            }
        }
    }
}

fn plateau_status_line(campaign: &CampaignStatusProjection) -> String {
    format!("plateau: count={}", campaign.plateau_count)
}

const DECISION_STATUS_SOURCE_ASC_SQL: &str =
    "SELECT cycle_id, source_terminal_at, source_experiment_id
     FROM decision_cycles INDEXED BY decision_cycles_campaign_state_source_order_idx
     WHERE campaign_id = ?1 AND state = ?2
     ORDER BY source_terminal_at, source_experiment_id, cycle_id
     LIMIT 1";
const DECISION_STATUS_WAIT_SQL: &str =
    "SELECT cycle_id, source_terminal_at, source_experiment_id
     FROM decision_cycles INDEXED BY decision_cycles_campaign_state_wake_source_order_idx
     WHERE campaign_id = ?1 AND state = 'waiting'
     ORDER BY next_wake_at, source_terminal_at, source_experiment_id, cycle_id
     LIMIT 1";
const DECISION_STATUS_SOURCE_DESC_SQL: &str =
    "SELECT cycle_id, source_terminal_at, source_experiment_id
     FROM decision_cycles INDEXED BY decision_cycles_campaign_state_source_order_idx
     WHERE campaign_id = ?1 AND state = ?2
     ORDER BY source_terminal_at DESC, source_experiment_id DESC, cycle_id DESC
     LIMIT 1";

#[derive(Debug)]
struct DecisionStatusCandidate {
    cycle_id: String,
    source_terminal_at: i64,
    source_experiment_id: String,
}

impl DecisionStatusCandidate {
    fn source_order(&self) -> (i64, &str, &str) {
        (
            self.source_terminal_at,
            &self.source_experiment_id,
            &self.cycle_id,
        )
    }
}

fn decision_status_candidate(
    connection: &rusqlite::Connection,
    sql: &str,
    parameters: &[&dyn rusqlite::ToSql],
    operation: &'static str,
) -> Result<Option<DecisionStatusCandidate>, AppError> {
    connection
        .query_row(sql, rusqlite::params_from_iter(parameters.iter()), |row| {
            Ok(DecisionStatusCandidate {
                cycle_id: row.get(0)?,
                source_terminal_at: row.get(1)?,
                source_experiment_id: row.get(2)?,
            })
        })
        .optional()
        .map_err(|source| AppError::Database { operation, source })
}

pub(crate) fn current_decision_projection(
    db: &Db,
    campaign_id: &str,
    _now: i64,
) -> Result<Option<DecisionDoctorProjection>, AppError> {
    let connection = db.connect()?;
    let analyzing = decision_status_candidate(
        &connection,
        DECISION_STATUS_SOURCE_ASC_SQL,
        &[&campaign_id, &"analyzing"],
        "read active decision status candidate",
    )?;
    let pending = if analyzing.is_none() {
        decision_status_candidate(
            &connection,
            DECISION_STATUS_SOURCE_ASC_SQL,
            &[&campaign_id, &"pending"],
            "read pending decision status candidate",
        )?
    } else {
        None
    };
    let waiting = if analyzing.is_none() && pending.is_none() {
        decision_status_candidate(
            &connection,
            DECISION_STATUS_WAIT_SQL,
            &[&campaign_id],
            "read waiting decision status candidate",
        )?
    } else {
        None
    };
    let terminal = if analyzing.is_none() && pending.is_none() && waiting.is_none()
    {
        let mut candidates = Vec::with_capacity(2);
        for state in ["completed", "degraded"] {
            if let Some(candidate) = decision_status_candidate(
                &connection,
                DECISION_STATUS_SOURCE_DESC_SQL,
                &[&campaign_id, &state],
                "read terminal decision status candidate",
            )?
            {
                candidates.push(candidate);
            }
        }
        candidates
            .into_iter()
            .max_by(|left, right| left.source_order().cmp(&right.source_order()))
    } else {
        None
    };
    let Some(cycle_id) = analyzing
        .or(pending)
        .or(waiting)
        .or(terminal)
        .map(|candidate| candidate.cycle_id)
    else {
        return Ok(None);
    };

    let cycle = connection
        .query_row(
            "SELECT dc.cycle_id, dc.campaign_id, dc.source_experiment_id,
                    dc.source_terminal_at, dc.state,
                    dc.next_wake_at, dc.consecutive_failed_attempts,
                    dc.last_decision_kind, dc.last_failure_code, dc.last_failure_summary,
                    dc.created_at, dc.updated_at
             FROM decision_cycles dc
             WHERE dc.cycle_id = ?1",
            [&cycle_id],
            |row| {
                Ok(DecisionCycle {
                    cycle_id: row.get(0)?,
                    campaign_id: row.get(1)?,
                    source_experiment_id: row.get(2)?,
                    source_terminal_at: row.get(3)?,
                    state: row.get(4)?,
                    next_wake_at: row.get(5)?,
                    consecutive_failed_attempts: row.get(6)?,
                    last_decision_kind: row.get(7)?,
                    last_failure_code: row.get(8)?,
                    last_failure_summary: row.get(9)?,
                    created_at: row.get(10)?,
                    updated_at: row.get(11)?,
                })
            },
        )
        .map_err(|source| AppError::Database {
            operation: "read bounded current decision status projection",
            source,
        })?;
    let attempt_count = connection
        .query_row(
            "SELECT COUNT(*) FROM (
                 SELECT 1
                 FROM decision_attempts
                 WHERE cycle_id = ?1
                 ORDER BY attempt_number
                 LIMIT 11
             )",
            [&cycle_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|source| AppError::Database {
            operation: "read bounded decision status attempt count",
            source,
        })?;
    let active_attempt = if cycle.state == crate::models::DecisionCycleState::Analyzing {
        connection
            .query_row(
                "SELECT attempt_number, state, agent_run_id
                 FROM decision_attempts
                 WHERE cycle_id = ?1
                   AND state IN ('reserved','evidence_ready','running','decided')
                 ORDER BY attempt_number DESC
                 LIMIT 1",
                [&cycle_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, DecisionAttemptState>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|source| AppError::Database {
                operation: "read bounded active decision status attempt",
                source,
            })?
    } else {
        None
    };
    Ok(Some(DecisionDoctorProjection {
        cycle,
        attempt_count,
        active_attempt_number: active_attempt.as_ref().map(|attempt| attempt.0),
        active_attempt_state: active_attempt.as_ref().map(|attempt| attempt.1),
        active_agent_run_id: active_attempt.and_then(|attempt| attempt.2),
    }))
}

fn render_counts(counts: &BTreeMap<String, i64>) -> String {
    if counts.is_empty() {
        return "none".to_owned();
    }
    counts
        .iter()
        .map(|(key, value)| format!("{}={value}", bounded_redacted_text(key)))
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn status_timestamp() -> Result<i64, AppError> {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| AppError::Runtime {
            operation: "read the system clock for status",
        })?
        .as_secs()
        .try_into()
        .map_err(|_| AppError::Runtime {
            operation: "represent the status timestamp",
        })
}

pub fn pause_project(db: &Db, project_id: &str, now: i64) -> Result<Project, AppError> {
    ProjectRepository::new(db).pause(project_id, now)
}

pub fn resume_project(db: &Db, project_id: &str, now: i64) -> Result<Project, AppError> {
    ProjectRepository::new(db).resume(project_id, now)
}

pub fn disable_project(
    db: &Db,
    project_id: &str,
    mode: DisableMode,
    pueue_tasks: &[PueueTask],
    now: i64,
) -> Result<Project, AppError> {
    let project = ProjectRepository::new(db)
        .find_by_id(project_id)?
        .ok_or(AppError::Runtime {
            operation: "find project before disable",
        })?;
    let unresolved_task_ids = unresolved_task_ids_for_group(&project, pueue_tasks);
    match mode {
        DisableMode::KeepReservation => {
            ProjectRepository::new(db).disable(project_id, now, &unresolved_task_ids)
        }
        DisableMode::Remove => {
            ProjectRepository::new(db).remove(project_id, now, &unresolved_task_ids)
        }
    }
}

fn unresolved_task_ids_for_group(project: &Project, pueue_tasks: &[PueueTask]) -> Vec<i64> {
    pueue_tasks
        .iter()
        .filter(|task| task.group == project.pueue_group && !task.is_terminal())
        .map(|task| task.id)
        .collect()
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

fn project_lifecycle_line(project: &Project) -> String {
    format!(
        "project: enabled={} paused={} halted={}",
        project.enabled,
        project.paused,
        project.halted_reason.is_some()
    )
}

#[cfg(test)]
mod decision_query_plan_tests {
    use tempfile::TempDir;

    use super::{
        DECISION_STATUS_SOURCE_ASC_SQL, DECISION_STATUS_SOURCE_DESC_SQL,
        DECISION_STATUS_WAIT_SQL,
    };
    use crate::db::Db;

    fn explain(db: &Db, sql: &str, parameters: &[&dyn rusqlite::ToSql]) -> Vec<String> {
        let connection = db.connect().unwrap();
        let mut statement = connection.prepare(sql).unwrap();
        statement
            .query_map(parameters, |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[test]
    fn decision_status_and_doctor_probes_use_bounded_index_plans() {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        for (name, sql, parameters, expected_index) in [
            (
                "analyzing",
                DECISION_STATUS_SOURCE_ASC_SQL,
                vec![
                    &"campaign-a" as &dyn rusqlite::ToSql,
                    &"analyzing" as &dyn rusqlite::ToSql,
                ],
                "decision_cycles_campaign_state_source_order_idx",
            ),
            (
                "pending",
                DECISION_STATUS_SOURCE_ASC_SQL,
                vec![
                    &"campaign-a" as &dyn rusqlite::ToSql,
                    &"pending" as &dyn rusqlite::ToSql,
                ],
                "decision_cycles_campaign_state_source_order_idx",
            ),
            (
                "waiting",
                DECISION_STATUS_WAIT_SQL,
                vec![&"campaign-a" as &dyn rusqlite::ToSql],
                "decision_cycles_campaign_state_wake_source_order_idx",
            ),
            (
                "completed",
                DECISION_STATUS_SOURCE_DESC_SQL,
                vec![
                    &"campaign-a" as &dyn rusqlite::ToSql,
                    &"completed" as &dyn rusqlite::ToSql,
                ],
                "decision_cycles_campaign_state_source_order_idx",
            ),
            (
                "degraded",
                DECISION_STATUS_SOURCE_DESC_SQL,
                vec![
                    &"campaign-a" as &dyn rusqlite::ToSql,
                    &"degraded" as &dyn rusqlite::ToSql,
                ],
                "decision_cycles_campaign_state_source_order_idx",
            ),
        ] {
            assert!(!sql.contains("state <>"), "{name}: {sql}");
            assert!(!sql.contains("experiments"), "{name}: {sql}");
            let plan = explain(&db, &format!("EXPLAIN QUERY PLAN {sql}"), &parameters);
            assert!(
                plan.iter().any(|detail| detail.contains(expected_index)),
                "{name}: {plan:?}"
            );
            assert!(
                plan.iter().all(|detail| {
                    !detail.contains("TEMP B-TREE") && !detail.starts_with("SCAN decision_cycles")
                }),
                "{name}: {plan:?}"
            );
        }

        let attempt_plan = explain(
            &db,
            "EXPLAIN QUERY PLAN
             SELECT attempt_number
             FROM decision_attempts
             WHERE cycle_id = ?1
             ORDER BY attempt_number
             LIMIT ?2",
            &[&"cycle-a", &4_i64],
        );
        assert!(
            attempt_plan.iter().any(|detail| {
                detail.contains("sqlite_autoindex_decision_attempts_1")
                    || detail.contains("PRIMARY KEY")
            }),
            "{attempt_plan:?}"
        );
    }
}

fn event_status_counts(db: &Db, project_id: &str) -> Result<BTreeMap<String, i64>, AppError> {
    grouped_counts(
        db,
        "SELECT status, COUNT(*) FROM events WHERE project_id = ?1 GROUP BY status",
        project_id,
        "query event status counts",
    )
}

fn termination_status_counts(db: &Db, project_id: &str) -> Result<BTreeMap<String, i64>, AppError> {
    grouped_counts(
        db,
        "SELECT status, COUNT(*) FROM termination_requests WHERE project_id = ?1 GROUP BY status",
        project_id,
        "query termination request counts",
    )
}

fn agent_run_status_counts(db: &Db, project_id: &str) -> Result<BTreeMap<String, i64>, AppError> {
    grouped_counts(
        db,
        "SELECT status, COUNT(*) FROM agent_runs WHERE project_id = ?1 GROUP BY status",
        project_id,
        "query agent run counts",
    )
}

fn grouped_counts(
    db: &Db,
    sql: &str,
    project_id: &str,
    operation: &'static str,
) -> Result<BTreeMap<String, i64>, AppError> {
    let connection = db.connect()?;
    let mut statement = connection
        .prepare(sql)
        .map_err(|source| AppError::Database { operation, source })?;
    let rows = statement
        .query_map([project_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(|source| AppError::Database { operation, source })?;
    rows.collect::<Result<BTreeMap<_, _>, _>>()
        .map_err(|source| AppError::Database { operation, source })
}

fn count(counts: &BTreeMap<String, i64>, key: &str) -> i64 {
    counts.get(key).copied().unwrap_or(0)
}

fn event_counts_line(counts: &BTreeMap<String, i64>) -> String {
    let retry_wait = count(counts, "retry_wait");
    let in_flight = count(counts, "in_flight");
    let dispatched = count(counts, "dispatched");
    let dead_letter = count(counts, "dead_letter");
    format!(
        "events: pending={} claimed={} retry_wait={} in_flight={} dispatched={} failed={} dead_letter={}",
        count(counts, "pending"),
        count(counts, "claimed"),
        retry_wait,
        in_flight,
        dispatched,
        count(counts, "failed"),
        dead_letter
    )
}

fn event_summary(event: &Event, has_run_link: bool) -> String {
    let policy = inferred_pre_binding_policy_code(event, has_run_link);
    let policy_code = policy.as_deref().unwrap_or("none");
    let failure_stage = if policy.is_some() { "pre_binding" } else { "none" };
    format!(
        "{} kind={} state={} policy_code={} failure_stage={}",
        render_id("event", event.event_id),
        event.kind,
        format_state(event.status.as_str()),
        policy_code,
        failure_stage,
    )
}

fn integration_error_count(db: &Db) -> Result<i64, AppError> {
    let connection = db.connect()?;
    connection
        .query_row("SELECT COUNT(*) FROM integration_events", [], |row| {
            row.get(0)
        })
        .map_err(|source| AppError::Database {
            operation: "count integration events",
            source,
        })
}

fn open_incident_count(db: &Db, project_id: &str) -> Result<i64, AppError> {
    let connection = db.connect()?;
    connection
        .query_row(
            "SELECT COUNT(*) FROM incidents
             WHERE project_id = ?1 AND status IN ('open', 'acknowledged')",
            [project_id],
            |row| row.get(0),
        )
        .map_err(|source| AppError::Database {
            operation: "count open incidents",
            source,
        })
}

fn failed_termination_errors(db: &Db, project_id: &str) -> Result<Vec<String>, AppError> {
    let connection = db.connect()?;
    let mut statement = connection
        .prepare(
            "SELECT request_id, last_error FROM termination_requests
             WHERE project_id = ?1 AND status IN ('failed', 'timed_out')
             ORDER BY requested_at DESC, request_id DESC
             LIMIT 3",
        )
        .map_err(|source| AppError::Database {
            operation: "prepare failed termination request query",
            source,
        })?;
    let rows = statement
        .query_map([project_id], |row| {
            let request_id: i64 = row.get(0)?;
            let last_error: Option<String> = row.get(1)?;
            Ok(format!(
                "{}:{}",
                render_id("request", request_id),
                bounded_redacted_text(&last_error.unwrap_or_else(|| "no detail".to_owned()))
            ))
        })
        .map_err(|source| AppError::Database {
            operation: "query failed termination requests",
            source,
        })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| AppError::Database {
            operation: "read failed termination requests",
            source,
        })
}

fn guardrail_lines(db: &Db, project: &Project) -> Result<Vec<String>, AppError> {
    let config = match config::load(&project.config_path) {
        Ok(config) => config,
        Err(error) => return Ok(vec![guardrail_error_line(&error)]),
    };
    let consecutive_failures = EventRepository::new(db).count_consecutive_failures(
        &project.project_id,
        config.guardrails.max_consecutive_failures as usize + 1,
    )?;
    let experiments =
        SubmissionRepository::new(db).count_started_or_accepted(&project.project_id)?;
    let agent_runs = AgentRunRepository::new(db).count_by_project(&project.project_id)?;
    Ok(vec![format!(
        "guardrails: consecutive_failures={}/{} experiments={}/{} agent_runs={}/{}",
        consecutive_failures,
        config.guardrails.max_consecutive_failures,
        experiments,
        config.guardrails.max_experiments,
        agent_runs,
        config.guardrails.max_agent_runs
    )])
}

fn guardrail_error_line(error: &AppError) -> String {
    format!(
        "guardrails: error: {}",
        bounded_redacted_text(&error.render())
    )
}

fn context_lines(db: &Db, project: &Project) -> Result<Vec<String>, AppError> {
    let mut lines = Vec::new();
    if let Ok(config) = config::load(&project.config_path) {
        if config.agent.program == "codex" {
            lines.push(format!(
                "codex_context: mode={}{}",
                config.agent.context.as_str(),
                config
                    .agent
                    .context
                    .session_id()
                    .map(|session| format!(" session={}", bounded_redacted_text(session)))
                    .unwrap_or_default()
            ));
        }
    }

    if let Some(lineage) = latest_context_lineage(db, &project.project_id)? {
        lines.push(format!(
            "last_lineage: {}",
            bounded_redacted_text(&lineage.join(" -> "))
        ));
    }

    Ok(lines)
}

fn latest_context_lineage(db: &Db, project_id: &str) -> Result<Option<Vec<String>>, AppError> {
    let connection = db.connect()?;
    let lineage_json = connection
        .query_row(
            "SELECT context_lineage_json FROM agent_runs
             WHERE project_id = ?1 AND context_lineage_json <> '[]'
             ORDER BY started_at DESC, run_id DESC
             LIMIT 1",
            [project_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|source| AppError::Database {
            operation: "read latest agent context lineage",
            source,
        })?;
    lineage_json
        .map(|json| {
            serde_json::from_str::<Vec<String>>(&json).map_err(|source| AppError::Serialization {
                operation: "parse latest agent context lineage",
                source,
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guardrail_error_line_bounds_and_redacts_error_text() {
        let error = AppError::Message {
            message: format!(
                "config failure --password GUARDRAIL_SECRET {}",
                "x".repeat(400)
            ),
        };

        let rendered = guardrail_error_line(&error);

        assert!(rendered.len() <= "guardrails: error: ".len() + 243);
        assert!(!rendered.contains("GUARDRAIL_SECRET"));
        assert!(rendered.contains("[REDACTED]"));
    }

    #[test]
    fn running_health_line_ranks_top_signal_and_ages_by_observation_time() {
        let summary = r#"[
            {"class":"oom","source":"builtin_probe","evidence_digest":"d1","observed_at":90},
            {"class":"numerical","source":"builtin_probe","evidence_digest":"d2","observed_at":95},
            {"class":"oom","source":"config_pattern","evidence_digest":"d3","observed_at":99}
        ]"#;
        let diagnosis = r#"{"root_cause_class":"oom","confidence":0.9,"recommended_action":"kill_and_resume","summary":"gpu exhausted"}"#;

        let line =
            running_health_line("experiment-cli", "suspicious", summary, Some(diagnosis), 100, 205);

        assert_eq!(
            line,
            "health: id=experiment-cli state=suspicious signals=oomx2 age=105 action=kill_and_resume"
        );
    }

    #[test]
    fn running_health_line_defaults_to_none_labels_without_signals_or_diagnosis() {
        let line = running_health_line("experiment-cli", "healthy", "[]", None, 100, 130);

        assert_eq!(
            line,
            "health: id=experiment-cli state=healthy signals=none age=30 action=none"
        );
    }

    #[test]
    fn running_health_line_bounds_and_redacts_untrusted_labels() {
        let experiment_id = format!("AWS_SECRET_ACCESS_KEY=EXPERIMENT_SECRET {}", "e".repeat(400));
        let class = format!("{}", "c".repeat(400));
        let summary = format!(
            r#"[{{"class":"{class}","source":"builtin_probe","evidence_digest":"d","observed_at":1}}]"#
        );
        let action = format!("kill_and_resume --password ACTION_SECRET {}", "a".repeat(400));
        let diagnosis = format!(r#"{{"recommended_action":"{action}"}}"#);

        let line = running_health_line(
            &experiment_id,
            "suspicious",
            &summary,
            Some(&diagnosis),
            100,
            101,
        );

        assert!(line.starts_with("health: "));
        assert!(!line.contains("EXPERIMENT_SECRET"));
        assert!(!line.contains("ACTION_SECRET"));
        assert!(line.contains("age=1"));
        for segment in line.split(' ') {
            let value = match segment.split_once('=') {
                Some((_, value)) => value,
                None => continue,
            };
            assert!(value.len() <= 243, "unbounded segment value: {segment}");
        }
        let (_, age_and_action) = line.split_once("age=").unwrap();
        assert!(age_and_action.starts_with("1 action="));
        assert!(age_and_action.ends_with("..."));
    }
}

