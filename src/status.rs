use std::collections::BTreeMap;

use rusqlite::OptionalExtension;

use crate::{
    config,
    db::{AgentRunRepository, Db, EventRepository, ProjectRepository, SubmissionRepository},
    models::{Event, Project},
    output::{bounded_redacted_text, format_state, human_header, human_summary, render_id},
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
    lines.push(format!("root: {}", bounded_redacted_text(&root_path)));
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
    let recent_events = EventRepository::new(db).recent_events(&project.project_id, 8)?;
    if !recent_events.is_empty() {
        lines.push(format!(
            "recent_events: {}",
            recent_events
                .iter()
                .map(event_summary)
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

    lines.extend(guardrail_lines(db, project)?);
    lines.extend(context_lines(db, project)?);
    lines.push(human_summary(format!(
        "{active_task_count} active task(s), {} pending event(s), {active_agent_runs} active agent run(s)",
        count(&event_counts, "pending")
    )));

    Ok(lines.join("\n"))
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

fn event_summary(event: &Event) -> String {
    format!(
        "{} kind={} state={}",
        render_id("event", event.event_id),
        event.kind,
        format_state(event.status.as_str())
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
}
