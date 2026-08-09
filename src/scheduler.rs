use std::collections::BTreeMap;

use crate::{
    agent::{AgentHandle, AgentRunner},
    config,
    db::{EventRepository, ProjectRepository},
    guardrails::{DispatchDecision, Guardrails},
    models::{Event, EventKind, EventStatus},
    AppError,
};

const MAX_PROMPT_BYTES: usize = 16 * 1024;
const MAX_EVENT_EVIDENCE_BYTES: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerConfig {
    pub now: i64,
    pub lease_seconds: i64,
    pub claim_limit: usize,
}

pub struct Scheduler {
    db: crate::db::Db,
    runner: AgentRunner,
    config: SchedulerConfig,
}

#[derive(Default)]
pub struct SchedulerReport {
    pub started: Vec<StartedAgent>,
    pub paused: Vec<String>,
    pub halted: Vec<String>,
    pub recovered_leases: usize,
}

pub struct StartedAgent {
    pub run_id: i64,
    pub primary_event_id: i64,
    pub mode: String,
    pub event_ids: Vec<i64>,
    pub prompt: String,
    pub handle: AgentHandle,
}

impl Scheduler {
    pub fn new(db: crate::db::Db, runner: AgentRunner, config: SchedulerConfig) -> Self {
        Self { db, runner, config }
    }

    pub fn recover_expired_leases(&self) -> Result<usize, AppError> {
        EventRepository::new(&self.db).recover_expired_claims(self.config.now)
    }

    pub async fn tick(&mut self) -> Result<SchedulerReport, AppError> {
        let recovered_leases = self.recover_expired_leases()?;
        let claimed = EventRepository::new(&self.db).claim_batch(
            self.config.now,
            self.config.now + self.config.lease_seconds,
            self.config.claim_limit,
        )?;
        let mut report = SchedulerReport {
            recovered_leases,
            ..SchedulerReport::default()
        };
        let mut first_error = None;

        for (project_id, mut events) in group_by_project(claimed) {
            events.sort_by_key(|event| (event_priority(event.kind), event.event_id));
            let event_ids = events
                .iter()
                .map(|event| event.event_id)
                .collect::<Vec<_>>();
            let Some(primary) = events.first().cloned() else {
                continue;
            };
            let Some(project) = ProjectRepository::new(&self.db).find_by_id(&project_id)? else {
                EventRepository::new(&self.db).transition_many(
                    &event_ids,
                    EventStatus::Failed,
                    self.config.now,
                    None,
                    Some("project missing"),
                )?;
                continue;
            };
            let project_config = match config::load(&project.config_path) {
                Ok(project_config) => project_config,
                Err(error) => {
                    let message = error.to_string();
                    EventRepository::new(&self.db).transition_many(
                        &event_ids,
                        EventStatus::Failed,
                        self.config.now,
                        None,
                        Some(&message),
                    )?;
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    continue;
                }
            };
            let guardrails = Guardrails::new(&self.db, self.config.now);
            match guardrails.check(&project, &project_config.guardrails, &events)? {
                DispatchDecision::Allow => {}
                DispatchDecision::Pause(reason) => {
                    guardrails.apply_pause(&project.project_id)?;
                    EventRepository::new(&self.db).transition_many(
                        &event_ids,
                        EventStatus::Failed,
                        self.config.now,
                        None,
                        Some(&reason),
                    )?;
                    report.paused.push(project.project_id);
                    continue;
                }
                DispatchDecision::Halt(reason) => {
                    guardrails.apply_halt(&project.project_id, &reason)?;
                    EventRepository::new(&self.db).transition_many(
                        &event_ids,
                        EventStatus::Failed,
                        self.config.now,
                        None,
                        Some(&reason),
                    )?;
                    report.halted.push(project.project_id);
                    continue;
                }
            }

            let mode = dispatch_mode(primary.kind).to_owned();
            let prompt = build_prompt(&project, &mode, &events)?;
            match self.runner.spawn(
                &self.db,
                &project,
                &project_config.agent,
                primary.event_id,
                &event_ids,
                &prompt,
                self.config.now,
            ) {
                Ok(handle) => {
                    EventRepository::new(&self.db).transition_many(
                        &event_ids,
                        EventStatus::Completed,
                        self.config.now,
                        None,
                        None,
                    )?;
                    report.started.push(StartedAgent {
                        run_id: handle.run_id,
                        primary_event_id: primary.event_id,
                        mode,
                        event_ids,
                        prompt,
                        handle,
                    });
                }
                Err(error) => {
                    let message = format!("agent spawn failed: {error}");
                    let retry_at = self.config.now + retry_backoff_seconds(primary.attempts);
                    let status = if primary.attempts <= i64::from(project_config.agent.max_retries)
                    {
                        EventStatus::RetryWait
                    } else {
                        EventStatus::Failed
                    };
                    EventRepository::new(&self.db).transition_many(
                        &event_ids,
                        status,
                        self.config.now,
                        Some(retry_at),
                        Some(&message),
                    )?;
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    continue;
                }
            }
        }

        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(report)
        }
    }
}

fn group_by_project(events: Vec<Event>) -> BTreeMap<String, Vec<Event>> {
    let mut grouped = BTreeMap::new();
    for event in events {
        grouped
            .entry(event.project_id.clone())
            .or_insert_with(Vec::new)
            .push(event);
    }
    grouped
}

fn event_priority(kind: EventKind) -> u8 {
    match kind {
        EventKind::Crash
        | EventKind::TaskFailed
        | EventKind::AutoKilled
        | EventKind::TerminationFailed => 0,
        EventKind::Stalled => 1,
        EventKind::TaskFinished => 2,
        EventKind::DeepCheck => 3,
    }
}

fn dispatch_mode(kind: EventKind) -> &'static str {
    match kind {
        EventKind::Crash | EventKind::AutoKilled | EventKind::TerminationFailed => "crash",
        EventKind::TaskFailed => "failure",
        EventKind::Stalled => "stalled",
        EventKind::TaskFinished => "completion",
        EventKind::DeepCheck => "deep_check",
    }
}

fn retry_backoff_seconds(attempts: i64) -> i64 {
    let exponent = attempts.clamp(0, 6) as u32;
    60 * 2_i64.pow(exponent)
}

fn build_prompt(
    project: &crate::models::Project,
    mode: &str,
    events: &[Event],
) -> Result<String, AppError> {
    let mut prompt = format!(
        "Dispatch mode: {mode}\nProject ID: {}\nProject root: {}\n\nContext references:\n- .pueue-agent/instructions.md\n- .pueue-agent/STATE.md\n\nBounded event summary:\n",
        project.project_id,
        project
            .root_path
            .to_str()
            .ok_or(AppError::Configuration {
                field: "project.root_path"
            })?
    );

    for event in events {
        let payload = truncate(&event.payload.to_string(), MAX_EVENT_EVIDENCE_BYTES);
        prompt.push_str(&format!(
            "- event_id={} kind={} attempts={} created_at={} evidence={}\n",
            event.event_id, event.kind, event.attempts, event.created_at, payload
        ));
    }
    prompt.push_str(
        "\nInstructions: read .pueue-agent/instructions.md first, then .pueue-agent/STATE.md. Preserve the configured guardrails and update STATE.md before exiting.\n",
    );

    Ok(truncate(&prompt, MAX_PROMPT_BYTES))
}

fn truncate(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...[truncated]", &value[..end])
}
