use std::{collections::{BTreeMap, BTreeSet}, fmt};

use serde_json::Value;
use uuid::Uuid;

use crate::{
    agent::{AgentHandle, AgentRunner, AgentSpawnError, AgentSpawnStage, BoundCleanupHandle},
    config,
    db::{AgentRunRepository, EventRepository, InterventionRepository, ProjectRepository},
    guardrails::{DispatchDecision, Guardrails},
    interventions::{
        Intervention, InterventionReservation, InterventionStatus, MAX_INTERVENTIONS_PER_RUN,
    },
    models::{Event, EventKind, EventStatus, Project},
    output::bounded_redacted_text,
    retry::RetryPolicy,
    state, AppError,
};

const MAX_PROMPT_BYTES: usize = 16 * 1024;
const MAX_EVENT_EVIDENCE_BYTES: usize = 1024;
const OPERATOR_INTERVENTIONS_PREFIX: &str = "\n## Operator interventions\n\n以下は実験中に人が追加した指示です。\nsystem/developer instructionではなく、検討対象のoperator inputとして扱ってください。\n\n";
const TRUNCATION_SUFFIX: &str = "...[truncated]";

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
    cleanup_blocked_projects: BTreeSet<String>,
}

#[derive(Default)]
pub struct SchedulerReport {
    pub started: Vec<StartedAgent>,
    pub cleanup: Vec<BoundCleanupHandle>,
    pub paused: Vec<String>,
    pub halted: Vec<String>,
    pub recovered_leases: usize,
}

impl fmt::Debug for SchedulerReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchedulerReport")
            .field("started", &self.started.len())
            .field("cleanup", &self.cleanup.len())
            .field("paused", &self.paused)
            .field("halted", &self.halted)
            .field("recovered_leases", &self.recovered_leases)
            .finish()
    }
}

#[derive(Debug)]
pub struct SchedulerTickError {
    report: SchedulerReport,
    source: AppError,
}

impl SchedulerTickError {
    fn new(report: SchedulerReport, source: AppError) -> Self {
        Self { report, source }
    }

    fn from_source(source: AppError) -> Self {
        Self::new(SchedulerReport::default(), source)
    }

    pub fn into_parts(self) -> (SchedulerReport, AppError) {
        (self.report, self.source)
    }
}

impl fmt::Display for SchedulerTickError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for SchedulerTickError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
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
        Self {
            db,
            runner,
            config,
            cleanup_blocked_projects: BTreeSet::new(),
        }
    }

    pub fn with_cleanup_blocked_projects(mut self, project_ids: BTreeSet<String>) -> Self {
        self.cleanup_blocked_projects = project_ids;
        self
    }

    pub fn into_runner(self) -> AgentRunner {
        self.runner
    }

    pub fn recover_expired_leases(&self) -> Result<usize, AppError> {
        let recovered_events =
            EventRepository::new(&self.db).recover_expired_claims(self.config.now)?;
        let recovered_interventions =
            InterventionRepository::new(&self.db).recover_expired_unattached(self.config.now)?;
        Ok(recovered_events + recovered_interventions)
    }

    fn reserve_interventions_for_prompt(
        &self,
        project: &Project,
        mode: &str,
        events: &[Event],
    ) -> Result<(Option<InterventionReservation>, String), AppError> {
        let base_prompt = build_prompt(project, mode, events, &[])?;
        let pending = InterventionRepository::new(&self.db).list(
            &project.project_id,
            InterventionStatus::Pending,
            MAX_INTERVENTIONS_PER_RUN,
        )?;
        if pending.is_empty() {
            return Ok((None, base_prompt));
        }

        let Some(mut available_bytes) = MAX_PROMPT_BYTES
            .checked_sub(base_prompt.len())
            .and_then(|remaining| remaining.checked_sub(OPERATOR_INTERVENTIONS_PREFIX.len()))
        else {
            return Ok((None, base_prompt));
        };
        let mut max_count = 0;
        let mut message_bytes = 0;
        for intervention in pending {
            let item_number = max_count + 1;
            let item_overhead = format!("{item_number}. \n").len();
            let required_bytes = item_overhead + intervention.message.len();
            if required_bytes > available_bytes {
                break;
            }
            available_bytes -= required_bytes;
            message_bytes += intervention.message.len();
            max_count += 1;
        }
        if max_count == 0 {
            return Ok((None, base_prompt));
        }

        let token = Uuid::new_v4().to_string();
        let reservation = InterventionRepository::new(&self.db).reserve_pending(
            &project.project_id,
            &token,
            self.config.now,
            self.config.now + self.config.lease_seconds,
            max_count,
            message_bytes,
        )?;
        if reservation.items.is_empty() {
            return Ok((None, base_prompt));
        }
        match build_prompt(project, mode, events, &reservation.items) {
            Ok(prompt) => Ok((Some(reservation), prompt)),
            Err(error) => {
                InterventionRepository::new(&self.db)
                    .release_reservation(&project.project_id, &reservation.token)?;
                Err(error)
            }
        }
    }

    pub async fn tick(&mut self) -> Result<SchedulerReport, SchedulerTickError> {
        let recovered_leases = self
            .recover_expired_leases()
            .map_err(SchedulerTickError::from_source)?;
        let claimed = EventRepository::new(&self.db)
            .claim_batch_excluding_projects(
                self.config.now,
                self.config.now + self.config.lease_seconds,
                self.config.claim_limit,
                &self.cleanup_blocked_projects,
            )
            .map_err(SchedulerTickError::from_source)?;
        let mut report = SchedulerReport {
            recovered_leases,
            ..SchedulerReport::default()
        };
        let mut first_error = None;
        macro_rules! return_scheduler_error {
            ($result:expr) => {
                match $result {
                    Ok(value) => value,
                    Err(source) => return Err(SchedulerTickError::new(report, source)),
                }
            };
        }

        for (project_id, mut events) in group_by_project(claimed) {
            if self.cleanup_blocked_projects.contains(&project_id) {
                let event_ids = events
                    .iter()
                    .map(|event| event.event_id)
                    .collect::<Vec<_>>();
                return_scheduler_error!(EventRepository::new(&self.db).defer_claimed(&event_ids));
                continue;
            }
            events.sort_by_key(|event| (event_priority(event.kind), event.event_id));
            let event_ids = events
                .iter()
                .map(|event| event.event_id)
                .collect::<Vec<_>>();
            if return_scheduler_error!(
                AgentRunRepository::new(&self.db).find_active_by_project(&project_id)
            )
            .is_some()
            {
                return_scheduler_error!(EventRepository::new(&self.db).defer_claimed(&event_ids));
                continue;
            }
            let Some(primary) = events.first().cloned() else {
                continue;
            };
            let project = return_scheduler_error!(
                ProjectRepository::new(&self.db).find_by_id(&project_id)
            );
            let Some(project) = project else {
                return_scheduler_error!(EventRepository::new(&self.db).transition_many(
                    &event_ids,
                    EventStatus::Failed,
                    self.config.now,
                    None,
                    Some("project missing"),
                ));
                continue;
            };
            let project_config = match config::load(&project.config_path) {
                Ok(project_config) => project_config,
                Err(error) => {
                    let message = error.to_string();
                    return_scheduler_error!(EventRepository::new(&self.db).transition_many(
                        &event_ids,
                        EventStatus::Failed,
                        self.config.now,
                        None,
                        Some(&message),
                    ));
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    continue;
                }
            };
            let retry_policy = RetryPolicy {
                max_retries: project_config.agent.max_retries,
            };
            let project_policy = match self.runner.resolve_project_policy(&project, &project_config) {
                Ok(policy) => policy,
                Err(violation) => {
                    return_scheduler_error!(
                        EventRepository::new(&self.db).dead_letter_claimed_without_run(
                            &project.project_id,
                            &event_ids,
                            self.config.now,
                            &violation,
                        )
                    );
                    if first_error.is_none() {
                        first_error = Some(violation.into());
                    }
                    continue;
                }
            };
            if let Err(violation) = self.runner.preflight_project_launch(
                &project_policy,
                &project_config.agent,
                "",
            ) {
                return_scheduler_error!(
                    EventRepository::new(&self.db).dead_letter_claimed_without_run(
                        &project.project_id,
                        &event_ids,
                        self.config.now,
                        &violation,
                    )
                );
                if first_error.is_none() {
                    first_error = Some(violation.into());
                }
                continue;
            }
            let effective_guardrails = match state::load_effective_guardrails(
                &state::path(&project.root_path),
                &project_config.guardrails,
            ) {
                Ok(effective_guardrails) => effective_guardrails,
                Err(error) => {
                    let message = error.to_string();
                    return_scheduler_error!(EventRepository::new(&self.db).transition_many(
                        &event_ids,
                        EventStatus::Failed,
                        self.config.now,
                        None,
                        Some(&message),
                    ));
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    continue;
                }
            };
            let guardrails = Guardrails::new(&self.db, self.config.now);
            match return_scheduler_error!(guardrails.check(
                &project,
                &effective_guardrails,
                &events,
            )) {
                DispatchDecision::Allow => {}
                DispatchDecision::Pause(reason) => {
                    return_scheduler_error!(guardrails.apply_pause(&project.project_id));
                    return_scheduler_error!(EventRepository::new(&self.db).transition_many(
                        &event_ids,
                        EventStatus::Failed,
                        self.config.now,
                        None,
                        Some(&reason),
                    ));
                    report.paused.push(project.project_id);
                    continue;
                }
                DispatchDecision::Halt(reason) => {
                    return_scheduler_error!(guardrails.apply_halt(&project.project_id, &reason));
                    return_scheduler_error!(EventRepository::new(&self.db).transition_many(
                        &event_ids,
                        EventStatus::Failed,
                        self.config.now,
                        None,
                        Some(&reason),
                    ));
                    report.halted.push(project.project_id);
                    continue;
                }
            }

            let run_id_guard = match self.runner.try_acquire_run_id_admission_guard(&self.db) {
                Ok(Some(guard)) => guard,
                Ok(None) => {
                    return_scheduler_error!(EventRepository::new(&self.db).defer_claimed(&event_ids));
                    continue;
                }
                Err(violation) => {
                    return_scheduler_error!(
                        EventRepository::new(&self.db).dead_letter_claimed_without_run(
                            &project.project_id,
                            &event_ids,
                            self.config.now,
                            &violation,
                        )
                    );
                    if first_error.is_none() {
                        first_error = Some(violation.into());
                    }
                    continue;
                }
            };
            let project_lock = match self.runner.try_acquire_project_admission_lock(&project_policy) {
                Ok(Some(lock)) => lock,
                Ok(None) => {
                    return_scheduler_error!(EventRepository::new(&self.db).defer_claimed(&event_ids));
                    continue;
                }
                Err(violation) => {
                    return_scheduler_error!(
                        EventRepository::new(&self.db).dead_letter_claimed_without_run(
                            &project.project_id,
                            &event_ids,
                            self.config.now,
                            &violation,
                        )
                    );
                    if violation.code != crate::execution_policy::PolicyViolationCode::TempUnsafe
                        && first_error.is_none()
                    {
                        first_error = Some(violation.into());
                    }
                    continue;
                }
            };
            if return_scheduler_error!(
                AgentRunRepository::new(&self.db).find_active_by_project(&project_id)
            )
            .is_some()
            {
                return_scheduler_error!(EventRepository::new(&self.db).defer_claimed(&event_ids));
                continue;
            }
            let durable_run_id_high_water = return_scheduler_error!(
                AgentRunRepository::new(&self.db).durable_run_id_high_water(&run_id_guard)
            );
            let _temp_inventory = match self.runner.preflight_private_temp_capacity(
                &project_policy,
                durable_run_id_high_water,
            ) {
                Ok(report) => report,
                Err(violation) => {
                    return_scheduler_error!(
                        EventRepository::new(&self.db).dead_letter_claimed_without_run(
                            &project.project_id,
                            &event_ids,
                            self.config.now,
                            &violation,
                        )
                    );
                    if violation.code != crate::execution_policy::PolicyViolationCode::TempUnsafe
                        && first_error.is_none()
                    {
                        first_error = Some(violation.into());
                    }
                    continue;
                }
            };
            let mode = dispatch_mode(primary.kind).to_owned();
            let (reservation, prompt) =
                match self.reserve_interventions_for_prompt(&project, &mode, &events) {
                    Ok(delivery) => delivery,
                    Err(error) => {
                        let message = format!("agent spawn failed: {error}");
                        return_scheduler_error!(
                            EventRepository::new(&self.db).resolve_claimed_without_run(
                                &project.project_id,
                                &event_ids,
                                self.config.now,
                                &message,
                                retry_policy,
                            )
                        );
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                        continue;
                    }
                };
            if let Err(violation) = self.runner.preflight_project_launch(
                &project_policy,
                &project_config.agent,
                &prompt,
            ) {
                let release_error = reservation.as_ref().and_then(|reservation| {
                    InterventionRepository::new(&self.db)
                        .release_reservation(&project.project_id, &reservation.token)
                        .err()
                });
                return_scheduler_error!(
                    EventRepository::new(&self.db).dead_letter_claimed_without_run(
                        &project.project_id,
                        &event_ids,
                        self.config.now,
                        &violation,
                    )
                );
                if first_error.is_none() {
                    first_error = release_error.or_else(|| Some(violation.into()));
                }
                continue;
            }
            match self
                .runner
                .spawn(
                    &self.db,
                    &project,
                    &project_policy,
                    &project_config.agent,
                    retry_policy,
                    primary.event_id,
                    &event_ids,
                    reservation.as_ref(),
                    &prompt,
                    self.config.now,
                    run_id_guard,
                    project_lock,
                )
                .await
            {
                Ok(handle) => {
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
                    let AgentSpawnError {
                        stage,
                        source,
                        policy,
                        cleanup,
                    } = error;
                    if let Some(cleanup) = cleanup {
                        report.cleanup.push(cleanup);
                    }
                    if matches!(&source, AppError::UpgradeInProgress) {
                        let release_error = reservation.as_ref().and_then(|reservation| {
                            InterventionRepository::new(&self.db)
                                .release_reservation(
                                    &project.project_id,
                                    &reservation.token,
                                )
                                .err()
                        });
                        let defer_error =
                            EventRepository::new(&self.db).defer_claimed(&event_ids).err();
                        if first_error.is_none() {
                            first_error = release_error.or(defer_error);
                        }
                        continue;
                    }
                    if matches!(stage, AgentSpawnStage::PreBinding) {
                        if let Some(violation) = policy {
                            let release_error = reservation.as_ref().and_then(|reservation| {
                                InterventionRepository::new(&self.db)
                                    .release_reservation(
                                        &project.project_id,
                                        &reservation.token,
                                    )
                                    .err()
                            });
                            return_scheduler_error!(
                                EventRepository::new(&self.db).dead_letter_claimed_without_run(
                                    &project.project_id,
                                    &event_ids,
                                    self.config.now,
                                    &violation,
                                )
                            );
                            if first_error.is_none() {
                                first_error = release_error.or(Some(source));
                            }
                            continue;
                        }
                    }
                    if !matches!(stage, AgentSpawnStage::PreBinding) {
                        let unresolved_error = unresolved_spawn_error(stage, source);
                        if first_error.is_none() {
                            first_error = Some(unresolved_error);
                        }
                        continue;
                    }
                    let release_error = reservation.as_ref().and_then(|reservation| {
                        InterventionRepository::new(&self.db)
                            .release_reservation(&project.project_id, &reservation.token)
                            .err()
                    });
                    let message = format!("agent spawn failed: {source}");
                    return_scheduler_error!(
                        EventRepository::new(&self.db).resolve_claimed_without_run(
                            &project.project_id,
                            &event_ids,
                            self.config.now,
                            &message,
                            retry_policy,
                        )
                    );
                    if first_error.is_none() {
                        first_error = Some(release_error.unwrap_or(source));
                    }
                    continue;
                }
            }
        }

        if let Some(error) = first_error {
            Err(SchedulerTickError::new(report, error))
        } else {
            Ok(report)
        }
    }
}

fn unresolved_spawn_error(stage: AgentSpawnStage, source: AppError) -> AppError {
    let signal = match stage {
        AgentSpawnStage::RunBoundPreMarker {
            run_id,
            resolved: false,
        } => format!(
            "agent spawn unresolved: RunBoundPreMarker resolved=false run_id={run_id}; source={}",
            bounded_redacted_text(&source.to_string())
        ),
        AgentSpawnStage::PostMarker {
            run_id,
            resolved: false,
        } => format!(
            "agent spawn unresolved: PostMarker resolved=false run_id={run_id}; source={}",
            bounded_redacted_text(&source.to_string())
        ),
        AgentSpawnStage::RunBoundPreMarker {
            resolved: true, ..
        }
        | AgentSpawnStage::PostMarker {
            resolved: true, ..
        }
        | AgentSpawnStage::PreBinding => return source,
    };
    AppError::Message {
        message: bounded_redacted_text(&signal),
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
        EventKind::OperatorWake => 2,
        EventKind::DeepCheck => 3,
    }
}

fn dispatch_mode(kind: EventKind) -> &'static str {
    match kind {
        EventKind::Crash | EventKind::AutoKilled | EventKind::TerminationFailed => "crash",
        EventKind::TaskFailed => "failure",
        EventKind::Stalled => "stalled",
        EventKind::TaskFinished => "completion",
        EventKind::OperatorWake => "operator_wake",
        EventKind::DeepCheck => "deep_check",
    }
}

pub fn build_prompt(
    project: &crate::models::Project,
    mode: &str,
    events: &[Event],
    interventions: &[Intervention],
) -> Result<String, AppError> {
    let base_prompt = build_base_prompt(project, mode, events)?;
    if interventions.is_empty() {
        return Ok(truncate_to_prompt_budget(&base_prompt, MAX_PROMPT_BYTES));
    }

    let mut prompt = truncate_to_prompt_budget(
        &base_prompt,
        MAX_PROMPT_BYTES - OPERATOR_INTERVENTIONS_PREFIX.len(),
    );
    prompt.push_str(OPERATOR_INTERVENTIONS_PREFIX);
    for (index, intervention) in interventions.iter().enumerate() {
        prompt.push_str(&format!("{}. {}\n", index + 1, intervention.message));
    }

    Ok(truncate_to_prompt_budget(&prompt, MAX_PROMPT_BYTES))
}

fn build_base_prompt(
    project: &crate::models::Project,
    mode: &str,
    events: &[Event],
) -> Result<String, AppError> {
    let mut prompt = format!(
        "Dispatch mode: {mode}\nProject ID: {}\nProject root: {}\n\nContext references:\n- .pueue-agent/instructions.md\n- .pueue-agent/STATE.md (human campaign objective)\n- .pueue-agent/state.json (bounded agent scratch projection)\n\nBounded event summary:\n",
        project.project_id,
        project
            .root_path
            .to_str()
            .ok_or(AppError::Configuration {
                field: "project.root_path"
            })?
    );

    for event in events {
        let evidence = prompt_event_evidence(event);
        prompt.push_str(&format!(
            "- event_id={} kind={} attempts={} created_at={} evidence={}\n",
            event.event_id, event.kind, event.attempts, event.created_at, evidence
        ));
    }
    prompt.push_str(
        "\nInstructions: read .pueue-agent/instructions.md first, then .pueue-agent/STATE.md as the human campaign objective, and finally .pueue-agent/state.json as bounded scratch context. SQLite owns campaign, objective, budget, and lineage authority; preserve configured guardrails.\n",
    );

    Ok(prompt)
}

fn prompt_event_evidence(event: &Event) -> String {
    let mut fields = Vec::with_capacity(5);
    if let Some(task_id) = event.payload.get("task_id").and_then(Value::as_i64) {
        fields.push(format!("task_id={task_id}"));
    }
    if let Some(source) = prompt_payload_text(&event.payload, "source") {
        fields.push(format!("source={source}"));
    }
    fields.push(format!("action={}", event.kind.as_str()));
    if let Some(state) = prompt_payload_text(&event.payload, "state").or_else(|| {
        event
            .payload
            .get("metadata")
            .and_then(|metadata| metadata.get("state"))
            .and_then(Value::as_str)
            .map(bounded_redacted_text)
    }) {
        fields.push(format!("state={state}"));
    }
    if let Some(reason) = prompt_payload_text(&event.payload, "reason") {
        fields.push(format!("reason={reason}"));
    }
    truncate_to_prompt_budget(
        &bounded_redacted_text(&fields.join(" ")),
        MAX_EVENT_EVIDENCE_BYTES,
    )
}

fn prompt_payload_text(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(bounded_redacted_text)
}

fn truncate_to_prompt_budget(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    if max_bytes <= TRUNCATION_SUFFIX.len() {
        return TRUNCATION_SUFFIX[..max_bytes].to_owned();
    }

    let mut end = max_bytes - TRUNCATION_SUFFIX.len();
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{0}{TRUNCATION_SUFFIX}", &value[..end])
}

#[cfg(test)]
mod tests {
    use super::unresolved_spawn_error;
    use crate::{agent::AgentSpawnStage, AppError};

    #[test]
    fn unresolved_error_signal_wraps_only_unresolved_stages() {
        let resolved = unresolved_spawn_error(
            AgentSpawnStage::PostMarker {
                run_id: 7,
                resolved: true,
            },
            AppError::Runtime {
                operation: "finish agent run",
            },
        );
        assert!(matches!(resolved, AppError::Runtime { .. }));

        let unresolved = unresolved_spawn_error(
            AgentSpawnStage::PostMarker {
                run_id: 7,
                resolved: false,
            },
            AppError::Runtime {
                operation: "finish agent run",
            },
        );
        let message = unresolved.to_string();
        assert!(message.contains("PostMarker resolved=false run_id=7"));
        assert!(message.len() <= 240);
    }
}
