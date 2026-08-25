use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use rusqlite::OptionalExtension;
use uuid::Uuid;

use crate::{
    db::{
        count_live_repair_descendants, database_error,
        running_health::HealthRepository,
        CampaignRepository, Db, EventRepository, IncidentRepository, ProposalRepository,
        ProjectRepository, SubmissionRepository,
    },
    execution_policy::CampaignLimits,
    health_diagnosis::{parse_and_validate_diagnosis, RecommendedAction, ValidatedDiagnosis},
    logs::LogSnapshot,
    models::{
        EventKind, Experiment, HealthState, NewEvent, NewIncident, Project, ProposalKind,
        RunningHealthRow, SignalSummaryEntry, TerminationRequestStatus,
    },
    proposals::{self, ProposalInput},
    pueue::{PueueApi, PueueTask},
    signals::{SignalClass, SignalObservation, SignalSource},
    reconcile::{task_incident_key, task_signature},
    termination::{latest_request_for_pueue_task, require_confirmed, TerminationManager},
    AppError,
};

const MAX_OBSERVATIONS_PER_PASS: usize = 100;
const MAX_ACTIONS_PER_PASS: usize = 100;
const STALENESS_CLASS: &str = "staleness";
const HEALTH_DIAGNOSIS_INCIDENT_KIND: &str = "health_diagnosis";

/// Signal observations collected during the daemon detection pass, keyed by
/// Pueue task id.  The observer consumes these instead of re-reading task
/// logs so every pass reads each log exactly once.
pub type DetectionSignals = BTreeMap<i64, Vec<SignalObservation>>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HealthReport {
    pub observed: usize,
    pub escalated: usize,
    pub executed_actions: usize,
}

pub struct HealthEngine;

impl HealthEngine {
    pub fn run_once(
        db: &Db,
        projects: &[Project],
        pueue_snapshot: &[PueueTask],
        detection_signals: &DetectionSignals,
        limits: &CampaignLimits,
        now: i64,
    ) -> Result<HealthReport, AppError> {
        let mut report = HealthReport::default();
        let due_rows = HealthRepository::due_observations(
            db,
            now,
            limits.observer_interval_minutes,
            MAX_OBSERVATIONS_PER_PASS,
        )?;
        let projects_by_id = projects
            .iter()
            .map(|project| (project.project_id.as_str(), project))
            .collect::<BTreeMap<_, _>>();
        let tasks_by_id = pueue_snapshot
            .iter()
            .map(|task| (task.id, task))
            .collect::<BTreeMap<_, _>>();

        for row in due_rows {
            let Some(project) = projects_by_id.get(row.project_id.as_str()) else {
                continue;
            };
            if project.paused || project.halted_reason.is_some() {
                continue;
            }
            if campaign_defers(db, &row.campaign_id)? {
                continue;
            }
            let Some(task) = tasks_by_id.get(&row.pueue_task_id).copied() else {
                continue;
            };
            if !task.is_running() {
                continue;
            }

            let signals = detection_signals
                .get(&row.pueue_task_id)
                .map(Vec::as_slice)
                .unwrap_or_default();
            observe_experiment(db, &row, signals, now, &mut report)?;
        }
        Ok(report)
    }

    /// Execute the stored diagnosis of `ActionPending` rows.  `continue`
    /// resets the row to healthy; kill actions ride the standard termination
    /// pipeline: a missing request is opened here as `Requested` (the
    /// daemon's termination pass performs the actual kill exactly once per
    /// request), in-flight requests are left to that same pipeline, and an
    /// already-confirmed request defers to the terminal projection hook.
    /// Failed or timed-out requests are refused with an idempotent error
    /// event.
    pub async fn execute_pending<P: PueueApi>(
        db: &Db,
        pueue: &P,
        projects: &[Project],
        _limits: &CampaignLimits,
        now: i64,
    ) -> Result<usize, AppError> {
        let mut executed = 0;
        let rows = HealthRepository::pending_actions(db, MAX_ACTIONS_PER_PASS)?;
        for row in rows {
            let Some(project) = projects
                .iter()
                .find(|project| project.project_id == row.project_id)
            else {
                continue;
            };
            if project.paused || project.halted_reason.is_some() {
                continue;
            }
            if campaign_defers(db, &row.campaign_id)? {
                continue;
            }
            let Some(diagnosis_json) = row.diagnosis_json.as_deref() else {
                continue;
            };
            let Ok(diagnosis) = parse_and_validate_diagnosis(diagnosis_json.as_bytes()) else {
                continue;
            };
            match diagnosis.recommended_action {
                RecommendedAction::Continue => {
                    HealthRepository::reset_to_healthy(db, &row.experiment_id, now)?;
                    executed += 1;
                }
                RecommendedAction::KillAndResume | RecommendedAction::KillAndEscalate => {
                    if require_confirmed(db, &row.project_id, row.pueue_task_id)?.is_some() {
                        continue;
                    }
                    match latest_request_for_pueue_task(db, &row.project_id, row.pueue_task_id)? {
                        Some(request)
                            if matches!(
                                request.status,
                                TerminationRequestStatus::Requested
                                    | TerminationRequestStatus::Dispatching
                                    | TerminationRequestStatus::Sent
                            ) => {}
                        None => {
                            if request_diagnosed_kill(db, pueue, &row, &diagnosis, now).await? {
                                executed += 1;
                            }
                        }
                        _ => {
                            record_refused_action(
                                db,
                                &row,
                                diagnosis.recommended_action,
                                now,
                            )?;
                        }
                    }
                }
            }
        }
        Ok(executed)
    }
}

/// Open the `Requested` termination request behind a diagnosed kill so the
/// daemon's standard termination pipeline performs the kill exactly once.
/// The incident is keyed by the live task's stable incident identity; tasks
/// that already disappeared return `false` and are left to the terminal
/// projection hook.
async fn request_diagnosed_kill<P: PueueApi>(
    db: &Db,
    pueue: &P,
    row: &RunningHealthRow,
    diagnosis: &ValidatedDiagnosis,
    now: i64,
) -> Result<bool, AppError> {
    let Some(project) = ProjectRepository::new(db).find_by_id(&row.project_id)? else {
        return Ok(false);
    };
    let task = match pueue
        .status_json()
        .await?
        .into_iter()
        .find(|task| task.id == row.pueue_task_id && task.group == project.pueue_group)
    {
        Some(task) => task,
        None => return Ok(false),
    };
    let incident_key = task_incident_key(&task);
    let update = IncidentRepository::new(db).upsert_active(&NewIncident::new(
        &row.project_id,
        HEALTH_DIAGNOSIS_INCIDENT_KIND,
        Some(incident_key.as_str()),
        format!("{}:{}", incident_key, row.experiment_id),
        now,
    ))?;
    let reason = serde_json::json!({
        "source": "health_action_executor",
        "experiment_id": row.experiment_id,
        "campaign_id": row.campaign_id,
        "recommended_action": diagnosis.recommended_action,
        "root_cause_class": diagnosis.root_cause_class,
        "confidence": diagnosis.confidence,
        "summary": diagnosis.summary,
    })
    .to_string();
    TerminationManager::new_without_pueue(db).request_with_reason(
        update.incident.incident_id,
        task_signature(&task),
        reason,
        now,
        None,
    )?;
    Ok(true)
}

/// Complete a confirmed health action once its killed task reaches the
/// terminal projection.  Runs before the running-health row is deleted; the
/// resume path reserves a same-spec successor experiment through the
/// coordinator budget path while escalation degrades the campaign and wakes
/// the operator.  Repeat projections are no-ops.
pub(crate) fn handle_terminal_projection(
    db: &Db,
    experiment: &Experiment,
    task: &PueueTask,
    limits: &CampaignLimits,
    now: i64,
) -> Result<(), AppError> {
    if !task.state.eq_ignore_ascii_case("killed") {
        return Ok(());
    }
    let Some(row) = HealthRepository::get(db, &experiment.experiment_id)? else {
        return Ok(());
    };
    if row.state != HealthState::ActionPending {
        return Ok(());
    }
    let Some(project) = ProjectRepository::new(db).find_by_id(&row.project_id)? else {
        return Ok(());
    };
    if project.paused || project.halted_reason.is_some() {
        return Ok(());
    }
    let Some(diagnosis_json) = row.diagnosis_json.as_deref() else {
        return Ok(());
    };
    let Ok(diagnosis) = parse_and_validate_diagnosis(diagnosis_json.as_bytes()) else {
        return Ok(());
    };
    match diagnosis.recommended_action {
        RecommendedAction::Continue => return Ok(()),
        RecommendedAction::KillAndResume | RecommendedAction::KillAndEscalate => {}
    }
    if require_confirmed(db, &row.project_id, row.pueue_task_id)?.is_none() {
        record_refused_action(db, &row, diagnosis.recommended_action, now)?;
        return Ok(());
    }
    // A successor already dispatched for this projection makes any further
    // work here a duplicate: reconcile reprocesses the same terminal task on
    // every pass until the row is deleted.
    if count_direct_resume_successors(db, &experiment.experiment_id)? > 0 {
        return Ok(());
    }
    match diagnosis.recommended_action {
        RecommendedAction::KillAndResume if limits.max_live_repairs > 0 => {
            if count_live_repairs(db, &experiment.experiment_id)?
                >= i64::from(limits.max_live_repairs)
            {
                escalate_after_terminal(
                    db,
                    &row,
                    diagnosis.recommended_action,
                    escalation_reason(diagnosis.recommended_action),
                    now,
                )?;
            } else if !resume_experiment(db, &row, experiment, limits, now)? {
                escalate_after_terminal(
                    db,
                    &row,
                    diagnosis.recommended_action,
                    "resume_budget_blocked",
                    now,
                )?;
            }
        }
        _ => escalate_after_terminal(
            db,
            &row,
            diagnosis.recommended_action,
            escalation_reason(diagnosis.recommended_action),
            now,
        )?,
    }
    Ok(())
}

fn count_direct_resume_successors(db: &Db, experiment_id: &str) -> Result<i64, AppError> {
    let connection = db.connect()?;
    connection
        .query_row(
            "SELECT COUNT(*) FROM experiments WHERE resume_of_experiment_id = ?1",
            [experiment_id],
            |row| row.get(0),
        )
        .map_err(database_error("count direct resume successors"))
}

/// Total resume repairs across the whole lineage rooted at the origin of
/// `experiment_id`, so repeated single-resume generations exhaust
/// `max_live_repairs` cumulatively instead of per generation.
fn count_live_repairs(db: &Db, experiment_id: &str) -> Result<i64, AppError> {
    let connection = db.connect()?;
    count_live_repair_descendants(&connection, experiment_id)
        .map_err(database_error("count live resume repairs"))
}

fn resume_experiment(
    db: &Db,
    row: &RunningHealthRow,
    source: &Experiment,
    limits: &CampaignLimits,
    now: i64,
) -> Result<bool, AppError> {
    let submission = SubmissionRepository::new(db)
        .find_by_id(&source.submission_id)?
        .ok_or(AppError::Runtime {
            operation: "read source submission for running-health resume",
        })?;
    let source_proposal = ProposalRepository::new(db)
        .find_by_id(&source.proposal_id)?
        .ok_or(AppError::Runtime {
            operation: "read source proposal for running-health resume",
        })?;
    let objective_digest = CampaignRepository::new(db)
        .find_by_id(&row.campaign_id)?
        .ok_or(AppError::Validation {
            field: "campaign_id",
            message: "running health row references a missing campaign",
        })?
        .objective_digest;
    let proposal = proposals::validate(
        ProposalInput {
            kind: ProposalKind::Experiment,
            hypothesis: format!("Resume {} after a confirmed kill", source.experiment_id),
            source_experiment_id: Some(source.experiment_id.clone()),
            argv: submission.argv.clone(),
            working_directory: source_proposal.working_directory.clone(),
            expected_evidence: source_proposal.expected_evidence.clone(),
        },
        &objective_digest,
    )?;
    Ok(CampaignRepository::new(db)
        .accept_resume_proposal(
            &row.campaign_id,
            &Uuid::new_v4().to_string(),
            &Uuid::new_v4().to_string(),
            &Uuid::new_v4().to_string(),
            &proposal,
            &format!("resumed-after-confirmed-kill:{}", source.experiment_id),
            limits,
            now,
        )?
        .is_some())
}

fn escalation_reason(action: RecommendedAction) -> &'static str {
    match action {
        RecommendedAction::KillAndResume => "live_repair_budget_exhausted",
        RecommendedAction::KillAndEscalate => "diagnosed_kill_and_escalate",
        RecommendedAction::Continue => "continue_is_not_an_escalation",
    }
}

fn escalate_after_terminal(
    db: &Db,
    row: &RunningHealthRow,
    action: RecommendedAction,
    reason: &str,
    now: i64,
) -> Result<(), AppError> {
    let connection = db.connect()?;
    let degraded = connection
        .execute(
            "UPDATE campaigns SET state = 'degraded', state_reason = ?1, updated_at = ?2
             WHERE campaign_id = ?3 AND state IN ('active','budget_waiting')",
            rusqlite::params![reason, now, row.campaign_id],
        )
        .map_err(database_error("degrade campaign after health escalation"))?;
    drop(connection);
    if degraded == 1 {
        let event = NewEvent::new(
            &row.project_id,
            EventKind::OperatorWake,
            format!("health-escalate:v1:{}", row.experiment_id),
            serde_json::json!({
                "source": "health_action_executor",
                "experiment_id": row.experiment_id,
                "campaign_id": row.campaign_id,
                "recommended_action": action,
                "reason": reason,
            }),
            now,
            now,
        )
        .with_campaign_lineage(row.campaign_id.as_str(), Some(row.experiment_id.as_str()));
        EventRepository::new(db).insert_idempotent(&event)?;
    }
    Ok(())
}

fn record_refused_action(
    db: &Db,
    row: &RunningHealthRow,
    action: RecommendedAction,
    now: i64,
) -> Result<(), AppError> {
    let event = NewEvent::new(
        &row.project_id,
        EventKind::TerminationFailed,
        format!("health-action-refused:v1:{}", row.experiment_id),
        serde_json::json!({
            "source": "health_action_executor",
            "experiment_id": row.experiment_id,
            "campaign_id": row.campaign_id,
            "pueue_task_id": row.pueue_task_id,
            "recommended_action": action,
            "reason": "no confirmed termination request for the task",
        }),
        now,
        now,
    )
    .with_campaign_lineage(row.campaign_id.as_str(), Some(row.experiment_id.as_str()));
    EventRepository::new(db).insert_idempotent(&event)?;
    Ok(())
}

fn observe_experiment(
    db: &Db,
    row: &RunningHealthRow,
    signals: &[SignalObservation],
    now: i64,
    report: &mut HealthReport,
) -> Result<(), AppError> {
    let repeated_class = repeated_non_staleness_class(&row.signal_summary_json, signals)?;

    for signal in signals {
        HealthRepository::record_observation(db, &row.experiment_id, now, summary_entry(signal))?;
    }
    if signals.is_empty() {
        HealthRepository::mark_observed(db, &row.experiment_id, now)?;
    }

    let staleness_breach = signals
        .iter()
        .any(|signal| signal.class == SignalClass::Staleness);
    if (staleness_breach || repeated_class) && row.state == HealthState::Healthy {
        HealthRepository::set_state(db, &row.experiment_id, HealthState::Suspicious, now)?;
        report.escalated += 1;
    }
    report.observed += 1;
    Ok(())
}

pub(crate) fn campaign_defers(db: &Db, campaign_id: &str) -> Result<bool, AppError> {
    let connection = db.connect()?;
    let state: Option<String> = connection
        .query_row(
            "SELECT state FROM campaigns WHERE campaign_id = ?1",
            [campaign_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(database_error("read campaign state for running health"))?;
    Ok(matches!(state.as_deref(), Some("paused") | Some("halted")))
}

pub(crate) fn read_task_tail(
    log_dir: &Path,
    task_id: i64,
    tail_bytes: u32,
) -> Result<Option<LogSnapshot>, AppError> {
    for candidate in [
        log_dir.join(format!("{task_id}.log")),
        log_dir.join(format!("task_{task_id}.log")),
    ] {
        match LogSnapshot::read_tail(&candidate, tail_bytes) {
            Ok(snapshot) => return Ok(Some(snapshot)),
            Err(AppError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

fn repeated_non_staleness_class(
    summary_json: &str,
    signals: &[SignalObservation],
) -> Result<bool, AppError> {
    let previous: Vec<SignalSummaryEntry> =
        serde_json::from_str(summary_json).map_err(|source| AppError::Serialization {
            operation: "parse stored running health signal summary",
            source,
        })?;
    let Some(latest_observed_at) = previous.iter().map(|entry| entry.observed_at).max() else {
        return Ok(false);
    };
    let latest_classes = previous
        .iter()
        .filter(|entry| entry.observed_at == latest_observed_at)
        .map(|entry| entry.class.as_str())
        .collect::<BTreeSet<_>>();
    Ok(signals.iter().any(|signal| {
        let class = class_label(&signal.class);
        class != STALENESS_CLASS && latest_classes.contains(class.as_str())
    }))
}

fn class_label(class: &SignalClass) -> String {
    match class {
        SignalClass::Oom => "oom".to_owned(),
        SignalClass::Numerical => "numerical".to_owned(),
        SignalClass::WorkerLoss => "worker_loss".to_owned(),
        SignalClass::Staleness => "staleness".to_owned(),
        SignalClass::Exception => "exception".to_owned(),
        SignalClass::Configured(name) => name.clone(),
    }
}

fn source_label(source: &SignalSource) -> &'static str {
    match source {
        SignalSource::BuiltinProbe => "builtin_probe",
        SignalSource::ConfigPattern => "config_pattern",
        SignalSource::Stall => "stall",
    }
}

fn summary_entry(signal: &SignalObservation) -> SignalSummaryEntry {
    SignalSummaryEntry {
        class: class_label(&signal.class),
        source: source_label(&signal.source).to_owned(),
        evidence_digest: signal.evidence_digest.clone(),
        observed_at: signal.observed_at,
    }
}
