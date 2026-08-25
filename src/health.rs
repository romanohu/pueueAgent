use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use rusqlite::OptionalExtension;

use crate::{
    config::{self, CheckConfig},
    db::{database_error, running_health::HealthRepository, Db},
    detect::Detector,
    execution_policy::CampaignLimits,
    logs::LogSnapshot,
    models::{HealthState, Project, RunningHealthRow, SignalSummaryEntry},
    pueue::PueueTask,
    signals::{SignalClass, SignalObservation, SignalSource},
    AppError,
};

const MAX_OBSERVATIONS_PER_PASS: usize = 100;
const STALENESS_CLASS: &str = "staleness";

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

            observe_experiment(db, project, &row, task, now, &mut report)?;
        }
        Ok(report)
    }
}

fn observe_experiment(
    db: &Db,
    project: &Project,
    row: &RunningHealthRow,
    task: &PueueTask,
    now: i64,
    report: &mut HealthReport,
) -> Result<(), AppError> {
    let project_config = config::load(&project.config_path)?;
    let log_dir = project.root_path.join(".pueue-agent/logs");
    let snapshot = read_task_tail(&log_dir, task.id, project_config.check.log_tail_bytes)?;
    let stalled = tail_is_stalled(snapshot.as_ref(), &project_config.check, now);
    let detector = Detector::for_project(project.project_id.as_str(), &project.root_path, &log_dir);
    let tail = snapshot
        .as_ref()
        .map(|snapshot| snapshot.evidence.as_str())
        .unwrap_or("");
    let signals = detector.signal_observations_for(task, tail, &project_config.check, stalled, now);

    let repeated_class = repeated_non_staleness_class(&row.signal_summary_json, &signals)?;

    for signal in &signals {
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

fn campaign_defers(db: &Db, campaign_id: &str) -> Result<bool, AppError> {
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

fn read_task_tail(
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

fn tail_is_stalled(snapshot: Option<&LogSnapshot>, config: &CheckConfig, now: i64) -> bool {
    let Some(modified_at_nanos) = snapshot.and_then(|snapshot| snapshot.modified_at_nanos) else {
        return false;
    };
    let Ok(now) = u128::try_from(now) else {
        return false;
    };
    let unchanged_seconds = now.saturating_sub(modified_at_nanos / 1_000_000_000);
    unchanged_seconds >= u128::from(config.stall_minutes) * 60
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
