use std::{collections::BTreeMap, time::SystemTime};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    db::{
        database_error, Db, DecisionRepository, EventRepository, ExperimentRepository,
        HealthRepository, ProjectRepository, SubmissionRepository, TaskObservationRepository,
    },
    detect::Observation,
    events::{callback_dedup_key, result_is_failure},
    execution_policy::CampaignLimits,
    incidents::IncidentStore,
    models::{
        EventKind, Experiment, ExperimentTerminalOutcome, NewEvent, NewTaskObservation,
        Submission, SubmissionStatus,
    },
    pueue::{PueueApi, PueueTask},
    termination::{
        auto_kill_request_for_terminal_task, confirm_auto_kill_terminal_observation,
        AutoKillConfirmation,
    },
    AppError,
};

pub type TaskSignature = String;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReconcileReport {
    pub status_task_count: usize,
    pub observed_task_count: usize,
    pub observed_tasks: Vec<PueueTask>,
    pub task_finished_events: usize,
    pub task_failed_events: usize,
    pub recovered_submission_ids: Vec<String>,
    pub unknown_groups: Vec<String>,
}

pub struct Reconciler<'db, P> {
    db: &'db Db,
    pueue: P,
    campaign_limits: CampaignLimits,
}

impl<'db, P> Reconciler<'db, P>
where
    P: PueueApi,
{
    pub fn new(db: &'db Db, pueue: P) -> Self {
        Self {
            db,
            pueue,
            campaign_limits: CampaignLimits::default(),
        }
    }

    pub fn with_campaign_limits(mut self, limits: CampaignLimits) -> Self {
        self.campaign_limits = limits;
        self
    }

    pub async fn run_once(&mut self) -> Result<ReconcileReport, AppError> {
        self.run_once_at(unix_timestamp()?).await
    }

    pub async fn run_once_at(&mut self, now: i64) -> Result<ReconcileReport, AppError> {
        // The status call is deliberately made before any observation write. An
        // unavailable or malformed response must never be treated as an idle
        // snapshot and must leave the previous observations untouched.
        let tasks = self.pueue.status_json().await?;
        let projects = ProjectRepository::new(self.db).list_enabled()?;
        let projects_by_group = projects
            .iter()
            .map(|project| (project.pueue_group.as_str(), project))
            .collect::<BTreeMap<_, _>>();
        let mut report = ReconcileReport {
            status_task_count: tasks.len(),
            ..ReconcileReport::default()
        };

        for task in &tasks {
            if !projects_by_group.contains_key(task.group.as_str()) {
                if !report.unknown_groups.contains(&task.group) {
                    report.unknown_groups.push(task.group.clone());
                }
                continue;
            }

            let project = projects_by_group[task.group.as_str()];
            let signature = task_signature(task);
            let observation = NewTaskObservation::new(
                project.project_id.clone(),
                signature.clone(),
                task.id,
                task.group.clone(),
                vec![task.command.clone()],
                task.state.clone(),
                task.enqueued_at.as_deref().and_then(parse_timestamp),
                task.started_at.as_deref().and_then(parse_timestamp),
                task.ended_at.as_deref().and_then(parse_timestamp),
                task.result
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(|source| AppError::Serialization {
                        operation: "serialize Pueue task result",
                        source,
                    })?,
                now,
            );
            TaskObservationRepository::new(self.db).upsert(&observation)?;
            report.observed_task_count += 1;
            report.observed_tasks.push(task.clone());

            if task.is_running() {
                register_running_health(self.db, &project.project_id, task.id, now)?;
            }

            if task.is_terminal() {
                let auto_kill_request =
                    auto_kill_request_for_terminal_task(self.db, &project.project_id, task)?;
                let auto_kill_confirmation = auto_kill_request
                    .as_ref()
                    .map(|request| {
                        confirm_auto_kill_terminal_observation(self.db, request.request_id, now)
                    })
                    .transpose()?;
                let event_kind = match auto_kill_confirmation {
                    Some(AutoKillConfirmation::Confirmed)
                    | Some(AutoKillConfirmation::AlreadyConfirmed) => EventKind::AutoKilled,
                    Some(AutoKillConfirmation::NotSent) | None => terminal_event_kind(task),
                };
                let experiment = resolve_terminal_experiment(
                    self.db,
                    &project.project_id,
                    task,
                    &tasks,
                    now,
                )?;
                if let Some(experiment) = experiment.as_ref() {
                    project_terminal_experiment(
                        self.db,
                        experiment,
                        task,
                        &self.campaign_limits,
                        now,
                    )?;
                    if let Some(objective) = crate::result_manifest::campaign_objective(
                        self.db,
                        &experiment.campaign_id,
                    )? {
                        crate::result_manifest::ingest(
                            self.db,
                            std::path::Path::new(&project.root_path),
                            &project.project_id,
                            &experiment.experiment_id,
                            task.id,
                            Some(&objective),
                            now,
                        )?;
                    }
                }
                let event = materialize_terminal_event(
                    self.db,
                    project.project_id.as_str(),
                    task,
                    &signature,
                    event_kind,
                    experiment.as_ref(),
                    now,
                )?;
                if let Some(experiment) = experiment.as_ref() {
                    let cycle_id = DecisionRepository::terminal_cycle_id(
                        &experiment.campaign_id,
                        &experiment.experiment_id,
                    );
                    let decision_event = NewEvent::new(
                        &project.project_id,
                        EventKind::CampaignDecision,
                        format!("campaign-decision:v1:{cycle_id}"),
                        json!({
                            "source": "terminal_experiment",
                            "cycle_id": cycle_id,
                            "source_experiment_id": experiment.experiment_id,
                            "terminal_observation": {
                                "task_id": task.id,
                                "task_signature": experiment.task_signature,
                                "group": task.group,
                                "state": task.state,
                                "enqueued_at": task.enqueued_at.as_deref().and_then(parse_timestamp),
                                "started_at": task.started_at.as_deref().and_then(parse_timestamp),
                                "ended_at": task.ended_at.as_deref().and_then(parse_timestamp),
                                "exit_code": task.result.as_ref().and_then(terminal_exit_code),
                            },
                        }),
                        now,
                        now,
                    )
                    .with_campaign_lineage(
                        experiment.campaign_id.clone(),
                        Some(experiment.experiment_id.clone()),
                    );
                    DecisionRepository::new(self.db).publish_terminal_cycle_event(
                        &experiment.campaign_id,
                        &experiment.experiment_id,
                        &decision_event,
                        now,
                    )?;
                }
                match event_kind {
                    EventKind::TaskFinished => report.task_finished_events += 1,
                    EventKind::TaskFailed => report.task_failed_events += 1,
                    EventKind::AutoKilled => {}
                    _ => unreachable!(
                        "terminal event kind is limited to task completion/failure/auto-kill"
                    ),
                }
                let _ = event;
                let _ = IncidentStore::new(self.db).observe(Observation::task_terminal(
                    project.project_id.as_str(),
                    task_incident_key(task),
                    now,
                ))?;
            }
        }

        recover_submissions(self.db, &projects, &tasks, &mut report)?;
        DecisionRepository::new(self.db).backfill_terminal_cycle_events(now)?;
        Ok(report)
    }
}

fn resolve_terminal_experiment(
    db: &Db,
    project_id: &str,
    task: &PueueTask,
    tasks: &[PueueTask],
    now: i64,
) -> Result<Option<Experiment>, AppError> {
    let connection = db.connect()?;
    let mut statement = connection
        .prepare(
             "SELECT submission_id FROM submissions
             WHERE project_id = ?1 AND pueue_task_id = ?2
               AND status = 'accepted'
             ORDER BY created_at, submission_id",
        )
        .map_err(|source| AppError::Database {
            operation: "prepare terminal campaign submission lookup",
            source,
        })?;
    let submission_ids = statement
        .query_map(rusqlite::params![project_id, task.id], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|source| AppError::Database {
            operation: "query terminal campaign submissions",
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| AppError::Database {
            operation: "read terminal campaign submissions",
            source,
        })?;
    drop(statement);
    drop(connection);

    let submissions = SubmissionRepository::new(db);
    let experiments = ExperimentRepository::new(db);
    if tasks.iter().filter(|candidate| candidate.id == task.id).count() != 1 {
        let mut ambiguous_experiment_ids = Vec::new();
        for submission_id in submission_ids {
            if let Some(experiment) = experiments.find_by_submission_id(&submission_id)? {
                ambiguous_experiment_ids.push(experiment.experiment_id);
            }
        }
        experiments.quarantine_accepted_identities(
            &ambiguous_experiment_ids,
            "pueue_task_identity_ambiguous",
            now,
        )?;
        return Ok(None);
    }
    let mut matches = Vec::new();
    for submission_id in submission_ids {
        let Some(submission) = submissions.find_by_id(&submission_id)? else {
            continue;
        };
        let matching_tasks = tasks
            .iter()
            .filter(|candidate| accepted_submission_matches_task(&submission, candidate))
            .collect::<Vec<_>>();
        if matching_tasks.len() == 1 {
            if accepted_submission_matches_task(&submission, task) {
                matches.push(submission);
            }
            continue;
        }
        if let Some(experiment) = experiments.find_by_submission_id(&submission.submission_id)? {
            experiments.quarantine_accepted_identity(
                &experiment.experiment_id,
                "pueue_task_identity_mismatch",
                now,
            )?;
        }
    }
    if matches.len() != 1 {
        let mut ambiguous_experiment_ids = Vec::new();
        for submission in matches {
            if let Some(experiment) = experiments.find_by_submission_id(&submission.submission_id)? {
                ambiguous_experiment_ids.push(experiment.experiment_id);
            }
        }
        experiments.quarantine_accepted_identities(
            &ambiguous_experiment_ids,
            "pueue_task_identity_ambiguous",
            now,
        )?;
        return Ok(None);
    }
    let Some(experiment) = experiments.find_by_submission_id(&matches[0].submission_id)? else {
        return Ok(None);
    };
    Ok(Some(experiment))
}

fn project_terminal_experiment(
    db: &Db,
    experiment: &Experiment,
    task: &PueueTask,
    limits: &CampaignLimits,
    now: i64,
) -> Result<(), AppError> {
    let experiments = ExperimentRepository::new(db);
    if task.state.eq_ignore_ascii_case("killed") {
        experiments.project_terminal_submission(
            &experiment.experiment_id,
            task.id,
            ExperimentTerminalOutcome::Cancelled,
            now,
        )?;
    } else if terminal_event_kind(task) == EventKind::TaskFailed {
        let failure_code = if task.state.eq_ignore_ascii_case("failed") {
            "pueue_failed"
        } else {
            "pueue_result_failed"
        };
        let failure_fingerprint = failure_fingerprint(task, failure_code);
        experiments.project_terminal_submission(
            &experiment.experiment_id,
            task.id,
            ExperimentTerminalOutcome::Failed {
                failure_code,
                failure_fingerprint: &failure_fingerprint,
            },
            now,
        )?;
    } else {
        experiments.project_terminal_submission(
            &experiment.experiment_id,
            task.id,
            ExperimentTerminalOutcome::Succeeded,
            now,
        )?;
    }
    crate::health::handle_terminal_projection(db, experiment, task, limits, now)?;
    HealthRepository::delete_for_experiment(db, &experiment.experiment_id)
}

fn register_running_health(
    db: &Db,
    project_id: &str,
    task_id: i64,
    now: i64,
) -> Result<(), AppError> {
    let connection = db.connect()?;
    let mut statement = connection
        .prepare(
            "SELECT e.campaign_id, e.experiment_id
             FROM experiments e
             JOIN submissions s ON s.submission_id = e.submission_id
             WHERE s.project_id = ?1 AND s.pueue_task_id = ?2 AND s.status = 'accepted'
             ORDER BY e.created_at, e.experiment_id
             LIMIT 2",
        )
        .map_err(database_error("prepare running health experiment lookup"))?;
    let matches = statement
        .query_map(rusqlite::params![project_id, task_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(database_error("query running health experiments"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(database_error("read running health experiments"))?;
    drop(statement);
    drop(connection);
    if let [(campaign_id, experiment_id)] = matches.as_slice() {
        HealthRepository::ensure_running(db, project_id, campaign_id, experiment_id, task_id, now)?;
    }
    Ok(())
}

fn accepted_submission_matches_task(submission: &Submission, task: &PueueTask) -> bool {
    if submission.pueue_task_id != Some(task.id) {
        return false;
    }
    let Some(stored_signature) = submission.task_signature.as_deref() else {
        return false;
    };
    managed_task_run_signature(task).as_deref() == Some(stored_signature)
}

pub fn managed_task_run_signature(task: &PueueTask) -> Option<TaskSignature> {
    let enqueued_at = task.enqueued_at.as_deref()?;
    parse_timestamp(enqueued_at)?;
    let identity = json!({
        "group": task.group,
        "id": task.id,
        "enqueued_at": enqueued_at,
        "command_sha256": format!("{:x}", Sha256::digest(task.command.as_bytes())),
    });
    Some(format!(
        "pueue-managed-run:v1:{:x}",
        Sha256::digest(
            serde_json::to_vec(&identity).expect("managed task identity JSON is serializable")
        )
    ))
}

fn failure_fingerprint(task: &PueueTask, failure_code: &str) -> String {
    let cause = json!({
        "version": 1,
        "failure_code": failure_code,
        "evidence": normalized_failure_evidence(task.result.as_ref()),
    });
    format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&cause).expect("failure cause JSON is serializable")
        )
    )
}

fn normalized_failure_evidence(result: Option<&Value>) -> Value {
    match result {
        Some(Value::Object(object)) => object
            .get("Failed")
            .and_then(Value::as_i64)
            .map(|exit_code| json!({"class": "exit_code", "exit_code": exit_code}))
            .unwrap_or_else(|| json!({"class": "unclassified"})),
        Some(Value::String(value)) if value.eq_ignore_ascii_case("failed") => {
            json!({"class": "failed"})
        }
        _ => json!({"class": "unclassified"}),
    }
}

fn terminal_exit_code(result: &Value) -> Option<i32> {
    match result {
        Value::Object(object) => object
            .get("Failed")
            .or_else(|| object.get("Success"))
            .and_then(Value::as_i64)
            .and_then(|code| i32::try_from(code).ok()),
        _ => None,
    }
}

pub fn task_signature(task: &PueueTask) -> TaskSignature {
    // JSON gives each component an unambiguous boundary. In particular, a
    // reused numeric ID cannot collide when any lifecycle timestamp or state
    // changes, and arbitrary group/state text cannot create a delimiter alias.
    let identity = json!({
        "group": task.group,
        "id": task.id,
        "enqueued_at": task.enqueued_at,
        "started_at": task.started_at,
        "ended_at": task.ended_at,
        "state": task.state,
    });
    format!(
        "pueue-task:v1:{}",
        serde_json::to_string(&identity).expect("task identity JSON is serializable")
    )
}

pub fn task_incident_key(task: &PueueTask) -> TaskSignature {
    // Task-scoped incidents must survive lifecycle transitions from running to
    // terminal, but numeric Pueue IDs can be reused. Use the stable run
    // identity fields and exclude state, ended_at, and result.
    let identity = json!({
        "group": task.group,
        "id": task.id,
        "enqueued_at": task.enqueued_at,
        "started_at": task.started_at,
    });
    format!(
        "pueue-task-incident:v1:{}",
        serde_json::to_string(&identity).expect("task incident identity JSON is serializable")
    )
}

fn terminal_event_kind(task: &PueueTask) -> EventKind {
    if task.state.eq_ignore_ascii_case("failed")
        || task.state.eq_ignore_ascii_case("killed")
        || task.result.as_ref().is_some_and(result_is_failure)
    {
        EventKind::TaskFailed
    } else {
        EventKind::TaskFinished
    }
}

fn materialize_terminal_event(
    db: &Db,
    project_id: &str,
    task: &PueueTask,
    signature: &str,
    kind: EventKind,
    experiment: Option<&Experiment>,
    now: i64,
) -> Result<crate::models::Event, AppError> {
    let result = task
        .result
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|source| AppError::Serialization {
            operation: "serialize Pueue event result",
            source,
        })?
        .unwrap_or_else(|| "null".to_owned());
    let dedup_key = format!("pueue-terminal:v1:{signature}:result={result}");
    let mut event = NewEvent::new(
        project_id,
        kind,
        dedup_key,
        json!({
            "source": "pueue_reconciliation",
            "group": task.group,
            "task_id": task.id,
            "task_signature": signature,
            "command": task.command,
            "state": task.state,
            "enqueued_at": task.enqueued_at,
            "started_at": task.started_at,
            "ended_at": task.ended_at,
            "result": task.result,
        }),
        now,
        now,
    );
    if let Some(experiment) = experiment {
        event = event.with_campaign_lineage(
            experiment.campaign_id.clone(),
            Some(experiment.experiment_id.clone()),
        );
    }
    let repository = EventRepository::new(db);

    if let Some(existing) = repository.find_by_dedup_key(project_id, &event.dedup_key)? {
        if event.campaign_id.is_some()
            && (existing.campaign_id != event.campaign_id
                || existing.experiment_id != event.experiment_id)
        {
            if existing.campaign_id.is_some() || existing.experiment_id.is_some() {
                return Err(AppError::Validation {
                    field: "event.lineage",
                    message: "conflicts with the resolved managed experiment lineage",
                });
            }
            if let Some(upgraded) = repository.replace_pending(existing.event_id, &event)? {
                return Ok(upgraded);
            }
            return repository.replace_callback_with_terminal(existing.event_id, &event);
        }
        if let Some(callback) =
            repository.find_by_dedup_key(project_id, &callback_dedup_key(&task.group, task.id))?
        {
            if callback.event_id != existing.event_id {
                repository.discard_pending(callback.event_id)?;
            }
        }
        return Ok(existing);
    }

    let callback_key = callback_dedup_key(&task.group, task.id);
    if let Some(callback) = repository.find_by_dedup_key(project_id, &callback_key)? {
        if let Some(replaced) = repository.replace_pending(callback.event_id, &event)? {
            return Ok(replaced);
        }
        return repository.replace_callback_with_terminal(callback.event_id, &event);
    }
    repository.insert_idempotent(&event)
}

fn recover_submissions(
    db: &Db,
    projects: &[crate::models::Project],
    tasks: &[PueueTask],
    report: &mut ReconcileReport,
) -> Result<(), AppError> {
    let repository = SubmissionRepository::new(db);
    let experiments = ExperimentRepository::new(db);
    for project in projects {
        let mut candidates = tasks
            .iter()
            .filter(|task| task.group == project.pueue_group)
            .collect::<Vec<_>>();
        candidates.sort_unstable_by_key(|task| task.id);
        for submission in repository.find_unreconciled(&project.project_id)? {
            // Managed submissions require an experiment transition in the same
            // repository transaction. Leave them for managed reconciliation;
            // the legacy adoption path only owns standalone submissions.
            if experiments
                .find_by_submission_id(&submission.submission_id)?
                .is_some()
            {
                continue;
            }
            let matches = candidates
                .iter()
                .filter(|task| submission_matches_task(submission_ref(&submission), task))
                .collect::<Vec<_>>();
            if matches.len() != 1 {
                continue;
            }
            let task = matches[0];
            let signature = task_signature(task);
            let stored = if submission.pueue_task_id.is_some() {
                repository.mark_accepted(&submission.submission_id, task.id, &signature)?
            } else {
                repository.adopt(&submission.submission_id, task.id, &signature)?
            };
            if stored.status != SubmissionStatus::Failed {
                report.recovered_submission_ids.push(stored.submission_id);
            }
        }
    }
    Ok(())
}

fn submission_ref(submission: &Submission) -> &Submission {
    submission
}

fn submission_matches_task(submission: &Submission, task: &PueueTask) -> bool {
    if submission
        .pueue_task_id
        .is_some_and(|task_id| task_id != task.id)
    {
        return false;
    }
    if canonical_command_display(&submission.argv) != task.command {
        return false;
    }

    match task.enqueued_at.as_deref().and_then(parse_timestamp) {
        Some(enqueued_at) => (enqueued_at - submission.created_at).abs() <= 600,
        None => submission.pueue_task_id.is_some(),
    }
}

pub(crate) fn canonical_command_display(argv: &[String]) -> String {
    argv.iter()
        .map(|argument| shell_quote(argument))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote(argument: &str) -> String {
    if !argument.is_empty()
        && argument
            .bytes()
            .all(|byte| matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'@' | b'%' | b'_' | b'+' | b'=' | b':' | b',' | b'.' | b'/' | b'-'))
    {
        return argument.to_owned();
    }
    format!("'{}'", argument.replace('\'', r"'\''"))
}

pub(crate) fn parse_timestamp(value: &str) -> Option<i64> {
    if let Ok(seconds) = value.parse::<i64>() {
        return Some(seconds);
    }
    parse_rfc3339(value)
}

fn parse_rfc3339(value: &str) -> Option<i64> {
    let (date, time_and_zone) = value.split_once('T')?;
    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse::<i64>().ok()?;
    let month = date_parts.next()?.parse::<i64>().ok()?;
    let day = date_parts.next()?.parse::<i64>().ok()?;
    if date_parts.next().is_some() {
        return None;
    }

    let (clock, offset_seconds) = if let Some(clock) = time_and_zone.strip_suffix('Z') {
        (clock, 0_i64)
    } else {
        let index = time_and_zone.rfind(['+', '-'])?;
        let (clock, offset) = time_and_zone.split_at(index);
        let sign = if offset.starts_with('-') { -1 } else { 1 };
        let offset = &offset[1..];
        let mut parts = offset.split(':');
        let hours = parts.next()?.parse::<i64>().ok()?;
        let minutes = parts.next()?.parse::<i64>().ok()?;
        if parts.next().is_some() || hours > 23 || minutes > 59 {
            return None;
        }
        (clock, sign * (hours * 3_600 + minutes * 60))
    };
    let clock = clock.split('.').next()?;
    let mut clock_parts = clock.split(':');
    let hour = clock_parts.next()?.parse::<i64>().ok()?;
    let minute = clock_parts.next()?.parse::<i64>().ok()?;
    let second = clock_parts.next()?.parse::<i64>().ok()?;
    if clock_parts.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    let days = days_from_civil(year, month, day)?;
    days.checked_mul(86_400)?
        .checked_add(hour * 3_600 + minute * 60 + second)?
        .checked_sub(offset_seconds)
}

fn days_from_civil(year: i64, month: i64, day: i64) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let adjusted_year = year.checked_sub(i64::from(month <= 2))?;
    let era = if adjusted_year >= 0 {
        adjusted_year / 400
    } else {
        (adjusted_year - 399) / 400
    };
    let year_of_era = adjusted_year - era * 400;
    let month_index = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era.checked_mul(146_097)?
        .checked_add(day_of_era)?
        .checked_sub(719_468)
}

fn unix_timestamp() -> Result<i64, AppError> {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| AppError::Runtime {
            operation: "read the system clock for reconciliation",
        })?
        .as_secs()
        .try_into()
        .map_err(|_| AppError::Runtime {
            operation: "represent the reconciliation timestamp",
        })
}
