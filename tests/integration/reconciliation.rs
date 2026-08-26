use std::{
    ffi::OsString,
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use clap::Parser as _;
use pueue_agent::{
    campaign::CampaignCoordinator,
    cli::{Cli, Command},
    db::{
        CampaignRepository, Db, EventRepository, ExperimentRepository, ManagedSubmissionIntent,
        MetricsRepository, ProjectRepository, StartCampaignRequest, SubmissionRepository,
        TaskObservationRepository,
    },
    environment,
    events::{
        callback_group_for_task, record_callback_with, CallbackMetadata, CallbackRecordResult,
    },
    execution_policy::CampaignLimits,
    models::{
        BudgetReservationStatus, EventKind, EventStatus, ExperimentMetricsRow, ExperimentStatus,
        MetricDirection, NewProject, NewSubmission, NewTaskObservation, ObjectiveMetric,
        ProposalKind, SubmissionStatus,
    },
    proposals::{self, ProposalInput},
    pueue::{PueueApi, PueueError, PueueTask},
    reconcile::{managed_task_run_signature, task_signature, Reconciler},
    state::ObjectiveSnapshot,
    AppError,
};
use serde_json::json;
use tempfile::TempDir;

#[derive(Clone)]
struct FakePueue {
    tasks: Arc<Mutex<Vec<PueueTask>>>,
    status_calls: Arc<Mutex<usize>>,
    malformed: Arc<Mutex<bool>>,
    recorded_adds: Arc<Mutex<Vec<Vec<OsString>>>>,
}

impl FakePueue {
    fn with_tasks(tasks: Vec<PueueTask>) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(tasks)),
            status_calls: Arc::new(Mutex::new(0)),
            malformed: Arc::new(Mutex::new(false)),
            recorded_adds: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn recorded_adds(&self) -> Vec<Vec<OsString>> {
        self.recorded_adds.lock().unwrap().clone()
    }

    fn set_tasks(&self, tasks: Vec<PueueTask>) {
        *self.tasks.lock().unwrap() = tasks;
    }

    fn set_malformed(&self, malformed: bool) {
        *self.malformed.lock().unwrap() = malformed;
    }

    fn status_calls(&self) -> usize {
        *self.status_calls.lock().unwrap()
    }
}

#[async_trait]
impl PueueApi for FakePueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        *self.status_calls.lock().unwrap() += 1;
        if *self.malformed.lock().unwrap() {
            let source = serde_json::from_str::<serde_json::Value>("not-json").unwrap_err();
            return Err(PueueError::InvalidStatusJson { source }.into());
        }
        Ok(self.tasks.lock().unwrap().clone())
    }

    async fn add(&self, args: &[std::ffi::OsString]) -> Result<i64, AppError> {
        self.recorded_adds.lock().unwrap().push(args.to_vec());
        Ok(41)
    }

    async fn kill(&self, _task_id: i64) -> Result<(), AppError> {
        panic!("reconciliation must not kill Pueue tasks")
    }

    async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
        panic!("reconciliation must not remove Pueue tasks")
    }

    async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
        panic!("reconciliation must not provision Pueue groups")
    }
}

fn accepts_api<P: PueueApi>(_api: &P) {}

#[test]
fn reconciliation_fake_preserves_the_pueue_api_contract() {
    accepts_api(&FakePueue::with_tasks(Vec::new()));
}

struct Harness {
    _temp: TempDir,
    db: Db,
}

impl Harness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &root,
                "pa-project",
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        Self { _temp: temp, db }
    }

    fn root(&self) -> PathBuf {
        self._temp.path().join("project")
    }

    fn pending_event_count(&self, kind: EventKind) -> i64 {
        self.event_status_count(kind, EventStatus::Pending)
    }

    fn event_status_count(&self, kind: EventKind, status: EventStatus) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM events
                 WHERE project_id = ?1 AND kind = ?2 AND status = ?3",
                rusqlite::params!["project-a", kind, status],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn event_count(&self) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap()
    }

    fn observation_count(&self) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM task_observations", [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    fn observation_observed_at(&self) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT observed_at FROM task_observations WHERE project_id = ?1",
                ["project-a"],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn integration_event_count(&self) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM integration_events", [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    fn campaign_intent(&self) -> ManagedSubmissionIntent {
        self.campaign_intent_with_objective(None)
    }

    fn campaign_intent_with_objective(
        &self,
        objective_metric: Option<&ObjectiveMetric>,
    ) -> ManagedSubmissionIntent {
        let objective = pueue_agent::state::ObjectiveSnapshot {
            text: "Reach validation loss below 0.20\n".to_owned(),
            digest: "objective-digest".to_owned(),
        };
        let argv = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "--name".to_owned(),
            "experiment".to_owned(),
        ];
        let proposal = proposals::validate_initial_baseline(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: "Establish the initial campaign baseline".to_owned(),
                source_experiment_id: None,
                argv: argv.clone(),
                working_directory: ".".to_owned(),
                expected_evidence: Vec::new(),
            },
            &objective.digest,
        )
        .unwrap();
        CampaignRepository::new(&self.db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id: "campaign-reconciliation",
                    project_id: "project-a",
                    objective: &objective,
                    initial_argv: &argv,
                    baseline: &proposal,
                    submission_id: "campaign-submission-baseline",
                    experiment_id: "campaign-experiment-baseline",
                    proposal_id: "campaign-proposal-baseline",
                    metadata: &json!({}),
                    origin_agent_run_id: None,
                    objective_metric,
                    now: 100,
                },
                &CampaignLimits::default(),
            )
            .unwrap()
    }

    fn accepted_campaign_experiment_with_objective(
        &self,
        task_id: i64,
        enqueued_at: &str,
        objective_metric: Option<&ObjectiveMetric>,
    ) -> String {
        let intent = self.campaign_intent_with_objective(objective_metric);
        let experiment_id = intent.experiment.experiment_id;
        let experiments = ExperimentRepository::new(&self.db);
        experiments.mark_submitting(&experiment_id, 101).unwrap();
        experiments
            .mark_accepted(
                &experiment_id,
                task_id,
                &managed_task_run_signature(&terminal_task(
                    task_id,
                    enqueued_at,
                    json!("Success"),
                ))
                .unwrap(),
                102,
            )
            .unwrap();
        experiment_id
    }

    fn write_result_manifest(&self, file_stem: &str, content: &str) -> PathBuf {
        let results = self.root().join(".pueue-agent/results");
        fs::create_dir_all(&results).unwrap();
        let path = results.join(format!("{file_stem}.json"));
        fs::write(&path, content).unwrap();
        path
    }

    fn metrics_row(&self, experiment_id: &str) -> Option<ExperimentMetricsRow> {
        MetricsRepository::get(&self.db, experiment_id).unwrap()
    }

    fn accepted_campaign_experiment(&self, task_id: i64) -> String {
        self.accepted_campaign_experiment_at(task_id, "100")
    }

    fn accepted_campaign_experiment_at(&self, task_id: i64, enqueued_at: &str) -> String {
        let intent = self.campaign_intent();
        let experiment_id = intent.experiment.experiment_id;
        let experiments = ExperimentRepository::new(&self.db);
        experiments.mark_submitting(&experiment_id, 101).unwrap();
        experiments
            .mark_accepted(
                &experiment_id,
                task_id,
                &managed_task_run_signature(&terminal_task(
                    task_id,
                    enqueued_at,
                    json!("Success"),
                ))
                .unwrap(),
                102,
            )
            .unwrap();
        experiment_id
    }

    fn duplicate_accepted_campaign_identity(&self) -> String {
        let duplicate_experiment_id = "campaign-experiment-ambiguous";
        self.db
            .connect()
            .unwrap()
            .execute_batch(
                "INSERT INTO submissions (
                     submission_id, project_id, argv_json, created_at, pueue_task_id,
                     task_signature, status, kind, metadata_json, origin_agent_run_id
                 )
                 SELECT 'campaign-submission-ambiguous', project_id, argv_json, created_at + 1,
                        pueue_task_id, task_signature, status, kind, metadata_json,
                        origin_agent_run_id
                 FROM submissions WHERE submission_id = 'campaign-submission-baseline';
                 INSERT INTO proposals (
                     proposal_id, campaign_id, kind, status, hypothesis, source_experiment_id,
                     argv_json, working_directory, expected_evidence_json, canonical_digest,
                     reject_reason, created_at, updated_at
                 )
                 SELECT 'campaign-proposal-ambiguous', campaign_id, kind, status, hypothesis,
                        source_experiment_id, argv_json, working_directory,
                        expected_evidence_json, 'campaign-proposal-ambiguous-digest',
                        reject_reason, created_at + 1, updated_at + 1
                 FROM proposals WHERE proposal_id = 'campaign-proposal-baseline';
                 INSERT INTO experiments (
                     experiment_id, campaign_id, proposal_id, submission_id,
                     parent_experiment_id, attempt, status, pueue_task_id, task_signature,
                     failure_code, failure_fingerprint, created_at, updated_at, finished_at
                 )
                 SELECT 'campaign-experiment-ambiguous', campaign_id,
                        'campaign-proposal-ambiguous', 'campaign-submission-ambiguous',
                        parent_experiment_id, attempt, status, pueue_task_id, task_signature,
                        failure_code, failure_fingerprint, created_at + 1, updated_at + 1,
                        finished_at
                 FROM experiments WHERE experiment_id = 'campaign-experiment-baseline';",
            )
            .unwrap();
        duplicate_experiment_id.to_owned()
    }

    async fn reconcile_task(&self, task: PueueTask) {
        Reconciler::new(&self.db, FakePueue::with_tasks(vec![task]))
            .run_once_at(200)
            .await
            .unwrap();
    }

    fn decision_cycle_count(&self, experiment_id: &str) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*)
                 FROM decision_cycles dc
                 JOIN campaigns c ON c.campaign_id = dc.campaign_id
                 JOIN experiments e
                   ON e.experiment_id = dc.source_experiment_id
                  AND e.campaign_id = dc.campaign_id
                 WHERE c.project_id = ?1 AND e.experiment_id = ?2",
                rusqlite::params!["project-a", experiment_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn decision_event_count(&self, experiment_id: &str) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*)
                 FROM events ev
                 JOIN experiments e
                   ON e.experiment_id = ev.experiment_id
                  AND e.campaign_id = ev.campaign_id
                 JOIN campaigns c ON c.campaign_id = e.campaign_id
                 WHERE c.project_id = ?1
                   AND e.experiment_id = ?2
                   AND ev.kind = 'campaign_decision'",
                rusqlite::params!["project-a", experiment_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn terminal_event_precedes_decision(&self, experiment_id: &str) -> bool {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT terminal.event_id < decision.event_id
                 FROM events terminal
                 JOIN events decision
                   ON decision.campaign_id = terminal.campaign_id
                  AND decision.experiment_id = terminal.experiment_id
                  AND decision.kind = 'campaign_decision'
                 WHERE terminal.experiment_id = ?1
                   AND terminal.kind IN ('task_finished','task_failed')",
                [experiment_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn set_event_status(&self, event_id: i64, status: EventStatus) {
        let lease_until = if status == EventStatus::Claimed {
            Some(10_000)
        } else {
            None
        };
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE events
                 SET status = ?1,
                     lease_until = ?2,
                     completed_at = CASE WHEN ?1 = 'completed' THEN 200 ELSE completed_at END
                 WHERE event_id = ?3",
                rusqlite::params![status, lease_until, event_id],
            )
            .unwrap();
    }

    fn observed_command(&self, task_signature: &str) -> Vec<String> {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT command_json FROM task_observations
                 WHERE project_id = ?1 AND task_signature = ?2",
                rusqlite::params!["project-a", task_signature],
                |row| {
                    let json: String = row.get(0)?;
                    serde_json::from_str::<Vec<String>>(&json).map_err(|source| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(source),
                        )
                    })
                },
            )
            .unwrap()
    }
}

fn terminal_task(id: i64, enqueue: &str, result: serde_json::Value) -> PueueTask {
    PueueTask {
        id,
        group: "pa-project".to_owned(),
        command: "python train.py --name experiment".to_owned(),
        state: "Done".to_owned(),
        enqueued_at: Some(enqueue.to_owned()),
        started_at: Some(enqueue.to_owned()),
        ended_at: Some(enqueue.to_owned()),
        result: Some(result),
    }
}

#[tokio::test]
async fn terminal_success_and_failure_each_create_one_decision_cycle_event() {
    for terminal in ["Done", "Failed"] {
        let harness = Harness::new();
        let experiment_id =
            harness.accepted_campaign_experiment_at(41, "2026-08-21T00:00:00Z");
        let task = terminal_task(
            41,
            "2026-08-21T00:00:00Z",
            serde_json::json!(terminal),
        );

        harness.reconcile_task(task.clone()).await;
        harness.reconcile_task(task).await;

        assert_eq!(harness.decision_cycle_count(&experiment_id), 1);
        assert_eq!(harness.decision_event_count(&experiment_id), 1);
        assert!(harness.terminal_event_precedes_decision(&experiment_id));
    }
}

#[tokio::test]
async fn persisted_terminal_observation_backfills_decision_after_terminal_event_interruption() {
    let harness = Harness::new();
    let experiment_id = harness.accepted_campaign_experiment(41);
    let task = terminal_task(41, "100", json!("Success"));
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_terminal_event_publication
             BEFORE INSERT ON events
             WHEN NEW.kind = 'task_finished'
             BEGIN
                 SELECT RAISE(ABORT, 'injected terminal event publication failure');
             END;",
        )
        .unwrap();

    assert!(Reconciler::new(&harness.db, FakePueue::with_tasks(vec![task.clone()]))
        .run_once_at(200)
        .await
        .is_err());

    assert_eq!(harness.decision_cycle_count(&experiment_id), 0);
    assert_eq!(harness.decision_event_count(&experiment_id), 0);
    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Succeeded
    );
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_terminal_event_publication;")
        .unwrap();

    Reconciler::new(&harness.db, FakePueue::with_tasks(Vec::new()))
        .run_once_at(201)
        .await
        .unwrap();

    assert_eq!(harness.decision_cycle_count(&experiment_id), 1);
    assert_eq!(harness.decision_event_count(&experiment_id), 1);
    let experiment = ExperimentRepository::new(&harness.db)
        .find_by_id(&experiment_id)
        .unwrap()
        .unwrap();
    let payload_json: String = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT payload_json FROM events
             WHERE experiment_id = ?1 AND kind = 'campaign_decision'",
            [&experiment_id],
            |row| row.get(0),
        )
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&payload_json).unwrap();
    assert_eq!(
        payload["terminal_observation"]["task_signature"],
        json!(experiment.task_signature.unwrap())
    );
    assert_eq!(payload["terminal_observation"]["enqueued_at"], json!(100));
    Reconciler::new(&harness.db, FakePueue::with_tasks(Vec::new()))
        .run_once_at(202)
        .await
        .unwrap();
    assert_eq!(harness.decision_cycle_count(&experiment_id), 1);
    assert_eq!(harness.decision_event_count(&experiment_id), 1);
}

#[tokio::test]
async fn persisted_terminal_without_terminal_observation_is_not_backfilled() {
    let harness = Harness::new();
    let experiment_id = harness.accepted_campaign_experiment(41);
    ExperimentRepository::new(&harness.db)
        .project_terminal_submission(
            &experiment_id,
            41,
            pueue_agent::models::ExperimentTerminalOutcome::Succeeded,
            200,
        )
        .unwrap();

    Reconciler::new(&harness.db, FakePueue::with_tasks(Vec::new()))
        .run_once_at(201)
        .await
        .unwrap();

    assert_eq!(harness.decision_cycle_count(&experiment_id), 0);
    assert_eq!(harness.decision_event_count(&experiment_id), 0);
}

#[tokio::test]
async fn terminal_backfill_rejects_ambiguous_or_mismatched_persisted_observations() {
    for observations in [
        vec![("observation-group-mismatch", "other-group", "Done", "Success")],
        vec![("observation-outcome-mismatch", "pa-project", "Done", "Failed")],
        vec![
            ("observation-first", "pa-project", "Done", "Success"),
            ("observation-second", "pa-project", "Done", "Success"),
        ],
    ] {
        let harness = Harness::new();
        let experiment_id = harness.accepted_campaign_experiment(41);
        ExperimentRepository::new(&harness.db)
            .project_terminal_submission(
                &experiment_id,
                41,
                pueue_agent::models::ExperimentTerminalOutcome::Succeeded,
                200,
            )
            .unwrap();
        for (signature, group, state, result) in observations {
            TaskObservationRepository::new(&harness.db)
                .upsert(&NewTaskObservation::new(
                    "project-a",
                    signature,
                    41,
                    group,
                    vec!["python train.py --name experiment".to_owned()],
                    state,
                    Some(100),
                    Some(100),
                    Some(100),
                    Some(json!(result).to_string()),
                    200,
                ))
                .unwrap();
        }

        Reconciler::new(&harness.db, FakePueue::with_tasks(Vec::new()))
            .run_once_at(201)
            .await
            .unwrap();

        assert_eq!(harness.decision_cycle_count(&experiment_id), 0);
        assert_eq!(harness.decision_event_count(&experiment_id), 0);
    }
}

#[tokio::test]
async fn ambiguous_accepted_identities_quarantine_atomically_across_interruption() {
    let harness = Harness::new();
    let first_experiment_id = harness.accepted_campaign_experiment(41);
    let second_experiment_id = harness.duplicate_accepted_campaign_identity();
    let task = terminal_task(41, "100", json!("Success"));
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER interrupt_ambiguous_identity_quarantine
             BEFORE UPDATE OF status ON experiments
             WHEN OLD.experiment_id = 'campaign-experiment-ambiguous'
             BEGIN
                 SELECT RAISE(ABORT, 'injected ambiguity quarantine interruption');
             END;",
        )
        .unwrap();

    assert!(Reconciler::new(&harness.db, FakePueue::with_tasks(vec![task.clone()]))
        .run_once_at(200)
        .await
        .is_err());

    let experiments = ExperimentRepository::new(&harness.db);
    assert_eq!(
        experiments
            .find_by_id(&first_experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Accepted
    );
    assert_eq!(
        experiments
            .find_by_id(&second_experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Accepted
    );
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER interrupt_ambiguous_identity_quarantine;")
        .unwrap();

    Reconciler::new(&harness.db, FakePueue::with_tasks(vec![task]))
        .run_once_at(201)
        .await
        .unwrap();

    assert_eq!(
        experiments
            .find_by_id(&first_experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Unreconciled
    );
    assert_eq!(
        experiments
            .find_by_id(&second_experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Unreconciled
    );
    assert_eq!(harness.decision_cycle_count(&first_experiment_id), 0);
    assert_eq!(harness.decision_cycle_count(&second_experiment_id), 0);
    assert_eq!(harness.pending_event_count(EventKind::CampaignDecision), 0);
}

#[tokio::test]
async fn unlineaged_terminal_task_does_not_create_a_decision_cycle() {
    let harness = Harness::new();

    harness
        .reconcile_task(terminal_task(41, "100", json!("Success")))
        .await;

    assert_eq!(harness.pending_event_count(EventKind::CampaignDecision), 0);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM decision_cycles", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn unreconciled_campaign_experiment_does_not_create_a_decision_cycle() {
    let harness = Harness::new();
    let experiment_id = harness.accepted_campaign_experiment(41);

    harness
        .reconcile_task(terminal_task(41, "200", json!({"Failed": 17})))
        .await;

    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Unreconciled
    );
    assert_eq!(harness.decision_cycle_count(&experiment_id), 0);
    assert_eq!(harness.decision_event_count(&experiment_id), 0);
}

#[test]
fn managed_run_identity_requires_a_valid_enqueue_timestamp() {
    for enqueue in ["", "not-a-timestamp"] {
        assert!(managed_task_run_signature(&terminal_task(41, enqueue, json!("Success"))).is_none());
    }
}

#[tokio::test]
async fn campaign_experiment_terminal_success_is_projected_and_consumes_reservation() {
    let harness = Harness::new();
    let experiment_id = harness.accepted_campaign_experiment(41);
    let fake = FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]);

    Reconciler::new(&harness.db, fake).run_once_at(200).await.unwrap();

    let experiment = ExperimentRepository::new(&harness.db)
        .find_by_id(&experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(experiment.status, ExperimentStatus::Succeeded);
    let reservation: BudgetReservationStatus = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status FROM budget_reservations WHERE experiment_id = ?1",
            [&experiment_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reservation, BudgetReservationStatus::Consumed);
}

#[tokio::test]
async fn campaign_experiment_terminal_failure_is_projected_idempotently() {
    let harness = Harness::new();
    let experiment_id = harness.accepted_campaign_experiment(41);
    let task = terminal_task(41, "100", json!({"Failed": 17}));
    let fake = FakePueue::with_tasks(vec![task]);
    let mut reconciler = Reconciler::new(&harness.db, fake);

    reconciler.run_once_at(200).await.unwrap();
    reconciler.run_once_at(201).await.unwrap();

    let experiment = ExperimentRepository::new(&harness.db)
        .find_by_id(&experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(experiment.status, ExperimentStatus::Failed);
    assert!(experiment.failure_code.is_some());
    assert!(experiment.failure_fingerprint.is_some());
    assert_eq!(experiment.finished_at, Some(200));
}

#[tokio::test]
async fn campaign_experiment_failed_to_spawn_is_projected_as_failure() {
    let harness = Harness::new();
    let experiment_id = harness.accepted_campaign_experiment(41);
    let fake = FakePueue::with_tasks(vec![terminal_task(
        41,
        "100",
        json!({"FailedToSpawn": "missing executable"}),
    )]);

    Reconciler::new(&harness.db, fake)
        .run_once_at(200)
        .await
        .unwrap();

    let experiment = ExperimentRepository::new(&harness.db)
        .find_by_id(&experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(experiment.status, ExperimentStatus::Failed);
    assert_eq!(experiment.failure_code.as_deref(), Some("pueue_result_failed"));
}

#[tokio::test]
async fn campaign_task_id_reuse_quarantines_the_managed_identity_without_consuming_budget() {
    let harness = Harness::new();
    let experiment_id = harness.accepted_campaign_experiment(41);
    let fake = FakePueue::with_tasks(vec![terminal_task(
        41,
        "200",
        json!({"Failed": 17}),
    )]);

    Reconciler::new(&harness.db, fake)
        .run_once_at(300)
        .await
        .unwrap();

    let experiment = ExperimentRepository::new(&harness.db)
        .find_by_id(&experiment_id)
        .unwrap()
        .unwrap();
    let submission = SubmissionRepository::new(&harness.db)
        .find_by_id(&experiment.submission_id)
        .unwrap()
        .unwrap();
    let reservation: BudgetReservationStatus = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status FROM budget_reservations WHERE experiment_id = ?1",
            [&experiment_id],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(experiment.status, ExperimentStatus::Unreconciled);
    assert_eq!(submission.status, SubmissionStatus::Unreconciled);
    assert_eq!(reservation, BudgetReservationStatus::Reserved);
}

#[tokio::test]
async fn equivalent_managed_failures_share_one_bounded_cause_fingerprint() {
    let first = Harness::new();
    let first_experiment_id = first.accepted_campaign_experiment(41);
    Reconciler::new(
        &first.db,
        FakePueue::with_tasks(vec![terminal_task(
            41,
            "100",
            json!({"Failed": 17, "message": "worker-a volatile detail"}),
        )]),
    )
    .run_once_at(200)
    .await
    .unwrap();

    let second = Harness::new();
    let second_experiment_id = second.accepted_campaign_experiment_at(99, "900");
    Reconciler::new(
        &second.db,
        FakePueue::with_tasks(vec![terminal_task(
            99,
            "900",
            json!({"Failed": 17, "message": "worker-b volatile detail"}),
        )]),
    )
    .run_once_at(1_000)
    .await
    .unwrap();

    let first_fingerprint = ExperimentRepository::new(&first.db)
        .find_by_id(&first_experiment_id)
        .unwrap()
        .unwrap()
        .failure_fingerprint
        .unwrap();
    let second_fingerprint = ExperimentRepository::new(&second.db)
        .find_by_id(&second_experiment_id)
        .unwrap()
        .unwrap()
        .failure_fingerprint
        .unwrap();

    assert_eq!(first_fingerprint, second_fingerprint);
}

#[tokio::test]
async fn managed_terminal_event_carries_campaign_and_experiment_lineage() {
    let harness = Harness::new();
    let experiment_id = harness.accepted_campaign_experiment(41);
    Reconciler::new(
        &harness.db,
        FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]),
    )
    .run_once_at(200)
    .await
    .unwrap();

    let lineage: (Option<String>, Option<String>) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT campaign_id, experiment_id FROM events
             WHERE project_id = ?1 AND kind = 'task_finished'",
            ["project-a"],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();

    assert_eq!(lineage.0.as_deref(), Some("campaign-reconciliation"));
    assert_eq!(lineage.1.as_deref(), Some(experiment_id.as_str()));
}

#[tokio::test]
async fn accepted_managed_identity_upgrades_an_existing_unlineaged_terminal_event() {
    let harness = Harness::new();
    let intent = harness.campaign_intent();
    let experiment_id = intent.experiment.experiment_id;
    let experiments = ExperimentRepository::new(&harness.db);
    experiments.mark_submitting(&experiment_id, 101).unwrap();
    let task = terminal_task(41, "100", json!({"Failed": 17}));
    let fake = FakePueue::with_tasks(vec![task.clone()]);
    let mut reconciler = Reconciler::new(&harness.db, fake);

    reconciler.run_once_at(200).await.unwrap();
    let before: (Option<String>, Option<String>) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT campaign_id, experiment_id FROM events WHERE kind = 'task_failed'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(before, (None, None));

    experiments
        .mark_accepted(
            &experiment_id,
            task.id,
            &managed_task_run_signature(&task).unwrap(),
            201,
        )
        .unwrap();
    reconciler.run_once_at(202).await.unwrap();

    let after: (Option<String>, Option<String>) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT campaign_id, experiment_id FROM events WHERE kind = 'task_failed'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(after.0.as_deref(), Some("campaign-reconciliation"));
    assert_eq!(after.1.as_deref(), Some(experiment_id.as_str()));
    assert_eq!(harness.pending_event_count(EventKind::TaskFailed), 1);
    assert_eq!(
        experiments
            .find_by_id(&experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Failed
    );
}

#[tokio::test]
async fn campaign_experiment_unreconciled_is_not_adopted_by_legacy_recovery() {
    let harness = Harness::new();
    let intent = harness.campaign_intent();
    let experiments = ExperimentRepository::new(&harness.db);
    experiments
        .mark_submitting(&intent.experiment.experiment_id, 101)
        .unwrap();
    experiments
        .mark_unreconciled(&intent.experiment.experiment_id, "pueue_add_unknown", 102)
        .unwrap();
    let fake = FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]);

    Reconciler::new(&harness.db, fake).run_once_at(200).await.unwrap();

    let experiment = experiments
        .find_by_id(&intent.experiment.experiment_id)
        .unwrap()
        .unwrap();
    let submission = SubmissionRepository::new(&harness.db)
        .find_by_id(&intent.submission.submission_id)
        .unwrap()
        .unwrap();
    assert_eq!(experiment.status, ExperimentStatus::Unreconciled);
    assert_eq!(submission.status, SubmissionStatus::Unreconciled);
    assert_eq!(submission.pueue_task_id, None);
}

#[tokio::test]
async fn campaign_unreconciled_accepted_identity_rejects_command_only_task_match() {
    let harness = Harness::new();
    let intent = harness.campaign_intent();
    let experiments = ExperimentRepository::new(&harness.db);
    experiments
        .mark_submitting(&intent.experiment.experiment_id, 101)
        .unwrap();
    experiments
        .mark_accepted(
            &intent.experiment.experiment_id,
            41,
            "provisional-submit:v1:group=pa-project:task-id=99:intent=campaign-submission-baseline",
            102,
        )
        .unwrap();
    let fake = FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]);

    Reconciler::new(&harness.db, fake)
        .run_once_at(200)
        .await
        .unwrap();

    assert_eq!(
        experiments
            .find_by_id(&intent.experiment.experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Unreconciled
    );
}

#[tokio::test]
async fn campaign_unreconciled_accepted_identity_requires_one_unique_status_task() {
    let harness = Harness::new();
    let experiment_id = harness.accepted_campaign_experiment(41);
    let task = terminal_task(41, "100", json!("Success"));
    let fake = FakePueue::with_tasks(vec![task.clone(), task]);

    Reconciler::new(&harness.db, fake)
        .run_once_at(200)
        .await
        .unwrap();

    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Unreconciled
    );
}

#[tokio::test]
async fn campaign_experiment_conflicting_terminal_observation_is_rejected() {
    let harness = Harness::new();
    let experiment_id = harness.accepted_campaign_experiment(41);
    let fake = FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]);
    let mut reconciler = Reconciler::new(&harness.db, fake.clone());
    reconciler.run_once_at(200).await.unwrap();
    fake.set_tasks(vec![terminal_task(41, "100", json!({"Failed": 17}))]);

    assert!(reconciler.run_once_at(201).await.is_err());
    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Succeeded
    );
}

#[test]
fn callback_resolves_and_validates_group_from_numeric_task_id() {
    let tasks = vec![terminal_task(41, "100", json!("Success"))];

    assert_eq!(callback_group_for_task(&tasks, 41).unwrap(), "pa-project");
    assert!(matches!(
        callback_group_for_task(&tasks, 42),
        Err(AppError::Validation {
            field: "callback.task_id",
            message: "was not found in the configured Pueue profile"
        })
    ));

    let duplicate = vec![
        terminal_task(41, "100", json!("Success")),
        terminal_task(41, "200", json!("Success")),
    ];
    assert!(matches!(
        callback_group_for_task(&duplicate, 41),
        Err(AppError::Validation {
            field: "callback.task_id",
            message: "is ambiguous in the configured Pueue profile"
        })
    ));

    let invalid = vec![PueueTask {
        group: "x'; touch injected; #".to_owned(),
        ..terminal_task(41, "100", json!("Success"))
    }];
    assert!(callback_group_for_task(&invalid, 41).is_err());
}

#[tokio::test]
async fn duplicate_callback_and_reconciliation_create_one_completion_event() {
    let harness = Harness::new();
    let fake = FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]);

    record_callback_with(&harness.db, "pa-project", 41, CallbackMetadata::default()).unwrap();
    record_callback_with(&harness.db, "pa-project", 41, CallbackMetadata::default()).unwrap();
    Reconciler::new(&harness.db, fake).run_once().await.unwrap();

    assert_eq!(harness.pending_event_count(EventKind::TaskFinished), 1);
    assert_eq!(harness.event_count(), 1);
}

#[tokio::test]
async fn callback_after_reconciliation_does_not_create_a_second_completion_row() {
    let harness = Harness::new();
    let fake = FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]);
    let mut reconciler = Reconciler::new(&harness.db, fake);

    reconciler.run_once().await.unwrap();
    record_callback_with(&harness.db, "pa-project", 41, CallbackMetadata::default()).unwrap();
    reconciler.run_once().await.unwrap();

    assert_eq!(harness.event_count(), 1);
    assert_eq!(harness.pending_event_count(EventKind::TaskFinished), 1);
}

#[tokio::test]
async fn reconciliation_materializes_a_completion_when_the_callback_was_missed() {
    let harness = Harness::new();
    let fake = FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]);

    let report = Reconciler::new(&harness.db, fake).run_once().await.unwrap();

    assert_eq!(report.task_finished_events, 1);
    assert_eq!(harness.pending_event_count(EventKind::TaskFinished), 1);
    assert_eq!(harness.observation_count(), 1);
}

#[tokio::test]
async fn reconciliation_uses_the_supplied_time_for_observations() {
    let harness = Harness::new();
    let fake = FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]);

    Reconciler::new(&harness.db, fake)
        .run_once_at(3_700)
        .await
        .unwrap();

    assert_eq!(harness.observation_observed_at(), 3_700);
}

#[tokio::test]
async fn reconciliation_is_idempotent_when_callback_event_is_claimed() {
    let harness = Harness::new();
    let fake = FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]);
    let callback = record_callback_with(&harness.db, "pa-project", 41, CallbackMetadata::default())
        .unwrap()
        .event_id();
    harness.set_event_status(callback, EventStatus::Claimed);

    Reconciler::new(&harness.db, fake).run_once().await.unwrap();

    assert_eq!(harness.event_count(), 1);
    assert_eq!(
        harness.event_status_count(EventKind::TaskFinished, EventStatus::Claimed),
        1
    );
}

#[tokio::test]
async fn reconciliation_is_idempotent_when_callback_event_is_completed() {
    let harness = Harness::new();
    let fake = FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]);
    let callback = record_callback_with(&harness.db, "pa-project", 41, CallbackMetadata::default())
        .unwrap()
        .event_id();
    harness.set_event_status(callback, EventStatus::Completed);

    Reconciler::new(&harness.db, fake).run_once().await.unwrap();

    assert_eq!(harness.event_count(), 1);
    assert_eq!(
        harness.event_status_count(EventKind::TaskFinished, EventStatus::Completed),
        1
    );
}

#[tokio::test]
async fn task_id_reuse_remains_distinct_after_callback_reconciliation() {
    let harness = Harness::new();
    let fake = FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]);
    let mut reconciler = Reconciler::new(&harness.db, fake.clone());
    let callback = record_callback_with(&harness.db, "pa-project", 41, CallbackMetadata::default())
        .unwrap()
        .event_id();
    harness.set_event_status(callback, EventStatus::Completed);

    reconciler.run_once().await.unwrap();
    fake.set_tasks(vec![terminal_task(41, "200", json!({"Failed": 17}))]);
    reconciler.run_once().await.unwrap();

    assert_eq!(harness.event_count(), 2);
    assert_eq!(harness.observation_count(), 2);
    assert_eq!(
        harness.event_status_count(EventKind::TaskFinished, EventStatus::Completed),
        1
    );
    assert_eq!(harness.pending_event_count(EventKind::TaskFailed), 1);
}

#[tokio::test]
async fn reused_task_id_creates_distinct_observations_and_events() {
    let harness = Harness::new();
    let fake = FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]);
    let mut reconciler = Reconciler::new(&harness.db, fake.clone());

    reconciler.run_once().await.unwrap();
    let first_signature = task_signature(&terminal_task(41, "100", json!("Success")));
    fake.set_tasks(vec![terminal_task(41, "200", json!({"Failed": 17}))]);
    reconciler.run_once().await.unwrap();
    let second_signature = task_signature(&terminal_task(41, "200", json!({"Failed": 17})));

    assert_ne!(first_signature, second_signature);
    assert!(first_signature.contains("pa-project"));
    assert!(first_signature.contains("41"));
    assert!(first_signature.contains("100"));
    assert!(second_signature.contains("200"));
    assert_eq!(harness.observation_count(), 2);
    assert_eq!(harness.pending_event_count(EventKind::TaskFinished), 1);
    assert_eq!(harness.pending_event_count(EventKind::TaskFailed), 1);
}

#[tokio::test]
async fn malformed_status_is_an_integration_error_not_idle() {
    let harness = Harness::new();
    let fake = FakePueue::with_tasks(Vec::new());
    fake.set_malformed(true);

    let error = Reconciler::new(&harness.db, fake)
        .run_once()
        .await
        .unwrap_err();

    assert!(error.to_string().contains("Pueue status JSON"));
    assert_eq!(harness.event_count(), 0);
    assert_eq!(harness.observation_count(), 0);
}

#[tokio::test]
async fn empty_status_is_an_authoritative_idle_snapshot() {
    let harness = Harness::new();
    let fake = FakePueue::with_tasks(Vec::new());

    let report = Reconciler::new(&harness.db, fake.clone())
        .run_once()
        .await
        .unwrap();

    assert_eq!(fake.status_calls(), 1);
    assert_eq!(report.status_task_count, 0);
    assert_eq!(harness.event_count(), 0);
}

#[test]
fn unknown_callback_group_records_visible_idempotent_integration_event_without_registering_a_project(
) {
    let harness = Harness::new();

    let first = record_callback_with(
        &harness.db,
        "unknown-group",
        41,
        CallbackMetadata::default(),
    )
    .unwrap();
    let second = record_callback_with(
        &harness.db,
        "unknown-group",
        41,
        CallbackMetadata::default(),
    )
    .unwrap();

    assert!(matches!(
        first,
        CallbackRecordResult::UnknownGroup {
            ref group,
            integration_event_id: _
        } if group == "unknown-group"
    ));
    assert_eq!(first, second);
    let project_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM projects", [], |row| row.get(0))
        .unwrap();
    assert_eq!(project_count, 1);
    assert_eq!(harness.event_count(), 0);
    assert_eq!(harness.integration_event_count(), 1);
}

#[tokio::test]
async fn reconciliation_adopts_one_matching_unlinked_submission() {
    let harness = Harness::new();
    SubmissionRepository::new(&harness.db)
        .insert_idempotent(&NewSubmission::new(
            "submission-1",
            "project-a",
            vec![
                "python".to_owned(),
                "train.py".to_owned(),
                "--name".to_owned(),
                "experiment".to_owned(),
            ],
            100,
        ))
        .unwrap();
    let fake = FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]);

    Reconciler::new(&harness.db, fake).run_once().await.unwrap();

    let submission = SubmissionRepository::new(&harness.db)
        .find_by_id("submission-1")
        .unwrap()
        .unwrap();
    assert_eq!(submission.status, SubmissionStatus::Adopted);
    assert_eq!(submission.pueue_task_id, Some(41));
    assert!(submission.task_signature.is_some());
}

#[tokio::test]
async fn reconciliation_recovers_submission_with_quoted_space_and_shell_special_argument() {
    let harness = Harness::new();
    SubmissionRepository::new(&harness.db)
        .insert_idempotent(&NewSubmission::new(
            "submission-quoted",
            "project-a",
            vec![
                "python".to_owned(),
                "train.py".to_owned(),
                "--name".to_owned(),
                "a b; echo bad && $(touch nope)".to_owned(),
            ],
            100,
        ))
        .unwrap();
    let task = PueueTask {
        command: "python train.py --name 'a b; echo bad && $(touch nope)'".to_owned(),
        ..terminal_task(41, "100", json!("Success"))
    };
    let signature = task_signature(&task);
    let fake = FakePueue::with_tasks(vec![task]);

    Reconciler::new(&harness.db, fake).run_once().await.unwrap();

    let submission = SubmissionRepository::new(&harness.db)
        .find_by_id("submission-quoted")
        .unwrap()
        .unwrap();
    assert_eq!(submission.status, SubmissionStatus::Adopted);
    assert_eq!(submission.pueue_task_id, Some(41));
    assert_eq!(
        submission.task_signature.as_deref(),
        Some(signature.as_str())
    );
    assert_eq!(
        harness.observed_command(&signature),
        vec!["python train.py --name 'a b; echo bad && $(touch nope)'"]
    );
}

#[test]
fn callback_metadata_is_retained_as_json() {
    let harness = Harness::new();
    let event_id = record_callback_with(
        &harness.db,
        "pa-project",
        41,
        CallbackMetadata::new(Some("Done"), Some(json!("Success"))),
    )
    .unwrap()
    .event_id();

    let event = EventRepository::new(&harness.db)
        .find_by_id(event_id)
        .unwrap()
        .unwrap();
    assert_eq!(event.kind, EventKind::TaskFinished);
    assert_eq!(event.payload["group"], "pa-project");
    assert_eq!(event.payload["task_id"], 41);
    assert_eq!(event.payload["metadata"]["state"], "Done");
}

fn objective_metric() -> ObjectiveMetric {
    ObjectiveMetric {
        name: "loss".to_owned(),
        direction: MetricDirection::Minimize,
        min_delta: Some(0.01),
    }
}

#[test]
fn campaign_task_environment_exposes_result_and_artifact_locations() {
    let variables = environment::campaign_experiment_task_environment(
        std::path::Path::new("/tmp/project-root"),
        "campaign-a",
        "experiment-a",
    );

    assert_eq!(
        variables,
        vec![
            (
                "PUEUE_AGENT_EXPERIMENT_ID".to_owned(),
                OsString::from("experiment-a"),
            ),
            (
                "PUEUE_AGENT_CAMPAIGN_ID".to_owned(),
                OsString::from("campaign-a"),
            ),
            (
                "PUEUE_AGENT_RESULT_PATH".to_owned(),
                OsString::from("/tmp/project-root/.pueue-agent/results/experiment-a.json"),
            ),
            (
                "PUEUE_AGENT_ARTIFACT_DIR".to_owned(),
                OsString::from("/tmp/project-root/.pueue-agent/artifacts/experiment-a"),
            ),
        ]
    );
}

#[test]
fn submit_metric_flags_require_the_full_trio() {
    let error = Cli::try_parse_from([
        "pueue-agent",
        "submit",
        "--metric-name",
        "loss",
        "--",
        "python",
        "train.py",
    ])
    .unwrap_err();
    assert!(error.to_string().contains("--metric-min-delta"));

    let error = Cli::try_parse_from([
        "pueue-agent",
        "submit",
        "--metric-direction",
        "maximize",
        "--metric-min-delta",
        "0.5",
        "--",
        "python",
        "train.py",
    ])
    .unwrap_err();
    assert!(error.to_string().contains("--metric-name"));

    let parsed = Cli::try_parse_from([
        "pueue-agent",
        "submit",
        "--metric-name",
        "loss",
        "--metric-direction",
        "minimize",
        "--metric-min-delta",
        "-0.5",
        "--",
        "python",
        "train.py",
    ])
    .unwrap();
    let Command::Submit(args) = parsed.command else {
        panic!("expected a submit command");
    };
    assert_eq!(args.metric_name.as_deref(), Some("loss"));
    assert_eq!(args.metric_min_delta, Some(-0.5));
}

#[tokio::test]
async fn start_baseline_persists_the_declared_objective_metric_json() {
    let harness = Harness::new();
    let root = harness.root();
    let project = ProjectRepository::new(&harness.db)
        .find_by_root(&root)
        .unwrap()
        .unwrap();
    let objective = ObjectiveSnapshot {
        text: "Reach validation loss below 0.20\n".to_owned(),
        digest: "objective-digest".to_owned(),
    };
    let argv = vec!["python".to_owned(), "train.py".to_owned()];
    let mut task = terminal_task(41, "300", json!("Success"));
    task.command = "python train.py".to_owned();
    let fake = FakePueue::with_tasks(vec![task]);
    let coordinator = CampaignCoordinator::new(&harness.db, &fake, CampaignLimits::default());

    coordinator
        .start_baseline(
            &project,
            &objective,
            &argv,
            &json!({}),
            None,
            Some(&objective_metric()),
            100,
        )
        .await
        .unwrap();

    let stored: String = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT objective_metric_json FROM campaigns WHERE project_id = 'project-a'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&stored).unwrap();
    assert_eq!(
        parsed,
        json!({"name": "loss", "direction": "minimize", "min_delta": 0.01})
    );

    let adds = fake.recorded_adds();
    assert_eq!(adds.len(), 1);
    assert!(
        !adds[0]
            .iter()
            .any(|argument| argument.to_string_lossy().contains("PUEUE_AGENT_")),
        "durable Pueue argv must stay free of supervisor env values"
    );
}

#[tokio::test]
async fn start_baseline_without_metric_flags_leaves_objective_json_null() {
    let harness = Harness::new();
    let root = harness.root();
    let project = ProjectRepository::new(&harness.db)
        .find_by_root(&root)
        .unwrap()
        .unwrap();
    let objective = ObjectiveSnapshot {
        text: "Reach validation loss below 0.20\n".to_owned(),
        digest: "objective-digest".to_owned(),
    };
    let argv = vec!["python".to_owned(), "train.py".to_owned()];
    let mut task = terminal_task(41, "300", json!("Success"));
    task.command = "python train.py".to_owned();
    let fake = FakePueue::with_tasks(vec![task]);
    let coordinator = CampaignCoordinator::new(&harness.db, &fake, CampaignLimits::default());

    coordinator
        .start_baseline(&project, &objective, &argv, &json!({}), None, None, 100)
        .await
        .unwrap();

    let stored: Option<String> = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT objective_metric_json FROM campaigns WHERE project_id = 'project-a'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored, None);
}

#[tokio::test]
async fn terminal_projection_ingests_manifest_metrics() {
    let harness = Harness::new();
    let experiment_id =
        harness.accepted_campaign_experiment_with_objective(41, "100", Some(&objective_metric()));
    harness.write_result_manifest(
        &experiment_id,
        r#"{"schema_version":1,"experiment_id":"campaign-experiment-baseline","metrics":{"loss":0.42,"accuracy":0.9}}"#,
    );

    Reconciler::new(
        &harness.db,
        FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]),
    )
    .run_once_at(200)
    .await
    .unwrap();

    let row = harness
        .metrics_row(&experiment_id)
        .expect("ingested metrics row");
    assert_eq!(row.source, "manifest");
    assert_eq!(row.artifact_defect, None);
    assert_eq!(row.created_at, 200);
    assert_eq!(row.updated_at, 200);
    assert_eq!(row.primary_metric_name.as_deref(), Some("loss"));
    assert_eq!(row.primary_metric_value, Some(0.42));
    let metrics: serde_json::Value = serde_json::from_str(&row.metrics_json).unwrap();
    assert_eq!(metrics["accuracy"], json!(0.9));
}

#[tokio::test]
async fn terminal_projection_records_result_missing_without_a_manifest() {
    let harness = Harness::new();
    let experiment_id =
        harness.accepted_campaign_experiment_with_objective(41, "100", Some(&objective_metric()));

    Reconciler::new(
        &harness.db,
        FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]),
    )
    .run_once_at(200)
    .await
    .unwrap();

    let row = harness.metrics_row(&experiment_id).expect("defect metrics row");
    assert_eq!(row.source, "manifest");
    assert_eq!(row.artifact_defect.as_deref(), Some("result_missing"));
    assert_eq!(row.metrics_json, "{}");
    assert_eq!(row.primary_metric_name, None);
    assert_eq!(row.primary_metric_value, None);
}

#[tokio::test]
async fn terminal_projection_rejects_manifests_with_mismatched_experiment_ids() {
    let harness = Harness::new();
    let experiment_id =
        harness.accepted_campaign_experiment_with_objective(41, "100", Some(&objective_metric()));
    harness.write_result_manifest(
        &experiment_id,
        r#"{"schema_version":1,"experiment_id":"experiment-elsewhere","metrics":{"loss":0.1}}"#,
    );

    Reconciler::new(
        &harness.db,
        FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]),
    )
    .run_once_at(200)
    .await
    .unwrap();

    let row = harness.metrics_row(&experiment_id).expect("defect metrics row");
    assert_eq!(row.artifact_defect.as_deref(), Some("result_invalid"));
    assert_eq!(row.metrics_json, "{}");
    assert_eq!(row.primary_metric_name, None);
    assert_eq!(row.primary_metric_value, None);
}

#[tokio::test]
async fn terminal_projection_rejects_non_finite_manifest_values() {
    let harness = Harness::new();
    let experiment_id =
        harness.accepted_campaign_experiment_with_objective(41, "100", Some(&objective_metric()));
    harness.write_result_manifest(
        &experiment_id,
        "{\"schema_version\":1,\"experiment_id\":\"campaign-experiment-baseline\",\"metrics\":{\"loss\":1e999}}",
    );

    Reconciler::new(
        &harness.db,
        FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]),
    )
    .run_once_at(200)
    .await
    .unwrap();

    let row = harness.metrics_row(&experiment_id).expect("defect metrics row");
    assert_eq!(row.artifact_defect.as_deref(), Some("result_invalid"));
    assert_eq!(row.metrics_json, "{}");
    assert_eq!(row.primary_metric_value, None);
}

#[tokio::test]
async fn campaigns_without_an_objective_skip_manifest_ingestion() {
    let harness = Harness::new();
    let experiment_id = harness.accepted_campaign_experiment(41);
    harness.write_result_manifest(
        &experiment_id,
        r#"{"schema_version":1,"experiment_id":"campaign-experiment-baseline","metrics":{"loss":0.42}}"#,
    );

    Reconciler::new(
        &harness.db,
        FakePueue::with_tasks(vec![terminal_task(41, "100", json!("Success"))]),
    )
    .run_once_at(200)
    .await
    .unwrap();

    assert!(harness.metrics_row(&experiment_id).is_none());
}
