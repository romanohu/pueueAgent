use std::{
    fs,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use pueue_agent::{
    db::{
        CampaignRepository, Db, EventRepository, ExperimentRepository, ProjectRepository,
        ManagedSubmissionIntent, StartCampaignRequest, SubmissionRepository,
    },
    execution_policy::CampaignLimits,
    events::{
        callback_group_for_task, record_callback_with, CallbackMetadata, CallbackRecordResult,
    },
    models::{
        BudgetReservationStatus, EventKind, EventStatus, ExperimentStatus, NewProject,
        NewSubmission, ProposalKind, SubmissionStatus,
    },
    proposals::{self, ProposalInput},
    pueue::{PueueApi, PueueError, PueueTask},
    reconcile::{task_signature, Reconciler},
    AppError,
};
use serde_json::json;
use tempfile::TempDir;

#[derive(Clone)]
struct FakePueue {
    tasks: Arc<Mutex<Vec<PueueTask>>>,
    status_calls: Arc<Mutex<usize>>,
    malformed: Arc<Mutex<bool>>,
}

impl FakePueue {
    fn with_tasks(tasks: Vec<PueueTask>) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(tasks)),
            status_calls: Arc::new(Mutex::new(0)),
            malformed: Arc::new(Mutex::new(false)),
        }
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

    async fn add(&self, _args: &[std::ffi::OsString]) -> Result<i64, AppError> {
        panic!("reconciliation must not submit Pueue tasks")
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
                    now: 100,
                },
                &CampaignLimits::default(),
            )
            .unwrap()
    }

    fn accepted_campaign_experiment(&self, task_id: i64) -> String {
        let intent = self.campaign_intent();
        let experiment_id = intent.experiment.experiment_id;
        let experiments = ExperimentRepository::new(&self.db);
        experiments.mark_submitting(&experiment_id, 101).unwrap();
        experiments
            .mark_accepted(
                &experiment_id,
                task_id,
                &format!(
                    "provisional-submit:v1:group=pa-project:task-id={task_id}:intent=campaign-submission-baseline"
                ),
                102,
            )
            .unwrap();
        experiment_id
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
        ExperimentStatus::Accepted
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
        ExperimentStatus::Accepted
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
