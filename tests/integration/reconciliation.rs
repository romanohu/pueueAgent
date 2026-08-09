use std::{
    fs,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use pueue_agent::{
    db::{Db, EventRepository, ProjectRepository, SubmissionRepository},
    events::{record_callback_with, CallbackMetadata},
    models::{EventKind, NewProject, NewSubmission, SubmissionStatus},
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
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM events
                 WHERE project_id = ?1 AND kind = ?2 AND status = 'pending'",
                rusqlite::params!["project-a", kind],
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
fn unknown_callback_group_is_visible_without_registering_a_project() {
    let harness = Harness::new();

    let error = record_callback_with(
        &harness.db,
        "unknown-group",
        41,
        CallbackMetadata::default(),
    )
    .unwrap_err();

    assert!(error.to_string().contains("unknown Pueue group"));
    let project_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM projects", [], |row| row.get(0))
        .unwrap();
    assert_eq!(project_count, 1);
    assert_eq!(harness.event_count(), 0);
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

#[test]
fn callback_metadata_is_retained_as_json() {
    let harness = Harness::new();
    let event_id = record_callback_with(
        &harness.db,
        "pa-project",
        41,
        CallbackMetadata::new(Some("Done"), Some(json!("Success"))),
    )
    .unwrap();

    let event = EventRepository::new(&harness.db)
        .find_by_id(event_id)
        .unwrap()
        .unwrap();
    assert_eq!(event.kind, EventKind::TaskFinished);
    assert_eq!(event.payload["group"], "pa-project");
    assert_eq!(event.payload["task_id"], 41);
    assert_eq!(event.payload["metadata"]["state"], "Done");
}
