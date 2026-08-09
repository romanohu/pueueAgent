use std::{
    fs,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use pueue_agent::{
    config::PatternAction,
    db::{Db, ProjectRepository, TerminationRequestRepository},
    detect::Observation,
    incidents::IncidentStore,
    models::{EventKind, EventStatus, NewProject, TerminationRequestStatus},
    pueue::{PueueApi, PueueTask},
    reconcile::{task_incident_key, task_signature, Reconciler},
    termination::{TerminationManager, TerminationOutcome, TerminationPolicy},
    AppError,
};
use tempfile::TempDir;

#[derive(Clone)]
struct FakePueue {
    tasks: Arc<Mutex<Result<Vec<PueueTask>, String>>>,
    kill_calls: Arc<Mutex<Vec<i64>>>,
    kill_error: Arc<Mutex<Option<String>>>,
}

impl FakePueue {
    fn with_tasks(tasks: Vec<PueueTask>) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(Ok(tasks))),
            kill_calls: Arc::new(Mutex::new(Vec::new())),
            kill_error: Arc::new(Mutex::new(None)),
        }
    }

    fn set_tasks(&self, tasks: Vec<PueueTask>) {
        *self.tasks.lock().unwrap() = Ok(tasks);
    }

    fn set_status_error(&self, message: impl Into<String>) {
        *self.tasks.lock().unwrap() = Err(message.into());
    }

    fn set_kill_error(&self, message: impl Into<String>) {
        *self.kill_error.lock().unwrap() = Some(message.into());
    }

    fn kill_calls(&self) -> Vec<i64> {
        self.kill_calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl PueueApi for FakePueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        self.tasks
            .lock()
            .unwrap()
            .clone()
            .map_err(|_| AppError::Runtime {
                operation: "fake Pueue status",
            })
    }

    async fn add(&self, _args: &[std::ffi::OsString]) -> Result<i64, AppError> {
        panic!("termination tests must not submit Pueue tasks")
    }

    async fn kill(&self, task_id: i64) -> Result<(), AppError> {
        self.kill_calls.lock().unwrap().push(task_id);
        if self.kill_error.lock().unwrap().is_some() {
            return Err(AppError::Runtime {
                operation: "fake Pueue kill",
            });
        }
        let mut tasks = self.tasks.lock().unwrap();
        if let Ok(tasks) = &mut *tasks {
            if let Some(task) = tasks.iter_mut().find(|task| task.id == task_id) {
                task.state = "Killed".to_owned();
                task.ended_at = Some("201".to_owned());
            }
        }
        Ok(())
    }
}

struct Harness {
    _temp: TempDir,
    db: Db,
    fake_pueue: FakePueue,
    task: PueueTask,
}

impl Harness {
    fn running_task(project_id: &str, task_id: i64) -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join(project_id);
        fs::create_dir_all(&root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                project_id,
                &root,
                "pa-project",
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        let task = PueueTask {
            id: task_id,
            group: "pa-project".to_owned(),
            command: "python train.py".to_owned(),
            state: "Running".to_owned(),
            enqueued_at: Some("100".to_owned()),
            started_at: Some("101".to_owned()),
            ended_at: None,
            result: None,
        };
        let fake_pueue = FakePueue::with_tasks(vec![task.clone()]);
        Self {
            _temp: temp,
            db,
            fake_pueue,
            task,
        }
    }

    fn stalled_task(project_id: &str, task_id: i64) -> Self {
        Self::running_task(project_id, task_id)
    }

    fn observe(&self, observation: Observation) {
        IncidentStore::new(&self.db).observe(observation).unwrap();
    }

    fn observe_fatal_pattern(&self, pattern_name: &str) {
        self.observe(Observation::task_pattern(
            "project-a",
            task_incident_key(&self.task),
            task_signature(&self.task),
            pattern_name,
            PatternAction::Kill,
            1,
            "CUDA out of memory",
            200,
        ));
    }

    fn observe_stalled_default_notify(&self) {
        self.observe(Observation::stalled(
            "project-a",
            task_incident_key(&self.task),
            pueue_agent::logs::LogSnapshot {
                byte_size: 10,
                modified_at_nanos: Some(1),
                fingerprint: "snapshot-a".to_owned(),
                evidence: "epoch 1\n".to_owned(),
            },
            PatternAction::Notify,
            200,
        ));
    }

    fn pending_request_ids(&self) -> Vec<i64> {
        TerminationRequestRepository::new(&self.db)
            .find_pending("project-a")
            .unwrap()
            .into_iter()
            .map(|request| request.request_id)
            .collect()
    }

    async fn run_termination_cycle(&self) -> Vec<TerminationOutcome> {
        let request_ids = self.pending_request_ids();
        let mut outcomes = Vec::new();
        for request_id in request_ids {
            outcomes.push(
                TerminationManager::new(&self.db, self.fake_pueue.clone())
                    .execute(request_id)
                    .await
                    .unwrap(),
            );
        }
        Reconciler::new(&self.db, self.fake_pueue.clone())
            .run_once()
            .await
            .unwrap();
        outcomes
    }

    fn pending_event_count(&self, kind: EventKind) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM events
                 WHERE project_id = ?1 AND kind = ?2 AND status = ?3",
                rusqlite::params!["project-a", kind, EventStatus::Pending],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn request_status(&self) -> Option<TerminationRequestStatus> {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status FROM termination_requests
                 WHERE project_id = ?1
                 ORDER BY request_id DESC
                 LIMIT 1",
                ["project-a"],
                |row| row.get(0),
            )
            .ok()
    }
}

#[test]
fn policy_maps_only_explicit_kill_observations_to_termination() {
    assert!(!TerminationPolicy.should_kill(&Observation::pattern(
        "project-a",
        "signature-a",
        "nan-loss",
        PatternAction::Wake,
        3,
        "loss NaN",
        200,
    )));
    assert!(TerminationPolicy.should_kill(&Observation::pattern(
        "project-a",
        "signature-a",
        "cuda-oom",
        PatternAction::Kill,
        1,
        "CUDA out of memory",
        200,
    )));
}

#[tokio::test]
async fn explicit_kill_policy_kills_only_the_matching_running_task_once() {
    let harness = Harness::running_task("project-a", 41);

    harness.observe_fatal_pattern("cuda-oom");
    harness.observe_fatal_pattern("cuda-oom");
    let outcomes = harness.run_termination_cycle().await;

    assert_eq!(outcomes, vec![TerminationOutcome::Confirmed]);
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);
    assert_eq!(harness.pending_event_count(EventKind::AutoKilled), 1);
    assert_eq!(
        harness.request_status(),
        Some(TerminationRequestStatus::Confirmed)
    );

    let second_outcomes = harness.run_termination_cycle().await;
    assert!(second_outcomes.is_empty());
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);
    assert_eq!(harness.pending_event_count(EventKind::AutoKilled), 1);
}

#[tokio::test]
async fn stalled_detection_does_not_kill_with_default_notify_policy() {
    let harness = Harness::stalled_task("project-a", 41);

    harness.observe_stalled_default_notify();
    let outcomes = harness.run_termination_cycle().await;

    assert!(outcomes.is_empty());
    assert!(harness.fake_pueue.kill_calls().is_empty());
}

#[tokio::test]
async fn termination_revalidates_full_task_signature_before_kill() {
    let harness = Harness::running_task("project-a", 41);
    harness.observe_fatal_pattern("cuda-oom");
    let changed_task = PueueTask {
        started_at: Some("999".to_owned()),
        ..harness.task.clone()
    };
    harness.fake_pueue.set_tasks(vec![changed_task]);

    let outcomes = harness.run_termination_cycle().await;

    assert_eq!(outcomes, vec![TerminationOutcome::AlreadyTerminal]);
    assert!(harness.fake_pueue.kill_calls().is_empty());
    assert_eq!(harness.pending_event_count(EventKind::AutoKilled), 0);
}

#[tokio::test]
async fn status_errors_are_not_treated_as_idle_or_safe_to_kill() {
    let harness = Harness::running_task("project-a", 41);
    harness.observe_fatal_pattern("cuda-oom");
    harness.fake_pueue.set_status_error("status unavailable");

    let error = TerminationManager::new(&harness.db, harness.fake_pueue.clone())
        .execute(harness.pending_request_ids()[0])
        .await
        .unwrap_err();

    assert!(error.to_string().contains("fake Pueue status"));
    assert!(harness.fake_pueue.kill_calls().is_empty());
    assert_eq!(
        harness.request_status(),
        Some(TerminationRequestStatus::Requested)
    );
}

#[tokio::test]
async fn failed_pueue_kill_stays_visible_without_auto_killed_event() {
    let harness = Harness::running_task("project-a", 41);
    harness.observe_fatal_pattern("cuda-oom");
    harness.fake_pueue.set_kill_error("kill failed");

    let outcomes = harness.run_termination_cycle().await;

    assert_eq!(outcomes, vec![TerminationOutcome::Failed]);
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);
    assert_eq!(harness.request_status(), Some(TerminationRequestStatus::Failed));
    assert_eq!(harness.pending_event_count(EventKind::TerminationFailed), 1);
    assert_eq!(harness.pending_event_count(EventKind::AutoKilled), 0);
}
