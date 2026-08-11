use std::{
    fs,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
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
    termination::{
        confirm_auto_kill_terminal_observation, TerminationManager, TerminationOutcome,
        TerminationPolicy, DEFAULT_CONFIRMATION_GRACE_SECONDS,
    },
    AppError,
};
use tempfile::TempDir;
use tokio::sync::Semaphore;

#[derive(Clone)]
struct FakePueue {
    tasks: Arc<Mutex<Result<Vec<PueueTask>, String>>>,
    kill_calls: Arc<Mutex<Vec<i64>>>,
    kill_error: Arc<Mutex<Option<String>>>,
    mark_killed_on_kill: Arc<Mutex<bool>>,
    block_kill: Arc<Mutex<bool>>,
    kill_started: Arc<Semaphore>,
    release_kill: Arc<Semaphore>,
}

impl FakePueue {
    fn with_tasks(tasks: Vec<PueueTask>) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(Ok(tasks))),
            kill_calls: Arc::new(Mutex::new(Vec::new())),
            kill_error: Arc::new(Mutex::new(None)),
            mark_killed_on_kill: Arc::new(Mutex::new(true)),
            block_kill: Arc::new(Mutex::new(false)),
            kill_started: Arc::new(Semaphore::new(0)),
            release_kill: Arc::new(Semaphore::new(0)),
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

    fn keep_running_after_kill(&self) {
        *self.mark_killed_on_kill.lock().unwrap() = false;
    }

    fn block_next_kill(&self) {
        *self.block_kill.lock().unwrap() = true;
    }

    async fn wait_until_kill_started(&self) {
        let permit = self.kill_started.acquire().await.unwrap();
        permit.forget();
    }

    fn release_blocked_kill(&self) {
        self.release_kill.add_permits(1);
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
        if *self.block_kill.lock().unwrap() {
            self.kill_started.add_permits(1);
            let permit = self.release_kill.acquire().await.unwrap();
            permit.forget();
        }
        if self.kill_error.lock().unwrap().is_some() {
            return Err(AppError::Runtime {
                operation: "fake Pueue kill",
            });
        }
        if !*self.mark_killed_on_kill.lock().unwrap() {
            return Ok(());
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

    async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
        panic!("termination tests must not remove Pueue tasks")
    }

    async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
        panic!("termination tests must not provision Pueue groups")
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
        Self::running_task_with_times(project_id, task_id, Some("100"), Some("101"))
    }

    fn running_task_without_lifecycle_timestamps(project_id: &str, task_id: i64) -> Self {
        Self::running_task_with_times(project_id, task_id, None, None)
    }

    fn running_task_with_times(
        project_id: &str,
        task_id: i64,
        enqueued_at: Option<&str>,
        started_at: Option<&str>,
    ) -> Self {
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
            enqueued_at: enqueued_at.map(str::to_owned),
            started_at: started_at.map(str::to_owned),
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

    fn make_pending_request_sent(&self, grace_until: Option<i64>) -> i64 {
        let request_id = self.pending_request_ids()[0];
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE termination_requests
                 SET status = ?1, grace_until = ?2
                 WHERE request_id = ?3",
                rusqlite::params![TerminationRequestStatus::Sent, grace_until, request_id],
            )
            .unwrap();
        request_id
    }

    fn make_pending_request_dispatching(&self) -> i64 {
        let request_id = self.pending_request_ids()[0];
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE termination_requests
                 SET status = 'dispatching', grace_until = NULL
                 WHERE request_id = ?1",
                [request_id],
            )
            .unwrap();
        request_id
    }

    fn set_request_grace_until(&self, request_id: i64, grace_until: i64) {
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE termination_requests
                 SET grace_until = ?1
                 WHERE request_id = ?2",
                rusqlite::params![grace_until, request_id],
            )
            .unwrap();
    }

    fn set_request_result(
        &self,
        request_id: i64,
        status: TerminationRequestStatus,
        confirmed_at: Option<i64>,
        last_error: Option<&str>,
    ) {
        TerminationRequestRepository::new(&self.db)
            .update_result(request_id, status, confirmed_at, last_error)
            .unwrap();
    }
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .try_into()
        .unwrap()
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

    assert_eq!(outcomes, vec![TerminationOutcome::PendingConfirmation]);
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
    assert_eq!(harness.pending_event_count(EventKind::TaskFailed), 0);
}

#[tokio::test]
async fn duplicate_execute_on_sent_request_does_not_kill_again() {
    let harness = Harness::running_task("project-a", 41);
    harness.fake_pueue.keep_running_after_kill();
    harness.observe_fatal_pattern("cuda-oom");
    let request_id = harness.pending_request_ids()[0];

    let first = TerminationManager::new(&harness.db, harness.fake_pueue.clone())
        .execute(request_id)
        .await
        .unwrap();
    let second = TerminationManager::new(&harness.db, harness.fake_pueue.clone())
        .execute(request_id)
        .await
        .unwrap();

    assert_eq!(first, TerminationOutcome::PendingConfirmation);
    assert_eq!(second, TerminationOutcome::PendingConfirmation);
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);
    assert_eq!(
        harness.request_status(),
        Some(TerminationRequestStatus::Sent)
    );
}

#[tokio::test]
async fn successful_kill_with_running_task_starts_confirmation_grace_at_dispatch() {
    let harness = Harness::running_task("project-a", 41);
    harness.fake_pueue.keep_running_after_kill();
    harness.observe_fatal_pattern("cuda-oom");
    let request_id = harness.pending_request_ids()[0];
    let requested = TerminationRequestRepository::new(&harness.db)
        .find_by_id(request_id)
        .unwrap()
        .unwrap();

    assert_eq!(requested.requested_at, 200);
    assert_eq!(requested.grace_until, None);

    let before_dispatch = unix_timestamp();
    let first = TerminationManager::new(&harness.db, harness.fake_pueue.clone())
        .execute(request_id)
        .await
        .unwrap();
    let after_dispatch = unix_timestamp();
    let request = TerminationRequestRepository::new(&harness.db)
        .find_by_id(request_id)
        .unwrap()
        .unwrap();

    assert_eq!(first, TerminationOutcome::PendingConfirmation);
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);
    assert_eq!(
        harness.request_status(),
        Some(TerminationRequestStatus::Sent)
    );
    assert!(request.grace_until.is_some_and(|grace_until| {
        grace_until >= before_dispatch + DEFAULT_CONFIRMATION_GRACE_SECONDS
            && grace_until <= after_dispatch + DEFAULT_CONFIRMATION_GRACE_SECONDS
    }));
}

#[tokio::test]
async fn sent_request_without_persisted_grace_recovers_to_pending_without_second_kill() {
    let harness = Harness::running_task("project-a", 41);
    harness.observe_fatal_pattern("cuda-oom");
    let request_id = harness.make_pending_request_sent(None);
    let before_recovery = unix_timestamp();

    let outcome = TerminationManager::new(&harness.db, harness.fake_pueue.clone())
        .execute(request_id)
        .await
        .unwrap();
    let after_recovery = unix_timestamp();
    let request = TerminationRequestRepository::new(&harness.db)
        .find_by_id(request_id)
        .unwrap()
        .unwrap();

    assert_eq!(outcome, TerminationOutcome::PendingConfirmation);
    assert!(harness.fake_pueue.kill_calls().is_empty());
    assert_eq!(request.status, TerminationRequestStatus::Sent);
    assert!(request.grace_until.is_some_and(|grace_until| {
        grace_until >= before_recovery + DEFAULT_CONFIRMATION_GRACE_SECONDS
            && grace_until <= after_recovery + DEFAULT_CONFIRMATION_GRACE_SECONDS
    }));
}

#[tokio::test]
async fn dispatching_request_retries_kill_after_restart_before_dispatch() {
    let harness = Harness::running_task("project-a", 41);
    harness.observe_fatal_pattern("cuda-oom");
    let request_id = harness.make_pending_request_dispatching();

    let outcome = TerminationManager::new(&harness.db, harness.fake_pueue.clone())
        .execute(request_id)
        .await
        .unwrap();

    assert_eq!(outcome, TerminationOutcome::PendingConfirmation);
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);
    assert_eq!(
        harness.request_status(),
        Some(TerminationRequestStatus::Sent)
    );
    let request = TerminationRequestRepository::new(&harness.db)
        .find_by_id(request_id)
        .unwrap()
        .unwrap();
    assert!(request.grace_until.is_some());
}

#[tokio::test]
async fn active_dispatching_claim_prevents_duplicate_kill() {
    let harness = Harness::running_task("project-a", 41);
    harness.observe_fatal_pattern("cuda-oom");
    harness.fake_pueue.block_next_kill();
    let request_id = harness.pending_request_ids()[0];
    let db = harness.db.clone();
    let fake_pueue = harness.fake_pueue.clone();
    let first = tokio::spawn(async move {
        TerminationManager::new(&db, fake_pueue)
            .execute(request_id)
            .await
            .unwrap()
    });
    harness.fake_pueue.wait_until_kill_started().await;

    let second = tokio::time::timeout(
        Duration::from_secs(1),
        TerminationManager::new(&harness.db, harness.fake_pueue.clone()).execute(request_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(second, TerminationOutcome::PendingConfirmation);

    harness.fake_pueue.release_blocked_kill();
    assert_eq!(
        first.await.unwrap(),
        TerminationOutcome::PendingConfirmation
    );
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);
}

#[tokio::test]
async fn explicit_kill_times_out_after_default_confirmation_grace_without_second_kill() {
    let harness = Harness::running_task("project-a", 41);
    harness.fake_pueue.keep_running_after_kill();
    harness.observe_fatal_pattern("cuda-oom");
    let request_id = harness.pending_request_ids()[0];

    let first_outcomes = harness.run_termination_cycle().await;

    assert_eq!(
        first_outcomes,
        vec![TerminationOutcome::PendingConfirmation]
    );
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);
    assert_eq!(
        harness.request_status(),
        Some(TerminationRequestStatus::Sent)
    );

    harness.set_request_grace_until(request_id, 0);
    let timed_out_outcomes = harness.run_termination_cycle().await;
    let final_outcomes = harness.run_termination_cycle().await;

    assert_eq!(timed_out_outcomes, vec![TerminationOutcome::TimedOut]);
    assert!(final_outcomes.is_empty());
    assert_eq!(
        harness.request_status(),
        Some(TerminationRequestStatus::TimedOut)
    );
    assert_eq!(harness.pending_event_count(EventKind::TerminationFailed), 1);
    assert_eq!(harness.pending_event_count(EventKind::AutoKilled), 0);
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);
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
async fn already_terminal_without_sent_kill_does_not_emit_auto_killed() {
    let harness = Harness::running_task("project-a", 41);
    harness.observe_fatal_pattern("cuda-oom");
    let terminal_task = PueueTask {
        state: "Killed".to_owned(),
        ended_at: Some("201".to_owned()),
        ..harness.task.clone()
    };
    harness.fake_pueue.set_tasks(vec![terminal_task]);

    let outcomes = harness.run_termination_cycle().await;

    assert_eq!(outcomes, vec![TerminationOutcome::AlreadyTerminal]);
    assert!(harness.fake_pueue.kill_calls().is_empty());
    assert_eq!(harness.pending_event_count(EventKind::AutoKilled), 0);
    assert_eq!(harness.pending_event_count(EventKind::TaskFailed), 1);
}

#[tokio::test]
async fn dispatching_terminal_task_is_not_marked_auto_killed_before_retry() {
    let harness = Harness::running_task("project-a", 41);
    harness.observe_fatal_pattern("cuda-oom");
    let request_id = harness.make_pending_request_dispatching();
    let terminal_task = PueueTask {
        state: "Finished".to_owned(),
        ended_at: Some("201".to_owned()),
        ..harness.task.clone()
    };
    harness.fake_pueue.set_tasks(vec![terminal_task]);

    Reconciler::new(&harness.db, harness.fake_pueue.clone())
        .run_once()
        .await
        .unwrap();

    assert_eq!(harness.pending_event_count(EventKind::AutoKilled), 0);
    assert_eq!(harness.pending_event_count(EventKind::TaskFinished), 1);
    assert_eq!(
        TerminationRequestRepository::new(&harness.db)
            .find_by_id(request_id)
            .unwrap()
            .unwrap()
            .status,
        TerminationRequestStatus::Dispatching
    );
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
async fn sent_request_past_grace_until_times_out_without_second_kill() {
    let harness = Harness::running_task("project-a", 41);
    harness.observe_fatal_pattern("cuda-oom");
    let request_id = harness.make_pending_request_sent(Some(0));

    let outcome = TerminationManager::new(&harness.db, harness.fake_pueue.clone())
        .execute(request_id)
        .await
        .unwrap();

    assert_eq!(outcome, TerminationOutcome::TimedOut);
    assert!(harness.fake_pueue.kill_calls().is_empty());
    assert_eq!(
        harness.request_status(),
        Some(TerminationRequestStatus::TimedOut)
    );
    assert_eq!(harness.pending_event_count(EventKind::TerminationFailed), 1);
    assert_eq!(harness.pending_event_count(EventKind::AutoKilled), 0);
}

#[test]
fn terminal_confirmation_does_not_overwrite_concurrent_timeout() {
    let harness = Harness::running_task("project-a", 41);
    harness.observe_fatal_pattern("cuda-oom");
    let request_id = harness.make_pending_request_sent(None);
    harness.set_request_result(
        request_id,
        TerminationRequestStatus::TimedOut,
        None,
        Some("Pueue kill was not confirmed before grace timeout"),
    );

    confirm_auto_kill_terminal_observation(&harness.db, request_id, 300).unwrap();

    assert_eq!(
        harness.request_status(),
        Some(TerminationRequestStatus::TimedOut)
    );
    assert_eq!(harness.pending_event_count(EventKind::AutoKilled), 0);
}

#[tokio::test]
async fn terminal_fallback_does_not_match_reused_id_without_lifecycle_timestamps() {
    let harness = Harness::running_task_without_lifecycle_timestamps("project-a", 41);
    harness.observe_fatal_pattern("cuda-oom");
    harness.make_pending_request_sent(None);
    let terminal_reused_id = PueueTask {
        state: "Killed".to_owned(),
        ended_at: Some("201".to_owned()),
        ..harness.task.clone()
    };
    harness.fake_pueue.set_tasks(vec![terminal_reused_id]);

    Reconciler::new(&harness.db, harness.fake_pueue.clone())
        .run_once()
        .await
        .unwrap();

    assert_eq!(harness.pending_event_count(EventKind::AutoKilled), 0);
    assert_eq!(harness.pending_event_count(EventKind::TaskFailed), 1);
    assert_eq!(
        harness.request_status(),
        Some(TerminationRequestStatus::Sent)
    );
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
    assert_eq!(
        harness.request_status(),
        Some(TerminationRequestStatus::Failed)
    );
    assert_eq!(harness.pending_event_count(EventKind::TerminationFailed), 1);
    assert_eq!(harness.pending_event_count(EventKind::AutoKilled), 0);
}

#[tokio::test]
async fn kill_error_does_not_overwrite_concurrent_auto_kill_confirmation() {
    let harness = Harness::running_task("project-a", 41);
    harness.observe_fatal_pattern("cuda-oom");
    harness.fake_pueue.block_next_kill();
    let request_id = harness.pending_request_ids()[0];
    let db = harness.db.clone();
    let fake_pueue = harness.fake_pueue.clone();
    let execute = tokio::spawn(async move {
        TerminationManager::new(&db, fake_pueue)
            .execute(request_id)
            .await
            .unwrap()
    });
    harness.fake_pueue.wait_until_kill_started().await;
    harness.make_pending_request_sent(Some(i64::MAX));

    let terminal_task = PueueTask {
        state: "Killed".to_owned(),
        ended_at: Some("201".to_owned()),
        ..harness.task.clone()
    };
    harness.fake_pueue.set_tasks(vec![terminal_task]);
    Reconciler::new(&harness.db, harness.fake_pueue.clone())
        .run_once()
        .await
        .unwrap();
    harness.fake_pueue.set_kill_error("late kill failure");
    harness.fake_pueue.release_blocked_kill();

    let outcome = execute.await.unwrap();

    assert_eq!(outcome, TerminationOutcome::Confirmed);
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);
    assert_eq!(
        harness.request_status(),
        Some(TerminationRequestStatus::Confirmed)
    );
    assert_eq!(harness.pending_event_count(EventKind::AutoKilled), 1);
    assert_eq!(harness.pending_event_count(EventKind::TerminationFailed), 0);
}

#[tokio::test]
async fn kill_error_does_not_overwrite_concurrent_timeout() {
    let harness = Harness::running_task("project-a", 41);
    harness.observe_fatal_pattern("cuda-oom");
    harness.fake_pueue.block_next_kill();
    let request_id = harness.pending_request_ids()[0];
    let db = harness.db.clone();
    let fake_pueue = harness.fake_pueue.clone();
    let execute = tokio::spawn(async move {
        TerminationManager::new(&db, fake_pueue)
            .execute(request_id)
            .await
            .unwrap()
    });
    harness.fake_pueue.wait_until_kill_started().await;

    harness.make_pending_request_sent(Some(0));
    let timeout_outcome = TerminationManager::new(&harness.db, harness.fake_pueue.clone())
        .execute(request_id)
        .await
        .unwrap();
    harness.fake_pueue.set_kill_error("late kill failure");
    harness.fake_pueue.release_blocked_kill();

    let late_kill_outcome = execute.await.unwrap();

    assert_eq!(timeout_outcome, TerminationOutcome::TimedOut);
    assert_eq!(late_kill_outcome, TerminationOutcome::TimedOut);
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);
    assert_eq!(
        harness.request_status(),
        Some(TerminationRequestStatus::TimedOut)
    );
    assert_eq!(harness.pending_event_count(EventKind::TerminationFailed), 1);
    assert_eq!(harness.pending_event_count(EventKind::AutoKilled), 0);
}
