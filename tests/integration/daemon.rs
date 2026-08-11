use std::{
    ffi::OsString,
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use pueue_agent::{
    agent::{AgentRunner, AgentRunnerConfig},
    daemon::{Daemon, DaemonConfig, DaemonReport},
    db::{AgentRunRepository, Db, EventRepository, InterventionRepository, ProjectRepository},
    interventions::InterventionStatus,
    models::{
        AgentContextMode, AgentRunStatus, EventKind, EventStatus, NewAgentRun, NewEvent, NewProject,
    },
    pueue::{PueueApi, PueueTask},
    AppError,
};
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct FakePueue {
    tasks: Arc<Mutex<Vec<PueueTask>>>,
    status_calls: Arc<Mutex<usize>>,
    kill_calls: Arc<Mutex<Vec<i64>>>,
    status_observed: Arc<Notify>,
}

impl FakePueue {
    fn with_tasks(tasks: Vec<PueueTask>) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(tasks)),
            status_calls: Arc::new(Mutex::new(0)),
            kill_calls: Arc::new(Mutex::new(Vec::new())),
            status_observed: Arc::new(Notify::new()),
        }
    }

    fn status_calls(&self) -> usize {
        *self.status_calls.lock().unwrap()
    }

    fn set_tasks(&self, tasks: Vec<PueueTask>) {
        *self.tasks.lock().unwrap() = tasks;
    }

    fn kill_calls(&self) -> Vec<i64> {
        self.kill_calls.lock().unwrap().clone()
    }

    async fn wait_for_status(&self) {
        self.status_observed.notified().await;
    }
}

#[async_trait]
impl PueueApi for FakePueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        *self.status_calls.lock().unwrap() += 1;
        self.status_observed.notify_waiters();
        Ok(self.tasks.lock().unwrap().clone())
    }

    async fn add(&self, _args: &[OsString]) -> Result<i64, AppError> {
        panic!("daemon loop must not submit Pueue tasks")
    }

    async fn kill(&self, task_id: i64) -> Result<(), AppError> {
        self.kill_calls.lock().unwrap().push(task_id);
        Ok(())
    }

    async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
        panic!("daemon loop must not remove Pueue tasks")
    }

    async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
        panic!("daemon loop must not provision Pueue groups")
    }
}

struct DaemonHarness {
    temp: TempDir,
    db: Db,
    fake_pueue: FakePueue,
    now: i64,
}

impl DaemonHarness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        let fake_pueue = FakePueue::with_tasks(vec![running_task()]);
        let harness = Self {
            temp,
            db,
            fake_pueue,
            now: 200,
        };
        harness.register_project("project-a", "pa-project", "/bin/echo");
        harness
    }

    fn root(&self, project_id: &str) -> PathBuf {
        self.temp.path().join(project_id)
    }

    fn register_project(&self, project_id: &str, group: &str, program: &str) {
        self.register_project_with_agent(project_id, group, program, &["{prompt}"], 1);
    }

    fn register_project_with_agent(
        &self,
        project_id: &str,
        group: &str,
        program: &str,
        args: &[&str],
        timeout_minutes: u32,
    ) {
        let root = self.root(project_id);
        fs::create_dir_all(root.join(".pueue-agent/logs")).unwrap();
        fs::write(
            root.join(".pueue-agent/logs/41.log"),
            "CUDA out of memory\n",
        )
        .unwrap();
        fs::write(root.join(".pueue-agent/STATE.md"), "state").unwrap();
        fs::write(root.join(".pueue-agent/instructions.md"), "instructions").unwrap();
        fs::write(
            root.join(".pueue-agent/config.toml"),
            format!(
                r#"
project_id = "{project_id}"
pueue_group = "{group}"

[agent]
program = "{program}"
args = [{args}]
timeout_minutes = {timeout_minutes}
max_retries = 1

[check]
interval_minutes = 10
deep_check_every = 6
deep_check_interval_minutes = 0
stall_minutes = 30
log_tail_bytes = 4096
extra_log_paths = []

[[check.patterns]]
name = "oom"
regex = "CUDA out of memory"
action = "kill"
confirm_matches = 1

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 3
max_experiments = 20
max_agent_runs = 10
"#,
                args = toml_string_array(args),
            ),
        )
        .unwrap();

        if ProjectRepository::new(&self.db)
            .find_by_id(project_id)
            .unwrap()
            .is_some()
        {
            return;
        }

        ProjectRepository::new(&self.db)
            .register(&NewProject::new(
                project_id,
                &root,
                group,
                root.join(".pueue-agent/config.toml"),
                self.now,
            ))
            .unwrap();
    }

    fn daemon(&self) -> Daemon<FakePueue> {
        self.daemon_at(self.now)
    }

    fn daemon_at(&self, now: i64) -> Daemon<FakePueue> {
        Daemon::new(
            self.db.clone(),
            self.fake_pueue.clone(),
            AgentRunner::new(AgentRunnerConfig::for_tests(
                self.temp.path().join("agent.log"),
            )),
            DaemonConfig {
                interval: Duration::from_millis(10),
                lease_seconds: 60,
                claim_limit: 100,
                now_override: Some(now),
                shutdown_grace_period: Duration::from_secs(30),
            },
        )
    }

    fn running_task_with_deep_check_interval(interval_minutes: u32) -> Self {
        let harness = Self::new();
        let config_path = harness.root("project-a").join(".pueue-agent/config.toml");
        let config = fs::read_to_string(&config_path).unwrap();
        fs::write(
            config_path,
            config.replace(
                "deep_check_interval_minutes = 0",
                &format!("deep_check_interval_minutes = {interval_minutes}"),
            ),
        )
        .unwrap();
        harness
    }

    async fn run_once_at(&self, now: i64) -> DaemonReport {
        self.daemon_at(now).run_once().await.unwrap()
    }

    fn enqueue(&self, kind: EventKind, project_id: &str, dedup_key: &str) -> i64 {
        EventRepository::new(&self.db)
            .insert_idempotent(&NewEvent::new(
                project_id,
                kind,
                dedup_key,
                json!({"source": "test"}),
                self.now,
                self.now,
            ))
            .unwrap()
            .event_id
    }

    fn event_status(&self, event_id: i64) -> EventStatus {
        EventRepository::new(&self.db)
            .find_by_id(event_id)
            .unwrap()
            .unwrap()
            .status
    }

    fn project_event_count(&self, kind: EventKind) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE project_id = ?1 AND kind = ?2",
                rusqlite::params!["project-a", kind],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn insert_active_agent_run(&self) {
        let event_id = self.enqueue(EventKind::TaskFailed, "project-a", "active-agent-run");
        self.insert_active_run("project-a", event_id, AgentRunStatus::Running);
    }

    fn observation_count(&self) -> i64 {
        self.count("task_observations")
    }

    fn incident_count(&self) -> i64 {
        self.count("incidents")
    }

    fn agent_run_count(&self) -> u32 {
        AgentRunRepository::new(&self.db)
            .count_by_project("project-a")
            .unwrap()
    }

    fn agent_run_statuses(&self) -> Vec<AgentRunStatus> {
        let connection = self.db.connect().unwrap();
        let mut statement = connection
            .prepare("SELECT status FROM agent_runs ORDER BY run_id")
            .unwrap();
        statement
            .query_map([], |row| row.get::<_, AgentRunStatus>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    async fn wait_for_active_agent(&self) {
        let repository = AgentRunRepository::new(&self.db);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if repository
                    .find_active_by_project("project-a")
                    .unwrap()
                    .is_some()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("agent should start");
    }

    fn count(&self, table: &str) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    fn pause_project(&self, project_id: &str) {
        ProjectRepository::new(&self.db)
            .pause(project_id, self.now)
            .unwrap();
    }

    fn claim_with_lease(&self, event_id: i64, lease_until: i64) {
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE events SET status = 'claimed', lease_until = ?1 WHERE event_id = ?2",
                rusqlite::params![lease_until, event_id],
            )
            .unwrap();
    }

    fn insert_active_run(
        &self,
        project_id: &str,
        primary_event_id: i64,
        status: AgentRunStatus,
    ) -> i64 {
        AgentRunRepository::new(&self.db)
            .insert(&NewAgentRun::with_context(
                project_id,
                primary_event_id,
                (status == AgentRunStatus::Running).then_some(42_424),
                status,
                self.now - 10,
                self.temp
                    .path()
                    .join(format!("{project_id}-interrupted.log")),
                AgentContextMode::Fresh,
                None,
                vec![primary_event_id.to_string()],
            ))
            .unwrap()
            .run_id
    }

    fn agent_run_state(&self, run_id: i64) -> (AgentRunStatus, Option<i64>, Option<String>) {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status, finished_at, last_error FROM agent_runs WHERE run_id = ?1",
                [run_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    }

    fn reserve_intervention(&self, message: &str, token: &str, lease_expires_at: i64) -> String {
        let repository = InterventionRepository::new(&self.db);
        let intervention_id = repository
            .insert_pending("project-a", message, self.now - 20)
            .unwrap()
            .intervention_id;
        repository
            .reserve_pending(
                "project-a",
                token,
                self.now - 10,
                lease_expires_at,
                1,
                message.len(),
            )
            .unwrap();
        intervention_id
    }

    fn attach_intervention(&self, intervention_id: &str, run_id: i64) {
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE interventions SET agent_run_id = ?1
                 WHERE project_id = 'project-a' AND intervention_id = ?2",
                rusqlite::params![run_id, intervention_id],
            )
            .unwrap();
    }

    fn intervention_state(&self, intervention_id: &str) -> (InterventionStatus, Option<i64>) {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status, agent_run_id FROM interventions WHERE intervention_id = ?1",
                [intervention_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }
}

fn toml_string_array(values: &[&str]) -> String {
    values
        .iter()
        .map(|value| format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(", ")
}

fn running_task() -> PueueTask {
    PueueTask {
        id: 41,
        group: "pa-project".to_owned(),
        command: "python train.py".to_owned(),
        state: "Running".to_owned(),
        enqueued_at: Some("100".to_owned()),
        started_at: Some("101".to_owned()),
        ended_at: None,
        result: None,
    }
}

#[tokio::test]
async fn daemon_run_once_invokes_reconciliation_detection_termination_and_scheduler() {
    let harness = DaemonHarness::new();
    let scheduled = harness.enqueue(EventKind::DeepCheck, "project-a", "deep-check");

    let mut daemon = harness.daemon();
    let report = daemon.run_once().await.unwrap();

    assert_eq!(report.reconciliation.observed_task_count, 1);
    assert_eq!(harness.fake_pueue.status_calls(), 2);
    assert_eq!(harness.observation_count(), 1);
    assert_eq!(harness.incident_count(), 1);
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);
    assert_eq!(harness.agent_run_count(), 1);
    assert_eq!(harness.event_status(scheduled), EventStatus::Completed);
}

#[tokio::test]
async fn daemon_schedules_deep_check_after_interval_for_running_task() {
    let harness = DaemonHarness::running_task_with_deep_check_interval(30);
    let report = harness.run_once_at(3_700).await;

    assert_eq!(report.scheduled_deep_checks, 1);
    assert_eq!(harness.project_event_count(EventKind::DeepCheck), 1);
}

#[tokio::test]
async fn daemon_does_not_schedule_deep_check_while_agent_is_active() {
    let harness = DaemonHarness::running_task_with_deep_check_interval(30);
    harness.fake_pueue.set_tasks(Vec::new());
    let mut daemon = harness.daemon_at(3_700);
    daemon.run_once().await.unwrap();
    harness.fake_pueue.set_tasks(vec![running_task()]);
    harness.insert_active_agent_run();
    let report = daemon.run_once().await.unwrap();

    assert_eq!(report.scheduled_deep_checks, 0);
    assert_eq!(harness.project_event_count(EventKind::DeepCheck), 0);
}

#[tokio::test]
async fn daemon_shutdown_is_graceful() {
    let harness = DaemonHarness::new();
    let mut daemon = harness.daemon();
    let shutdown = CancellationToken::new();
    let join = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { daemon.run(shutdown).await }
    });

    harness.fake_pueue.wait_for_status().await;
    shutdown.cancel();
    let result = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .expect("daemon should stop within the graceful shutdown timeout")
        .expect("daemon task should not panic");

    result.unwrap();
}

#[tokio::test]
async fn daemon_shutdown_drains_child_agent_that_finishes_promptly() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 0.1"],
        1,
    );
    harness.enqueue(EventKind::DeepCheck, "project-a", "deep-check");
    let mut daemon = harness.daemon();
    let shutdown = CancellationToken::new();
    let join = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { daemon.run(shutdown).await }
    });

    harness.wait_for_active_agent().await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("daemon should drain promptly")
        .expect("daemon task should not panic")
        .unwrap();

    assert_eq!(
        harness.agent_run_statuses(),
        vec![AgentRunStatus::Completed]
    );
    assert!(AgentRunRepository::new(&harness.db)
        .find_active_by_project("project-a")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn daemon_shutdown_bounds_long_running_child_agent_and_marks_it_terminal() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 10"],
        1,
    );
    harness.enqueue(EventKind::DeepCheck, "project-a", "deep-check");
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        AgentRunner::new(AgentRunnerConfig::for_tests(
            harness.temp.path().join("agent.log"),
        )),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_millis(100),
        },
    );
    let shutdown = CancellationToken::new();
    let join = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { daemon.run(shutdown).await }
    });

    harness.wait_for_active_agent().await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("daemon should not hang indefinitely on a long-running child")
        .expect("daemon task should not panic")
        .unwrap();

    assert_eq!(harness.agent_run_statuses(), vec![AgentRunStatus::TimedOut]);
    assert!(AgentRunRepository::new(&harness.db)
        .find_active_by_project("project-a")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn injected_shutdown_signal_cancels_daemon_token() {
    let shutdown = CancellationToken::new();
    let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(pueue_agent::daemon::cancel_token_on_shutdown_signal(
        shutdown.clone(),
        async move {
            let _ = receiver.await;
        },
    ));

    assert!(!shutdown.is_cancelled());
    sender.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), shutdown.cancelled())
        .await
        .expect("injected signal should cancel token");
    task.await.unwrap();
}

#[tokio::test]
async fn daemon_restart_recovers_expired_claims_without_requiring_a_new_callback() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(
        EventKind::TaskFinished,
        "project-a",
        "finished-before-crash",
    );
    harness.claim_with_lease(event_id, harness.now - 1);

    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();

    let event = EventRepository::new(&harness.db)
        .find_by_id(event_id)
        .unwrap()
        .unwrap();
    assert_eq!(event.status, EventStatus::Pending);
    assert_eq!(event.lease_until, None);
}

#[tokio::test]
async fn intervention_recovery_returns_an_expired_unattached_reservation_to_pending() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let intervention_id =
        harness.reserve_intervention("expired instruction", "expired-token", harness.now);

    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();

    assert_eq!(
        harness.intervention_state(&intervention_id),
        (InterventionStatus::Pending, None)
    );
}

#[tokio::test]
async fn later_daemon_tick_recovers_unattached_intervention_after_startup_recovery() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let intervention_id =
        harness.reserve_intervention("expires after startup", "later-token", harness.now + 1);

    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();
    assert_eq!(
        harness.intervention_state(&intervention_id),
        (InterventionStatus::Reserved, None)
    );

    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE interventions SET lease_expires_at = ?1 WHERE intervention_id = ?2",
            rusqlite::params![harness.now - 1, intervention_id],
        )
        .unwrap();

    daemon.run_once().await.unwrap();

    assert_eq!(
        harness.intervention_state(&intervention_id),
        (InterventionStatus::Pending, None)
    );
}

#[tokio::test]
async fn intervention_recovery_applies_a_reserved_row_attached_to_a_run_with_a_pid_once() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "delivered-before-crash");
    let run_id = harness.insert_active_run("project-a", event_id, AgentRunStatus::Running);
    let intervention_id =
        harness.reserve_intervention("already delivered", "live-token", harness.now + 600);
    harness.attach_intervention(&intervention_id, run_id);

    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();
    daemon.run_once().await.unwrap();
    let mut restarted_daemon = harness.daemon();
    restarted_daemon.run_once().await.unwrap();

    assert_eq!(
        harness.intervention_state(&intervention_id),
        (InterventionStatus::Applied, Some(run_id))
    );
}

#[tokio::test]
async fn intervention_recovery_releases_a_reserved_row_attached_to_a_failed_pre_spawn_run() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "failed-before-spawn");
    let run_id = harness.insert_active_run("project-a", event_id, AgentRunStatus::Starting);
    let intervention_id =
        harness.reserve_intervention("not delivered", "failed-token", harness.now + 600);
    harness.attach_intervention(&intervention_id, run_id);
    AgentRunRepository::new(&harness.db)
        .finish(
            run_id,
            AgentRunStatus::Failed,
            harness.now - 5,
            None,
            Some("spawn failed"),
        )
        .unwrap();

    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();

    assert_eq!(
        harness.intervention_state(&intervention_id),
        (InterventionStatus::Pending, None)
    );
}

#[tokio::test]
async fn first_daemon_cycle_recovers_persisted_runs_and_only_their_claimed_events_once() {
    let harness = DaemonHarness::new();
    harness.register_project("project-b", "pa-project-b", "/bin/echo");
    harness.pause_project("project-a");
    harness.pause_project("project-b");

    let starting_event = harness.enqueue(EventKind::TaskFailed, "project-a", "starting-event");
    let running_event = harness.enqueue(EventKind::TaskFailed, "project-b", "running-event");
    let completed_event = harness.enqueue(EventKind::TaskFinished, "project-a", "completed-event");
    let unrelated_claim = harness.enqueue(EventKind::DeepCheck, "project-a", "unrelated-claim");
    for event_id in [starting_event, running_event, unrelated_claim] {
        harness.claim_with_lease(event_id, harness.now + 600);
    }
    EventRepository::new(&harness.db)
        .transition_many(
            &[completed_event],
            EventStatus::Completed,
            harness.now - 5,
            None,
            None,
        )
        .unwrap();

    let starting_run =
        harness.insert_active_run("project-a", starting_event, AgentRunStatus::Starting);
    let running_run =
        harness.insert_active_run("project-b", running_event, AgentRunStatus::Running);
    let runs = AgentRunRepository::new(&harness.db);
    runs.attach_event(starting_run, starting_event).unwrap();
    runs.attach_event(starting_run, completed_event).unwrap();
    runs.attach_event(running_run, running_event).unwrap();

    let mut daemon = harness.daemon();
    let first = daemon.run_once().await.unwrap();
    let second = daemon.run_once().await.unwrap();
    let mut restarted_daemon = harness.daemon();
    let repeated_recovery = restarted_daemon.run_once().await.unwrap();

    assert_eq!(first.recovered_agent_runs, 2);
    assert_eq!(first.requeued_agent_events, 2);
    assert_eq!(second.recovered_agent_runs, 0);
    assert_eq!(second.requeued_agent_events, 0);
    assert_eq!(repeated_recovery.recovered_agent_runs, 0);
    assert_eq!(repeated_recovery.requeued_agent_events, 0);
    for run_id in [starting_run, running_run] {
        let (status, finished_at, reason) = harness.agent_run_state(run_id);
        assert_eq!(status, AgentRunStatus::Failed);
        assert_eq!(finished_at, Some(harness.now));
        assert!(reason
            .as_deref()
            .is_some_and(|reason| reason.contains("daemon restart")));
    }
    for event_id in [starting_event, running_event] {
        let event = EventRepository::new(&harness.db)
            .find_by_id(event_id)
            .unwrap()
            .unwrap();
        assert_eq!(event.status, EventStatus::Pending);
        assert_eq!(event.lease_until, None);
    }
    assert_eq!(
        harness.event_status(completed_event),
        EventStatus::Completed
    );
    let unrelated = EventRepository::new(&harness.db)
        .find_by_id(unrelated_claim)
        .unwrap()
        .unwrap();
    assert_eq!(unrelated.status, EventStatus::Claimed);
    assert_eq!(unrelated.lease_until, Some(harness.now + 600));
}

#[tokio::test]
async fn startup_recovery_is_atomic_and_retried_after_a_database_failure() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "atomic-recovery");
    harness.claim_with_lease(event_id, harness.now + 600);
    let run_id = harness.insert_active_run("project-a", event_id, AgentRunStatus::Starting);
    AgentRunRepository::new(&harness.db)
        .attach_event(run_id, event_id)
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_restart_recovery
             BEFORE UPDATE OF status ON agent_runs
             WHEN OLD.status IN ('starting', 'running') AND NEW.status = 'failed'
             BEGIN
                 SELECT RAISE(ABORT, 'injected recovery failure');
             END;",
        )
        .unwrap();

    let mut daemon = harness.daemon();
    assert!(daemon.run_once().await.is_err());
    let event = EventRepository::new(&harness.db)
        .find_by_id(event_id)
        .unwrap()
        .unwrap();
    assert_eq!(event.status, EventStatus::Claimed);
    assert_eq!(event.lease_until, Some(harness.now + 600));
    assert_eq!(harness.agent_run_state(run_id).0, AgentRunStatus::Starting);

    harness
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_restart_recovery;")
        .unwrap();
    let report = daemon.run_once().await.unwrap();

    assert_eq!(report.recovered_agent_runs, 1);
    assert_eq!(report.requeued_agent_events, 1);
    assert_eq!(harness.event_status(event_id), EventStatus::Pending);
    assert_eq!(harness.agent_run_state(run_id).0, AgentRunStatus::Failed);
}

#[tokio::test]
async fn startup_recovery_runs_before_the_first_scheduling_pass() {
    let harness = DaemonHarness::new();
    let event_id = harness.enqueue(EventKind::TaskFinished, "project-a", "restart-dispatch");
    harness.claim_with_lease(event_id, harness.now + 600);
    let interrupted = harness.insert_active_run("project-a", event_id, AgentRunStatus::Running);
    AgentRunRepository::new(&harness.db)
        .attach_event(interrupted, event_id)
        .unwrap();

    let mut daemon = harness.daemon();
    let report = daemon.run_once().await.unwrap();

    assert_eq!(report.recovered_agent_runs, 1);
    assert_eq!(report.requeued_agent_events, 1);
    assert_eq!(harness.agent_run_count(), 2);
    assert_eq!(
        harness.agent_run_state(interrupted).0,
        AgentRunStatus::Failed
    );
    assert_eq!(harness.event_status(event_id), EventStatus::Completed);
}
