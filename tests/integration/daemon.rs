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
    daemon::{Daemon, DaemonConfig},
    db::{AgentRunRepository, Db, EventRepository, ProjectRepository},
    models::{AgentRunStatus, EventKind, EventStatus, NewEvent, NewProject},
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
                now_override: Some(self.now),
                shutdown_grace_period: Duration::from_secs(30),
            },
        )
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

    fn pause_project(&self) {
        ProjectRepository::new(&self.db)
            .pause("project-a", self.now)
            .unwrap();
    }

    fn claim_with_expired_lease(&self, event_id: i64) {
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE events SET status = 'claimed', lease_until = ?1 WHERE event_id = ?2",
                rusqlite::params![self.now - 1, event_id],
            )
            .unwrap();
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
    harness.pause_project();
    let event_id = harness.enqueue(
        EventKind::TaskFinished,
        "project-a",
        "finished-before-crash",
    );
    harness.claim_with_expired_lease(event_id);

    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();

    let event = EventRepository::new(&harness.db)
        .find_by_id(event_id)
        .unwrap()
        .unwrap();
    assert_eq!(event.status, EventStatus::Pending);
    assert_eq!(event.lease_until, None);
}
