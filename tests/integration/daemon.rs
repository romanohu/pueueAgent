use std::{
    ffi::OsString,
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
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

#[cfg(unix)]
use std::os::unix::fs::{symlink, PermissionsExt};

#[cfg(unix)]
#[path = "../support/execution_policy_fixture.rs"]
mod execution_policy_fixture;

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

fn accepts_api<P: PueueApi>(_api: &P) {}

#[test]
fn daemon_fake_preserves_the_pueue_api_contract() {
    accepts_api(&FakePueue::with_tasks(Vec::new()));
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

    fn registered_root(&self, project_id: &str) -> PathBuf {
        ProjectRepository::new(&self.db)
            .find_by_id(project_id)
            .unwrap()
            .unwrap()
            .root_path
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

    fn runner(&self) -> AgentRunner {
        AgentRunner::new(
            AgentRunnerConfig::production()
                .with_codex_capabilities(pueue_agent::codex_command::CodexCapabilities::all()),
            self.policy(),
        )
    }

    fn policy(&self) -> Arc<pueue_agent::execution_policy::ResolvedExecutionPolicy> {
        let projects = ProjectRepository::new(&self.db).list_all().unwrap();
        let owned = projects
            .iter()
            .map(|project| {
                (
                    project.project_id.clone(),
                    project.root_path.clone(),
                    execution_policy_fixture::prepare_configured_program(
                        self.temp.path(),
                        &project.project_id,
                        &project.config_path,
                    ),
                )
            })
            .collect::<Vec<_>>();
        let borrowed = owned
            .iter()
            .map(|(project_id, root, program)| (project_id.as_str(), root.as_path(), program.as_path()))
            .collect::<Vec<_>>();
        execution_policy_fixture::resolved_policy(self.temp.path(), &borrowed)
    }

    fn daemon_at(&self, now: i64) -> Daemon<FakePueue> {
        Daemon::new(
            self.db.clone(),
            self.fake_pueue.clone(),
            self.policy(),
            self.runner(),
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

    #[cfg(unix)]
    async fn wait_for_native_dispatch(&self, event_id: i64) {
        // Native lifecycle readiness is production-bounded at 30 seconds. Give
        // the scheduler and SQLite status observation a small margin without
        // including this setup in the shutdown-deadline measurement below.
        let setup_timeout = Duration::from_secs(35);
        if tokio::time::timeout(setup_timeout, async {
            while self.event_status(event_id) != EventStatus::Dispatched {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_err()
        {
            let event_status = self.event_status(event_id);
            let run = AgentRunRepository::new(&self.db)
                .find_active_by_project("project-a")
                .unwrap();
            panic!(
                "native dispatch setup exceeded {setup_timeout:?}: event_status={event_status:?}, run_status={:?}, launch_gate_state={:?}",
                run.as_ref().map(|run| &run.status),
                run.as_ref().map(|run| run.launch_gate_state.as_str()),
            );
        }
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
                self.registered_root(project_id).join(format!(
                    ".pueue-agent/logs/agent-190-{primary_event_id}.log"
                )),
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

#[cfg(unix)]
fn process_exists(pid: i32) -> bool {
    unsafe extern "C" {
        fn kill(pid: std::os::raw::c_int, signal: std::os::raw::c_int) -> std::os::raw::c_int;
    }
    unsafe { kill(pid, 0) == 0 }
}

#[cfg(unix)]
fn create_cleanup_depth_overflow(run_temp: &PathBuf) -> PathBuf {
    let mut nested = run_temp.clone();
    for index in 0..=pueue_agent::environment::MAX_PRIVATE_TEMP_CLEANUP_DEPTH + 1 {
        nested.push(format!("cleanup-level-{index}"));
        fs::create_dir(&nested).unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::write(nested.join("retained-leaf"), b"owned").unwrap();
    nested.parent().unwrap().to_path_buf()
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn startup_temp_inventory_rejects_symlink_weak_and_over_limit_without_mutation() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    {
        let harness = DaemonHarness::new();
        harness.register_project_with_agent(
            "project-b",
            "pb-project",
            "/bin/sh",
            &["-c", "sleep 1"],
            1,
        );
        let tmp = harness.root("project-a").join(".pueue-agent/tmp");
        fs::create_dir_all(&tmp).unwrap();
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o700)).unwrap();
        let outside = harness.temp.path().join("outside-retained");
        fs::create_dir(&outside).unwrap();
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o700)).unwrap();
        let symlinked = tmp.join("999");
        symlink(&outside, &symlinked).unwrap();
        let symlink_event =
            harness.enqueue(EventKind::TaskFailed, "project-a", "startup-symlink");
        let unrelated_event =
            harness.enqueue(EventKind::TaskFailed, "project-b", "startup-unrelated");

        let mut daemon = harness.daemon_at(harness.now);
        daemon.run_once().await.unwrap();
        assert_eq!(harness.event_status(symlink_event), EventStatus::DeadLetter);
        assert_eq!(harness.event_status(unrelated_event), EventStatus::Dispatched);
        assert!(symlinked.is_symlink());
        assert!(outside.is_dir());

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while harness.event_status(unrelated_event) != EventStatus::Completed {
            assert!(
                tokio::time::Instant::now() < deadline,
                "unrelated project did not drain"
            );
            daemon.run_once().await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    {
        let harness = DaemonHarness::new();
        let tmp = harness.root("project-a").join(".pueue-agent/tmp");
        fs::create_dir_all(&tmp).unwrap();
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o700)).unwrap();
        let weak = tmp.join("999");
        fs::create_dir(&weak).unwrap();
        fs::set_permissions(&weak, fs::Permissions::from_mode(0o755)).unwrap();
        let weak_event = harness.enqueue(EventKind::TaskFailed, "project-a", "startup-weak");
        let mut daemon = harness.daemon_at(harness.now);
        daemon.run_once().await.unwrap();
        assert_eq!(harness.event_status(weak_event), EventStatus::DeadLetter);
        assert_eq!(
            fs::metadata(&weak).unwrap().permissions().mode() & 0o777,
            0o755
        );
        assert!(weak.is_dir());
    }

    {
        let harness = DaemonHarness::new();
        let tmp = harness.root("project-a").join(".pueue-agent/tmp");
        fs::create_dir_all(&tmp).unwrap();
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o700)).unwrap();
        for run_id in 1..=pueue_agent::environment::MAX_PRIVATE_TEMP_GENERATIONS + 1 {
            let generation = tmp.join(run_id.to_string());
            fs::create_dir(&generation).unwrap();
            fs::set_permissions(&generation, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let over_limit_count = fs::read_dir(&tmp).unwrap().count();
        let over_limit_event =
            harness.enqueue(EventKind::TaskFailed, "project-a", "startup-over-limit");
        let mut daemon = harness.daemon_at(harness.now);
        daemon.run_once().await.unwrap();
        assert_eq!(harness.event_status(over_limit_event), EventStatus::DeadLetter);
        assert_eq!(fs::read_dir(&tmp).unwrap().count(), over_limit_count);
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn daemon_keeps_running_after_temp_inventory_violation_until_cancel() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 1"],
        1,
    );
    let tmp = harness.root("project-a").join(".pueue-agent/tmp");
    fs::create_dir_all(&tmp).unwrap();
    fs::set_permissions(&tmp, fs::Permissions::from_mode(0o700)).unwrap();
    let outside = harness.temp.path().join("outside-daemon-run");
    fs::create_dir(&outside).unwrap();
    fs::set_permissions(&outside, fs::Permissions::from_mode(0o700)).unwrap();
    let retained = tmp.join("999");
    symlink(&outside, &retained).unwrap();
    let unsafe_event = harness.enqueue(EventKind::TaskFailed, "project-a", "daemon-unsafe");
    let unrelated_event = harness.enqueue(EventKind::TaskFailed, "project-b", "daemon-unrelated");

    let shutdown = CancellationToken::new();
    let task_shutdown = shutdown.clone();
    let mut daemon = harness.daemon_at(harness.now);
    let task = tokio::spawn(async move { daemon.run(task_shutdown).await });
    // Native lifecycle setup includes compiling the generated fixture agent;
    // use the same production-bounded readiness contract as the other daemon
    // lifecycle tests instead of a short wall-clock bound around setup.
    harness.wait_for_native_dispatch(unrelated_event).await;
    let completion = tokio::time::timeout(Duration::from_secs(35), async {
        while harness.event_status(unrelated_event) != EventStatus::Completed {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    completion.expect("unrelated project should complete while daemon remains running");
    assert_eq!(harness.event_status(unsafe_event), EventStatus::DeadLetter);
    assert!(!task.is_finished());

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("daemon should stop after explicit cancellation")
        .unwrap()
        .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn cleanup_pending_project_defers_without_attempt_while_other_project_dispatches() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 1"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let first_event = harness.enqueue(EventKind::TaskFinished, "project-a", "cleanup-blocked-first");
    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();

    let run_id = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT run_id FROM agent_runs WHERE project_id = 'project-a' ORDER BY run_id DESC LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    let run_temp = harness
        .root("project-a")
        .join(".pueue-agent/tmp")
        .join(run_id.to_string());
    let overflow_subtree = create_cleanup_depth_overflow(&run_temp);

    let terminal_deadline = Instant::now() + Duration::from_secs(10);
    while harness.event_status(first_event) != EventStatus::Completed {
        assert!(Instant::now() < terminal_deadline, "project-a did not reach terminal persistence");
        daemon.run_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(overflow_subtree.is_dir());

    let blocked_event = harness.enqueue(EventKind::TaskFailed, "project-a", "cleanup-blocked-new");
    let other_event = harness.enqueue(EventKind::TaskFailed, "project-b", "cleanup-blocked-other");
    let intervention_id = InterventionRepository::new(&harness.db)
        .insert_pending("project-a", "must remain pending", harness.now)
        .unwrap();

    daemon.run_once().await.unwrap();
    let blocked = EventRepository::new(&harness.db)
        .find_by_id(blocked_event)
        .unwrap()
        .unwrap();
    assert_eq!(blocked.status, EventStatus::Pending);
    assert_eq!(blocked.attempts, 0);
    assert_eq!(harness.event_status(other_event), EventStatus::Dispatched);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE project_id = 'project-a' AND status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    let intervention = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, agent_run_id, attempts FROM interventions WHERE intervention_id = ?1",
            [intervention_id.intervention_id.as_str()],
            |row| Ok((
                row.get::<_, pueue_agent::interventions::InterventionStatus>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, i64>(2)?,
            )),
        )
        .unwrap();
    assert_eq!(
        intervention,
        (
            pueue_agent::interventions::InterventionStatus::Pending,
            None,
            0,
        )
    );

    fs::remove_dir_all(overflow_subtree).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn cleanup_retry_is_fair_across_projects_and_finishes_after_fault_removal() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 1"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 1"],
        1,
    );
    let event_a = harness.enqueue(EventKind::TaskFinished, "project-a", "cleanup-fair-a");
    let event_b = harness.enqueue(EventKind::TaskFinished, "project-b", "cleanup-fair-b");
    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();

    let run_id = |project_id: &str| {
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT run_id FROM agent_runs WHERE project_id = ?1 ORDER BY run_id DESC LIMIT 1",
                [project_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
    };
    let run_a = harness
        .root("project-a")
        .join(".pueue-agent/tmp")
        .join(run_id("project-a").to_string());
    let run_b = harness
        .root("project-b")
        .join(".pueue-agent/tmp")
        .join(run_id("project-b").to_string());
    let overflow_a = create_cleanup_depth_overflow(&run_a);
    let overflow_b = create_cleanup_depth_overflow(&run_b);

    let terminal_deadline = Instant::now() + Duration::from_secs(10);
    while harness.event_status(event_a) != EventStatus::Completed
        || harness.event_status(event_b) != EventStatus::Completed
    {
        assert!(Instant::now() < terminal_deadline, "agents did not reach terminal persistence");
        daemon.run_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(overflow_a.is_dir());
    assert!(overflow_b.is_dir());

    for project_id in ["project-a", "project-b"] {
        let config_path = harness.root(project_id).join(".pueue-agent/config.toml");
        let config = fs::read_to_string(&config_path).unwrap();
        fs::write(config_path, config.replace("sleep 1", "sleep 30")).unwrap();
    }

    let retry_event_a = harness.enqueue(EventKind::TaskFailed, "project-a", "cleanup-fair-retry-a");
    let retry_event_b = harness.enqueue(EventKind::TaskFailed, "project-b", "cleanup-fair-retry-b");
    daemon.run_once().await.unwrap();
    assert_eq!(harness.event_status(retry_event_a), EventStatus::Pending);
    assert_eq!(harness.event_status(retry_event_b), EventStatus::Pending);

    fs::remove_dir_all(overflow_a).unwrap();
    daemon.run_once().await.unwrap();
    assert!(!run_a.join("cleanup-level-0").exists());
    assert!(run_b.join("cleanup-level-0").is_dir());

    fs::remove_dir_all(overflow_b).unwrap();
    daemon.run_once().await.unwrap();
    assert!(!run_b.join("cleanup-level-0").exists());
    daemon.run_once().await.unwrap();
    assert_eq!(harness.event_status(retry_event_a), EventStatus::Dispatched);
    assert_eq!(harness.event_status(retry_event_b), EventStatus::Dispatched);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE project_id IN ('project-a', 'project-b')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        4
    );
    for project_id in ["project-a", "project-b"] {
        assert_eq!(
            harness
                .db
                .connect()
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM agent_runs WHERE project_id = ?1",
                    [project_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            2,
            "each project retains its original terminal run and one retry run",
        );
    }
    let retry_bindings = harness
        .db
        .connect()
        .unwrap()
        .prepare(
            "SELECT events.project_id, MAX(agent_run_events.run_id)
             FROM events
             JOIN agent_run_events
               ON agent_run_events.project_id = events.project_id
              AND agent_run_events.event_id = events.event_id
             WHERE events.event_id IN (?1, ?2)
             GROUP BY events.event_id
             ORDER BY events.event_id",
        )
        .unwrap()
        .query_map(rusqlite::params![retry_event_a, retry_event_b], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(retry_bindings.len(), 2);
    assert_eq!(retry_bindings[0].0, "project-a");
    assert_eq!(retry_bindings[1].0, "project-b");
    assert!(retry_bindings.iter().all(|(_, run_id)| run_id.is_some()));
    assert_ne!(retry_bindings[0].1, retry_bindings[1].1);
}

#[cfg(unix)]
#[tokio::test]
async fn shutdown_retains_temp_cleanup_when_deadline_expires() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 1"],
        1,
    );
    let event_id = harness.enqueue(EventKind::TaskFinished, "project-a", "cleanup-shutdown-deadline");
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_millis(150),
        },
    );
    daemon.run_once().await.unwrap();

    let run_id = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT run_id FROM agent_runs WHERE project_id = 'project-a' ORDER BY run_id DESC LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    let run_temp = harness
        .root("project-a")
        .join(".pueue-agent/tmp")
        .join(run_id.to_string());
    let overflow_subtree = create_cleanup_depth_overflow(&run_temp);
    let terminal_deadline = Instant::now() + Duration::from_secs(10);
    while harness.event_status(event_id) != EventStatus::Completed {
        assert!(Instant::now() < terminal_deadline, "agent did not reach terminal persistence");
        daemon.run_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(overflow_subtree.is_dir());

    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let started = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(3), daemon.run(shutdown))
        .await
        .expect("shutdown cleanup must remain bounded");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(result.is_err());
    assert_eq!(harness.event_status(event_id), EventStatus::Completed);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE project_id = 'project-a' AND status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );

    fs::remove_dir_all(overflow_subtree).unwrap();
    daemon.run_once().await.unwrap();
    assert!(!run_temp.join("cleanup-level-0").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn bound_cleanup_pending_project_defers_without_attempt_while_other_project_dispatches() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let initial_event = harness.enqueue(EventKind::TaskFailed, "project-a", "bound-cleanup-pending-first");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_bound_cleanup_pending_dispatch_ack
             BEFORE UPDATE OF launch_gate_state ON agent_runs
             WHEN NEW.project_id = 'project-a' AND NEW.launch_gate_state = 'released'
             BEGIN
                 SELECT RAISE(ABORT, 'injected bound cleanup pending dispatch acknowledgement failure');
             END;
             CREATE TRIGGER reject_bound_cleanup_pending_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.project_id = 'project-a'
                  AND NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected bound cleanup pending finalizer failure');
             END;",
        )
        .unwrap();

    let mut daemon = harness.daemon();
    assert!(daemon.run_once().await.is_err());
    assert_eq!(harness.event_status(initial_event), EventStatus::InFlight);

    let run_id = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT run_id FROM agent_runs WHERE project_id = 'project-a' ORDER BY run_id DESC LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    let run_temp = harness
        .root("project-a")
        .join(".pueue-agent/tmp")
        .join(run_id.to_string());
    let overflow_subtree = create_cleanup_depth_overflow(&run_temp);
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "DROP TRIGGER reject_bound_cleanup_pending_dispatch_ack;
             DROP TRIGGER reject_bound_cleanup_pending_finalizer;",
        )
        .unwrap();

    let blocked_event = harness.enqueue(EventKind::TaskFailed, "project-a", "bound-cleanup-pending-new");
    let other_event = harness.enqueue(EventKind::TaskFailed, "project-b", "bound-cleanup-pending-other");
    let intervention_id = InterventionRepository::new(&harness.db)
        .insert_pending("project-a", "must remain pending", harness.now)
        .unwrap();

    assert!(daemon.run_once().await.is_ok());
    let blocked = EventRepository::new(&harness.db)
        .find_by_id(blocked_event)
        .unwrap()
        .unwrap();
    assert_eq!(blocked.status, EventStatus::Pending);
    assert_eq!(blocked.attempts, 0);
    assert_eq!(harness.event_status(other_event), EventStatus::Dispatched);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE project_id = 'project-a' AND status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    let intervention = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, agent_run_id, attempts FROM interventions WHERE intervention_id = ?1",
            [intervention_id.intervention_id.as_str()],
            |row| Ok((
                row.get::<_, pueue_agent::interventions::InterventionStatus>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, i64>(2)?,
            )),
        )
        .unwrap();
    assert_eq!(
        intervention,
        (
            pueue_agent::interventions::InterventionStatus::Pending,
            None,
            0,
        )
    );

    fs::remove_dir_all(overflow_subtree).unwrap();
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), daemon.run(shutdown))
        .await
        .expect("bound cleanup owner shutdown must remain bounded")
        .unwrap();
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
    assert_eq!(harness.event_status(scheduled), EventStatus::Dispatched);
    assert_eq!(report.finished_agents, 0);
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
        vec![AgentRunStatus::TimedOut]
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
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_secs(3),
        },
    );
    let shutdown = CancellationToken::new();
    let join = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { daemon.run(shutdown).await }
    });

    harness.wait_for_active_agent().await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), join)
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
async fn daemon_shutdown_retains_handle_when_finalizer_exhausts_grace() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 10"],
        1,
    );
    let event_id = harness.enqueue(EventKind::DeepCheck, "project-a", "shutdown-finalizer");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_shutdown_terminal_run
             BEFORE UPDATE OF status ON agent_runs
             WHEN NEW.status = 'timed_out'
             BEGIN
                 SELECT RAISE(ABORT, 'injected shutdown finalizer failure');
             END;",
        )
        .unwrap();
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
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
    let result = tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("daemon should return after bounded shutdown grace")
        .expect("daemon task should not panic");
    assert!(result.is_err());
    assert_eq!(harness.event_status(event_id), EventStatus::Dispatched);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daemon_shutdown_database_lock_respects_global_deadline_and_retains_terminal_outcome() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let event_id = harness.enqueue(EventKind::DeepCheck, "project-a", "shutdown-database-lock");
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_secs(60),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_millis(1_500),
        },
    );
    let shutdown = CancellationToken::new();
    let join = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let result = daemon.run(shutdown).await;
            (daemon, result)
        }
    });
    harness.wait_for_active_agent().await;
    harness.wait_for_native_dispatch(event_id).await;
    let lock = harness.db.connect().unwrap();
    lock.execute_batch("BEGIN IMMEDIATE;").unwrap();

    let started = Instant::now();
    shutdown.cancel();
    let (mut daemon, result) = join.await.expect("daemon task should not panic");
    assert!(result.is_err());
    assert!(
        started.elapsed() < Duration::from_millis(2_500),
        "SQLite finalization must honor the shared shutdown deadline",
    );
    assert_eq!(harness.event_status(event_id), EventStatus::Dispatched);
    drop(lock);

    daemon.run_once().await.unwrap();
    assert_eq!(harness.event_status(event_id), EventStatus::RetryWait);
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_shutdown_attempts_later_agent_after_first_finalizer_persists() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let event_a = harness.enqueue(EventKind::DeepCheck, "project-a", "shutdown-round-robin-a");
    let event_b = harness.enqueue(EventKind::DeepCheck, "project-b", "shutdown-round-robin-b");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_project_a_shutdown_finalizer
             BEFORE UPDATE OF status ON agent_runs
             WHEN NEW.project_id = 'project-a' AND NEW.status = 'timed_out'
             BEGIN
                 SELECT RAISE(ABORT, 'injected persistent project-a finalizer failure');
             END;",
        )
        .unwrap();
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_millis(100),
        },
    );
    daemon.run_once().await.unwrap();
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    let started = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(5), daemon.run(shutdown))
        .await
        .expect("all agent owners must receive a shutdown attempt");
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "global shutdown grace must bound the whole retained-owner pass"
    );
    assert!(result.is_err());
    assert_eq!(harness.event_status(event_a), EventStatus::Dispatched);
    assert_eq!(harness.event_status(event_b), EventStatus::Dispatched);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        2,
    );
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_shutdown_retries_transient_finalizer_failure_with_same_handle() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 10"],
        1,
    );
    let event_id = harness.enqueue(EventKind::DeepCheck, "project-a", "shutdown-retry");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_one_shutdown_finalizer
             BEFORE UPDATE OF status ON agent_runs
             WHEN NEW.status = 'timed_out'
             BEGIN
                 SELECT RAISE(ABORT, 'injected one-shot shutdown finalizer failure');
             END;",
        )
        .unwrap();
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_secs(3),
        },
    );
    let shutdown = CancellationToken::new();
    let join = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { daemon.run(shutdown).await }
    });

    harness.wait_for_active_agent().await;
    let pid: i32 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT pid FROM agent_runs WHERE status IN ('starting', 'running')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let drop_trigger = tokio::spawn({
        let db = harness.db.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if !process_exists(pid) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("agent process should terminate during shutdown");
            tokio::time::sleep(Duration::from_millis(100)).await;
            db.connect()
                .unwrap()
                .execute_batch("DROP TRIGGER reject_one_shutdown_finalizer;")
                .unwrap();
        }
    });

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(4), join)
        .await
        .expect("daemon should retry the transient shutdown finalizer failure")
        .expect("daemon task should not panic")
        .unwrap();
    drop_trigger.await.unwrap();

    assert_eq!(harness.event_status(event_id), EventStatus::RetryWait);
    assert_eq!(harness.agent_run_statuses(), vec![AgentRunStatus::TimedOut]);
    assert!(AgentRunRepository::new(&harness.db)
        .find_active_by_project("project-a")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn daemon_retains_and_retries_unresolved_bound_cleanup_after_scheduler_error() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "runner-restore");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_runner_restore_dispatch_ack
             BEFORE UPDATE OF launch_gate_state ON agent_runs
             WHEN NEW.launch_gate_state = 'released'
             BEGIN
                 SELECT RAISE(ABORT, 'injected dispatch acknowledgement failure');
             END;
             CREATE TRIGGER reject_runner_restore_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected finalizer failure');
             END;",
        )
        .unwrap();

    let mut daemon = harness.daemon();
    let first = daemon.run_once().await;
    assert!(first.is_err());
    assert_eq!(harness.event_status(event_id), EventStatus::InFlight);

    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "DROP TRIGGER reject_runner_restore_dispatch_ack;
             DROP TRIGGER reject_runner_restore_finalizer;",
        )
        .unwrap();
    let second = daemon.run_once().await;
    assert!(second.is_ok(), "runner and cleanup owner should survive scheduler error");
    assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
    assert!(AgentRunRepository::new(&harness.db)
        .find_active_by_project("project-a")
        .unwrap()
        .is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_shutdown_keeps_bound_cleanup_after_repeated_finalizer_failure() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "bound-cleanup-shutdown");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_bound_cleanup_dispatch_ack
             BEFORE UPDATE OF launch_gate_state ON agent_runs
             WHEN NEW.launch_gate_state = 'released'
             BEGIN
                 SELECT RAISE(ABORT, 'injected dispatch acknowledgement failure');
             END;
             CREATE TRIGGER reject_bound_cleanup_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected bound cleanup finalizer failure');
             END;",
        )
        .unwrap();
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_millis(150),
        },
    );

    assert!(daemon.run_once().await.is_err());
    assert_eq!(harness.event_status(event_id), EventStatus::InFlight);
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    assert!(tokio::time::timeout(Duration::from_secs(3), daemon.run(shutdown))
        .await
        .expect("bound cleanup shutdown retry must remain bounded")
        .is_err());
    assert_eq!(harness.event_status(event_id), EventStatus::InFlight);
    assert!(AgentRunRepository::new(&harness.db)
        .find_active_by_project("project-a")
        .unwrap()
        .is_some());

    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "DROP TRIGGER reject_bound_cleanup_dispatch_ack;
             DROP TRIGGER reject_bound_cleanup_finalizer;",
        )
        .unwrap();
    daemon.run_once().await.unwrap();
    assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
    assert!(AgentRunRepository::new(&harness.db)
        .find_active_by_project("project-a")
        .unwrap()
        .is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_error_drain_preserves_started_and_bound_cleanup_owners_together() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let started_event = harness.enqueue(EventKind::TaskFinished, "project-a", "mixed-started");
    let cleanup_event = harness.enqueue(EventKind::TaskFailed, "project-b", "mixed-cleanup");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_project_b_dispatch_ack
             BEFORE UPDATE OF launch_gate_state ON agent_runs
             WHEN NEW.project_id = 'project-b' AND NEW.launch_gate_state = 'released'
             BEGIN
                 SELECT RAISE(ABORT, 'injected dispatch acknowledgement failure');
             END;
             CREATE TRIGGER reject_project_b_cleanup_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.project_id = 'project-b'
                  AND NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected cleanup finalizer failure');
             END;",
        )
        .unwrap();
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_secs(5),
        },
    );
    let drop_trigger = tokio::spawn({
        let db = harness.db.clone();
        async move {
            let pid = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let pid = db
                        .connect()
                        .unwrap()
                        .query_row(
                            "SELECT pid FROM agent_runs
                             WHERE project_id = 'project-b' AND pid IS NOT NULL",
                            [],
                            |row| row.get::<_, i32>(0),
                        )
                        .ok();
                    if let Some(pid) = pid {
                        break pid;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("project-b cleanup child should start");
            tokio::time::timeout(Duration::from_secs(5), async {
                while process_exists(pid) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("project-b cleanup child should terminate");
            tokio::time::sleep(Duration::from_millis(100)).await;
            db.connect()
                .unwrap()
                .execute_batch("DROP TRIGGER reject_project_b_cleanup_finalizer;")
                .unwrap();
        }
    });

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        daemon.run(CancellationToken::new()),
    )
    .await
    .expect("daemon error drain must remain bounded");
    assert!(result.is_err(), "the original scheduler error must remain visible");
    drop_trigger.await.unwrap();
    assert_eq!(harness.event_status(started_event), EventStatus::RetryWait);
    assert_eq!(harness.event_status(cleanup_event), EventStatus::DeadLetter);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0,
    );
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_cleanup_queue_retries_each_owner_without_loss_across_ticks() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let event_a = harness.enqueue(EventKind::TaskFailed, "project-a", "cleanup-queue-a");
    let event_b = harness.enqueue(EventKind::TaskFailed, "project-b", "cleanup-queue-b");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_cleanup_queue_dispatch_ack
             BEFORE UPDATE OF launch_gate_state ON agent_runs
             WHEN NEW.launch_gate_state = 'released'
             BEGIN
                 SELECT RAISE(ABORT, 'injected dispatch acknowledgement failure');
             END;
             CREATE TRIGGER reject_cleanup_queue_a_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.project_id = 'project-a'
                  AND NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected project-a cleanup failure');
             END;
             CREATE TRIGGER reject_cleanup_queue_b_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.project_id = 'project-b'
                  AND NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected project-b cleanup failure');
             END;",
        )
        .unwrap();
    let mut daemon = harness.daemon();

    assert!(daemon.run_once().await.is_err());
    assert_eq!(harness.event_status(event_a), EventStatus::InFlight);
    assert_eq!(harness.event_status(event_b), EventStatus::InFlight);

    harness
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_cleanup_queue_b_finalizer;")
        .unwrap();
    assert!(daemon.run_once().await.is_err());
    assert_eq!(harness.event_status(event_a), EventStatus::InFlight);
    assert_eq!(harness.event_status(event_b), EventStatus::DeadLetter);

    harness
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_cleanup_queue_a_finalizer;")
        .unwrap();
    daemon.run_once().await.unwrap();
    assert_eq!(harness.event_status(event_a), EventStatus::DeadLetter);
    assert_eq!(harness.event_status(event_b), EventStatus::DeadLetter);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0,
    );
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_poll_attempts_cleanup_when_an_agent_finalizer_fails_in_the_same_tick() {
    let harness = DaemonHarness::new();
    let release_path = harness.temp.path().join("cross-poll-agent-release");
    let release_text = release_path.to_str().unwrap();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["--wait-for-release", release_text],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let agent_event = harness.enqueue(EventKind::TaskFinished, "project-a", "cross-poll-agent");
    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();
    assert_eq!(harness.event_status(agent_event), EventStatus::Dispatched);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !PathBuf::from(format!("{}.ready", release_path.display())).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("project-a fixture should report readiness");

    let cleanup_event = harness.enqueue(EventKind::TaskFailed, "project-b", "cross-poll-cleanup");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_cross_poll_agent_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.project_id = 'project-a'
                  AND NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected project-a finalizer failure');
             END;
             CREATE TRIGGER reject_cross_poll_cleanup_dispatch_ack
             BEFORE UPDATE OF launch_gate_state ON agent_runs
             WHEN NEW.project_id = 'project-b' AND NEW.launch_gate_state = 'released'
             BEGIN
                 SELECT RAISE(ABORT, 'injected project-b dispatch acknowledgement failure');
             END;
             CREATE TRIGGER reject_cross_poll_cleanup_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.project_id = 'project-b'
                  AND NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected project-b cleanup failure');
             END;",
        )
        .unwrap();
    assert!(daemon.run_once().await.is_err());
    assert_eq!(harness.event_status(agent_event), EventStatus::Dispatched);
    assert_eq!(harness.event_status(cleanup_event), EventStatus::InFlight);
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_cross_poll_cleanup_finalizer;")
        .unwrap();
    fs::write(&release_path, b"release").unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(daemon.run_once().await.is_err());
    assert_eq!(harness.event_status(agent_event), EventStatus::Dispatched);
    assert_eq!(
        harness.event_status(cleanup_event),
        EventStatus::DeadLetter,
        "a failing agent finalizer must not starve a healthy cleanup owner",
    );
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_retains_started_agent_when_a_later_project_scheduler_error_occurs() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 1"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/path/that/does/not/exist/pueue-agent",
        &["{prompt}"],
        1,
    );
    let successful_event = harness.enqueue(
        EventKind::TaskFinished,
        "project-a",
        "mixed-success",
    );
    let failed_event = harness.enqueue(EventKind::TaskFailed, "project-b", "mixed-error");

    let mut daemon = harness.daemon();
    let first = daemon.run_once().await;
    assert!(first.is_err());
    assert_eq!(harness.event_status(successful_event), EventStatus::Dispatched);
    assert_eq!(harness.event_status(failed_event), EventStatus::DeadLetter);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs
                 WHERE project_id = 'project-a' AND status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );

    let deadline = Instant::now() + Duration::from_secs(15);
    while harness.event_status(successful_event) == EventStatus::Dispatched {
        assert!(Instant::now() < deadline, "retained agent did not reach terminal state");
        tokio::time::sleep(Duration::from_millis(100)).await;
        daemon.run_once().await.unwrap();
    }
    assert_eq!(harness.event_status(successful_event), EventStatus::Completed);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs
                 WHERE project_id = 'project-a' AND status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_run_drains_started_agent_before_returning_scheduler_error() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 10"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/path/that/does/not/exist/pueue-agent",
        &["{prompt}"],
        1,
    );
    let successful_event = harness.enqueue(
        EventKind::TaskFinished,
        "project-a",
        "run-mixed-success",
    );
    let failed_event = harness.enqueue(EventKind::TaskFailed, "project-b", "run-mixed-error");

    let mut daemon = harness.daemon();
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        daemon.run(CancellationToken::new()),
    )
    .await
    .expect("daemon should drain retained agents before returning");
    let error = result.expect_err("later scheduler failure should remain visible");
    assert!(error.to_string().contains("policy_blocked"));
    assert_eq!(harness.event_status(successful_event), EventStatus::RetryWait);
    assert_eq!(harness.event_status(failed_event), EventStatus::DeadLetter);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs
                 WHERE status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    let pid: i32 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT pid FROM agent_runs WHERE project_id = 'project-a'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!process_exists(pid));
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

#[cfg(unix)]
#[tokio::test]
async fn startup_recovery_confirms_release_marker_through_project_root_descriptor() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "descriptor-marker");
    harness.claim_with_lease(event_id, harness.now + 600);
    let project_root = ProjectRepository::new(&harness.db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap()
        .root_path;
    let log_path = project_root.join(format!(".pueue-agent/logs/agent-190-{event_id}.log"));
    let runs = AgentRunRepository::new(&harness.db);
    let run = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                &log_path,
            ),
            &[event_id],
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 42_424, harness.now - 5)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    let marker_path = PathBuf::from(format!("{}.gate-started", log_path.display()));
    fs::write(&marker_path, b"authorized\n").unwrap();
    fs::set_permissions(&marker_path, fs::Permissions::from_mode(0o600)).unwrap();

    let report = harness.daemon().run_once().await.unwrap();

    assert_eq!(report.dead_lettered_agent_events, 1);
    assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
    let gate: String = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT launch_gate_state FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(gate, "released");
}

#[cfg(unix)]
#[tokio::test]
async fn startup_recovery_closes_pending_marker_evidence_crash_window() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "pending-marker-crash-window");
    harness.claim_with_lease(event_id, harness.now + 600);
    let project_root = ProjectRepository::new(&harness.db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap()
        .root_path;
    let log_path = project_root.join(format!(".pueue-agent/logs/agent-190-{event_id}.log"));
    let run = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                &log_path,
            ),
            &[event_id],
        )
        .unwrap();
    let marker_path = PathBuf::from(format!("{}.gate-started", log_path.display()));
    fs::write(&marker_path, b"authorized\n").unwrap();
    fs::set_permissions(&marker_path, fs::Permissions::from_mode(0o600)).unwrap();

    let report = harness.daemon().run_once().await.unwrap();

    assert_eq!(report.dead_lettered_agent_events, 1);
    assert_eq!(report.requeued_agent_events, 0);
    let event = EventRepository::new(&harness.db)
        .find_by_id(event_id)
        .unwrap()
        .unwrap();
    assert_eq!(event.status, EventStatus::DeadLetter);
    assert_eq!(event.attempts, 0);
    assert!(marker_path.exists());
    let state: (AgentRunStatus, String, Option<String>, Option<String>) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, launch_gate_state, policy_code, failure_stage
             FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        state,
        (
            AgentRunStatus::Failed,
            "failed".to_owned(),
            Some("native_gate_failed".to_owned()),
            Some("post_marker".to_owned()),
        ),
    );
}

#[cfg(unix)]
#[tokio::test]
async fn startup_recovery_rejects_symlinked_log_directory_without_database_mutation() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "symlinked-log-dir");
    harness.claim_with_lease(event_id, harness.now + 600);
    let project_root = ProjectRepository::new(&harness.db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap()
        .root_path;
    let log_path = project_root.join(format!(".pueue-agent/logs/agent-190-{event_id}.log"));
    let runs = AgentRunRepository::new(&harness.db);
    let run = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                &log_path,
            ),
            &[event_id],
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 42_424, harness.now - 5)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    let mut daemon = harness.daemon();
    let logs = harness.root("project-a").join(".pueue-agent/logs");
    let retained_logs = harness.root("project-a").join(".pueue-agent/logs-retained");
    fs::rename(&logs, &retained_logs).unwrap();
    symlink(&retained_logs, &logs).unwrap();

    assert!(daemon.run_once().await.is_err());

    assert_eq!(harness.event_status(event_id), EventStatus::InFlight);
    let state: (AgentRunStatus, String) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, launch_gate_state FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, (AgentRunStatus::Running, "release_requested".to_owned()));
}

#[cfg(unix)]
#[tokio::test]
async fn startup_recovery_rejects_replaced_project_root_without_database_mutation() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "replaced-root");
    harness.claim_with_lease(event_id, harness.now + 600);
    let root = ProjectRepository::new(&harness.db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap()
        .root_path;
    let log_path = root.join(format!(".pueue-agent/logs/agent-190-{event_id}.log"));
    let runs = AgentRunRepository::new(&harness.db);
    let run = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                &log_path,
            ),
            &[event_id],
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 42_424, harness.now - 5)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    let mut daemon = harness.daemon();
    let config = fs::read(root.join(".pueue-agent/config.toml")).unwrap();
    let retained_root = harness.temp.path().join("project-a-retained");
    fs::rename(&root, &retained_root).unwrap();
    fs::create_dir_all(root.join(".pueue-agent")).unwrap();
    fs::write(root.join(".pueue-agent/config.toml"), config).unwrap();

    assert!(daemon.run_once().await.is_err());

    assert_eq!(harness.event_status(event_id), EventStatus::InFlight);
    let state: (AgentRunStatus, String) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, launch_gate_state FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, (AgentRunStatus::Running, "release_requested".to_owned()));
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
        (InterventionStatus::Pending, None)
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

    let runs = AgentRunRepository::new(&harness.db);
    let starting_run = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                starting_event,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                harness.registered_root("project-a").join(format!(
                    ".pueue-agent/logs/agent-190-{starting_event}.log"
                )),
            ),
            &[starting_event],
        )
        .unwrap()
        .run_id;
    let running_run = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-b",
                running_event,
                None,
                AgentRunStatus::Running,
                harness.now - 10,
                harness.registered_root("project-b").join(format!(
                    ".pueue-agent/logs/agent-190-{running_event}.log"
                )),
            ),
            &[running_event],
        )
        .unwrap()
        .run_id;

    let mut daemon = harness.daemon();
    let first = daemon.run_once().await.unwrap();
    let second = daemon.run_once().await.unwrap();
    let mut restarted_daemon = harness.daemon();
    let repeated_recovery = restarted_daemon.run_once().await.unwrap();

    assert_eq!(first.recovered_agent_runs, 2);
    assert_eq!(first.requeued_agent_events, 1);
    assert_eq!(first.dead_lettered_agent_events, 1);
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
    assert_eq!(harness.event_status(starting_event), EventStatus::RetryWait);
    assert_eq!(harness.event_status(running_event), EventStatus::DeadLetter);
    for event_id in [starting_event, running_event] {
        assert_eq!(
            EventRepository::new(&harness.db)
                .find_by_id(event_id)
                .unwrap()
                .unwrap()
                .lease_until,
            None
        );
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
    let run_id = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                harness.registered_root("project-a").join(format!(
                    ".pueue-agent/logs/agent-190-{event_id}.log"
                )),
            ),
            &[event_id],
        )
        .unwrap()
        .run_id;
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
    assert_eq!(event.status, EventStatus::InFlight);
    assert_eq!(event.lease_until, None);
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
    assert_eq!(harness.event_status(event_id), EventStatus::RetryWait);
    assert_eq!(harness.agent_run_state(run_id).0, AgentRunStatus::Failed);
}

#[tokio::test]
async fn startup_recovery_loads_disabled_project_config() {
    let harness = DaemonHarness::new();
    harness.register_project("project-b", "pb-project", "/bin/echo");
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE projects SET enabled = 0, paused = 1 WHERE project_id = 'project-b'",
            [],
        )
        .unwrap();
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-b", "disabled-recovery");
    harness.claim_with_lease(event_id, harness.now + 600);
    let run = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-b",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                harness.registered_root("project-b").join(format!(
                    ".pueue-agent/logs/agent-190-{event_id}.log"
                )),
            ),
            &[event_id],
        )
        .unwrap();

    let mut daemon = harness.daemon();
    let report = daemon.run_once().await.unwrap();

    assert_eq!(report.recovered_agent_runs, 1);
    assert_eq!(harness.agent_run_state(run.run_id).0, AgentRunStatus::Failed);
    assert_eq!(harness.event_status(event_id), EventStatus::RetryWait);
}

#[tokio::test]
async fn startup_recovery_rejects_project_identity_mismatch_before_mutation() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "identity-mismatch");
    harness.claim_with_lease(event_id, harness.now + 600);
    let run = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                harness.registered_root("project-a").join(format!(
                    ".pueue-agent/logs/agent-190-{event_id}.log"
                )),
            ),
            &[event_id],
        )
        .unwrap();
    let mut daemon = harness.daemon();
    let config_path = harness.root("project-a").join(".pueue-agent/config.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        config.replace("pueue_group = \"pa-project\"", "pueue_group = \"wrong-group\""),
    )
    .unwrap();

    assert!(daemon.run_once().await.is_err());
    let unchanged_event = EventRepository::new(&harness.db)
        .find_by_id(event_id)
        .unwrap()
        .unwrap();
    assert_eq!(unchanged_event.status, EventStatus::InFlight);
    assert_eq!(unchanged_event.lease_until, None);
    assert_eq!(harness.agent_run_state(run.run_id).0, AgentRunStatus::Starting);

    fs::write(&config_path, config).unwrap();
    let report = daemon.run_once().await.unwrap();
    assert_eq!(report.recovered_agent_runs, 1);
    assert_eq!(harness.event_status(event_id), EventStatus::RetryWait);
    assert_eq!(harness.agent_run_state(run.run_id).0, AgentRunStatus::Failed);
}

#[tokio::test]
async fn startup_recovery_commits_projects_independently_and_retries_failed_project() {
    let harness = DaemonHarness::new();
    harness.register_project("project-b", "pb-project", "/bin/echo");
    harness.pause_project("project-a");
    harness.pause_project("project-b");
    let first_event = harness.enqueue(EventKind::TaskFailed, "project-a", "project-a-recovery");
    let second_event = harness.enqueue(EventKind::TaskFailed, "project-b", "project-b-recovery");
    harness.claim_with_lease(first_event, harness.now + 600);
    harness.claim_with_lease(second_event, harness.now + 600);
    let runs = AgentRunRepository::new(&harness.db);
    runs.insert_with_events(
        &NewAgentRun::new(
            "project-a",
            first_event,
            None,
            AgentRunStatus::Starting,
            harness.now - 10,
            harness.registered_root("project-a").join(format!(
                ".pueue-agent/logs/agent-190-{first_event}.log"
            )),
        ),
        &[first_event],
    )
    .unwrap();
    runs.insert_with_events(
        &NewAgentRun::new(
            "project-b",
            second_event,
            None,
            AgentRunStatus::Starting,
            harness.now - 10,
            harness.registered_root("project-b").join(format!(
                ".pueue-agent/logs/agent-190-{second_event}.log"
            )),
        ),
        &[second_event],
    )
    .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_project_b_recovery
             BEFORE UPDATE OF status ON agent_runs
             WHEN OLD.project_id = 'project-b' AND NEW.status = 'failed'
             BEGIN
                 SELECT RAISE(ABORT, 'injected project-b recovery failure');
             END;",
        )
        .unwrap();

    let mut daemon = harness.daemon();
    assert!(daemon.run_once().await.is_err());
    assert_eq!(harness.event_status(first_event), EventStatus::RetryWait);
    assert_eq!(harness.event_status(second_event), EventStatus::InFlight);

    harness
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_project_b_recovery;")
        .unwrap();
    let report = daemon.run_once().await.unwrap();
    assert_eq!(report.recovered_agent_runs, 1);
    assert_eq!(harness.event_status(second_event), EventStatus::RetryWait);
}

#[tokio::test]
async fn startup_recovery_runs_before_the_first_scheduling_pass() {
    let harness = DaemonHarness::new();
    let event_id = harness.enqueue(EventKind::TaskFinished, "project-a", "restart-dispatch");
    harness.claim_with_lease(event_id, harness.now + 600);
    let interrupted = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Running,
                harness.now - 10,
                harness.registered_root("project-a").join(format!(
                    ".pueue-agent/logs/agent-190-{event_id}.log"
                )),
            ),
            &[event_id],
        )
        .unwrap()
        .run_id;

    let mut daemon = harness.daemon();
    let report = daemon.run_once().await.unwrap();

    assert_eq!(report.recovered_agent_runs, 1);
    assert_eq!(report.dead_lettered_agent_events, 1);
    assert_eq!(harness.agent_run_count(), 1);
    assert_eq!(
        harness.agent_run_state(interrupted).0,
        AgentRunStatus::Failed
    );
    assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
}
