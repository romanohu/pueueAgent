use std::{fs, path::PathBuf};

use pueue_agent::{
    agent::{AgentRunner, AgentRunnerConfig},
    config,
    db::{AgentRunRepository, Db, EventRepository, ProjectRepository, SubmissionRepository},
    models::{
        AgentContextMode, AgentRunStatus, EventKind, EventStatus, NewAgentRun, NewEvent,
        NewProject, NewSubmission, SubmissionStatus,
    },
    scheduler::{Scheduler, SchedulerConfig},
};
use rusqlite::params;
use serde_json::json;
use tempfile::TempDir;
#[cfg(unix)]
use tokio::time::{sleep, Duration, Instant};

struct SchedulerHarness {
    temp: TempDir,
    db: Db,
    now: i64,
}

impl SchedulerHarness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        let harness = Self { temp, db, now: 100 };
        harness.register_project("project-a", "pa-project-a", "/bin/echo", "");
        harness
    }

    fn root(&self, project_id: &str) -> PathBuf {
        self.temp.path().join(project_id)
    }

    fn register_project(&self, project_id: &str, group: &str, program: &str, context: &str) {
        let root = self.root(project_id);
        fs::create_dir_all(root.join(".pueue-agent/logs")).unwrap();
        fs::write(root.join(".pueue-agent/STATE.md"), "state reference").unwrap();
        fs::write(
            root.join(".pueue-agent/instructions.md"),
            "instructions reference",
        )
        .unwrap();
        fs::write(
            root.join(".pueue-agent/config.toml"),
            format!(
                r#"
project_id = "{project_id}"
pueue_group = "{group}"

[agent]
program = "{program}"
args = ["--agent-arg", "{{prompt}}"]
timeout_minutes = 1
max_retries = 2
{context}

[check]
interval_minutes = 10
deep_check_every = 6
deep_check_interval_minutes = 0
stall_minutes = 30
log_tail_bytes = 1024
extra_log_paths = []

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 3
max_experiments = 20
max_agent_runs = 10
"#
            ),
        )
        .unwrap();

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

    fn enqueue(&self, kind: EventKind, project_id: &str, dedup_key: &str) -> i64 {
        EventRepository::new(&self.db)
            .insert_idempotent(&NewEvent::new(
                project_id,
                kind,
                dedup_key,
                json!({
                    "task_id": 41,
                    "evidence": "x".repeat(4096),
                }),
                self.now,
                self.now,
            ))
            .unwrap()
            .event_id
    }

    fn scheduler(&self) -> Scheduler {
        Scheduler::new(
            self.db.clone(),
            AgentRunner::new(AgentRunnerConfig::for_tests(
                self.temp.path().join("agent.log"),
            )),
            SchedulerConfig {
                now: self.now,
                lease_seconds: 60,
                claim_limit: 100,
            },
        )
    }

    fn event_status(&self, event_id: i64) -> EventStatus {
        self.event(event_id).status
    }

    fn event(&self, event_id: i64) -> pueue_agent::models::Event {
        EventRepository::new(&self.db)
            .find_by_id(event_id)
            .unwrap()
            .unwrap()
    }

    fn event_status_and_error(&self, event_id: i64) -> (EventStatus, Option<String>) {
        let event = EventRepository::new(&self.db)
            .find_by_id(event_id)
            .unwrap()
            .unwrap();
        (event.status, event.last_error)
    }

    fn active_runs(&self, project_id: &str) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs
                 WHERE project_id = ?1 AND status IN ('starting', 'running')",
                [project_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn claimed_with_lease_count(&self) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM events
                 WHERE status = 'claimed' OR lease_until IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }
}

#[tokio::test]
async fn crash_and_deep_check_for_one_project_start_one_crash_run() {
    let harness = SchedulerHarness::new();
    let deep_check = harness.enqueue(EventKind::DeepCheck, "project-a", "deep-check");
    let crash = harness.enqueue(EventKind::Crash, "project-a", "crash");

    let mut scheduler = harness.scheduler();
    let report = scheduler.tick().await.unwrap();

    assert_eq!(report.started.len(), 1);
    assert_eq!(report.started[0].primary_event_id, crash);
    assert_eq!(report.started[0].mode, "crash");
    assert_eq!(report.started[0].event_ids, vec![crash, deep_check]);
    assert!(report.started[0].prompt.contains("Dispatch mode: crash"));
    assert!(report.started[0]
        .prompt
        .contains(&format!("event_id={crash}")));
    assert!(report.started[0].prompt.contains(".pueue-agent/STATE.md"));
    assert!(report.started[0]
        .prompt
        .contains(".pueue-agent/instructions.md"));
    assert!(report.started[0].prompt.len() <= 16 * 1024);
    let stored_context: (String, Option<String>, String) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT context_mode, context_session_id, context_lineage_json
             FROM agent_runs WHERE run_id = ?1",
            [report.started[0].run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(stored_context.0, "fresh");
    assert_eq!(stored_context.1, None);
    assert!(stored_context.2.contains(&crash.to_string()));
    assert!(!stored_context.2.contains("state reference"));
    assert_eq!(harness.event_status(crash), EventStatus::Completed);
    assert_eq!(harness.event_status(deep_check), EventStatus::Completed);
}

#[tokio::test]
async fn active_agent_prevents_new_claim_for_same_project() {
    let harness = SchedulerHarness::new();
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "failure");
    AgentRunRepository::new(&harness.db)
        .insert(&NewAgentRun::with_context(
            "project-a",
            event_id,
            Some(1234),
            AgentRunStatus::Running,
            harness.now,
            harness.temp.path().join("active.log"),
            AgentContextMode::Fresh,
            None,
            Vec::new(),
        ))
        .unwrap();

    let mut scheduler = harness.scheduler();
    let report = scheduler.tick().await.unwrap();

    assert!(report.started.is_empty());
    assert_eq!(harness.active_runs("project-a"), 1);
    assert_eq!(harness.event_status(event_id), EventStatus::Pending);
}

#[tokio::test]
async fn invalid_resume_config_does_not_leave_claimed_event_stranded() {
    let harness = SchedulerHarness::new();
    let root = harness.root("project-a");
    fs::write(
        root.join(".pueue-agent/config.toml"),
        r#"
project_id = "project-a"
pueue_group = "pa-project-a"

[agent]
program = "codex"
args = ["exec", "{prompt}"]
timeout_minutes = 1
max_retries = 2

[agent.context]
mode = "resume"

[check]
interval_minutes = 10
deep_check_every = 6
deep_check_interval_minutes = 0
stall_minutes = 30
log_tail_bytes = 1024
extra_log_paths = []

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 3
max_experiments = 20
max_agent_runs = 10
"#,
    )
    .unwrap();
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "bad-resume");

    let mut scheduler = harness.scheduler();
    let error = match scheduler.tick().await {
        Ok(_) => panic!("invalid resume config should fail the scheduler tick"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("agent.context.session_id"));
    let (status, last_error) = harness.event_status_and_error(event_id);
    assert_eq!(status, EventStatus::Failed);
    assert!(last_error
        .as_deref()
        .is_some_and(|message| message.contains("agent.context.session_id")));
    assert_eq!(harness.active_runs("project-a"), 0);
}

#[tokio::test]
async fn multi_project_config_error_resolves_all_claimed_events_before_returning() {
    let harness = SchedulerHarness::new();
    harness.register_project("project-b", "pa-project-b", "/bin/echo", "");
    fs::write(
        harness.root("project-a").join(".pueue-agent/config.toml"),
        r#"
project_id = "project-a"
pueue_group = "pa-project-a"

[agent]
program = "codex"
args = ["exec", "{prompt}"]
timeout_minutes = 1
max_retries = 2

[agent.context]
mode = "resume"

[check]
interval_minutes = 10
deep_check_every = 6
deep_check_interval_minutes = 0
stall_minutes = 30
log_tail_bytes = 1024
extra_log_paths = []

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 3
max_experiments = 20
max_agent_runs = 10
"#,
    )
    .unwrap();
    let invalid_event = harness.enqueue(EventKind::TaskFailed, "project-a", "bad-resume-batch");
    let valid_event = harness.enqueue(EventKind::TaskFinished, "project-b", "valid-batch");

    let mut scheduler = harness.scheduler();
    let error = match scheduler.tick().await {
        Ok(_) => panic!("invalid project config should fail the scheduler tick"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("agent.context.session_id"));
    let invalid = harness.event(invalid_event);
    assert_eq!(invalid.status, EventStatus::Failed);
    assert_eq!(invalid.lease_until, None);
    assert!(invalid
        .last_error
        .as_deref()
        .is_some_and(|message| message.contains("agent.context.session_id")));

    let valid = harness.event(valid_event);
    assert_eq!(valid.status, EventStatus::Completed);
    assert_eq!(valid.lease_until, None);
    assert_eq!(harness.active_runs("project-b"), 1);
    assert_eq!(harness.claimed_with_lease_count(), 0);
}

#[tokio::test]
async fn expired_claim_is_requeued_after_restart_recovery() {
    let mut harness = SchedulerHarness::new();
    let event_id = harness.enqueue(EventKind::TaskFinished, "project-a", "finished");
    EventRepository::new(&harness.db)
        .claim_batch(harness.now, harness.now + 10, 1)
        .unwrap();

    harness.now += 11;
    let scheduler = harness.scheduler();
    let recovered = scheduler.recover_expired_leases().unwrap();

    assert_eq!(recovered, 1);
    assert_eq!(harness.event_status(event_id), EventStatus::Pending);
}

#[tokio::test]
async fn retry_wait_events_obey_not_before_backoff() {
    let mut harness = SchedulerHarness::new();
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "retry-wait");
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'retry_wait', not_before = ?1 WHERE event_id = ?2",
            params![harness.now + 30, event_id],
        )
        .unwrap();

    let mut scheduler = harness.scheduler();
    assert!(scheduler.tick().await.unwrap().started.is_empty());
    assert_eq!(harness.event_status(event_id), EventStatus::RetryWait);

    harness.now += 30;
    let mut scheduler = harness.scheduler();
    assert_eq!(scheduler.tick().await.unwrap().started.len(), 1);
    assert_eq!(harness.event_status(event_id), EventStatus::Completed);
}

#[tokio::test]
async fn guardrails_halt_when_agent_run_limit_is_reached() {
    let harness = SchedulerHarness::new();
    let first_event = harness.enqueue(EventKind::TaskFinished, "project-a", "first");
    for offset in 0..10 {
        AgentRunRepository::new(&harness.db)
            .insert(&NewAgentRun::with_context(
                "project-a",
                first_event,
                None,
                AgentRunStatus::Completed,
                harness.now - 20 + offset,
                harness.temp.path().join(format!("run-{offset}.log")),
                AgentContextMode::Fresh,
                None,
                Vec::new(),
            ))
            .unwrap();
    }
    let event_id = harness.enqueue(EventKind::TaskFinished, "project-a", "over-limit");

    let mut scheduler = harness.scheduler();
    let report = scheduler.tick().await.unwrap();

    assert!(report.started.is_empty());
    assert_eq!(report.halted.len(), 1);
    assert_eq!(harness.event_status(event_id), EventStatus::Failed);
    let project = ProjectRepository::new(&harness.db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap();
    assert!(project.halted_reason.unwrap().contains("max_agent_runs"));
}

#[tokio::test]
async fn guardrails_halt_when_consecutive_failure_limit_is_reached() {
    let harness = SchedulerHarness::new();
    let first = harness.enqueue(EventKind::Crash, "project-a", "crash-1");
    let second = harness.enqueue(EventKind::Stalled, "project-a", "stalled-1");
    EventRepository::new(&harness.db)
        .transition_many(
            &[first, second],
            EventStatus::Completed,
            harness.now,
            None,
            None,
        )
        .unwrap();
    let current = harness.enqueue(EventKind::TaskFailed, "project-a", "failure-current");

    let mut scheduler = harness.scheduler();
    let report = scheduler.tick().await.unwrap();

    assert!(report.started.is_empty());
    assert_eq!(report.halted.len(), 1);
    assert_eq!(harness.event_status(current), EventStatus::Failed);
    let project = ProjectRepository::new(&harness.db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap();
    assert!(project
        .halted_reason
        .unwrap()
        .contains("max_consecutive_failures"));
}

#[tokio::test]
async fn guardrails_pause_when_experiment_limit_is_reached() {
    let harness = SchedulerHarness::new();
    for index in 0..20 {
        let submission = NewSubmission {
            status: SubmissionStatus::Accepted,
            ..NewSubmission::new(
                format!("submission-{index}"),
                "project-a",
                vec!["python".to_owned(), "train.py".to_owned()],
                harness.now - 20 + index,
            )
        };
        SubmissionRepository::new(&harness.db)
            .insert_idempotent(&submission)
            .unwrap();
    }
    let event_id = harness.enqueue(EventKind::TaskFinished, "project-a", "experiment-limit");

    let mut scheduler = harness.scheduler();
    let report = scheduler.tick().await.unwrap();

    assert!(report.started.is_empty());
    assert_eq!(report.paused.len(), 1);
    assert_eq!(harness.event_status(event_id), EventStatus::Failed);
    let project = ProjectRepository::new(&harness.db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap();
    assert!(project.paused);
}

#[test]
fn codex_resume_argv_uses_project_scoped_resume_without_shell() {
    let harness = SchedulerHarness::new();
    let root = harness.root("project-a").canonicalize().unwrap();
    let project = ProjectRepository::new(&harness.db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap();
    let mut agent = config::load(&root.join(".pueue-agent/config.toml"))
        .unwrap()
        .agent;
    agent.program = "codex".to_owned();
    agent.context = AgentContextMode::Resume {
        session_id: "session-123".to_owned(),
    };

    let command = AgentRunner::command_for(&project, &agent, "bounded prompt").unwrap();

    assert_eq!(command.program, "codex");
    assert_eq!(
        command.args,
        vec![
            "exec",
            "-C",
            root.to_str().unwrap(),
            "resume",
            "session-123",
            "bounded prompt",
        ]
    );
}

#[cfg(unix)]
#[tokio::test]
async fn agent_timeout_terminates_descendant_agent_processes() {
    let harness = SchedulerHarness::new();
    let root = harness.root("project-a");
    let pid_path = root.join(".pueue-agent/logs/descendant.pid");
    fs::write(
        root.join(".pueue-agent/config.toml"),
        r#"
project_id = "project-a"
pueue_group = "pa-project-a"

[agent]
program = "/bin/sh"
args = ["-c", "sleep 30 & echo $! > .pueue-agent/logs/descendant.pid; wait"]
timeout_minutes = 1
max_retries = 2

[check]
interval_minutes = 10
deep_check_every = 6
deep_check_interval_minutes = 0
stall_minutes = 30
log_tail_bytes = 1024
extra_log_paths = []

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 3
max_experiments = 20
max_agent_runs = 10
"#,
    )
    .unwrap();
    harness.enqueue(EventKind::TaskFailed, "project-a", "process-tree");

    let mut scheduler = harness.scheduler();
    let mut started = scheduler.tick().await.unwrap().started.pop().unwrap();
    let descendant_pid = wait_for_pid_file(&pid_path).await;
    assert!(
        process_exists(descendant_pid),
        "descendant process should be running before timeout cleanup"
    );

    started.handle.timeout_deadline = Instant::now();
    let status = started
        .handle
        .wait(&harness.db, harness.now + 1)
        .await
        .unwrap();

    assert_eq!(status, AgentRunStatus::TimedOut);
    let descendant_exited = wait_until_process_exits(descendant_pid).await;
    if !descendant_exited {
        kill_process(descendant_pid);
    }
    assert!(
        descendant_exited,
        "timeout cleanup must terminate the full agent process tree"
    );
}

#[test]
fn codex_resume_latest_argv_is_opt_in_and_project_scoped() {
    let harness = SchedulerHarness::new();
    let root = harness.root("project-a").canonicalize().unwrap();
    let project = ProjectRepository::new(&harness.db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap();
    let mut agent = config::load(&root.join(".pueue-agent/config.toml"))
        .unwrap()
        .agent;
    agent.program = "codex".to_owned();
    agent.context = AgentContextMode::ResumeLatest;

    let command = AgentRunner::command_for(&project, &agent, "bounded prompt").unwrap();

    assert_eq!(
        command.args,
        vec![
            "exec",
            "-C",
            root.to_str().unwrap(),
            "resume",
            "--last",
            "bounded prompt",
        ]
    );
}

#[cfg(unix)]
async fn wait_for_pid_file(path: &std::path::Path) -> i32 {
    for _ in 0..50 {
        if let Ok(contents) = fs::read_to_string(path) {
            if let Ok(pid) = contents.trim().parse::<i32>() {
                return pid;
            }
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("descendant pid file was not written");
}

#[cfg(unix)]
async fn wait_until_process_exits(pid: i32) -> bool {
    for _ in 0..50 {
        if !process_exists(pid) {
            return true;
        }
        sleep(Duration::from_millis(20)).await;
    }
    false
}

#[cfg(unix)]
fn process_exists(pid: i32) -> bool {
    unsafe extern "C" {
        fn kill(pid: std::os::raw::c_int, sig: std::os::raw::c_int) -> std::os::raw::c_int;
    }
    unsafe { kill(pid, 0) == 0 }
}

#[cfg(unix)]
fn kill_process(pid: i32) {
    unsafe extern "C" {
        fn kill(pid: std::os::raw::c_int, sig: std::os::raw::c_int) -> std::os::raw::c_int;
    }
    const SIGKILL: std::os::raw::c_int = 9;
    let _ = unsafe { kill(pid, SIGKILL) };
}
