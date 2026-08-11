use std::fs;

use pueue_agent::{
    db::{AgentRunRepository, Db, EventRepository, ProjectRepository},
    models::{AgentRunStatus, Event, EventKind, NewAgentRun, NewEvent, NewProject},
    periodic::PeriodicDeepCheckScheduler,
    pueue::PueueTask,
};
use serde_json::json;
use tempfile::TempDir;

const NOW: i64 = 3_600;

struct PeriodicHarness {
    temp: TempDir,
    db: Db,
    now: i64,
}

impl PeriodicHarness {
    fn with_interval(interval_minutes: u32) -> Self {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        let harness = Self { temp, db, now: NOW };
        harness.register_project(interval_minutes);
        harness
    }

    fn register_project(&self, interval_minutes: u32) {
        let root = self.temp.path().join("project-a");
        let state = root.join(".pueue-agent");
        fs::create_dir_all(&state).unwrap();
        let config_path = state.join("config.toml");
        fs::write(
            &config_path,
            format!(
                r#"
project_id = "project-a"
pueue_group = "periodic-project-a"

[agent]
program = "/bin/echo"
args = []
timeout_minutes = 1
max_retries = 0

[check]
interval_minutes = 1
deep_check_every = 1
deep_check_interval_minutes = {interval_minutes}
stall_minutes = 1
log_tail_bytes = 1024
extra_log_paths = []

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 1
max_experiments = 1
max_agent_runs = 1
"#,
            ),
        )
        .unwrap();
        ProjectRepository::new(&self.db)
            .register(&NewProject::new(
                "project-a",
                root,
                "periodic-project-a",
                config_path,
                self.now,
            ))
            .unwrap();
    }

    fn schedule(&self, tasks: &[PueueTask]) -> usize {
        PeriodicDeepCheckScheduler::new(&self.db, self.now)
            .schedule(tasks)
            .unwrap()
    }

    fn running_task(&self, task_id: i64) -> PueueTask {
        PueueTask {
            id: task_id,
            group: "periodic-project-a".to_owned(),
            command: "sensitive-command --token=secret".to_owned(),
            state: "running".to_owned(),
            enqueued_at: Some("0".to_owned()),
            started_at: Some("1800".to_owned()),
            ended_at: None,
            result: None,
        }
    }

    fn only_event(&self) -> Event {
        let connection = self.db.connect().unwrap();
        let event_id = connection
            .query_row("SELECT event_id FROM events", [], |row| row.get(0))
            .unwrap();
        EventRepository::new(&self.db)
            .find_by_id(event_id)
            .unwrap()
            .unwrap()
    }

    fn event_count(&self) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap()
    }

    fn insert_active_agent_run(&self) -> i64 {
        let event_id = EventRepository::new(&self.db)
            .insert_idempotent(&NewEvent::new(
                "project-a",
                EventKind::TaskFailed,
                "active-agent-run",
                json!({}),
                self.now,
                self.now,
            ))
            .unwrap()
            .event_id;
        AgentRunRepository::new(&self.db)
            .insert(&NewAgentRun::new(
                "project-a",
                event_id,
                Some(42),
                AgentRunStatus::Running,
                self.now,
                self.temp.path().join("active-agent.log"),
            ))
            .unwrap()
            .run_id
    }

    fn finish_active_agent_run(&self, run_id: i64) {
        AgentRunRepository::new(&self.db)
            .finish(run_id, AgentRunStatus::Completed, self.now, Some(0), None)
            .unwrap();
    }

    fn insert_open_periodic_event(&self) {
        EventRepository::new(&self.db)
            .insert_idempotent(&NewEvent::new(
                "project-a",
                EventKind::DeepCheck,
                "periodic-deep-check:v1:existing",
                json!({ "source": "periodic" }),
                self.now,
                self.now,
            ))
            .unwrap();
    }
}

#[test]
fn scheduler_creates_one_bounded_periodic_event_for_multiple_running_tasks() {
    let harness = PeriodicHarness::with_interval(30);
    let count = harness.schedule(&[harness.running_task(41), harness.running_task(42)]);

    assert_eq!(count, 1);
    let event = harness.only_event();
    assert_eq!(event.kind, EventKind::DeepCheck);
    assert_eq!(event.payload["source"], "periodic");
    assert_eq!(event.payload["task_count"], 2);
    assert_eq!(event.payload["task_ids"], json!([41, 42]));
    assert!(event.payload.get("command").is_none());
    assert!(event.payload.get("environment").is_none());
    assert!(event.payload.get("prompt").is_none());
    assert!(event.payload.get("transcript").is_none());
}

#[test]
fn scheduler_is_idempotent_for_the_same_periodic_bucket() {
    let harness = PeriodicHarness::with_interval(30);
    assert_eq!(harness.schedule(&[harness.running_task(41)]), 1);
    assert_eq!(harness.schedule(&[harness.running_task(41)]), 0);
    assert_eq!(harness.event_count(), 1);
}

#[test]
fn scheduler_skips_active_agent_and_open_periodic_event() {
    let harness = PeriodicHarness::with_interval(30);
    let run_id = harness.insert_active_agent_run();
    assert_eq!(harness.schedule(&[harness.running_task(41)]), 0);
    harness.finish_active_agent_run(run_id);
    harness.insert_open_periodic_event();
    assert_eq!(harness.schedule(&[harness.running_task(41)]), 0);
}

#[test]
fn scheduler_limits_task_ids_without_copying_task_contents() {
    let harness = PeriodicHarness::with_interval(30);
    let tasks = (0..100)
        .map(|task_id| harness.running_task(task_id))
        .collect::<Vec<_>>();

    assert_eq!(harness.schedule(&tasks), 1);
    let event = harness.only_event();
    assert_eq!(event.payload["task_count"], 100);
    assert!(event.payload["task_ids"].as_array().unwrap().len() <= 64);
    assert!(!event.payload.to_string().contains("sensitive-command"));
}

#[test]
fn scheduler_uses_now_as_the_anchor_for_an_unparseable_task_start() {
    let harness = PeriodicHarness::with_interval(30);
    let mut task = harness.running_task(41);
    task.started_at = Some("not-a-timestamp".to_owned());

    assert_eq!(harness.schedule(&[task]), 0);
}
