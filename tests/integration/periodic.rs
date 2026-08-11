use std::fs;

use pueue_agent::{
    db::{
        AgentRunRepository, Db, EventRepository, ProjectRepository, TaskObservationRepository,
    },
    models::{
        AgentRunStatus, Event, EventKind, EventStatus, NewAgentRun, NewEvent, NewProject,
        NewTaskObservation,
    },
    periodic::PeriodicDeepCheckScheduler,
    pueue::PueueTask,
    reconcile::task_signature,
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
        harness.register_project("project-a", "periodic-project-a", interval_minutes);
        harness
    }

    fn register_project(&self, project_id: &str, group: &str, interval_minutes: u32) {
        let root = self.temp.path().join(project_id);
        let state = root.join(".pueue-agent");
        fs::create_dir_all(&state).unwrap();
        let config_path = state.join("config.toml");
        fs::write(
            &config_path,
            format!(
                r#"
project_id = "{project_id}"
pueue_group = "{group}"

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
                project_id,
                root,
                group,
                config_path,
                self.now,
            ))
            .unwrap();
    }

    fn schedule(&self, tasks: &[PueueTask]) -> usize {
        self.schedule_at(self.now, tasks)
    }

    fn schedule_at(&self, now: i64, tasks: &[PueueTask]) -> usize {
        PeriodicDeepCheckScheduler::new(&self.db, now)
            .schedule(tasks)
            .unwrap()
    }

    fn observe_running_task(&self, task: &PueueTask, observed_at: i64) {
        TaskObservationRepository::new(&self.db)
            .upsert(&NewTaskObservation::new(
                "project-a",
                task_signature(task),
                task.id,
                task.group.clone(),
                vec![task.command.clone()],
                task.state.clone(),
                None,
                None,
                None,
                None,
                observed_at,
            ))
            .unwrap();
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
        self.event_count_for_project("project-a")
    }

    fn event_count_for_project(&self, project_id: &str) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE project_id = ?1",
                [project_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn complete_only_event(&self) {
        let event = self.only_event();
        assert_eq!(
            EventRepository::new(&self.db)
                .transition_many(
                    &[event.event_id],
                    EventStatus::Completed,
                    self.now,
                    None,
                    None,
                )
                .unwrap(),
            1
        );
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
    harness.complete_only_event();
    assert_eq!(harness.schedule(&[harness.running_task(41)]), 0);
    assert_eq!(harness.event_count(), 1);
}

#[test]
fn scheduler_keeps_tasks_in_their_registered_project_group() {
    let harness = PeriodicHarness::with_interval(30);
    harness.register_project("project-b", "periodic-project-b", 30);
    let mut task = harness.running_task(99);
    task.group = "periodic-project-b".to_owned();

    assert_eq!(harness.schedule(&[task]), 1);
    assert_eq!(harness.event_count_for_project("project-a"), 0);
    assert_eq!(harness.event_count_for_project("project-b"), 1);
}

#[test]
fn event_repository_reports_whether_idempotent_insert_created_a_row() {
    let harness = PeriodicHarness::with_interval(30);
    let first = NewEvent::new(
        "project-a",
        EventKind::DeepCheck,
        "periodic-deep-check:v1:test-insert-outcome",
        json!({"source": "periodic"}),
        harness.now,
        harness.now,
    );
    let duplicate = NewEvent::new(
        "project-a",
        EventKind::DeepCheck,
        "periodic-deep-check:v1:test-insert-outcome",
        json!({"source": "replacement"}),
        harness.now,
        harness.now,
    );
    let repository = EventRepository::new(&harness.db);

    let (inserted, inserted_new) = repository.insert_idempotent_with_inserted(&first).unwrap();
    let (existing, existing_new) = repository
        .insert_idempotent_with_inserted(&duplicate)
        .unwrap();

    assert!(inserted_new);
    assert!(!existing_new);
    assert_eq!(inserted.event_id, existing.event_id);
    assert_eq!(existing.payload, json!({"source": "periodic"}));
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
fn scheduler_uses_persisted_first_observation_for_a_missing_task_start() {
    let harness = PeriodicHarness::with_interval(30);
    let mut task = harness.running_task(41);
    task.started_at = None;

    harness.observe_running_task(&task, 1_000);
    assert_eq!(harness.schedule_at(1_000, std::slice::from_ref(&task)), 0);

    harness.observe_running_task(&task, 2_000);
    assert_eq!(harness.schedule_at(2_799, std::slice::from_ref(&task)), 0);
    assert_eq!(harness.schedule_at(2_800, std::slice::from_ref(&task)), 1);

    let observation = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT first_observed_at, observed_at FROM task_observations
             WHERE project_id = 'project-a'",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .unwrap();
    assert_eq!(observation, (1_000, 2_000));
}

#[test]
fn scheduler_uses_persisted_first_observation_for_an_invalid_task_start() {
    let harness = PeriodicHarness::with_interval(30);
    let mut task = harness.running_task(41);
    task.started_at = Some("not-a-timestamp".to_owned());

    harness.observe_running_task(&task, 1_000);
    harness.observe_running_task(&task, 2_000);

    assert_eq!(harness.schedule_at(2_800, std::slice::from_ref(&task)), 1);
}
