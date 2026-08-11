use std::{
    ffi::OsString,
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use clap::Parser;
use pueue_agent::{
    agent::{AgentRunner, AgentRunnerConfig},
    daemon::{Daemon, DaemonConfig},
    db::{
        AgentRunRepository, Db, EventRepository, IncidentRepository, ProjectRepository,
        TerminationRequestRepository,
    },
    models::{
        AgentContextMode, AgentRunStatus, EventKind, NewAgentRun, NewEvent, NewIncident,
        NewProject, NewTerminationRequest, TerminationRequestStatus,
    },
    pueue::{PueueApi, PueueTask},
    service::ServiceStatus,
    status::{self, DisableMode, PueueSnapshot, StatusInput},
    AppError,
};
use serde_json::json;
use tempfile::TempDir;

const CODEX_SESSION_ID: &str = "019f9f30-5f31-7a40-8e28-bd95e1f6c537";

#[test]
fn service_lifecycle_commands_parse_without_project_state() {
    for (command, expected_json) in [("start", true), ("stop", false)] {
        let cli = pueue_agent::cli::Cli::try_parse_from([
            "pueue-agent",
            command,
            if expected_json { "--json" } else { "" },
        ]
        .into_iter()
        .filter(|argument| !argument.is_empty()))
        .unwrap();

        match cli.command {
            pueue_agent::cli::Command::Start(args) if command == "start" => {
                assert_eq!(args.json, expected_json);
            }
            pueue_agent::cli::Command::Stop(args) if command == "stop" => {
                assert_eq!(args.json, expected_json);
            }
            _ => panic!("expected {command} service lifecycle command"),
        }
    }
}

#[cfg(unix)]
#[test]
fn service_lifecycle_commands_report_only_verified_service_state() {
    use std::os::unix::fs::PermissionsExt;

    let temp = TempDir::new().unwrap();
    let bin = temp.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    let launchctl = bin.join("launchctl");
    fs::write(&launchctl, "#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(&launchctl, fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());

    let start = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .current_dir(temp.path())
        .env("HOME", temp.path())
        .env("PATH", &path)
        .arg("start")
        .output()
        .unwrap();
    assert!(start.status.success(), "{}", String::from_utf8_lossy(&start.stderr));
    let start_text = String::from_utf8_lossy(&start.stdout);
    assert!(start_text.contains("service: running"));
    assert!(!start_text.contains("project"));
    assert!(!start_text.contains("Pueue"));

    let stop = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .current_dir(temp.path())
        .env("HOME", temp.path())
        .env("PATH", &path)
        .args(["stop", "--json"])
        .output()
        .unwrap();
    assert!(stop.status.success(), "{}", String::from_utf8_lossy(&stop.stderr));
    let body: serde_json::Value = serde_json::from_slice(&stop.stdout).unwrap();
    assert_eq!(body["schema_version"], 1);
    assert_eq!(body["operation"], "stop");
    assert_eq!(body["service"], "stopped");
    assert!(body.get("project_id").is_none());
    assert!(body.get("pueue").is_none());

    fs::write(
        &launchctl,
        "#!/bin/sh\nif [ \"$1\" = print ]; then exit 1; fi\nexit 0\n",
    )
    .unwrap();
    fs::set_permissions(&launchctl, fs::Permissions::from_mode(0o755)).unwrap();
    let unverified_start = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .current_dir(temp.path())
        .env("HOME", temp.path())
        .env("PATH", &path)
        .arg("start")
        .output()
        .unwrap();
    assert!(!unverified_start.status.success());
    assert!(String::from_utf8_lossy(&unverified_start.stderr).contains("verify service started"));
}

#[derive(Clone)]
struct OperatorPueue {
    tasks: Arc<Mutex<Result<Vec<PueueTask>, String>>>,
    kill_calls: Arc<Mutex<Vec<i64>>>,
}

impl OperatorPueue {
    fn with_tasks(tasks: Vec<PueueTask>) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(Ok(tasks))),
            kill_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn kill_calls(&self) -> Vec<i64> {
        self.kill_calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl PueueApi for OperatorPueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        self.tasks
            .lock()
            .unwrap()
            .clone()
            .map_err(|_| AppError::Runtime {
                operation: "fake Pueue status",
            })
    }

    async fn add(&self, _args: &[OsString]) -> Result<i64, AppError> {
        panic!("operator tests must not submit Pueue tasks")
    }

    async fn kill(&self, task_id: i64) -> Result<(), AppError> {
        self.kill_calls.lock().unwrap().push(task_id);
        Ok(())
    }

    async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
        panic!("operator tests must not provision Pueue groups")
    }
}

struct OperatorHarness {
    temp: TempDir,
    db: Db,
    now: i64,
}

impl OperatorHarness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project");
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
project_id = "project-a"
pueue_group = "pa-project"

[agent]
program = "codex"
args = ["exec", "{{prompt}}"]
timeout_minutes = 10
max_retries = 2

[agent.context]
mode = "resume"
session_id = "{CODEX_SESSION_ID}"

[check]
interval_minutes = 10
deep_check_every = 6
deep_check_interval_minutes = 0
stall_minutes = 30
log_tail_bytes = 1024
extra_log_paths = []
patterns = []

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
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        let now = 100;
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &root,
                "pa-project",
                root.join(".pueue-agent/config.toml"),
                now,
            ))
            .unwrap();
        Self { temp, db, now }
    }

    fn project(&self) -> pueue_agent::models::Project {
        ProjectRepository::new(&self.db)
            .find_by_id("project-a")
            .unwrap()
            .unwrap()
    }

    fn event(&self, kind: EventKind, dedup_key: &str) -> i64 {
        EventRepository::new(&self.db)
            .insert_idempotent(&NewEvent::new(
                "project-a",
                kind,
                dedup_key,
                json!({
                    "task_id": 41,
                    "transcript": "hidden transcript must not be printed",
                }),
                self.now,
                self.now,
            ))
            .unwrap()
            .event_id
    }

    fn running_task(&self) -> PueueTask {
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

    fn status_input(&self, snapshot: PueueSnapshot) -> StatusInput {
        StatusInput {
            daemon_health: ServiceStatus::Running,
            pueue: snapshot,
        }
    }

    fn operator_log_rows(&self) -> Vec<(String, String)> {
        let connection = self.db.connect().unwrap();
        let mut statement = connection
            .prepare("SELECT action, details_json FROM operator_logs ORDER BY log_id")
            .unwrap();
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }
}

#[test]
fn status_shows_failed_termination_without_marking_project_idle_or_dumping_transcripts() {
    let harness = OperatorHarness::new();
    let pending_event_id = harness.event(EventKind::TerminationFailed, "termination-failed");
    let failed_event_id = harness.event(EventKind::TaskFailed, "agent-primary");
    EventRepository::new(&harness.db)
        .transition_many(
            &[failed_event_id],
            pueue_agent::models::EventStatus::Failed,
            harness.now,
            None,
            Some("agent failed"),
        )
        .unwrap();
    let incident = IncidentRepository::new(&harness.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "termination",
            Some("task-41"),
            "termination-fingerprint",
            harness.now,
        ))
        .unwrap()
        .incident;
    let request = TerminationRequestRepository::new(&harness.db)
        .insert_idempotent(&NewTerminationRequest::new(
            incident.incident_id,
            "project-a",
            "task-signature",
            "fatal pattern",
            harness.now,
            Some(harness.now + 60),
        ))
        .unwrap();
    TerminationRequestRepository::new(&harness.db)
        .update_result(
            request.request_id,
            TerminationRequestStatus::Failed,
            None,
            Some("pueue kill failed"),
        )
        .unwrap();
    let failed_run = AgentRunRepository::new(&harness.db)
        .insert(&NewAgentRun::new(
            "project-a",
            failed_event_id,
            None,
            AgentRunStatus::Starting,
            harness.now - 10,
            harness.temp.path().join("failed-agent.log"),
        ))
        .unwrap();
    AgentRunRepository::new(&harness.db)
        .finish(
            failed_run.run_id,
            AgentRunStatus::Failed,
            harness.now - 1,
            Some(1),
            Some("agent failed"),
        )
        .unwrap();
    let active_run = AgentRunRepository::new(&harness.db)
        .insert(&NewAgentRun::with_context(
            "project-a",
            pending_event_id,
            Some(1234),
            AgentRunStatus::Running,
            harness.now,
            harness.temp.path().join("active-agent.log"),
            AgentContextMode::Resume {
                session_id: CODEX_SESSION_ID.to_owned(),
            },
            Some(CODEX_SESSION_ID.to_owned()),
            vec!["session-prev".to_owned(), "session-current".to_owned()],
        ))
        .unwrap();
    AgentRunRepository::new(&harness.db)
        .attach_event(active_run.run_id, pending_event_id)
        .unwrap();

    let output = status::render_project_status(
        &harness.db,
        &harness.project(),
        &harness.status_input(PueueSnapshot::Tasks(vec![harness.running_task()])),
    )
    .unwrap();

    assert!(output.contains("daemon: running"));
    assert!(output.contains("project: project-a"));
    assert!(output.contains("enabled: true"));
    assert!(output.contains("paused: false"));
    assert!(output.contains("halted: no"));
    assert!(output.contains("active_tasks: 1"));
    assert!(output.contains("task=41 state=running"));
    assert!(output.contains("events: pending=1 failed=1"));
    assert!(output.contains("event="));
    assert!(output
        .contains("termination_requests: requested=0 sent=0 confirmed=0 timed_out=0 failed=1"));
    assert!(output.contains("termination_failed"));
    assert!(output.contains("request="));
    assert!(output.contains("open_incidents: 1"));
    assert!(output.contains("agent_runs: active=1 failed=1"));
    assert!(
        output.contains("guardrails: consecutive_failures=2/3 experiments=0/20 agent_runs=2/10")
    );
    assert!(output.contains(&format!(
        "codex_context: mode=resume session={CODEX_SESSION_ID}"
    )));
    assert!(output.contains("last_lineage: session-prev -> session-current"));
    assert!(!output.contains("idle"));
    assert!(!output.contains("hidden transcript"));
}

#[test]
fn status_bounds_the_joined_termination_error_line() {
    let harness = OperatorHarness::new();
    for index in 0..3 {
        let incident = IncidentRepository::new(&harness.db)
            .upsert_active(&NewIncident::new(
                "project-a",
                "termination",
                Some(&format!("task-{index}")),
                format!("termination-fingerprint-{index}"),
                harness.now + index,
            ))
            .unwrap()
            .incident;
        let request = TerminationRequestRepository::new(&harness.db)
            .insert_idempotent(&NewTerminationRequest::new(
                incident.incident_id,
                "project-a",
                format!("task-signature-{index}"),
                "fatal pattern",
                harness.now + index,
                Some(harness.now + index + 60),
            ))
            .unwrap();
        TerminationRequestRepository::new(&harness.db)
            .update_result(
                request.request_id,
                TerminationRequestStatus::Failed,
                None,
                Some(&format!(
                    "termination failure {index} AWS_SECRET_ACCESS_KEY=SECRET_{index} {}",
                    "e".repeat(300)
                )),
            )
            .unwrap();
    }

    let output = status::render_project_status(
        &harness.db,
        &harness.project(),
        &harness.status_input(PueueSnapshot::Tasks(Vec::new())),
    )
    .unwrap();
    let termination_line = output
        .lines()
        .find(|line| line.starts_with("termination_errors: "))
        .expect("termination error line");

    assert!(termination_line.len() <= "termination_errors: ".len() + 240);
    assert!(output
        .contains("termination_requests: requested=0 sent=0 confirmed=0 timed_out=0 failed=3"));
    assert!(!termination_line.contains("SECRET_0"));
    assert!(!termination_line.contains("SECRET_1"));
    assert!(!termination_line.contains("SECRET_2"));
}

#[test]
fn status_shows_pueue_integration_error_without_claiming_active_tasks_are_empty() {
    let harness = OperatorHarness::new();

    let output = status::render_project_status(
        &harness.db,
        &harness.project(),
        &harness.status_input(PueueSnapshot::Error("Pueue status unavailable".to_owned())),
    )
    .unwrap();

    assert!(output.contains("pueue: error: Pueue status unavailable"));
    assert!(!output.contains("active_tasks: 0"));
    assert!(!output.contains("active_tasks: none"));
    assert!(!output.contains("idle"));
}

#[test]
fn status_human_bounds_halted_reason_and_context_lineage() {
    let harness = OperatorHarness::new();
    let halted_reason = format!(
        "manual halt --password HALT_SECRET_VALUE {}",
        "h".repeat(400)
    );
    ProjectRepository::new(&harness.db)
        .halt("project-a", &halted_reason, harness.now + 1)
        .unwrap();

    let event_id = harness.event(EventKind::Crash, "bounded-context");
    let lineage = format!("LINEAGE_VALUE {}", "l".repeat(400));
    AgentRunRepository::new(&harness.db)
        .insert(&NewAgentRun::with_context(
            "project-a",
            event_id,
            Some(1234),
            AgentRunStatus::Running,
            harness.now + 2,
            harness.temp.path().join("active-agent.log"),
            AgentContextMode::Resume {
                session_id: CODEX_SESSION_ID.to_owned(),
            },
            Some(CODEX_SESSION_ID.to_owned()),
            vec![lineage],
        ))
        .unwrap();

    let output = status::render_project_status(
        &harness.db,
        &harness.project(),
        &harness.status_input(PueueSnapshot::Tasks(vec![])),
    )
    .unwrap();

    let halted_line = output
        .lines()
        .find(|line| line.starts_with("halted: "))
        .unwrap();
    assert!(halted_line.len() <= "halted: ".len() + 243);
    assert!(halted_line.ends_with("..."));
    assert!(!halted_line.contains("HALT_SECRET_VALUE"));

    let lineage_line = output
        .lines()
        .find(|line| line.starts_with("last_lineage: "))
        .unwrap();
    assert!(lineage_line.len() <= "last_lineage: ".len() + 243);
    assert!(lineage_line.ends_with("..."));
    assert!(!lineage_line.contains(&"l".repeat(400)));

    let context_line = output
        .lines()
        .find(|line| line.starts_with("codex_context: "))
        .unwrap();
    assert!(context_line.len() <= "codex_context: mode=resume session=".len() + 243);
    assert!(context_line.contains(CODEX_SESSION_ID));
}

#[test]
fn status_human_bounds_and_redacts_task_state() {
    let harness = OperatorHarness::new();
    let task = PueueTask {
        id: 41,
        group: "pa-project".to_owned(),
        command: "python train.py".to_owned(),
        state: format!(
            "running AWS_SECRET_ACCESS_KEY=TASK_STATE_SECRET {}\x1b[31m",
            "s".repeat(400)
        ),
        enqueued_at: Some("100".to_owned()),
        started_at: Some("101".to_owned()),
        ended_at: None,
        result: None,
    };

    let output = status::render_project_status(
        &harness.db,
        &harness.project(),
        &harness.status_input(PueueSnapshot::Tasks(vec![task])),
    )
    .unwrap();
    let task_line = output
        .lines()
        .find(|line| line.starts_with("task=41 "))
        .expect("task line");

    assert!(task_line.len() <= "task=41 state=".len() + 240 + " python train.py".len());
    assert!(!task_line.contains("TASK_STATE_SECRET"));
    assert!(!task_line.chars().any(char::is_control));
    assert!(task_line.contains("running"));
}

#[test]
fn status_human_bounds_and_redacts_project_root_path() {
    let harness = OperatorHarness::new();
    let mut project = harness.project();
    project.root_path = std::path::PathBuf::from(format!(
        "/tmp/{}/AWS_SECRET_ACCESS_KEY=AKIA_ROOT_SECRET/{}/tail",
        "root".repeat(160),
        "segment".repeat(160)
    ));

    let output = status::render_project_status(
        &harness.db,
        &project,
        &harness.status_input(PueueSnapshot::Tasks(vec![])),
    )
    .unwrap();

    let root_line = output
        .lines()
        .find(|line| line.starts_with("root: "))
        .unwrap();
    assert!(root_line.len() <= "root: ".len() + 243);
    assert!(root_line.contains("[path]"));
    assert!(!root_line.contains("AWS_SECRET_ACCESS_KEY"));
    assert!(!root_line.contains("AKIA_ROOT_SECRET"));
}

#[test]
fn status_human_bounds_and_redacts_project_and_group() {
    let harness = OperatorHarness::new();
    let mut project = harness.project();
    project.project_id = format!(
        "AWS_SECRET_ACCESS_KEY=PROJECT_SECRET {}",
        "project".repeat(160)
    );
    project.pueue_group = format!("ACCESS_KEY=GROUP_SECRET {}", "group".repeat(160));

    let output = status::render_project_status(
        &harness.db,
        &project,
        &harness.status_input(PueueSnapshot::Tasks(vec![])),
    )
    .unwrap();

    let project_line = output
        .lines()
        .find(|line| line.starts_with("project: "))
        .unwrap();
    assert!(project_line.len() <= "project: ".len() + 243);
    assert!(!project_line.contains("PROJECT_SECRET"));

    let group_line = output
        .lines()
        .find(|line| line.starts_with("group: "))
        .unwrap();
    assert!(group_line.len() <= "group: ".len() + 243);
    assert!(!group_line.contains("GROUP_SECRET"));
}

#[test]
fn status_text_output_is_byte_compatible_for_an_active_project() {
    let harness = OperatorHarness::new();
    let project = harness.project();

    let output = status::render_project_status(
        &harness.db,
        &project,
        &harness.status_input(PueueSnapshot::Tasks(vec![harness.running_task()])),
    )
    .unwrap();

    assert_eq!(
        output,
        format!(
            "pueue-agent status project=project-a\ndaemon: running\nproject: project-a\nroot: [path]\ngroup: pa-project\nenabled: true\npaused: false\nhalted: no\nactive_tasks: 1\ntask=41 state=running python train.py\nevents: pending=0 failed=0\nintegration_errors: 0\nopen_incidents: 0\ntermination_requests: requested=0 sent=0 confirmed=0 timed_out=0 failed=0\nagent_runs: active=0 failed=0\nguardrails: consecutive_failures=0/3 experiments=0/20 agent_runs=0/10\ncodex_context: mode=resume session={CODEX_SESSION_ID}\nsummary: 1 active task(s), 0 pending event(s), 0 active agent run(s)",
        )
    );
}

#[tokio::test]
async fn pause_prevents_new_agent_claims_and_automatic_termination_until_resume() {
    let harness = OperatorHarness::new();
    let event_id = harness.event(EventKind::Crash, "crash");
    let incident = IncidentRepository::new(&harness.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "fatal-pattern",
            Some("task-41"),
            "fatal-fingerprint",
            harness.now,
        ))
        .unwrap()
        .incident;
    let request = TerminationRequestRepository::new(&harness.db)
        .insert_idempotent(&NewTerminationRequest::new(
            incident.incident_id,
            "project-a",
            pueue_agent::reconcile::task_signature(&harness.running_task()),
            "fatal pattern",
            harness.now,
            Some(harness.now + 60),
        ))
        .unwrap();

    status::pause_project(&harness.db, "project-a", harness.now + 1).unwrap();
    assert!(EventRepository::new(&harness.db)
        .claim_batch(harness.now + 1, harness.now + 60, 10)
        .unwrap()
        .is_empty());

    let pueue = OperatorPueue::with_tasks(vec![harness.running_task()]);
    let mut daemon = Daemon::new(
        harness.db.clone(),
        pueue.clone(),
        AgentRunner::new(AgentRunnerConfig::for_tests(
            harness.temp.path().join("agent.log"),
        )),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now + 1),
            shutdown_grace_period: Duration::from_secs(1),
        },
    );
    daemon.run_once().await.unwrap();

    assert!(pueue.kill_calls().is_empty());
    assert_eq!(
        TerminationRequestRepository::new(&harness.db)
            .find_by_id(request.request_id)
            .unwrap()
            .unwrap()
            .status,
        TerminationRequestStatus::Requested
    );

    status::resume_project(&harness.db, "project-a", harness.now + 2).unwrap();
    let claimed = EventRepository::new(&harness.db)
        .claim_batch(harness.now + 2, harness.now + 60, 10)
        .unwrap();
    assert_eq!(
        claimed
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>(),
        vec![event_id]
    );
    let project = harness.project();
    assert!(!project.paused);
    assert!(project.halted_reason.is_none());
}

#[test]
fn state_transitions_write_durable_operator_logs_in_the_transition_transaction() {
    let harness = OperatorHarness::new();

    status::pause_project(&harness.db, "project-a", harness.now + 1).unwrap();
    ProjectRepository::new(&harness.db)
        .halt("project-a", "manual halt", harness.now + 2)
        .unwrap();
    status::resume_project(&harness.db, "project-a", harness.now + 3).unwrap();
    status::disable_project(
        &harness.db,
        "project-a",
        DisableMode::KeepReservation,
        &[],
        harness.now + 4,
    )
    .unwrap();
    status::disable_project(
        &harness.db,
        "project-a",
        DisableMode::Remove,
        &[],
        harness.now + 5,
    )
    .unwrap();

    let logs = harness.operator_log_rows();
    assert_eq!(
        logs.iter()
            .map(|(action, _)| action.as_str())
            .collect::<Vec<_>>(),
        vec!["pause", "halt", "resume", "disable", "remove"]
    );
    assert!(logs[1].1.contains("\"halted_reason\":\"manual halt\""));
    assert!(logs[2].1.contains("\"cleared_halt\":true"));
    assert!(logs[3].1.contains("\"unresolved_task_count\":0"));
    assert!(logs[4].1.contains("\"group_released\":true"));

    let restarted = Db::open(harness.db.path()).unwrap();
    let persisted_count: i64 = restarted
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM operator_logs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(persisted_count, 5);
}

#[test]
fn disable_without_remove_keeps_group_reserved_when_unresolved_tasks_remain() {
    let harness = OperatorHarness::new();
    let unresolved = vec![harness.running_task()];

    let disabled = status::disable_project(
        &harness.db,
        "project-a",
        DisableMode::KeepReservation,
        &unresolved,
        harness.now + 1,
    )
    .unwrap();

    assert!(!disabled.enabled);
    assert!(ProjectRepository::new(&harness.db)
        .find_by_group("pa-project")
        .unwrap()
        .is_some());

    let other_root = harness.temp.path().join("other");
    fs::create_dir_all(&other_root).unwrap();
    let duplicate = ProjectRepository::new(&harness.db).register(&NewProject::new(
        "project-b",
        &other_root,
        "pa-project",
        other_root.join(".pueue-agent/config.toml"),
        harness.now + 2,
    ));
    assert!(matches!(
        duplicate,
        Err(AppError::DatabaseConflict {
            field: "pueue_group"
        })
    ));
    let logs = harness.operator_log_rows();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].0, "disable");
    assert!(logs[0].1.contains("\"unresolved_task_count\":1"));
    assert!(logs[0].1.contains("\"unresolved_task_ids\":[41]"));
    assert!(logs[0].1.contains("\"group_released\":false"));
}

#[test]
fn disable_without_remove_keeps_group_reserved_when_pueue_group_is_empty() {
    let harness = OperatorHarness::new();

    let disabled = status::disable_project(
        &harness.db,
        "project-a",
        DisableMode::KeepReservation,
        &[],
        harness.now + 1,
    )
    .unwrap();

    assert!(!disabled.enabled);
    assert!(ProjectRepository::new(&harness.db)
        .find_by_group("pa-project")
        .unwrap()
        .is_some());
    let logs = harness.operator_log_rows();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].0, "disable");
    assert!(logs[0].1.contains("\"unresolved_task_count\":0"));
    assert!(logs[0].1.contains("\"unresolved_task_ids\":[]"));
    assert!(logs[0].1.contains("\"group_released\":false"));
}

#[test]
fn disable_remove_explicitly_releases_group_without_controlling_pueue_tasks() {
    let harness = OperatorHarness::new();
    let unresolved = vec![harness.running_task()];

    let removed = status::disable_project(
        &harness.db,
        "project-a",
        DisableMode::Remove,
        &unresolved,
        harness.now + 1,
    )
    .unwrap();

    assert_eq!(removed.project_id, "project-a");
    assert!(ProjectRepository::new(&harness.db)
        .find_by_group("pa-project")
        .unwrap()
        .is_none());

    let other_root = harness.temp.path().join("replacement");
    fs::create_dir_all(&other_root).unwrap();
    ProjectRepository::new(&harness.db)
        .register(&NewProject::new(
            "project-b",
            &other_root,
            "pa-project",
            other_root.join(".pueue-agent/config.toml"),
            harness.now + 2,
        ))
        .unwrap();

    let logs = harness.operator_log_rows();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].0, "remove");
    assert!(logs[0].1.contains("\"unresolved_task_count\":1"));
    assert!(logs[0].1.contains("\"group_released\":true"));
}
