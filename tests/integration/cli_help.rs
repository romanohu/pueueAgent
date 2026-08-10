#[test]
fn help_lists_diagnostics_commands_and_status_options() {
    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("submit"));
    assert!(text.contains("daemon"));
    assert!(text.contains("events"));
    assert!(text.contains("inspect"));
    assert!(text.contains("explain"));
    assert!(text.contains("doctor"));
    assert!(text.contains("wake"));

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["status", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("--json"));
    assert!(text.contains("--compact"));

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["submit", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("--kind"));
    assert!(text.contains("--metadata"));
    assert!(text.contains("--metadata-json"));
    assert!(text.contains("--json"));
}

#[test]
fn wake_help_describes_bounded_operator_wake_options() {
    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["wake", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("--reason"));
    assert!(text.contains("--json"));
}

#[test]
fn runs_help_and_limit_are_bounded() {
    let help = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["runs", "--help"])
        .output()
        .unwrap();
    assert!(help.status.success());
    let text = String::from_utf8_lossy(&help.stdout);
    assert!(text.contains("--json"));
    assert!(text.contains("--follow"));
    assert!(text.contains("--limit"));

    for value in ["0", "129"] {
        let output = assert_cmd::Command::cargo_bin("pueue-agent")
            .unwrap()
            .args(["runs", "--limit", value])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr)
            .contains("runs limit must be between 1 and 128"));
    }
}

#[test]
fn formatter_never_emits_ansi_for_json_or_piped_output() {
    use pueue_agent::output::{OutputMode, OutputTarget};

    assert!(!OutputMode::Json.uses_ansi(OutputTarget::Terminal));
    assert!(!OutputMode::Human.uses_ansi(OutputTarget::Pipe));
    assert!(!pueue_agent::output::format_state("running").contains('\x1b'));
    if std::env::var_os("NO_COLOR").is_none() {
        assert!(OutputMode::Human.uses_ansi(OutputTarget::Terminal));
    }
}

#[test]
fn events_rejects_limits_outside_the_diagnostic_bound() {
    let zero = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["events", "--limit", "0"])
        .output()
        .unwrap();

    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr)
        .contains("diagnostic event limit must be between 1 and 1000"));

    let too_large = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["events", "--limit", "1001"])
        .output()
        .unwrap();

    assert!(!too_large.status.success());
    assert!(String::from_utf8_lossy(&too_large.stderr)
        .contains("diagnostic event limit must be between 1 and 1000"));
}

#[test]
fn invalid_cli_parse_errors_redact_and_bound_values_while_help_stays_complete() {
    let secret = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args([
            "events",
            "--limit",
            "AWS_SECRET_ACCESS_KEY=CLI_PARSE_SECRET",
        ])
        .output()
        .unwrap();

    assert_eq!(secret.status.code(), Some(2));
    let secret_stderr = String::from_utf8_lossy(&secret.stderr);
    assert!(!secret_stderr.contains("CLI_PARSE_SECRET"));
    assert!(secret_stderr.len() <= 241);
    assert!(secret_stderr.contains("invalid value"));

    let long_value = format!("AWS_SECRET_ACCESS_KEY={}", "x".repeat(1_000));
    let long = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["events", "--limit", &long_value])
        .output()
        .unwrap();

    assert_eq!(long.status.code(), Some(2));
    let long_stderr = String::from_utf8_lossy(&long.stderr);
    assert!(long_stderr.len() <= 241);
    assert!(!long_stderr.contains(&"x".repeat(1_000)));

    let help = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .arg("--help")
        .output()
        .unwrap();
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("Usage:"));
    assert!(String::from_utf8_lossy(&help.stdout).contains("events"));
}

#[test]
fn events_cli_renders_the_project_scoped_event_projection() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("project");
    fs::create_dir_all(&root).unwrap();
    pueue_agent::init::run(&root).unwrap();
    let state_dir = temp.path().join("state");
    let db = Db::open(&state_dir.join("state.sqlite3")).unwrap();
    let project_config = config::load(&root.join(".pueue-agent/config.toml")).unwrap();
    ProjectRepository::new(&db)
        .register(&NewProject::new(
            &project_config.project_id,
            &root,
            &project_config.pueue_group,
            root.join(".pueue-agent/config.toml"),
            100,
        ))
        .unwrap();
    EventRepository::new(&db)
        .insert_idempotent(&NewEvent::new(
            &project_config.project_id,
            EventKind::TaskFailed,
            "cli-events-test",
            serde_json::json!({}),
            100,
            100,
        ))
        .unwrap();

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .env("PUEUE_AGENT_STATE_DIR", &state_dir)
        .current_dir(&root)
        .args(["events", "--json", "--limit", "1"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["project_id"], project_config.project_id);
    assert_eq!(body["events"].as_array().unwrap().len(), 1);
    assert_eq!(body["events"][0]["kind"], "task_failed");
}

#[test]
fn cli_output_contract_events_and_wake_have_human_and_json_boundaries() {
    let harness = DiagnosticsCliHarness::new();
    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            &harness.project_id,
            EventKind::TaskFailed,
            "cli-output-contract-event",
            serde_json::json!({}),
            100,
            100,
        ))
        .unwrap();

    let events = harness
        .command()
        .args(["events", "--limit", "1"])
        .output()
        .unwrap();
    assert!(
        events.status.success(),
        "{}",
        String::from_utf8_lossy(&events.stderr)
    );
    let events_text = String::from_utf8_lossy(&events.stdout);
    assert!(
        events_text.starts_with("pueue-agent events"),
        "{events_text}"
    );
    assert!(
        events_text.contains(&format!("event={}", event.event_id)),
        "{events_text}"
    );
    assert!(events_text.contains("state=pending"), "{events_text}");
    assert!(events_text.contains("summary:"), "{events_text}");
    assert!(!events_text.contains('\x1b'), "{events_text}");

    let events_json = harness
        .command()
        .args(["events", "--json", "--limit", "1"])
        .output()
        .unwrap();
    assert!(events_json.status.success());
    let events_json_text = String::from_utf8_lossy(&events_json.stdout);
    assert!(events_json_text.starts_with('{'), "{events_json_text}");
    assert!(
        !events_json_text.contains("pueue-agent"),
        "{events_json_text}"
    );
    assert!(!events_json_text.contains('\x1b'), "{events_json_text}");
    let _: Value = serde_json::from_str(&events_json_text).unwrap();

    let wake = harness
        .command()
        .env("NO_COLOR", "1")
        .env("PATH", "/definitely-no-pueue")
        .args(["wake", "--reason", "inspect current loss"])
        .output()
        .unwrap();
    assert!(
        wake.status.success(),
        "{}",
        String::from_utf8_lossy(&wake.stderr)
    );
    let wake_text = String::from_utf8_lossy(&wake.stdout);
    assert!(wake_text.starts_with("pueue-agent wake"), "{wake_text}");
    assert!(wake_text.contains("event="), "{wake_text}");
    assert!(wake_text.contains("state=pending"), "{wake_text}");
    assert!(wake_text.contains("summary:"), "{wake_text}");
    assert!(!wake_text.contains('\x1b'), "{wake_text}");

    let wake_json = harness
        .command()
        .env("NO_COLOR", "1")
        .args(["wake", "--reason", "inspect current loss", "--json"])
        .output()
        .unwrap();
    assert!(wake_json.status.success());
    let wake_json_text = String::from_utf8_lossy(&wake_json.stdout);
    assert!(wake_json_text.starts_with('{'), "{wake_json_text}");
    assert!(!wake_json_text.contains("pueue-agent"), "{wake_json_text}");
    assert!(!wake_json_text.contains('\x1b'), "{wake_json_text}");
    let _: Value = serde_json::from_str(&wake_json_text).unwrap();
}

#[test]
fn inspect_cli_process_renders_json_and_text() {
    let harness = DiagnosticsCliHarness::new();
    TaskObservationRepository::new(&harness.db)
        .upsert(&NewTaskObservation::new(
            &harness.project_id,
            "cli-inspect-signature",
            41,
            "pa-project",
            vec!["python".to_owned(), "train.py".to_owned()],
            "done",
            Some(10),
            Some(11),
            Some(12),
            Some("0".to_owned()),
            100,
        ))
        .unwrap();

    let output = harness
        .command()
        .args(["inspect", "41", "--json"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["task_id"], 41);
    assert_eq!(body["latest"]["task_signature"], "cli-inspect-signature");

    let output = harness.command().args(["inspect", "41"]).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("task=41"));
}

#[test]
fn explain_cli_process_renders_json_and_text() {
    let harness = DiagnosticsCliHarness::new();
    let incident = IncidentRepository::new(&harness.db)
        .upsert_active(&NewIncident::new(
            &harness.project_id,
            "cli-incident",
            Some("cli-task-signature"),
            "cli-explain",
            100,
        ))
        .unwrap()
        .incident;

    let output = harness
        .command()
        .args(["explain", &incident.incident_id.to_string(), "--json"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["incident"]["incident_id"], incident.incident_id);
    assert_eq!(body["policy"]["status"], "not_configured");

    let output = harness
        .command()
        .args(["explain", &incident.incident_id.to_string()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("observation ->"));
}

#[test]
fn doctor_cli_process_reports_errors_without_repairing_schema() {
    let harness = DiagnosticsCliHarness::new();
    let connection = harness.db.connect().unwrap();
    connection
        .execute("DROP INDEX events_project_status_idx", [])
        .unwrap();
    let before_cookie: i64 = connection
        .query_row("PRAGMA schema_version", [], |row| row.get(0))
        .unwrap();
    drop(connection);

    let output = harness
        .command()
        .args(["doctor", "--json"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(body["checks"]
        .as_array()
        .unwrap()
        .iter()
        .any(|check| check["status"] == "error"));

    let connection = harness.db.connect().unwrap();
    let after_cookie: i64 = connection
        .query_row("PRAGMA schema_version", [], |row| row.get(0))
        .unwrap();
    let index_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'events_project_status_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(after_cookie, before_cookie);
    assert_eq!(index_count, 0);
}

#[test]
fn steer_help_describes_enqueue_and_bounded_list_options() {
    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("steer"));

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["steer", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("list"));
    assert!(text.contains("MESSAGE"));
    assert!(text.contains("--json"));

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["steer", "list", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("--json"));

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .arg("steer")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("required arguments were not provided")
    );
}

#[test]
fn readme_documents_human_intervention_workflow() {
    let readme = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))
        .expect("README.md should be readable");

    for clause in [
        r#"pueue-agent steer -- "次は learning rate を半分にして""#,
        "pueue-agent steer list",
        "pueue-agent status --json",
        "SQLite へ登録するだけで、agent の起動、Pueue 操作、実行中 process への入力は行いません",
        "FIFO 順で一度だけ、次回の agent run の prompt に渡されますが、1回ですべての pending メッセージを配信するとは限りません",
        "各メッセージは最大 `4,096 bytes` です。1回の run には最大 `16 messages`、合計 `16,384 intervention bytes` までを、残りの prompt budget に収まる範囲で配信します",
        "上限または残りの prompt budget を超える FIFO の後続メッセージ（超過分）は pending のまま、後続の run へ繰り越されます",
        "agent の spawn に失敗した場合、メッセージは pending に戻されるため、次回の run で再試行されます",
        "`pause` または `disable` 中でもメッセージはキューへ登録できますが、配信はせず、resume または enable 後の次回 run まで保持されます",
        "`status --json` はキューの件数などの診断情報を返しますが、メッセージ本文は含めません",
        "実行中の agent は中断しません",
        "介入メッセージによって、安全ポリシーや既存の制約を上書きすることはできません",
    ] {
        assert!(
            readme.contains(clause),
            "README is missing contract clause: {clause}"
        );
    }
}

#[test]
fn wake_cli_persists_scoped_redacted_events_without_running_pueue() {
    let harness = DiagnosticsCliHarness::new();
    let secret = "ghp_abcdefghijklmnopqrstuvwxyz123456";
    let first = harness
        .command()
        .env("PATH", "/definitely-no-pueue")
        .args(["wake", "--reason", &format!("inspect {secret}")])
        .output()
        .unwrap();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(!String::from_utf8_lossy(&first.stdout).contains(secret));
    let second = harness
        .command()
        .args(["wake", "--reason", "inspect current loss", "--json"])
        .output()
        .unwrap();
    assert!(second.status.success());
    let json: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(json["project_id"], harness.project_id);
    let connection = harness.db.connect().unwrap();
    let rows: Vec<(String, String, String)> = connection.prepare("SELECT project_id, dedup_key, payload_json FROM events WHERE kind = 'operator_wake' ORDER BY event_id").unwrap().query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, harness.project_id);
    assert_ne!(rows[0].1, rows[1].1);
    assert!(!rows[0].2.contains(secret));
    let blank = harness
        .command()
        .args(["wake", "--reason", "   "])
        .output()
        .unwrap();
    assert!(!blank.status.success());
    let oversize = "x".repeat(1025);
    assert!(!harness
        .command()
        .args(["wake", "--reason", &oversize])
        .output()
        .unwrap()
        .status
        .success());
}
use std::fs;

use pueue_agent::{
    config,
    db::{
        AgentRunRepository, Db, EventRepository, IncidentRepository, ProjectRepository,
        SubmissionRepository, TaskObservationRepository,
    },
    models::{
        AgentRunStatus, EventKind, NewAgentRun, NewEvent, NewIncident, NewProject, NewSubmission,
        NewTaskObservation,
    },
};
use serde_json::Value;
use tempfile::TempDir;

struct DiagnosticsCliHarness {
    _temp: TempDir,
    root: std::path::PathBuf,
    state_dir: std::path::PathBuf,
    db: Db,
    project_id: String,
}

impl DiagnosticsCliHarness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        pueue_agent::init::run(&root).unwrap();
        let state_dir = temp.path().join("state");
        let db = Db::open(&state_dir.join("state.sqlite3")).unwrap();
        let project_config = config::load(&root.join(".pueue-agent/config.toml")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                &project_config.project_id,
                &root,
                &project_config.pueue_group,
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        Self {
            _temp: temp,
            root,
            state_dir,
            db,
            project_id: project_config.project_id,
        }
    }

    fn command(&self) -> assert_cmd::Command {
        let mut command = assert_cmd::Command::cargo_bin("pueue-agent").unwrap();
        command
            .env("PUEUE_AGENT_STATE_DIR", &self.state_dir)
            .current_dir(&self.root);
        command
    }
}

#[test]
fn cli_output_contract_runs_emits_bounded_json_and_human_lineage_without_sensitive_fields() {
    let harness = DiagnosticsCliHarness::new();
    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            &harness.project_id,
            EventKind::TaskFailed,
            "runs-cli-event",
            serde_json::json!({"prompt": "hidden prompt", "metadata": {"transcript": "hidden transcript"}}),
            100,
            100,
        ))
        .unwrap();
    let run = AgentRunRepository::new(&harness.db)
        .insert(&NewAgentRun::new(
            &harness.project_id,
            event.event_id,
            None,
            AgentRunStatus::Completed,
            101,
            "/tmp/hidden-agent.log",
        ))
        .unwrap();
    let submissions = SubmissionRepository::new(&harness.db);
    submissions
        .insert_idempotent(&NewSubmission::with_kind_metadata(
            "runs-cli-submission",
            &harness.project_id,
            vec![
                "python".to_owned(),
                "train.py".to_owned(),
                "--prompt".to_owned(),
                "hidden command".to_owned(),
            ],
            102,
            pueue_agent::models::SubmissionKind::Experiment,
            serde_json::json!({"transcript": "hidden submission metadata"}),
            Some(run.run_id),
        ))
        .unwrap();
    submissions
        .mark_accepted("runs-cli-submission", 41, "runs-cli-task")
        .unwrap();

    let json = harness
        .command()
        .args(["runs", "--json", "--limit", "1"])
        .output()
        .unwrap();
    assert!(
        json.status.success(),
        "{}",
        String::from_utf8_lossy(&json.stderr)
    );
    let json_text = String::from_utf8_lossy(&json.stdout);
    assert!(json_text.starts_with('{'));
    assert!(!json_text.contains("pueue-agent"), "{json_text}");
    assert!(!json_text.contains('\x1b'), "{json_text}");
    let body: Value = serde_json::from_str(&json_text).unwrap();
    assert_eq!(body["schema_version"], 1);
    assert_eq!(body["runs"][0]["event"]["kind"], "task_failed");
    assert_eq!(body["runs"][0]["submissions"][0]["kind"], "experiment");
    assert_eq!(body["runs"][0]["submissions"][0]["task_id"], 41);

    let human = harness
        .command()
        .args(["runs", "--limit", "1"])
        .output()
        .unwrap();
    assert!(
        human.status.success(),
        "{}",
        String::from_utf8_lossy(&human.stderr)
    );
    let human_text = String::from_utf8_lossy(&human.stdout);
    for expected in ["pueue-agent runs", "run=", "event=", "sub=", "task="] {
        assert!(
            human_text.contains(expected),
            "missing {expected}: {human_text}"
        );
    }
    assert!(human_text.contains("summary:"), "{human_text}");
    assert!(
        human_text.lines().any(|line| {
            line.split_whitespace()
                .any(|field| field == "state=completed")
        }),
        "{human_text}"
    );
    assert!(!human_text.contains('\x1b'), "{human_text}");
    for leaked in [
        "hidden prompt",
        "hidden transcript",
        "hidden submission metadata",
        "hidden command",
        "/tmp/hidden-agent.log",
    ] {
        assert!(
            !json_text.contains(leaked),
            "JSON leaked {leaked}: {json_text}"
        );
        assert!(
            !human_text.contains(leaked),
            "human output leaked {leaked}: {human_text}"
        );
    }
}
