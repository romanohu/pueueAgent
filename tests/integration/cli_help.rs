#[test]
fn help_lists_diagnostics_commands_and_status_json_option() {
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

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["status", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("--json"));
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
use std::fs;

use pueue_agent::{
    config,
    db::{Db, EventRepository, ProjectRepository},
    models::{EventKind, NewEvent, NewProject},
};
use serde_json::Value;
use tempfile::TempDir;
