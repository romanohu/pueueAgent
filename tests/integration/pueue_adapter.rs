#[path = "../support/fake_pueue.rs"]
mod fake_pueue;

use std::{ffi::OsString, fs, path::PathBuf};

use fake_pueue::{FakePueue, FakePueueCommand};
use pueue_agent::{
    db::{Db, ProjectRepository, SubmissionRepository},
    models::{NewProject, SubmissionStatus},
    pueue::{CommandPueue, PueueApi, PueueError, PueueTask},
    submit, AppError,
};
use serde_json::json;
use tempfile::TempDir;

const STATUS_JSON: &str = r#"{
  "tasks": {
    "41": {
      "id": "41",
      "group": "pa-project",
      "command": "python train.py --name experiment",
      "status": {
        "Done": {
          "enqueued_at": "2026-08-09T10:00:00Z",
          "start": "2026-08-09T10:00:03Z",
          "end": "2026-08-09T10:05:00Z",
          "result": {"Failed": 17}
        }
      }
    }
  }
}"#;

struct SubmitHarness {
    _temp: TempDir,
    db: Db,
    root: PathBuf,
}

impl SubmitHarness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project");
        let config_dir = root.join(".pueue-agent");
        fs::create_dir_all(&config_dir).unwrap();
        let config_path = config_dir.join("config.toml");
        fs::write(
            &config_path,
            r#"
project_id = "project-a"
pueue_group = "pa-project"

[agent]
program = "codex"
args = ["exec", "{prompt}"]
timeout_minutes = 60
max_retries = 2

[check]
interval_minutes = 10
deep_check_every = 6
deep_check_interval_minutes = 0
stall_minutes = 30
extra_log_paths = []

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 3
max_experiments = 20
"#,
        )
        .unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &root,
                "pa-project",
                config_path,
                1,
            ))
            .unwrap();
        Self {
            _temp: temp,
            db,
            root,
        }
    }
}

fn expected_provisional_signature(group: &str, task_id: i64, submission_id: &str) -> String {
    format!("provisional-submit:v1:group={group}:task-id={task_id}:intent={submission_id}")
}

#[tokio::test]
async fn command_adapter_preserves_fixed_and_arbitrary_arguments() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", None);
    let adapter = CommandPueue::new(fixture.executable(), ["--config", "profile path.yml"]);
    let add_args = [
        OsString::from("-g"),
        OsString::from("pa-project"),
        OsString::from("--"),
        OsString::from("python"),
        OsString::from("train.py"),
        OsString::from("--name"),
        OsString::from("a b; echo bad && $(touch nope)"),
    ];

    let task_id = adapter.add(&add_args).await.unwrap();

    assert_eq!(task_id, 73);
    assert_eq!(
        fixture.captured_args(),
        vec![
            "--config",
            "profile path.yml",
            "add",
            "--print-task-id",
            "-g",
            "pa-project",
            "--escape",
            "--",
            "python",
            "train.py",
            "--name",
            "a b; echo bad && $(touch nope)",
        ]
        .into_iter()
        .map(OsString::from)
        .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn command_adapter_provisions_group_without_shell() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", None);
    let adapter = CommandPueue::new(fixture.executable(), ["--config", "profile path.yml"]);

    adapter
        .ensure_group("pa-project with spaces")
        .await
        .unwrap();

    assert_eq!(
        fixture.captured_args(),
        vec![
            "--config",
            "profile path.yml",
            "group",
            "add",
            "pa-project with spaces",
        ]
        .into_iter()
        .map(OsString::from)
        .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn status_json_preserves_task_identity_timestamps_and_result() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", None);
    let adapter = CommandPueue::new(fixture.executable(), Vec::<OsString>::new());

    let tasks = adapter.status_json().await.unwrap();

    assert_eq!(
        tasks,
        vec![PueueTask {
            id: 41,
            group: "pa-project".to_owned(),
            command: "python train.py --name experiment".to_owned(),
            state: "Done".to_owned(),
            enqueued_at: Some("2026-08-09T10:00:00Z".to_owned()),
            started_at: Some("2026-08-09T10:00:03Z".to_owned()),
            ended_at: Some("2026-08-09T10:05:00Z".to_owned()),
            result: Some(json!({"Failed": 17})),
        }]
    );
    assert_eq!(fixture.captured_args(), vec!["status", "--json"]);
}

#[tokio::test]
async fn status_json_rejects_state_details_that_are_not_objects() {
    let status_json = r#"{
      "tasks": {
        "41": {
          "id": "41",
          "group": "pa-project",
          "command": "python train.py",
          "status": {
            "Done": "Success"
          }
        }
      }
    }"#;
    let fixture = FakePueueCommand::new(status_json, "73\n", None);
    let adapter = CommandPueue::new(fixture.executable(), Vec::<OsString>::new());

    let error = adapter.status_json().await.unwrap_err();

    match error {
        AppError::Pueue(PueueError::InvalidStatusTask { reason }) => {
            assert_eq!(reason, "state details must be an object");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn status_json_rejects_wrong_typed_optional_timestamps() {
    let status_json = r#"{
      "tasks": {
        "41": {
          "id": "41",
          "group": "pa-project",
          "command": "python train.py",
          "status": {
            "Running": {
              "enqueued_at": "2026-08-09T10:00:00Z",
              "start": 123
            }
          }
        }
      }
    }"#;
    let fixture = FakePueueCommand::new(status_json, "73\n", None);
    let adapter = CommandPueue::new(fixture.executable(), Vec::<OsString>::new());

    let error = adapter.status_json().await.unwrap_err();

    match error {
        AppError::Pueue(PueueError::InvalidStatusTask { reason }) => {
            assert_eq!(reason, "start timestamp must be a string when present");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn non_zero_exit_is_a_typed_error_with_captured_output() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", Some("kill"));
    let adapter = CommandPueue::new(fixture.executable(), Vec::<OsString>::new());

    let error = adapter.kill(41).await.unwrap_err();

    match error {
        AppError::Pueue(PueueError::CommandFailed {
            operation,
            exit_code,
            stdout,
            stderr,
        }) => {
            assert_eq!(operation, "kill");
            assert_eq!(exit_code, Some(7));
            assert_eq!(stdout, b"partial output");
            assert_eq!(stderr, b"daemon unavailable");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn malformed_status_json_is_a_typed_integration_error() {
    let fixture = FakePueueCommand::new("not-json", "73\n", None);
    let adapter = CommandPueue::new(fixture.executable(), Vec::<OsString>::new());

    let error = adapter.status_json().await.unwrap_err();

    assert!(matches!(
        error,
        AppError::Pueue(PueueError::InvalidStatusJson { .. })
    ));
}

#[tokio::test]
async fn submit_records_intent_before_add_and_preserves_arguments() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    fake.pause_add();
    let args = vec![
        OsString::from("python"),
        OsString::from("train.py"),
        OsString::from("--name"),
        OsString::from("a b; echo bad"),
    ];
    let task_db = harness.db.clone();
    let task_root = harness.root.clone();
    let task_fake = fake.clone();
    let submit_task =
        tokio::spawn(
            async move { submit::run_with(&task_db, &task_root, &args, &task_fake).await },
        );

    fake.wait_for_add().await;
    let pending = SubmissionRepository::new(&harness.db)
        .find_unreconciled("project-a")
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status, SubmissionStatus::Pending);
    assert_eq!(
        pending[0].argv,
        vec!["python", "train.py", "--name", "a b; echo bad"]
    );
    assert_eq!(
        fake.last_add_args(),
        vec![
            OsString::from("-g"),
            OsString::from("pa-project"),
            OsString::from("--"),
            OsString::from("python"),
            OsString::from("train.py"),
            OsString::from("--name"),
            OsString::from("a b; echo bad"),
        ]
    );

    fake.release_add();
    let accepted = submit_task.await.unwrap().unwrap();
    assert_eq!(accepted.status, SubmissionStatus::Accepted);
    assert_eq!(accepted.pueue_task_id, Some(73));
    assert_eq!(
        accepted.task_signature.as_deref(),
        Some(expected_provisional_signature("pa-project", 73, &accepted.submission_id).as_str())
    );
}

#[tokio::test]
async fn submit_keeps_pending_intent_when_add_fails() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_failure();
    let args = vec![
        OsString::from("python"),
        OsString::from("train.py"),
        OsString::from("--name"),
        OsString::from("a b; echo bad"),
    ];

    let error = submit::run_with(&harness.db, &harness.root, &args, &fake)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        AppError::Pueue(PueueError::CommandFailed {
            operation: "add",
            exit_code: Some(7),
            ..
        })
    ));
    let pending = SubmissionRepository::new(&harness.db)
        .find_unreconciled("project-a")
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status, SubmissionStatus::Pending);
    assert_eq!(pending[0].pueue_task_id, None);
    assert_eq!(pending[0].task_signature, None);
    assert_eq!(
        pending[0].argv,
        vec!["python", "train.py", "--name", "a b; echo bad"]
    );
}

#[tokio::test]
async fn submit_provisional_signature_uses_submission_intent_to_avoid_task_id_collisions() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    let args = vec![OsString::from("python"), OsString::from("train.py")];

    let first = submit::run_with(&harness.db, &harness.root, &args, &fake)
        .await
        .unwrap();
    let second = submit::run_with(&harness.db, &harness.root, &args, &fake)
        .await
        .unwrap();

    assert_eq!(first.pueue_task_id, Some(73));
    assert_eq!(second.pueue_task_id, Some(73));
    assert_ne!(first.task_signature, second.task_signature);
    assert_eq!(
        first.task_signature.as_deref(),
        Some(expected_provisional_signature("pa-project", 73, &first.submission_id).as_str())
    );
    assert_eq!(
        second.task_signature.as_deref(),
        Some(expected_provisional_signature("pa-project", 73, &second.submission_id).as_str())
    );
}
