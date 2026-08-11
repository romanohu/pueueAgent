#[path = "../support/fake_pueue.rs"]
mod fake_pueue;

use std::{ffi::OsString, fs, path::PathBuf};

use fake_pueue::{FakePueue, FakePueueCommand};
use pueue_agent::{
    batches::BatchJobResult,
    db::{BatchRepository, Db, EventRepository, ProjectRepository, SubmissionRepository},
    models::{
        AgentRunStatus, EventKind, NewAgentRun, NewBatchJob, NewBatchRequest, NewEvent, NewProject,
        Submission, SubmissionKind, SubmissionStatus,
    },
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
async fn command_adapter_kills_only_the_requested_task_id() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", None);
    let adapter = CommandPueue::new(fixture.executable(), ["--config", "profile path.yml"]);

    adapter.kill(41).await.unwrap();

    assert_eq!(
        fixture.captured_args(),
        vec!["--config", "profile path.yml", "kill", "41"]
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn command_adapter_removes_only_the_requested_task_id() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", None);
    let adapter = CommandPueue::new(fixture.executable(), ["--config", "profile path.yml"]);

    adapter.remove(41).await.unwrap();

    assert_eq!(
        fixture.captured_args(),
        vec!["--config", "profile path.yml", "remove", "41"]
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
async fn command_adapter_skips_group_add_when_group_already_exists() {
    let fixture = FakePueueCommand::new_with_group_lists(
        STATUS_JSON,
        "73\n",
        &[r#"{"default":{"parallel_tasks":1},"pa-project with spaces":{"parallel_tasks":1}}"#],
        None,
    );
    let adapter = CommandPueue::new(fixture.executable(), ["--config", "profile path.yml"]);

    adapter
        .ensure_group("pa-project with spaces")
        .await
        .unwrap();

    assert_eq!(
        fixture.captured_invocations(),
        vec![vec!["--config", "profile path.yml", "group", "-j"]
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>()]
    );
}

#[tokio::test]
async fn command_adapter_adds_missing_group_after_json_list_check() {
    let fixture = FakePueueCommand::new_with_group_lists(
        STATUS_JSON,
        "73\n",
        &[r#"{"default":{"parallel_tasks":1}}"#],
        None,
    );
    let adapter = CommandPueue::new(fixture.executable(), ["--config", "profile path.yml"]);

    adapter
        .ensure_group("pa-project with spaces")
        .await
        .unwrap();

    assert_eq!(
        fixture.captured_invocations(),
        vec![
            vec!["--config", "profile path.yml", "group", "-j"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>(),
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
        ]
    );
}

#[tokio::test]
async fn command_adapter_treats_racing_group_add_as_success_when_group_appears() {
    let fixture = FakePueueCommand::new_with_group_lists(
        STATUS_JSON,
        "73\n",
        &[
            r#"{"default":{"parallel_tasks":1}}"#,
            r#"{"default":{"parallel_tasks":1},"pa-project":{"parallel_tasks":1}}"#,
        ],
        Some("group-add"),
    );
    let adapter = CommandPueue::new(fixture.executable(), Vec::<OsString>::new());

    adapter.ensure_group("pa-project").await.unwrap();

    assert_eq!(
        fixture.captured_invocations(),
        vec![
            vec!["group", "-j"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>(),
            vec!["group", "add", "pa-project"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>(),
            vec!["group", "-j"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        ]
    );
}

#[tokio::test]
async fn command_adapter_preserves_group_add_error_when_group_remains_absent() {
    let fixture = FakePueueCommand::new_with_group_lists(
        STATUS_JSON,
        "73\n",
        &[r#"{"default":{"parallel_tasks":1}}"#],
        Some("group-add"),
    );
    let adapter = CommandPueue::new(fixture.executable(), Vec::<OsString>::new());

    let error = adapter.ensure_group("pa-project").await.unwrap_err();

    match error {
        AppError::Pueue(PueueError::CommandFailed {
            operation,
            exit_code,
            stdout,
            stderr,
        }) => {
            assert_eq!(operation, "group");
            assert_eq!(exit_code, Some(7));
            assert_eq!(stdout, b"partial output");
            assert_eq!(stderr, b"daemon unavailable");
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(
        fixture.captured_invocations(),
        vec![
            vec!["group", "-j"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>(),
            vec!["group", "add", "pa-project"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>(),
            vec!["group", "-j"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        ]
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
async fn remove_non_zero_exit_is_a_typed_error_with_captured_output() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", Some("remove"));
    let adapter = CommandPueue::new(fixture.executable(), Vec::<OsString>::new());

    let error = adapter.remove(41).await.unwrap_err();

    match error {
        AppError::Pueue(PueueError::CommandFailed { operation, .. }) => {
            assert_eq!(operation, "remove");
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
async fn submit_options_default_to_experiment_and_persist_metadata_without_changing_pueue_argv() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    let args = vec![OsString::from("python"), OsString::from("train.py")];
    let metadata = submit::load_metadata(None, Some(r#"{"trial":"baseline","epochs":3}"#)).unwrap();

    let submission = submit::run_with_options(
        &harness.db,
        &harness.root,
        &args,
        &submit::SubmitOptions::new(SubmissionKind::Experiment, metadata, None),
        &fake,
    )
    .await
    .unwrap();

    assert_eq!(submission.kind, SubmissionKind::Experiment);
    assert_eq!(submission.metadata, json!({"trial":"baseline","epochs":3}));
    assert_eq!(submission.origin_agent_run_id, None);
    assert_eq!(
        fake.last_add_args(),
        vec![
            OsString::from("-g"),
            OsString::from("pa-project"),
            OsString::from("--"),
            OsString::from("python"),
            OsString::from("train.py"),
        ]
    );
}

#[tokio::test]
async fn submit_options_accept_explicit_control_kind() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    let args = vec![OsString::from("python"), OsString::from("control.py")];

    let submission = submit::run_with_options(
        &harness.db,
        &harness.root,
        &args,
        &submit::SubmitOptions::new(SubmissionKind::Control, json!({}), None),
        &fake,
    )
    .await
    .unwrap();

    assert_eq!(submission.kind, SubmissionKind::Control);
}

#[test]
fn metadata_loader_rejects_conflicting_or_out_of_bounds_values() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("metadata.json");
    fs::write(&path, r#"{"from":"file"}"#).unwrap();
    let oversized_path = temp.path().join("oversized-metadata.json");
    fs::write(&oversized_path, "x".repeat(16 * 1024 + 1)).unwrap();

    assert!(submit::load_metadata(Some(&path), Some(r#"{"inline":true}"#)).is_err());
    assert!(submit::load_metadata(Some(&oversized_path), None).is_err());
    assert!(submit::load_metadata(None, Some("[]")).is_err());
    assert!(submit::load_metadata(
        None,
        Some(&format!(r#"{{"value":"{}"}}"#, "x".repeat(1025)))
    )
    .is_err());
    assert!(submit::load_metadata(
        None,
        Some(&format!(r#"{{"items":[{}]}}"#, vec!["0"; 65].join(",")))
    )
    .is_err());
    assert!(submit::load_metadata(None, Some(&format!(r#"{{"{}":1}}"#, "k".repeat(65)))).is_err());
    assert!(submit::load_metadata(
        None,
        Some(&format!(
            r#"{{{}}}"#,
            (0..33)
                .map(|index| format!(r#""k{index}":0"#))
                .collect::<Vec<_>>()
                .join(",")
        ))
    )
    .is_err());
    assert!(submit::load_metadata(
        None,
        Some(&format!(
            r#"{{"nested":{}}}"#,
            "{".repeat(8) + "0" + &"}".repeat(8)
        ))
    )
    .is_err());
    assert!(submit::load_metadata(
        None,
        Some(&format!(r#"{{"value":"{}"}}"#, "x".repeat(16 * 1024)))
    )
    .is_err());
}

#[tokio::test]
async fn run_with_options_rejects_oversized_metadata_before_pueue_add() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    let args = vec![OsString::from("python"), OsString::from("train.py")];
    let oversized = serde_json::Value::Object(
        (0..32)
            .map(|index| {
                (
                    format!("key-{index}"),
                    serde_json::Value::String("x".repeat(1024)),
                )
            })
            .collect(),
    );

    let error = submit::run_with_options(
        &harness.db,
        &harness.root,
        &args,
        &submit::SubmitOptions::new(SubmissionKind::Experiment, oversized, None),
        &fake,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        AppError::Validation {
            field: "submit.metadata",
            ..
        }
    ));
    assert!(fake.last_add_args().is_empty());
    assert!(SubmissionRepository::new(&harness.db)
        .find_unreconciled("project-a")
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn invalid_origin_is_rejected_before_pueue_add_and_valid_origin_is_persisted() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    let args = vec![OsString::from("python"), OsString::from("train.py")];

    let invalid = submit::run_with_options(
        &harness.db,
        &harness.root,
        &args,
        &submit::SubmitOptions::new(SubmissionKind::Experiment, json!({}), Some(999)),
        &fake,
    )
    .await;
    assert!(invalid.is_err());
    assert!(fake.last_add_args().is_empty());

    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFinished,
            "origin-test",
            json!({}),
            1,
            1,
        ))
        .unwrap();
    let run = pueue_agent::db::AgentRunRepository::new(&harness.db)
        .insert(&NewAgentRun::new(
            "project-a",
            event.event_id,
            None,
            AgentRunStatus::Running,
            1,
            harness.root.join("agent.log"),
        ))
        .unwrap();
    let submission = submit::run_with_options(
        &harness.db,
        &harness.root,
        &args,
        &submit::SubmitOptions::new(SubmissionKind::Experiment, json!({}), Some(run.run_id)),
        &fake,
    )
    .await
    .unwrap();
    assert_eq!(submission.origin_agent_run_id, Some(run.run_id));
}

#[test]
fn agent_origin_environment_requires_a_complete_matching_pair() {
    assert_eq!(
        submit::origin_from_values(None, None, "project-a").unwrap(),
        None
    );
    assert!(submit::origin_from_values(Some("7"), None, "project-a").is_err());
    assert!(submit::origin_from_values(Some("nope"), Some("project-a"), "project-a").is_err());
    assert!(submit::origin_from_values(Some("7"), Some("project-b"), "project-a").is_err());
    assert_eq!(
        submit::origin_from_values(Some("7"), Some("project-a"), "project-a").unwrap(),
        Some(7)
    );
}

#[test]
fn submission_output_is_bounded_and_never_includes_raw_metadata() {
    let submission = Submission {
        submission_id: "submission-1".to_owned(),
        project_id: "project-a".to_owned(),
        argv: vec!["python".to_owned(), "train.py".to_owned()],
        created_at: 1,
        pueue_task_id: Some(73),
        task_signature: Some("signature".to_owned()),
        status: SubmissionStatus::Accepted,
        kind: SubmissionKind::Control,
        metadata: json!({"credential":"very-secret"}),
        origin_agent_run_id: Some(7),
    };

    let human = submit::render_submission(&submission, "pa-project", false).unwrap();
    assert_eq!(
        human,
        "pueue-agent submit project=project-a\nsub=submission-1 task=73 kind=control group=pa-project state=accepted\nsummary: submission accepted"
    );
    assert!(!human.contains("very-secret"));

    let json = submit::render_submission(&submission, "pa-project", true).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["submission_id"], "submission-1");
    assert_eq!(value["task_id"], 73);
    assert_eq!(value["kind"], "control");
    assert_eq!(value["group"], "pa-project");
    assert_eq!(value["state"], "accepted");
    assert!(value.get("metadata").is_none());
}

#[test]
fn cli_output_contract_submit_has_header_ids_state_summary_and_pure_json() {
    let submission = Submission {
        submission_id: "submission-contract".to_owned(),
        project_id: "project-a".to_owned(),
        argv: vec!["python".to_owned(), "train.py".to_owned()],
        created_at: 1,
        pueue_task_id: Some(73),
        task_signature: Some("signature".to_owned()),
        status: SubmissionStatus::Accepted,
        kind: SubmissionKind::Experiment,
        metadata: json!({"credential":"very-secret"}),
        origin_agent_run_id: None,
    };

    let human = submit::render_submission(&submission, "pa-project", false).unwrap();
    assert!(human.starts_with("pueue-agent submit"), "{human}");
    assert!(human.contains("sub=submission-contract"), "{human}");
    assert!(human.contains("task=73"), "{human}");
    assert!(human
        .split_whitespace()
        .any(|field| field == "state=accepted"));
    assert!(human.contains("summary:"), "{human}");
    assert!(!human.contains("very-secret"), "{human}");
    assert!(!human.contains('\x1b'), "{human}");

    let json = submit::render_submission(&submission, "pa-project", true).unwrap();
    assert!(json.starts_with('{'), "{json}");
    assert!(!json.contains("pueue-agent"), "{json}");
    assert!(!json.contains('\x1b'), "{json}");
    let _: serde_json::Value = serde_json::from_str(&json).unwrap();
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

#[tokio::test]
async fn batch_external_add_result_is_recorded_after_durable_intent() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    let repository = BatchRepository::new(&harness.db);
    repository
        .create_or_get(&NewBatchRequest::new(
            "batch-external-result",
            "project-a",
            "sha256:external-result",
            vec![NewBatchJob::new(
                "job-a",
                0,
                SubmissionKind::Experiment,
                vec!["python".to_owned(), "train.py".to_owned()],
                serde_json::json!({"name": "external-result"}),
            )],
            100,
        ))
        .unwrap();
    let claimed = repository
        .claim("project-a", "batch-external-result", 100, 110)
        .unwrap()
        .unwrap();
    let lease_token = claimed.lease_token.as_deref().unwrap();

    let pueue_task_id = fake
        .add(&[OsString::from("--"), OsString::from("python")])
        .await
        .unwrap();
    let completed = repository
        .record_job_result(
            "project-a",
            "batch-external-result",
            "job-a",
            lease_token,
            BatchJobResult::Accepted {
                pueue_task_id,
                submission_id: "submission-external-result".to_owned(),
            },
            101,
        )
        .unwrap();

    assert_eq!(completed.jobs[0].pueue_task_id, Some(73));
    assert_eq!(
        completed.jobs[0].submission_id.as_deref(),
        Some("submission-external-result")
    );
    assert_eq!(completed.jobs[0].status.to_string(), "accepted");
}

#[tokio::test]
async fn submit_batch_cli_core_flow_uses_pueue_adapter_double_and_shared_renderers() {
    let harness = SubmitHarness::new();
    let manifest_path = harness.root.join("jobs.json");
    fs::write(
        &manifest_path,
        r#"{"jobs":[{"id":"job-a","argv":["python","train.py"],"metadata":{"credential":"hidden"}}]}"#,
    )
    .unwrap();
    let fake = FakePueue::new().with_add_task_id(73);

    let batch = pueue_agent::batches::run_with(
        &harness.db,
        &harness.root,
        "22222222-2222-4222-8222-222222222222",
        &manifest_path,
        None,
        &fake,
    )
    .await
    .unwrap();

    assert_eq!(batch.status.to_string(), "completed");
    assert_eq!(batch.jobs[0].pueue_task_id, Some(73));
    assert!(batch.jobs[0].submission_id.is_some());
    assert_eq!(
        fake.last_add_args(),
        vec![
            OsString::from("-g"),
            OsString::from("pa-project"),
            OsString::from("--"),
            OsString::from("python"),
            OsString::from("train.py"),
        ]
    );

    let human = pueue_agent::batches::render_batch(&batch, "pa-project", false).unwrap();
    assert!(human.starts_with("pueue-agent submit-batch"));
    assert!(human.contains("request=22222222-2222-4222-8222-222222222222"));
    assert!(human.contains("state=completed"));
    assert!(human.contains("accepted=1 failed=0 pending=0"));
    assert!(!human.contains("hidden"));

    let json = pueue_agent::batches::render_batch(&batch, "pa-project", true).unwrap();
    assert!(json.starts_with('{'));
    assert!(!json.contains("pueue-agent"));
    assert!(!json.contains("hidden"));
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["status"], "completed");
    assert_eq!(value["jobs"][0]["task_id"], 73);
}
