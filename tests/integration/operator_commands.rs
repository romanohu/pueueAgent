use std::{
    collections::VecDeque,
    ffi::OsString,
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};

#[cfg(unix)]
use std::path::Path;

use async_trait::async_trait;
use clap::Parser;
use rusqlite::types::Value;
use pueue_agent::{
    agent::{AgentRunner, AgentRunnerConfig},
    cancel::{cancel_task_with, render_cancel_result},
    daemon::{Daemon, DaemonConfig},
    db::{
        AgentRunRepository, CampaignRepository, Db, EventRepository, ExperimentRepository,
        HealthRepository, IncidentRepository, ProjectRepository, StartCampaignRequest,
        ResearchRepository, TaskObservationRepository, TerminationRequestRepository,
    },
    diagnostics::render_project_status_json,
    models::{
        AgentContextMode, AgentRunStatus, CampaignState, EventKind, ExecutionProjection,
        ExperimentTerminalOutcome, HealthState, NewAgentRun, NewEvent, NewIncident, NewProject,
        NewTaskObservation, NewTerminationRequest, ProposalKind, SignalSummaryEntry,
        TerminationRequestStatus,
    },
    proposals::{self, ProposalInput},
    pueue::{PueueApi, PueueTask},
    service::ServiceStatus,
    status::{self, DisableMode, PueueSnapshot, StatusInput},
    AppError,
};
use serde_json::json;
use tempfile::TempDir;

#[cfg(unix)]
#[path = "../support/execution_policy_fixture.rs"]
mod execution_policy_fixture;

const CODEX_SESSION_ID: &str = "019f9f30-5f31-7a40-8e28-bd95e1f6c537";

#[cfg(unix)]
#[test]
fn read_only_cli_actions_do_not_create_missing_database_or_policy_state() {
    let temporary = TempDir::new().unwrap();
    let project_root = temporary.path().join("project");
    fs::create_dir(&project_root).unwrap();
    pueue_agent::init::run(&project_root).unwrap();
    let state_dir = temporary.path().join("state");
    fs::create_dir(&state_dir).unwrap();
    let codex_home = temporary.path().join("codex-home");
    fs::create_dir(&codex_home).unwrap();
    let pueue_config = temporary.path().join("pueue.yml");
    fs::write(&pueue_config, "fixture: true\n").unwrap();

    let root = project_root.display().to_string();
    let config = pueue_config.display().to_string();
    let commands = [
        vec![
            "events".to_owned(),
            root.clone(),
            "--pueue-config".to_owned(),
            config.clone(),
        ],
        vec![
            "inspect".to_owned(),
            "42".to_owned(),
            root.clone(),
            "--pueue-config".to_owned(),
            config.clone(),
        ],
        vec![
            "explain".to_owned(),
            "42".to_owned(),
            root.clone(),
            "--pueue-config".to_owned(),
            config.clone(),
        ],
        vec![
            "steer".to_owned(),
            "--project-root".to_owned(),
            root,
            "--pueue-config".to_owned(),
            config,
            "list".to_owned(),
        ],
    ];
    for arguments in commands {
        let output = assert_cmd::Command::cargo_bin("pueue-agent")
            .unwrap()
            .env("HOME", temporary.path())
            .env("CODEX_HOME", &codex_home)
            .env("PUEUE_AGENT_STATE_DIR", &state_dir)
            .args(arguments)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(!state_dir.join("state.sqlite3").exists());
        assert!(!state_dir.join("execution-policy.toml").exists());
    }
}

#[cfg(unix)]
#[test]
fn diagnostic_and_steer_list_commands_do_not_migrate_a_legacy_database() {
    use std::os::unix::fs::PermissionsExt;

    let temporary = TempDir::new().unwrap();
    let project_root = temporary.path().join("project");
    fs::create_dir(&project_root).unwrap();
    pueue_agent::init::run(&project_root).unwrap();
    let config_path = project_root.join(".pueue-agent/config.toml");
    let state_dir = temporary.path().join("state");
    fs::create_dir(&state_dir).unwrap();
    fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700)).unwrap();
    let database_path = state_dir.join("state.sqlite3");
    let db = Db::open(&database_path).unwrap();
    ProjectRepository::new(&db)
        .register(&NewProject::new(
            "project-a",
            &project_root,
            "pa-project-a",
            &config_path,
            1,
        ))
        .unwrap();
    drop(db);

    let _policy = execution_policy_fixture::resolved_policy(
        temporary.path(),
        &[("project-a", project_root.as_path(), Path::new("codex"))],
    );
    let policy_fixture_state = temporary.path().join("execution-policy-state");
    fs::copy(
        policy_fixture_state.join("execution-policy.toml"),
        state_dir.join("execution-policy.toml"),
    )
    .unwrap();
    fs::set_permissions(
        state_dir.join("execution-policy.toml"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let connection = rusqlite::Connection::open(&database_path).unwrap();
    connection.execute_batch("PRAGMA user_version = 14;").unwrap();
    drop(connection);

    let pueue_config = temporary.path().join("execution-policy-pueue.yml");
    let codex_home = temporary.path().join("execution-policy-codex-home");
    let config_argument = pueue_config.display().to_string();
    let commands = [
        vec![
            "events".to_owned(),
            "--pueue-config".to_owned(),
            config_argument.clone(),
            project_root.display().to_string(),
        ],
        vec![
            "inspect".to_owned(),
            "--pueue-config".to_owned(),
            config_argument.clone(),
            "42".to_owned(),
            project_root.display().to_string(),
        ],
        vec![
            "explain".to_owned(),
            "--pueue-config".to_owned(),
            config_argument.clone(),
            "42".to_owned(),
            project_root.display().to_string(),
        ],
        vec![
            "steer".to_owned(),
            "--project-root".to_owned(),
            project_root.display().to_string(),
            "--pueue-config".to_owned(),
            config_argument,
            "list".to_owned(),
        ],
    ];
    for arguments in commands {
        let _ = assert_cmd::Command::cargo_bin("pueue-agent")
            .unwrap()
            .env("HOME", temporary.path())
            .env("CODEX_HOME", &codex_home)
            .env("PUEUE_AGENT_STATE_DIR", &state_dir)
            .args(arguments)
            .output()
            .unwrap();
        let version: i64 = rusqlite::Connection::open(&database_path)
            .unwrap()
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 14);
        assert!(!database_path.with_extension("sqlite3-wal").exists());
    }
}

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

#[test]
fn cancel_command_requires_one_explicit_task_id() {
    let cli = pueue_agent::cli::Cli::try_parse_from([
        "pueue-agent",
        "cancel",
        "--task-id",
        "41",
        "--json",
        "project",
    ])
    .unwrap();
    match cli.command {
        pueue_agent::cli::Command::Cancel(args) => {
            assert_eq!(args.task_id, 41);
            assert!(args.json);
            assert_eq!(
                args.project_root.as_deref(),
                Some(std::path::Path::new("project"))
            );
        }
        _ => panic!("expected cancel command"),
    }

    assert!(pueue_agent::cli::Cli::try_parse_from(["pueue-agent", "cancel"]).is_err());
    assert!(pueue_agent::cli::Cli::try_parse_from(["pueue-agent", "cancel", "--all"])
        .is_err());
}

#[test]
fn trial_cli_contract_accepts_timeout_bounds_and_complete_metric_flags() {
    for timeout in ["1", "60", "300"] {
        let parsed = pueue_agent::cli::Cli::try_parse_from([
            "pueue-agent",
            "trial",
            "--timeout-seconds",
            timeout,
            "--metric-name",
            "loss",
            "--metric-direction",
            "minimize",
            "--metric-min-delta",
            "0.1",
            "--json",
            "--",
            "python",
            "train.py",
        ]);
        assert!(parsed.is_ok(), "timeout {timeout} should parse: {parsed:?}");
    }

    let default =
        pueue_agent::cli::Cli::try_parse_from(["pueue-agent", "trial", "--", "python", "train.py"])
            .unwrap();
    assert!(format!("{default:?}").contains("timeout_seconds: 60"));

    for arguments in [
        vec!["trial", "--timeout-seconds", "0", "--", "true"],
        vec!["trial", "--timeout-seconds", "301", "--", "true"],
        vec!["trial", "--metric-name", "loss", "--", "true"],
        vec!["trial", "--metric-direction", "minimize", "--", "true"],
        vec!["trial", "--metric-min-delta", "0.1", "--", "true"],
        vec![
            "trial",
            "--metric-name",
            "loss",
            "--metric-direction",
            "minimize",
            "--metric-min-delta",
            "-0.1",
            "--",
            "true",
        ],
        vec!["trial", "--"],
    ] {
        let mut full = vec!["pueue-agent"];
        full.extend(arguments);
        assert!(
            pueue_agent::cli::Cli::try_parse_from(full).is_err(),
            "invalid trial arguments must be rejected"
        );
    }
}

#[cfg(unix)]
#[test]
fn trial_rejects_non_utf8_command_argument_before_database_setup() {
    use std::os::unix::ffi::OsStringExt;

    let temporary = TempDir::new().unwrap();
    let state_dir = temporary.path().join("state");
    fs::create_dir(&state_dir).unwrap();
    let invalid_argument = OsString::from_vec(vec![b'c', 0xff]);
    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .env("PUEUE_AGENT_STATE_DIR", &state_dir)
        .current_dir(temporary.path())
        .args(["trial", "--"])
        .arg(invalid_argument)
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("must be valid UTF-8"), "{stderr}");
    assert!(!state_dir.join("state.sqlite3").exists());
}

#[cfg(unix)]
#[test]
fn trial_cli_runs_one_isolated_task_and_keeps_the_database_read_only() {
    let harness = TrialCliHarness::new();
    let before = trial_cli_database_snapshot(&harness.database_path);
    let output = harness
        .command()
        .args([
            "trial",
            "--metric-name",
            &harness.metric_name,
            "--metric-direction",
            "minimize",
            "--",
        ])
        .arg(&harness.child)
        .arg(&harness.secret)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let human = String::from_utf8_lossy(&output.stdout);
    assert!(human.contains("outcome: succeeded"), "{human}");
    assert!(human.contains("terminal: succeeded"), "{human}");
    assert!(human.contains("manifest: valid"), "{human}");
    assert!(human.contains("selected_metric_value: 1.25"), "{human}");
    assert!(human.contains("task_cleanup: confirmed"), "{human}");
    assert!(human.contains("group_cleanup: confirmed"), "{human}");
    assert!(human.contains("output_cleanup: confirmed"), "{human}");
    assert!(!human.contains(&harness.secret));
    assert!(!human.contains(".pueue-agent/trials/"));

    let observation = fs::read_to_string(&harness.child_observation).unwrap();
    assert!(observation.contains(&format!("cwd={}\n", harness.root.display())));
    assert!(observation.contains("result=.pueue-agent/trials/"));
    assert!(observation.contains("artifact=.pueue-agent/trials/"));
    assert!(observation.contains("experiment_id="));
    assert!(observation.contains("campaign_id="));
    assert!(observation.contains("result_path="));
    assert!(observation.contains("artifact_dir="));
    assert_eq!(
        fs::read(harness.root.join("trial-input.txt")).unwrap(),
        b"fixture input"
    );
    assert!(!harness.codex_marker.exists());
    assert!(!harness.task_state.exists());
    assert!(!harness.group_state.exists());

    let calls = fs::read_to_string(&harness.pueue_calls).unwrap();
    let add_group = calls
        .lines()
        .find_map(|line| line.strip_prefix("add:"))
        .expect("one trial Pueue add");
    assert!(add_group.starts_with("pueue-agent-trial-"));
    assert_eq!(add_group.len(), 50);
    assert_ne!(add_group, harness.project_group);
    assert!(calls.contains("remove:41"));
    assert!(calls.contains(&format!("group-remove:{add_group}")));

    let after = trial_cli_database_snapshot(&harness.database_path);
    assert_eq!(before, after);
}

#[cfg(unix)]
#[test]
fn trial_json_failure_is_bounded_and_exits_after_cleanup() {
    let harness = TrialCliHarness::new();
    let before = trial_cli_database_snapshot(&harness.database_path);
    let output = harness
        .command()
        .args([
            "trial",
            "--metric-name",
            &harness.metric_name,
            "--metric-direction",
            "minimize",
            "--json",
            "--",
        ])
        .arg(&harness.child)
        .arg(&harness.secret)
        .arg("--fail")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["outcome"], "command_failed");
    assert_eq!(report["terminal"], "failed");
    assert_eq!(report["manifest"], "not_read");
    assert_eq!(report["task_cleanup"], "confirmed");
    assert_eq!(report["group_cleanup"], "confirmed");
    assert_eq!(report["output_cleanup"], "confirmed");
    assert!(!stdout.contains(&harness.secret));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(&harness.secret));
    assert!(!harness.task_state.exists());
    assert!(!harness.group_state.exists());
    assert_eq!(before, trial_cli_database_snapshot(&harness.database_path));
}

#[cfg(unix)]
#[test]
fn trial_json_preflight_error_does_not_fabricate_a_report_or_leak_inputs() {
    let harness = TrialCliHarness::new();
    fs::write(harness.root.join(".pueue-agent/STATE.md"), "").unwrap();
    let before = trial_cli_database_snapshot(&harness.database_path);
    let output = harness
        .command()
        .args(["trial", "--json", "--"])
        .arg(&harness.child)
        .arg(&harness.secret)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("trial stopped before a bounded report was available"),
        "{stderr}"
    );
    assert!(!stderr.contains(&harness.secret));
    assert!(!stderr.contains(&harness.root.display().to_string()));
    assert!(!stderr.contains(&harness.child.display().to_string()));
    assert!(!harness.pueue_calls.exists());
    assert!(!harness.root.join(".pueue-agent/trials").exists());
    assert_eq!(before, trial_cli_database_snapshot(&harness.database_path));
}

#[cfg(unix)]
struct TrialCliHarness {
    _temporary: TempDir,
    root: std::path::PathBuf,
    database_path: std::path::PathBuf,
    project_group: String,
    metric_name: String,
    secret: String,
    child: std::path::PathBuf,
    child_observation: std::path::PathBuf,
    codex_marker: std::path::PathBuf,
    pueue_calls: std::path::PathBuf,
    task_state: std::path::PathBuf,
    group_state: std::path::PathBuf,
    state_dir: std::path::PathBuf,
    home: std::path::PathBuf,
    codex_home: std::path::PathBuf,
    trusted_dir: std::path::PathBuf,
}

#[cfg(unix)]
impl TrialCliHarness {
    fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;

        let temporary = TempDir::new().unwrap();
        let temporary_root = fs::canonicalize(temporary.path()).unwrap();
        let root = temporary_root.join("project");
        fs::create_dir(&root).unwrap();
        pueue_agent::init::run(&root).unwrap();
        fs::write(
            root.join(".pueue-agent/STATE.md"),
            "Minimize validation loss below 0.10.\n",
        )
        .unwrap();
        fs::write(root.join("trial-input.txt"), b"fixture input").unwrap();

        let state_dir = temporary_root.join("state");
        fs::create_dir(&state_dir).unwrap();
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700)).unwrap();
        let database_path = state_dir.join("state.sqlite3");
        let db = Db::open(&database_path).unwrap();
        let project_config =
            pueue_agent::config::load(&root.join(".pueue-agent/config.toml")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                &project_config.project_id,
                &root,
                &project_config.pueue_group,
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        db.connect()
            .unwrap()
            .execute(
                "UPDATE projects SET paused = 1 WHERE project_id = ?1",
                [&project_config.project_id],
            )
            .unwrap();

        let home = temporary_root.join("home");
        let codex_home = temporary_root.join("codex-home");
        let trusted_dir = temporary_root.join("trusted-bin");
        fs::create_dir(&home).unwrap();
        fs::create_dir(&codex_home).unwrap();
        fs::create_dir(&trusted_dir).unwrap();
        fs::set_permissions(&home, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&codex_home, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&trusted_dir, fs::Permissions::from_mode(0o700)).unwrap();

        let pueue_config = home.join(".config/pueue/pueue.yml");
        fs::create_dir_all(pueue_config.parent().unwrap()).unwrap();
        fs::write(&pueue_config, "fixture: true\n").unwrap();
        fs::set_permissions(&pueue_config, fs::Permissions::from_mode(0o600)).unwrap();

        let pueue = trusted_dir.join("pueue");
        let pueue_calls = temporary_root.join("pueue-calls.log");
        let task_state = temporary_root.join("pueue-task.state");
        let group_state = temporary_root.join("pueue-group.state");
        compile_trial_pueue(&pueue, &pueue_calls, &task_state, &group_state);
        let codex = trusted_dir.join("codex");
        fs::write(
            &codex,
            "#!/bin/sh\nprintf invoked > \"$0.marker\"\nexit 0\n",
        )
        .unwrap();
        fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
        let codex_marker = codex.with_extension("marker");

        let policy_path = state_dir.join("execution-policy.toml");
        fs::write(
            &policy_path,
            format!(
                "version = 1\ntrusted_path = {:?}\n\n[executables]\ncodex = {:?}\npueue = {:?}\n",
                trusted_dir.display().to_string(),
                codex.display().to_string(),
                pueue.display().to_string(),
            ),
        )
        .unwrap();
        fs::set_permissions(&policy_path, fs::Permissions::from_mode(0o600)).unwrap();

        let secret = "trial-secret-sentinel-6e2ab1".to_owned();
        let metric_name = format!("API_KEY={secret}");
        let child = temporary_root.join("trial-child");
        let child_observation = temporary_root.join("trial-child-observation.txt");
        compile_trial_child(&child, &root, &child_observation, &secret, &metric_name);

        Self {
            _temporary: temporary,
            root,
            database_path,
            project_group: project_config.pueue_group,
            metric_name,
            secret,
            child,
            child_observation,
            codex_marker,
            pueue_calls,
            task_state,
            group_state,
            state_dir,
            home,
            codex_home,
            trusted_dir,
        }
    }

    fn command(&self) -> assert_cmd::Command {
        let mut command = assert_cmd::Command::cargo_bin("pueue-agent").unwrap();
        command
            .env("PUEUE_AGENT_STATE_DIR", &self.state_dir)
            .env("HOME", &self.home)
            .env("CODEX_HOME", &self.codex_home)
            .env("PATH", &self.trusted_dir)
            .env_remove("PUEUE_CONFIG")
            .current_dir(&self.root);
        command
    }
}

#[cfg(unix)]
fn compile_trial_pueue(
    target: &std::path::Path,
    calls: &std::path::Path,
    task_state: &std::path::Path,
    group_state: &std::path::Path,
) {
    use std::os::unix::fs::PermissionsExt;

    let source_path = target.with_extension("rs");
    let source = r#"
use std::{env, fs, fs::OpenOptions, io::Write, path::Path, process::Command};

const CALLS: &str = __CALLS__;
const TASK_STATE: &str = __TASK_STATE__;
const GROUP_STATE: &str = __GROUP_STATE__;
const SECRET: &str = __SECRET__;

fn quote(value: &str) -> String {
    let mut quoted = String::from("\"");
    for character in value.chars() {
        match character {
            '\"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            character if character.is_control() => {
                quoted.push_str(&format!("\\u{:04x}", character as u32));
            }
            character => quoted.push(character),
        }
    }
    quoted.push('\"');
    quoted
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value.bytes().all(|byte| matches!(
            byte,
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'@' | b'%' | b'_' | b'+' |
                b'=' | b',' | b'.' | b'/' | b'-'
        ))
    {
        value.to_owned()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn log_call(value: &str) {
    writeln!(OpenOptions::new().create(true).append(true).open(CALLS).unwrap(), "{value}").unwrap();
}

fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let operation = ["status", "group", "add", "kill", "remove"]
        .iter()
        .find(|operation| args.iter().any(|argument| argument == **operation))
        .copied()
        .unwrap_or("");
    let operation_index = args.iter().position(|argument| argument == operation).unwrap();
    let operation_args = &args[operation_index + 1..];

    match operation {
        "status" => {
            log_call("status");
            let Ok(task) = fs::read_to_string(TASK_STATE) else {
                println!("{{\"tasks\":{{}}}}");
                return;
            };
            let fields = task.splitn(4, '\n').collect::<Vec<_>>();
            let mut output = String::from("{\"tasks\":{\"41\":{\"id\":41,\"group\":");
            output.push_str(&quote(fields[0]));
            output.push_str(",\"command\":");
            output.push_str(&quote(fields[1]));
            output.push_str(",\"status\":{");
            output.push_str(&quote(fields[2]));
            output.push_str(":{\"enqueued_at\":\"100\",\"start\":\"100\",\"end\":\"101\",\"result\":");
            output.push_str(&quote(fields[3]));
            output.push_str("}}}}}");
            println!("{output}");
        }
        "group" if operation_args.first().is_some_and(|argument| argument == "-j") => {
            let Ok(group) = fs::read_to_string(GROUP_STATE) else {
                log_call("group-json");
                println!("{{}}");
                return;
            };
            log_call("group-json");
            println!("{{{}:{{\"status\":\"Running\"}}}}", quote(group.trim()));
        }
        "group" if operation_args.first().is_some_and(|argument| argument == "add") => {
            let group = operation_args.get(1).expect("group name");
            fs::write(GROUP_STATE, group).unwrap();
            log_call(&format!("group-add:{group}"));
        }
        "group" if operation_args.first().is_some_and(|argument| argument == "remove") => {
            let group = operation_args.get(1).expect("group name");
            let _ = fs::remove_file(GROUP_STATE);
            log_call(&format!("group-remove:{group}"));
        }
        "add" => {
            let group_index = operation_args.iter().position(|argument| argument == "-g").unwrap();
            let group = operation_args[group_index + 1].clone();
            let directory_index = operation_args.iter().position(|argument| argument == "--working-directory").unwrap();
            let working_directory = &operation_args[directory_index + 1];
            let separator_index = operation_args.iter().position(|argument| argument == "--").unwrap();
            let argv = &operation_args[separator_index + 1..];
            let command = argv.iter().map(|argument| shell_quote(argument)).collect::<Vec<_>>().join(" ");
            let mut child = Command::new(argv.first().expect("runtime executable"));
            child.args(&argv[1..]).current_dir(working_directory).env("TASK_SECRET_SENTINEL", SECRET);
            let status = child.status().expect("launch one trial command");
            let succeeded = status.success();
            let state = if succeeded { "Done" } else { "Failed" };
            let result = if succeeded { "Success" } else { "Failed" };
            fs::write(TASK_STATE, format!("{group}\n{command}\n{state}\n{result}" )).unwrap();
            log_call(&format!("add:{group}"));
            println!("41");
        }
        "remove" => {
            let task_id = operation_args.first().unwrap();
            let _ = fs::remove_file(TASK_STATE);
            log_call(&format!("remove:{task_id}"));
        }
        "kill" => log_call(&format!("kill:{}", operation_args.first().unwrap_or(&String::new()))),
        _ => panic!("unexpected Pueue operation"),
    }
}
"#
    .replace("__CALLS__", &format!("{:?}", calls.to_string_lossy()))
    .replace("__TASK_STATE__", &format!("{:?}", task_state.to_string_lossy()))
    .replace("__GROUP_STATE__", &format!("{:?}", group_state.to_string_lossy()))
    .replace("__SECRET__", &format!("{:?}", "trial-secret-sentinel-6e2ab1"));
    fs::write(&source_path, source).unwrap();
    let output = std::process::Command::new("rustc")
        .args(["--edition=2021", "-O", "-o"])
        .arg(target)
        .arg(&source_path)
        .output()
        .expect("compile disposable Pueue fixture");
    assert!(
        output.status.success(),
        "disposable Pueue fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::set_permissions(target, fs::Permissions::from_mode(0o700)).unwrap();
}

#[cfg(unix)]
fn compile_trial_child(
    target: &std::path::Path,
    root: &std::path::Path,
    observation: &std::path::Path,
    secret: &str,
    metric_name: &str,
) {
    use std::os::unix::fs::PermissionsExt;

    let source_path = target.with_extension("rs");
    let source = r#"
use std::{env, fs, path::{Path, PathBuf}, process};

const ROOT: &str = __ROOT__;
const OBSERVATION: &str = __OBSERVATION__;
const SECRET: &str = __SECRET__;
const METRIC_NAME: &str = __METRIC_NAME__;

fn main() {
    let root = PathBuf::from(ROOT);
    let cwd = env::current_dir().unwrap();
    assert_eq!(cwd, root);
    assert_eq!(fs::read(root.join("trial-input.txt")).unwrap(), b"fixture input");
    let experiment_id = env::var("PUEUE_AGENT_EXPERIMENT_ID").unwrap();
    let campaign_id = env::var("PUEUE_AGENT_CAMPAIGN_ID").unwrap();
    let result = PathBuf::from(env::var_os("PUEUE_AGENT_RESULT_PATH").unwrap());
    let artifact = PathBuf::from(env::var_os("PUEUE_AGENT_ARTIFACT_DIR").unwrap());
    let expected_trials = root.join(".pueue-agent/trials");
    let generation = result.parent().unwrap();
    assert!(generation.starts_with(&expected_trials));
    assert_eq!(result.file_name().unwrap(), "result.json");
    assert_eq!(artifact, generation.join("artifacts"));
    let generation_id = generation.file_name().unwrap().to_string_lossy();
    assert_eq!(generation_id.len(), 36);
    assert_eq!(env::var("TASK_SECRET_SENTINEL").unwrap(), SECRET);
    let args = env::args().skip(1).collect::<Vec<_>>();
    assert!(args.contains(&SECRET.to_owned()));

    fs::create_dir_all(&artifact).unwrap();
    fs::write(artifact.join("fixture.bin"), b"fixture artifact").unwrap();
    let observation = format!(
        "cwd={}\nresult={}\nartifact={}\nexperiment_id={}\ncampaign_id={}\nresult_path={}\nartifact_dir={}\n",
        cwd.display(),
        result.strip_prefix(&root).unwrap().display(),
        artifact.strip_prefix(&root).unwrap().display(),
        experiment_id,
        campaign_id,
        result.display(),
        artifact.display(),
    );
    fs::write(OBSERVATION, observation).unwrap();
    let manifest = format!(
        "{{\"schema_version\":1,\"experiment_id\":\"{experiment_id}\",\"metrics\":{{\"{METRIC_NAME}\":1.25}},\"private_note\":\"{SECRET}\"}}"
    );
    fs::write(&result, manifest).unwrap();
    if args.iter().any(|argument| argument == "--fail") {
        process::exit(7);
    }
    assert!(Path::new(&result).exists());
}
"#
    .replace("__ROOT__", &format!("{:?}", root.to_string_lossy()))
    .replace("__OBSERVATION__", &format!("{:?}", observation.to_string_lossy()))
    .replace("__SECRET__", &format!("{:?}", secret))
    .replace("__METRIC_NAME__", &format!("{:?}", metric_name));
    fs::write(&source_path, source).unwrap();
    let output = std::process::Command::new("rustc")
        .args(["--edition=2021", "-O", "-o"])
        .arg(target)
        .arg(&source_path)
        .output()
        .expect("compile disposable trial command");
    assert!(
        output.status.success(),
        "disposable trial command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::set_permissions(target, fs::Permissions::from_mode(0o700)).unwrap();
}

#[cfg(unix)]
fn trial_cli_database_snapshot(
    path: &std::path::Path,
) -> std::collections::BTreeMap<String, Vec<Vec<String>>> {
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let mut snapshot = std::collections::BTreeMap::new();
    for table in [
        "campaigns",
        "proposals",
        "experiments",
        "submissions",
        "budget_reservations",
        "agent_runs",
        "research_reviews",
        "events",
        "integration_events",
        "task_observations",
    ] {
        let mut statement = connection
            .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
            .unwrap();
        let column_count = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..column_count)
                    .map(|index| row.get::<_, Value>(index).map(|value| format!("{value:?}")))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        snapshot.insert(table.to_owned(), rows);
    }
    snapshot
}

#[cfg(unix)]
#[test]
fn service_lifecycle_commands_report_only_verified_service_state() {
    let temp = TempDir::new().unwrap();
    let bin = temp.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    let launchctl = bin.join("launchctl");
    let systemctl = bin.join("systemctl");
    write_service_shim(&launchctl, "#!/bin/sh\necho 'state = running'\nexit 0\n");
    write_service_shim(
        &systemctl,
        "#!/bin/sh\nif [ \"$2\" = show ]; then echo loaded; fi\nif [ \"$2\" = is-active ]; then echo active; fi\nexit 0\n",
    );
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

    write_service_shim(
        &launchctl,
        "#!/bin/sh\nif [ \"$1\" = print ]; then echo 'Could not find service' >&2; exit 1; fi\nexit 0\n",
    );
    write_service_shim(
        &systemctl,
        "#!/bin/sh\nif [ \"$2\" = show ]; then echo not-found; fi\nif [ \"$2\" = is-active ]; then echo inactive; fi\nexit 0\n",
    );
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

#[cfg(unix)]
#[test]
fn stop_service_manager_failure_emits_no_success_output() {
    let temp = TempDir::new().unwrap();
    let bin = temp.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    let launchctl = bin.join("launchctl");
    let systemctl = bin.join("systemctl");
    write_service_shim(
        &launchctl,
        "#!/bin/sh\nif [ \"$1\" = bootout ]; then exit 1; fi\nexit 0\n",
    );
    write_service_shim(
        &systemctl,
        "#!/bin/sh\nif [ \"$2\" = stop ]; then exit 1; fi\nexit 0\n",
    );
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .current_dir(temp.path())
        .env("HOME", temp.path())
        .env("PATH", path)
        .args(["stop", "--json"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("service command failed"));
}

#[cfg(unix)]
fn write_service_shim(path: &std::path::Path, contents: &str) {
    use std::os::unix::fs::PermissionsExt;

    fs::write(path, contents).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

#[derive(Clone)]
struct OperatorPueue {
    status_responses: Arc<Mutex<VecDeque<Result<Vec<PueueTask>, String>>>>,
    kill_calls: Arc<Mutex<Vec<i64>>>,
    remove_calls: Arc<Mutex<Vec<i64>>>,
    remove_error: Arc<Mutex<Option<String>>>,
    status_calls: Arc<Mutex<usize>>,
}

impl OperatorPueue {
    fn with_tasks(tasks: Vec<PueueTask>) -> Self {
        Self {
            status_responses: Arc::new(Mutex::new(VecDeque::from([Ok(tasks)]))),
            kill_calls: Arc::new(Mutex::new(Vec::new())),
            remove_calls: Arc::new(Mutex::new(Vec::new())),
            remove_error: Arc::new(Mutex::new(None)),
            status_calls: Arc::new(Mutex::new(0)),
        }
    }

    fn with_status_responses(statuses: Vec<Vec<PueueTask>>) -> Self {
        Self {
            status_responses: Arc::new(Mutex::new(
                statuses.into_iter().map(Ok).collect(),
            )),
            kill_calls: Arc::new(Mutex::new(Vec::new())),
            remove_calls: Arc::new(Mutex::new(Vec::new())),
            remove_error: Arc::new(Mutex::new(None)),
            status_calls: Arc::new(Mutex::new(0)),
        }
    }

    fn with_status_failure() -> Self {
        Self {
            status_responses: Arc::new(Mutex::new(VecDeque::from([Err(
                "unavailable".to_owned(),
            )]))),
            kill_calls: Arc::new(Mutex::new(Vec::new())),
            remove_calls: Arc::new(Mutex::new(Vec::new())),
            remove_error: Arc::new(Mutex::new(None)),
            status_calls: Arc::new(Mutex::new(0)),
        }
    }

    fn with_remove_failure(tasks: Vec<PueueTask>) -> Self {
        let pueue = Self::with_tasks(tasks);
        *pueue.remove_error.lock().unwrap() = Some("remove failed".to_owned());
        pueue
    }

    fn kill_calls(&self) -> Vec<i64> {
        self.kill_calls.lock().unwrap().clone()
    }

    fn remove_calls(&self) -> Vec<i64> {
        self.remove_calls.lock().unwrap().clone()
    }

    fn status_calls(&self) -> usize {
        *self.status_calls.lock().unwrap()
    }
}

#[async_trait]
impl PueueApi for OperatorPueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        *self.status_calls.lock().unwrap() += 1;
        let mut responses = self.status_responses.lock().unwrap();
        let response = if responses.len() > 1 {
            responses.pop_front().unwrap()
        } else {
            responses.front().cloned().unwrap()
        };
        response.map_err(|_| AppError::Runtime {
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

    async fn remove(&self, task_id: i64) -> Result<(), AppError> {
        self.remove_calls.lock().unwrap().push(task_id);
        if self.remove_error.lock().unwrap().is_some() {
            return Err(AppError::Runtime {
                operation: "fake Pueue remove",
            });
        }
        Ok(())
    }

    async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
        panic!("operator tests must not provision Pueue groups")
    }
}

fn accepts_api<P: PueueApi>(_api: &P) {}

#[test]
fn operator_fake_preserves_the_pueue_api_contract() {
    accepts_api(&OperatorPueue::with_tasks(Vec::new()));
}

struct CancelHarness {
    operator: OperatorHarness,
    pueue: OperatorPueue,
}

impl CancelHarness {
    fn with_tasks(tasks: Vec<PueueTask>) -> Self {
        Self {
            operator: OperatorHarness::new(),
            pueue: OperatorPueue::with_tasks(tasks),
        }
    }

    fn with_status_failure() -> Self {
        Self {
            operator: OperatorHarness::new(),
            pueue: OperatorPueue::with_status_failure(),
        }
    }

    async fn cancel(
        &self,
        task_id: i64,
    ) -> Result<pueue_agent::cancel::CancelResult, AppError> {
        cancel_task_with(
            &self.operator.db,
            &self.operator.project(),
            &self.pueue,
            task_id,
            self.operator.now,
        )
        .await
    }

    fn task(&self, group: &str, state: &str, enqueued_at: &str) -> PueueTask {
        PueueTask {
            id: 41,
            group: group.to_owned(),
            command: "python train.py".to_owned(),
            state: state.to_owned(),
            enqueued_at: Some(enqueued_at.to_owned()),
            started_at: Some("101".to_owned()),
            ended_at: None,
            result: None,
        }
    }

    fn record_observation(&self, task: &PueueTask) {
        TaskObservationRepository::new(&self.operator.db)
            .upsert(&NewTaskObservation::new(
                "project-a",
                pueue_agent::reconcile::task_signature(task),
                task.id,
                &task.group,
                vec![task.command.clone()],
                &task.state,
                task.enqueued_at.as_deref().and_then(|value| value.parse().ok()),
                task.started_at.as_deref().and_then(|value| value.parse().ok()),
                task.ended_at.as_deref().and_then(|value| value.parse().ok()),
                task.result.as_ref().map(ToString::to_string),
                self.operator.now,
            ))
            .unwrap();
    }

    fn operator_log_contains(&self, action: &str) -> bool {
        self.operator
            .operator_log_rows()
            .iter()
            .any(|(stored_action, _)| stored_action == action)
    }
}

#[tokio::test]
async fn cancel_kills_only_a_running_task_in_the_project_group() {
    let requested_task = PueueTask {
        id: 41,
        group: "pa-project".to_owned(),
        command: "python train.py".to_owned(),
        state: "Running".to_owned(),
        enqueued_at: Some("100".to_owned()),
        started_at: Some("101".to_owned()),
        ended_at: None,
        result: None,
    };
    let terminal_task = PueueTask {
        state: "Killed".to_owned(),
        ended_at: Some("102".to_owned()),
        ..requested_task.clone()
    };
    let harness = CancelHarness {
        operator: OperatorHarness::new(),
        pueue: OperatorPueue::with_status_responses(vec![
            vec![requested_task],
            vec![terminal_task],
        ]),
    };

    let result = harness.cancel(41).await.unwrap();

    assert!(result.kill_sent);
    assert_eq!(result.task_id, 41);
    assert_eq!(result.requested_state, "Running");
    assert_eq!(result.final_observed_state.as_deref(), Some("Killed"));
    assert_eq!(harness.pueue.kill_calls(), vec![41]);
    assert_eq!(harness.pueue.status_calls(), 2);
    assert!(harness.operator_log_contains("cancel"));
    assert_eq!(
        harness
            .operator
            .operator_log_rows()
            .iter()
            .filter(|(action, _)| action == "cancel")
            .count(),
        2
    );
}

#[tokio::test]
async fn cancel_reports_termination_failure_when_kill_leaves_task_nonterminal() {
    let task = cancel_task(41, "pa-project", "Running", "100");
    let harness = CancelHarness {
        operator: OperatorHarness::new(),
        pueue: OperatorPueue::with_status_responses(vec![vec![task.clone()], vec![task]]),
    };

    let error = harness.cancel(41).await.unwrap_err();

    assert!(error.to_string().contains("termination"));
    assert_eq!(harness.pueue.kill_calls(), vec![41]);
    let details = harness
        .operator
        .operator_log_rows()
        .into_iter()
        .find(|(_, details)| details.contains("termination failure"))
        .map(|(_, details)| details)
        .expect("termination failure must be recorded");
    assert!(details.contains("\"action\":\"kill\""));
    assert!(details.contains("\"final_state\":\"Running\""));
    assert_eq!(harness.operator.event_count(EventKind::TerminationFailed), 1);
}

#[tokio::test]
async fn cancel_refuses_status_failure_without_killing() {
    let harness = CancelHarness::with_status_failure();

    assert!(harness.cancel(41).await.is_err());
    assert!(harness.pueue.kill_calls().is_empty());
    assert_eq!(harness.pueue.status_calls(), 1);
    assert!(!harness.operator_log_contains("cancel"));
}

#[tokio::test]
async fn cancel_refuses_other_group_terminal_and_ambiguous_task_ids_without_killing() {
    for tasks in [
        vec![cancel_task(41, "other-group", "Running", "100")],
        vec![cancel_task(41, "pa-project", "Done", "100")],
        vec![
            cancel_task(41, "pa-project", "Running", "100"),
            cancel_task(41, "pa-project", "Running", "200"),
        ],
    ] {
        let harness = CancelHarness::with_tasks(tasks);

        assert!(harness.cancel(41).await.is_err());
        assert!(harness.pueue.kill_calls().is_empty());
        assert_eq!(harness.pueue.status_calls(), 1);
        assert!(!harness.operator_log_contains("cancel"));
    }
}

#[tokio::test]
async fn cancel_allows_case_insensitive_queued_and_running_states() {
    let requested = cancel_task(
        41,
        "pa-project",
        "rUnNiNg",
        "100",
    );
    let terminal = PueueTask {
        state: "fInIsHeD".to_owned(),
        ended_at: Some("102".to_owned()),
        ..requested.clone()
    };
    let running = CancelHarness {
        operator: OperatorHarness::new(),
        pueue: OperatorPueue::with_status_responses(vec![vec![requested], vec![terminal]]),
    };
    let running_result = running.cancel(41).await.unwrap();
    assert_eq!(running_result.action, "kill");
    assert!(running_result.kill_sent);
    assert_eq!(running.pueue.kill_calls(), vec![41]);
    assert!(running.pueue.remove_calls().is_empty());
}

#[tokio::test]
async fn cancel_removes_a_case_insensitive_queued_task() {
    let queued = cancel_task(
        41,
        "pa-project",
        "qUeUeD",
        "100",
    );
    let harness = CancelHarness {
        operator: OperatorHarness::new(),
        pueue: OperatorPueue::with_status_responses(vec![vec![queued], Vec::new()]),
    };

    let result = harness.cancel(41).await.unwrap();

    assert_eq!(result.action, "remove");
    assert!(!result.kill_sent);
    assert_eq!(result.final_observed_state.as_deref(), Some("Removed"));
    assert_eq!(harness.pueue.remove_calls(), vec![41]);
    assert!(harness.pueue.kill_calls().is_empty());
    assert!(harness.operator_log_contains("cancel"));
    let details = harness
        .operator
        .operator_log_rows()
        .into_iter()
        .find(|(_, details)| details.contains("remove"))
        .map(|(_, details)| details)
        .expect("remove action must be recorded");
    assert!(details.contains("\"action\":\"remove\""));
    let output = render_cancel_result(&harness.operator.project(), &result, true);
    let body: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(body["action"], "remove");
    assert_eq!(body["kill_sent"], false);
    assert_eq!(body["state"], "Removed");
}

#[tokio::test]
async fn cancel_reports_termination_failure_when_queued_remove_is_not_confirmed() {
    let queued = cancel_task(41, "pa-project", "Queued", "100");
    let harness = CancelHarness {
        operator: OperatorHarness::new(),
        pueue: OperatorPueue::with_status_responses(vec![vec![queued.clone()], vec![queued]]),
    };

    let error = harness.cancel(41).await.unwrap_err();

    assert!(error.to_string().contains("termination"));
    assert_eq!(harness.pueue.remove_calls(), vec![41]);
    assert!(harness.pueue.kill_calls().is_empty());
    let details = harness
        .operator
        .operator_log_rows()
        .into_iter()
        .find(|(_, details)| details.contains("termination failure"))
        .map(|(_, details)| details)
        .expect("termination failure must be recorded");
    assert!(details.contains("\"action\":\"remove\""));
    assert!(details.contains("\"final_state\":\"Queued\""));
}

#[tokio::test]
async fn cancel_reports_queued_remove_failure_without_killing() {
    let harness = CancelHarness {
        operator: OperatorHarness::new(),
        pueue: OperatorPueue::with_remove_failure(vec![cancel_task(
            41,
            "pa-project",
            "Queued",
            "100",
        )]),
    };

    assert!(harness.cancel(41).await.is_err());
    assert_eq!(harness.pueue.remove_calls(), vec![41]);
    assert!(harness.pueue.kill_calls().is_empty());
    assert_eq!(harness.pueue.status_calls(), 2);
    let details = harness
        .operator
        .operator_log_rows()
        .into_iter()
        .find(|(_, details)| details.contains("remove failed"))
        .map(|(_, details)| details)
        .expect("remove failure must be recorded");
    assert!(details.contains("\"action\":\"remove\""));
}

#[tokio::test]
async fn cancel_refuses_paused_stashed_and_unknown_states_before_side_effects() {
    for state in ["paused", "stashed", "mysterious"] {
        let harness = CancelHarness::with_tasks(vec![cancel_task(
            41,
            "pa-project",
            state,
            "100",
        )]);

        assert!(harness.cancel(41).await.is_err());
        assert!(harness.pueue.kill_calls().is_empty());
        assert_eq!(harness.pueue.status_calls(), 1);
        assert!(harness.operator.operator_log_rows().is_empty());
    }
}

#[tokio::test]
async fn cancel_refuses_a_reused_task_id_with_a_different_stable_identity() {
    let harness = CancelHarness::with_tasks(vec![PueueTask {
        id: 41,
        group: "pa-project".to_owned(),
        command: "python train.py".to_owned(),
        state: "Running".to_owned(),
        enqueued_at: Some("200".to_owned()),
        started_at: Some("201".to_owned()),
        ended_at: None,
        result: None,
    }]);
    let stale_task = harness.task("pa-project", "Running", "100");
    harness.record_observation(&stale_task);

    assert!(harness.cancel(41).await.is_err());
    assert!(harness.pueue.kill_calls().is_empty());
    assert_eq!(harness.pueue.status_calls(), 1);
    assert!(!harness.operator_log_contains("cancel"));
}

#[tokio::test]
async fn cancel_refuses_task_id_reuse_when_current_and_historical_enqueued_at_are_missing() {
    let old_task = PueueTask {
        id: 41,
        group: "pa-project".to_owned(),
        command: "old command".to_owned(),
        state: "Running".to_owned(),
        enqueued_at: None,
        started_at: Some("101".to_owned()),
        ended_at: None,
        result: None,
    };
    let current_task = PueueTask {
        command: "new command".to_owned(),
        started_at: Some("201".to_owned()),
        ..old_task.clone()
    };
    let harness = CancelHarness::with_tasks(vec![current_task]);
    harness.record_observation(&old_task);

    assert!(harness.cancel(41).await.is_err());
    assert!(harness.pueue.kill_calls().is_empty());
    assert!(harness.pueue.remove_calls().is_empty());
    assert_eq!(harness.pueue.status_calls(), 1);
    assert!(!harness.operator_log_contains("cancel"));
}

#[tokio::test]
async fn cancel_refuses_a_task_without_enqueued_at_even_without_history() {
    for state in ["Queued", "Running"] {
        let harness = CancelHarness::with_tasks(vec![PueueTask {
            id: 41,
            group: "pa-project".to_owned(),
            command: "python train.py".to_owned(),
            state: state.to_owned(),
            enqueued_at: None,
            started_at: Some("101".to_owned()),
            ended_at: None,
            result: None,
        }]);

        assert!(harness.cancel(41).await.is_err());
        assert!(harness.pueue.kill_calls().is_empty());
        assert!(harness.pueue.remove_calls().is_empty());
        assert_eq!(harness.pueue.status_calls(), 1);
        assert!(!harness.operator_log_contains("cancel"));
    }
}

#[tokio::test]
async fn cancel_allows_a_state_transition_with_the_same_stable_task_identity() {
    let running_task = cancel_task(41, "pa-project", "Running", "100");
    let terminal_task = PueueTask {
        state: "Killed".to_owned(),
        ended_at: Some("102".to_owned()),
        ..running_task.clone()
    };
    let harness = CancelHarness {
        operator: OperatorHarness::new(),
        pueue: OperatorPueue::with_status_responses(vec![
            vec![running_task],
            vec![terminal_task],
        ]),
    };
    let queued_task = PueueTask {
        started_at: None,
        ..cancel_task(41, "pa-project", "Queued", "100")
    };
    harness.record_observation(&queued_task);

    let result = harness.cancel(41).await.unwrap();

    assert!(result.kill_sent);
    assert_eq!(harness.pueue.kill_calls(), vec![41]);
}

#[tokio::test]
async fn cancel_fails_closed_when_a_same_id_group_replacement_hides_kill_confirmation() {
    let requested_task = cancel_task(41, "pa-project", "Running", "100");
    let replacement_task = PueueTask {
        started_at: Some("201".to_owned()),
        ..cancel_task(41, "pa-project", "Running", "200")
    };
    let harness = CancelHarness {
        operator: OperatorHarness::new(),
        pueue: OperatorPueue::with_status_responses(vec![
            vec![requested_task],
            vec![replacement_task],
        ]),
    };

    let error = harness.cancel(41).await.unwrap_err();

    assert!(error.to_string().contains("termination"));
    assert_eq!(harness.pueue.kill_calls(), vec![41]);
    assert!(harness
        .operator
        .operator_log_rows()
        .iter()
        .any(|(_, details)| details.contains("termination failure")));
}

#[tokio::test]
async fn cancel_renders_human_and_json_final_state() {
    let requested_task = PueueTask {
        id: 41,
        group: "pa-project".to_owned(),
        command: "python train.py".to_owned(),
        state: "Running".to_owned(),
        enqueued_at: Some("100".to_owned()),
        started_at: Some("101".to_owned()),
        ended_at: None,
        result: None,
    };
    let terminal_task = PueueTask {
        state: "Killed".to_owned(),
        ended_at: Some("102".to_owned()),
        ..requested_task.clone()
    };
    let harness = CancelHarness {
        operator: OperatorHarness::new(),
        pueue: OperatorPueue::with_status_responses(vec![
            vec![requested_task],
            vec![terminal_task],
        ]),
    };
    let result = harness.cancel(41).await.unwrap();

    let human = render_cancel_result(&harness.operator.project(), &result, false);
    assert!(human.contains("task=41"));
    assert!(human.contains("state=killed"));
    assert!(human.contains("summary:"));

    let json = render_cancel_result(&harness.operator.project(), &result, true);
    let body: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(body["schema_version"], 1);
    assert_eq!(body["project_id"], "project-a");
    assert_eq!(body["task_id"], 41);
    assert_eq!(body["action"], "kill");
    assert_eq!(body["kill_sent"], true);
    assert_eq!(body["state"], "Killed");
}

fn cancel_task(id: i64, group: &str, state: &str, enqueued_at: &str) -> PueueTask {
    PueueTask {
        id,
        group: group.to_owned(),
        command: "python train.py".to_owned(),
        state: state.to_owned(),
        enqueued_at: Some(enqueued_at.to_owned()),
        started_at: Some("101".to_owned()),
        ended_at: None,
        result: None,
    }
}

struct OperatorHarness {
    temp: TempDir,
    db: Db,
    now: i64,
}

#[derive(Debug, PartialEq)]
struct RecoverySnapshot {
    campaign_research: Vec<Vec<Value>>,
    proposals: Vec<Vec<Value>>,
    submissions: Vec<Vec<Value>>,
    experiments: Vec<Vec<Value>>,
    research_reviews: Vec<Vec<Value>>,
    budget_reservations: Vec<Vec<Value>>,
    events: Vec<Vec<Value>>,
    agent_runs: Vec<Vec<Value>>,
    agent_run_events: Vec<Vec<Value>>,
    termination_requests: Vec<Vec<Value>>,
    decision_cycles: Vec<Vec<Value>>,
    decision_attempts: Vec<Vec<Value>>,
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
        let state_dir = temp.path().join("execution-policy-state");
        fs::create_dir_all(&state_dir).unwrap();
        let db = Db::open(&state_dir.join("state.sqlite3")).unwrap();
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

    fn with_campaign() -> Self {
        let harness = Self::new();
        harness.create_campaign();
        harness
    }

    fn with_accepted_experiment() -> Self {
        let harness = Self::with_campaign();
        let experiment = ExperimentRepository::new(&harness.db)
            .find_by_id("experiment-cli")
            .unwrap()
            .unwrap();
        let experiments = ExperimentRepository::new(&harness.db);
        experiments
            .mark_submitting(&experiment.experiment_id, harness.now + 1)
            .unwrap();
        experiments
            .mark_accepted(
                &experiment.experiment_id,
                41,
                "task-signature-cli",
                harness.now + 2,
            )
            .unwrap();
        harness
    }

    fn with_terminal_experiment() -> Self {
        let harness = Self::with_accepted_experiment();
        ExperimentRepository::new(&harness.db)
            .project_terminal_submission(
                "experiment-cli",
                41,
                ExperimentTerminalOutcome::Succeeded,
                harness.now + 3,
            )
            .unwrap();
        harness
    }

    fn with_unreconciled_experiment() -> Self {
        let harness = Self::with_campaign();
        let experiments = ExperimentRepository::new(&harness.db);
        experiments
            .mark_submitting("experiment-cli", harness.now + 1)
            .unwrap();
        experiments
            .mark_unreconciled("experiment-cli", "pueue_add_unknown", harness.now + 2)
            .unwrap();
        harness
    }

    fn with_termination_unknown_experiment() -> Self {
        let harness = Self::with_accepted_experiment();
        ExperimentRepository::new(&harness.db)
            .project_terminal_submission(
                "experiment-cli",
                41,
                ExperimentTerminalOutcome::Failed {
                    failure_code: "termination_unknown",
                    failure_fingerprint: "termination-unknown-cli",
                },
                harness.now + 3,
            )
            .unwrap();
        harness
    }

    fn add_historical_campaign_tasks(&self, count: i64) {
        let mut connection = self.db.connect().unwrap();
        let transaction = connection.transaction().unwrap();
        for offset in 0..count {
            let proposal_id = format!("proposal-history-{offset:03}");
            let submission_id = format!("submission-history-{offset:03}");
            let experiment_id = format!("experiment-history-{offset:03}");
            transaction
                .execute(
                    "INSERT INTO proposals (
                        proposal_id, campaign_id, kind, status, hypothesis,
                        source_experiment_id, argv_json, working_directory,
                        expected_evidence_json, canonical_digest, reject_reason,
                        created_at, updated_at
                     ) VALUES (?1, 'campaign-cli', 'experiment', 'accepted', 'history',
                        'experiment-cli', '[\"true\"]', '.', '[]', ?1, NULL, ?2, ?2)",
                    rusqlite::params![proposal_id, self.now + offset + 1],
                )
                .unwrap();
            transaction
                .execute(
                    "INSERT INTO submissions (
                        submission_id, project_id, argv_json, created_at, pueue_task_id,
                        task_signature, status, kind, metadata_json, origin_agent_run_id
                     ) VALUES (?1, 'project-a', '[\"true\"]', ?2, ?3, ?4,
                        'accepted', 'experiment', '{}', NULL)",
                    rusqlite::params![
                        submission_id,
                        self.now + offset + 1,
                        1_000 + offset,
                        format!("history-signature-{offset:03}"),
                    ],
                )
                .unwrap();
            transaction
                .execute(
                    "INSERT INTO experiments (
                        experiment_id, campaign_id, proposal_id, submission_id,
                        parent_experiment_id, attempt, status, pueue_task_id,
                        task_signature, failure_code, failure_fingerprint,
                        created_at, updated_at, finished_at
                     ) VALUES (?1, 'campaign-cli', ?2, ?3, 'experiment-cli', ?4,
                        'succeeded', ?5, ?6, NULL, NULL, ?7, ?7, ?7)",
                    rusqlite::params![
                        experiment_id,
                        proposal_id,
                        submission_id,
                        offset + 1,
                        1_000 + offset,
                        format!("history-signature-{offset:03}"),
                        self.now + offset + 1,
                    ],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
    }

    fn create_campaign(&self) {
        let objective = pueue_agent::state::ObjectiveSnapshot {
            text: "OBJECTIVE_SECRET_TEXT must never be printed".to_owned(),
            digest: "objective-digest-cli".to_owned(),
        };
        let argv = vec![
            "python".to_owned(),
            "--token".to_owned(),
            "RAW_ARGV_SECRET".to_owned(),
        ];
        let baseline = proposals::validate_initial_baseline(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: "bounded baseline hypothesis".to_owned(),
                source_experiment_id: None,
                argv: argv.clone(),
                working_directory: ".".to_owned(),
                expected_evidence: vec!["metrics.json".to_owned()],
            },
            &objective.digest,
        )
        .unwrap();
        CampaignRepository::new(&self.db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id: "campaign-cli",
                    project_id: "project-a",
                    objective: &objective,
                    initial_argv: &argv,
                    baseline: &baseline,
                    submission_id: "submission-cli",
                    experiment_id: "experiment-cli",
                    proposal_id: "proposal-cli",
                    metadata: &json!({}),
                    origin_agent_run_id: None,
                    objective_metric: None,
                    now: self.now,
                },
                &self.policy().campaign_limits,
            )
            .unwrap();
    }

    fn campaign_state(&self) -> CampaignState {
        CampaignRepository::new(&self.db)
            .find_by_id("campaign-cli")
            .unwrap()
            .unwrap()
            .state
    }

    fn snapshot_rows(&self, query: &str) -> Vec<Vec<Value>> {
        let connection = self.db.connect().unwrap();
        let mut statement = connection.prepare(query).unwrap();
        let column_count = statement.column_count();
        statement
            .query_map([], |row| {
                let mut values = Vec::with_capacity(column_count);
                for index in 0..column_count {
                    values.push(row.get(index)?);
                }
                Ok(values)
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn recovery_snapshot(&self) -> RecoverySnapshot {
        RecoverySnapshot {
            campaign_research: self.snapshot_rows(
                "SELECT * FROM campaign_research
                 WHERE campaign_id = 'campaign-cli'",
            ),
            proposals: self.snapshot_rows(
                "SELECT * FROM proposals
                 WHERE campaign_id = 'campaign-cli' ORDER BY proposal_id",
            ),
            submissions: self.snapshot_rows(
                "SELECT * FROM submissions
                 WHERE project_id = 'project-a' ORDER BY submission_id",
            ),
            experiments: self.snapshot_rows(
                "SELECT * FROM experiments
                 WHERE campaign_id = 'campaign-cli' ORDER BY experiment_id",
            ),
            research_reviews: self.snapshot_rows(
                "SELECT * FROM research_reviews
                 WHERE campaign_id = 'campaign-cli' ORDER BY review_id",
            ),
            budget_reservations: self.snapshot_rows(
                "SELECT * FROM budget_reservations
                 WHERE campaign_id = 'campaign-cli' ORDER BY reservation_id",
            ),
            events: self.snapshot_rows(
                "SELECT * FROM events
                 WHERE project_id = 'project-a' ORDER BY event_id",
            ),
            agent_runs: self.snapshot_rows(
                "SELECT * FROM agent_runs
                 WHERE project_id = 'project-a' ORDER BY run_id",
            ),
            agent_run_events: self.snapshot_rows(
                "SELECT * FROM agent_run_events
                 WHERE project_id = 'project-a' ORDER BY run_id, event_id",
            ),
            termination_requests: self.snapshot_rows(
                "SELECT * FROM termination_requests
                 WHERE project_id = 'project-a' ORDER BY request_id",
            ),
            decision_cycles: self.snapshot_rows(
                "SELECT * FROM decision_cycles
                 WHERE campaign_id = 'campaign-cli' ORDER BY cycle_id",
            ),
            decision_attempts: self.snapshot_rows(
                "SELECT attempts.* FROM decision_attempts AS attempts
                 JOIN decision_cycles AS cycles ON cycles.cycle_id = attempts.cycle_id
                 WHERE cycles.campaign_id = 'campaign-cli'
                 ORDER BY attempts.cycle_id, attempts.attempt_number",
            ),
        }
    }

    fn prepare_blocked_paused_research(&self, session_id: Option<&str>, generation: i64) {
        ResearchRepository::new(&self.db)
            .ensure_campaign("campaign-cli")
            .unwrap();
        CampaignRepository::new(&self.db)
            .pause("project-a", self.now + 1)
            .unwrap();
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE campaign_research
                 SET session_id = ?1, session_generation = ?2,
                     blocked_reason = 'research_policy_blocked', next_due_at = NULL
                 WHERE campaign_id = 'campaign-cli'",
                rusqlite::params![session_id, generation],
            )
            .unwrap();
    }

    fn prepare_blocked_active_research(&self, session_id: Option<&str>, generation: i64) {
        ResearchRepository::new(&self.db)
            .ensure_campaign("campaign-cli")
            .unwrap();
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE campaign_research
                 SET session_id = ?1, session_generation = ?2,
                     blocked_reason = 'research_policy_blocked', next_due_at = NULL
                 WHERE campaign_id = 'campaign-cli'",
                rusqlite::params![session_id, generation],
            )
            .unwrap();
    }

    fn add_open_research_review(&self) {
        let event_id = self.event(EventKind::CampaignResearch, "recovery-open-review");
        self.db
            .connect()
            .unwrap()
            .execute(
                "INSERT INTO research_reviews (
                    review_id, campaign_id, experiment_id, task_signature, attempt,
                    state, operation_stage, agent_run_id, context_json, context_digest,
                    response_json, termination_request_id, successor_experiment_id,
                    evidence_schema_version, session_generation, event_id, not_before,
                    notes_json, failure_code, decision_cycle_id, checkpoint_json,
                    created_at, started_at, finished_at, updated_at
                 ) VALUES (
                    'recovery-open-review', 'campaign-cli', 'experiment-cli',
                    'recovery-open-review-signature', 0, 'pending', NULL, NULL,
                    NULL, NULL, NULL, NULL, NULL, NULL, 0, ?1, ?2,
                    NULL, NULL, NULL, NULL, ?2, NULL, NULL, ?2
                 )",
                rusqlite::params![event_id, self.now],
            )
            .unwrap();
    }

    fn add_terminal_research_history(&self) {
        let event_id = EventRepository::new(&self.db)
            .insert_idempotent(
                &NewEvent::new(
                    "project-a",
                    EventKind::CampaignResearch,
                    "recovery-terminal-history-event",
                    json!({"history": "terminal"}),
                    self.now,
                    self.now,
                )
                .with_campaign_lineage("campaign-cli", Some("experiment-cli")),
            )
            .unwrap()
            .event_id;
        let connection = self.db.connect().unwrap();
        let session_generation: i64 = connection
            .query_row(
                "SELECT session_generation FROM campaign_research
                 WHERE campaign_id = 'campaign-cli'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO research_reviews (
                    review_id, campaign_id, experiment_id, task_signature, attempt,
                    state, operation_stage, agent_run_id, context_json, context_digest,
                    response_json, termination_request_id, successor_experiment_id,
                    evidence_schema_version, session_generation, event_id, not_before,
                    notes_json, failure_code, decision_cycle_id, checkpoint_json,
                    created_at, started_at, finished_at, updated_at
                 ) VALUES (
                    'recovery-terminal-history', 'campaign-cli', 'experiment-cli',
                    'recovery-terminal-history-signature', 1, 'discarded', NULL,
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?1, ?2, ?3,
                    NULL, 'recovery_terminal_history', NULL, NULL, ?4, ?4, ?5, ?5
                 )",
                rusqlite::params![
                    session_generation,
                    event_id,
                    self.now,
                    self.now,
                    self.now + 1,
                ],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE campaign_research
                 SET last_review_id = 'recovery-terminal-history'
                 WHERE campaign_id = 'campaign-cli'",
                [],
            )
            .unwrap();
    }

    fn add_native_research_owner(&self, status: AgentRunStatus) {
        let event_id = self.event(EventKind::CampaignResearch, "recovery-native-owner");
        let execution = ExecutionProjection::new(
            "campaign_research",
            "/bin/codex",
            "recovery-fixture",
        )
        .unwrap();
        AgentRunRepository::new(&self.db)
            .insert(
                &NewAgentRun::new(
                    "project-a",
                    event_id,
                    None,
                    status,
                    self.now,
                    "/tmp/recovery-native-owner.log",
                )
                .with_execution(execution),
            )
            .unwrap();
    }

    #[cfg(unix)]
    fn run(&self, arguments: &[&str]) -> std::process::Output {
        self.run_with_codex_fixture(arguments, false, false)
    }

    #[cfg(unix)]
    fn run_with_recovery_fixture(&self, arguments: &[&str]) -> std::process::Output {
        self.run_with_codex_fixture(arguments, true, false)
    }

    #[cfg(target_os = "linux")]
    fn run_with_owned_recovery_fixture(&self, arguments: &[&str]) -> std::process::Output {
        self.run_with_codex_fixture(arguments, true, true)
    }

    #[cfg(unix)]
    fn run_with_codex_fixture(
        &self,
        arguments: &[&str],
        recovery_fixture: bool,
        owned_session: bool,
    ) -> std::process::Output {
        use std::os::unix::fs::PermissionsExt;

        let base = fs::canonicalize(self.temp.path()).unwrap();
        let state_dir = base.join("execution-policy-state");
        let home = base.join("cli-home");
        let codex_home = base.join("cli-codex-home");
        let trusted_dir = base.join("cli-trusted-bin");
        let pueue_config = home.join(".config/pueue/pueue.yml");
        for directory in [
            &state_dir,
            &home,
            &codex_home,
            &trusted_dir,
            pueue_config.parent().unwrap(),
        ] {
            fs::create_dir_all(directory).unwrap();
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        if owned_session {
            let sessions = codex_home.join("sessions");
            fs::create_dir_all(&sessions).unwrap();
            fs::set_permissions(&sessions, fs::Permissions::from_mode(0o700)).unwrap();
            let project_root = fs::canonicalize(&self.project().root_path).unwrap();
            let session = sessions.join(format!("rollout-{CODEX_SESSION_ID}.jsonl"));
            fs::write(
                &session,
                format!(
                    "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{CODEX_SESSION_ID}\",\"cwd\":{cwd:?}}}}}\n",
                    cwd = project_root.to_string_lossy()
                ),
            )
            .unwrap();
            fs::set_permissions(&session, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let executable = trusted_dir.join("pueue-agent-fixture");
        fs::copy(env!("CARGO_BIN_EXE_pueue-agent"), &executable).unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        let codex_executable = trusted_dir.join("codex-fixture");
        if recovery_fixture {
            let source = codex_executable.with_extension("rs");
            fs::write(
                &source,
                r#"use std::env;

fn main() {
    let first = env::args().nth(1);
    match first.as_deref() {
        Some("--version") => println!("codex 0.138.0"),
        Some("--help") => println!(
            "workspace-write read-only --ask-for-approval never --strict-config"
        ),
        Some("exec") => println!(
            "--strict-config --ignore-user-config --ignore-rules --output-schema --output-last-message --json"
        ),
        _ => std::process::exit(42),
    }
}
"#,
            )
            .unwrap();
            let output = std::process::Command::new("rustc")
                .args(["--edition=2021", "-o"])
                .arg(&codex_executable)
                .arg(&source)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "generated recovery Codex failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            fs::set_permissions(&codex_executable, fs::Permissions::from_mode(0o700)).unwrap();
        }
        fs::write(&pueue_config, "fixture: true\n").unwrap();
        fs::set_permissions(&pueue_config, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(
            state_dir.join("execution-policy.toml"),
            format!(
                "version = 1\ntrusted_path = {:?}\n\n[executables]\ncodex = {:?}\npueue = {:?}\n",
                trusted_dir.display().to_string(),
                if recovery_fixture {
                    codex_executable.display().to_string()
                } else {
                    executable.display().to_string()
                },
                executable.display().to_string(),
            ),
        )
        .unwrap();
        fs::set_permissions(
            state_dir.join("execution-policy.toml"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        assert_cmd::Command::cargo_bin("pueue-agent")
            .unwrap()
            .current_dir(self.project().root_path)
            .env("HOME", home)
            .env("CODEX_HOME", codex_home)
            .env("PUEUE_AGENT_STATE_DIR", state_dir)
            .env("PATH", trusted_dir)
            .args(arguments)
            .output()
            .unwrap()
    }

    fn runner(&self) -> AgentRunner {
        AgentRunner::new(
            AgentRunnerConfig::production()
                .with_codex_capabilities(pueue_agent::codex_command::CodexCapabilities::all()),
            self.policy(),
        )
    }

    fn policy(&self) -> Arc<pueue_agent::execution_policy::ResolvedExecutionPolicy> {
        execution_policy_fixture::resolved_policy(
            self.temp.path(),
            &[(
                "project-a",
                self.project().root_path.as_path(),
                std::path::Path::new("codex"),
            )],
        )
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

    fn event_count(&self, kind: EventKind) -> usize {
        EventRepository::new(&self.db)
            .recent_events("project-a", 100)
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == kind)
            .count()
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
            now_override: None,
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

fn assert_recovery_preserves_snapshot(before: &RecoverySnapshot, after: &RecoverySnapshot) {
    assert_eq!(before.proposals, after.proposals);
    assert_eq!(before.submissions, after.submissions);
    assert_eq!(before.experiments, after.experiments);
    assert_eq!(before.research_reviews, after.research_reviews);
    assert_eq!(before.budget_reservations, after.budget_reservations);
    assert_eq!(before.events, after.events);
    assert_eq!(before.agent_runs, after.agent_runs);
    assert_eq!(before.agent_run_events, after.agent_run_events);
    assert_eq!(before.termination_requests, after.termination_requests);
    assert_eq!(before.decision_cycles, after.decision_cycles);
    assert_eq!(before.decision_attempts, after.decision_attempts);

    assert_eq!(before.campaign_research.len(), 1);
    assert_eq!(after.campaign_research.len(), 1);
    let before_research = &before.campaign_research[0];
    let after_research = &after.campaign_research[0];
    assert_eq!(before_research.len(), 7);
    assert_eq!(after_research.len(), 7);
    for column in [0, 1, 2, 5] {
        assert_eq!(before_research[column], after_research[column]);
    }
    assert_eq!(before_research[3], Value::Null);
    assert!(matches!(after_research[3], Value::Integer(_)));
    assert_eq!(
        before_research[4],
        Value::Text("research_policy_blocked".to_owned())
    );
    assert_eq!(after_research[4], Value::Null);
}

fn assert_seeded_research_history(snapshot: &RecoverySnapshot) {
    assert_eq!(snapshot.research_reviews.len(), 1);
    assert!(!snapshot.events.is_empty());
    assert_eq!(
        snapshot.research_reviews[0][0],
        Value::Text("recovery-terminal-history".to_owned())
    );
    assert_eq!(snapshot.research_reviews[0][4], Value::Integer(1));
    assert_eq!(
        snapshot.research_reviews[0][5],
        Value::Text("discarded".to_owned())
    );
    assert_eq!(snapshot.research_reviews[0][7], Value::Null);
    assert_eq!(
        snapshot.campaign_research[0][5],
        Value::Text("recovery-terminal-history".to_owned())
    );
}

#[cfg(unix)]
#[test]
fn campaign_pause_and_resume_change_only_campaign_state_and_are_idempotent() {
    let harness = OperatorHarness::with_campaign();

    for _ in 0..2 {
        let output = harness.run(&["campaign", "pause", "--json"]);
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert_eq!(harness.campaign_state(), CampaignState::Paused);
        assert!(!harness.project().paused);
    }
    for _ in 0..2 {
        let output = harness.run(&["campaign", "resume", "--json"]);
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert_eq!(harness.campaign_state(), CampaignState::Active);
        assert!(!harness.project().paused);
    }
}

#[cfg(unix)]
#[test]
fn blocked_research_campaign_requires_explicit_pause_before_resume() {
    let harness = OperatorHarness::with_campaign();
    harness.prepare_blocked_active_research(None, 0);
    let before = harness.recovery_snapshot();
    let repository = ResearchRepository::new(&harness.db);
    let expected = repository.state("campaign-cli").unwrap();

    let error = repository
        .resume_after_validation("project-a", &expected, harness.now + 2)
        .unwrap_err();

    match error {
        AppError::Validation { field, .. } => assert_eq!(field, "research.recovery"),
        other => panic!("expected recovery validation error, got {other:?}"),
    }
    assert_eq!(harness.campaign_state(), CampaignState::Active);
    assert_eq!(repository.state("campaign-cli").unwrap(), expected);
    assert_eq!(before, harness.recovery_snapshot());
}

#[cfg(target_os = "linux")]
#[test]
fn explicit_recovery_accepts_safe_missing_session_and_preserves_binding() {
    let harness = OperatorHarness::with_campaign();
    harness.prepare_blocked_paused_research(Some(CODEX_SESSION_ID), 3);
    harness.add_terminal_research_history();
    let before = harness.recovery_snapshot();
    assert_seeded_research_history(&before);

    let output = harness.run_with_recovery_fixture(&["campaign", "resume", "--json"]);

    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(harness.campaign_state(), CampaignState::Active);
    let state = ResearchRepository::new(&harness.db)
        .state("campaign-cli")
        .unwrap();
    assert_eq!(state.session_id.as_deref(), Some(CODEX_SESSION_ID));
    assert_eq!(state.session_generation, 3);
    assert!(state.next_due_at.is_some());
    assert_eq!(state.blocked_reason, None);
    let connection = harness.db.connect().unwrap();
    let review_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM research_reviews WHERE campaign_id = 'campaign-cli'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let run_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM agent_runs WHERE project_id = 'project-a'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(review_count, 1);
    assert_eq!(run_count, 0);
    assert_recovery_preserves_snapshot(&before, &harness.recovery_snapshot());
}

#[cfg(target_os = "linux")]
#[test]
fn explicit_recovery_accepts_owned_session_with_pinned_capabilities() {
    let harness = OperatorHarness::with_campaign();
    harness.prepare_blocked_paused_research(Some(CODEX_SESSION_ID), 5);
    harness.add_terminal_research_history();
    let before = harness.recovery_snapshot();
    assert_seeded_research_history(&before);

    let output = harness.run_with_owned_recovery_fixture(&["campaign", "resume", "--json"]);

    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(harness.campaign_state(), CampaignState::Active);
    let state = ResearchRepository::new(&harness.db)
        .state("campaign-cli")
        .unwrap();
    assert_eq!(state.session_id.as_deref(), Some(CODEX_SESSION_ID));
    assert_eq!(state.session_generation, 5);
    assert!(state.next_due_at.is_some());
    assert_eq!(state.blocked_reason, None);
    assert_recovery_preserves_snapshot(&before, &harness.recovery_snapshot());
}

#[cfg(not(target_os = "linux"))]
#[test]
fn explicit_recovery_rejects_unsupported_platform_before_clearing_block() {
    let harness = OperatorHarness::with_campaign();
    harness.prepare_blocked_paused_research(None, 0);

    let output = harness.run_with_recovery_fixture(&["campaign", "resume"]);

    assert!(!output.status.success());
    assert_eq!(harness.campaign_state(), CampaignState::Paused);
    assert_eq!(
        ResearchRepository::new(&harness.db)
            .state("campaign-cli")
            .unwrap()
            .blocked_reason
            .as_deref(),
        Some("research_policy_blocked")
    );
}

#[cfg(unix)]
#[test]
fn research_recovery_cas_rejects_stale_reason_session_generation_and_project() {
    let harness = OperatorHarness::with_campaign();
    harness.prepare_blocked_paused_research(Some(CODEX_SESSION_ID), 4);
    let repository = ResearchRepository::new(&harness.db);
    let current = repository.state("campaign-cli").unwrap();

    let mut wrong_reason = current.clone();
    wrong_reason.blocked_reason = Some("different_reason".to_owned());
    assert!(repository
        .resume_after_validation("project-a", &wrong_reason, harness.now + 2)
        .is_err());

    let mut wrong_session = current.clone();
    wrong_session.session_id = Some("019f9f30-5f31-7a40-8e28-bd95e1f6c538".to_owned());
    assert!(repository
        .resume_after_validation("project-a", &wrong_session, harness.now + 2)
        .is_err());

    let mut wrong_generation = current.clone();
    wrong_generation.session_generation += 1;
    assert!(repository
        .resume_after_validation("project-a", &wrong_generation, harness.now + 2)
        .is_err());

    assert!(repository
        .resume_after_validation("different-project", &current, harness.now + 2)
        .is_err());

    assert_eq!(repository.state("campaign-cli").unwrap(), current);
}

#[cfg(unix)]
#[test]
fn research_recovery_cas_rejects_open_review_and_native_owner_interleavings() {
    fn assert_rejected(prepare: impl FnOnce(&OperatorHarness), expected_field: &str) {
        let harness = OperatorHarness::with_campaign();
        harness.prepare_blocked_paused_research(None, 0);
        let repository = ResearchRepository::new(&harness.db);
        let expected = repository.state("campaign-cli").unwrap();
        prepare(&harness);

        let error = repository
            .resume_after_validation("project-a", &expected, harness.now + 2)
            .unwrap_err();
        match error {
            AppError::Validation { field, .. } => assert_eq!(field, expected_field),
            other => panic!("expected recovery validation error, got {other:?}"),
        }
        assert_eq!(repository.state("campaign-cli").unwrap(), expected);
        assert_eq!(harness.campaign_state(), CampaignState::Paused);
    }

    assert_rejected(|harness| harness.add_open_research_review(), "research.review");
    assert_rejected(
        |harness| harness.add_native_research_owner(AgentRunStatus::Running),
        "research.owner",
    );
    assert_rejected(
        |harness| harness.add_native_research_owner(AgentRunStatus::Failed),
        "research.owner",
    );
}

#[cfg(unix)]
#[test]
fn campaign_resume_rejects_a_research_block_restored_after_recovery_clear() {
    let harness = OperatorHarness::with_campaign();
    harness.prepare_blocked_paused_research(None, 0);
    harness.add_terminal_research_history();
    let before = harness.recovery_snapshot();
    let repository = ResearchRepository::new(&harness.db);
    let expected = repository.state("campaign-cli").unwrap();
    repository
        .resume_after_validation("project-a", &expected, harness.now + 2)
        .unwrap();
    let after_clear = harness.recovery_snapshot();
    assert_recovery_preserves_snapshot(&before, &after_clear);
    let (next_due_at, updated_at): (Option<i64>, i64) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT next_due_at, updated_at FROM campaign_research
             WHERE campaign_id = 'campaign-cli'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(next_due_at, Some(harness.now + 2));
    assert_eq!(updated_at, harness.now + 2);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaign_research
             SET blocked_reason = 'research_recovery_required', next_due_at = NULL
             WHERE campaign_id = 'campaign-cli'",
            [],
        )
        .unwrap();

    let result = CampaignRepository::new(&harness.db).resume("project-a", harness.now + 3);

    assert!(result.is_err());
    assert_eq!(harness.campaign_state(), CampaignState::Paused);
    assert_eq!(
        repository.state("campaign-cli").unwrap().blocked_reason.as_deref(),
        Some("research_recovery_required")
    );
}

#[cfg(unix)]
#[test]
fn campaign_retire_requires_no_nonterminal_experiment() {
    let harness = OperatorHarness::with_accepted_experiment();

    let output = harness.run(&["campaign", "retire"]);

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("nonterminal"));
    assert_eq!(harness.campaign_state(), CampaignState::Active);
}

#[cfg(unix)]
#[test]
fn campaign_retire_and_resume_reject_unreconciled_experiment() {
    let harness = OperatorHarness::with_unreconciled_experiment();

    let retire = harness.run(&["campaign", "retire"]);
    assert!(!retire.status.success());
    assert!(String::from_utf8_lossy(&retire.stderr).contains("nonterminal"));
    assert_eq!(harness.campaign_state(), CampaignState::Active);

    assert!(harness.run(&["campaign", "pause"]).status.success());
    let resume = harness.run(&["campaign", "resume"]);
    assert!(!resume.status.success());
    assert!(String::from_utf8_lossy(&resume.stderr).contains("reconciliation"));
    assert_eq!(harness.campaign_state(), CampaignState::Paused);
}

#[cfg(unix)]
#[test]
fn campaign_retire_rejects_terminal_experiment_with_unknown_termination() {
    let harness = OperatorHarness::with_termination_unknown_experiment();

    let output = harness.run(&["campaign", "retire"]);

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("termination"));
    assert_eq!(harness.campaign_state(), CampaignState::Active);
}

#[cfg(unix)]
#[test]
fn campaign_status_bounds_historical_task_ids() {
    let harness = OperatorHarness::with_campaign();
    harness.add_historical_campaign_tasks(105);

    let output = harness.run(&["campaign", "status", "--json"]);

    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let body: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let task_ids = body["task_ids"].as_array().unwrap();
    assert_eq!(task_ids.len(), 100);
    assert_eq!(task_ids[0], 1_104);
    assert_eq!(task_ids[99], 1_005);
}

#[cfg(unix)]
#[test]
fn campaign_retire_is_idempotent_after_terminal_experiments() {
    let harness = OperatorHarness::with_terminal_experiment();

    for _ in 0..2 {
        let output = harness.run(&["campaign", "retire", "--json"]);
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert_eq!(harness.campaign_state(), CampaignState::Retired);
    }
}

#[cfg(unix)]
#[test]
fn campaign_json_status_omits_objective_text_and_raw_argv() {
    let harness = OperatorHarness::with_campaign();

    let output = harness.run(&["campaign", "status", "--json"]);

    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let text = String::from_utf8(output.stdout).unwrap();
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["campaign_id"], "campaign-cli");
    assert_eq!(body["objective_digest"], "objective-digest-cli");
    assert!(!text.contains("OBJECTIVE_SECRET_TEXT"));
    assert!(!text.contains("RAW_ARGV_SECRET"));
}

#[cfg(unix)]
#[test]
fn campaign_resume_revalidates_project_availability() {
    let harness = OperatorHarness::with_campaign();
    assert!(harness.run(&["campaign", "pause"]).status.success());
    ProjectRepository::new(&harness.db)
        .disable("project-a", harness.now + 1, &[])
        .unwrap();

    let output = harness.run(&["campaign", "resume"]);

    assert!(!output.status.success());
    assert_eq!(harness.campaign_state(), CampaignState::Paused);
}

#[cfg(unix)]
#[test]
fn proposal_list_and_inspect_are_scoped_bounded_and_omit_raw_argv() {
    let harness = OperatorHarness::with_campaign();

    let list = harness.run(&["proposal", "list", "--limit", "20", "--json"]);
    assert!(list.status.success(), "{}", String::from_utf8_lossy(&list.stderr));
    let list_text = String::from_utf8(list.stdout).unwrap();
    let body: serde_json::Value = serde_json::from_str(&list_text).unwrap();
    assert_eq!(body["proposals"][0]["proposal_id"], "proposal-cli");
    assert!(!list_text.contains("RAW_ARGV_SECRET"));

    let inspect = harness.run(&["proposal", "inspect", "proposal-cli", "--json"]);
    assert!(inspect.status.success(), "{}", String::from_utf8_lossy(&inspect.stderr));
    let inspect_text = String::from_utf8(inspect.stdout).unwrap();
    let body: serde_json::Value = serde_json::from_str(&inspect_text).unwrap();
    assert_eq!(body["hypothesis"], "bounded baseline hypothesis");
    assert_eq!(body["expected_evidence"][0], "metrics.json");
    assert!(body.get("argv").is_none());
    assert!(!inspect_text.contains("RAW_ARGV_SECRET"));

    assert!(!harness
        .run(&["proposal", "list", "--limit", "101"])
        .status
        .success());
}

#[cfg(unix)]
#[test]
fn experiment_list_and_inspect_use_argv_digest_and_task_identity() {
    let harness = OperatorHarness::with_accepted_experiment();

    let list = harness.run(&["experiment", "list", "--json"]);
    assert!(list.status.success(), "{}", String::from_utf8_lossy(&list.stderr));
    let list_text = String::from_utf8(list.stdout).unwrap();
    let body: serde_json::Value = serde_json::from_str(&list_text).unwrap();
    assert_eq!(body["experiments"][0]["experiment_id"], "experiment-cli");
    assert!(!list_text.contains("RAW_ARGV_SECRET"));

    let inspect = harness.run(&["experiment", "inspect", "experiment-cli", "--json"]);
    assert!(inspect.status.success(), "{}", String::from_utf8_lossy(&inspect.stderr));
    let inspect_text = String::from_utf8(inspect.stdout).unwrap();
    let body: serde_json::Value = serde_json::from_str(&inspect_text).unwrap();
    assert_eq!(body["submission_id"], "submission-cli");
    assert_eq!(body["pueue_task_id"], 41);
    assert!(body["argv_digest"].as_str().is_some_and(|digest| digest.len() == 64));
    assert!(body.get("argv").is_none());
    assert!(!inspect_text.contains("RAW_ARGV_SECRET"));
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
    assert!(output.contains("service: running"));
    assert!(output.contains("automation: active"));
    assert!(output.contains("project: project-a"));
    assert!(output.contains("project: enabled=true paused=false halted=false"));
    assert!(output.contains("enabled: true"));
    assert!(output.contains("paused: false"));
    assert!(output.contains("halted: no"));
    assert!(output.contains("active_tasks: 1"));
    assert!(output.contains("pueue: total=1 active=1 queued=0"));
    assert!(output.contains("task=41 state=running"));
    assert!(output.contains(
        "events: pending=1 claimed=0 retry_wait=0 in_flight=0 dispatched=0 failed=1 dead_letter=0"
    ));
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

    let compact = status::render_project_status_compact(
        &harness.db,
        &harness.project(),
        &harness.status_input(PueueSnapshot::Tasks(vec![harness.running_task()])),
    )
    .unwrap();
    assert!(compact.contains("service: running"));
    assert!(compact.contains("automation: active"));
    assert!(compact.contains("project: enabled=true paused=false halted=false"));
    assert!(compact.contains("pueue: total=1 active=1 queued=0"));
    assert!(compact.contains("agent_runs: active=1 failed=1"));

    let rendered_json = render_project_status_json(
        &harness.db,
        &harness.project(),
        &harness.status_input(PueueSnapshot::Tasks(vec![harness.running_task()])),
    )
    .unwrap();
    let status_json: serde_json::Value = serde_json::from_str(&rendered_json).unwrap();
    assert_eq!(status_json["service"], "running");
    assert_eq!(status_json["automation"], "active");
}

#[test]
fn status_reports_disabled_automation_without_inferring_pueue_state() {
    let harness = OperatorHarness::new();
    ProjectRepository::new(&harness.db)
        .disable("project-a", harness.now + 1, &[])
        .unwrap();

    let output = status::render_project_status(
        &harness.db,
        &harness.project(),
        &harness.status_input(PueueSnapshot::Tasks(vec![harness.running_task()])),
    )
    .unwrap();

    assert!(output.contains("automation: disabled"));
    assert!(output.contains("pueue: total=1 active=1 queued=0"));
}

#[test]
fn status_distinguishes_paused_and_halted_automation() {
    let harness = OperatorHarness::new();
    status::pause_project(&harness.db, "project-a", harness.now + 1).unwrap();

    let paused = status::render_project_status(
        &harness.db,
        &harness.project(),
        &harness.status_input(PueueSnapshot::Tasks(Vec::new())),
    )
    .unwrap();
    assert!(paused.contains("automation: paused"));
    assert!(paused.contains("project: enabled=true paused=true halted=false"));

    ProjectRepository::new(&harness.db)
        .halt("project-a", "operator halt", harness.now + 2)
        .unwrap();
    let halted = status::render_project_status(
        &harness.db,
        &harness.project(),
        &harness.status_input(PueueSnapshot::Tasks(Vec::new())),
    )
    .unwrap();
    assert!(halted.contains("automation: halted"));
    assert!(halted.contains("project: enabled=true paused=true halted=true"));
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
fn status_human_bounds_and_preserves_typed_project_root_path() {
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
    assert!(root_line.starts_with("root: /tmp/"));
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
fn status_text_output_has_stable_active_project_projection() {
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
            "pueue-agent status project=project-a\ndaemon: running\nservice: running\nautomation: active\nproject: project-a\nproject: enabled=true paused=false halted=false\nroot: {}\ngroup: pa-project\nenabled: true\npaused: false\nhalted: no\npueue: total=1 active=1 queued=0\nactive_tasks: 1\ntask=41 state=running python train.py\nevents: pending=0 claimed=0 retry_wait=0 in_flight=0 dispatched=0 failed=0 dead_letter=0\nintegration_errors: 0\nopen_incidents: 0\ntermination_requests: requested=0 sent=0 confirmed=0 timed_out=0 failed=0\nagent_runs: active=0 failed=0\nguardrails: consecutive_failures=0/3 experiments=0/20 agent_runs=0/10\ncodex_context: mode=resume session={CODEX_SESSION_ID}\nsummary: 1 active task(s), 0 pending event(s), 0 active agent run(s)",
            project.root_path.display(),
        )
    );
}

#[test]
fn status_text_output_has_stable_running_health_projection() {
    let harness = OperatorHarness::with_accepted_experiment();
    HealthRepository::ensure_running(
        &harness.db,
        "project-a",
        "campaign-cli",
        "experiment-cli",
        41,
        harness.now + 10,
    )
    .unwrap();
    for observed_at in [harness.now + 11, harness.now + 12] {
        HealthRepository::record_observation(
            &harness.db,
            "experiment-cli",
            observed_at,
            SignalSummaryEntry {
                class: "oom".to_owned(),
                source: "builtin_probe".to_owned(),
                evidence_digest: format!("oom-digest-{observed_at}"),
                observed_at,
            },
        )
        .unwrap();
    }
    HealthRepository::set_state(
        &harness.db,
        "experiment-cli",
        HealthState::Suspicious,
        harness.now + 13,
    )
    .unwrap();
    HealthRepository::store_diagnosis(
        &harness.db,
        "experiment-cli",
        &json!({
            "root_cause_class": "oom",
            "confidence": 0.9,
            "recommended_action": "kill_and_resume",
            "summary": "gpu exhausted",
        }),
        harness.now + 13,
    )
    .unwrap();
    let input = StatusInput {
        daemon_health: ServiceStatus::Running,
        pueue: PueueSnapshot::Tasks(vec![harness.running_task()]),
        now_override: Some(harness.now + 20),
    };

    let output = status::render_project_status(
        &harness.db,
        &harness.project(),
        &input,
    )
    .unwrap();

    assert_eq!(
        output,
        format!(
            "pueue-agent status project=project-a\ndaemon: running\nservice: running\nautomation: active\nproject: project-a\nproject: enabled=true paused=false halted=false\nroot: {}\ngroup: pa-project\nenabled: true\npaused: false\nhalted: no\npueue: total=1 active=1 queued=0\nactive_tasks: 1\ntask=41 state=running python train.py\nevents: pending=0 claimed=0 retry_wait=0 in_flight=0 dispatched=0 failed=0 dead_letter=0\nintegration_errors: 0\nopen_incidents: 0\ntermination_requests: requested=0 sent=0 confirmed=0 timed_out=0 failed=0\nagent_runs: active=0 failed=0\ncampaign: id=campaign-cli state=active reason=none experiments=accepted=1 rolling_usage=experiment=1 next_eligible_at=none unreconciled=0 objective_digest=objective-digest-cli\nresearch: state=idle next_due_at=none last_review_id=none experiment_id=none last_action=none checkpoint_confirmation=none blocked_reason=none discarded_reason=none session_generation=0 session_rebuilt=false\nhealth: id=experiment-cli state=suspicious signals=oomx2 age=8 action=kill_and_resume\nguardrails: consecutive_failures=0/3 experiments=1/20 agent_runs=0/10\ncodex_context: mode=resume session={CODEX_SESSION_ID}\nsummary: 1 active task(s), 0 pending event(s), 0 active agent run(s)",
            harness.project().root_path.display(),
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

    let pueue = OperatorPueue::with_tasks(vec![harness.running_task()]);
    let mut daemon = Daemon::new(
        harness.db.clone(),
        pueue.clone(),
        harness.policy(),
        harness.runner(),
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
    let paused_event = EventRepository::new(&harness.db)
        .find_by_id(event_id)
        .unwrap()
        .unwrap();
    assert_eq!(paused_event.status, pueue_agent::models::EventStatus::RetryWait);
    let paused_attempts: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT attempts FROM events WHERE event_id = ?1",
            [event_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(paused_attempts, 0);
    let paused_runs: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM agent_runs WHERE project_id = 'project-a'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(paused_runs, 0);

    status::resume_project(&harness.db, "project-a", harness.now + 2).unwrap();
    let claimed = EventRepository::new(&harness.db)
        .claim_batch(harness.now + 61, harness.now + 120, 10)
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
