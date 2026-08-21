#[test]
fn version_reports_package_revision_and_json_mode() {
    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["version", "--json"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(value["schema_version"].is_u64());
    assert!(value["package_version"].is_string());
    assert!(value["revision"].is_string());
    assert!(value["source"].is_string());
    assert!(matches!(
        value["service"].as_str(),
        Some("running" | "stopped" | "not_installed" | "unknown")
    ));
}

#[test]
fn version_help_describes_json_output() {
    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["version", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("--json"));
}

#[test]
fn upgrade_pre_run_errors_include_a_safe_diagnostic_command() {
    for args in [
        vec!["upgrade", "--source", "/tmp/nonexistent"],
        vec!["upgrade", "--source", "/tmp/nonexistent", "--json"],
    ] {
        let output = assert_cmd::Command::cargo_bin("pueue-agent")
            .unwrap()
            .args(args)
            .output()
            .unwrap();

        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("next diagnostic: pueue-agent version"));
        assert!(stderr.len() <= 500);
    }
}

#[test]
fn upgrade_without_an_existing_policy_creates_no_database_or_upgrade_state() {
    let temporary = tempfile::tempdir().unwrap();
    let state_dir = temporary.path().join("state");
    let codex_home = temporary.path().join("codex-home");
    std::fs::create_dir(&state_dir).unwrap();
    std::fs::create_dir(&codex_home).unwrap();
    let pueue_config = temporary.path().join("pueue.yml");
    std::fs::write(&pueue_config, "fixture: true\n").unwrap();

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .env("HOME", temporary.path())
        .env("CODEX_HOME", &codex_home)
        .env("PUEUE_AGENT_STATE_DIR", &state_dir)
        .args([
            "upgrade",
            "--source",
            temporary.path().to_str().unwrap(),
            "--pueue-config",
            pueue_config.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!state_dir.join("state.sqlite3").exists());
    assert!(!state_dir.join("upgrade.lock").exists());
    assert!(!state_dir.join("upgrade.pending").exists());
}

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
    assert!(text.contains("start"));
    assert!(text.contains("stop"));
    assert!(text.contains("upgrade"));

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["upgrade", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("--source"));
    assert!(text.contains("--pueue-config"));
    assert!(text.contains("--json"));

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
fn campaign_command_help_exposes_campaign_proposal_and_experiment_actions() {
    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    for command in ["campaign", "proposal", "experiment"] {
        assert!(text.contains(command), "missing {command} from top-level help");
    }

    for (command, actions) in [
        ("campaign", &["status", "pause", "resume", "retire"][..]),
        ("proposal", &["list", "inspect"][..]),
        ("experiment", &["list", "inspect"][..]),
    ] {
        let output = assert_cmd::Command::cargo_bin("pueue-agent")
            .unwrap()
            .args([command, "--help"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let text = String::from_utf8_lossy(&output.stdout);
        for action in actions {
            assert!(text.contains(action), "missing {command} {action} from help");
        }
    }
}

#[test]
fn command_reference_covers_every_public_cli_without_exposing_internal_launch() {
    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    let visible_commands = help
        .lines()
        .skip_while(|line| *line != "Commands:")
        .skip(1)
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.split_whitespace().next())
        .filter(|command| *command != "help")
        .collect::<std::collections::BTreeSet<_>>();
    assert!(!visible_commands.contains("internal-launch"));

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let commands = std::fs::read_to_string(root.join("docs/commands-ja.md")).unwrap();
    let documented_headings = commands
        .lines()
        .filter_map(|line| {
            line.strip_prefix("### `pueue-agent ")
                .and_then(|heading| heading.strip_suffix('`'))
        })
        .collect::<std::collections::BTreeSet<_>>();
    let mut expected_headings = visible_commands;
    expected_headings.insert("steer list");

    assert_eq!(documented_headings, expected_headings);
    assert!(commands.lines().any(|line| line == "### `pueue-agent steer list`"));
    assert!(!commands
        .lines()
        .any(|line| line == "### `pueue-agent internal-launch`"));
}

#[test]
fn operations_redirect_links_to_workflows_and_troubleshooting() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let operations = std::fs::read_to_string(root.join("docs/operations-ja.md")).unwrap();

    assert!(operations.contains("[運用ワークフロー](workflows-ja.md)"));
    assert!(operations.contains("[トラブルシューティング](troubleshooting-ja.md)"));
    assert!(root.join("docs/workflows-ja.md").is_file());
    assert!(root.join("docs/troubleshooting-ja.md").is_file());
}

#[test]
fn readme_links_every_user_and_developer_guide() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let readme = std::fs::read_to_string(root.join("README.md")).unwrap();
    for link in [
        "docs/getting-started-ja.md",
        "docs/commands-ja.md",
        "docs/workflows-ja.md",
        "docs/architecture-ja.md",
        "docs/troubleshooting-ja.md",
    ] {
        assert!(readme.contains(link), "README is missing link {link}");
        assert!(
            root.join(link).is_file(),
            "README link target does not exist: {link}"
        );
    }
}

#[test]
fn phase_2_campaign_documentation_covers_autonomous_terminal_loop_and_phase_3_boundary() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let readme = std::fs::read_to_string(root.join("README.md")).unwrap();
    let getting_started =
        std::fs::read_to_string(root.join("docs/getting-started-ja.md")).unwrap();
    let commands = std::fs::read_to_string(root.join("docs/commands-ja.md")).unwrap();
    let workflows = std::fs::read_to_string(root.join("docs/workflows-ja.md")).unwrap();
    let architecture = std::fs::read_to_string(root.join("docs/architecture-ja.md")).unwrap();
    let troubleshooting =
        std::fs::read_to_string(root.join("docs/troubleshooting-ja.md")).unwrap();

    let quick_start = concat!(
        "pueue-agent init\n",
        "# edit .pueue-agent/STATE.md\n",
        "pueue-agent enable\n",
        "pueue-agent submit -- python train.py"
    );
    assert!(readme.contains(quick_start));
    assert!(getting_started.contains(quick_start));

    for command in [
        "pueue-agent campaign status",
        "pueue-agent campaign pause",
        "pueue-agent campaign resume",
        "pueue-agent campaign retire",
        "pueue-agent proposal list",
        "pueue-agent proposal inspect <proposal-id>",
        "pueue-agent experiment list",
        "pueue-agent experiment inspect <experiment-id>",
    ] {
        assert!(
            commands.contains(command),
            "command reference is missing {command}"
        );
    }

    assert!(workflows.contains("Campaign を retire して新しい目的を開始する"));
    assert!(workflows.contains(
        "pueue-agent campaign retire\n# edit .pueue-agent/STATE.md\npueue-agent submit -- python train.py"
    ));

    let intent = architecture
        .find("campaign、baseline proposal、experiment、budget reservation、submission intent")
        .expect("architecture must describe the atomic managed intent");
    let add = architecture[intent..]
        .find("verified Pueue add")
        .map(|offset| intent + offset)
        .expect("architecture must place Pueue add after the durable intent");
    let accepted = architecture[add..]
        .find("accepted task ID/signature")
        .map(|offset| add + offset)
        .expect("architecture must place accepted identity after Pueue add");
    assert!(intent < add && add < accepted);

    assert!(troubleshooting.contains("unreconciled"));
    assert!(!troubleshooting.to_ascii_lowercase().contains("retry submit"));

    for document in [&readme, &getting_started] {
        assert!(document.contains("Phase 2"));
        assert!(document.contains("terminal completion loop"));
        assert!(document.contains("proposal"));
        assert!(document.contains("finite wait"));
        assert!(document.contains("Phase 3"));
        assert!(document.contains("running OOM/stall observer"));
        assert!(document.contains("periodic observer"));
        assert!(document.contains("goal review"));
        assert!(document.contains("code worktree"));
        assert!(!document.contains("現時点では自動で次の学習を投入しません"));
    }

    for document in [&architecture, &workflows] {
        assert!(document.contains("decision cycle"));
        assert!(document.contains("pending"));
        assert!(document.contains("analyzing"));
        assert!(document.contains("waiting"));
        assert!(document.contains("completed"));
        assert!(document.contains("degraded"));
        assert!(document.contains("next_wake_at"));
        assert!(document.contains("attempt_count"));
        assert!(document.contains("last_decision_kind"));
    }

    assert!(troubleshooting.contains("decision_attempts_exhausted"));
    assert!(troubleshooting.contains("decision.digests"));
    assert!(troubleshooting.contains("campaign pause"));

    let instructions = std::fs::read_to_string(root.join("templates/instructions.md")).unwrap();
    assert!(instructions.contains("Phase 2 decision agent"));
    assert!(instructions.contains("exactly one structured decision"));
    assert!(instructions.contains("proposal"));
    assert!(instructions.contains("wait"));
    assert!(instructions.contains("Pueue を直接呼び出さない"));
    assert!(instructions.contains("source を編集しない"));
}

#[test]
fn help_lists_service_lifecycle_commands() {
    for command in ["start", "stop"] {
        let output = assert_cmd::Command::cargo_bin("pueue-agent")
            .unwrap()
            .args([command, "--help"])
            .output()
            .unwrap();

        assert!(output.status.success());
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(text.contains("--json"));
        assert!(!text.contains("PROJECT_ROOT"));
        assert!(!text.contains("PUEUE_CONFIG"));
    }
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
    let trusted_dir = temp.path().join("trusted-bin");
    fs::create_dir_all(&trusted_dir).unwrap();
    let pueue = trusted_dir.join("pueue");
    fs::copy(env!("CARGO_BIN_EXE_pueue-agent"), &pueue).unwrap();
    make_executable(&pueue);
    let policy_paths = install_cli_policy(temp.path(), &state_dir, &trusted_dir, &pueue);

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .env("PUEUE_AGENT_STATE_DIR", &policy_paths.state_dir)
        .env("HOME", &policy_paths.home)
        .env("CODEX_HOME", &policy_paths.codex_home)
        .env("PATH", &policy_paths.trusted_dir)
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
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use pueue_agent::{
    config,
    db::{
        AgentRunRepository, CampaignRepository, Db, EventRepository, IncidentRepository,
        ProjectRepository, StartCampaignRequest, SubmissionRepository, TaskObservationRepository,
    },
    execution_policy::{CampaignLimits, StartupEnvironment},
    models::{
        AgentRunStatus, EventKind, NewAgentRun, NewEvent, NewIncident, NewProject, NewSubmission,
        NewTaskObservation, ProposalKind,
    },
    proposals::{self, ProposalInput},
    service::{
        callback_command, CallbackRegistry, PueueConfigCallbackRegistry, ServiceDefinition,
        ServicePaths,
    },
};
use serde_json::Value;
use tempfile::TempDir;

struct CliPolicyPaths {
    state_dir: PathBuf,
    home: PathBuf,
    codex_home: PathBuf,
    trusted_dir: PathBuf,
}

#[cfg(unix)]
fn install_cli_policy(
    base: &std::path::Path,
    state_dir: &std::path::Path,
    trusted_dir: &std::path::Path,
    pueue_bin: &std::path::Path,
) -> CliPolicyPaths {
    use std::os::unix::fs::PermissionsExt;

    let base = fs::canonicalize(base).unwrap();
    let state_dir = fs::canonicalize(state_dir).unwrap();
    fs::create_dir_all(trusted_dir).unwrap();
    fs::set_permissions(trusted_dir, fs::Permissions::from_mode(0o700)).unwrap();
    let trusted_dir = fs::canonicalize(trusted_dir).unwrap();
    let pueue_bin = fs::canonicalize(pueue_bin).unwrap();
    let codex = trusted_dir.join("codex");
    fs::copy(env!("CARGO_BIN_EXE_pueue-agent"), &codex).unwrap();
    make_executable(&codex);
    let home = base.join("home");
    let codex_home = base.join("codex-home");
    let pueue_config = home.join(".config/pueue/pueue.yml");
    fs::create_dir_all(pueue_config.parent().unwrap()).unwrap();
    fs::create_dir_all(&codex_home).unwrap();
    fs::set_permissions(&home, fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(&codex_home, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(&pueue_config, "fixture: true\n").unwrap();
    fs::set_permissions(&pueue_config, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(
        state_dir.join("execution-policy.toml"),
        format!(
            "version = 1\ntrusted_path = {:?}\n\n[executables]\ncodex = {:?}\npueue = {:?}\n",
            trusted_dir.display().to_string(),
            codex.display().to_string(),
            pueue_bin.display().to_string(),
        ),
    )
    .unwrap();
    fs::set_permissions(
        state_dir.join("execution-policy.toml"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    CliPolicyPaths {
        state_dir,
        home,
        codex_home,
        trusted_dir,
    }
}

#[cfg(unix)]
fn compile_marker_pueue(target: &std::path::Path, marker: &std::path::Path) {
    let source_path = target.with_extension("rs");
    fs::write(
        &source_path,
        format!(
            "fn main() {{ std::fs::write({:?}, b\"invoked\").unwrap(); }}\n",
            marker.to_string_lossy()
        ),
    )
    .unwrap();
    let output = Command::new("rustc")
        .args(["--edition=2021", "-O", "-o"])
        .arg(target)
        .arg(&source_path)
        .output()
        .expect("compile marker Pueue fixture");
    assert!(
        output.status.success(),
        "marker Pueue fixture failed to compile: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    make_executable(target);
}

#[cfg(unix)]
#[test]
fn status_missing_policy_is_existing_only_and_never_invokes_pueue() {
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
    let trusted_dir = temp.path().join("trusted-bin");
    fs::create_dir_all(&trusted_dir).unwrap();
    let marker = temp.path().join("pueue-invoked");
    let pueue = trusted_dir.join("pueue");
    compile_marker_pueue(&pueue, &marker);
    let policy_paths = install_cli_policy(temp.path(), &state_dir, &trusted_dir, &pueue);
    let policy_path = state_dir.join("execution-policy.toml");
    fs::remove_file(&policy_path).unwrap();

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .env("PUEUE_AGENT_STATE_DIR", &policy_paths.state_dir)
        .env("HOME", &policy_paths.home)
        .env("CODEX_HOME", &policy_paths.codex_home)
        .env("PATH", &policy_paths.trusted_dir)
        .current_dir(&root)
        .args(["status", "--json"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!policy_path.exists());
    assert!(!marker.exists());
    assert!(ProjectRepository::new(&db)
        .find_by_root(&root)
        .unwrap()
        .is_some());
}

#[cfg(unix)]
#[test]
fn enable_policy_failure_has_zero_downstream_side_effects() {
    use std::os::unix::fs::PermissionsExt;

    let temp = TempDir::new().unwrap();
    let root = temp.path().join("project");
    fs::create_dir_all(&root).unwrap();
    pueue_agent::init::run(&root).unwrap();
    let state_dir = temp.path().join("state");
    fs::create_dir_all(&state_dir).unwrap();
    fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700)).unwrap();
    let trusted_dir = temp.path().join("trusted-bin");
    fs::create_dir_all(&trusted_dir).unwrap();
    let marker = temp.path().join("pueue-invoked");
    let pueue = trusted_dir.join("pueue");
    compile_marker_pueue(&pueue, &marker);
    let policy_paths = install_cli_policy(temp.path(), &state_dir, &trusted_dir, &pueue);
    let policy_path = state_dir.join("execution-policy.toml");
    fs::write(&policy_path, "version = 2\n").unwrap();
    fs::set_permissions(&policy_path, fs::Permissions::from_mode(0o600)).unwrap();
    let pueue_config = policy_paths.home.join(".config/pueue/pueue.yml");
    let config_before = fs::read(&pueue_config).unwrap();

    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .env("PUEUE_AGENT_STATE_DIR", &policy_paths.state_dir)
        .env("HOME", &policy_paths.home)
        .env("CODEX_HOME", &policy_paths.codex_home)
        .env("PATH", &policy_paths.trusted_dir)
        .args(["enable", root.to_str().unwrap()])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!state_dir.join("state.sqlite3").exists());
    assert!(!marker.exists());
    assert_eq!(fs::read(&pueue_config).unwrap(), config_before);
    assert!(!policy_paths
        .home
        .join(".config/systemd/user/pueue-agent.service")
        .exists());
    assert!(!policy_paths
        .home
        .join("Library/LaunchAgents/com.pueue-agent.plist")
        .exists());
}

struct DiagnosticsCliHarness {
    _temp: TempDir,
    root: std::path::PathBuf,
    db: Db,
    project_id: String,
    policy_paths: CliPolicyPaths,
}

#[cfg(unix)]
struct CallbackCliHarness {
    _temp: TempDir,
    root: PathBuf,
    db: Db,
    policy_paths: CliPolicyPaths,
    pueue_config: PathBuf,
    injected_marker: PathBuf,
    pueue_invoked_marker: PathBuf,
}

#[cfg(unix)]
impl CallbackCliHarness {
    fn with_task(task_id: i64, group: &str) -> Self {
        Self::with_task_and_registered_group(task_id, group, group)
    }

    fn with_task_and_registered_group(
        task_id: i64,
        task_group: &str,
        registered_group: &str,
    ) -> Self {
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
                registered_group,
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();

        let trusted_dir = temp.path().join("trusted-bin");
        fs::create_dir_all(&trusted_dir).unwrap();
        let pueue = trusted_dir.join("pueue");
        let injected_marker = temp.path().join("injected");
        let pueue_invoked_marker = temp.path().join("pueue-invoked");
        let status = serde_json::json!({
            "tasks": {
                task_id.to_string(): {
                    "id": task_id,
                    "group": task_group,
                    "command": "true",
                    "status": {
                        "Done": {
                            "enqueued_at": "100",
                            "start": "100",
                            "end": "100",
                            "result": "Success"
                        }
                    }
                }
            }
        })
        .to_string();
        let source = pueue.with_extension("rs");
        fs::write(
            &source,
            format!(
                "use std::{{env, fs}};\nfn main() {{\n    let args = env::args().collect::<Vec<_>>();\n    fs::write({:?}, b\"invoked\").unwrap();\n    if args.iter().any(|argument| argument.contains(\"touch injected\")) {{ fs::write({:?}, b\"injected\").unwrap(); }}\n    if args.iter().any(|argument| argument == \"status\") {{ println!(\"{{}}\", {:?}); }}\n}}\n",
                pueue_invoked_marker, injected_marker, status
            ),
        )
        .unwrap();
        let output = Command::new("rustc")
            .args(["--edition=2021", "-O", "-o"])
            .arg(&pueue)
            .arg(&source)
            .output()
            .expect("compile callback Pueue fixture");
        assert!(
            output.status.success(),
            "callback Pueue fixture failed to compile: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        make_executable(&pueue);
        let policy_paths = install_cli_policy(temp.path(), &state_dir, &trusted_dir, &pueue);
        let default_pueue_config = policy_paths.home.join(".config/pueue/pueue.yml");
        let pueue_config = temp.path().join("callback-profile/pueue.yml");
        fs::create_dir_all(pueue_config.parent().unwrap()).unwrap();
        fs::set_permissions(
            pueue_config.parent().unwrap(),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        fs::rename(&default_pueue_config, &pueue_config).unwrap();

        Self {
            _temp: temp,
            root,
            db,
            policy_paths,
            pueue_config,
            injected_marker,
            pueue_invoked_marker,
        }
    }

    fn run_installed_callback(&self, task_id: i64) -> std::process::Output {
        self.run_installed_callback_from(task_id, &self.root)
    }

    fn run_installed_callback_from(
        &self,
        task_id: i64,
        working_directory: &Path,
    ) -> std::process::Output {
        let paths = ServicePaths {
            release_binary: PathBuf::from(env!("CARGO_BIN_EXE_pueue-agent")),
            pueue_config: self.pueue_config.clone(),
            state_dir: self.policy_paths.state_dir.clone(),
            execution_policy: self.policy_paths.state_dir.join("execution-policy.toml"),
            working_dir: self.root.clone(),
            home: self.policy_paths.home.clone(),
            codex_home: self.policy_paths.codex_home.clone(),
            path_env: self.policy_paths.trusted_dir.display().to_string(),
            startup_environment: StartupEnvironment::default(),
        };
        let definition = if cfg!(target_os = "macos") {
            self.policy_paths
                .home
                .join("Library/LaunchAgents/com.pueue-agent.plist")
        } else {
            self.policy_paths
                .home
                .join(".config/systemd/user/pueue-agent.service")
        };
        fs::create_dir_all(definition.parent().unwrap()).unwrap();
        let rendered = if cfg!(target_os = "macos") {
            ServiceDefinition::launchd(&paths).render()
        } else {
            ServiceDefinition::systemd(&paths).render()
        };
        fs::write(definition, rendered).unwrap();
        let expected = callback_command(&paths);
        let registry = PueueConfigCallbackRegistry::new(&self.pueue_config);
        registry.set_callback(&expected).unwrap();
        let installed = registry.current_callback().unwrap().unwrap();
        assert_eq!(installed, expected);
        assert!(!installed.contains("{{ group }}"));
        let command = installed.replace("{{ id }}", &task_id.to_string());
        assert!(!command.contains("{{"));

        Command::new("/bin/sh")
            .args(["-c", &command])
            .env("PUEUE_AGENT_STATE_DIR", &self.policy_paths.state_dir)
            .env("HOME", &self.policy_paths.home)
            .env("CODEX_HOME", &self.policy_paths.codex_home)
            .env("PATH", &self.policy_paths.trusted_dir)
            .current_dir(working_directory)
            .output()
            .unwrap()
    }

    fn run_callback_with_arguments(&self, arguments: &[&str]) -> std::process::Output {
        self.command().args(arguments).output().unwrap()
    }

    fn register_additional_project(&self, directory: &str, group: &str) {
        let root = self._temp.path().join(directory);
        fs::create_dir(&root).unwrap();
        pueue_agent::init::run(&root).unwrap();
        let project_config = config::load(&root.join(".pueue-agent/config.toml")).unwrap();
        ProjectRepository::new(&self.db)
            .register(&NewProject::new(
                &project_config.project_id,
                &root,
                group,
                root.join(".pueue-agent/config.toml"),
                101,
            ))
            .unwrap();
    }

    fn command(&self) -> assert_cmd::Command {
        let mut command = assert_cmd::Command::cargo_bin("pueue-agent").unwrap();
        command
            .env("PUEUE_AGENT_STATE_DIR", &self.policy_paths.state_dir)
            .env("HOME", &self.policy_paths.home)
            .env("CODEX_HOME", &self.policy_paths.codex_home)
            .env("PATH", &self.policy_paths.trusted_dir)
            .current_dir(&self.root);
        command
    }

    fn events_for(&self, task_id: i64) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE payload_json LIKE ?1",
                [format!("%\"task_id\":{task_id}%")],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn event_count(&self) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap()
    }

    fn integration_event_count(&self) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM integration_events", [], |row| row.get(0))
            .unwrap()
    }
}

#[cfg(unix)]
#[test]
fn callback_resolves_and_validates_group_from_numeric_task_id() {
    let valid = CallbackCliHarness::with_task(41, "pa-project");

    let first = valid.run_installed_callback(41);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let second = valid.run_installed_callback(41);
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(valid.events_for(41), 1);

    let injected = CallbackCliHarness::with_task(42, "x'; touch injected; #");
    let output = injected.run_installed_callback(42);
    assert!(!output.status.success());
    assert!(!injected.injected_marker.exists());
    assert_eq!(injected.event_count(), 0);
    assert_eq!(injected.integration_event_count(), 0);
}

#[cfg(unix)]
#[test]
fn implicit_callback_resolves_registered_project_from_an_unrelated_working_directory() {
    let harness =
        CallbackCliHarness::with_task_and_registered_group(45, "pa-second", "pa-first");
    harness.register_additional_project("second-project", "pa-second");
    let unrelated = harness._temp.path().join("unrelated-daemon-cwd");
    fs::create_dir(&unrelated).unwrap();

    let output = harness.run_installed_callback_from(45, &unrelated);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(harness.events_for(45), 1);
}

#[cfg(unix)]
#[test]
fn explicit_callback_group_remains_independent_of_cwd_and_pueue_status() {
    let harness = CallbackCliHarness::with_task(46, "pa-project");
    let unrelated = harness._temp.path().join("explicit-callback-cwd");
    fs::create_dir(&unrelated).unwrap();

    let output = harness
        .command()
        .current_dir(&unrelated)
        .args([
            "event",
            "callback",
            "--group",
            "pa-project",
            "--task-id",
            "46",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!harness.pueue_invoked_marker.exists());
    assert_eq!(harness.events_for(46), 1);
}

#[cfg(unix)]
#[test]
fn implicit_callback_rejects_an_unregistered_pueue_group_without_event_mutation() {
    let harness = CallbackCliHarness::with_task_and_registered_group(
        43,
        "pa-unregistered",
        "pa-registered",
    );

    let output = harness.run_installed_callback(43);

    assert!(!output.status.success());
    assert_eq!(harness.event_count(), 0);
    assert_eq!(harness.integration_event_count(), 0);
}

#[cfg(unix)]
#[test]
fn negative_callback_task_id_rejects_before_metadata_pueue_or_sqlite_work() {
    let harness = CallbackCliHarness::with_task(44, "pa-project");
    let database = harness.policy_paths.state_dir.join("state.sqlite3");
    let before = fs::read(&database).unwrap();

    let output = harness.run_callback_with_arguments(&[
        "event",
        "callback",
        "--task-id=-1",
        "--metadata",
        "not-json",
    ]);

    assert!(!output.status.success());
    assert!(!harness.pueue_invoked_marker.exists());
    assert_eq!(fs::read(database).unwrap(), before);
    assert_eq!(harness.event_count(), 0);
    assert_eq!(harness.integration_event_count(), 0);
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
        let trusted_dir = temp.path().join("trusted-bin");
        fs::create_dir_all(&trusted_dir).unwrap();
        let pueue = trusted_dir.join("pueue");
        fs::copy(env!("CARGO_BIN_EXE_pueue-agent"), &pueue).unwrap();
        make_executable(&pueue);
        let policy_paths = install_cli_policy(temp.path(), &state_dir, &trusted_dir, &pueue);
        Self {
            _temp: temp,
            root,
            db,
            project_id: project_config.project_id,
            policy_paths,
        }
    }

    fn command(&self) -> assert_cmd::Command {
        let mut command = assert_cmd::Command::cargo_bin("pueue-agent").unwrap();
        command
            .env("PUEUE_AGENT_STATE_DIR", &self.policy_paths.state_dir)
            .env("HOME", &self.policy_paths.home)
            .env("CODEX_HOME", &self.policy_paths.codex_home)
            .env("PATH", &self.policy_paths.trusted_dir)
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

const SUBMIT_BATCH_PUEUE_SOURCE: &str = r#"
use std::{env, fs, io::Write};

fn main() {
    let is_add = env::args_os().any(|argument| argument == "add");
    if !is_add {
        return;
    }
    let count = fs::read_to_string(__ADD_COUNT__)
        .expect("read add count")
        .parse::<usize>()
        .expect("parse add count") + 1;
    fs::write(__ADD_COUNT__, count.to_string()).expect("write add count");
    let fail = fs::read_to_string(__FAIL_ON_ADD__)
        .expect("read failure ordinal")
        .parse::<usize>()
        .expect("parse failure ordinal");
    if fail > 0 && count == fail {
        std::io::stderr().write_all(b"fake pueue failure secret").expect("write failure");
        std::process::exit(7);
    }
    println!("{}", 700 + count);
}
"#;

struct SubmitBatchCliHarness {
    _temp: TempDir,
    root: PathBuf,
    db: Db,
    project_id: String,
    manifest: PathBuf,
    add_count: PathBuf,
    fail_on_add: PathBuf,
    policy_paths: CliPolicyPaths,
}

struct CustomPueueProfileHarness {
    _temp: TempDir,
    root: PathBuf,
    policy_paths: CliPolicyPaths,
    custom_config: PathBuf,
    conflicting_config: PathBuf,
    pueue_calls: PathBuf,
    service_definition: PathBuf,
}

impl CustomPueueProfileHarness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        pueue_agent::init::run(&root).unwrap();
        fs::write(
            root.join(".pueue-agent/STATE.md"),
            "Reach validation loss below 0.20\n",
        )
        .unwrap();

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

        let trusted_dir = temp.path().join("trusted-bin");
        fs::create_dir_all(&trusted_dir).unwrap();
        let pueue = trusted_dir.join("pueue");
        let pueue_calls = temp.path().join("pueue-calls");
        let pueue_state = temp.path().join("pueue-state");
        let source = temp.path().join("custom-profile-pueue.rs");
        let source_template = r#"use std::{fs, fs::OpenOptions, io::Write};
fn current_count() -> usize {
    fs::read_to_string(__STATE__)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}
fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let operation = args
        .iter()
        .find(|argument| matches!(argument.as_str(), "status" | "add" | "group" | "kill" | "remove"))
        .map(String::as_str)
        .unwrap_or("");
    let config = fs::read_to_string("/dev/fd/9").unwrap_or_default();
    writeln!(OpenOptions::new().create(true).append(true).open(__CALLS__).unwrap(), "{}:{}", operation, config.trim()).unwrap();
    if operation == "add" {
        let count = current_count() + 1;
        fs::write(__STATE__, count.to_string()).unwrap();
        println!("{}", 700 + count);
    }
    if operation == "status" {
        let count = current_count();
        if count == 0 {
            std::io::stdout().write_all(b"\x7b\"tasks\":\x7b\x7d\x7d\n").unwrap();
        } else {
            let task_id = 700 + count;
            println!(
                "{{\"tasks\":{{\"{}\":{{\"id\":{},\"group\":{:?},\"command\":\"/usr/bin/true\",\"status\":{{\"Queued\":{{\"enqueued_at\":\"{}\"}}}}}}}}}}",
                task_id,
                task_id,
                __GROUP__,
                task_id,
            );
        }
    }
    if operation == "group" && args.iter().any(|argument| argument == "-j") {
        std::io::stdout().write_all(b"\x7b\x7d\n").unwrap();
    }
}"#;
        let source_body = source_template
            .replace("__STATE__", &format!("{pueue_state:?}"))
            .replace("__CALLS__", &format!("{pueue_calls:?}"))
            .replace("__GROUP__", &format!("{:?}", project_config.pueue_group));
        fs::write(
            &source,
            source_body,
        )
        .unwrap();
        let output = Command::new("rustc")
            .args(["--edition=2021", "-O", "-o"])
            .arg(&pueue)
            .arg(&source)
            .output()
            .expect("compile custom-profile Pueue fixture");
        assert!(
            output.status.success(),
            "custom-profile Pueue fixture failed to compile: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        make_executable(&pueue);
        let service_command = if cfg!(target_os = "macos") {
            trusted_dir.join("launchctl")
        } else {
            trusted_dir.join("systemctl")
        };
        let service_script = if cfg!(target_os = "macos") {
            "#!/bin/sh\necho 'state = running'\n"
        } else {
            "#!/bin/sh\ncase \"$*\" in\n  *LoadState*) echo loaded ;;\n  *is-active*) echo active ;;\nesac\n"
        };
        fs::write(&service_command, service_script).unwrap();
        make_executable(&service_command);
        let policy_paths = install_cli_policy(temp.path(), &state_dir, &trusted_dir, &pueue);

        let custom_config = temp.path().join("custom/pueue.yml");
        let conflicting_config = temp.path().join("conflicting/pueue.yml");
        fs::create_dir_all(custom_config.parent().unwrap()).unwrap();
        fs::create_dir_all(conflicting_config.parent().unwrap()).unwrap();
        fs::write(&custom_config, "fixture: custom\n").unwrap();
        fs::write(&conflicting_config, "fixture: conflicting\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                custom_config.parent().unwrap(),
                fs::Permissions::from_mode(0o700),
            )
            .unwrap();
            fs::set_permissions(&custom_config, fs::Permissions::from_mode(0o600)).unwrap();
            fs::set_permissions(
                conflicting_config.parent().unwrap(),
                fs::Permissions::from_mode(0o700),
            )
            .unwrap();
            fs::set_permissions(&conflicting_config, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let definition = if cfg!(target_os = "macos") {
            policy_paths
                .home
                .join("Library/LaunchAgents/com.pueue-agent.plist")
        } else {
            policy_paths
                .home
                .join(".config/systemd/user/pueue-agent.service")
        };
        fs::create_dir_all(definition.parent().unwrap()).unwrap();
        let service_paths = ServicePaths {
            release_binary: PathBuf::from(env!("CARGO_BIN_EXE_pueue-agent")),
            pueue_config: custom_config.clone(),
            state_dir: policy_paths.state_dir.clone(),
            execution_policy: policy_paths.state_dir.join("execution-policy.toml"),
            working_dir: root.clone(),
            home: policy_paths.home.clone(),
            codex_home: policy_paths.codex_home.clone(),
            path_env: policy_paths.trusted_dir.display().to_string(),
            startup_environment: StartupEnvironment::default(),
        };
        let rendered = if cfg!(target_os = "macos") {
            ServiceDefinition::launchd(&service_paths).render()
        } else {
            ServiceDefinition::systemd(&service_paths).render()
        };
        fs::write(&definition, rendered).unwrap();
        fs::remove_file(policy_paths.home.join(".config/pueue/pueue.yml")).unwrap();

        Self {
            _temp: temp,
            root,
            policy_paths,
            custom_config,
            conflicting_config,
            pueue_calls,
            service_definition: definition,
        }
    }

    fn command(&self) -> assert_cmd::Command {
        let mut command = assert_cmd::Command::cargo_bin("pueue-agent").unwrap();
        command
            .env("PUEUE_AGENT_STATE_DIR", &self.policy_paths.state_dir)
            .env("HOME", &self.policy_paths.home)
            .env("CODEX_HOME", &self.policy_paths.codex_home)
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.policy_paths.trusted_dir.display()),
            )
            .current_dir(&self.root);
        command
    }
}

impl SubmitBatchCliHarness {
    fn new(manifest: &str) -> Self {
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

        let manifest_path = temp.path().join("jobs.json");
        fs::write(&manifest_path, manifest).unwrap();
        let pueue_dir = temp.path().join("bin");
        fs::create_dir_all(&pueue_dir).unwrap();
        let pueue_bin = pueue_dir.join("pueue");
        let add_count = temp.path().join("add-count");
        let fail_on_add = temp.path().join("fail-on-add");
        fs::write(&add_count, "0").unwrap();
        fs::write(&fail_on_add, "0").unwrap();
        let source_path = temp.path().join("submit-batch-pueue.rs");
        let source = SUBMIT_BATCH_PUEUE_SOURCE
            .replace("__ADD_COUNT__", &rust_string(&add_count))
            .replace("__FAIL_ON_ADD__", &rust_string(&fail_on_add));
        fs::write(&source_path, source).unwrap();
        let output = Command::new("rustc")
            .args(["--edition=2021", "-O", "-o"])
            .arg(&pueue_bin)
            .arg(&source_path)
            .output()
            .expect("compile generated submit-batch Pueue fixture");
        assert!(
            output.status.success(),
            "generated submit-batch Pueue fixture failed to compile: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        make_executable(&pueue_bin);
        let policy_paths = install_cli_policy(temp.path(), &state_dir, &pueue_dir, &pueue_bin);

        Self {
            _temp: temp,
            root,
            db,
            project_id: project_config.project_id,
            manifest: manifest_path,
            add_count,
            fail_on_add,
            policy_paths,
        }
    }

    fn command(&self) -> assert_cmd::Command {
        let mut command = assert_cmd::Command::cargo_bin("pueue-agent").unwrap();
        command
            .env("PUEUE_AGENT_STATE_DIR", &self.policy_paths.state_dir)
            .env("HOME", &self.policy_paths.home)
            .env("CODEX_HOME", &self.policy_paths.codex_home)
            .env("PATH", &self.policy_paths.trusted_dir)
            .current_dir(&self.root);
        command
    }

    fn run_json(&self, request_id: &str) -> std::process::Output {
        self.command()
            .args([
                "submit-batch",
                "--request-id",
                request_id,
                "--manifest",
                self.manifest.to_str().unwrap(),
                "--json",
            ])
            .output()
            .unwrap()
    }

    fn set_fail_on_add(&self, ordinal: usize) {
        fs::write(&self.fail_on_add, ordinal.to_string()).unwrap();
    }

    fn add_count(&self) -> usize {
        fs::read_to_string(&self.add_count)
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }

    fn activate_campaign(&self) {
        fs::write(
            self.root.join(".pueue-agent/STATE.md"),
            "Reach validation loss below 0.20\n",
        )
        .unwrap();
        let objective = pueue_agent::state::load_objective(&self.root).unwrap();
        let argv = vec!["python".to_owned(), "train.py".to_owned()];
        let proposal = proposals::validate_initial_baseline(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: "Establish the initial campaign baseline".to_owned(),
                source_experiment_id: None,
                argv: argv.clone(),
                working_directory: ".".to_owned(),
                expected_evidence: Vec::new(),
            },
            &objective.digest,
        )
        .unwrap();
        CampaignRepository::new(&self.db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id: "active-campaign-submit-cli",
                    project_id: &self.project_id,
                    objective: &objective,
                    initial_argv: &argv,
                    baseline: &proposal,
                    submission_id: "active-campaign-submit-cli-submission",
                    experiment_id: "active-campaign-submit-cli-experiment",
                    proposal_id: "active-campaign-submit-cli-proposal",
                    metadata: &Value::Object(Default::default()),
                    origin_agent_run_id: None,
                    now: 100,
                },
                &CampaignLimits::default(),
            )
            .unwrap();
    }
}

#[cfg(unix)]
fn make_executable(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn rust_string(path: &std::path::Path) -> String {
    format!("{:?}", path.to_string_lossy())
}

const BATCH_REQUEST_ID: &str = "11111111-1111-4111-8111-111111111111";

#[test]
fn active_campaign_submit_and_batch_are_rejected_before_persistence_or_pueue_add() {
    let harness = SubmitBatchCliHarness::new(
        r#"{"jobs":[{"id":"job-a","argv":["python","other.py"]}]}"#,
    );
    harness.activate_campaign();

    let submit = harness
        .command()
        .args(["submit", "--", "python", "other.py"])
        .output()
        .unwrap();
    let batch = harness.run_json(BATCH_REQUEST_ID);

    assert!(!submit.status.success());
    assert!(!batch.status.success());
    let message = "a managed campaign is active; use pueue-agent steer";
    assert!(String::from_utf8_lossy(&submit.stderr).contains(message));
    assert!(String::from_utf8_lossy(&batch.stderr).contains(message));
    assert_eq!(harness.add_count(), 0);
    let connection = harness.db.connect().unwrap();
    let submissions: i64 = connection
        .query_row("SELECT COUNT(*) FROM submissions", [], |row| row.get(0))
        .unwrap();
    let batches: i64 = connection
        .query_row("SELECT COUNT(*) FROM batch_requests", [], |row| row.get(0))
        .unwrap();
    assert_eq!(submissions, 1);
    assert_eq!(batches, 0);
}

#[test]
fn submit_batch_cli_help_lists_request_manifest_group_and_json_options() {
    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["submit-batch", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    for option in ["--request-id", "--manifest", "--group", "--json"] {
        assert!(text.contains(option), "missing {option}: {text}");
    }
}

#[cfg(unix)]
#[test]
fn custom_pueue_profile_submit_reuses_the_profile_pinned_by_enable_without_a_repeated_flag() {
    let harness = CustomPueueProfileHarness::new();

    let enable = harness
        .command()
        .args(["enable", harness.root.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        enable.status.success(),
        "{}",
        String::from_utf8_lossy(&enable.stderr)
    );
    assert!(
        String::from_utf8_lossy(&fs::read(&harness.service_definition).unwrap())
            .contains(harness.custom_config.to_str().unwrap())
    );

    let manifest = harness.root.join("jobs.json");
    fs::write(
        &manifest,
        r#"{"jobs":[{"id":"custom-profile-batch","argv":["/usr/bin/true"]}]}"#,
    )
    .unwrap();
    for arguments in [
        vec!["submit-batch", "--request-id", BATCH_REQUEST_ID, "--manifest", manifest.to_str().unwrap()],
        vec!["status", "--json"],
    ] {
        let output = harness.command().args(arguments).output().unwrap();
        assert!(
            output.status.success(),
            "stderr:\n{}\nstdout:\n{}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout),
        );
    }
    let output = harness
        .command()
        .args(["submit", "--", "/usr/bin/true"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let doctor = harness
        .command()
        .args(["doctor", "--json"])
        .output()
        .unwrap();
    let doctor_report: Value = serde_json::from_slice(&doctor.stdout).unwrap();
    let doctor_checks = doctor_report["checks"].as_array().unwrap();
    let check_status = |name: &str| {
        doctor_checks
            .iter()
            .find(|check| check["name"] == name)
            .and_then(|check| check["status"].as_str())
    };
    assert_eq!(check_status("pueue.config"), Some("ok"));
    assert_ne!(check_status("pueue.status"), Some("error"));
    let calls = fs::read_to_string(&harness.pueue_calls).unwrap();
    assert!(calls.lines().any(|operation| operation.starts_with("group:fixture: custom")));
    assert!(calls.lines().filter(|operation| operation.starts_with("add:fixture: custom")).count() >= 2);
    assert!(calls.lines().filter(|operation| operation.starts_with("status:fixture: custom")).count() >= 2);
    assert!(!calls.contains("fixture: conflicting"));
}

#[cfg(unix)]
#[test]
fn conflicting_explicit_profile_fails_before_pueue_database_callback_or_service_mutation() {
    let harness = CustomPueueProfileHarness::new();
    let config_before = fs::read(&harness.custom_config).unwrap();
    let definition_before = fs::read(&harness.service_definition).unwrap();
    let database_before = fs::read(
        harness
            .policy_paths
            .state_dir
            .join("state.sqlite3"),
    )
    .unwrap();

    let output = harness
        .command()
        .args([
            "enable",
            "--pueue-config",
            harness.conflicting_config.to_str().unwrap(),
            harness.root.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!harness.pueue_calls.exists());
    assert_eq!(fs::read(&harness.custom_config).unwrap(), config_before);
    assert_eq!(fs::read(&harness.service_definition).unwrap(), definition_before);
    assert_eq!(
        fs::read(harness.policy_paths.state_dir.join("state.sqlite3")).unwrap(),
        database_before
    );
}

#[cfg(unix)]
#[test]
fn conflicting_environment_profile_fails_before_pueue_database_callback_or_service_mutation() {
    let harness = CustomPueueProfileHarness::new();
    let config_before = fs::read(&harness.custom_config).unwrap();
    let definition_before = fs::read(&harness.service_definition).unwrap();
    let database_before = fs::read(
        harness
            .policy_paths
            .state_dir
            .join("state.sqlite3"),
    )
    .unwrap();

    let output = harness
        .command()
        .env("PUEUE_CONFIG", &harness.conflicting_config)
        .args(["status", "--json"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!harness.pueue_calls.exists());
    assert_eq!(fs::read(&harness.custom_config).unwrap(), config_before);
    assert_eq!(fs::read(&harness.service_definition).unwrap(), definition_before);
    assert_eq!(
        fs::read(harness.policy_paths.state_dir.join("state.sqlite3")).unwrap(),
        database_before
    );
}

#[cfg(unix)]
#[test]
fn missing_pueue_config_doctor_renders_a_degraded_error_report() {
    let harness = DiagnosticsCliHarness::new();
    let config = harness.policy_paths.home.join(".config/pueue/pueue.yml");
    fs::remove_file(config).unwrap();

    let output = harness
        .command()
        .args(["doctor", "--json"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(report["checks"].as_array().unwrap().iter().any(|check| {
        check["name"] == "pueue.config" && check["status"] == "error"
    }));
}

#[test]
fn submit_batch_cli_rejects_malformed_manifest_before_pueue_add() {
    let harness = SubmitBatchCliHarness::new("{not-json");
    let output = harness.run_json(BATCH_REQUEST_ID);

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("manifest"));
    assert_eq!(harness.add_count(), 0);
}

#[test]
fn submit_batch_cli_validates_job_shape_before_pueue_add() {
    let harness = SubmitBatchCliHarness::new(
        r#"{"jobs":[{"id":"duplicate","argv":["python"]},{"id":"duplicate","argv":[] }]}"#,
    );
    let output = harness.run_json(BATCH_REQUEST_ID);

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("job_id"));
    assert_eq!(harness.add_count(), 0);
}

#[test]
fn submit_batch_cli_same_request_does_not_add_pueue_task_twice() {
    let harness = SubmitBatchCliHarness::new(
        r#"{"jobs":[{"id":"job-a","argv":["python","train.py"],"metadata":{"secret":"hidden"}}]}"#,
    );
    let first = harness.run_json(BATCH_REQUEST_ID);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let second = harness.run_json(BATCH_REQUEST_ID);
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );

    let result: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(result["status"], "completed");
    assert_eq!(result["jobs"][0]["task_id"], 701);
    assert!(!String::from_utf8_lossy(&second.stdout).contains("hidden"));
    assert_eq!(harness.add_count(), 1);
}

#[test]
fn submit_batch_cli_reports_partial_json_and_stops_after_failed_add() {
    let harness = SubmitBatchCliHarness::new(
        r#"{"jobs":[{"id":"job-a","argv":["python","a.py"]},{"id":"job-b","argv":["python","b.py"]},{"id":"job-c","argv":["python","c.py"]}]}"#,
    );
    harness.set_fail_on_add(2);
    let output = harness.run_json(BATCH_REQUEST_ID);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["status"], "partial");
    assert_eq!(result["counts"]["accepted"], 1);
    assert_eq!(result["counts"]["failed"], 1);
    assert_eq!(result["counts"]["pending"], 1);
    assert_eq!(result["jobs"][0]["task_id"], 701);
    assert_eq!(result["jobs"][1]["status"], "failed");
    assert_eq!(result["jobs"][2]["status"], "pending");
    assert!(!String::from_utf8_lossy(&output.stdout).contains("fake pueue failure secret"));
    assert_eq!(harness.add_count(), 2);

    let replay = harness.run_json(BATCH_REQUEST_ID);
    assert!(
        replay.status.success(),
        "{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    let replay_result: Value = serde_json::from_slice(&replay.stdout).unwrap();
    assert_eq!(replay_result["status"], "partial");
    assert_eq!(replay_result["counts"]["accepted"], 2);
    assert_eq!(replay_result["counts"]["failed"], 1);
    assert_eq!(replay_result["counts"]["pending"], 0);
    assert_eq!(replay_result["jobs"][2]["task_id"], 703);
    assert_eq!(harness.add_count(), 3);
}

#[test]
fn submit_batch_cli_rejects_group_mismatch_before_pueue_add() {
    let harness =
        SubmitBatchCliHarness::new(r#"{"jobs":[{"id":"job-a","argv":["python","train.py"]}]}"#);
    let output = harness
        .command()
        .args([
            "submit-batch",
            "--request-id",
            BATCH_REQUEST_ID,
            "--manifest",
            harness.manifest.to_str().unwrap(),
            "--group",
            "wrong-group",
            "--json",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("group"));
    assert_eq!(harness.add_count(), 0);
}
