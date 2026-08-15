use std::fs;

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::{fs::PermissionsExt, process::CommandExt};

use assert_cmd::{cargo::CommandCargoExt, Command};
use pueue_agent::{
    config,
    db::{AgentRunRepository, Db, EventRepository, ProjectRepository},
    environment::PrivateRunTemp,
    execution_policy::ProjectRootAnchor,
    models::{
        AgentContextMode, AgentRunStatus, EventKind, NewAgentRun, NewEvent, NewProject,
    },
};
use tempfile::TempDir;

fn init(root: &std::path::Path) -> std::process::Output {
    Command::cargo_bin("pueue-agent")
        .unwrap()
        .arg("init")
        .arg(root)
        .output()
        .unwrap()
}

#[test]
fn init_creates_toml_state_and_instructions() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("experiment");
    fs::create_dir(&root).unwrap();

    let output = init(&root);

    assert!(output.status.success());
    let state = root.join(".pueue-agent");
    assert!(state.join("config.toml").is_file());
    assert!(state.join("STATE.md").is_file());
    assert!(state.join("instructions.md").is_file());
    assert!(state.join("logs").is_dir());
    let loaded = config::load(&state.join("config.toml")).unwrap();
    assert_eq!(loaded.agent.context, AgentContextMode::Fresh);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn ordinary_init_container_allows_private_temp_inventory_create_and_removal() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("experiment");
    fs::create_dir(&root).unwrap();
    let mut command = std::process::Command::cargo_bin("pueue-agent").unwrap();
    unsafe {
        command.pre_exec(|| {
            libc::umask(0o022);
            Ok(())
        });
    }

    let output = command.arg("init").arg(&root).output().unwrap();

    assert!(output.status.success());
    assert_eq!(
        fs::metadata(root.join(".pueue-agent"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
    ProjectRepository::new(&db)
        .register(&NewProject::new(
            "project-a",
            &root,
            "pa-init-private-temp",
            root.join(".pueue-agent/config.toml"),
            100,
        ))
        .unwrap();
    let anchor = ProjectRootAnchor::resolve(&fs::canonicalize(&root).unwrap()).unwrap();
    let verified_root = anchor.verify_identity().unwrap();
    let inventory = PrivateRunTemp::inspect_capacity(&verified_root).unwrap();
    assert_eq!(inventory.generations, 0);

    let event_id = EventRepository::new(&db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFinished,
            "init-private-temp-run",
            serde_json::json!({}),
            101,
            101,
        ))
        .unwrap()
        .event_id;
    let run = AgentRunRepository::new(&db)
        .insert(&NewAgentRun::new(
            "project-a",
            event_id,
            None,
            AgentRunStatus::Starting,
            102,
            root.join(".pueue-agent/logs/init-private-temp.log"),
        ))
        .unwrap();
    let generation = PrivateRunTemp::create(&verified_root, run.run_id).unwrap();

    assert_eq!(
        fs::metadata(root.join(".pueue-agent/tmp"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(generation.path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    drop(generation);

    ProjectRepository::new(&db)
        .remove("project-a", 110, &[])
        .unwrap();
    assert_eq!(
        db.connect()
            .unwrap()
            .query_row(
                "SELECT last_run_id FROM agent_run_id_sequence WHERE sequence_id = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        run.run_id
    );
}

#[test]
fn canonical_state_init_creates_bounded_machine_state() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("experiment");
    fs::create_dir(&root).unwrap();

    let output = init(&root);

    assert!(output.status.success());
    let state = root.join(".pueue-agent/state.json");
    let value: serde_json::Value = serde_json::from_slice(&fs::read(state).unwrap()).unwrap();
    assert_eq!(value["schema_version"], 1);
    assert!(value["current_facts"].is_array());
    assert!(value["historical_facts"].is_array());
    assert!(value["next_action"].is_string());
    assert!(value["budgets"].is_object());
    assert!(value["active_lineage"].is_object());
}

#[test]
fn canonical_state_init_preserves_existing_state_json_and_state_md() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("experiment");
    let state = root.join(".pueue-agent");
    fs::create_dir_all(&state).unwrap();
    let state_json = br#"{"schema_version":1,"current_facts":["keep me"],"historical_facts":[],"next_action":"keep next","budgets":{"max_experiments":3},"active_lineage":{"event_id":null,"run_id":null,"submission_ids":[],"task_ids":[]}}"#;
    fs::write(state.join("state.json"), state_json).unwrap();
    fs::write(state.join("STATE.md"), "campaign stopped\nuser history\n").unwrap();
    fs::write(state.join("instructions.md"), "user instructions\n").unwrap();

    let output = init(&root);

    assert!(output.status.success());
    assert_eq!(fs::read(state.join("state.json")).unwrap(), state_json);
    assert_eq!(
        fs::read_to_string(state.join("STATE.md")).unwrap(),
        "campaign stopped\nuser history\n"
    );
    assert_eq!(
        fs::read_to_string(state.join("instructions.md")).unwrap(),
        "user instructions\n"
    );
}

#[test]
fn init_bounds_and_redacts_successful_project_root_output() {
    let temp = TempDir::new().unwrap();
    let root = temp
        .path()
        .join(format!("prefix-{}", "p".repeat(80)))
        .join(format!("segment-{}", "s".repeat(80)))
        .join("AWS_SECRET_ACCESS_KEY=AKIA_INIT_SECRET")
        .join(format!("tail-{}", "t".repeat(80)));
    fs::create_dir_all(&root).unwrap();

    let output = init(&root);

    assert!(output.status.success());
    let line = String::from_utf8_lossy(&output.stdout);
    assert!(line.len() <= "initialized: ".len() + 243);
    assert!(line.contains("[path]"));
    assert!(!line.contains("AWS_SECRET_ACCESS_KEY"));
    assert!(!line.contains("AKIA_INIT_SECRET"));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn operator_success_output_bounds_and_redacts_project_and_group_identity() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("operator-project");
    fs::create_dir(&root).unwrap();
    assert!(init(&root).status.success());

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

    let project_id = format!(
        "project-{} AWS_SECRET_ACCESS_KEY=AKIA_PROJECT_SECRET",
        "p".repeat(320)
    );
    let group = format!("group-{} password=GROUP_SECRET", "g".repeat(320));
    let connection = db.connect().unwrap();
    connection
        .execute(
            "UPDATE projects SET project_id = ?1, pueue_group = ?2",
            (&project_id, &group),
        )
        .unwrap();

    let bin_dir = temp.path().join("bin");
    fs::create_dir(&bin_dir).unwrap();
    let fake_pueue = bin_dir.join("pueue");
    fs::write(&fake_pueue, "#!/bin/sh\nprintf '%s' '{\"tasks\":{}}'\n").unwrap();
    let fake_codex = bin_dir.join("codex");
    fs::copy(env!("CARGO_BIN_EXE_pueue-agent"), &fake_codex).unwrap();
    let home = temp.path().join("home");
    let codex_home = temp.path().join("codex-home");
    let pueue_config = home.join(".config/pueue/pueue.yml");
    fs::create_dir_all(pueue_config.parent().unwrap()).unwrap();
    fs::create_dir(&codex_home).unwrap();
    for directory in [&state_dir, &bin_dir, &home, &codex_home] {
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::set_permissions(&fake_pueue, fs::Permissions::from_mode(0o755)).unwrap();
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(&pueue_config, "fixture: true\n").unwrap();
    fs::set_permissions(&pueue_config, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(
        state_dir.join("execution-policy.toml"),
        format!(
            "version = 1\ntrusted_path = {:?}\n\n[executables]\ncodex = {:?}\npueue = {:?}\n",
            bin_dir.display().to_string(),
            fake_codex.display().to_string(),
            fake_pueue.display().to_string(),
        ),
    )
    .unwrap();
    fs::set_permissions(
        state_dir.join("execution-policy.toml"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();

    let command = |args: &[&str]| {
        let mut command = Command::cargo_bin("pueue-agent").unwrap();
        command
            .env("PUEUE_AGENT_STATE_DIR", &state_dir)
            .env("HOME", &home)
            .env("CODEX_HOME", &codex_home)
            .env("PATH", &bin_dir)
            .current_dir(&root)
            .args(args)
            .output()
            .unwrap()
    };

    let outputs = [
        command(&["pause"]),
        command(&["resume"]),
        command(&["disable"]),
        command(&["disable", "--remove"]),
    ];
    for output in &outputs {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        let max_line_len = if stdout.starts_with("paused: ") {
            "paused: ".len() + 240
        } else if stdout.starts_with("resumed: ") {
            "resumed: ".len() + 240
        } else if stdout.starts_with("disabled: ") {
            "disabled: ".len() + 240 + " (group reserved: )".len() + 240
        } else {
            "removed: ".len() + 240 + " (group released: )".len() + 240
        };
        assert!(stdout.lines().all(|line| line.len() <= max_line_len));
        assert!(!stdout.contains(&project_id));
        assert!(!stdout.contains(&group));
        assert!(!stdout.contains("AKIA_PROJECT_SECRET"));
        assert!(!stdout.contains("GROUP_SECRET"));
    }

    assert!(String::from_utf8_lossy(&outputs[0].stdout).starts_with("paused: "));
    assert!(String::from_utf8_lossy(&outputs[1].stdout).starts_with("resumed: "));
    assert!(String::from_utf8_lossy(&outputs[2].stdout).starts_with("disabled: "));
    assert!(String::from_utf8_lossy(&outputs[3].stdout).starts_with("removed: "));
}

#[test]
fn same_basename_projects_receive_distinct_ids_and_groups() {
    let temp = TempDir::new().unwrap();
    let first = temp.path().join("one/shared");
    let second = temp.path().join("two/shared");
    fs::create_dir_all(&first).unwrap();
    fs::create_dir_all(&second).unwrap();

    assert!(init(&first).status.success());
    assert!(init(&second).status.success());

    let first_config = fs::read_to_string(first.join(".pueue-agent/config.toml")).unwrap();
    let second_config = fs::read_to_string(second.join(".pueue-agent/config.toml")).unwrap();
    let first: toml::Value = toml::from_str(&first_config).unwrap();
    let second: toml::Value = toml::from_str(&second_config).unwrap();
    assert_ne!(first["project_id"], second["project_id"]);
    assert_ne!(first["pueue_group"], second["pueue_group"]);
}

#[test]
fn init_refuses_to_overwrite_an_existing_configuration() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("experiment");
    fs::create_dir(&root).unwrap();
    assert!(init(&root).status.success());
    let config = root.join(".pueue-agent/config.toml");
    fs::write(&config, "keep me\n").unwrap();

    let output = init(&root);

    assert!(!output.status.success());
    assert_eq!(fs::read_to_string(config).unwrap(), "keep me\n");
}

#[test]
fn init_error_output_bounds_and_redacts_long_credential_like_paths() {
    let temp = TempDir::new().unwrap();
    let root = temp
        .path()
        .join(format!("prefix-{}", "p".repeat(160)))
        .join("AWS_SECRET_ACCESS_KEY=INIT_SECRET")
        .join(format!("tail-{}", "t".repeat(160)));
    fs::create_dir_all(&root).unwrap();
    assert!(init(&root).status.success());

    let output = init(&root);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.len() <= 241);
    assert!(stderr.contains("project is already initialized"));
    assert!(!stderr.contains("AWS_SECRET_ACCESS_KEY"));
    assert!(!stderr.contains("INIT_SECRET"));
    assert!(!stderr.contains(&"p".repeat(160)));
    assert!(!stderr.trim_end_matches('\n').chars().any(char::is_control));
}

#[test]
fn init_preserves_existing_durable_context_files() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("experiment");
    let state = root.join(".pueue-agent");
    fs::create_dir_all(&state).unwrap();
    fs::write(state.join("STATE.md"), "existing experiment history\n").unwrap();
    fs::write(state.join("instructions.md"), "existing project rules\n").unwrap();

    let output = init(&root);

    assert!(output.status.success());
    assert_eq!(
        fs::read_to_string(state.join("STATE.md")).unwrap(),
        "existing experiment history\n"
    );
    assert_eq!(
        fs::read_to_string(state.join("instructions.md")).unwrap(),
        "existing project rules\n"
    );
    assert!(state.join("config.toml").is_file());
}
