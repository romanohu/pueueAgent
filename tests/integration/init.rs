use std::fs;

use assert_cmd::Command;
use pueue_agent::{
    config,
    db::{Db, ProjectRepository},
    models::{AgentContextMode, NewProject},
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
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(&fake_pueue, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let command = |args: &[&str]| {
        let mut command = Command::cargo_bin("pueue-agent").unwrap();
        command
            .env("PUEUE_AGENT_STATE_DIR", &state_dir)
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
