use std::fs;

use assert_cmd::Command;
use pueue_agent::{config, models::AgentContextMode};
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
