use std::{fs, path::Path};

use pueue_agent::{
    config::{self, PatternAction},
    project,
};
use tempfile::TempDir;

fn write_config(temp: &TempDir, body: &str) -> std::path::PathBuf {
    let path = temp.path().join("config.toml");
    fs::write(&path, body).unwrap();
    path
}

fn valid_config() -> String {
    r#"
project_id = "project-1234567890abcdef"
pueue_group = "pa-training-abcdef"

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

[[check.patterns]]
name = "nan-loss"
regex = "loss: NaN"
action = "wake"
confirm_matches = 3

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 3
max_experiments = 20
"#
    .to_owned()
}

fn load_config(body: String) -> Result<config::ProjectConfig, pueue_agent::AppError> {
    let temp = TempDir::new().unwrap();
    let path = write_config(&temp, &body);
    config::load(&path)
}

#[test]
fn command_is_an_argument_vector_not_a_shell_string() {
    let config = load_config(valid_config()).unwrap();

    assert_eq!(config.agent.program, "codex");
    assert_eq!(config.agent.args, vec!["exec", "{prompt}"]);
}

#[test]
fn valid_toml_preserves_detector_actions() {
    let config = load_config(valid_config()).unwrap();

    assert_eq!(config.check.patterns[0].action, PatternAction::Wake);
    assert_eq!(config.check.stall.action, PatternAction::Notify);
}

#[test]
fn zero_stall_interval_is_rejected() {
    let config = valid_config().replace("stall_minutes = 30", "stall_minutes = 0");

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("stall_minutes"));
}

#[test]
fn missing_agent_program_is_rejected() {
    let config = valid_config().replace("program = \"codex\"", "program = \"\"");

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("agent.program"));
}

#[test]
fn empty_pueue_group_is_rejected() {
    let config = valid_config().replace(
        "pueue_group = \"pa-training-abcdef\"",
        "pueue_group = \" \"",
    );

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("pueue_group"));
}

#[test]
fn non_positive_check_interval_is_rejected() {
    let config = valid_config().replace("interval_minutes = 10", "interval_minutes = 0");

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("interval_minutes"));
}

#[test]
fn negative_retry_count_is_rejected() {
    let config = valid_config().replace("max_retries = 2", "max_retries = -1");

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("max_retries"));
}

#[test]
fn invalid_pattern_action_is_rejected() {
    let config = valid_config().replace("action = \"wake\"", "action = \"restart\"");

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("check.patterns.action"));
}

#[test]
fn kill_pattern_requires_a_name() {
    let config = valid_config()
        .replace("name = \"nan-loss\"", "name = \"\"")
        .replace("action = \"wake\"", "action = \"kill\"");

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("check.patterns.name"));
}

#[test]
fn stalled_kill_requires_a_positive_kill_delay() {
    let config = valid_config().replace("action = \"notify\"", "action = \"kill\"");

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("kill_after_minutes"));
}

#[test]
fn finds_project_root_from_a_nested_directory() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("project");
    let nested = root.join("src/deep");
    fs::create_dir_all(root.join(".pueue-agent")).unwrap();
    fs::create_dir_all(&nested).unwrap();
    fs::write(root.join(".pueue-agent/config.toml"), valid_config()).unwrap();

    assert_eq!(
        project::find_root(&nested).unwrap(),
        root.canonicalize().unwrap()
    );
}

#[test]
fn default_group_uses_the_stable_project_id_suffix() {
    let root = Path::new("/work/training");

    assert_eq!(
        project::default_pueue_group(root, "project-1234567890abcdef").unwrap(),
        "pa-training-abcdef"
    );
}
