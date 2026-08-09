use std::{
    fs,
    path::{Path, PathBuf},
};

use pueue_agent::{
    config::{self, PatternAction},
    models::AgentContextMode,
    paths, project,
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
log_tail_bytes = 16384
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
max_agent_runs = 10
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
    assert_eq!(config.agent.context, AgentContextMode::Fresh);
}

#[test]
fn codex_context_resume_requires_explicit_non_empty_session() {
    let config = valid_config().replace(
        "max_retries = 2\n",
        "max_retries = 2\n\n[agent.context]\nmode = \"resume\"\nsession_id = \"session-abc\"\n",
    );
    let config = load_config(config).unwrap();
    assert_eq!(
        config.agent.context,
        AgentContextMode::Resume {
            session_id: "session-abc".to_owned()
        }
    );

    let missing = valid_config().replace(
        "max_retries = 2\n",
        "max_retries = 2\n\n[agent.context]\nmode = \"resume\"\n",
    );
    assert!(load_config(missing)
        .unwrap_err()
        .to_string()
        .contains("agent.context.session_id"));
}

#[test]
fn resume_latest_rejects_session_id_and_non_codex_programs() {
    let latest = valid_config().replace(
        "max_retries = 2\n",
        "max_retries = 2\n\n[agent.context]\nmode = \"resume_latest\"\n",
    );
    assert_eq!(
        load_config(latest).unwrap().agent.context,
        AgentContextMode::ResumeLatest
    );

    let with_session = valid_config().replace(
        "max_retries = 2\n",
        "max_retries = 2\n\n[agent.context]\nmode = \"resume_latest\"\nsession_id = \"session-abc\"\n",
    );
    assert!(load_config(with_session)
        .unwrap_err()
        .to_string()
        .contains("agent.context.session_id"));

    let non_codex = valid_config()
        .replace("program = \"codex\"", "program = \"/bin/echo\"")
        .replace(
            "max_retries = 2\n",
            "max_retries = 2\n\n[agent.context]\nmode = \"resume_latest\"\n",
        );
    assert!(load_config(non_codex)
        .unwrap_err()
        .to_string()
        .contains("agent.context.mode"));
}

#[test]
fn valid_toml_preserves_detector_actions() {
    let config = load_config(valid_config()).unwrap();

    assert_eq!(config.check.patterns[0].action, PatternAction::Wake);
    assert_eq!(config.check.stall.action, PatternAction::Notify);
    assert_eq!(config.check.log_tail_bytes, 16_384);
    assert_eq!(config.guardrails.max_agent_runs, 10);
}

#[test]
fn omitted_future_limits_use_bounded_defaults() {
    let config = valid_config()
        .replace("log_tail_bytes = 16384\n", "")
        .replace("max_agent_runs = 10\n", "");

    let config = load_config(config).unwrap();

    assert_eq!(config.check.log_tail_bytes, 65_536);
    assert_eq!(config.guardrails.max_agent_runs, 100);
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
fn zero_agent_timeout_is_rejected() {
    let config = valid_config().replace("timeout_minutes = 60", "timeout_minutes = 0");

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("agent.timeout_minutes"));
}

#[test]
fn zero_deep_check_frequency_is_rejected() {
    let config = valid_config().replace("deep_check_every = 6", "deep_check_every = 0");

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("check.deep_check_every"));
}

#[test]
fn negative_deep_check_interval_is_rejected() {
    let config = valid_config().replace(
        "deep_check_interval_minutes = 0",
        "deep_check_interval_minutes = -1",
    );

    let error = load_config(config).unwrap_err();

    assert!(error
        .to_string()
        .contains("check.deep_check_interval_minutes"));
}

#[test]
fn negative_retry_count_is_rejected() {
    let config = valid_config().replace("max_retries = 2", "max_retries = -1");

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("max_retries"));
}

#[test]
fn non_positive_guardrail_limits_are_rejected() {
    for (configured, invalid, field) in [
        (
            "max_consecutive_failures = 3",
            "max_consecutive_failures = 0",
            "guardrails.max_consecutive_failures",
        ),
        (
            "max_experiments = 20",
            "max_experiments = 0",
            "guardrails.max_experiments",
        ),
        (
            "max_agent_runs = 10",
            "max_agent_runs = 0",
            "guardrails.max_agent_runs",
        ),
    ] {
        let error = load_config(valid_config().replace(configured, invalid)).unwrap_err();

        assert!(error.to_string().contains(field));
    }
}

#[test]
fn values_above_u32_are_rejected() {
    let config = valid_config().replace("log_tail_bytes = 16384", "log_tail_bytes = 4294967296");

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("check.log_tail_bytes"));
}

#[test]
fn invalid_pattern_action_is_rejected() {
    let config = valid_config().replace("action = \"wake\"", "action = \"restart\"");

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("check.patterns.action"));
}

#[test]
fn misspelled_pattern_key_is_rejected() {
    let config = valid_config().replace("confirm_matches = 3", "confirm_match = 3");

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("config.toml"));
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
fn invalid_stalled_action_is_rejected() {
    let config = valid_config().replace("action = \"notify\"", "action = \"restart\"");

    let error = load_config(config).unwrap_err();

    assert!(error.to_string().contains("check.stall.action"));
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

#[test]
fn absolute_xdg_state_home_is_used_for_the_database() {
    let state_home = Path::new("/var/state/pueue-agent-test");
    let home = Path::new("/home/tester");

    assert_eq!(
        paths::state_db_path_with(Some(state_home), Some(home)).unwrap(),
        state_home.join("pueue-agent/state.sqlite3")
    );
}

#[test]
fn unset_xdg_state_home_uses_the_platform_fallback() {
    let home = Path::new("/home/tester");

    assert_eq!(
        paths::state_db_path_with(None, Some(home)).unwrap(),
        expected_platform_state_path(home)
    );
}

#[test]
fn empty_xdg_state_home_uses_the_platform_fallback() {
    let home = Path::new("/home/tester");

    assert_eq!(
        paths::state_db_path_with(Some(Path::new("")), Some(home)).unwrap(),
        expected_platform_state_path(home)
    );
}

#[test]
fn relative_xdg_state_home_uses_the_platform_fallback() {
    let home = Path::new("/home/tester");

    assert_eq!(
        paths::state_db_path_with(Some(Path::new("relative/state")), Some(home)).unwrap(),
        expected_platform_state_path(home)
    );
}

#[cfg(target_os = "macos")]
fn expected_platform_state_path(home: &Path) -> PathBuf {
    home.join("Library/Application Support/pueue-agent/state.sqlite3")
}

#[cfg(not(target_os = "macos"))]
fn expected_platform_state_path(home: &Path) -> PathBuf {
    home.join(".local/state/pueue-agent/state.sqlite3")
}
