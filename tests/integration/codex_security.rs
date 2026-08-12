use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use pueue_agent::{
    codex_command::{CodexArgvBuilder, CodexCapabilities},
    codex_session::resolve_latest_owned_session,
    config::{
        AgentConfig, AgentCodexConfig, AgentExecutionConfig, CodexReasoningEffort,
    },
    execution_policy::{
        AgentKind, ExecutableAnchor, ExecutableIdentity, NetworkMode,
        ProjectRootAnchor, PolicyViolationCode, ResolvedProjectExecutionPolicy,
    },
    models::AgentContextMode,
};
use tempfile::TempDir;

#[test]
fn forbidden_codex_security_args_fail_but_structured_model_reasoning_survive() {
    let harness = Harness::new();
    let mut config = config_with_args(vec!["exec", "{prompt}"]);
    config.codex.model = Some("model-a".to_owned());
    config.codex.reasoning_effort = Some(CodexReasoningEffort::High);

    let argv = harness
        .builder()
        .build(&config, "literal ; $(touch pwned)", &harness.private_tmp)
        .unwrap();
    assert!(contains_pair(&argv, "--sandbox", "workspace-write"));
    assert!(contains_pair(&argv, "--ask-for-approval", "never"));
    assert!(argv
        .iter()
        .any(|arg| arg == "model_reasoning_effort=\"high\""));
    assert!(argv.iter().any(|arg| arg == "literal ; $(touch pwned)"));

    for args in [
        vec!["exec", "-c", "x=y", "{prompt}"],
        vec!["exec", "--config=x=y", "{prompt}"],
        vec!["exec", "--profile=x", "{prompt}"],
        vec!["exec", "--dangerously-bypass-approvals-and-sandbox", "{prompt}"],
        vec!["exec", "--dangerously-bypass-hook-trust", "{prompt}"],
        vec!["exec", "--sandbox=danger-full-access", "{prompt}"],
        vec!["exec", "--add-dir=/tmp", "{prompt}"],
        vec!["exec", "-C", "/other", "{prompt}"],
        vec!["exec", "--cwd=/other", "{prompt}"],
        vec!["exec", "--network-access=enabled", "{prompt}"],
        vec!["exec", "{prompt}", "{prompt}"],
        vec!["exec", "{prompt}", "extra"],
    ] {
        let error = harness
            .builder()
            .build(&config_with_args(args), "p", &harness.private_tmp)
            .unwrap_err();
        assert_eq!(error.code, PolicyViolationCode::UnsafeCodexArgument);
    }
}

#[test]
fn missing_capability_fails_closed_and_auth_names_never_enter_filters() {
    let harness = Harness::new();
    let error = CodexArgvBuilder::new(
        harness.builder().policy().clone(),
        CodexCapabilities::none(),
    )
    .build(
        &config_with_args(vec!["{prompt}"]),
        "p",
        &harness.private_tmp,
    )
    .unwrap_err();
    assert_eq!(error.code, PolicyViolationCode::UnsafeCodexArgument);

    let argv = harness
        .builder()
        .build(&config_with_args(vec!["{prompt}"]), "p", &harness.private_tmp)
        .unwrap();
    let filters = argv
        .iter()
        .find_map(|arg| {
            arg.to_str()
                .filter(|value| value.starts_with("shell_environment_policy="))
        })
        .unwrap();
    assert!(!filters.contains("OPENAI_API_KEY"));
    assert!(!filters.contains("AGENT_ONLY"));
    assert!(filters.contains("SAFE_VAR=\"include\""));
}

#[test]
fn latest_without_owned_candidate_is_missing_and_duplicate_id_is_not_owned() {
    let harness = Harness::new();
    write_session(&harness.home, "foreign", &harness.other);
    let error = resolve_latest_owned_session(&harness.home, &harness.root).unwrap_err();
    assert_eq!(error.code, PolicyViolationCode::SessionMissing);

    write_session(&harness.home, "old", &harness.root);
    fs::copy(
        harness
            .home
            .join("sessions/rollout-019f9f30-5f31-7a40-8e28-bd95e1f6c537.jsonl"),
        harness
            .home
            .join("archived_sessions/duplicate-019f9f30-5f31-7a40-8e28-bd95e1f6c537.jsonl"),
    )
    .unwrap();
    let error = resolve_latest_owned_session(&harness.home, &harness.root).unwrap_err();
    assert_eq!(error.code, PolicyViolationCode::SessionNotOwned);
}

#[test]
fn latest_chooses_verified_same_project_and_never_last_or_fresh() {
    let harness = Harness::new();
    write_session(&harness.home, "old", &harness.root);
    thread::sleep(Duration::from_millis(10));
    write_session(&harness.home, "new", &harness.root);
    write_session(&harness.home, "foreign", &harness.other);

    let id = resolve_latest_owned_session(&harness.home, &harness.root).unwrap();
    assert_eq!(id, harness.id("new"));

    let config = AgentConfig {
        context: AgentContextMode::ResumeLatest,
        ..config_with_args(vec!["exec", "{prompt}"])
    };
    let argv = harness
        .builder()
        .build(&config, "p", &harness.private_tmp)
        .unwrap();
    assert!(!argv.iter().any(|arg| arg == "--last"));
    assert!(argv.iter().any(|arg| arg == id.as_str()));
}

fn contains_pair(args: &[OsString], first: &str, second: &str) -> bool {
    args.windows(2)
        .any(|pair| pair[0] == first && pair[1] == second)
}

fn config_with_args(args: Vec<&str>) -> AgentConfig {
    AgentConfig {
        program: "codex".to_owned(),
        args: args.into_iter().map(str::to_owned).collect(),
        timeout_minutes: 1,
        max_retries: 0,
        context: AgentContextMode::Fresh,
        execution: AgentExecutionConfig {
            network: NetworkMode::Enabled,
        },
        codex: AgentCodexConfig {
            model: None,
            reasoning_effort: None,
        },
    }
}

struct Harness {
    _temp: TempDir,
    root: PathBuf,
    other: PathBuf,
    home: PathBuf,
    private_tmp: PathBuf,
}

impl Harness {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("project");
        let other = temp.path().join("other");
        let home = temp.path().join("codex-home");
        let private_tmp = root.join(".pueue-agent/tmp/run");
        fs::create_dir_all(&private_tmp).unwrap();
        fs::create_dir_all(&other).unwrap();
        fs::create_dir_all(home.join("sessions")).unwrap();
        fs::create_dir_all(home.join("archived_sessions")).unwrap();
        Self {
            _temp: temp,
            root,
            other,
            home,
            private_tmp: fs::canonicalize(private_tmp).unwrap(),
        }
    }

    fn id(&self, label: &str) -> String {
        match label {
            "old" => "019f9f30-5f31-7a40-8e28-bd95e1f6c537".to_owned(),
            "new" => "019f9f30-a553-7e21-b108-16a5c341f728".to_owned(),
            "foreign" => "019f9f30-cf19-7d4a-ae2c-3a2ce6a7e534".to_owned(),
            _ => unreachable!(),
        }
    }

    fn builder(&self) -> CodexArgvBuilder {
        let root = fs::canonicalize(&self.root).unwrap_or_else(|_| self.root.clone());
        let identity = ExecutableIdentity {
            device: 1,
            inode: 1,
            owner: 1,
            mode: 0o700,
        };
        CodexArgvBuilder::new(
            ResolvedProjectExecutionPolicy {
                project_id: "project".to_owned(),
                root_anchor: ProjectRootAnchor {
                    canonical_path: root.clone(),
                    identity,
                    resolution_fingerprint: "test".to_owned(),
                },
                agent_anchor: ExecutableAnchor {
                    canonical_path: PathBuf::from("/usr/bin/codex"),
                    identity,
                    resolution_fingerprint: "test".to_owned(),
                },
                agent_kind: AgentKind::BuiltInCodex,
                network: NetworkMode::Enabled,
                agent_environment_allow: ["AGENT_ONLY".to_owned()].into_iter().collect(),
                task_environment_allow: ["SAFE_VAR".to_owned(), "OPENAI_API_KEY".to_owned()]
                    .into_iter()
                    .collect(),
                codex_home: self.home.clone(),
                private_temp_relative_root: PathBuf::from(".pueue-agent/tmp"),
            },
            CodexCapabilities::all(),
        )
    }
}

fn write_session(home: &Path, label: &str, cwd: &Path) {
    let id = match label {
        "old" => "019f9f30-5f31-7a40-8e28-bd95e1f6c537",
        "new" => "019f9f30-a553-7e21-b108-16a5c341f728",
        "foreign" => "019f9f30-cf19-7d4a-ae2c-3a2ce6a7e534",
        _ => unreachable!(),
    };
    let store = if label == "old" {
        home.join("sessions")
    } else {
        home.join("archived_sessions")
    };
    fs::write(
        store.join(format!("rollout-{id}.jsonl")),
        format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{id}\",\"cwd\":{cwd:?}}}}}\n",
            cwd = cwd.to_string_lossy()
        ),
    )
    .unwrap();
}
