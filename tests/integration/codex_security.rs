use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use pueue_agent::{
    codex_command::{CodexArgvBuilder, CodexCapabilities},
    codex_session::{resolve_latest_owned_session, verify_project_ownership},
    config::{
        AgentConfig, AgentCodexConfig, AgentExecutionConfig, CodexReasoningEffort,
    },
    execution_policy::{
        load_or_create_policy, AgentKind, ExecutableAnchor, ExecutableIdentity, NetworkMode,
        PolicyLoadInput, PolicyViolationCode, ProjectRootAnchor, ResolvedProjectExecutionPolicy,
        StartupEnvironment,
    },
    decision_evidence::{
        MAX_ARTIFACT_HINT_DEPTH, MAX_ARTIFACT_HINT_FIELD_BYTES, MAX_ARTIFACT_HINTS,
    },
    environment::{collect_decision_artifact_hints, PrivateRunTemp, SanitizedEnvironment},
    models::AgentContextMode,
};
use tempfile::TempDir;

#[cfg(target_os = "linux")]
use std::{
    os::unix::fs::PermissionsExt,
    process::Command,
    sync::Arc,
};
#[cfg(target_os = "linux")]
use pueue_agent::{
    agent::{AgentRunner, AgentRunnerConfig},
    config::{self, ProjectConfig},
    db::{
        CampaignRepository, Db, DecisionRepository, EventRepository, ExperimentRepository,
        ProjectRepository, StartCampaignRequest,
    },
    decision_evidence::DecisionContextBundle,
    execution_policy::{load_existing_policy, CampaignLimits},
    models::{
        EventKind, ExperimentTerminalOutcome, NewEvent, NewProject, ProposalKind,
    },
    proposals::{self, ProposalInput},
    retry::RetryPolicy,
    state::ObjectiveSnapshot,
};
#[cfg(target_os = "linux")]
use rusqlite::params;
#[cfg(target_os = "linux")]
use serde_json::json;
#[cfg(target_os = "linux")]
use sha2::{Digest, Sha256};

#[path = "../support/execution_policy_fixture.rs"]
mod execution_policy_fixture;

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::time::Instant;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use pueue_agent::{
    execution_policy::{PolicyViolationDetail, TempUnsafeReason},
};

#[cfg(target_os = "linux")]
#[tokio::test]
async fn decision_runner_is_read_only_network_enabled_and_persists_output_before_cleanup() {
    let mut harness = DecisionRunnerHarness::new(OutputMutation::Valid);
    harness.run_to_terminal().await.unwrap();
    assert!(!harness.project_root.join("forbidden-write").exists());
    assert_eq!(harness.capture("network_access"), "true");
    assert!(!harness.captured_environment_names().iter().any(|name| {
        matches!(
            name.as_str(),
            "OPENAI_API_KEY" | "AWS_SECRET_ACCESS_KEY" | "SSH_AUTH_SOCK"
        )
    }));
    assert_eq!(harness.stored_decision_kind(), Some("proposal".to_owned()));
    assert_eq!(
        harness.cleanup_order(),
        vec!["decision_commit", "agent_terminal", "temp_cleanup"]
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn replaced_symlinked_or_weak_decision_output_is_rejected_without_project_mutation() {
    for mutation in [
        OutputMutation::Replaced,
        OutputMutation::Symlink,
        OutputMutation::Mode0644,
        OutputMutation::SecondHardLink,
        OutputMutation::Oversize,
    ] {
        let mut harness = DecisionRunnerHarness::new(mutation);
        harness.run_to_terminal().await.unwrap();
        assert_eq!(harness.stored_decision_kind(), None, "{mutation:?}");
        assert_eq!(
            harness.attempt_failure_code(),
            Some("decision_missing".to_owned()),
            "{mutation:?}"
        );
        assert!(!harness.project_root.join("forbidden-write").exists());
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn decision_runner_rejects_valid_output_from_an_unsuccessful_process() {
    let mut harness = DecisionRunnerHarness::new(OutputMutation::ExitNonzeroValid);
    harness.run_to_terminal().await.unwrap();

    assert_eq!(harness.stored_decision_kind(), None);
    assert_eq!(
        harness.attempt_failure_code(),
        Some("decision_missing".to_owned())
    );
    assert!(!harness.project_root.join("forbidden-write").exists());
}

#[test]
fn forbidden_codex_security_args_fail_but_structured_model_reasoning_survive() {
    let harness = Harness::new();
    let mut config = config_with_args(vec!["exec", "{prompt}"]);
    config.codex.model = Some("model-a".to_owned());
    config.codex.reasoning_effort = Some(CodexReasoningEffort::High);

    let argv = harness
        .builder()
        .build(&config, "literal ; $(touch pwned)")
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
            .build(&config_with_args(args), "p")
            .unwrap_err();
        assert_eq!(error.code, PolicyViolationCode::UnsafeCodexArgument);
    }
}

#[test]
fn codex_network_config_uses_exact_toml_booleans() {
    let harness = Harness::new();

    for (network, expected) in [
        (
            NetworkMode::Enabled,
            "sandbox_workspace_write.network_access=true",
        ),
        (
            NetworkMode::Disabled,
            "sandbox_workspace_write.network_access=false",
        ),
    ] {
        let mut policy = harness.builder().policy().clone();
        policy.network = network;
        let argv = CodexArgvBuilder::new(policy, CodexCapabilities::all())
            .build(&config_with_args(vec!["{prompt}"]), "p")
            .unwrap();

        assert_eq!(
            argv.iter().filter(|argument| *argument == expected).count(),
            1
        );
        assert!(!argv.iter().any(|argument| {
            argument == "sandbox_workspace_write.network_access=\"enabled\""
                || argument == "sandbox_workspace_write.network_access=\"disabled\""
        }));
    }
}

#[test]
fn task_env_is_default_deny_and_auth_never_inherits() {
    let harness = Harness::new();
    let startup = pueue_agent::execution_policy::StartupEnvironment::from_pairs([
        ("OPENAI_API_KEY", "secret"),
        ("DATASET_ROOT", "/data"),
        ("PATH", "/trusted/bin"),
    ]);
    let mut policy = harness.builder().policy().clone();
    policy.task_environment_allow = ["DATASET_ROOT", "OPENAI_API_KEY"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    let environment = SanitizedEnvironment::for_codex_task(&startup, &policy, 41).unwrap();
    assert_eq!(environment.get("DATASET_ROOT"), Some(std::ffi::OsStr::new("/data")));
    assert_eq!(environment.get("OPENAI_API_KEY"), None);
    for name in ["TMPDIR", "TMP", "TEMP"] {
        assert_eq!(environment.get(name), Some(std::ffi::OsStr::new("/dev/fd/11")));
    }
    assert!(!format!("{environment:?}").contains("secret"));
}

#[test]
fn fixture_policy_environment_keeps_network_enabled_without_unlisted_credentials() {
    let fixture = tempfile::tempdir().unwrap();
    let global = execution_policy_fixture::resolved_policy(fixture.path(), &[]);
    assert_eq!(global.default_network, NetworkMode::Enabled);

    let identity = ExecutableIdentity {
        device: 1,
        inode: 1,
        owner: 1,
        mode: 0o700,
    };
    let project_policy = ResolvedProjectExecutionPolicy {
        project_id: "fixture".to_owned(),
        root_anchor: ProjectRootAnchor {
            canonical_path: fixture.path().to_owned(),
            identity,
            resolution_fingerprint: "fixture".to_owned(),
        },
        agent_anchor: ExecutableAnchor {
            canonical_path: PathBuf::from("/usr/bin/codex"),
            identity,
            resolution_fingerprint: "fixture".to_owned(),
        },
        agent_kind: AgentKind::BuiltInCodex,
        network: global.default_network,
        agent_environment_allow: Default::default(),
        task_environment_allow: Default::default(),
        codex_home: global.codex_home.clone(),
        trusted_path: global.trusted_path.clone(),
        private_temp_relative_root: PathBuf::from(".pueue-agent/tmp"),
    };

    for environment in [
        SanitizedEnvironment::for_codex_agent(&global.startup_environment, &project_policy, 41)
            .unwrap(),
        SanitizedEnvironment::for_codex_task(&global.startup_environment, &project_policy, 41)
            .unwrap(),
    ] {
        for name in ["AWS_SECRET_ACCESS_KEY", "WANDB_API_KEY", "SSH_AUTH_SOCK"] {
            assert_eq!(environment.get(name), None, "{name}");
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn decision_artifact_hints_do_not_follow_symlinks_or_escape_the_pinned_root() {
    let fixture = tempfile::tempdir().unwrap();
    let project_root = fixture.path().join("project");
    let external_root = fixture.path().join("external");
    fs::create_dir_all(project_root.join("metrics")).unwrap();
    fs::create_dir_all(&external_root).unwrap();
    fs::write(
        project_root.join("metrics/epoch.json"),
        "{\"loss\":1.0}\n",
    )
    .unwrap();
    fs::write(external_root.join("secret.txt"), "EVIDENCE_SECRET").unwrap();
    std::os::unix::fs::symlink(
        external_root.join("secret.txt"),
        project_root.join("metrics/external"),
    )
    .unwrap();
    let project_root = fs::canonicalize(project_root).unwrap();
    let anchor = ProjectRootAnchor::resolve(&project_root).unwrap();

    let hints = collect_decision_artifact_hints(
        &anchor,
        MAX_ARTIFACT_HINTS,
        MAX_ARTIFACT_HINT_DEPTH,
        MAX_ARTIFACT_HINT_FIELD_BYTES,
    )
    .unwrap();
    let json = serde_json::to_string(&serde_json::json!({ "artifact_hints": hints })).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    let hints = value["artifact_hints"].as_array().unwrap();
    assert!(hints
        .iter()
        .any(|hint| hint["path"] == "metrics/epoch.json"));
    assert!(!hints
        .iter()
        .any(|hint| hint["path"] == "metrics/external"));
    assert!(!json.contains("EVIDENCE_SECRET"));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn decision_artifact_hints_are_deterministic_and_enforce_count_and_depth_bounds() {
    let fixture = tempfile::tempdir().unwrap();
    let project_root = fixture.path().join("project");
    fs::create_dir_all(project_root.join("one/two/three/four/five")).unwrap();
    fs::write(
        project_root.join("one/two/three/four/visible.json"),
        "visible",
    )
    .unwrap();
    fs::write(
        project_root.join("one/two/three/four/five/too-deep.json"),
        "too deep",
    )
    .unwrap();
    for index in 0..70 {
        fs::write(project_root.join(format!("metric-{index:02}.json")), "x").unwrap();
    }
    let project_root = fs::canonicalize(project_root).unwrap();
    let anchor = ProjectRootAnchor::resolve(&project_root).unwrap();

    let first = collect_decision_artifact_hints(
        &anchor,
        MAX_ARTIFACT_HINTS,
        MAX_ARTIFACT_HINT_DEPTH,
        MAX_ARTIFACT_HINT_FIELD_BYTES,
    )
    .unwrap();
    let second = collect_decision_artifact_hints(
        &anchor,
        MAX_ARTIFACT_HINTS,
        MAX_ARTIFACT_HINT_DEPTH,
        MAX_ARTIFACT_HINT_FIELD_BYTES,
    )
    .unwrap();

    assert_eq!(first, second);
    assert_eq!(first.len(), MAX_ARTIFACT_HINTS);
    assert!(first.iter().all(|hint| hint.path.split('/').count() <= 4));
    assert!(!first.iter().any(|hint| hint.path.contains("too-deep")));
}

#[test]
fn proxy_and_control_names_are_scoped_or_denied() {
    let harness = Harness::new();
    let startup = pueue_agent::execution_policy::StartupEnvironment::from_pairs([
        ("HTTP_PROXY", "http://user:password@example.invalid"),
        ("HTTPS_PROXY", "https://proxy.invalid"),
        ("CODEX_HOME", "/ambient/codex"),
        ("GOOGLE_APPLICATION_CREDENTIALS", "/secret/google.json"),
        ("DOCKER_AUTH_CONFIG", "{\"auths\":{}}"),
        ("SSH_AUTH_SOCK", "/tmp/agent.sock"),
        ("GIT_ASKPASS", "/tmp/askpass"),
        ("DATASET_ROOT", "/data"),
    ]);
    let mut policy = harness.builder().policy().clone();
    policy.agent_environment_allow = [
        "HTTP_PROXY",
        "CODEX_HOME",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "DOCKER_AUTH_CONFIG",
        "SSH_AUTH_SOCK",
        "GIT_ASKPASS",
        "DATASET_ROOT",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    policy.task_environment_allow = policy.agent_environment_allow.clone();

    let agent = SanitizedEnvironment::for_codex_agent(&startup, &policy, 41).unwrap();
    assert_eq!(agent.get("HTTP_PROXY"), Some(std::ffi::OsStr::new("http://user:password@example.invalid")));
    assert_eq!(agent.get("CODEX_HOME"), Some(policy.codex_home.as_os_str()));
    assert_eq!(agent.get("GOOGLE_APPLICATION_CREDENTIALS"), None);
    assert_eq!(agent.get("DOCKER_AUTH_CONFIG"), None);
    assert!(!format!("{agent:?}").contains("password"));

    for environment in [
        SanitizedEnvironment::for_codex_task(&startup, &policy, 41).unwrap(),
        SanitizedEnvironment::for_custom_agent(&startup, &policy, 41).unwrap(),
    ] {
        for name in [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "CODEX_HOME",
            "GOOGLE_APPLICATION_CREDENTIALS",
            "DOCKER_AUTH_CONFIG",
            "SSH_AUTH_SOCK",
            "GIT_ASKPASS",
        ] {
            assert_eq!(environment.get(name), None, "{name}");
        }
        assert!(!format!("{environment:?}").contains("password"));
    }
}

#[test]
fn codex_agent_env_allows_only_known_startup_auth_names() {
    let harness = Harness::new();
    let startup = pueue_agent::execution_policy::StartupEnvironment::from_pairs([
        ("OPENAI_API_KEY", "secret"),
        ("CODEX_AUTH_TOKEN", "token"),
        ("UNRELATED_SECRET", "must-not-appear"),
    ]);
    let environment = SanitizedEnvironment::for_codex_agent(
        &startup,
        harness.builder().policy(),
        41,
    )
    .unwrap();
    assert_eq!(environment.get("OPENAI_API_KEY"), Some(std::ffi::OsStr::new("secret")));
    assert_eq!(environment.get("CODEX_AUTH_TOKEN"), Some(std::ffi::OsStr::new("token")));
    assert_eq!(environment.get("UNRELATED_SECRET"), None);
    assert!(!format!("{environment:?}").contains("secret"));
}

#[cfg(unix)]
#[test]
fn startup_environment_preserves_non_utf8_names_and_values() {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    let startup = pueue_agent::execution_policy::StartupEnvironment::from_pairs([(
        OsString::from_vec(vec![b'N', b'O', b'N', b'U', b'T', b'F', b'8', 0x80]),
        OsString::from("SENSITIVE_VALUE"),
    )]);
    let name = startup.names().next().unwrap();
    assert_eq!(name.as_bytes(), b"NONUTF8\x80");
    assert!(!format!("{startup:?}").contains("SENSITIVE_VALUE"));
}

#[test]
fn sanitized_environment_apply_clears_before_setting_explicit_values() {
    use pueue_agent::environment::EnvironmentCommand;
    use std::collections::BTreeMap;

    struct Probe {
        cleared: bool,
        values: BTreeMap<OsString, OsString>,
    }
    impl EnvironmentCommand for Probe {
        fn environment_clear(&mut self) {
            self.cleared = true;
            self.values.clear();
        }

        fn environment_set(&mut self, name: &std::ffi::OsStr, value: &std::ffi::OsStr) {
            self.values.insert(name.to_os_string(), value.to_os_string());
        }
    }

    let startup = pueue_agent::execution_policy::StartupEnvironment::from_pairs([
        ("HOME", "/home/service"),
        ("DATASET_ROOT", "/data"),
    ]);
    let harness = Harness::new();
    let environment = SanitizedEnvironment::for_codex_task(
        &startup,
        &harness.builder().policy().clone(),
        41,
    )
    .unwrap();
    let mut probe = Probe {
        cleared: false,
        values: [(OsString::from("AMBIENT"), OsString::from("discard"))]
            .into_iter()
            .collect(),
    };
    environment.apply(&mut probe);
    assert!(probe.cleared);
    assert_eq!(probe.values.get(std::ffi::OsStr::new("AMBIENT")), None);
    assert_eq!(
        probe.values.get(std::ffi::OsStr::new("HOME")),
        Some(&OsString::from("/home/service"))
    );
}

#[test]
fn custom_env_hard_denies_auth_names() {
    let harness = Harness::new();
    let startup = pueue_agent::execution_policy::StartupEnvironment::from_pairs([
        ("OPENAI_API_KEY", "secret"),
        ("CODEX_AUTH_TOKEN", "secret-token"),
        ("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "/secret/gcloud.json"),
        ("AZURE_STORAGE_KEY", "secret-storage-key"),
        ("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountKey=secret"),
        ("STORAGE_ACCESS_KEY", "secret-storage-access-key"),
        ("SAFE_VALUE", "safe"),
        ("HTTP_PROXY", "http://proxy.invalid"),
    ]);
    let mut policy = harness.builder().policy().clone();
    policy.agent_environment_allow = [
        "OPENAI_API_KEY",
        "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE",
        "AZURE_STORAGE_KEY",
        "AZURE_STORAGE_CONNECTION_STRING",
        "STORAGE_ACCESS_KEY",
        "SAFE_VALUE",
    ]
        .into_iter()
        .map(str::to_owned)
        .collect();
    let custom = SanitizedEnvironment::for_custom_agent(&startup, &policy, 41).unwrap();
    assert_eq!(custom.get("OPENAI_API_KEY"), None);
    assert_eq!(custom.get("CODEX_AUTH_TOKEN"), None);
    assert_eq!(custom.get("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"), None);
    assert_eq!(custom.get("AZURE_STORAGE_KEY"), None);
    assert_eq!(custom.get("AZURE_STORAGE_CONNECTION_STRING"), None);
    assert_eq!(custom.get("STORAGE_ACCESS_KEY"), None);
    assert_eq!(custom.get("SAFE_VALUE"), Some(std::ffi::OsStr::new("safe")));

}

#[cfg(unix)]
#[test]
fn pueue_environment_is_fixed_baseline_without_proxy_auth_or_task_values() {
    use std::os::unix::fs::PermissionsExt;

    let harness = Harness::new();
    let base = fs::canonicalize(harness._temp.path()).unwrap();
    let state_dir = base.join("state");
    let trusted_bin = base.join("trusted-bin");
    let codex_home = base.join("pueue-codex-home");
    fs::create_dir(&state_dir).unwrap();
    fs::create_dir(&trusted_bin).unwrap();
    fs::create_dir(&codex_home).unwrap();
    let pueue = trusted_bin.join("pueue");
    let codex = trusted_bin.join("codex");
    let launcher = trusted_bin.join("launcher");
    for executable in [&pueue, &codex, &launcher] {
        fs::write(executable, b"fixture").unwrap();
        fs::set_permissions(executable, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let pueue_config = base.join("pueue.yml");
    fs::write(&pueue_config, b"fixture: true\n").unwrap();
    fs::set_permissions(&pueue_config, fs::Permissions::from_mode(0o600)).unwrap();
    for directory in [&state_dir, &trusted_bin, &codex_home] {
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::set_permissions(&harness.root, fs::Permissions::from_mode(0o700)).unwrap();

    let startup = StartupEnvironment::from_pairs([
        ("HOME", "/service/home"),
        ("PATH", "/ambient/bin"),
        ("TMPDIR", "/service/tmp"),
        ("TMP", "/service/tmp"),
        ("TEMP", "/service/tmp"),
        ("HTTP_PROXY", "http://proxy.invalid"),
        ("SSL_CERT_FILE", "/secret/cert.pem"),
        ("OPENAI_API_KEY", "secret"),
        ("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "/secret/gcloud.json"),
        ("AZURE_STORAGE_KEY", "secret-storage-key"),
        ("TASK_ONLY", "task-value"),
    ]);
    let global = load_or_create_policy(&PolicyLoadInput {
        state_dir,
        project_roots: vec![harness.root.clone()],
        inherited_path: trusted_bin.clone().into_os_string(),
        startup_environment: startup,
        codex_home,
        pueue_config,
        launcher_path: launcher,
    })
    .unwrap();
    let environment = SanitizedEnvironment::for_pueue(&global).unwrap();
    assert_eq!(environment.get("PATH"), Some(trusted_bin.as_os_str()));
    assert_eq!(environment.get("HOME"), Some(std::ffi::OsStr::new("/service/home")));
    assert_eq!(environment.get("LANG"), Some(std::ffi::OsStr::new("C")));
    for name in [
        "HTTP_PROXY",
        "SSL_CERT_FILE",
        "OPENAI_API_KEY",
        "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE",
        "AZURE_STORAGE_KEY",
        "TASK_ONLY",
        "PUEUE_AGENT_RUN_ID",
        "PUEUE_AGENT_PROJECT_ID",
    ] {
        assert_eq!(environment.get(name), None, "{name}");
    }
}

#[test]
fn codex_shell_filters_always_have_a_nonsecret_baseline() {
    let harness = Harness::new();
    let mut policy = harness.builder().policy().clone();
    policy.task_environment_allow.clear();
    let argv = CodexArgvBuilder::new(policy, CodexCapabilities::all())
        .build(&config_with_args(vec!["{prompt}"]), "p")
        .unwrap();
    let filters = argv
        .iter()
        .find_map(|arg| {
            arg.to_str()
                .filter(|value| value.starts_with("shell_environment_policy="))
        })
        .unwrap();
    assert!(filters.contains("PATH=\"include\""));
    assert!(filters.contains("TMPDIR=\"include\""));
    assert!(!filters.contains("OPENAI_API_KEY"));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_temp_is_0700_and_retained_after_cleanup_and_drop() {
    use std::os::unix::fs::PermissionsExt;

    let harness = Harness::new();
    let anchor = ProjectRootAnchor::resolve(&harness.root).unwrap();
    let root = anchor.verify_identity().unwrap();
    let mut temp = PrivateRunTemp::create(&root, 41).unwrap();
    assert_eq!(fs::metadata(temp.path()).unwrap().permissions().mode() & 0o777, 0o700);
    let path = temp.path().to_owned();
    fs::write(path.join("original-generation"), b"original").unwrap();
    let error = temp.cleanup().unwrap_err();
    assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
    assert!(path.exists());
    assert_eq!(fs::read(path.join("original-generation")).unwrap(), b"original");
    drop(temp);
    assert!(path.exists());
    assert_eq!(fs::read(path.join("original-generation")).unwrap(), b"original");
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_temp_rejects_collision_and_unsafe_fixed_parents() {
    use std::os::unix::fs::PermissionsExt;

    let harness = Harness::new();
    let anchor = ProjectRootAnchor::resolve(&harness.root).unwrap();
    let root = anchor.verify_identity().unwrap();
    let first = PrivateRunTemp::create(&root, 42).unwrap();
    assert!(PrivateRunTemp::create(&root, 42).is_err());
    drop(first);

    let weak_harness = Harness::new();
    fs::set_permissions(
        weak_harness.root.join(".pueue-agent"),
        fs::Permissions::from_mode(0o775),
    )
    .unwrap();
    let weak_anchor = ProjectRootAnchor::resolve(&weak_harness.root).unwrap();
    let weak_root = weak_anchor.verify_identity().unwrap();
    assert!(PrivateRunTemp::create(&weak_root, 42).is_err());
    assert!(PrivateRunTemp::inspect_capacity(&weak_root).is_err());

    let symlink_harness = Harness::new();
    let outside = symlink_harness._temp.path().join("outside");
    fs::create_dir(&outside).unwrap();
    fs::remove_dir_all(symlink_harness.root.join(".pueue-agent")).unwrap();
    std::os::unix::fs::symlink(&outside, symlink_harness.root.join(".pueue-agent")).unwrap();
    let symlink_anchor = ProjectRootAnchor::resolve(&symlink_harness.root).unwrap();
    let symlink_root = symlink_anchor.verify_identity().unwrap();
    assert!(PrivateRunTemp::create(&symlink_root, 42).is_err());
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_temp_does_not_follow_nested_symlinks_or_delete_replacement() {
    use std::os::unix::fs::symlink;

    let harness = Harness::new();
    let anchor = ProjectRootAnchor::resolve(&harness.root).unwrap();
    let root = anchor.verify_identity().unwrap();
    let temp = PrivateRunTemp::create(&root, 43).unwrap();
    let outside = harness._temp.path().join("outside-file");
    fs::write(&outside, b"keep").unwrap();
    symlink(&outside, temp.path().join("link")).unwrap();
    let path = temp.path().to_owned();
    drop(temp);
    assert!(outside.exists());
    assert!(path.exists());
    assert!(path.join("link").exists());

    let replacement = PrivateRunTemp::create(&root, 44).unwrap();
    let replacement_path = replacement.path().to_owned();
    fs::write(replacement_path.join("original"), b"original").unwrap();
    let moved = harness._temp.path().join("moved-generation");
    fs::rename(&replacement_path, &moved).unwrap();
    fs::create_dir(&replacement_path).unwrap();
    fs::write(replacement_path.join("keep"), b"replacement").unwrap();
    drop(replacement);
    assert!(replacement_path.exists());
    assert_eq!(fs::read(replacement_path.join("keep")).unwrap(), b"replacement");
    assert!(moved.exists());
    assert_eq!(fs::read(moved.join("original")).unwrap(), b"original");
    assert!(!moved.join("keep").exists());
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_temp_revalidation_rejects_replaced_generation_before_authorization() {
    use std::os::unix::fs::PermissionsExt;

    let harness = Harness::new();
    let anchor = ProjectRootAnchor::resolve(&harness.root).unwrap();
    let root = anchor.verify_identity().unwrap();
    let temp = PrivateRunTemp::create(&root, 46).unwrap();
    temp.revalidate_current().unwrap();
    let path = temp.path().to_owned();
    fs::rename(&path, harness._temp.path().join("retired-run-46")).unwrap();
    fs::create_dir(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();

    let error = temp.revalidate_current().unwrap_err();
    assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_temp_retains_tree_without_traversal() {
    let harness = Harness::new();
    let anchor = ProjectRootAnchor::resolve(&harness.root).unwrap();
    let root = anchor.verify_identity().unwrap();
    let temp = PrivateRunTemp::create(&root, 45).unwrap();
    for index in 0..4097 {
        fs::write(temp.path().join(format!("entry-{index}")), b"x").unwrap();
    }
    let path = temp.path().to_owned();
    drop(temp);
    assert!(path.exists());
    assert_eq!(fs::read(path.join("entry-0")).unwrap(), b"x");
    assert_eq!(fs::read(path.join("entry-4096")).unwrap(), b"x");
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_temp_cleanup_removes_bounded_contents_but_retains_run_directory() {
    use std::os::unix::fs::PermissionsExt;
    let harness = Harness::new();
    let root = ProjectRootAnchor::resolve(&harness.root)
        .unwrap()
        .verify_identity()
        .unwrap();
    let mut temp = PrivateRunTemp::create(&root, 101).unwrap();
    fs::create_dir(temp.path().join("nested")).unwrap();
    fs::set_permissions(temp.path().join("nested"), fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(temp.path().join("nested/file"), b"payload").unwrap();
    fs::write(temp.path().join("top"), b"top").unwrap();

    let report = temp.cleanup_contents_before(None).unwrap();

    assert_eq!(report.entries_removed, 3);
    assert!(report.allocated_bytes_reclaimed >= 2 * 512);
    assert!(temp.path().is_dir());
    assert!(fs::read_dir(temp.path()).unwrap().next().is_none());
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_temp_cleanup_does_not_follow_symlink_fifo_or_socket_targets() {
    use std::os::unix::fs::symlink;
    use std::os::unix::net::UnixListener;

    let harness = Harness::new();
    let root = ProjectRootAnchor::resolve(&harness.root)
        .unwrap()
        .verify_identity()
        .unwrap();
    let mut temp = PrivateRunTemp::create(&root, 102).unwrap();
    let outside = harness._temp.path().join("outside");
    fs::write(&outside, b"keep").unwrap();
    symlink(&outside, temp.path().join("link")).unwrap();
    let fifo = temp.path().join("fifo");
    assert_eq!(unsafe { libc::mkfifo(std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap().as_ptr(), 0o600) }, 0);
    let socket = temp.path().join("socket");
    let listener = UnixListener::bind(&socket).unwrap();
    let report = temp.cleanup_contents_before(None).unwrap();

    assert_eq!(report.entries_removed, 3);
    assert_eq!(fs::read(&outside).unwrap(), b"keep");
    assert!(temp.path().is_dir());
    assert!(fs::read_dir(temp.path()).unwrap().next().is_none());
    drop(listener);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_temp_cleanup_rejects_depth_entry_and_allocated_byte_overflow_before_mutation() {
    use std::os::unix::fs::PermissionsExt;
    let harness = Harness::new();
    let root = ProjectRootAnchor::resolve(&harness.root)
        .unwrap()
        .verify_identity()
        .unwrap();

    let mut depth = PrivateRunTemp::create(&root, 103).unwrap();
    let mut current = depth.path().to_owned();
    for index in 0..=pueue_agent::environment::MAX_PRIVATE_TEMP_CLEANUP_DEPTH {
        current.push(format!("d{index}"));
        fs::create_dir(&current).unwrap();
        fs::set_permissions(&current, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let before = fs::read_dir(depth.path()).unwrap().count();
    let error = depth.cleanup_contents_before(None).unwrap_err();
    assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::DepthLimit));
    assert_eq!(fs::read_dir(depth.path()).unwrap().count(), before);

    let mut entries = PrivateRunTemp::create(&root, 104).unwrap();
    for index in 0..=pueue_agent::environment::MAX_PRIVATE_TEMP_CLEANUP_ENTRIES {
        fs::write(entries.path().join(format!("entry-{index}")), b"x").unwrap();
    }
    let before = fs::read_dir(entries.path()).unwrap().count();
    let error = entries.cleanup_contents_before(None).unwrap_err();
    assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::EntryLimit));
    assert_eq!(fs::read_dir(entries.path()).unwrap().count(), before);

}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_temp_cleanup_expired_deadline_does_not_mutate() {
    let harness = Harness::new();
    let root = ProjectRootAnchor::resolve(&harness.root)
        .unwrap()
        .verify_identity()
        .unwrap();
    let mut temp = PrivateRunTemp::create(&root, 105).unwrap();
    fs::write(temp.path().join("small"), b"small").unwrap();
    let error = temp
        .cleanup_contents_before(Some(Instant::now() - std::time::Duration::from_secs(1)))
        .unwrap_err();
    assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
    assert_eq!(fs::read(temp.path().join("small")).unwrap(), b"small");
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_temp_cleanup_never_touches_a_replacement_run_generation() {
    let harness = Harness::new();
    let root = ProjectRootAnchor::resolve(&harness.root)
        .unwrap()
        .verify_identity()
        .unwrap();
    let mut temp = PrivateRunTemp::create(&root, 106).unwrap();
    let visible = temp.path().to_owned();
    fs::write(visible.join("original"), b"original").unwrap();
    let moved = harness._temp.path().join("retired-generation");
    fs::rename(&visible, &moved).unwrap();
    fs::create_dir(&visible).unwrap();
    fs::write(visible.join("replacement"), b"replacement").unwrap();

    let report = temp.cleanup_contents_before(None).unwrap();

    assert_eq!(report.entries_removed, 1);
    assert!(!moved.join("original").exists());
    assert_eq!(fs::read(visible.join("replacement")).unwrap(), b"replacement");
    assert!(visible.is_dir());
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_temp_inventory_allows_empty_generations_and_rejects_nonempty_or_unsafe_generations() {
    use std::os::unix::fs::PermissionsExt;

    let harness = Harness::new();
    let tmp = harness.root.join(".pueue-agent/tmp");
    fs::remove_dir_all(harness.private_run_temp.path()).unwrap();
    for run_id in ["1", "2", "3"] {
        fs::create_dir(tmp.join(run_id)).unwrap();
        fs::set_permissions(tmp.join(run_id), fs::Permissions::from_mode(0o700)).unwrap();
    }
    let root = ProjectRootAnchor::resolve(&harness.root)
        .unwrap()
        .verify_identity()
        .unwrap();
    let report = PrivateRunTemp::inspect_capacity(&root).unwrap();
    assert_eq!(report.generations, 3);
    assert_eq!(report.retained_nonempty_generations, 0);
    assert_eq!(report.retained_allocated_bytes, 0);

    fs::write(tmp.join("2/preserved"), b"preserved").unwrap();
    let error = PrivateRunTemp::inspect_capacity(&root).unwrap_err();
    assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::InvalidEntry));
    assert_eq!(error.stage, pueue_agent::execution_policy::PolicyViolationStage::PreBinding);
    assert_eq!(fs::read(tmp.join("2/preserved")).unwrap(), b"preserved");

    fs::remove_file(tmp.join("2/preserved")).unwrap();
    fs::remove_dir(tmp.join("3")).unwrap();
    std::os::unix::fs::symlink(tmp.join("1"), tmp.join("3")).unwrap();
    let error = PrivateRunTemp::inspect_capacity(&root).unwrap_err();
    assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::InvalidEntry));
    assert_eq!(error.stage, pueue_agent::execution_policy::PolicyViolationStage::PreBinding);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_temp_inventory_rejects_weak_fixed_components_at_prebinding() {
    use std::os::unix::fs::PermissionsExt;

    let harness = Harness::new();
    let anchor = ProjectRootAnchor::resolve(&harness.root).unwrap();
    let root = anchor.verify_identity().unwrap();
    fs::set_permissions(
        harness.root.join(".pueue-agent/tmp"),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();

    let error = PrivateRunTemp::inspect_capacity(&root).unwrap_err();

    assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
    assert_eq!(error.stage, pueue_agent::execution_policy::PolicyViolationStage::PreBinding);
    assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::InvalidEntry));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_temp_inventory_rejects_generation_overflow_without_mutation() {
    use std::os::unix::fs::PermissionsExt;

    let harness = Harness::new();
    let tmp = harness.root.join(".pueue-agent/tmp");
    fs::remove_dir_all(harness.private_run_temp.path()).unwrap();
    for run_id in 1..=pueue_agent::environment::MAX_PRIVATE_TEMP_GENERATIONS + 1 {
        let path = tmp.join(run_id.to_string());
        fs::create_dir(&path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let root = ProjectRootAnchor::resolve(&harness.root)
        .unwrap()
        .verify_identity()
        .unwrap();

    let error = PrivateRunTemp::inspect_capacity(&root).unwrap_err();

    assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::GenerationLimit));
    assert_eq!(fs::read_dir(tmp).unwrap().count(), pueue_agent::environment::MAX_PRIVATE_TEMP_GENERATIONS + 1);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_temp_inventory_rejects_noncanonical_decimal_generation_names() {
    use std::os::unix::fs::PermissionsExt;

    for name in ["+1", "+01"] {
        let harness = Harness::new();
        let tmp = harness.root.join(".pueue-agent/tmp");
        fs::remove_dir_all(harness.private_run_temp.path()).unwrap();
        let generation = tmp.join(name);
        fs::create_dir(&generation).unwrap();
        fs::set_permissions(&generation, fs::Permissions::from_mode(0o700)).unwrap();
        let root = ProjectRootAnchor::resolve(&harness.root)
            .unwrap()
            .verify_identity()
            .unwrap();

        let error = PrivateRunTemp::inspect_capacity(&root).unwrap_err();

        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::InvalidEntry),
            "name={name}"
        );
        assert!(generation.is_dir());
    }
}

#[test]
fn prompts_are_literal_after_separator_for_fresh_and_resume() {
    let harness = Harness::new();
    write_session(&harness.home, "old", &harness.root);
    let prompts = [
        "-",
        "--dangerously-bypass-approvals-and-sandbox",
        "resume",
        "review",
        "help",
        "",
        "こんにちは — literal prompt",
    ];

    for prompt in prompts {
        let fresh = harness
            .builder()
            .build(&config_with_args(vec!["exec", "{prompt}"]), prompt)
            .unwrap();
        assert_prompt_after_separator(&fresh, prompt);

        let mut resume_config = config_with_args(vec!["{prompt}"]);
        resume_config.context = AgentContextMode::Resume {
            session_id: harness.id("old"),
        };
        let resumed = harness
            .builder()
            .build(&resume_config, prompt)
            .unwrap();
        assert_prompt_after_separator(&resumed, prompt);
    }
}

fn assert_prompt_after_separator(argv: &[OsString], prompt: &str) {
    let prompt = OsString::from(prompt);
    assert_eq!(argv.last(), Some(&prompt));
    assert_eq!(argv.get(argv.len().saturating_sub(2)), Some(&OsString::from("--")));
}

#[test]
fn missing_capability_fails_closed_and_auth_names_never_enter_filters() {
    let harness = Harness::new();
    let error = CodexArgvBuilder::new(
        harness.builder().policy().clone(),
        CodexCapabilities::none(),
    )
    .build(&config_with_args(vec!["{prompt}"]), "p")
    .unwrap_err();
    assert_eq!(error.code, PolicyViolationCode::UnsafeCodexArgument);

    let argv = harness
        .builder()
        .build(&config_with_args(vec!["{prompt}"]), "p")
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
fn latest_rejects_duplicate_id_regardless_of_ownership_or_store_order() {
    for (case, sessions_owned, archived_sessions_owned) in [
        ("owned + owned", true, true),
        ("foreign + foreign", false, false),
        ("owned + foreign", true, false),
        ("foreign + owned", false, true),
    ] {
        let harness = Harness::new();
        let id = harness.id("old");
        let sessions_cwd = if sessions_owned {
            &harness.root
        } else {
            &harness.other
        };
        let archived_sessions_cwd = if archived_sessions_owned {
            &harness.root
        } else {
            &harness.other
        };
        write_session_with_id(&harness.home, "sessions", &id, sessions_cwd);
        write_session_with_id(
            &harness.home,
            "archived_sessions",
            &id,
            archived_sessions_cwd,
        );

        let error = resolve_latest_owned_session(&harness.home, &harness.root).unwrap_err();
        assert_eq!(error.code, PolicyViolationCode::SessionNotOwned, "{case}");
    }
}

#[cfg(unix)]
#[test]
fn latest_rejects_symlinked_session_store() {
    use std::os::unix::fs::symlink;

    let harness = Harness::new();
    let outside_store = harness._temp.path().join("outside-sessions");
    fs::create_dir_all(&outside_store).unwrap();
    fs::write(
        outside_store.join(format!(
            "rollout-{}.jsonl",
            harness.id("old")
        )),
        format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{}\",\"cwd\":{:?}}}}}\n",
            harness.id("old"),
            harness.root.to_string_lossy()
        ),
    )
    .unwrap();
    fs::remove_dir(harness.home.join("sessions")).unwrap();
    symlink(&outside_store, harness.home.join("sessions")).unwrap();

    let error = resolve_latest_owned_session(&harness.home, &harness.root).unwrap_err();
    assert_eq!(error.code, PolicyViolationCode::SessionNotOwned);
}

#[cfg(unix)]
#[test]
fn explicit_resume_rejects_symlinked_session_store() {
    use std::os::unix::fs::symlink;

    let harness = Harness::new();
    let outside_store = harness._temp.path().join("outside-sessions");
    fs::create_dir_all(&outside_store).unwrap();
    let id = harness.id("old");
    fs::write(
        outside_store.join(format!("rollout-{id}.jsonl")),
        format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{id}\",\"cwd\":{:?}}}}}\n",
            harness.root.to_string_lossy()
        ),
    )
    .unwrap();
    fs::remove_dir(harness.home.join("sessions")).unwrap();
    symlink(&outside_store, harness.home.join("sessions")).unwrap();

    assert!(matches!(
        verify_project_ownership(&harness.home, &harness.root, &id),
        Err(pueue_agent::AppError::CodexSessionMetadata { .. })
    ));
}

#[cfg(unix)]
#[test]
fn explicit_and_latest_resume_reject_symlinked_candidate_files() {
    use std::os::unix::fs::symlink;

    let harness = Harness::new();
    let id = harness.id("old");
    let outside = harness._temp.path().join("outside-session.jsonl");
    fs::write(
        &outside,
        format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{id}\",\"cwd\":{:?}}}}}\n",
            harness.root.to_string_lossy()
        ),
    )
    .unwrap();
    let candidate = harness.home.join("sessions/rollout-{id}.jsonl");
    symlink(&outside, &candidate).unwrap();

    assert!(matches!(
        verify_project_ownership(&harness.home, &harness.root, &id),
        Err(pueue_agent::AppError::CodexSessionMetadata { .. })
    ));
    assert_eq!(
        resolve_latest_owned_session(&harness.home, &harness.root)
            .unwrap_err()
            .code,
        PolicyViolationCode::SessionMissing
    );
}

#[test]
fn private_tmp_uses_only_the_fixed_verified_descriptor_path() {
    let harness = Harness::new();
    let argv = harness
        .builder()
        .build(&config_with_args(vec!["{prompt}"]), "p")
        .unwrap();
    let writable_root = argv
        .iter()
        .filter_map(|argument| argument.to_str())
        .find(|argument| argument.starts_with("sandbox_workspace_write.writable_roots="))
        .unwrap();
    assert_eq!(
        writable_root,
        "sandbox_workspace_write.writable_roots=[\"/dev/fd/11\"]"
    );
    assert!(!argv.iter().any(|argument| {
        argument
            .to_string_lossy()
            .contains(harness.private_run_temp.path().to_string_lossy().as_ref())
    }));
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
        .build(&config, "p")
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

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy)]
enum OutputMutation {
    Valid,
    ExitNonzeroValid,
    Replaced,
    Symlink,
    Mode0644,
    SecondHardLink,
    Oversize,
}

#[cfg(target_os = "linux")]
struct DecisionRunnerHarness {
    _temp: TempDir,
    db: Db,
    project_root: PathBuf,
    project: pueue_agent::models::Project,
    config: ProjectConfig,
    policy: Arc<pueue_agent::execution_policy::ResolvedExecutionPolicy>,
    reservation: pueue_agent::db::DecisionReservation,
    context: DecisionContextBundle,
    event_id: i64,
    capture_path: PathBuf,
    run_temp_path: Option<PathBuf>,
}

#[cfg(target_os = "linux")]
impl DecisionRunnerHarness {
    fn new(mutation: OutputMutation) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let fixture_root = fs::canonicalize(temp.path()).unwrap();
        let project_root = fixture_root.join("project");
        let service_dir = project_root.join(".pueue-agent");
        let trusted_bin = fixture_root.join("trusted-bin");
        let policy_state = fixture_root.join("policy-state");
        let codex_home = fixture_root.join("codex-home");
        for directory in [
            &project_root,
            &service_dir,
            &trusted_bin,
            &policy_state,
            &codex_home,
        ] {
            fs::create_dir_all(directory).unwrap();
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        fs::create_dir_all(service_dir.join("logs")).unwrap();
        fs::set_permissions(
            service_dir.join("logs"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        fs::write(service_dir.join("STATE.md"), "fixture state\n").unwrap();
        fs::write(service_dir.join("instructions.md"), "fixture instructions\n").unwrap();

        let capture_path = fixture_root.join("decision-capture.txt");
        let external_decision_path = fixture_root.join("external-decision.json");
        let codex = trusted_bin.join("codex");
        compile_decision_codex(
            &trusted_bin,
            &codex,
            &capture_path,
            &external_decision_path,
            mutation,
        );
        let custom = trusted_bin.join("custom-experiment-agent");
        fs::copy(&codex, &custom).unwrap();
        fs::set_permissions(&custom, fs::Permissions::from_mode(0o700)).unwrap();
        let launcher = trusted_bin.join("pueue-agent-launcher");
        fs::copy(env!("CARGO_BIN_EXE_pueue-agent"), &launcher).unwrap();
        fs::set_permissions(&launcher, fs::Permissions::from_mode(0o700)).unwrap();
        let pueue = trusted_bin.join("pueue");
        fs::copy(&codex, &pueue).unwrap();
        fs::set_permissions(&pueue, fs::Permissions::from_mode(0o700)).unwrap();
        let pueue_config = fixture_root.join("pueue.yml");
        fs::write(&pueue_config, "fixture: true\n").unwrap();
        fs::set_permissions(&pueue_config, fs::Permissions::from_mode(0o600)).unwrap();

        let config_path = service_dir.join("config.toml");
        fs::write(
            &config_path,
            format!(
                r#"project_id = "decision-project"
pueue_group = "decision-project"

[agent]
program = {:?}
args = ["--custom-project-agent-must-not-run"]
timeout_minutes = 1
max_retries = 0

[agent.execution]
network = "enabled"

[check]
interval_minutes = 10
deep_check_interval_minutes = 0
stall_minutes = 30
log_tail_bytes = 1024
extra_log_paths = []

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 3
max_experiments = 20
max_agent_runs = 10
"#,
                custom.display().to_string()
            ),
        )
        .unwrap();
        let config = config::load(&config_path).unwrap();

        fs::write(
            policy_state.join("execution-policy.toml"),
            format!(
                "version = 1\ntrusted_path = {:?}\n\n[executables]\ncodex = {:?}\npueue = {:?}\n\n[projects.\"decision-project\"]\ncustom_agent = {:?}\n",
                trusted_bin.display().to_string(),
                codex.display().to_string(),
                pueue.display().to_string(),
                custom.display().to_string(),
            ),
        )
        .unwrap();
        fs::set_permissions(
            policy_state.join("execution-policy.toml"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let policy = Arc::new(
            load_existing_policy(&PolicyLoadInput {
                state_dir: policy_state,
                project_roots: vec![fs::canonicalize(&project_root).unwrap()],
                inherited_path: trusted_bin.clone().into_os_string(),
                startup_environment: StartupEnvironment::from_pairs([
                    ("HOME", "/fixture"),
                    ("OPENAI_API_KEY", "fixture-openai-secret"),
                    ("AWS_SECRET_ACCESS_KEY", "fixture-aws-secret"),
                    ("SSH_AUTH_SOCK", "/fixture/ssh-agent.sock"),
                ]),
                codex_home,
                pueue_config,
                launcher_path: launcher,
            })
            .unwrap(),
        );

        let db = Db::open(&fixture_root.join("state.sqlite3")).unwrap();
        let project = ProjectRepository::new(&db)
            .register(&NewProject::new(
                "decision-project",
                fs::canonicalize(&project_root).unwrap(),
                "decision-project",
                config_path,
                100,
            ))
            .unwrap();
        let objective = ObjectiveSnapshot {
            text: "Improve the validation result safely.\n".to_owned(),
            digest: "decision-objective-digest".to_owned(),
        };
        let initial_argv = vec!["python".to_owned(), "train.py".to_owned()];
        let baseline = proposals::validate_initial_baseline(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: "Establish a baseline".to_owned(),
                source_experiment_id: None,
                argv: initial_argv.clone(),
                working_directory: ".".to_owned(),
                expected_evidence: vec!["validation loss".to_owned()],
            },
            &objective.digest,
        )
        .unwrap();
        let campaign = CampaignRepository::new(&db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id: "decision-campaign",
                    project_id: &project.project_id,
                    objective: &objective,
                    initial_argv: &initial_argv,
                    baseline: &baseline,
                    submission_id: "decision-submission",
                    experiment_id: "decision-experiment",
                    proposal_id: "decision-proposal",
                    metadata: &json!({}),
                    origin_agent_run_id: None,
                    now: 100,
                },
                &CampaignLimits::default(),
            )
            .unwrap()
            .campaign;
        let experiments = ExperimentRepository::new(&db);
        experiments.mark_submitting("decision-experiment", 101).unwrap();
        experiments
            .mark_accepted("decision-experiment", 41, "decision-task-signature", 102)
            .unwrap();
        experiments
            .project_terminal_submission(
                "decision-experiment",
                41,
                ExperimentTerminalOutcome::Succeeded,
                103,
            )
            .unwrap();
        let decisions = DecisionRepository::new(&db);
        let cycle = decisions
            .ensure_cycle_for_terminal(&campaign.campaign_id, "decision-experiment", 104)
            .unwrap();
        let reservation = decisions
            .reserve_next_attempt(&project.project_id, &cycle.cycle_id, 105)
            .unwrap()
            .unwrap();
        let context_json = json!({
            "schema_version": 1,
            "objective": {
                "text": objective.text,
                "digest": objective.digest,
            },
            "source_experiment": {
                "experiment_id": "decision-experiment",
            },
        })
        .to_string();
        let context = DecisionContextBundle {
            digest: format!("{:x}", Sha256::digest(context_json.as_bytes())),
            json: context_json,
        };
        decisions
            .store_evidence(&reservation, &context.json, &context.digest, 106)
            .unwrap();
        let event_id = EventRepository::new(&db)
            .insert_idempotent(
                &NewEvent::new(
                    &project.project_id,
                    EventKind::CampaignDecision,
                    "decision-runner-fixture",
                    json!({"cycle_id": cycle.cycle_id}),
                    106,
                    106,
                )
                .with_campaign_lineage(&campaign.campaign_id, Some("decision-experiment")),
            )
            .unwrap()
            .event_id;
        let claimed = EventRepository::new(&db)
            .claim_batch(106, 166, 1)
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].event_id, event_id);

        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE decision_runner_order (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    label TEXT NOT NULL
                 );
                 CREATE TRIGGER decision_runner_decision_commit
                 AFTER UPDATE OF state ON decision_attempts
                 WHEN NEW.state IN ('decided','failed') AND OLD.state <> NEW.state
                 BEGIN
                     INSERT INTO decision_runner_order(label) VALUES ('decision_commit');
                 END;
                 CREATE TRIGGER decision_runner_agent_terminal
                 AFTER UPDATE OF status ON agent_runs
                 WHEN NEW.status IN ('completed','failed','timed_out','cancelled')
                      AND OLD.status <> NEW.status
                 BEGIN
                     INSERT INTO decision_runner_order(label) VALUES ('agent_terminal');
                 END;",
            )
            .unwrap();

        Self {
            _temp: temp,
            db,
            project_root: fs::canonicalize(project_root).unwrap(),
            project,
            config,
            policy,
            reservation,
            context,
            event_id,
            capture_path,
            run_temp_path: None,
        }
    }

    async fn run_to_terminal(&mut self) -> Result<(), pueue_agent::AppError> {
        let runner = AgentRunner::new(AgentRunnerConfig::production(), Arc::clone(&self.policy));
        let project_policy = runner.resolve_project_policy(&self.project, &self.config)?;
        let run_id_guard = runner
            .try_acquire_run_id_admission_guard(&self.db)?
            .expect("run ID admission guard");
        let project_lock = runner
            .try_acquire_project_admission_lock(&project_policy)?
            .expect("project admission lock");
        let mut handle = runner
            .spawn_decision(
                &self.db,
                &self.project,
                &project_policy,
                &self.config.agent,
                RetryPolicy { max_retries: 0 },
                self.event_id,
                &[self.event_id],
                &self.reservation,
                &self.context,
                107,
                run_id_guard,
                project_lock,
            )
            .await
            .map_err(|error| error.source)?;
        self.run_temp_path = Some(
            self.project_root
                .join(".pueue-agent/tmp")
                .join(handle.run_id.to_string()),
        );
        handle.wait(&self.db, 108).await?;
        Ok(())
    }

    fn capture(&self, field: &str) -> String {
        fs::read_to_string(&self.capture_path)
            .unwrap()
            .lines()
            .find_map(|line| line.split_once('=').filter(|(name, _)| *name == field))
            .map(|(_, value)| value.to_owned())
            .unwrap()
    }

    fn captured_environment_names(&self) -> Vec<String> {
        fs::read_to_string(&self.capture_path)
            .unwrap()
            .lines()
            .filter_map(|line| {
                line.strip_prefix("environment_name=")
                    .map(str::to_owned)
            })
            .collect()
    }

    fn stored_decision_kind(&self) -> Option<String> {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT decision_kind FROM decision_attempts
                 WHERE cycle_id = ?1 AND attempt_number = ?2",
                params![self.reservation.cycle_id, self.reservation.attempt_number],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn attempt_failure_code(&self) -> Option<String> {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT failure_code FROM decision_attempts
                 WHERE cycle_id = ?1 AND attempt_number = ?2",
                params![self.reservation.cycle_id, self.reservation.attempt_number],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn cleanup_order(&self) -> Vec<&'static str> {
        let connection = self.db.connect().unwrap();
        let mut statement = connection
            .prepare("SELECT label FROM decision_runner_order ORDER BY sequence")
            .unwrap();
        let labels = statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let mut order = labels
            .into_iter()
            .map(|label| match label.as_str() {
                "decision_commit" => "decision_commit",
                "agent_terminal" => "agent_terminal",
                _ => panic!("unexpected order label"),
            })
            .collect::<Vec<_>>();
        let run_temp = self.run_temp_path.as_ref().unwrap();
        if fs::read_dir(run_temp).unwrap().next().is_none() {
            order.push("temp_cleanup");
        }
        order
    }
}

#[cfg(target_os = "linux")]
fn compile_decision_codex(
    trusted_bin: &Path,
    target: &Path,
    capture_path: &Path,
    external_decision_path: &Path,
    mutation: OutputMutation,
) {
    let source = trusted_bin.join(format!("codex-{mutation:?}.rs"));
    fs::write(
        &source,
        format!(
            r##"use std::{{env, fs, io::Write, os::unix::fs::{{OpenOptionsExt, PermissionsExt}}, path::Path, process::exit}};

fn pair<'a>(args: &'a [String], name: &str) -> Option<&'a str> {{
    args.windows(2).find(|pair| pair[0] == name).map(|pair| pair[1].as_str())
}}

fn main() {{
    let args = env::args().skip(1).collect::<Vec<_>>();
    if pair(&args, "--sandbox") != Some("read-only") {{
        fs::write("forbidden-write", b"unsafe").unwrap();
    }}
    let network = args.windows(2).find_map(|pair| {{
        (pair[0] == "-c").then_some(pair[1].as_str())
    }}).and_then(|value| value.strip_prefix("sandbox_workspace_write.network_access=")).unwrap_or("missing");
    let mut names = env::vars_os().filter_map(|(name, _)| name.into_string().ok()).collect::<Vec<_>>();
    names.sort();
    let mut capture = format!("network_access={{network}}\n");
    for name in names {{ capture.push_str(&format!("environment_name={{name}}\n")); }}
    fs::write({capture_path:?}, capture).unwrap();

    let schema = pair(&args, "--output-schema").unwrap();
    let output = pair(&args, "--output-last-message").unwrap();
    if !Path::new(schema).is_file() || !Path::new(output).is_file() {{ exit(71); }}
    let decision = br#"{{"schema_version":1,"decision":"proposal","proposal":{{"kind":"experiment","hypothesis":"lower learning rate","source_experiment_id":"decision-experiment","argv":["python","train.py","--lr","0.001"],"working_directory":".","expected_evidence":["validation loss"]}}}}"#;
    match {mutation:?} {{
        "Valid" => fs::write(output, decision).unwrap(),
        "ExitNonzeroValid" => {{
            fs::write(output, decision).unwrap();
            exit(73);
        }}
        "Replaced" => {{
            fs::remove_file(output).unwrap();
            let mut file = fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(output).unwrap();
            file.write_all(decision).unwrap();
        }}
        "Symlink" => {{
            fs::write({external_decision_path:?}, decision).unwrap();
            fs::remove_file(output).unwrap();
            std::os::unix::fs::symlink({external_decision_path:?}, output).unwrap();
        }}
        "Mode0644" => {{
            fs::write(output, decision).unwrap();
            fs::set_permissions(output, fs::Permissions::from_mode(0o644)).unwrap();
        }}
        "SecondHardLink" => {{
            fs::write(output, decision).unwrap();
            std::fs::hard_link(output, "/dev/fd/11/decision-hard-link").unwrap();
        }}
        "Oversize" => fs::write(output, vec![b'x'; 128 * 1024 + 1]).unwrap(),
        _ => exit(72),
    }}
}}
"##,
            capture_path = capture_path,
            external_decision_path = external_decision_path,
            mutation = format!("{mutation:?}"),
        ),
    )
    .unwrap();
    let output = Command::new("rustc")
        .args(["--edition=2021", "-o"])
        .arg(target)
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "generated decision Codex failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::set_permissions(target, fs::Permissions::from_mode(0o700)).unwrap();
}

struct Harness {
    _temp: TempDir,
    root: PathBuf,
    other: PathBuf,
    home: PathBuf,
    private_run_temp: PrivateRunTemp,
}

impl Harness {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("project");
        let other = temp.path().join("other");
        let home = temp.path().join("codex-home");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&other).unwrap();
        fs::create_dir_all(home.join("sessions")).unwrap();
        fs::create_dir_all(home.join("archived_sessions")).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for directory in [
                root.clone(),
                other.clone(),
                home.clone(),
                home.join("sessions"),
                home.join("archived_sessions"),
            ] {
                fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
            }
        }
        let root = fs::canonicalize(root).unwrap();
        let other = fs::canonicalize(other).unwrap();
        let home = fs::canonicalize(home).unwrap();
        let verified_root = ProjectRootAnchor::resolve(&root)
            .unwrap()
            .verify_identity()
            .unwrap();
        let private_run_temp = PrivateRunTemp::create(&verified_root, 1).unwrap();
        Self {
            _temp: temp,
            root,
            other,
            home,
            private_run_temp,
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
                trusted_path: vec![self.root.join("trusted-bin")],
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
    let store_name = if label == "old" {
        "sessions"
    } else {
        "archived_sessions"
    };
    write_session_with_id(home, store_name, id, cwd);
}

fn write_session_with_id(home: &Path, store_name: &str, id: &str, cwd: &Path) {
    fs::write(
        home.join(store_name).join(format!("rollout-{id}.jsonl")),
        format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{id}\",\"cwd\":{cwd:?}}}}}\n",
            cwd = cwd.to_string_lossy()
        ),
    )
    .unwrap();
}
