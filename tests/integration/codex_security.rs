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
        PolicyLoadInput, PolicyViolationCode, PolicyViolationDetail, ProjectRootAnchor,
        ResolvedProjectExecutionPolicy, StartupEnvironment, TempUnsafeReason,
    },
    environment::{PrivateRunTemp, SanitizedEnvironment},
    models::AgentContextMode,
};
use tempfile::TempDir;

#[cfg(unix)]
use std::time::Instant;

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
    assert!(!format!("{environment:?}").contains("secret"));
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
        .build(&config_with_args(vec!["{prompt}"]), "p", &harness.private_tmp)
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

#[cfg(unix)]
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

#[cfg(unix)]
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
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    let weak_anchor = ProjectRootAnchor::resolve(&weak_harness.root).unwrap();
    let weak_root = weak_anchor.verify_identity().unwrap();
    assert!(PrivateRunTemp::create(&weak_root, 42).is_err());

    let symlink_harness = Harness::new();
    let outside = symlink_harness._temp.path().join("outside");
    fs::create_dir(&outside).unwrap();
    fs::remove_dir_all(symlink_harness.root.join(".pueue-agent")).unwrap();
    std::os::unix::fs::symlink(&outside, symlink_harness.root.join(".pueue-agent")).unwrap();
    let symlink_anchor = ProjectRootAnchor::resolve(&symlink_harness.root).unwrap();
    let symlink_root = symlink_anchor.verify_identity().unwrap();
    assert!(PrivateRunTemp::create(&symlink_root, 42).is_err());
}

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
#[test]
fn private_temp_cleanup_does_not_follow_symlink_or_fifo_targets() {
    use std::os::unix::fs::symlink;

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
    let report = temp.cleanup_contents_before(None).unwrap();

    assert_eq!(report.entries_removed, 2);
    assert_eq!(fs::read(&outside).unwrap(), b"keep");
    assert!(temp.path().is_dir());
    assert!(fs::read_dir(temp.path()).unwrap().next().is_none());
}

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
#[test]
fn private_temp_inventory_allows_empty_generations_and_rejects_nonempty_or_unsafe_generations() {
    use std::os::unix::fs::PermissionsExt;

    let harness = Harness::new();
    let tmp = harness.root.join(".pueue-agent/tmp");
    fs::remove_dir_all(tmp.join("run")).unwrap();
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

#[cfg(unix)]
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

#[cfg(unix)]
#[test]
fn private_temp_inventory_rejects_generation_overflow_without_mutation() {
    use std::os::unix::fs::PermissionsExt;

    let harness = Harness::new();
    let tmp = harness.root.join(".pueue-agent/tmp");
    fs::remove_dir_all(tmp.join("run")).unwrap();
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
            .build(&config_with_args(vec!["exec", "{prompt}"]), prompt, &harness.private_tmp)
            .unwrap();
        assert_prompt_after_separator(&fresh, prompt);

        let mut resume_config = config_with_args(vec!["{prompt}"]);
        resume_config.context = AgentContextMode::Resume {
            session_id: harness.id("old"),
        };
        let resumed = harness
            .builder()
            .build(&resume_config, prompt, &harness.private_tmp)
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
fn private_tmp_under_trusted_project_root_is_allowed_when_root_is_in_tmp() {
    let harness = Harness::new();
    let mut policy = harness.builder().policy().clone();
    policy.root_anchor.canonical_path = PathBuf::from("/tmp/trusted-project");
    let private_tmp = PathBuf::from("/tmp/trusted-project/.pueue-agent/tmp/run");

    CodexArgvBuilder::new(policy, CodexCapabilities::all())
        .build(&config_with_args(vec!["{prompt}"]), "p", &private_tmp)
        .expect("trusted project temp must remain an allowed writable root");
}

#[test]
fn private_tmp_requires_fixed_root_and_one_bounded_run_component() {
    let harness = Harness::new();
    let fixed_root = harness.root.join(".pueue-agent/tmp");
    for private_tmp in [
        fixed_root.clone(),
        fixed_root.join("run/nested"),
        harness.root.join("other/run"),
        harness.root.join(".pueue-agent/tmp/../tmp/run"),
    ] {
        let error = harness
            .builder()
            .build(&config_with_args(vec!["{prompt}"]), "p", &private_tmp)
            .unwrap_err();
        assert_eq!(error.code, PolicyViolationCode::UnsafeCodexArgument);
    }

    for relative_root in [
        PathBuf::from("/absolute/private-tmp"),
        PathBuf::from("."),
        PathBuf::from(".pueue-agent/tmp/nested"),
    ] {
        let mut policy = harness.builder().policy().clone();
        policy.private_temp_relative_root = relative_root;
        let error = CodexArgvBuilder::new(policy, CodexCapabilities::all())
            .build(
                &config_with_args(vec!["{prompt}"]),
                "p",
                &harness.private_tmp,
            )
            .unwrap_err();
        assert_eq!(error.code, PolicyViolationCode::UnsafeCodexArgument);
    }
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
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for directory in [
                root.join(".pueue-agent"),
                root.join(".pueue-agent/tmp"),
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
