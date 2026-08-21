use std::{
    fs,
    path::{Path, PathBuf},
};

use pueue_agent::{
    config::{AgentConfig, AgentCodexConfig, AgentExecutionConfig, ProjectConfig},
    db::{Db, ProjectRepository},
    diagnostics::{build_doctor_report_with_policy, DoctorCheckStatus, DoctorExternal},
    execution_policy::{
        load_existing_policy, load_or_create_policy, resolve_project_policy, AgentKind,
        CampaignLimits, NetworkMode, PolicyLoadInput, PolicyViolationCode, StartupEnvironment,
    },
    models::{AgentContextMode, NewProject, Project},
    service::{ServicePaths, ServiceStatus},
};
use tempfile::{tempdir, TempDir};

#[test]
fn doctor_detects_replaced_pueue_executable_and_config_anchors() {
    for replacement in ["pueue", "config"] {
        let harness = PolicyHarness::new();
        let db = Db::open(&harness.path("doctor.sqlite3")).unwrap();
        let project = harness.project("doctor-project");
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                &project.project_id,
                &project.root_path,
                &project.pueue_group,
                &project.config_path,
                0,
            ))
            .unwrap();
        let policy = load_or_create_policy(&harness.input()).unwrap();
        let replaced = if replacement == "pueue" {
            &harness.pueue
        } else {
            &harness.pueue_config
        };
        fs::rename(replaced, replaced.with_extension("old")).unwrap();
        fs::write(replaced, b"replacement").unwrap();
        secure_file(replaced);

        let paths = ServicePaths {
            release_binary: harness.launcher.clone(),
            pueue_config: harness.pueue_config.clone(),
            state_dir: harness.state_dir.clone(),
            execution_policy: harness.policy(),
            working_dir: project.root_path.clone(),
            home: harness.path("home"),
            codex_home: harness.codex_home.clone(),
            path_env: harness.trusted_bin.to_string_lossy().into_owned(),
            startup_environment: StartupEnvironment::default(),
        };
        let report = build_doctor_report_with_policy(
            &db,
            &project,
            &paths,
            DoctorExternal {
                pueue: Ok(Vec::new()),
                service: Ok(ServiceStatus::Stopped),
                callback: Ok(None),
            },
            0,
            &Ok(policy),
        )
        .unwrap();
        let anchors = report
            .checks
            .iter()
            .find(|check| check.name == "execution.anchors")
            .unwrap();
        assert_eq!(anchors.status, DoctorCheckStatus::Error, "{replacement:?}");
    }
}

struct PolicyHarness {
    temp: TempDir,
    state_dir: PathBuf,
    project_root: PathBuf,
    trusted_bin: PathBuf,
    codex: PathBuf,
    pueue: PathBuf,
    launcher: PathBuf,
    pueue_config: PathBuf,
    codex_home: PathBuf,
}

impl PolicyHarness {
    fn new() -> Self {
        let temp = tempdir().unwrap();
        let base = fs::canonicalize(temp.path()).unwrap();
        let state_dir = base.join("state");
        let project_root = base.join("project");
        let trusted_bin = base.join("trusted-bin");
        let codex_home = base.join("codex-home");
        for directory in [&state_dir, &project_root, &trusted_bin, &codex_home] {
            fs::create_dir(directory).unwrap();
            secure_directory(directory);
        }
        let codex = trusted_bin.join("codex");
        let pueue = trusted_bin.join("pueue");
        let launcher = trusted_bin.join("launcher-fixture");
        for executable in [&codex, &pueue, &launcher] {
            fs::write(executable, b"fixture executable").unwrap();
            secure_executable(executable);
        }
        let pueue_config = base.join("pueue.yml");
        fs::write(&pueue_config, b"fixture: true\n").unwrap();
        secure_file(&pueue_config);

        Self {
            temp,
            state_dir,
            project_root,
            trusted_bin,
            codex,
            pueue,
            launcher,
            pueue_config,
            codex_home,
        }
    }

    fn policy(&self) -> PathBuf {
        self.state_dir.join("execution-policy.toml")
    }

    fn path(&self, name: &str) -> PathBuf {
        fs::canonicalize(self.temp.path()).unwrap().join(name)
    }

    fn input(&self) -> PolicyLoadInput {
        PolicyLoadInput {
            state_dir: self.state_dir.clone(),
            project_roots: vec![self.project_root.clone()],
            inherited_path: self.trusted_bin.clone().into_os_string(),
            startup_environment: StartupEnvironment::from_pairs([("FIXTURE_NAME", "fixture")]),
            codex_home: self.codex_home.clone(),
            pueue_config: self.pueue_config.clone(),
            launcher_path: self.launcher.clone(),
        }
    }

    fn project(&self, project_id: &str) -> Project {
        Project {
            project_id: project_id.to_owned(),
            root_path: self.project_root.clone(),
            pueue_group: "pa-test".to_owned(),
            config_path: self.project_root.join("config.toml"),
            enabled: true,
            paused: false,
            halted_reason: None,
            created_at: 0,
            updated_at: 0,
        }
    }

    #[cfg(unix)]
    fn with_symlinked_pueue_config() -> Self {
        let mut harness = Self::new();
        let link = harness.path("pueue-link.yml");
        std::os::unix::fs::symlink(&harness.pueue_config, &link).unwrap();
        harness.pueue_config = link;
        harness
    }
}

fn custom_config(project_id: &str, program: &str, network: NetworkMode) -> ProjectConfig {
    ProjectConfig {
        project_id: project_id.to_owned(),
        pueue_group: "pa-test".to_owned(),
        agent: AgentConfig {
            program: program.to_owned(),
            args: vec!["{prompt}".to_owned()],
            timeout_minutes: 1,
            max_retries: 0,
            context: AgentContextMode::Fresh,
            execution: AgentExecutionConfig { network },
            codex: AgentCodexConfig {
                model: None,
                reasoning_effort: None,
            },
        },
        check: pueue_agent::config::CheckConfig {
            interval_minutes: 1,
            deep_check_interval_minutes: 0,
            stall_minutes: 1,
            log_tail_bytes: 1,
            extra_log_paths: Vec::new(),
            patterns: Vec::new(),
            stall: pueue_agent::config::StallConfig {
                action: pueue_agent::config::PatternAction::Notify,
                kill_after_minutes: 0,
            },
        },
        guardrails: pueue_agent::config::GuardrailsConfig {
            max_consecutive_failures: 1,
            max_experiments: 1,
            max_agent_runs: 1,
        },
    }
}

#[test]
fn missing_policy_is_atomic_secure_default() {
    let h = PolicyHarness::new();
    let p = load_or_create_policy(&h.input()).unwrap();
    assert_eq!(p.default_network, NetworkMode::Enabled);
    assert!(p.custom_allowlist.is_empty());
    assert_eq!(policy_mode(&h.policy()) & 0o077, 0);
}

#[test]
fn decision_campaign_limits_have_safe_service_defaults() {
    let harness = PolicyHarness::new();
    let policy = load_or_create_policy(&harness.input()).unwrap();
    assert_eq!(
        policy.campaign_limits,
        CampaignLimits {
            max_parallel_experiments: 1,
            max_new_experiments_per_24h: 24,
            max_agent_runs_per_hour: 6,
            max_code_change_proposals_per_24h: 10,
            max_same_spec_retries: 2,
            max_repairs_per_failure_fingerprint: 2,
            max_proposals_per_cycle: 1,
            observer_interval_minutes: 30,
            max_decision_attempts_per_cycle: 3,
            max_decision_wait_minutes: 1_440,
        }
    );
    assert_eq!(policy.default_network, NetworkMode::Enabled);

    let legacy_policy = fs::read_to_string(harness.policy())
        .unwrap()
        .replacen("max_decision_attempts_per_cycle = 3\n", "", 1)
        .replacen("max_decision_wait_minutes = 1440\n", "", 1);
    fs::write(harness.policy(), legacy_policy).unwrap();
    secure_file(&harness.policy());
    let legacy_limits = load_existing_policy(&harness.input())
        .unwrap()
        .campaign_limits;
    assert_eq!(legacy_limits.max_decision_attempts_per_cycle, 3);
    assert_eq!(legacy_limits.max_decision_wait_minutes, 1_440);
}

#[test]
fn decision_campaign_limits_reject_values_outside_service_bounds() {
    let harness = PolicyHarness::new();
    load_or_create_policy(&harness.input()).unwrap();
    let default_policy = fs::read_to_string(harness.policy()).unwrap();

    for (field, value) in [
        ("max_parallel_experiments", 0),
        ("max_parallel_experiments", 65),
        ("max_new_experiments_per_24h", 0),
        ("max_new_experiments_per_24h", 10_001),
        ("max_agent_runs_per_hour", 0),
        ("max_agent_runs_per_hour", 1_001),
        ("max_code_change_proposals_per_24h", 1_001),
        ("max_same_spec_retries", 101),
        ("max_repairs_per_failure_fingerprint", 101),
        ("max_proposals_per_cycle", 0),
        ("max_proposals_per_cycle", 33),
        ("observer_interval_minutes", 0),
        ("observer_interval_minutes", 1_441),
        ("max_decision_attempts_per_cycle", 0),
        ("max_decision_attempts_per_cycle", 11),
        ("max_decision_wait_minutes", 0),
        ("max_decision_wait_minutes", 10_081),
    ] {
        let updated = default_policy.replacen(
            &format!("{field} = {}", campaign_limit_default(field)),
            &format!("{field} = {value}"),
            1,
        );
        fs::write(harness.policy(), updated).unwrap();
        secure_file(&harness.policy());
        assert!(matches!(
            load_existing_policy(&harness.input()),
            Err(pueue_agent::execution_policy::PolicyViolation {
                code: PolicyViolationCode::PolicyUnknownField,
                ..
            })
        ), "{field} = {value}");
    }

    for field in [
        "max_code_change_proposals_per_24h",
        "max_same_spec_retries",
        "max_repairs_per_failure_fingerprint",
    ] {
        let updated = default_policy.replacen(
            &format!("{field} = {}", campaign_limit_default(field)),
            &format!("{field} = 0"),
            1,
        );
        fs::write(harness.policy(), updated).unwrap();
        secure_file(&harness.policy());
        assert!(load_existing_policy(&harness.input()).is_ok(), "{field} = 0");
    }
}

#[test]
fn trusted_path_rejects_project_or_weak_component() {
    let h = PolicyHarness::new();
    let mut i = h.input();
    i.inherited_path = h.project_root.join("bin").into_os_string();
    fs::create_dir(h.project_root.join("bin")).unwrap();
    assert!(matches!(
        load_or_create_policy(&i),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::TrustedPathUnsafe,
            ..
        })
    ));
}

#[cfg(unix)]
#[test]
fn trusted_path_rejects_a_symlink_component() {
    let h = PolicyHarness::new();
    let symlink = h.path("trusted-bin-link");
    std::os::unix::fs::symlink(&h.trusted_bin, &symlink).unwrap();
    let mut input = h.input();
    input.inherited_path = symlink.into_os_string();
    assert!(matches!(
        load_or_create_policy(&input),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::TrustedPathUnsafe,
            ..
        })
    ));
}

#[cfg(unix)]
#[test]
fn pueue_config_symlink_is_rejected_before_anchor_creation() {
    let harness = PolicyHarness::with_symlinked_pueue_config();
    let error = load_or_create_policy(&harness.input()).unwrap_err();
    assert_eq!(error.code, PolicyViolationCode::AnchorMissing);
}

#[test]
fn replacement_fails_closed_without_reresolution() {
    let h = PolicyHarness::new();
    let p = load_or_create_policy(&h.input()).unwrap();
    let a = p.codex_anchor;
    fs::rename(&a.canonical_path, h.path("old")).unwrap();
    fs::write(&a.canonical_path, b"new").unwrap();
    secure_executable(&a.canonical_path);
    assert!(matches!(
        a.verify_identity(),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::AnchorReplaced,
            ..
        })
    ));
}

#[test]
fn daemon_startup_creates_missing_policy_before_anchor_resolution() {
    let h = PolicyHarness::new();
    assert!(!h.policy().exists());
    let p = load_or_create_policy(&h.input()).unwrap();
    assert!(h.policy().is_file());
    assert!(p.pueue_config_anchor.canonical_path.is_absolute());
}

#[test]
fn existing_loader_never_creates_missing_policy() {
    let h = PolicyHarness::new();
    assert!(matches!(
        load_existing_policy(&h.input()),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::PolicyMissing,
            ..
        })
    ));
    assert!(!h.policy().exists());
}

#[cfg(not(unix))]
#[test]
fn non_unix_policy_entry_points_fail_closed() {
    let h = PolicyHarness::new();
    assert!(matches!(
        load_or_create_policy(&h.input()),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::UnsupportedPlatform,
            ..
        })
    ));
    assert!(matches!(
        load_existing_policy(&h.input()),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::UnsupportedPlatform,
            ..
        })
    ));
}

#[test]
fn existing_policy_schema_resolves_defaults_and_enrollment() {
    let h = PolicyHarness::new();
    let custom = h.trusted_bin.join("custom-agent");
    fs::write(&custom, b"custom").unwrap();
    secure_executable(&custom);
    fs::write(
        h.policy(),
        format!(
            "version = 1\ntrusted_path = {:?}\n\n[defaults]\nnetwork = \"disabled\"\n\n[executables]\ncodex = {:?}\npueue = {:?}\n\n[projects.\"project-a\"]\ncustom_agent = {:?}\nagent_environment_allow = [\"DATASET_ROOT\"]\ntask_environment_allow = [\"NVIDIA_VISIBLE_DEVICES\"]\n",
            h.trusted_bin.display().to_string(),
            h.codex.display().to_string(),
            h.pueue.display().to_string(),
            custom.display().to_string(),
        ),
    )
    .unwrap();
    secure_file(&h.policy());

    let p = load_existing_policy(&h.input()).unwrap();
    assert_eq!(p.default_network, NetworkMode::Disabled);
    assert_eq!(p.trusted_path, vec![fs::canonicalize(&h.trusted_bin).unwrap()]);
    assert_eq!(p.custom_allowlist["project-a"].canonical_path, custom);
}

#[test]
fn project_network_can_only_narrow_global_policy() {
    let h = PolicyHarness::new();
    let global = load_or_create_policy(&h.input()).unwrap();
    let project = h.project("project-a");

    let enabled = custom_config("project-a", "codex", NetworkMode::Enabled);
    assert_eq!(
        resolve_project_policy(&global, &project, &enabled)
            .unwrap()
            .network,
        NetworkMode::Enabled
    );

    let disabled = custom_config("project-a", "codex", NetworkMode::Disabled);
    assert_eq!(
        resolve_project_policy(&global, &project, &disabled)
            .unwrap()
            .network,
        NetworkMode::Disabled
    );

    fs::write(
        h.policy(),
        format!(
            "version = 1\ntrusted_path = {:?}\n\n[defaults]\nnetwork = \"disabled\"\n\n[executables]\ncodex = {:?}\npueue = {:?}\n",
            h.trusted_bin.display().to_string(),
            h.codex.display().to_string(),
            h.pueue.display().to_string(),
        ),
    )
    .unwrap();
    secure_file(&h.policy());
    let global_disabled = load_existing_policy(&h.input()).unwrap();
    assert_eq!(
        resolve_project_policy(&global_disabled, &project, &enabled)
            .unwrap()
            .network,
        NetworkMode::Disabled
    );
}

#[test]
fn custom_agent_requires_exact_service_enrollment_and_absolute_canonical_path() {
    let h = PolicyHarness::new();
    let custom = h.trusted_bin.join("custom-agent");
    fs::write(&custom, b"custom").unwrap();
    secure_executable(&custom);
    let other = h.trusted_bin.join("other-agent");
    fs::write(&other, b"other").unwrap();
    secure_executable(&other);

    fs::write(
        h.policy(),
        format!(
            "version = 1\ntrusted_path = {:?}\n\n[defaults]\nnetwork = \"enabled\"\n\n[executables]\ncodex = {:?}\npueue = {:?}\n\n[projects.\"project-a\"]\ncustom_agent = {:?}\n",
            h.trusted_bin.display().to_string(),
            h.codex.display().to_string(),
            h.pueue.display().to_string(),
            custom.display().to_string(),
        ),
    )
    .unwrap();
    secure_file(&h.policy());
    let global = load_existing_policy(&h.input()).unwrap();
    let project = h.project("project-a");

    let mismatched = custom_config("project-a", &other.to_string_lossy(), NetworkMode::Enabled);
    assert!(matches!(
        resolve_project_policy(&global, &project, &mismatched),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::CustomAgentNotEnrolled,
            ..
        })
    ));

    let bare = custom_config("project-a", "custom-agent", NetworkMode::Enabled);
    assert!(matches!(
        resolve_project_policy(&global, &project, &bare),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::CustomAgentNotEnrolled,
            ..
        })
    ));

    let traversal = custom
        .parent()
        .unwrap()
        .join("..")
        .join("trusted-bin")
        .join(custom.file_name().unwrap());
    let traversal = custom_config(
        "project-a",
        &traversal.to_string_lossy(),
        NetworkMode::Enabled,
    );
    assert!(matches!(
        resolve_project_policy(&global, &project, &traversal),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::CustomAgentNotEnrolled,
            ..
        })
    ));

    let exact = custom_config("project-a", &custom.to_string_lossy(), NetworkMode::Enabled);
    let resolved = resolve_project_policy(&global, &project, &exact).unwrap();
    assert_eq!(resolved.agent_kind, AgentKind::Custom);
    assert_eq!(resolved.agent_anchor.canonical_path, custom);

    fs::rename(&custom, h.path("custom-agent-old")).unwrap();
    fs::write(&custom, b"replacement").unwrap();
    secure_executable(&custom);
    assert!(matches!(
        resolved.agent_anchor.verify_identity(),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::AnchorReplaced,
            ..
        })
    ));
}

#[cfg(unix)]
#[test]
fn custom_agent_symlink_alias_is_not_an_exact_enrollment() {
    let h = PolicyHarness::new();
    let custom = h.trusted_bin.join("custom-agent");
    let alias = h.trusted_bin.join("custom-agent-alias");
    fs::write(&custom, b"custom").unwrap();
    secure_executable(&custom);
    std::os::unix::fs::symlink(&custom, &alias).unwrap();

    fs::write(
        h.policy(),
        format!(
            "version = 1\ntrusted_path = {:?}\n\n[defaults]\nnetwork = \"enabled\"\n\n[executables]\ncodex = {:?}\npueue = {:?}\n\n[projects.\"project-a\"]\ncustom_agent = {:?}\n",
            h.trusted_bin.display().to_string(),
            h.codex.display().to_string(),
            h.pueue.display().to_string(),
            custom.display().to_string(),
        ),
    )
    .unwrap();
    secure_file(&h.policy());
    let global = load_existing_policy(&h.input()).unwrap();
    let project = h.project("project-a");
    let config = custom_config("project-a", &alias.to_string_lossy(), NetworkMode::Enabled);

    assert!(matches!(
        resolve_project_policy(&global, &project, &config),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::CustomAgentNotEnrolled,
            ..
        })
    ));
}

#[test]
fn custom_agent_inside_current_project_root_is_rejected_even_when_root_is_not_in_inventory() {
    let h = PolicyHarness::new();
    let project_root = h.path("unregistered-project");
    fs::create_dir(&project_root).unwrap();
    secure_directory(&project_root);
    let custom = project_root.join("custom-agent");
    fs::write(&custom, b"custom").unwrap();
    secure_executable(&custom);

    fs::write(
        h.policy(),
        format!(
            "version = 1\ntrusted_path = {:?}\n\n[defaults]\nnetwork = \"enabled\"\n\n[executables]\ncodex = {:?}\npueue = {:?}\n\n[projects.\"project-a\"]\ncustom_agent = {:?}\n",
            h.trusted_bin.display().to_string(),
            h.codex.display().to_string(),
            h.pueue.display().to_string(),
            custom.display().to_string(),
        ),
    )
    .unwrap();
    secure_file(&h.policy());
    let global = load_existing_policy(&h.input()).unwrap();
    let mut project = h.project("project-a");
    project.root_path = project_root;
    let config = custom_config("project-a", &custom.to_string_lossy(), NetworkMode::Enabled);

    assert!(matches!(
        resolve_project_policy(&global, &project, &config),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::ProjectRootExecutable,
            ..
        })
    ));
}

#[test]
fn custom_agent_outside_an_uninventoried_project_root_fails_as_root_changed() {
    let h = PolicyHarness::new();
    let project_root = h.path("unregistered-project");
    fs::create_dir(&project_root).unwrap();
    secure_directory(&project_root);
    let custom = h.trusted_bin.join("custom-agent");
    fs::write(&custom, b"custom").unwrap();
    secure_executable(&custom);
    fs::write(
        h.policy(),
        format!(
            "version = 1\ntrusted_path = {:?}\n\n[defaults]\nnetwork = \"enabled\"\n\n[executables]\ncodex = {:?}\npueue = {:?}\n\n[projects.\"project-a\"]\ncustom_agent = {:?}\n",
            h.trusted_bin.display().to_string(),
            h.codex.display().to_string(),
            h.pueue.display().to_string(),
            custom.display().to_string(),
        ),
    )
    .unwrap();
    secure_file(&h.policy());
    let global = load_existing_policy(&h.input()).unwrap();
    let mut project = h.project("project-a");
    project.root_path = project_root;
    let config = custom_config("project-a", &custom.to_string_lossy(), NetworkMode::Enabled);

    assert!(matches!(
        resolve_project_policy(&global, &project, &config),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::RootChanged,
            stage: pueue_agent::execution_policy::PolicyViolationStage::Startup,
            ..
        })
    ));
}

#[test]
fn weak_existing_policy_fails_closed_without_repairing_mode() {
    let h = PolicyHarness::new();
    fs::write(h.policy(), b"version = 1\n").unwrap();
    weak_file(&h.policy());
    let before = policy_mode(&h.policy());
    assert!(matches!(
        load_existing_policy(&h.input()),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::PolicyWeakPermissions,
            ..
        })
    ));
    assert_eq!(policy_mode(&h.policy()), before);
}

#[test]
fn unknown_policy_fields_fail_closed() {
    let h = PolicyHarness::new();
    fs::write(h.policy(), b"version = 1\nunknown = true\n").unwrap();
    secure_file(&h.policy());
    assert!(matches!(
        load_existing_policy(&h.input()),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::PolicyUnknownField,
            ..
        })
    ));
}

#[test]
fn existing_policy_requires_an_explicit_supported_version() {
    let h = PolicyHarness::new();
    fs::write(h.policy(), b"[defaults]\nnetwork = \"enabled\"\n").unwrap();
    secure_file(&h.policy());
    assert!(matches!(
        load_existing_policy(&h.input()),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::PolicyUnknownField,
            ..
        })
    ));
}

#[test]
fn project_root_parent_replacement_fails_closed_even_when_root_identity_is_preserved() {
    let h = PolicyHarness::new();
    let parent = h.path("root-parent");
    let nested_root = parent.join("project");
    fs::create_dir(&parent).unwrap();
    secure_directory(&parent);
    fs::create_dir(&nested_root).unwrap();
    secure_directory(&nested_root);

    let anchor = pueue_agent::execution_policy::ProjectRootAnchor::resolve(&nested_root).unwrap();
    let old_parent = h.path("root-parent-old");
    fs::rename(&parent, &old_parent).unwrap();
    fs::create_dir(&parent).unwrap();
    secure_directory(&parent);
    fs::rename(old_parent.join("project"), &nested_root).unwrap();
    assert!(matches!(
        anchor.verify_identity(),
        Err(pueue_agent::execution_policy::PolicyViolation {
            code: PolicyViolationCode::RootChanged,
            ..
        })
    ));
}

fn secure_directory(path: &Path) {
    set_mode(path, 0o700);
}

fn secure_executable(path: &Path) {
    set_mode(path, 0o700);
}

fn secure_file(path: &Path) {
    set_mode(path, 0o600);
}

fn weak_file(path: &Path) {
    set_mode(path, 0o644);
}

fn policy_mode(path: &Path) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::metadata(path).unwrap().permissions().mode()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        0
    }
}

fn set_mode(path: &Path, mode: u32) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions).unwrap();
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
    }
}

fn campaign_limit_default(field: &str) -> u32 {
    match field {
        "max_parallel_experiments" => 1,
        "max_new_experiments_per_24h" => 24,
        "max_agent_runs_per_hour" => 6,
        "max_code_change_proposals_per_24h" => 10,
        "max_same_spec_retries" => 2,
        "max_repairs_per_failure_fingerprint" => 2,
        "max_proposals_per_cycle" => 1,
        "observer_interval_minutes" => 30,
        "max_decision_attempts_per_cycle" => 3,
        "max_decision_wait_minutes" => 1_440,
        _ => unreachable!(),
    }
}
