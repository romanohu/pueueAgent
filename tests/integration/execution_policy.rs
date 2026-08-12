use std::{
    fs,
    path::{Path, PathBuf},
};

use pueue_agent::{
    execution_policy::{
        load_existing_policy, load_or_create_policy, NetworkMode, PolicyLoadInput,
        PolicyViolationCode, StartupEnvironment,
    },
};
use tempfile::{tempdir, TempDir};

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
        self.temp.path().join(name)
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
