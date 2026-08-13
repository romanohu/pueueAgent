#![cfg(unix)]

use std::{
    ffi::OsString,
    fs,
    os::unix::fs::{symlink, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use pueue_agent::{
    environment::SanitizedEnvironment,
    execution_policy::{
        ExecutableAnchor, PolicyViolationCode, PolicyViolationStage, ProjectRootAnchor,
        VerifiedProjectRoot,
    },
    native_launcher::{NativeLaunchSpec, NativeLauncher},
    project_logs::{create_gate_marker, ensure_agent_log_dir, ProjectRootLogReader},
};
use tempfile::tempdir;

const LOG: &str = ".pueue-agent/logs/fixture.log";
const MARKER: &str = ".pueue-agent/logs/fixture.authorized";

struct Harness {
    temporary: tempfile::TempDir,
    launcher: ExecutableAnchor,
    target: ExecutableAnchor,
    root: VerifiedProjectRoot,
}

impl Harness {
    fn new() -> Self {
        let temporary = tempdir().expect("temporary project root");
        let launcher_path = temporary.path().join("pueue-agent-launcher");
        fs::copy(env!("CARGO_BIN_EXE_pueue-agent"), &launcher_path)
            .expect("copy native launcher fixture");
        fs::set_permissions(&launcher_path, fs::Permissions::from_mode(0o700))
            .expect("set launcher mode");
        let launcher = ExecutableAnchor::from_absolute(
            &fs::canonicalize(&launcher_path).expect("canonical launcher"),
            &[],
        )
        .expect("launcher anchor");

        let source = temporary.path().join("generated-native-agent.rs");
        let target_path = temporary.path().join("generated-native-agent");
        fs::write(
            &source,
            r#"use std::{env, fs, thread, time::Duration};
fn main() {
    let mut args = env::args_os();
    let _program = args.next();
    let started = args.next().expect("started path");
    fs::write(started, b"started").expect("write started marker");
    println!("fixture-stdout");
    eprintln!("fixture-stderr");
    if args.next().as_deref() == Some(std::ffi::OsStr::new("hold")) {
        thread::sleep(Duration::from_secs(30));
    }
}"#,
        )
        .expect("write generated fixture source");
        let output = Command::new("rustc")
            .args(["--edition=2021", "-o"])
            .arg(&target_path)
            .arg(&source)
            .output()
            .expect("compile generated fixture");
        assert!(
            output.status.success(),
            "generated fixture failed to compile: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let target = ExecutableAnchor::from_absolute(
            &fs::canonicalize(&target_path).expect("canonical target"),
            &[],
        )
        .expect("target anchor");

        let root_anchor = ProjectRootAnchor::resolve(
            &fs::canonicalize(temporary.path()).expect("canonical project root"),
        )
        .expect("project root anchor");
        let root = root_anchor.verify_identity().expect("verified project root");

        Self {
            temporary,
            launcher,
            target,
            root,
        }
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.temporary.path().join(relative)
    }

    fn spec(&self) -> NativeLaunchSpec {
        NativeLaunchSpec {
            launcher: self.launcher.clone(),
            executable: self.target.clone(),
            argv: vec![
                OsString::from("generated-native-agent"),
                self.path("target-started").into_os_string(),
            ],
            cwd: Some(self.root.anchor.canonical_path.clone()),
            environment: SanitizedEnvironment::default(),
            project_root: self.root.try_clone().expect("clone verified root"),
            relative_log_path: PathBuf::from(LOG),
            relative_marker_path: PathBuf::from(MARKER),
        }
    }

    fn reader(&self) -> ProjectRootLogReader {
        ProjectRootLogReader::from_verified(self.root.try_clone().expect("clone verified root"))
    }
}

async fn assert_pre_marker_authorization_failure(
    child: &mut pueue_agent::native_launcher::NativeAgentChild,
    expected_code: PolicyViolationCode,
) {
    let error = child
        .authorize_marker()
        .await
        .expect_err("replaced authorization dependency must reject");
    match error {
        pueue_agent::AppError::PolicyViolation { violation } => {
            assert_eq!(violation.code, expected_code);
            assert_eq!(violation.stage, PolicyViolationStage::RunBoundPreMarker);
        }
        _ => panic!("replacement lost policy classification"),
    }
    assert!(
        tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .is_ok(),
        "authorization failure did not reap the blocked process group"
    );
}

#[tokio::test]
async fn durable_marker_precedes_release_and_agent_output_uses_verified_log() {
    let harness = Harness::new();
    let mut child = NativeLauncher::spawn(harness.spec()).expect("spawn blocked agent");

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!harness.path("target-started").exists());
    assert!(!harness.path(MARKER).exists());

    child.authorize_marker().await.expect("authorize marker");
    assert!(child.is_authorized());
    assert!(child.wait().await.expect("wait for target").success());

    assert_eq!(
        fs::read(harness.path(MARKER)).expect("read marker"),
        b"authorized\n"
    );
    let marker_metadata = fs::metadata(harness.path(MARKER)).expect("marker metadata");
    assert_eq!(marker_metadata.permissions().mode() & 0o777, 0o600);
    assert_eq!(marker_metadata.uid(), unsafe { libc::geteuid() });
    assert_eq!(
        fs::read(harness.path("target-started")).expect("target output"),
        b"started"
    );
    let log = fs::read(harness.path(LOG)).expect("agent log");
    assert!(log
        .windows(b"fixture-stdout".len())
        .any(|v| v == b"fixture-stdout"));
    assert!(log
        .windows(b"fixture-stderr".len())
        .any(|v| v == b"fixture-stderr"));
}

#[tokio::test]
async fn existing_marker_rejects_before_target_creation() {
    let harness = Harness::new();
    let reader = harness.reader();
    ensure_agent_log_dir(&reader).expect("agent log directory");
    create_gate_marker(&reader, Path::new(MARKER)).expect("pre-existing marker");

    let error = match NativeLauncher::spawn(harness.spec()) {
        Ok(_) => panic!("existing marker unexpectedly allowed a child"),
        Err(error) => error,
    };
    match error {
        pueue_agent::AppError::PolicyViolation { violation } => {
            assert_eq!(violation.stage, PolicyViolationStage::PostMarker);
        }
        _ => panic!("existing marker lost post-marker classification"),
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!harness.path("target-started").exists());
    assert_eq!(
        fs::read(harness.path(MARKER)).expect("marker retained"),
        b"authorized\n"
    );
}

#[tokio::test]
async fn published_marker_then_directory_swap_is_post_marker_and_reaps() {
    let harness = Harness::new();
    let mut child = NativeLauncher::spawn(harness.spec()).expect("spawn blocked agent");
    let reader = harness.reader();
    create_gate_marker(&reader, Path::new(MARKER)).expect("publish marker in bound generation");
    let current = harness.path(".pueue-agent/logs");
    let retired = harness.path(".pueue-agent/retired-logs");
    fs::rename(&current, &retired).expect("retire published generation");
    fs::create_dir(&current).expect("create replacement generation");
    fs::set_permissions(&current, fs::Permissions::from_mode(0o700))
        .expect("set replacement permissions");

    match child
        .authorize_marker()
        .await
        .expect_err("published generation swap must reject")
    {
        pueue_agent::AppError::PolicyViolation { violation } => {
            assert_eq!(violation.stage, PolicyViolationStage::PostMarker);
        }
        _ => panic!("published generation swap lost post-marker classification"),
    }
    assert_eq!(
        fs::read(retired.join("fixture.authorized")).expect("retained published marker"),
        b"authorized\n"
    );
    assert!(!harness.path("target-started").exists());
    assert!(
        tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .is_ok(),
        "published generation swap did not reap the blocked process group"
    );
}

#[tokio::test]
async fn unsafe_log_symlink_rejects_before_target_creation() {
    let harness = Harness::new();
    let reader = harness.reader();
    ensure_agent_log_dir(&reader).expect("agent log directory");
    fs::write(harness.path("outside.log"), b"sentinel").expect("outside sentinel");
    symlink(harness.path("outside.log"), harness.path(LOG)).expect("unsafe log symlink");

    assert!(NativeLauncher::spawn(harness.spec()).is_err());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!harness.path("target-started").exists());
    assert!(!harness.path(MARKER).exists());
    assert_eq!(
        fs::read(harness.path("outside.log")).expect("outside sentinel"),
        b"sentinel"
    );
}

#[tokio::test]
async fn duplicate_authorization_terminates_the_running_group() {
    let harness = Harness::new();
    let mut spec = harness.spec();
    spec.argv.push(OsString::from("hold"));
    let mut child = NativeLauncher::spawn(spec).expect("spawn blocked agent");
    child.authorize_marker().await.expect("first authorization");

    assert!(child.authorize_marker().await.is_err());
    let outcome = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
    if outcome.is_err() {
        child.terminate().await;
    }
    assert!(
        outcome.is_ok(),
        "duplicate authorization did not terminate the process group"
    );
}

#[tokio::test]
async fn target_replacement_after_spawn_fails_before_marker_and_reaps() {
    let harness = Harness::new();
    let mut child = NativeLauncher::spawn(harness.spec()).expect("spawn blocked agent");
    let retired = harness.path("retired-target");
    fs::rename(&harness.target.canonical_path, &retired).expect("retire target inode");
    fs::copy(&retired, &harness.target.canonical_path).expect("replace target inode");

    assert_pre_marker_authorization_failure(&mut child, PolicyViolationCode::AnchorReplaced).await;
    assert!(!harness.path(MARKER).exists());
    assert!(!harness.path("target-started").exists());
}

#[tokio::test]
async fn log_replacement_after_spawn_fails_before_marker_and_reaps() {
    let harness = Harness::new();
    let mut child = NativeLauncher::spawn(harness.spec()).expect("spawn blocked agent");
    fs::rename(harness.path(LOG), harness.path("retired.log")).expect("retire opened log");
    fs::write(harness.path(LOG), b"replacement").expect("replace log inode");
    fs::set_permissions(harness.path(LOG), fs::Permissions::from_mode(0o600))
        .expect("owner-only replacement log");

    assert_pre_marker_authorization_failure(&mut child, PolicyViolationCode::LogUnsafe).await;
    assert!(!harness.path(MARKER).exists());
    assert!(!harness.path("target-started").exists());
}

#[tokio::test]
async fn log_directory_replacement_after_spawn_fails_before_marker_and_reaps() {
    let harness = Harness::new();
    let mut child = NativeLauncher::spawn(harness.spec()).expect("spawn blocked agent");
    let log_directory = harness.path(".pueue-agent/logs");
    let retired = harness.path(".pueue-agent/retired-logs");
    fs::rename(&log_directory, &retired).expect("retire log directory");
    fs::create_dir(&log_directory).expect("replace log directory");
    fs::set_permissions(&log_directory, fs::Permissions::from_mode(0o700))
        .expect("owner-only replacement directory");
    fs::hard_link(retired.join("fixture.log"), log_directory.join("fixture.log"))
        .expect("preserve log inode across directory replacement");

    assert_pre_marker_authorization_failure(&mut child, PolicyViolationCode::LogUnsafe).await;
    assert!(!harness.path(MARKER).exists());
    assert!(!harness.path("target-started").exists());
}

#[tokio::test]
async fn missing_log_directory_after_spawn_is_a_pre_marker_policy_failure() {
    let harness = Harness::new();
    let mut child = NativeLauncher::spawn(harness.spec()).expect("spawn blocked agent");
    let log_directory = harness.path(".pueue-agent/logs");
    let retired = harness.path(".pueue-agent/retired-logs");
    fs::rename(&log_directory, &retired).expect("retire log directory");

    assert_pre_marker_authorization_failure(&mut child, PolicyViolationCode::LogUnsafe).await;
    assert!(!harness.path(MARKER).exists());
    assert!(!harness.path("target-started").exists());
}

#[tokio::test]
async fn project_root_replacement_after_spawn_fails_before_marker_and_reaps() {
    let harness = Harness::new();
    let mut child = NativeLauncher::spawn(harness.spec()).expect("spawn blocked agent");
    let original = harness.temporary.path().to_path_buf();
    let retired = original.with_extension("retired-root");
    fs::rename(&original, &retired).expect("retire project root");
    fs::create_dir(&original).expect("replace project root");
    fs::set_permissions(&original, fs::Permissions::from_mode(0o700))
        .expect("owner-only replacement root");

    assert_pre_marker_authorization_failure(&mut child, PolicyViolationCode::RootChanged).await;
    assert!(!original.join(MARKER).exists());
    assert!(!original.join("target-started").exists());

    fs::remove_dir(&original).expect("remove replacement root");
    fs::rename(&retired, &original).expect("restore fixture root for cleanup");
}
