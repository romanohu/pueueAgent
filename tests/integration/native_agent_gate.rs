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
    environment::{PrivateRunTemp, SanitizedEnvironment},
    execution_policy::{
        AgentKind, ExecutableAnchor, NetworkMode, PolicyViolationCode, PolicyViolationStage,
        ProjectRootAnchor, ResolvedProjectExecutionPolicy, StartupEnvironment,
        VerifiedProjectRoot,
    },
    native_launcher::{NativeLaunchSpec, NativeLauncher},
    project_logs::{create_gate_marker, ensure_agent_log_dir, ProjectRootLogReader},
};
#[cfg(not(target_os = "linux"))]
use pueue_agent::{
    db::{AgentRunRepository, Db},
    execution_policy::preflight_decision_runtime,
};
use tempfile::tempdir;

const LOG: &str = ".pueue-agent/logs/fixture.log";
const MARKER: &str = ".pueue-agent/logs/fixture.authorized";

#[cfg(not(target_os = "linux"))]
#[test]
fn decision_output_capability_fails_before_agent_run_allocation() {
    let temporary = tempdir().expect("temporary database root");
    let db = Db::open(&temporary.path().join("state.sqlite3")).expect("open database");

    let error = preflight_decision_runtime().expect_err("non-Linux decision runtime must fail");

    assert_eq!(error.code, PolicyViolationCode::UnsupportedPlatform);
    assert_eq!(error.stage, PolicyViolationStage::PreBinding);
    assert!(AgentRunRepository::new(&db)
        .find_active_by_project("unallocated-project")
        .expect("query agent runs")
        .is_none());
}

struct Harness {
    temporary: tempfile::TempDir,
    launcher: ExecutableAnchor,
    target: ExecutableAnchor,
    root: VerifiedProjectRoot,
    private_temp: PrivateRunTemp,
}

impl Harness {
    fn new() -> Self {
        let temporary = tempdir().expect("temporary project root");
        fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700))
            .expect("secure native launcher fixture root");
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
    match args.next().as_deref() {
        Some(value) if value == std::ffi::OsStr::new("hold") => {
            thread::sleep(Duration::from_secs(30));
        }
        Some(value) if value == std::ffi::OsStr::new("write-temp") => {
            fs::write(std::path::PathBuf::from(env::var_os("TMPDIR").unwrap()).join("target-write"), b"ok")
                .expect("write through private temp descriptor");
        }
        _ => {}
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
        fs::set_permissions(&target_path, fs::Permissions::from_mode(0o700))
            .expect("secure generated fixture executable");
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

        let private_temp = PrivateRunTemp::create(&root, 1).expect("private run temp");
        Self {
            temporary,
            launcher,
            target,
            root,
            private_temp,
        }
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.temporary.path().join(relative)
    }

    fn spec(&self) -> NativeLaunchSpec {
        let policy = ResolvedProjectExecutionPolicy {
            project_id: "native-agent-gate".to_owned(),
            root_anchor: self.root.anchor.clone(),
            agent_anchor: self.target.clone(),
            agent_kind: AgentKind::Custom,
            network: NetworkMode::Disabled,
            agent_environment_allow: Default::default(),
            task_environment_allow: Default::default(),
            codex_home: self.path("codex-home"),
            trusted_path: Vec::new(),
            private_temp_relative_root: PathBuf::from(".pueue-agent/tmp"),
        };
        let environment = SanitizedEnvironment::for_custom_agent(
            &StartupEnvironment::from_pairs(std::iter::empty::<(&str, &str)>()),
            &policy,
            1,
        )
        .expect("sanitized agent environment");
        for name in ["TMPDIR", "TMP", "TEMP"] {
            assert_eq!(environment.get(name), Some(std::ffi::OsStr::new("/dev/fd/11")));
        }
        NativeLaunchSpec {
            launcher: self.launcher.clone(),
            executable: self.target.clone(),
            argv: vec![
                OsString::from("generated-native-agent"),
                self.path("target-started").into_os_string(),
            ],
            cwd: Some(self.root.anchor.canonical_path.clone()),
            environment,
            project_root: self.root.try_clone().expect("clone verified root"),
            log_root: self.root.try_clone().expect("clone verified log root"),
            relative_log_path: PathBuf::from(LOG),
            relative_marker_path: PathBuf::from(MARKER),
        }
    }

    fn reader(&self) -> ProjectRootLogReader {
        ProjectRootLogReader::from_verified(self.root.try_clone().expect("clone verified root"))
    }
}

#[tokio::test]
async fn private_temp_path_replacement_cannot_redirect_target_writes() {
    let mut harness = Harness::new();
    let mut spec = harness.spec();
    spec.argv.push(OsString::from("write-temp"));
    let mut child = NativeLauncher::spawn(spec, &harness.private_temp)
        .expect("spawn blocked temp writer");

    let replacement_path = harness.private_temp.path().to_path_buf();
    let old_generation = harness.path("retired-private-temp");
    fs::rename(&replacement_path, &old_generation).expect("retire private temp generation");
    fs::create_dir(&replacement_path).expect("create replacement private temp generation");
    fs::set_permissions(&replacement_path, fs::Permissions::from_mode(0o700))
        .expect("set replacement private temp permissions");
    fs::write(replacement_path.join("replacement-sentinel"), b"keep")
        .expect("write replacement sentinel");

    child.authorize_marker().await.expect("authorize target");
    assert!(child.wait().await.expect("wait for target").success());
    assert_eq!(fs::read(old_generation.join("target-write")).unwrap(), b"ok");
    assert!(!replacement_path.join("target-write").exists());

    let cleanup = harness
        .private_temp
        .cleanup_contents_before(None)
        .expect("clean retired private temp generation through retained descriptor");
    assert_eq!(cleanup.entries_removed, 1);
    assert!(!old_generation.join("target-write").exists());
    assert_eq!(
        fs::read(replacement_path.join("replacement-sentinel")).unwrap(),
        b"keep"
    );
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
    let mut child = NativeLauncher::spawn(harness.spec(), &harness.private_temp)
        .expect("spawn blocked agent");

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

    let error = match NativeLauncher::spawn(harness.spec(), &harness.private_temp) {
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
    let mut child = NativeLauncher::spawn(harness.spec(), &harness.private_temp)
        .expect("spawn blocked agent");
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

    assert!(NativeLauncher::spawn(harness.spec(), &harness.private_temp).is_err());
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
    let mut child = NativeLauncher::spawn(spec, &harness.private_temp)
        .expect("spawn blocked agent");
    child.authorize_marker().await.expect("first authorization");

    assert!(child.authorize_marker().await.is_err());
    let outcome = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
    if outcome.is_err() {
        child.terminate().await.unwrap();
    }
    assert!(
        outcome.is_ok(),
        "duplicate authorization did not terminate the process group"
    );
}

#[tokio::test]
async fn target_replacement_after_spawn_fails_before_marker_and_reaps() {
    let harness = Harness::new();
    let mut child = NativeLauncher::spawn(harness.spec(), &harness.private_temp)
        .expect("spawn blocked agent");
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
    let mut child = NativeLauncher::spawn(harness.spec(), &harness.private_temp)
        .expect("spawn blocked agent");
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
    let mut child = NativeLauncher::spawn(harness.spec(), &harness.private_temp)
        .expect("spawn blocked agent");
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
    let mut child = NativeLauncher::spawn(harness.spec(), &harness.private_temp)
        .expect("spawn blocked agent");
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
    let mut child = NativeLauncher::spawn(harness.spec(), &harness.private_temp)
        .expect("spawn blocked agent");
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
