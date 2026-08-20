#[path = "../support/fake_pueue.rs"]
mod fake_pueue;
#[cfg(unix)]
#[path = "../support/config_read_barrier.rs"]
mod config_read_barrier;
#[cfg(all(unix, debug_assertions))]
#[path = "../support/native_process_fixture.rs"]
mod native_process_fixture;

use std::{
    ffi::OsString,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;

#[cfg(all(unix, debug_assertions))]
use std::time::{Duration, Instant};

#[cfg(all(unix, debug_assertions))]
use std::process::Command;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use fake_pueue::{FakePueue, FakePueueCommand};
#[cfg(unix)]
use config_read_barrier::ConfigReadBarrier;
#[cfg(all(unix, debug_assertions))]
use native_process_fixture::{
    NativeBehavior, NativeFakePueue, OUTPUT_SENTINEL, OVERFLOW_SENTINEL_REPETITIONS,
    TIMEOUT_SENTINEL_REPETITIONS,
};
use pueue_agent::{
    batches::{self, BatchJobResult},
    campaign::CampaignCoordinator,
    db::{
        BatchRepository, CampaignRepository, Db, EventRepository, ExperimentRepository,
        ManagedSubmissionIntent, ProjectRepository, StartCampaignRequest, SubmissionRepository,
    },
    execution_policy::{CampaignLimits, ProjectRootAnchor},
    models::{
        AgentRunStatus, EventKind, NewAgentRun, NewBatchJob, NewBatchRequest, NewEvent, NewProject,
        ExperimentStatus, ProposalKind, Submission, SubmissionKind, SubmissionStatus,
    },
    proposals::{self, ProposalInput},
    pueue::{configured_pueue, validate_add_argv, PueueApi, PueueError, PueueTask, PUEUE_TIMEOUT},
    pueue_security::{validate_group, MAX_PUEUE_OUTPUT_BYTES},
    state::{self, ObjectiveSnapshot},
    submit, AppError,
};
use pueue_agent::process::MAX_FIELD_SIZE;
#[cfg(all(unix, debug_assertions))]
use pueue_agent::pueue_process::PueueProcessRunner;
#[cfg(unix)]
use pueue_agent::execution_policy::{
    load_or_create_policy, PolicyLoadInput, PolicyViolation, PolicyViolationCode,
    PolicyViolationStage, StartupEnvironment,
};
use serde_json::json;
use tempfile::TempDir;

fn accepts_api<P: PueueApi>(_api: &P) {}

#[test]
fn shared_fake_preserves_the_pueue_api_contract() {
    accepts_api(&FakePueue::new());
}

#[test]
fn pueue_process_runner_has_a_fail_closed_non_unix_contract() {
    let source = include_str!("../../src/pueue_process.rs");
    assert!(source.contains("#[cfg(not(unix))]\n    pub(crate) async fn run_with_environment"));
    assert!(source.contains("operation: \"run verified Pueue on this platform\""));
}

#[test]
fn production_pueue_limits_are_exact() {
    assert_eq!(PUEUE_TIMEOUT, std::time::Duration::from_secs(30));
    assert_eq!(MAX_PUEUE_OUTPUT_BYTES, 65_536);
}

#[test]
fn native_add_preflight_accepts_a_shape_valid_small_request() {
    assert!(validate_add_argv(&[
        OsString::from("-g"),
        OsString::from("pa-project"),
        OsString::from("--"),
        OsString::from("python"),
    ])
    .is_ok());
}

#[test]
fn native_add_preflight_accepts_the_last_conservative_frame_and_rejects_the_next_byte() {
    let (prefix_bytes, tail_bytes) = native_add_byte_boundary();
    let accepted = native_add_byte_boundary_args(prefix_bytes, tail_bytes);
    let rejected = native_add_byte_boundary_args(prefix_bytes, tail_bytes + 1);

    assert!(validate_add_argv(&accepted).is_ok());
    assert!(validate_add_argv(&rejected).is_err());
}

fn native_add_byte_boundary() -> (usize, usize) {
    let add_args = |prefix_bytes, tail_bytes| {
        native_add_byte_boundary_args(prefix_bytes, tail_bytes)
    };
    let mut low = 0;
    let mut high = MAX_FIELD_SIZE;
    while low < high {
        let middle = low + (high - low + 1) / 2;
        if validate_add_argv(&add_args(middle, middle)).is_ok() {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let mut tail_low = 0;
    let mut tail_high = MAX_FIELD_SIZE;
    while tail_low < tail_high {
        let middle = tail_low + (tail_high - tail_low + 1) / 2;
        if validate_add_argv(&add_args(low, middle)).is_ok() {
            tail_low = middle;
        } else {
            tail_high = middle - 1;
        }
    }

    (low, tail_low)
}

fn native_add_byte_boundary_args(prefix_bytes: usize, tail_bytes: usize) -> Vec<OsString> {
    let mut args = vec![
        OsString::from("-g"),
        OsString::from("pa-project"),
        OsString::from("--"),
    ];
    args.extend((0..31).map(|_| OsString::from("x".repeat(prefix_bytes))));
    args.push(OsString::from("x".repeat(tail_bytes)));
    args
}

fn native_add_byte_boundary_command(rejected: bool) -> Vec<OsString> {
    let (prefix_bytes, tail_bytes) = native_add_byte_boundary();
    native_add_byte_boundary_args(prefix_bytes, tail_bytes + usize::from(rejected))[3..].to_vec()
}

fn source_before_test_module(source: &str) -> &str {
    source.split("\n#[cfg(test)]").next().unwrap_or(source)
}

#[test]
fn pueue_control_sources_forbid_bare_and_shell_execution() {
    let adapter_source = include_str!("../../src/pueue.rs");
    let factory_and_backend = adapter_source
        .split("\n#[cfg(test)]\nimpl CommandPueue {")
        .next()
        .expect("adapter source must include the canonical factory and backend");
    let api_impl_start = adapter_source
        .find("\n#[async_trait]\nimpl PueueApi for CommandPueue {")
        .expect("adapter source must include its PueueApi implementation");
    let api_impl = &adapter_source[api_impl_start..];
    assert!(factory_and_backend.contains("pub fn configured_pueue"));
    assert!(factory_and_backend.contains("async fn execute"));
    assert!(api_impl.contains("async fn status_json"));
    assert!(api_impl.contains("async fn ensure_group"));
    let sources = [
        ("pueue factory/backend", factory_and_backend),
        ("pueue API", api_impl),
        (
            "pueue_process",
            source_before_test_module(include_str!("../../src/pueue_process.rs")),
        ),
        (
            "pueue_security",
            source_before_test_module(include_str!("../../src/pueue_security.rs")),
        ),
        ("main", source_before_test_module(include_str!("../../src/main.rs"))),
        ("submit", source_before_test_module(include_str!("../../src/submit.rs"))),
        ("batches", source_before_test_module(include_str!("../../src/batches.rs"))),
        ("cancel", source_before_test_module(include_str!("../../src/cancel.rs"))),
        (
            "reconcile",
            source_before_test_module(include_str!("../../src/reconcile.rs")),
        ),
        (
            "termination",
            source_before_test_module(include_str!("../../src/termination.rs")),
        ),
        (
            "diagnostics",
            source_before_test_module(include_str!("../../src/diagnostics.rs")),
        ),
        (
            "service",
            source_before_test_module(include_str!("../../src/service.rs")),
        ),
    ];
    let forbidden = [
        "Command::new(\"pueue\")",
        "Command::new(\"/bin/sh\")",
        "/bin/sh",
        "sh -c",
        "command -v",
    ];

    for (name, source) in &sources {
        for pattern in forbidden {
            assert!(
                !source.contains(pattern),
                "production Pueue control source {name} contains forbidden {pattern:?}"
            );
        }
        assert!(
            !source
                .split(|character: char| !character.is_ascii_alphabetic())
                .any(|word| word == "eval"),
            "production Pueue control source {name} contains a shell eval token"
        );
    }
    for (name, source) in sources
        .iter()
        .filter(|(name, _)| !name.starts_with("pueue "))
    {
        assert!(
            !source.contains("CommandPueue"),
            "production Pueue control source {name} bypasses configured_pueue"
        );
    }
    assert_eq!(
        factory_and_backend
            .lines()
            .filter(|line| {
                let line = line.trim_start();
                line.contains("Ok(CommandPueue {")
            })
            .count(),
        1,
        "only configured_pueue may construct CommandPueue"
    );
    assert!(factory_and_backend.contains("Ok(CommandPueue {"));
}

#[cfg(all(unix, debug_assertions))]
static NATIVE_PROCESS_FIXTURE_LOCK: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

#[cfg(all(unix, debug_assertions))]
#[tokio::test]
async fn configured_adapter_uses_only_the_startup_pinned_pueue_and_config_descriptor() {
    let _guard = NATIVE_PROCESS_FIXTURE_LOCK.lock().await;
    let fixture = NativeFakePueue::new(NativeBehavior::ExactLimitSuccess);
    let adapter = configured_pueue(fixture.policy()).unwrap();

    adapter.kill(41).await.unwrap();

    assert_eq!(
        fixture.captured_argv().await,
        vec![
            b"--config".to_vec(),
            b"/dev/fd/9".to_vec(),
            b"kill".to_vec(),
            b"41".to_vec(),
        ]
    );
    assert_eq!(fixture.captured_config().await, b"fixture-config-fd9\n");
    fixture.wait_for_processes_gone().await;
}

#[cfg(all(unix, debug_assertions))]
#[tokio::test]
async fn ambient_path_replacement_cannot_override_the_pinned_pueue_executable() {
    let _guard = NATIVE_PROCESS_FIXTURE_LOCK.lock().await;
    let ambient = TempDir::new().unwrap();
    let marker = ambient.path().join("ambient-pueue-ran");
    let source = ambient.path().join("replacement.rs");
    fs::write(
        &source,
        format!("fn main() {{ std::fs::write({:?}, b\"ran\").unwrap(); }}", marker),
    )
    .unwrap();
    let replacement = ambient.path().join("pueue");
    let output = Command::new("rustc")
        .args(["--edition=2021", "-O", "-o"])
        .arg(&replacement)
        .arg(&source)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    fs::set_permissions(&replacement, fs::Permissions::from_mode(0o700)).unwrap();
    let fixture = NativeFakePueue::new_with_ambient_path(
        NativeBehavior::ExactLimitSuccess,
        Some(ambient.path()),
    );
    let adapter = configured_pueue(fixture.policy()).unwrap();

    adapter.kill(41).await.unwrap();

    assert!(!marker.exists());
    assert_eq!(
        fixture.captured_argv().await.first(),
        Some(&b"--config".to_vec())
    );
    fixture.wait_for_processes_gone().await;
}

#[cfg(all(unix, debug_assertions))]
#[tokio::test]
async fn adapter_rejects_replaced_pinned_config_before_native_execution() {
    let _guard = NATIVE_PROCESS_FIXTURE_LOCK.lock().await;
    let fixture = NativeFakePueue::new(NativeBehavior::ExactLimitSuccess);
    let adapter = configured_pueue(fixture.policy()).unwrap();
    fixture.replace_pinned_config();

    let error = adapter.status_json().await.unwrap_err();

    assert!(matches!(
        error,
        AppError::PolicyViolation {
            violation: PolicyViolation {
                code: PolicyViolationCode::AnchorReplaced,
                stage: PolicyViolationStage::RunBoundPreMarker,
                ..
            }
        }
    ));
    fixture.assert_no_execution_artifacts();
}

const STATUS_JSON: &str = r#"{
  "tasks": {
    "41": {
      "id": "41",
      "group": "pa-project",
      "command": "python train.py --name experiment",
      "status": {
        "Done": {
          "enqueued_at": "2026-08-09T10:00:00Z",
          "start": "2026-08-09T10:00:03Z",
          "end": "2026-08-09T10:05:00Z",
          "result": {"Failed": 17}
        }
      }
    }
  }
}"#;

#[cfg(all(unix, debug_assertions))]
async fn run_native_failure(
    behavior: NativeBehavior,
    timeout: std::time::Duration,
) -> (NativeFakePueue, AppError) {
    let fixture = NativeFakePueue::new(behavior);
    let policy = fixture.policy();
    let runner = PueueProcessRunner::with_limits(timeout, MAX_PUEUE_OUTPUT_BYTES);
    let run = tokio::spawn(async move {
        runner
            .run(
                policy.as_ref(),
                &[
                    OsString::from("status"),
                    OsString::from("--json"),
                ],
            )
            .await
    });
    fixture.wait_until_ready().await;
    fixture.assert_started_processes_alive().await;
    let error = tokio::time::timeout(std::time::Duration::from_secs(8), run)
        .await
        .expect("native Pueue runner exceeded outer test bound")
        .expect("native Pueue runner task panicked")
        .expect_err("native Pueue failure fixture unexpectedly succeeded");
    (fixture, error)
}

#[cfg(all(unix, debug_assertions))]
async fn assert_native_cleanup_contract(fixture: &NativeFakePueue) {
    assert_native_launch_contract(fixture).await;
    assert_eq!(fixture.term_observation().await, b"pipe-open");
    fixture.wait_for_processes_gone().await;
}

#[cfg(all(unix, debug_assertions))]
async fn assert_native_launch_contract(fixture: &NativeFakePueue) {
    assert_eq!(
        fixture.captured_argv().await,
        vec![
            b"--config".to_vec(),
            b"/dev/fd/9".to_vec(),
            b"status".to_vec(),
            b"--json".to_vec(),
        ]
    );
    assert_eq!(fixture.captured_config().await, b"fixture-config-fd9\n");
}

#[cfg(all(unix, debug_assertions))]
#[tokio::test]
async fn pueue_timeout_terminates_the_process_group() {
    let _guard = NATIVE_PROCESS_FIXTURE_LOCK.lock().await;
    let (fixture, error) = run_native_failure(
        NativeBehavior::HoldWithSentinel,
        std::time::Duration::from_secs(2),
    )
    .await;

    assert!(matches!(
        &error,
        AppError::Pueue(PueueError::Timeout {
            operation: "status"
        })
    ));
    assert_native_error_does_not_render_fixture_output(
        &error,
        "Pueue status timed out",
        TIMEOUT_SENTINEL_REPETITIONS,
    );
    assert_native_cleanup_contract(&fixture).await;
}

#[cfg(all(unix, debug_assertions))]
#[tokio::test]
async fn pueue_launch_phases_share_one_absolute_deadline() {
    let _guard = NATIVE_PROCESS_FIXTURE_LOCK.lock().await;
    let fixture = NativeFakePueue::delays(150, 150, 150);
    let policy = fixture.policy();
    let started = Instant::now();

    let error = PueueProcessRunner::with_limits(Duration::from_millis(300), MAX_PUEUE_OUTPUT_BYTES)
        .run(
            policy.as_ref(),
            &[OsString::from("status"), OsString::from("--json")],
        )
        .await
        .expect_err("cumulative launch delays unexpectedly fit the operation deadline");

    assert!(matches!(error, AppError::Pueue(PueueError::Timeout { .. })));
    assert!(started.elapsed() < Duration::from_millis(700));
    fixture.assert_helper_lifecycle_deadline_started().await;
}

#[cfg(all(unix, debug_assertions))]
#[tokio::test]
async fn stdout_overflow_terminates_and_reaps_the_process_group() {
    let _guard = NATIVE_PROCESS_FIXTURE_LOCK.lock().await;
    let (fixture, error) = run_native_failure(
        NativeBehavior::StdoutOverflowWithSentinel,
        std::time::Duration::from_secs(2),
    )
    .await;

    assert!(matches!(
        &error,
        AppError::Pueue(PueueError::OutputLimit {
            operation: "status",
            stream: "stdout"
        })
    ));
    assert_native_error_does_not_render_fixture_output(
        &error,
        "Pueue status output limit exceeded for stdout",
        OVERFLOW_SENTINEL_REPETITIONS,
    );
    assert_native_cleanup_contract(&fixture).await;
}

#[cfg(all(unix, debug_assertions))]
#[tokio::test]
async fn stderr_overflow_terminates_and_reaps_the_process_group() {
    let _guard = NATIVE_PROCESS_FIXTURE_LOCK.lock().await;
    let (fixture, error) = run_native_failure(
        NativeBehavior::StderrOverflowWithSentinel,
        std::time::Duration::from_secs(2),
    )
    .await;

    assert!(matches!(
        &error,
        AppError::Pueue(PueueError::OutputLimit {
            operation: "status",
            stream: "stderr"
        })
    ));
    assert_native_error_does_not_render_fixture_output(
        &error,
        "Pueue status output limit exceeded for stderr",
        OVERFLOW_SENTINEL_REPETITIONS,
    );
    assert_native_cleanup_contract(&fixture).await;
}

#[cfg(all(unix, debug_assertions))]
fn assert_native_error_does_not_render_fixture_output(
    error: &AppError,
    expected: &str,
    fixture_repetitions: usize,
) {
    let display = error.to_string();
    let debug = format!("{error:?}");
    let fixture_output = OUTPUT_SENTINEL.repeat(fixture_repetitions);

    assert!(fixture_output.len() > 256);
    assert!(display.contains(expected), "unexpected display: {display}");
    assert!(!display.contains(OUTPUT_SENTINEL), "display leaked fixture marker: {display}");
    assert!(!debug.contains(OUTPUT_SENTINEL), "debug leaked fixture marker: {debug}");
    assert!(!display.contains(&fixture_output), "display leaked fixture output");
    assert!(!debug.contains(&fixture_output), "debug leaked fixture output");
}

#[cfg(all(unix, debug_assertions))]
#[tokio::test]
async fn cancelling_the_caller_still_terminates_and_reaps_the_process_group() {
    let _guard = NATIVE_PROCESS_FIXTURE_LOCK.lock().await;
    let fixture = NativeFakePueue::new(NativeBehavior::Hold);
    let policy = fixture.policy();
    let runner = PueueProcessRunner::with_limits(
        std::time::Duration::from_secs(8),
        MAX_PUEUE_OUTPUT_BYTES,
    );
    let caller = tokio::spawn(async move {
        runner
            .run(
                policy.as_ref(),
                &[
                    OsString::from("status"),
                    OsString::from("--json"),
                ],
            )
            .await
    });
    fixture.wait_until_ready().await;
    fixture.assert_started_processes_alive().await;

    caller.abort();
    assert!(
        caller
            .await
            .expect_err("caller abort unexpectedly completed")
            .is_cancelled()
    );

    assert_native_cleanup_contract(&fixture).await;
}

#[cfg(all(unix, debug_assertions))]
#[tokio::test]
async fn dual_stream_overflow_preserves_the_primary_failure_after_cleanup() {
    let _guard = NATIVE_PROCESS_FIXTURE_LOCK.lock().await;
    let (fixture, error) = run_native_failure(
        NativeBehavior::BothStreamsOverflow,
        std::time::Duration::from_secs(2),
    )
    .await;

    assert!(matches!(
        error,
        AppError::Pueue(PueueError::OutputLimit {
            operation: "status",
            stream: "stdout" | "stderr"
        })
    ));
    assert_native_launch_contract(&fixture).await;
    assert!(matches!(
        fixture.term_observation().await.as_slice(),
        b"pipe-open" | b"pipe-closed"
    ));
    fixture.wait_for_processes_gone().await;
}

#[cfg(all(unix, debug_assertions))]
#[tokio::test]
async fn verified_runner_accepts_both_streams_at_the_exact_limit() {
    let _guard = NATIVE_PROCESS_FIXTURE_LOCK.lock().await;
    let fixture = NativeFakePueue::new(NativeBehavior::ExactLimitSuccess);
    let policy = fixture.policy();
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(8),
        PueueProcessRunner::with_limits(
            std::time::Duration::from_secs(2),
            MAX_PUEUE_OUTPUT_BYTES,
        )
        .run(
            policy.as_ref(),
            &[OsString::from("status"), OsString::from("--json")],
        ),
    )
    .await
    .expect("exact-limit runner exceeded outer test bound")
    .expect("exact-limit runner failed");

    assert!(output.status.success());
    assert_eq!(output.stdout, vec![b'x'; 64 * 1024]);
    assert_eq!(output.stderr, vec![b'x'; 64 * 1024]);
    assert_native_launch_contract(&fixture).await;
    fixture.wait_for_processes_gone().await;
}

#[cfg(all(unix, debug_assertions))]
#[tokio::test]
async fn verified_runner_returns_bounded_output_for_nonzero_status() {
    let _guard = NATIVE_PROCESS_FIXTURE_LOCK.lock().await;
    let fixture = NativeFakePueue::new(NativeBehavior::NonzeroExit);
    let policy = fixture.policy();
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(8),
        PueueProcessRunner::with_limits(
            std::time::Duration::from_secs(2),
            MAX_PUEUE_OUTPUT_BYTES,
        )
        .run(
            policy.as_ref(),
            &[OsString::from("status"), OsString::from("--json")],
        ),
    )
    .await
    .expect("nonzero runner exceeded outer test bound")
    .expect_err("nonzero runner unexpectedly succeeded");

    match error {
        AppError::Pueue(PueueError::CommandFailed {
            operation,
            exit_code,
            stdout,
            stderr,
        }) => {
            assert_eq!(operation, "status");
            assert_eq!(exit_code, Some(7));
            assert_eq!(stdout, vec![b'x'; 17]);
            assert_eq!(stderr, vec![b'x'; 17]);
        }
        other => panic!("unexpected runner error: {other:?}"),
    }
    assert_native_launch_contract(&fixture).await;
    fixture.wait_for_processes_gone().await;
}

#[test]
fn pueue_error_display_and_debug_redact_captured_bytes_and_spawn_sources() {
    const SENTINEL: &str = "/secret/pueue-config-SENTINEL";
    let spawn = PueueError::Spawn {
        operation: "status",
        source_kind: std::io::ErrorKind::NotFound,
    };
    let command_failed = PueueError::CommandFailed {
        operation: "status",
        exit_code: Some(7),
        stdout: SENTINEL.as_bytes().to_vec(),
        stderr: SENTINEL.as_bytes().to_vec(),
    };
    let invalid_task_id = PueueError::InvalidTaskId {
        stdout: SENTINEL.as_bytes().to_vec(),
    };

    let spawn_debug = format!("{spawn:?}");
    let command_debug = format!("{command_failed:?}");
    let task_id_debug = format!("{invalid_task_id:?}");
    assert!(spawn_debug.contains("source_kind: NotFound"));
    assert!(command_debug.contains(&format!("stdout_len: {}", SENTINEL.len())));
    assert!(command_debug.contains(&format!("stderr_len: {}", SENTINEL.len())));
    assert!(task_id_debug.contains(&format!("stdout_len: {}", SENTINEL.len())));

    let mut source_chain = String::new();
    let mut source = std::error::Error::source(&spawn);
    while let Some(error) = source {
        source_chain.push_str(&format!("{error} {error:?}"));
        source = error.source();
    }
    assert!(source_chain.is_empty(), "unexpected source chain: {source_chain}");
    assert!(
        !source_chain.contains(SENTINEL),
        "Error::source leaked: {source_chain}"
    );

    for error in [spawn, command_failed, invalid_task_id] {
        let display = error.to_string();
        let debug = format!("{error:?}");
        assert!(!display.contains(SENTINEL), "Display leaked: {display}");
        assert!(!debug.contains(SENTINEL), "Debug leaked: {debug}");
    }
}

#[test]
fn pueue_timeout_error_renders_a_bounded_status_without_fixture_output() {
    let credential_marker = "credential-marker-should-not-render";
    let fixture_output = format!("{credential_marker}{}", "x".repeat(512));
    let error = PueueError::Timeout {
        operation: "status",
    };
    let rendered = error.to_string();

    assert!(rendered.contains("Pueue status timed out"));
    assert!(!rendered.contains(&fixture_output));
    assert!(!rendered.contains(credential_marker));
    assert!(rendered.len() <= 256);
}

#[cfg(unix)]
struct CorePolicyHarness {
    _temp: TempDir,
    state_dir: PathBuf,
    project_root: PathBuf,
    trusted_dir: PathBuf,
    codex_home: PathBuf,
    launcher: PathBuf,
    config_path: PathBuf,
}

#[cfg(unix)]
impl CorePolicyHarness {
    fn new(config_under_project_root: bool, weak_config: bool) -> Self {
        let temp = TempDir::new().unwrap();
        let base = fs::canonicalize(temp.path()).unwrap();
        let state_dir = base.join("state");
        let project_root = base.join("project");
        let trusted_dir = base.join("trusted");
        let codex_home = base.join("codex-home");
        for directory in [&state_dir, &project_root, &trusted_dir, &codex_home] {
            fs::create_dir(directory).unwrap();
            secure_directory(directory);
        }

        let codex = trusted_dir.join("codex");
        let pueue = trusted_dir.join("pueue");
        let launcher = trusted_dir.join("launcher");
        for executable in [&codex, &pueue, &launcher] {
            fs::write(executable, b"generated fixture").unwrap();
            secure_executable(executable);
        }

        let config_path = if config_under_project_root {
            project_root.join("pueue.yml")
        } else {
            base.join("pueue.yml")
        };
        fs::write(&config_path, b"fixture: true\n").unwrap();
        if weak_config {
            fs::set_permissions(&config_path, fs::Permissions::from_mode(0o666)).unwrap();
        } else {
            secure_file(&config_path);
        }

        let policy_path = state_dir.join("execution-policy.toml");
        fs::write(
            &policy_path,
            format!(
                "version = 1\ntrusted_path = {:?}\n\n[executables]\ncodex = {:?}\npueue = {:?}\n",
                trusted_dir.display().to_string(),
                codex.display().to_string(),
                pueue.display().to_string(),
            ),
        )
        .unwrap();
        secure_file(&policy_path);

        Self {
            _temp: temp,
            state_dir,
            project_root,
            trusted_dir,
            codex_home,
            launcher,
            config_path,
        }
    }

    fn input(&self) -> PolicyLoadInput {
        PolicyLoadInput {
            state_dir: self.state_dir.clone(),
            project_roots: vec![self.project_root.clone()],
            inherited_path: self.trusted_dir.clone().into_os_string(),
            startup_environment: StartupEnvironment::from_pairs([("FIXTURE", "true")]),
            codex_home: self.codex_home.clone(),
            pueue_config: self.config_path.clone(),
            launcher_path: self.launcher.clone(),
        }
    }

    fn load_core_policy(
        &self,
    ) -> Result<pueue_agent::execution_policy::ResolvedExecutionPolicy, PolicyViolation> {
        load_or_create_policy(&self.input())
    }
}

#[cfg(unix)]
fn secure_directory(path: &std::path::Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

#[cfg(unix)]
fn secure_file(path: &std::path::Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

#[cfg(unix)]
fn secure_executable(path: &std::path::Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

#[derive(Clone)]
struct CountingFakePueue {
    inner: FakePueue,
    add_calls: Arc<AtomicUsize>,
}

impl CountingFakePueue {
    fn new() -> Self {
        Self {
            inner: FakePueue::new(),
            add_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn last_add_args(&self) -> Vec<OsString> {
        self.inner.last_add_args()
    }

    fn add_calls(&self) -> usize {
        self.add_calls.load(Ordering::SeqCst)
    }

    fn pause_add(&self) {
        self.inner.pause_add();
    }

    async fn wait_for_add(&self) {
        self.inner.wait_for_add().await;
    }

    fn release_add(&self) {
        self.inner.release_add();
    }
}

#[async_trait]
impl PueueApi for CountingFakePueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        self.inner.status_json().await
    }

    async fn add(&self, args: &[OsString]) -> Result<i64, AppError> {
        self.add_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.add(args).await
    }

    async fn kill(&self, task_id: i64) -> Result<(), AppError> {
        self.inner.kill(task_id).await
    }

    async fn remove(&self, task_id: i64) -> Result<(), AppError> {
        self.inner.remove(task_id).await
    }

    async fn ensure_group(&self, group: &str) -> Result<(), AppError> {
        self.inner.ensure_group(group).await
    }
}

struct SubmitHarness {
    _temp: TempDir,
    db: Db,
    root: PathBuf,
    fake: CountingFakePueue,
    initial_campaigns: i64,
    initial_experiments: i64,
    initial_submissions: i64,
}

impl SubmitHarness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project");
        let config_dir = root.join(".pueue-agent");
        fs::create_dir_all(&config_dir).unwrap();
        let config_path = config_dir.join("config.toml");
        fs::write(
            &config_path,
            r#"
project_id = "project-a"
pueue_group = "pa-project"

[agent]
program = "codex"
args = ["exec", "{prompt}"]
timeout_minutes = 60
max_retries = 2

[check]
interval_minutes = 10
deep_check_interval_minutes = 0
stall_minutes = 30
extra_log_paths = []

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 3
max_experiments = 20
"#,
        )
        .unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &root,
                "pa-project",
                config_path,
                1,
            ))
            .unwrap();
        Self {
            _temp: temp,
            db,
            root,
            fake: CountingFakePueue::new(),
            initial_campaigns: 0,
            initial_experiments: 0,
            initial_submissions: 0,
        }
    }

    fn with_objective(objective: &str) -> Self {
        let harness = Self::new();
        fs::write(
            harness.root.join(".pueue-agent/STATE.md"),
            format!("{objective}\n"),
        )
        .unwrap();
        harness
    }

    fn with_active_campaign() -> Self {
        let mut harness = Self::with_objective("Reach validation loss below 0.20");
        let _ = harness.reserve_baseline(&["python", "train.py"]);
        harness.initial_campaigns = harness.table_count("campaigns");
        harness.initial_experiments = harness.table_count("experiments");
        harness.initial_submissions = harness.table_count("submissions");
        harness
    }

    fn project(&self) -> pueue_agent::models::Project {
        ProjectRepository::new(&self.db)
            .find_by_root(&self.root)
            .unwrap()
            .unwrap()
    }

    fn objective(&self) -> ObjectiveSnapshot {
        state::load_objective(&self.root).unwrap()
    }

    fn reserve_baseline(&self, argv: &[&str]) -> ManagedSubmissionIntent {
        let objective = self.objective();
        let argv = argv.iter().map(|argument| (*argument).to_owned()).collect::<Vec<_>>();
        let baseline = proposals::validate_initial_baseline(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: "Establish the initial campaign baseline".to_owned(),
                source_experiment_id: None,
                argv: argv.clone(),
                working_directory: ".".to_owned(),
                expected_evidence: Vec::new(),
            },
            &objective.digest,
        )
        .unwrap();
        CampaignRepository::new(&self.db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id: &uuid::Uuid::new_v4().to_string(),
                    project_id: "project-a",
                    objective: &objective,
                    initial_argv: &argv,
                    baseline: &baseline,
                    submission_id: &uuid::Uuid::new_v4().to_string(),
                    experiment_id: &uuid::Uuid::new_v4().to_string(),
                    proposal_id: &uuid::Uuid::new_v4().to_string(),
                    metadata: &json!({}),
                    origin_agent_run_id: None,
                    now: 100,
                },
                &CampaignLimits::default(),
            )
            .unwrap()
    }

    async fn submit(&self, argv: &[&str]) -> Result<Submission, AppError> {
        let argv = argv.iter().map(OsString::from).collect::<Vec<_>>();
        submit::run_with_options(
            &self.db,
            &self.root,
            &argv,
            &submit::SubmitOptions::default(),
            &CampaignLimits::default(),
            &self.fake,
        )
        .await
    }

    async fn submit_from_agent_run(&self, argv: &[&str]) -> Result<Submission, AppError> {
        let event = EventRepository::new(&self.db)
            .insert_idempotent(&NewEvent::new(
                "project-a",
                EventKind::TaskFinished,
                format!("campaign-submit-origin-{}", uuid::Uuid::new_v4()),
                json!({}),
                101,
                101,
            ))?;
        let run = pueue_agent::db::AgentRunRepository::new(&self.db).insert(&NewAgentRun::new(
            "project-a",
            event.event_id,
            None,
            AgentRunStatus::Running,
            101,
            self.root.join("agent.log"),
        ))?;
        let argv = argv.iter().map(OsString::from).collect::<Vec<_>>();
        submit::run_with_options(
            &self.db,
            &self.root,
            &argv,
            &submit::SubmitOptions::new(SubmissionKind::Experiment, json!({}), Some(run.run_id)),
            &CampaignLimits::default(),
            &self.fake,
        )
        .await
    }

    fn live_campaigns(&self) -> i64 {
        self.table_count("campaigns") - self.initial_campaigns
    }

    fn experiments(&self) -> i64 {
        self.table_count("experiments") - self.initial_experiments
    }

    fn submissions(&self) -> i64 {
        self.table_count("submissions") - self.initial_submissions
    }

    fn table_count(&self, table: &str) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row.get(0))
            .unwrap()
    }

    fn pueue_add_calls(&self) -> usize {
        self.fake.add_calls()
    }
}

async fn submit_legacy_control<P: PueueApi + ?Sized>(
    db: &Db,
    project_root: &std::path::Path,
    args: &[OsString],
    pueue: &P,
) -> Result<Submission, AppError> {
    submit::run_with_options(
        db,
        project_root,
        args,
        &submit::SubmitOptions::new(SubmissionKind::Control, json!({}), None),
        &CampaignLimits::default(),
        pueue,
    )
    .await
}

struct TimeoutPueue {
    add_calls: AtomicUsize,
}

impl TimeoutPueue {
    fn new() -> Self {
        Self {
            add_calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl PueueApi for TimeoutPueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        Ok(Vec::new())
    }

    async fn add(&self, _args: &[OsString]) -> Result<i64, AppError> {
        self.add_calls.fetch_add(1, Ordering::SeqCst);
        Err(PueueError::Timeout { operation: "add" }.into())
    }

    async fn kill(&self, _task_id: i64) -> Result<(), AppError> {
        Ok(())
    }

    async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
        Ok(())
    }

    async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
        Ok(())
    }
}

fn expected_provisional_signature(group: &str, task_id: i64, submission_id: &str) -> String {
    format!("provisional-submit:v1:group={group}:task-id={task_id}:intent={submission_id}")
}

#[tokio::test]
async fn campaign_submit_first_experiment_creates_baseline_and_one_pueue_task() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");

    let result = harness.submit(&["python", "train.py"]).await.unwrap();

    assert_eq!(harness.live_campaigns(), 1);
    assert_eq!(harness.experiments(), 1);
    assert_eq!(harness.submissions(), 1);
    assert_eq!(harness.pueue_add_calls(), 1);
    assert_eq!(result.pueue_task_id, Some(41));
    assert_eq!(
        harness.fake.last_add_args(),
        vec![
            OsString::from("-g"),
            OsString::from("pa-project"),
            OsString::from("--working-directory"),
            fs::canonicalize(&harness.root).unwrap().into_os_string(),
            OsString::from("--"),
            OsString::from("python"),
            OsString::from("train.py"),
        ]
    );
}

#[tokio::test]
async fn campaign_submit_preserves_user_metadata_and_agent_origin_with_managed_ids() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFinished,
            "campaign-submit-metadata-origin",
            json!({}),
            101,
            101,
        ))
        .unwrap();
    let run = pueue_agent::db::AgentRunRepository::new(&harness.db)
        .insert(&NewAgentRun::new(
            "project-a",
            event.event_id,
            None,
            AgentRunStatus::Running,
            101,
            harness.root.join("agent.log"),
        ))
        .unwrap();
    let args = vec![OsString::from("python"), OsString::from("train.py")];

    let result = submit::run_with_options(
        &harness.db,
        &harness.root,
        &args,
        &submit::SubmitOptions::new(
            SubmissionKind::Experiment,
            json!({"trial":"baseline","epochs":3}),
            Some(run.run_id),
        ),
        &CampaignLimits::default(),
        &harness.fake,
    )
    .await
    .unwrap();

    let stored = SubmissionRepository::new(&harness.db)
        .find_by_id(&result.submission_id)
        .unwrap()
        .unwrap();
    let campaign = CampaignRepository::new(&harness.db)
        .find_live_by_project("project-a")
        .unwrap()
        .unwrap();
    let experiment = ExperimentRepository::new(&harness.db)
        .find_by_id(stored.metadata["experiment_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(stored.origin_agent_run_id, Some(run.run_id));
    assert_eq!(stored.metadata["trial"], "baseline");
    assert_eq!(stored.metadata["epochs"], 3);
    assert_eq!(stored.metadata["campaign_id"], campaign.campaign_id);
    assert_eq!(stored.metadata["proposal_id"], experiment.proposal_id);
    assert_eq!(stored.metadata["experiment_id"], experiment.experiment_id);
    assert_eq!(experiment.submission_id, stored.submission_id);
    assert_eq!(result, stored);
}

#[tokio::test]
async fn campaign_submit_active_campaign_rejects_human_and_agent_before_persistence() {
    let harness = SubmitHarness::with_active_campaign();

    let human = harness.submit(&["python", "other.py"]).await.unwrap_err();
    let agent = harness
        .submit_from_agent_run(&["python", "other.py"])
        .await
        .unwrap_err();

    for error in [human, agent] {
        assert!(matches!(
            error,
            AppError::Validation {
                field: "submit",
                message: "a managed campaign is active; use pueue-agent steer",
            }
        ));
    }
    assert_eq!(harness.submissions(), 0);
    assert_eq!(harness.pueue_add_calls(), 0);
}

#[tokio::test]
async fn campaign_submit_active_campaign_rejects_batch_before_manifest_persistence() {
    let harness = SubmitHarness::with_active_campaign();
    let manifest = harness.root.join("campaign-batch.json");
    fs::write(
        &manifest,
        r#"{"jobs":[{"id":"job-a","argv":["python","other.py"]}]}"#,
    )
    .unwrap();

    let error = batches::run_with(
        &harness.db,
        &harness.root,
        &uuid::Uuid::new_v4().to_string(),
        &manifest,
        None,
        &harness.fake,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        AppError::Validation {
            field: "submit-batch",
            message: "a managed campaign is active; use pueue-agent steer",
        }
    ));
    assert_eq!(harness.table_count("batch_requests"), 0);
    assert_eq!(harness.submissions(), 0);
    assert_eq!(harness.pueue_add_calls(), 0);
}

#[tokio::test]
async fn campaign_baseline_holds_admission_through_add_against_control_and_batch() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    harness.fake.pause_add();
    let db = harness.db.clone();
    let root = harness.root.clone();
    let fake = harness.fake.clone();
    let baseline = tokio::spawn(async move {
        let args = vec![OsString::from("python"), OsString::from("train.py")];
        submit::run_with_options(
            &db,
            &root,
            &args,
            &submit::SubmitOptions::default(),
            &CampaignLimits::default(),
            &fake,
        )
        .await
    });
    harness.fake.wait_for_add().await;

    let control = submit_legacy_control(
        &harness.db,
        &harness.root,
        &[OsString::from("python"), OsString::from("control.py")],
        &harness.fake,
    )
    .await
    .unwrap_err();
    let manifest = harness.root.join("admission-race-batch.json");
    fs::write(
        &manifest,
        r#"{"jobs":[{"id":"job-a","argv":["python","batch.py"]}]}"#,
    )
    .unwrap();
    let batch = batches::run_with(
        &harness.db,
        &harness.root,
        &uuid::Uuid::new_v4().to_string(),
        &manifest,
        None,
        &harness.fake,
    )
    .await
    .unwrap_err();

    assert!(matches!(control, AppError::Validation { field: "submit", .. }));
    assert!(matches!(
        batch,
        AppError::Validation {
            field: "submit-batch",
            ..
        }
    ));
    assert_eq!(harness.table_count("batch_requests"), 0);
    assert_eq!(harness.table_count("campaigns"), 1);
    assert_eq!(harness.table_count("submissions"), 1);
    assert_eq!(harness.pueue_add_calls(), 1);

    harness.fake.release_add();
    baseline.await.unwrap().unwrap();
}

#[tokio::test]
async fn control_admission_blocks_a_racing_campaign_baseline_before_persistence() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    harness.fake.pause_add();
    let db = harness.db.clone();
    let root = harness.root.clone();
    let fake = harness.fake.clone();
    let control = tokio::spawn(async move {
        submit_legacy_control(
            &db,
            &root,
            &[OsString::from("python"), OsString::from("control.py")],
            &fake,
        )
        .await
    });
    harness.fake.wait_for_add().await;

    let campaign = harness.submit(&["python", "train.py"]).await.unwrap_err();

    assert!(matches!(campaign, AppError::Runtime { .. }));
    assert_eq!(harness.table_count("campaigns"), 0);
    assert_eq!(harness.table_count("submissions"), 1);
    assert_eq!(harness.pueue_add_calls(), 1);
    harness.fake.release_add();
    control.await.unwrap().unwrap();
}

#[tokio::test]
async fn batch_admission_blocks_a_racing_campaign_baseline_before_persistence() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    let manifest = harness.root.join("blocking-batch.json");
    fs::write(
        &manifest,
        r#"{"jobs":[{"id":"job-a","argv":["python","batch.py"]}]}"#,
    )
    .unwrap();
    harness.fake.pause_add();
    let db = harness.db.clone();
    let root = harness.root.clone();
    let fake = harness.fake.clone();
    let request_id = uuid::Uuid::new_v4().to_string();
    let batch = tokio::spawn(async move {
        batches::run_with(&db, &root, &request_id, &manifest, None, &fake).await
    });
    harness.fake.wait_for_add().await;

    let campaign = harness.submit(&["python", "train.py"]).await.unwrap_err();

    assert!(matches!(campaign, AppError::Runtime { .. }));
    assert_eq!(harness.table_count("campaigns"), 0);
    assert_eq!(harness.table_count("batch_requests"), 1);
    assert_eq!(harness.table_count("submissions"), 1);
    assert_eq!(harness.pueue_add_calls(), 1);
    harness.fake.release_add();
    batch.await.unwrap().unwrap();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn project_pause_winning_before_control_admission_prevents_persistence_and_add() {
    let harness = SubmitHarness::new();
    let config_path = harness.root.join(".pueue-agent/config.toml");
    let barrier_path = harness.root.join(".pueue-agent/control-read-barrier.toml");
    fs::copy(config_path, &barrier_path).unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE projects SET config_path = ?1 WHERE project_id = 'project-a'",
            [barrier_path.to_string_lossy().as_ref()],
        )
        .unwrap();
    let barrier = ConfigReadBarrier::install(&barrier_path);
    let db = harness.db.clone();
    let root = harness.root.clone();
    let fake = harness.fake.clone();
    let control = tokio::spawn(async move {
        submit_legacy_control(
            &db,
            &root,
            &[OsString::from("python"), OsString::from("control.py")],
            &fake,
        )
        .await
    });
    barrier.wait_until_reader_opened().await;
    ProjectRepository::new(&harness.db)
        .pause("project-a", 101)
        .unwrap();
    barrier.release();

    assert!(control.await.unwrap().is_err());
    assert_eq!(harness.table_count("submissions"), 0);
    assert_eq!(harness.pueue_add_calls(), 0);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn project_disable_winning_before_batch_admission_prevents_persistence_and_add() {
    let harness = SubmitHarness::new();
    let manifest = harness.root.join("lifecycle-before-batch.json");
    fs::write(
        &manifest,
        r#"{"jobs":[{"id":"job-a","argv":["python","batch.py"]}]}"#,
    )
    .unwrap();
    let barrier = ConfigReadBarrier::install(&manifest);
    let db = harness.db.clone();
    let root = harness.root.clone();
    let fake = harness.fake.clone();
    let request_id = uuid::Uuid::new_v4().to_string();
    let batch = tokio::spawn(async move {
        batches::run_with(&db, &root, &request_id, &manifest, None, &fake).await
    });
    barrier.wait_until_reader_opened().await;
    ProjectRepository::new(&harness.db)
        .disable("project-a", 101, &[])
        .unwrap();
    barrier.release();

    assert!(batch.await.unwrap().is_err());
    assert_eq!(harness.table_count("batch_requests"), 0);
    assert_eq!(harness.table_count("submissions"), 0);
    assert_eq!(harness.pueue_add_calls(), 0);
}

#[tokio::test]
async fn campaign_submit_control_remains_a_legacy_one_off_without_a_live_campaign() {
    let harness = SubmitHarness::new();
    let args = vec![OsString::from("python"), OsString::from("control.py")];

    let result = submit::run_with_options(
        &harness.db,
        &harness.root,
        &args,
        &submit::SubmitOptions::new(SubmissionKind::Control, json!({}), None),
        &CampaignLimits::default(),
        &harness.fake,
    )
    .await
    .unwrap();

    assert_eq!(result.kind, SubmissionKind::Control);
    assert_eq!(harness.live_campaigns(), 0);
    assert_eq!(harness.submissions(), 1);
    assert_eq!(harness.pueue_add_calls(), 1);
}

#[tokio::test]
async fn campaign_submit_reserved_intent_resumes_with_exactly_one_add() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    let intent = harness.reserve_baseline(&["python", "train.py"]);
    let coordinator = CampaignCoordinator::new(
        &harness.db,
        &harness.fake,
        CampaignLimits::default(),
    );

    let result = coordinator
        .submit_accepted_intent(&intent, &harness.project(), 101)
        .await
        .unwrap();

    assert_eq!(result.pueue_task_id, Some(41));
    assert_eq!(harness.pueue_add_calls(), 1);
    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&intent.experiment.experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Accepted
    );
}

#[tokio::test]
async fn campaign_submit_reserved_intent_stays_reserved_after_campaign_pause() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    let intent = harness.reserve_baseline(&["python", "train.py"]);
    CampaignRepository::new(&harness.db)
        .pause("project-a", 101)
        .unwrap();
    let coordinator = CampaignCoordinator::new(
        &harness.db,
        &harness.fake,
        CampaignLimits::default(),
    );

    assert!(coordinator
        .submit_accepted_intent(&intent, &harness.project(), 102)
        .await
        .is_err());

    assert_eq!(harness.pueue_add_calls(), 0);
    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&intent.experiment.experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Reserved
    );
}

#[cfg(unix)]
#[tokio::test]
async fn campaign_submit_rejects_a_replaced_startup_pinned_root_before_add() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    let intent = harness.reserve_baseline(&["python", "train.py"]);
    let project = harness.project();
    let root_anchor = ProjectRootAnchor::resolve(&project.root_path).unwrap();
    let replaced_root = project
        .root_path
        .with_file_name("project-before-replacement");
    fs::rename(&project.root_path, &replaced_root).unwrap();
    fs::create_dir(&project.root_path).unwrap();
    let coordinator = CampaignCoordinator::new(
        &harness.db,
        &harness.fake,
        CampaignLimits::default(),
    )
    .with_root_anchor(root_anchor);

    let error = coordinator
        .submit_accepted_intent(&intent, &project, 101)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        AppError::PolicyViolation {
            violation: pueue_agent::execution_policy::PolicyViolation {
                code: PolicyViolationCode::RootChanged,
                ..
            }
        }
    ));
    assert_eq!(harness.pueue_add_calls(), 0);
    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&intent.experiment.experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Reserved
    );
}

#[tokio::test]
async fn campaign_submit_mutated_values_cannot_change_the_durable_pueue_add() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    let mut intent = harness.reserve_baseline(&["python", "train.py"]);
    intent.submission.argv = vec![
        "sh".to_owned(),
        "-c".to_owned(),
        "echo changed".to_owned(),
    ];
    let mut caller_project = harness.project();
    caller_project.pueue_group = "changed-group".to_owned();
    caller_project.root_path = harness.root.join("changed-root");
    let coordinator = CampaignCoordinator::new(
        &harness.db,
        &harness.fake,
        CampaignLimits::default(),
    );

    let result = coordinator
        .submit_accepted_intent(&intent, &caller_project, 101)
        .await
        .unwrap();

    assert_eq!(result.argv, vec!["python", "train.py"]);
    assert_eq!(harness.pueue_add_calls(), 1);
    assert_eq!(
        harness.fake.last_add_args(),
        vec![
            OsString::from("-g"),
            OsString::from("pa-project"),
            OsString::from("--working-directory"),
            fs::canonicalize(&harness.root).unwrap().into_os_string(),
            OsString::from("--"),
            OsString::from("python"),
            OsString::from("train.py"),
        ]
    );
}

#[tokio::test]
async fn campaign_submit_conflicting_intent_identity_fails_before_pueue_add() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    let mut intent = harness.reserve_baseline(&["python", "train.py"]);
    intent.submission.submission_id = "conflicting-submission".to_owned();
    let coordinator = CampaignCoordinator::new(
        &harness.db,
        &harness.fake,
        CampaignLimits::default(),
    );

    let error = coordinator
        .submit_accepted_intent(&intent, &harness.project(), 101)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        AppError::Validation {
            field: "campaign.intent",
            message: "must match the durable managed submission identity",
        }
    ));
    assert_eq!(harness.pueue_add_calls(), 0);
    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&intent.experiment.experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Reserved
    );
}

#[tokio::test]
async fn campaign_submit_restart_after_submitting_before_add_never_readds() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    let intent = harness.reserve_baseline(&["python", "train.py"]);
    ExperimentRepository::new(&harness.db)
        .mark_submitting(&intent.experiment.experiment_id, 101)
        .unwrap();
    let coordinator = CampaignCoordinator::new(
        &harness.db,
        &harness.fake,
        CampaignLimits::default(),
    );

    assert!(coordinator
        .submit_accepted_intent(&intent, &harness.project(), 102)
        .await
        .is_err());

    assert_eq!(harness.pueue_add_calls(), 0);
    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&intent.experiment.experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Unreconciled
    );
}

#[tokio::test]
async fn campaign_submit_restart_after_success_before_acceptance_never_readds() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    let intent = harness.reserve_baseline(&["python", "train.py"]);
    ExperimentRepository::new(&harness.db)
        .mark_submitting(&intent.experiment.experiment_id, 101)
        .unwrap();
    assert_eq!(harness.fake.add(&[]).await.unwrap(), 41);
    let coordinator = CampaignCoordinator::new(
        &harness.db,
        &harness.fake,
        CampaignLimits::default(),
    );

    assert!(coordinator
        .submit_accepted_intent(&intent, &harness.project(), 102)
        .await
        .is_err());

    assert_eq!(harness.pueue_add_calls(), 1);
    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&intent.experiment.experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Unreconciled
    );
}

#[tokio::test]
async fn campaign_submit_add_timeout_is_unreconciled_before_returning_original_error() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    let intent = harness.reserve_baseline(&["python", "train.py"]);
    let pueue = TimeoutPueue::new();
    let coordinator = CampaignCoordinator::new(&harness.db, &pueue, CampaignLimits::default());

    let error = coordinator
        .submit_accepted_intent(&intent, &harness.project(), 101)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        AppError::Pueue(PueueError::Timeout { operation: "add" })
    ));
    assert_eq!(pueue.add_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&intent.experiment.experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Unreconciled
    );
}

#[test]
fn pueue_group_accepts_bounded_grammar_and_rejects_shell_text() {
    for value in ["pa-project", "A9._-x"] {
        validate_group(value).unwrap();
    }
    for value in ["", "-bad", "pa project", "pa/project", "pa;touch-x", &"a".repeat(129)] {
        assert!(validate_group(value).is_err(), "accepted {value:?}");
    }
}

#[cfg(unix)]
#[test]
fn pueue_config_anchor_rejects_weak_mode_and_project_root_config() {
    let weak = CorePolicyHarness::new(false, true);
    let weak_result = weak.load_core_policy();
    match &weak_result {
        Err(error)
            if error.code == PolicyViolationCode::AnchorMissing
                && error.stage == PolicyViolationStage::Startup => {}
        result => panic!("weak config result: {result:?}"),
    }

    let under_root = CorePolicyHarness::new(true, false);
    let under_root_result = under_root.load_core_policy();
    match &under_root_result {
        Err(error)
            if error.code == PolicyViolationCode::TrustedPathUnsafe
                && error.stage == PolicyViolationStage::Startup => {}
        result => panic!("project-root config result: {result:?}"),
    }
}

#[tokio::test]
async fn command_adapter_preserves_fixed_and_arbitrary_arguments() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", None);
    let adapter = configured_pueue(fixture.policy()).unwrap();
    let add_args = [
        OsString::from("-g"),
        OsString::from("pa-project"),
        OsString::from("--"),
        OsString::from("python"),
        OsString::from("train.py"),
        OsString::from("--name"),
        OsString::from("a b; echo bad && $(touch nope)"),
    ];

    let task_id = adapter.add(&add_args).await.unwrap();

    assert_eq!(task_id, 73);
    assert_eq!(
        fixture.captured_args(),
        vec![
            "--config",
            "/dev/fd/9",
            "add",
            "--print-task-id",
            "-g",
            "pa-project",
            "--escape",
            "--",
            "python",
            "train.py",
            "--name",
            "a b; echo bad && $(touch nope)",
        ]
        .into_iter()
        .map(OsString::from)
        .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn command_adapter_kills_only_the_requested_task_id() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", None);
    let adapter = configured_pueue(fixture.policy()).unwrap();

    adapter.kill(41).await.unwrap();

    assert_eq!(
        fixture.captured_args(),
        vec!["--config", "/dev/fd/9", "kill", "41"]
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn command_adapter_removes_only_the_requested_task_id() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", None);
    let adapter = configured_pueue(fixture.policy()).unwrap();

    adapter.remove(41).await.unwrap();

    assert_eq!(
        fixture.captured_args(),
        vec!["--config", "/dev/fd/9", "remove", "41"]
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn command_adapter_provisions_group_without_shell() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", None);
    let adapter = configured_pueue(fixture.policy()).unwrap();

    adapter
        .ensure_group("pa-project")
        .await
        .unwrap();

    assert_eq!(
        fixture.captured_args(),
        vec![
            "--config",
            "/dev/fd/9",
            "group",
            "add",
            "pa-project",
        ]
        .into_iter()
        .map(OsString::from)
        .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn command_adapter_rejects_invalid_group_before_pueue_execution() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", None);
    let adapter = configured_pueue(fixture.policy()).unwrap();

    let error = adapter.ensure_group("pa project").await.unwrap_err();

    assert!(matches!(
        error,
        AppError::Validation {
            field: "pueue_group",
            ..
        }
    ));
    assert!(fixture.captured_invocations().is_empty());
}

#[tokio::test]
async fn command_adapter_skips_group_add_when_group_already_exists() {
    let fixture = FakePueueCommand::new_with_group_lists(
        STATUS_JSON,
        "73\n",
        &[r#"{"default":{"parallel_tasks":1},"pa-project":{"parallel_tasks":1}}"#],
        None,
    );
    let adapter = configured_pueue(fixture.policy()).unwrap();

    adapter
        .ensure_group("pa-project")
        .await
        .unwrap();

    assert_eq!(
        fixture.captured_invocations(),
        vec![vec!["--config", "/dev/fd/9", "group", "-j"]
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>()]
    );
}

#[tokio::test]
async fn command_adapter_adds_missing_group_after_json_list_check() {
    let fixture = FakePueueCommand::new_with_group_lists(
        STATUS_JSON,
        "73\n",
        &[r#"{"default":{"parallel_tasks":1}}"#],
        None,
    );
    let adapter = configured_pueue(fixture.policy()).unwrap();

    adapter
        .ensure_group("pa-project")
        .await
        .unwrap();

    assert_eq!(
        fixture.captured_invocations(),
        vec![
            vec!["--config", "/dev/fd/9", "group", "-j"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>(),
            vec![
                "--config",
                "/dev/fd/9",
                "group",
                "add",
                "pa-project",
            ]
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>()
        ]
    );
}

#[tokio::test]
async fn command_adapter_rejects_non_object_group_json() {
    for groups in ["[]", "null", "\"pa-project\""] {
        let fixture = FakePueueCommand::new_with_group_lists(
            STATUS_JSON,
            "73\n",
            &[groups],
            None,
        );
        let adapter = configured_pueue(fixture.policy()).unwrap();

        let error = adapter.ensure_group("pa-project").await.unwrap_err();

        assert!(matches!(
            error,
            AppError::Pueue(PueueError::InvalidGroupJson { .. })
        ));
        assert_eq!(fixture.captured_invocations().len(), 1);
    }
}

#[tokio::test]
async fn command_adapter_treats_racing_group_add_as_success_when_group_appears() {
    let fixture = FakePueueCommand::new_with_group_lists(
        STATUS_JSON,
        "73\n",
        &[
            r#"{"default":{"parallel_tasks":1}}"#,
            r#"{"default":{"parallel_tasks":1},"pa-project":{"parallel_tasks":1}}"#,
        ],
        Some("group-add"),
    );
    let adapter = configured_pueue(fixture.policy()).unwrap();

    adapter.ensure_group("pa-project").await.unwrap();

    assert_eq!(
        fixture.captured_invocations(),
        vec![
            vec!["--config", "/dev/fd/9", "group", "-j"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>(),
            vec!["--config", "/dev/fd/9", "group", "add", "pa-project"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>(),
            vec!["--config", "/dev/fd/9", "group", "-j"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        ]
    );
}

#[tokio::test]
async fn command_adapter_preserves_group_add_error_when_group_remains_absent() {
    let fixture = FakePueueCommand::new_with_group_lists(
        STATUS_JSON,
        "73\n",
        &[r#"{"default":{"parallel_tasks":1}}"#],
        Some("group-add"),
    );
    let adapter = configured_pueue(fixture.policy()).unwrap();

    let error = adapter.ensure_group("pa-project").await.unwrap_err();

    match error {
        AppError::Pueue(PueueError::CommandFailed {
            operation,
            exit_code,
            stdout,
            stderr,
        }) => {
            assert_eq!(operation, "group");
            assert_eq!(exit_code, Some(7));
            assert_eq!(stdout, b"partial output");
            assert_eq!(stderr, b"daemon unavailable");
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(
        fixture.captured_invocations(),
        vec![
            vec!["--config", "/dev/fd/9", "group", "-j"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>(),
            vec!["--config", "/dev/fd/9", "group", "add", "pa-project"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>(),
            vec!["--config", "/dev/fd/9", "group", "-j"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        ]
    );
}

#[tokio::test]
async fn status_json_preserves_task_identity_timestamps_and_result() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", None);
    let adapter = configured_pueue(fixture.policy()).unwrap();

    let tasks = adapter.status_json().await.unwrap();

    assert_eq!(
        tasks,
        vec![PueueTask {
            id: 41,
            group: "pa-project".to_owned(),
            command: "python train.py --name experiment".to_owned(),
            state: "Done".to_owned(),
            enqueued_at: Some("2026-08-09T10:00:00Z".to_owned()),
            started_at: Some("2026-08-09T10:00:03Z".to_owned()),
            ended_at: Some("2026-08-09T10:05:00Z".to_owned()),
            result: Some(json!({"Failed": 17})),
        }]
    );
    assert_eq!(
        fixture.captured_args(),
        vec!["--config", "/dev/fd/9", "status", "--json"]
    );
}

#[tokio::test]
async fn status_json_rejects_state_details_that_are_not_objects() {
    let status_json = r#"{
      "tasks": {
        "41": {
          "id": "41",
          "group": "pa-project",
          "command": "python train.py",
          "status": {
            "Done": "Success"
          }
        }
      }
    }"#;
    let fixture = FakePueueCommand::new(status_json, "73\n", None);
    let adapter = configured_pueue(fixture.policy()).unwrap();

    let error = adapter.status_json().await.unwrap_err();

    match error {
        AppError::Pueue(PueueError::InvalidStatusTask { reason }) => {
            assert_eq!(reason, "state details must be an object");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn status_json_rejects_wrong_typed_optional_timestamps() {
    let status_json = r#"{
      "tasks": {
        "41": {
          "id": "41",
          "group": "pa-project",
          "command": "python train.py",
          "status": {
            "Running": {
              "enqueued_at": "2026-08-09T10:00:00Z",
              "start": 123
            }
          }
        }
      }
    }"#;
    let fixture = FakePueueCommand::new(status_json, "73\n", None);
    let adapter = configured_pueue(fixture.policy()).unwrap();

    let error = adapter.status_json().await.unwrap_err();

    match error {
        AppError::Pueue(PueueError::InvalidStatusTask { reason }) => {
            assert_eq!(reason, "start timestamp must be a string when present");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn non_zero_exit_is_a_typed_error_with_captured_output() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", Some("kill"));
    let adapter = configured_pueue(fixture.policy()).unwrap();

    let error = adapter.kill(41).await.unwrap_err();

    match error {
        AppError::Pueue(PueueError::CommandFailed {
            operation,
            exit_code,
            stdout,
            stderr,
        }) => {
            assert_eq!(operation, "kill");
            assert_eq!(exit_code, Some(7));
            assert_eq!(stdout, b"partial output");
            assert_eq!(stderr, b"daemon unavailable");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn remove_non_zero_exit_is_a_typed_error_with_captured_output() {
    let fixture = FakePueueCommand::new(STATUS_JSON, "73\n", Some("remove"));
    let adapter = configured_pueue(fixture.policy()).unwrap();

    let error = adapter.remove(41).await.unwrap_err();

    match error {
        AppError::Pueue(PueueError::CommandFailed { operation, .. }) => {
            assert_eq!(operation, "remove");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn malformed_status_json_is_a_typed_integration_error() {
    let fixture = FakePueueCommand::new("not-json", "73\n", None);
    let adapter = configured_pueue(fixture.policy()).unwrap();

    let error = adapter.status_json().await.unwrap_err();

    assert!(matches!(
        error,
        AppError::Pueue(PueueError::InvalidStatusJson { .. })
    ));
}

#[tokio::test]
async fn submit_records_intent_before_add_and_preserves_arguments() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    fake.pause_add();
    let args = vec![
        OsString::from("python"),
        OsString::from("train.py"),
        OsString::from("--name"),
        OsString::from("a b; echo bad"),
    ];
    let task_db = harness.db.clone();
    let task_root = harness.root.clone();
    let task_fake = fake.clone();
    let submit_task = tokio::spawn(async move {
        submit_legacy_control(&task_db, &task_root, &args, &task_fake).await
    });

    fake.wait_for_add().await;
    let pending = SubmissionRepository::new(&harness.db)
        .find_unreconciled("project-a")
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status, SubmissionStatus::Pending);
    assert_eq!(
        pending[0].argv,
        vec!["python", "train.py", "--name", "a b; echo bad"]
    );
    assert_eq!(
        fake.last_add_args(),
        vec![
            OsString::from("-g"),
            OsString::from("pa-project"),
            OsString::from("--"),
            OsString::from("python"),
            OsString::from("train.py"),
            OsString::from("--name"),
            OsString::from("a b; echo bad"),
        ]
    );

    fake.release_add();
    let accepted = submit_task.await.unwrap().unwrap();
    assert_eq!(accepted.status, SubmissionStatus::Accepted);
    assert_eq!(accepted.pueue_task_id, Some(73));
    assert_eq!(
        accepted.task_signature.as_deref(),
        Some(expected_provisional_signature("pa-project", 73, &accepted.submission_id).as_str())
    );
}

#[tokio::test]
async fn oversized_native_add_argv_is_rejected_before_submission_insert() {
    // Native argv contains the user's command plus the submit group separator,
    // add flags, and the fixed pueue --config operation prefix.
    let max_user_add_args = 256 - 9;
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    let command = vec![OsString::from("x"); max_user_add_args + 1];

    assert!(submit_legacy_control(&harness.db, &harness.root, &command, &fake)
        .await
        .is_err());
    assert!(SubmissionRepository::new(&harness.db)
        .find_unreconciled("project-a")
        .unwrap()
        .is_empty());
    assert!(fake.last_add_args().is_empty());
}

#[tokio::test]
async fn maximum_native_add_argv_is_persisted_and_submitted() {
    let max_user_add_args = 256 - 9;
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    let command = vec![OsString::from("x"); max_user_add_args];

    let submission = submit_legacy_control(&harness.db, &harness.root, &command, &fake)
        .await
        .unwrap();

    assert_eq!(submission.status, SubmissionStatus::Accepted);
    assert_eq!(fake.last_add_args().len(), max_user_add_args + 3);
}

#[tokio::test]
async fn first_excess_native_add_byte_is_rejected_before_direct_persistence() {
    let accepted_harness = SubmitHarness::new();
    let accepted_fake = FakePueue::new().with_add_task_id(73);
    let accepted = native_add_byte_boundary_command(false);
    assert!(submit_legacy_control(
        &accepted_harness.db,
        &accepted_harness.root,
        &accepted,
        &accepted_fake,
    )
    .await
    .is_ok());

    let rejected_harness = SubmitHarness::new();
    let rejected_fake = FakePueue::new().with_add_task_id(73);
    let rejected = native_add_byte_boundary_command(true);
    assert!(submit_legacy_control(
        &rejected_harness.db,
        &rejected_harness.root,
        &rejected,
        &rejected_fake,
    )
    .await
    .is_err());
    assert!(SubmissionRepository::new(&rejected_harness.db)
        .find_unreconciled("project-a")
        .unwrap()
        .is_empty());
    assert!(rejected_fake.last_add_args().is_empty());
}

#[tokio::test]
async fn oversized_native_batch_add_argv_is_rejected_before_submission_insert() {
    let max_user_add_args = 256 - 9;
    let harness = SubmitHarness::new();
    let manifest_path = harness.root.join("oversized-jobs.json");
    let argv = std::iter::repeat("x")
        .take(max_user_add_args + 1)
        .collect::<Vec<_>>();
    fs::write(
        &manifest_path,
        serde_json::json!({"jobs":[{"id":"too-large","argv":argv}]}).to_string(),
    )
    .unwrap();
    let fake = FakePueue::new().with_add_task_id(73);
    let request_id = uuid::Uuid::new_v4().to_string();

    assert!(batches::run_with(
        &harness.db,
        &harness.root,
        &request_id,
        &manifest_path,
        None,
        &fake,
    )
    .await
    .is_err());
    assert!(SubmissionRepository::new(&harness.db)
        .find_unreconciled("project-a")
        .unwrap()
        .is_empty());
    assert!(BatchRepository::new(&harness.db)
        .find("project-a", &request_id)
        .unwrap()
        .is_none());
    assert!(fake.last_add_args().is_empty());
}

#[tokio::test]
async fn submit_options_persist_control_metadata_without_changing_pueue_argv() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    let args = vec![OsString::from("python"), OsString::from("train.py")];
    let metadata = submit::load_metadata(None, Some(r#"{"trial":"baseline","epochs":3}"#)).unwrap();

    let submission = submit::run_with_options(
        &harness.db,
        &harness.root,
        &args,
        &submit::SubmitOptions::new(SubmissionKind::Control, metadata, None),
        &CampaignLimits::default(),
        &fake,
    )
    .await
    .unwrap();

    assert_eq!(submission.kind, SubmissionKind::Control);
    assert_eq!(submission.metadata, json!({"trial":"baseline","epochs":3}));
    assert_eq!(submission.origin_agent_run_id, None);
    assert_eq!(
        fake.last_add_args(),
        vec![
            OsString::from("-g"),
            OsString::from("pa-project"),
            OsString::from("--"),
            OsString::from("python"),
            OsString::from("train.py"),
        ]
    );
}

#[tokio::test]
async fn submit_options_accept_explicit_control_kind() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    let args = vec![OsString::from("python"), OsString::from("control.py")];

    let submission = submit::run_with_options(
        &harness.db,
        &harness.root,
        &args,
        &submit::SubmitOptions::new(SubmissionKind::Control, json!({}), None),
        &CampaignLimits::default(),
        &fake,
    )
    .await
    .unwrap();

    assert_eq!(submission.kind, SubmissionKind::Control);
}

#[test]
fn metadata_loader_rejects_conflicting_or_out_of_bounds_values() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("metadata.json");
    fs::write(&path, r#"{"from":"file"}"#).unwrap();
    let oversized_path = temp.path().join("oversized-metadata.json");
    fs::write(&oversized_path, "x".repeat(16 * 1024 + 1)).unwrap();

    assert!(submit::load_metadata(Some(&path), Some(r#"{"inline":true}"#)).is_err());
    assert!(submit::load_metadata(Some(&oversized_path), None).is_err());
    assert!(submit::load_metadata(None, Some("[]")).is_err());
    assert!(submit::load_metadata(
        None,
        Some(&format!(r#"{{"value":"{}"}}"#, "x".repeat(1025)))
    )
    .is_err());
    assert!(submit::load_metadata(
        None,
        Some(&format!(r#"{{"items":[{}]}}"#, vec!["0"; 65].join(",")))
    )
    .is_err());
    assert!(submit::load_metadata(None, Some(&format!(r#"{{"{}":1}}"#, "k".repeat(65)))).is_err());
    assert!(submit::load_metadata(
        None,
        Some(&format!(
            r#"{{{}}}"#,
            (0..33)
                .map(|index| format!(r#""k{index}":0"#))
                .collect::<Vec<_>>()
                .join(",")
        ))
    )
    .is_err());
    assert!(submit::load_metadata(
        None,
        Some(&format!(
            r#"{{"nested":{}}}"#,
            "{".repeat(8) + "0" + &"}".repeat(8)
        ))
    )
    .is_err());
    assert!(submit::load_metadata(
        None,
        Some(&format!(r#"{{"value":"{}"}}"#, "x".repeat(16 * 1024)))
    )
    .is_err());
}

#[tokio::test]
async fn run_with_options_rejects_oversized_metadata_before_pueue_add() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    let args = vec![OsString::from("python"), OsString::from("train.py")];
    let oversized = serde_json::Value::Object(
        (0..32)
            .map(|index| {
                (
                    format!("key-{index}"),
                    serde_json::Value::String("x".repeat(1024)),
                )
            })
            .collect(),
    );

    let error = submit::run_with_options(
        &harness.db,
        &harness.root,
        &args,
        &submit::SubmitOptions::new(SubmissionKind::Experiment, oversized, None),
        &CampaignLimits::default(),
        &fake,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        AppError::Validation {
            field: "submit.metadata",
            ..
        }
    ));
    assert!(fake.last_add_args().is_empty());
    assert!(SubmissionRepository::new(&harness.db)
        .find_unreconciled("project-a")
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn invalid_origin_is_rejected_before_pueue_add_and_valid_origin_is_persisted() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    let args = vec![OsString::from("python"), OsString::from("train.py")];

    let invalid = submit::run_with_options(
        &harness.db,
        &harness.root,
        &args,
        &submit::SubmitOptions::new(SubmissionKind::Experiment, json!({}), Some(999)),
        &CampaignLimits::default(),
        &fake,
    )
    .await;
    assert!(invalid.is_err());
    assert!(fake.last_add_args().is_empty());

    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFinished,
            "origin-test",
            json!({}),
            1,
            1,
        ))
        .unwrap();
    let run = pueue_agent::db::AgentRunRepository::new(&harness.db)
        .insert(&NewAgentRun::new(
            "project-a",
            event.event_id,
            None,
            AgentRunStatus::Running,
            1,
            harness.root.join("agent.log"),
        ))
        .unwrap();
    let submission = submit::run_with_options(
        &harness.db,
        &harness.root,
        &args,
        &submit::SubmitOptions::new(SubmissionKind::Control, json!({}), Some(run.run_id)),
        &CampaignLimits::default(),
        &fake,
    )
    .await
    .unwrap();
    assert_eq!(submission.origin_agent_run_id, Some(run.run_id));
}

#[test]
fn agent_origin_environment_requires_a_complete_matching_pair() {
    assert_eq!(
        submit::origin_from_values(None, None, "project-a").unwrap(),
        None
    );
    assert!(submit::origin_from_values(Some("7"), None, "project-a").is_err());
    assert!(submit::origin_from_values(Some("nope"), Some("project-a"), "project-a").is_err());
    assert!(submit::origin_from_values(Some("7"), Some("project-b"), "project-a").is_err());
    assert_eq!(
        submit::origin_from_values(Some("7"), Some("project-a"), "project-a").unwrap(),
        Some(7)
    );
}

#[test]
fn submission_output_is_bounded_and_never_includes_raw_metadata() {
    let submission = Submission {
        submission_id: "submission-1".to_owned(),
        project_id: "project-a".to_owned(),
        argv: vec!["python".to_owned(), "train.py".to_owned()],
        created_at: 1,
        pueue_task_id: Some(73),
        task_signature: Some("signature".to_owned()),
        status: SubmissionStatus::Accepted,
        kind: SubmissionKind::Control,
        metadata: json!({"credential":"very-secret"}),
        origin_agent_run_id: Some(7),
    };

    let human = submit::render_submission(&submission, "pa-project", false).unwrap();
    assert_eq!(
        human,
        "pueue-agent submit project=project-a\nsub=submission-1 task=73 kind=control group=pa-project state=accepted\nsummary: submission accepted"
    );
    assert!(!human.contains("very-secret"));

    let json = submit::render_submission(&submission, "pa-project", true).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["submission_id"], "submission-1");
    assert_eq!(value["task_id"], 73);
    assert_eq!(value["kind"], "control");
    assert_eq!(value["group"], "pa-project");
    assert_eq!(value["state"], "accepted");
    assert!(value.get("metadata").is_none());
}

#[test]
fn cli_output_contract_submit_has_header_ids_state_summary_and_pure_json() {
    let submission = Submission {
        submission_id: "submission-contract".to_owned(),
        project_id: "project-a".to_owned(),
        argv: vec!["python".to_owned(), "train.py".to_owned()],
        created_at: 1,
        pueue_task_id: Some(73),
        task_signature: Some("signature".to_owned()),
        status: SubmissionStatus::Accepted,
        kind: SubmissionKind::Experiment,
        metadata: json!({"credential":"very-secret"}),
        origin_agent_run_id: None,
    };

    let human = submit::render_submission(&submission, "pa-project", false).unwrap();
    assert!(human.starts_with("pueue-agent submit"), "{human}");
    assert!(human.contains("sub=submission-contract"), "{human}");
    assert!(human.contains("task=73"), "{human}");
    assert!(human
        .split_whitespace()
        .any(|field| field == "state=accepted"));
    assert!(human.contains("summary:"), "{human}");
    assert!(!human.contains("very-secret"), "{human}");
    assert!(!human.contains('\x1b'), "{human}");

    let json = submit::render_submission(&submission, "pa-project", true).unwrap();
    assert!(json.starts_with('{'), "{json}");
    assert!(!json.contains("pueue-agent"), "{json}");
    assert!(!json.contains('\x1b'), "{json}");
    let _: serde_json::Value = serde_json::from_str(&json).unwrap();
}

#[tokio::test]
async fn submit_keeps_pending_intent_when_add_fails() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_failure();
    let args = vec![
        OsString::from("python"),
        OsString::from("train.py"),
        OsString::from("--name"),
        OsString::from("a b; echo bad"),
    ];

    let error = submit_legacy_control(&harness.db, &harness.root, &args, &fake)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        AppError::Pueue(PueueError::CommandFailed {
            operation: "add",
            exit_code: Some(7),
            ..
        })
    ));
    let pending = SubmissionRepository::new(&harness.db)
        .find_unreconciled("project-a")
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status, SubmissionStatus::Pending);
    assert_eq!(pending[0].pueue_task_id, None);
    assert_eq!(pending[0].task_signature, None);
    assert_eq!(
        pending[0].argv,
        vec!["python", "train.py", "--name", "a b; echo bad"]
    );
}

#[tokio::test]
async fn submit_provisional_signature_uses_submission_intent_to_avoid_task_id_collisions() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    let args = vec![OsString::from("python"), OsString::from("train.py")];

    let first = submit_legacy_control(&harness.db, &harness.root, &args, &fake)
        .await
        .unwrap();
    let second = submit_legacy_control(&harness.db, &harness.root, &args, &fake)
        .await
        .unwrap();

    assert_eq!(first.pueue_task_id, Some(73));
    assert_eq!(second.pueue_task_id, Some(73));
    assert_ne!(first.task_signature, second.task_signature);
    assert_eq!(
        first.task_signature.as_deref(),
        Some(expected_provisional_signature("pa-project", 73, &first.submission_id).as_str())
    );
    assert_eq!(
        second.task_signature.as_deref(),
        Some(expected_provisional_signature("pa-project", 73, &second.submission_id).as_str())
    );
}

#[tokio::test]
async fn batch_external_add_result_is_recorded_after_durable_intent() {
    let harness = SubmitHarness::new();
    let fake = FakePueue::new().with_add_task_id(73);
    let repository = BatchRepository::new(&harness.db);
    repository
        .create_or_get(&NewBatchRequest::new(
            "batch-external-result",
            "project-a",
            "sha256:external-result",
            vec![NewBatchJob::new(
                "job-a",
                0,
                SubmissionKind::Experiment,
                vec!["python".to_owned(), "train.py".to_owned()],
                serde_json::json!({"name": "external-result"}),
            )],
            100,
        ))
        .unwrap();
    let claimed = repository
        .claim("project-a", "batch-external-result", 100, 110)
        .unwrap()
        .unwrap();
    let lease_token = claimed.lease_token.as_deref().unwrap();

    let pueue_task_id = fake
        .add(&[OsString::from("--"), OsString::from("python")])
        .await
        .unwrap();
    let completed = repository
        .record_job_result(
            "project-a",
            "batch-external-result",
            "job-a",
            lease_token,
            BatchJobResult::Accepted {
                pueue_task_id,
                submission_id: "submission-external-result".to_owned(),
            },
            101,
        )
        .unwrap();

    assert_eq!(completed.jobs[0].pueue_task_id, Some(73));
    assert_eq!(
        completed.jobs[0].submission_id.as_deref(),
        Some("submission-external-result")
    );
    assert_eq!(completed.jobs[0].status.to_string(), "accepted");
}

#[tokio::test]
async fn submit_batch_cli_core_flow_uses_pueue_adapter_double_and_shared_renderers() {
    let harness = SubmitHarness::new();
    let manifest_path = harness.root.join("jobs.json");
    fs::write(
        &manifest_path,
        r#"{"jobs":[{"id":"job-a","argv":["python","train.py"],"metadata":{"credential":"hidden"}}]}"#,
    )
    .unwrap();
    let fake = FakePueue::new().with_add_task_id(73);

    let batch = pueue_agent::batches::run_with(
        &harness.db,
        &harness.root,
        "22222222-2222-4222-8222-222222222222",
        &manifest_path,
        None,
        &fake,
    )
    .await
    .unwrap();

    assert_eq!(batch.status.to_string(), "completed");
    assert_eq!(batch.jobs[0].pueue_task_id, Some(73));
    assert!(batch.jobs[0].submission_id.is_some());
    assert_eq!(
        fake.last_add_args(),
        vec![
            OsString::from("-g"),
            OsString::from("pa-project"),
            OsString::from("--"),
            OsString::from("python"),
            OsString::from("train.py"),
        ]
    );

    let human = pueue_agent::batches::render_batch(&batch, "pa-project", false).unwrap();
    assert!(human.starts_with("pueue-agent submit-batch"));
    assert!(human.contains("request=22222222-2222-4222-8222-222222222222"));
    assert!(human.contains("state=completed"));
    assert!(human.contains("accepted=1 failed=0 pending=0"));
    assert!(!human.contains("hidden"));

    let json = pueue_agent::batches::render_batch(&batch, "pa-project", true).unwrap();
    assert!(json.starts_with('{'));
    assert!(!json.contains("pueue-agent"));
    assert!(!json.contains("hidden"));
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["status"], "completed");
    assert_eq!(value["jobs"][0]["task_id"], 73);
}
