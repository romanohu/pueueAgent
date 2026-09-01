#[path = "../support/fake_pueue.rs"]
mod fake_pueue;
#[cfg(unix)]
#[path = "../support/config_read_barrier.rs"]
mod config_read_barrier;
#[cfg(all(unix, debug_assertions))]
#[path = "../support/native_process_fixture.rs"]
mod native_process_fixture;
#[cfg(unix)]
#[path = "../support/execution_policy_fixture.rs"]
mod execution_policy_fixture;

use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;

#[cfg(all(unix, debug_assertions))]
use std::time::Instant;

#[cfg(unix)]
use std::process::Command;

#[cfg(unix)]
use std::os::{fd::AsRawFd, unix::fs::PermissionsExt};

use fake_pueue::{FakePueue, FakePueueCommand};
#[cfg(unix)]
use config_read_barrier::ConfigReadBarrier;
#[cfg(all(unix, debug_assertions))]
use native_process_fixture::{
    NativeBehavior, NativeFakePueue, OUTPUT_SENTINEL, OVERFLOW_SENTINEL_REPETITIONS,
    TIMEOUT_SENTINEL_REPETITIONS,
};
use pueue_agent::{
    agent::{AgentRunner, AgentRunnerConfig},
    batches::{self, BatchJobResult},
    campaign::CampaignCoordinator,
    code_change,
    daemon::{Daemon, DaemonConfig},
    decision::DecisionCoordinator,
    decision_evidence::MAX_DECISION_CONTEXT_BYTES,
    decision_protocol::parse_and_validate_decision,
    db::{
        AgentRunRepository, BatchRepository, CampaignRepository, CodeChangeRepository, Db,
        DecisionRepository,
        EventRepository, ExperimentRepository, ManagedSubmissionIntent, ProjectRepository,
        ProposalRepository, StartCampaignRequest, SubmissionRepository,
    },
    execution_policy::{CampaignLimits, ProjectRootAnchor},
    models::{
        AgentRunStatus, CampaignState, CodeChangeState, DecisionAttemptState, DecisionCycleState,
        EventKind, EventStatus, ExperimentStatus, ExperimentTerminalOutcome, NewAgentRun,
        NewBatchJob, NewBatchRequest, NewEvent, NewProject, ProposalKind, ProposalStatus,
        Submission, SubmissionKind, SubmissionStatus,
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
use sha2::{Digest, Sha256};
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

#[cfg(unix)]
#[test]
fn production_decision_proposal_id_derives_git_safe_candidate_ref() {
    let proposal_id = format!("decision-proposal:{}", "a".repeat(64));
    let candidate = code_change::candidate_ref("decision-campaign", &proposal_id).unwrap();
    assert!(!candidate.contains(':'));
    let ref_name = format!("refs/heads/{candidate}");
    let output = Command::new("/usr/bin/git")
        .args(["check-ref-format", &ref_name])
        .output()
        .unwrap();
    assert!(output.status.success(), "invalid ref {candidate:?}: {output:?}");
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
                    objective_metric: None,
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

#[derive(Clone, Copy)]
enum DecisionFailpoint {
    AfterIntentAcceptBeforeDecisionComplete,
    AfterDecisionCommit,
    AfterPueueAdd,
}

struct DecisionHarness {
    temp: TempDir,
    db: Db,
    root: PathBuf,
    pueue: CountingFakePueue,
    campaign_id: String,
    source_experiment_id: String,
    cycle_id: String,
    decision_json: String,
}

impl DecisionHarness {
    fn with_ready_proposal(status: ExperimentStatus) -> Self {
        let mut harness = Self::with_terminal_source(status, None);
        let decision = json!({
            "schema_version": 1,
            "decision": "proposal",
            "proposal": {
                "kind": "experiment",
                "hypothesis": "Lower the learning rate after the terminal baseline",
                "source_experiment_id": harness.source_experiment_id,
                "argv": ["python", "train.py", "--lr", "0.001"],
                "working_directory": ".",
                "expected_evidence": ["validation loss"]
            }
        })
        .to_string();
        harness.persist_ready_decision(&decision, "proposal", 200);
        harness.decision_json = decision;
        harness
    }

    fn with_ready_repair(fingerprint: Option<&str>) -> Self {
        Self::with_ready_repair_source(ExperimentStatus::Failed, fingerprint)
    }

    fn with_ready_repair_source(
        status: ExperimentStatus,
        fingerprint: Option<&str>,
    ) -> Self {
        let mut harness = Self::with_terminal_source(status, fingerprint);
        let decision = json!({
            "schema_version": 1,
            "decision": "proposal",
            "proposal": {
                "kind": "repair",
                "hypothesis": "Retry the trusted failure with a smaller batch",
                "source_experiment_id": harness.source_experiment_id,
                "argv": ["python", "train.py", "--batch-size", "16"],
                "working_directory": ".",
                "expected_evidence": ["failure no longer reproduces"]
            }
        })
        .to_string();
        harness.persist_ready_decision(&decision, "proposal", 200);
        harness.decision_json = decision;
        harness
    }

    fn with_ready_wait(minutes: u32) -> Self {
        let mut harness = Self::with_terminal_source(ExperimentStatus::Succeeded, None);
        let decision = json!({
            "schema_version": 1,
            "decision": "wait",
            "reason": "Wait for the next finite evidence window",
            "requested_wait_minutes": minutes,
            "expected_evidence": ["fresh checkpoint"]
        })
        .to_string();
        harness.persist_ready_decision(&decision, "wait", 200);
        harness.decision_json = decision;
        harness
    }

    fn with_ready_code_change() -> Self {
        let mut harness = Self::with_terminal_source(ExperimentStatus::Succeeded, None);
        let decision = harness.code_change_decision();
        harness.persist_ready_decision(&decision, "proposal", 200);
        harness.decision_json = decision;
        harness
    }

    fn with_ready_malformed_decision() -> Self {
        let mut harness = Self::with_terminal_source(ExperimentStatus::Succeeded, None);
        let decision =
            r#"{"schema_version":1,"decision":"proposal","proposal":{"kind":"unknown"}}"#;
        harness.persist_ready_decision(decision, "proposal", 200);
        harness.decision_json = decision.to_owned();
        harness
    }

    fn with_terminal_source(
        status: ExperimentStatus,
        trusted_failure_fingerprint: Option<&str>,
    ) -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("decision-project");
        fs::create_dir_all(root.join(".pueue-agent/logs")).unwrap();
        fs::write(
            root.join(".pueue-agent/config.toml"),
            r#"
project_id = "decision-project"
pueue_group = "decision-group"

[agent]
program = "/bin/echo"
args = ["{prompt}"]
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
        fs::write(
            root.join(".pueue-agent/STATE.md"),
            "Reach validation loss below 0.20\n",
        )
        .unwrap();
        fs::write(root.join(".pueue-agent/instructions.md"), "instructions\n").unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "decision-project",
                &root,
                "decision-group",
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        let objective = state::load_objective(&root).unwrap();
        let baseline = proposals::validate_initial_baseline(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: "Establish the initial campaign baseline".to_owned(),
                source_experiment_id: None,
                argv: vec!["python".to_owned(), "train.py".to_owned()],
                working_directory: ".".to_owned(),
                expected_evidence: Vec::new(),
            },
            &objective.digest,
        )
        .unwrap();
        let campaign_id = "decision-campaign".to_owned();
        let source_experiment_id = "decision-source-experiment".to_owned();
        CampaignRepository::new(&db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id: &campaign_id,
                    project_id: "decision-project",
                    objective: &objective,
                    initial_argv: baseline.argv(),
                    baseline: &baseline,
                    submission_id: "decision-source-submission",
                    experiment_id: &source_experiment_id,
                    proposal_id: "decision-source-proposal",
                    metadata: &json!({}),
                    origin_agent_run_id: None,
                    objective_metric: None,
                    now: 100,
                },
                &CampaignLimits::default(),
            )
            .unwrap();
        let experiments = ExperimentRepository::new(&db);
        experiments
            .mark_submitting(&source_experiment_id, 110)
            .unwrap();
        experiments
            .mark_accepted(
                &source_experiment_id,
                40,
                "pueue-task:v1:decision-source",
                120,
            )
            .unwrap();
        let outcome = match status {
            ExperimentStatus::Succeeded => ExperimentTerminalOutcome::Succeeded,
            ExperimentStatus::Failed => ExperimentTerminalOutcome::Failed {
                failure_code: "training_failed",
                failure_fingerprint: "trusted-fingerprint",
            },
            ExperimentStatus::Cancelled => ExperimentTerminalOutcome::Cancelled,
            _ => panic!("decision source must be terminal"),
        };
        experiments
            .project_terminal_submission(&source_experiment_id, 40, outcome, 150)
            .unwrap();
        db.connect()
            .unwrap()
            .execute(
                "UPDATE experiments SET failure_fingerprint = ?1 WHERE experiment_id = ?2",
                rusqlite::params![trusted_failure_fingerprint, source_experiment_id],
            )
            .unwrap();
        let cycle_id = DecisionRepository::new(&db)
            .ensure_cycle_for_terminal(&campaign_id, &source_experiment_id, 160)
            .unwrap()
            .cycle_id;
        Self {
            temp,
            db,
            root,
            pueue: CountingFakePueue::new(),
            campaign_id,
            source_experiment_id,
            cycle_id,
            decision_json: String::new(),
        }
    }

    fn persist_ready_decision(&self, decision_json: &str, kind: &str, now: i64) {
        let decisions = DecisionRepository::new(&self.db);
        let reservation = decisions
            .reserve_next_attempt("decision-project", &self.cycle_id, now)
            .unwrap()
            .unwrap();
        let context_json = self.valid_decision_context_json();
        let context_digest = digest_text(&context_json);
        decisions
            .store_evidence(&reservation, &context_json, &context_digest, now + 1)
            .unwrap();
        let event = EventRepository::new(&self.db)
            .insert_idempotent(&NewEvent::new(
                "decision-project",
                EventKind::TaskFinished,
                format!("decision-ready-run-{now}"),
                json!({"source":"decision-test"}),
                now + 1,
                now + 1,
            ))
            .unwrap();
        let run = AgentRunRepository::new(&self.db)
            .insert(&NewAgentRun::new(
                "decision-project",
                event.event_id,
                None,
                AgentRunStatus::Running,
                now + 1,
                self.root
                    .join(format!(".pueue-agent/logs/decision-{now}.log")),
            ))
            .unwrap();
        decisions
            .bind_agent_run(&reservation, run.run_id, now + 2)
            .unwrap();
        let digest = parse_and_validate_decision(
            decision_json.as_bytes(),
            &self.objective_digest(),
            CampaignLimits::default(),
        )
        .map(|decision| decision.canonical_digest().to_owned())
        .unwrap_or_else(|_| "persisted-invalid-decision-digest".to_owned());
        decisions
            .store_decision(run.run_id, decision_json, &digest, kind, now + 3)
            .unwrap();
        AgentRunRepository::new(&self.db)
            .finish(
                run.run_id,
                AgentRunStatus::Completed,
                now + 4,
                Some(0),
                None,
            )
            .unwrap();
    }

    fn objective_digest(&self) -> String {
        CampaignRepository::new(&self.db)
            .find_by_id(&self.campaign_id)
            .unwrap()
            .unwrap()
            .objective_digest
    }

    fn valid_decision_context_json(&self) -> String {
        let campaign = CampaignRepository::new(&self.db)
            .find_by_id(&self.campaign_id)
            .unwrap()
            .unwrap();
        let source = ExperimentRepository::new(&self.db)
            .find_by_id(&self.source_experiment_id)
            .unwrap()
            .unwrap();
        json!({
            "schema_version": 1,
            "objective": {
                "text": campaign.objective_text,
                "digest": campaign.objective_digest,
            },
            "source_experiment": {
                "experiment_id": source.experiment_id,
                "proposal_id": source.proposal_id,
                "proposal_kind": "experiment",
                "status": source.status,
                "attempt": source.attempt,
                "command_digest": "decision-source-command-digest",
                "failure_code": source.failure_code,
                "failure_fingerprint": source.failure_fingerprint,
                "created_at": source.created_at,
                "updated_at": source.updated_at,
                "finished_at": source.finished_at,
            },
            "terminal_observation": {
                "task_id": source.pueue_task_id,
                "task_signature": source.task_signature,
                "state": source.status.as_str(),
                "enqueued_at": 110,
                "started_at": 120,
                "ended_at": 150,
                "exit_code": 0,
            },
            "recent_outcomes": {"proposals": [], "experiments": []},
            "budgets": {
                "campaign_state": campaign.state,
                "next_eligible_at": null,
                "rolling_usage": {},
                "experiment_counts": {},
            },
            "intervention": {"pending": []},
            "artifact_hints": [],
        })
        .to_string()
    }

    fn overwrite_decision_context(
        &self,
        context_schema_version: i64,
        context_json: &str,
        context_digest: &str,
    ) {
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE decision_attempts
                 SET context_schema_version = ?1, context_json = ?2, context_digest = ?3
                 WHERE cycle_id = ?4 AND attempt_number = (
                     SELECT MAX(attempt_number) FROM decision_attempts WHERE cycle_id = ?4
                 )",
                rusqlite::params![
                    context_schema_version,
                    context_json,
                    context_digest,
                    self.cycle_id,
                ],
            )
            .unwrap();
    }

    fn decision_states(&self) -> (DecisionCycleState, DecisionAttemptState) {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT dc.state, da.state
                 FROM decision_cycles dc
                 JOIN decision_attempts da ON da.cycle_id = dc.cycle_id
                 WHERE dc.cycle_id = ?1
                 ORDER BY da.attempt_number DESC LIMIT 1",
                [&self.cycle_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }

    fn coordinator(&self) -> DecisionCoordinator<'_, CountingFakePueue> {
        DecisionCoordinator::new(&self.db, &self.pueue, CampaignLimits::default())
    }

    fn child_experiment_count(&self) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM experiments WHERE parent_experiment_id = ?1",
                [&self.source_experiment_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn campaign_state(&self) -> CampaignState {
        CampaignRepository::new(&self.db)
            .find_by_id(&self.campaign_id)
            .unwrap()
            .unwrap()
            .state
    }

    fn complete_campaign_decision_event(&self, now: i64) -> i64 {
        let event = NewEvent::new(
            "decision-project",
            EventKind::CampaignDecision,
            format!("campaign-decision:v1:{}", self.cycle_id),
            json!({
                "source": "terminal_experiment",
                "cycle_id": self.cycle_id,
                "source_experiment_id": self.source_experiment_id,
            }),
            now,
            now,
        )
        .with_campaign_lineage(
            self.campaign_id.clone(),
            Some(self.source_experiment_id.clone()),
        );
        let (_, event) = DecisionRepository::new(&self.db)
            .publish_terminal_cycle_event(
                &self.campaign_id,
                &self.source_experiment_id,
                &event,
                now,
            )
            .unwrap();
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE events
                 SET status = 'completed', attempts = 1, completed_at = ?1
                 WHERE event_id = ?2",
                rusqlite::params![now, event.event_id],
            )
            .unwrap();
        event.event_id
    }

    fn event_status_and_attempts(&self, event_id: i64) -> (EventStatus, i64) {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status, attempts FROM events WHERE event_id = ?1",
                [event_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }

    fn enable_failpoint(&self, failpoint: DecisionFailpoint) {
        let sql = match failpoint {
            DecisionFailpoint::AfterIntentAcceptBeforeDecisionComplete => {
                "CREATE TRIGGER decision_fail_before_cycle_completion
                 BEFORE UPDATE OF state ON decision_cycles
                 WHEN OLD.state = 'analyzing' AND NEW.state = 'completed'
                 BEGIN SELECT RAISE(ABORT, 'injected before decision cycle completion'); END;"
            }
            DecisionFailpoint::AfterDecisionCommit => {
                "CREATE TRIGGER decision_fail_after_commit
                 BEFORE UPDATE OF status ON experiments
                 WHEN OLD.status = 'reserved' AND NEW.status = 'submitting'
                 BEGIN SELECT RAISE(ABORT, 'injected after-decision-commit failure'); END;"
            }
            DecisionFailpoint::AfterPueueAdd => {
                "CREATE TRIGGER decision_fail_after_pueue_add
                 BEFORE UPDATE OF status ON experiments
                 WHEN OLD.status = 'submitting' AND NEW.status = 'accepted'
                 BEGIN SELECT RAISE(ABORT, 'injected after-pueue-add failure'); END;"
            }
        };
        self.db.connect().unwrap().execute_batch(sql).unwrap();
    }

    fn disable_failpoints(&self) {
        self.db
            .connect()
            .unwrap()
            .execute_batch(
                "DROP TRIGGER IF EXISTS decision_fail_before_cycle_completion;
                 DROP TRIGGER IF EXISTS decision_fail_after_commit;
                 DROP TRIGGER IF EXISTS decision_fail_after_pueue_add;",
            )
            .unwrap();
    }

    fn code_change_decision(&self) -> String {
        json!({
            "schema_version": 1,
            "decision": "proposal",
            "proposal": {
                "kind": "code_change",
                "hypothesis": "Edit the training source",
                "source_experiment_id": self.source_experiment_id,
                "argv": ["python", "train.py"],
                "working_directory": ".",
                "expected_evidence": []
            }
        })
        .to_string()
    }

    fn persist_duplicate_as_terminal(&self) {
        let decision = parse_and_validate_decision(
            self.decision_json.as_bytes(),
            &self.objective_digest(),
            CampaignLimits::default(),
        )
        .unwrap();
        let pueue_agent::decision_protocol::ValidatedDecision::Proposal(proposal) = decision else {
            panic!("duplicate fixture requires a proposal");
        };
        let intent = CampaignRepository::new(&self.db)
            .accept_proposal(
                &self.campaign_id,
                "existing-duplicate-proposal",
                "existing-duplicate-experiment",
                "existing-duplicate-submission",
                &proposal,
                &CampaignLimits::default(),
                250,
            )
            .unwrap()
            .accepted()
            .unwrap();
        let experiments = ExperimentRepository::new(&self.db);
        experiments
            .mark_submitting(&intent.experiment.experiment_id, 251)
            .unwrap();
        experiments
            .mark_accepted(
                &intent.experiment.experiment_id,
                39,
                "pueue-task:v1:existing-duplicate",
                252,
            )
            .unwrap();
        experiments
            .project_terminal_submission(
                &intent.experiment.experiment_id,
                39,
                ExperimentTerminalOutcome::Succeeded,
                253,
            )
            .unwrap();
    }

    #[cfg(unix)]
    async fn restart_daemon_once(&self) -> Result<(), AppError> {
        let configured_program = execution_policy_fixture::prepare_configured_program(
            self.temp.path(),
            "decision-project",
            &self.root.join(".pueue-agent/config.toml"),
        );
        let policy = execution_policy_fixture::resolved_policy(
            self.temp.path(),
            &[("decision-project", self.root.as_path(), configured_program.as_path())],
        );
        let runner = AgentRunner::new(
            AgentRunnerConfig::production()
                .with_codex_capabilities(pueue_agent::codex_command::CodexCapabilities::all()),
            policy.clone(),
        );
        Daemon::new(
            self.db.clone(),
            self.pueue.clone(),
            policy,
            runner,
            DaemonConfig {
                interval: Duration::from_secs(60),
                lease_seconds: 60,
                claim_limit: 10,
                now_override: Some(301),
                shutdown_grace_period: Duration::from_secs(1),
            },
        )
        .run_once()
        .await?;
        Ok(())
    }
}

fn digest_text(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

#[cfg(unix)]
fn initialize_clean_git(root: &std::path::Path) -> String {
    for args in [
        ["init", "-q"].as_slice(),
        ["add", "."].as_slice(),
        [
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "baseline",
        ]
        .as_slice(),
    ] {
        let output = Command::new("/usr/bin/git")
            .args(args)
            .current_dir(root)
            .output()
            .unwrap();
        assert!(output.status.success(), "git {:?}: {:?}", args, output);
    }
    String::from_utf8(
        Command::new("/usr/bin/git")
            .args(["rev-parse", "HEAD"])
            .current_dir(root)
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned()
}

#[tokio::test]
async fn successful_terminal_decision_adds_exactly_one_next_experiment() {
    let harness = DecisionHarness::with_ready_proposal(ExperimentStatus::Succeeded);

    let first = harness.coordinator().apply_ready(300, 10).await.unwrap();
    let second = harness.coordinator().apply_ready(301, 10).await.unwrap();

    assert_eq!(first.proposals_applied, 1);
    assert_eq!(second.proposals_applied, 0);
    assert_eq!(harness.pueue.add_calls(), 1);
    assert_eq!(harness.child_experiment_count(), 1);
}

#[tokio::test]
async fn failed_terminal_decision_allows_trusted_repair_and_rejects_untrusted_repair() {
    let trusted = DecisionHarness::with_ready_repair(Some("trusted-fingerprint"));
    assert_eq!(
        trusted
            .coordinator()
            .apply_ready(300, 10)
            .await
            .unwrap()
            .proposals_applied,
        1
    );
    assert_eq!(trusted.pueue.add_calls(), 1);

    let untrusted = DecisionHarness::with_ready_repair(None);
    let report = untrusted.coordinator().apply_ready(300, 10).await.unwrap();
    assert_eq!(report.proposals_applied, 0);
    assert_eq!(untrusted.pueue.add_calls(), 0);
    assert_eq!(
        untrusted.decision_states(),
        (DecisionCycleState::Pending, DecisionAttemptState::Failed)
    );
}

#[tokio::test]
async fn repair_decision_rejects_nonfailed_sources_even_with_a_stale_fingerprint() {
    for status in [ExperimentStatus::Succeeded, ExperimentStatus::Cancelled] {
        let harness =
            DecisionHarness::with_ready_repair_source(status, Some("stale-fingerprint"));

        let report = harness.coordinator().apply_ready(300, 10).await.unwrap();

        assert_eq!(report.proposals_applied, 0);
        assert_eq!(harness.pueue.add_calls(), 0);
        assert_eq!(harness.child_experiment_count(), 0);
        assert_eq!(
            harness.decision_states(),
            (DecisionCycleState::Pending, DecisionAttemptState::Failed)
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn decision_application_rejects_tampered_context_before_project_admission() {
    let harness = DecisionHarness::with_ready_proposal(ExperimentStatus::Succeeded);
    let original = harness.valid_decision_context_json();
    harness.overwrite_decision_context(1, &format!("{original} "), &digest_text(&original));
    let guard = fs::File::open(&harness.root).unwrap();
    unsafe extern "C" {
        fn flock(file_descriptor: std::os::raw::c_int, operation: std::os::raw::c_int)
            -> std::os::raw::c_int;
    }
    assert_eq!(unsafe { flock(guard.as_raw_fd(), 2) }, 0);

    let report = harness.coordinator().apply_ready(300, 10).await.unwrap();

    assert_eq!(report.deferred, 0);
    assert_eq!(harness.pueue.add_calls(), 0);
    assert_eq!(harness.child_experiment_count(), 0);
    assert_eq!(
        harness.decision_states(),
        (DecisionCycleState::Pending, DecisionAttemptState::Failed)
    );
}

#[tokio::test]
async fn decision_application_rejects_context_schema_bound_and_lineage_mutations() {
    for mutation in [
        "attempt-schema",
        "embedded-schema",
        "objective",
        "source",
        "unknown-field",
        "oversize",
    ] {
        let harness = DecisionHarness::with_ready_proposal(ExperimentStatus::Succeeded);
        let mut context: serde_json::Value =
            serde_json::from_str(&harness.valid_decision_context_json()).unwrap();
        let attempt_schema = match mutation {
            "attempt-schema" => 2,
            "embedded-schema" => {
                context["schema_version"] = json!(2);
                1
            }
            "objective" => {
                context["objective"]["digest"] = json!("stale-objective-digest");
                1
            }
            "source" => {
                context["source_experiment"]["experiment_id"] =
                    json!("different-source-experiment");
                1
            }
            "unknown-field" => {
                context["unknown"] = json!(true);
                1
            }
            "oversize" => 1,
            _ => unreachable!(),
        };
        let context_json = if mutation == "oversize" {
            "x".repeat(MAX_DECISION_CONTEXT_BYTES + 1)
        } else {
            context.to_string()
        };
        harness.overwrite_decision_context(
            attempt_schema,
            &context_json,
            &digest_text(&context_json),
        );

        let report = harness.coordinator().apply_ready(300, 10).await.unwrap();

        assert_eq!(report.proposals_applied, 0, "mutation={mutation}");
        assert_eq!(harness.pueue.add_calls(), 0, "mutation={mutation}");
        assert_eq!(harness.child_experiment_count(), 0, "mutation={mutation}");
        assert_eq!(
            harness.decision_states(),
            (DecisionCycleState::Pending, DecisionAttemptState::Failed),
            "mutation={mutation}"
        );
    }
}

#[tokio::test]
async fn wait_decision_adds_no_task_and_wakes_same_cycle_at_the_finite_deadline() {
    let harness = DecisionHarness::with_ready_wait(1);
    let event_id = harness.complete_campaign_decision_event(299);

    assert_eq!(
        harness
            .coordinator()
            .apply_ready(300, 10)
            .await
            .unwrap()
            .waits_scheduled,
        1
    );
    assert_eq!(harness.pueue.add_calls(), 0);
    assert!(
        DecisionRepository::new(&harness.db)
            .due_cycles(359, 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        harness.event_status_and_attempts(event_id),
        (EventStatus::Completed, 1)
    );
    assert_eq!(
        DecisionRepository::new(&harness.db)
            .due_cycles(360, 10)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        harness.event_status_and_attempts(event_id),
        (EventStatus::Pending, 0)
    );
}

#[cfg(unix)]
#[tokio::test]
async fn wait_decision_defers_while_project_admission_is_locked() {
    let harness = DecisionHarness::with_ready_wait(1);
    let guard = fs::File::open(&harness.root).unwrap();
    unsafe extern "C" {
        fn flock(file_descriptor: std::os::raw::c_int, operation: std::os::raw::c_int)
            -> std::os::raw::c_int;
    }
    assert_eq!(unsafe { flock(guard.as_raw_fd(), 2) }, 0);

    let report = harness.coordinator().apply_ready(300, 10).await.unwrap();

    assert_eq!(report.waits_scheduled, 0);
    assert_eq!(report.deferred, 1);
    assert_eq!(harness.pueue.add_calls(), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn restart_after_decision_or_pueue_add_never_duplicates_the_external_add() {
    for point in [
        DecisionFailpoint::AfterDecisionCommit,
        DecisionFailpoint::AfterPueueAdd,
    ] {
        let harness = DecisionHarness::with_ready_proposal(ExperimentStatus::Succeeded);
        harness.enable_failpoint(point);
        assert!(harness.coordinator().apply_ready(300, 10).await.is_err());
        harness.disable_failpoints();

        harness.restart_daemon_once().await.unwrap();

        assert_eq!(harness.pueue.add_calls(), 1);
        assert_eq!(harness.child_experiment_count(), 1);
    }
}

#[tokio::test]
async fn same_decision_attempt_replays_its_exact_accepted_intent_after_interruption() {
    let harness = DecisionHarness::with_ready_proposal(ExperimentStatus::Succeeded);
    harness.enable_failpoint(DecisionFailpoint::AfterIntentAcceptBeforeDecisionComplete);

    assert!(harness.coordinator().apply_ready(300, 10).await.is_err());
    assert_eq!(
        harness.decision_states(),
        (DecisionCycleState::Analyzing, DecisionAttemptState::Decided)
    );
    assert_eq!(harness.child_experiment_count(), 1);
    assert_eq!(harness.pueue.add_calls(), 0);
    harness.disable_failpoints();

    let replay = harness.coordinator().apply_ready(301, 10).await.unwrap();

    assert_eq!(replay.proposals_applied, 1);
    assert_eq!(
        harness.decision_states(),
        (DecisionCycleState::Completed, DecisionAttemptState::Decided)
    );
    assert_eq!(harness.child_experiment_count(), 1);
    assert_eq!(harness.pueue.add_calls(), 1);
}

#[tokio::test]
async fn code_change_decision_is_rejected_before_submission() {
    let harness = DecisionHarness::with_ready_code_change();

    let report = harness.coordinator().apply_ready(300, 10).await.unwrap();

    assert_eq!(report.proposals_applied, 0);
    assert_eq!(harness.pueue.add_calls(), 0);
    assert_eq!(harness.child_experiment_count(), 0);
    assert_eq!(
        harness.decision_states(),
        (DecisionCycleState::Completed, DecisionAttemptState::Decided)
    );
    let proposal = ProposalRepository::new(&harness.db)
        .list_for_campaign(&harness.campaign_id, 10)
        .unwrap()
        .into_iter()
        .find(|proposal| proposal.kind == ProposalKind::CodeChange)
        .unwrap();
    assert_eq!(proposal.status, ProposalStatus::Rejected);
}

#[cfg(unix)]
#[tokio::test]
async fn code_change_admission_releases_lock_without_pueue_add() {
    let harness = DecisionHarness::with_ready_code_change();
    let expected = initialize_clean_git(&harness.root);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![expected, harness.campaign_id],
        )
        .unwrap();
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let policy = execution_policy_fixture::resolved_policy(
        harness.temp.path(),
        &[(
            "decision-project",
            canonical_root.as_path(),
            Path::new("codex"),
        )],
    );

    let report = DecisionCoordinator::new(&harness.db, &harness.pueue, policy.campaign_limits)
        .with_policy(&policy)
        .apply_ready(300, 10)
        .await
        .unwrap();

    assert_eq!(report.proposals_applied, 1);
    assert_eq!(harness.pueue.add_calls(), 0);
    let runs = CodeChangeRepository::new(&harness.db)
        .list_recoverable(10)
        .unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].state, CodeChangeState::Reserved);

    let lock_file = fs::File::open(&harness.root).unwrap();
    assert_eq!(
        unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
}

#[cfg(unix)]
#[tokio::test]
async fn code_change_admission_uses_campaign_base_when_best_ref_is_absent() {
    let harness = DecisionHarness::with_ready_code_change();
    let expected = initialize_clean_git(&harness.root);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![expected, harness.campaign_id],
        )
        .unwrap();
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let policy = execution_policy_fixture::resolved_policy(
        harness.temp.path(),
        &[("decision-project", canonical_root.as_path(), Path::new("codex"))],
    );

    DecisionCoordinator::new(&harness.db, &harness.pueue, policy.campaign_limits)
        .with_policy(&policy)
        .apply_ready(300, 10)
        .await
        .unwrap();

    let proposal = ProposalRepository::new(&harness.db)
        .list_for_campaign(&harness.campaign_id, 10)
        .unwrap()
        .into_iter()
        .find(|proposal| proposal.kind == ProposalKind::CodeChange)
        .unwrap();
    let run = CodeChangeRepository::new(&harness.db)
        .find_by_proposal(&proposal.proposal_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.base_sha, expected);
}

#[cfg(unix)]
#[tokio::test]
async fn invalid_present_best_ref_rejects_code_change() {
    let harness = DecisionHarness::with_ready_code_change();
    let expected = initialize_clean_git(&harness.root);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![expected, harness.campaign_id],
        )
        .unwrap();
    let best = code_change::best_ref(&harness.campaign_id).unwrap();
    let best_path = harness
        .root
        .join(".git")
        .join("refs/heads")
        .join(best);
    fs::create_dir_all(best_path.parent().unwrap()).unwrap();
    fs::write(best_path, b"not-a-ref\n").unwrap();
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let policy = execution_policy_fixture::resolved_policy(
        harness.temp.path(),
        &[("decision-project", canonical_root.as_path(), Path::new("codex"))],
    );

    let report = DecisionCoordinator::new(&harness.db, &harness.pueue, policy.campaign_limits)
        .with_policy(&policy)
        .apply_ready(300, 10)
        .await
        .unwrap();
    assert_eq!(report.proposals_applied, 0);
    assert_eq!(report.deferred, 0);
    assert_eq!(harness.pueue.add_calls(), 0);
    let proposal = ProposalRepository::new(&harness.db)
        .list_for_campaign(&harness.campaign_id, 10)
        .unwrap()
        .into_iter()
        .find(|proposal| proposal.kind == ProposalKind::CodeChange)
        .unwrap();
    assert_eq!(proposal.status, ProposalStatus::Rejected);
    assert_eq!(
        proposal.reject_reason.as_deref(),
        Some("best_ref_invalid")
    );
    assert!(CodeChangeRepository::new(&harness.db)
        .find_by_proposal(&proposal.proposal_id)
        .unwrap()
        .is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_best_ref_parent_rejects_code_change() {
    let harness = DecisionHarness::with_ready_code_change();
    let expected = initialize_clean_git(&harness.root);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![expected, harness.campaign_id],
        )
        .unwrap();
    let best = code_change::best_ref(&harness.campaign_id).unwrap();
    let outside = harness.temp.path().join("outside-refs");
    let outside_best = outside.join(best.strip_prefix("campaign/").unwrap());
    fs::create_dir_all(outside_best.parent().unwrap()).unwrap();
    fs::write(&outside_best, format!("{expected}\n")).unwrap();
    let campaign_parent = harness.root.join(".git/refs/heads/campaign");
    fs::create_dir_all(campaign_parent.parent().unwrap()).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, &campaign_parent).unwrap();
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let policy = execution_policy_fixture::resolved_policy(
        harness.temp.path(),
        &[("decision-project", canonical_root.as_path(), Path::new("codex"))],
    );

    let report = DecisionCoordinator::new(&harness.db, &harness.pueue, policy.campaign_limits)
        .with_policy(&policy)
        .apply_ready(300, 10)
        .await
        .unwrap();

    assert_eq!(report.proposals_applied, 0);
    assert_eq!(report.deferred, 0);
    let proposal = ProposalRepository::new(&harness.db)
        .list_for_campaign(&harness.campaign_id, 10)
        .unwrap()
        .into_iter()
        .find(|proposal| proposal.kind == ProposalKind::CodeChange)
        .unwrap();
    assert_eq!(proposal.status, ProposalStatus::Rejected);
    assert_eq!(
        proposal.reject_reason.as_deref(),
        Some("best_ref_invalid")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn same_name_best_tag_is_ignored_for_code_change_base_resolution() {
    let harness = DecisionHarness::with_ready_code_change();
    let expected = initialize_clean_git(&harness.root);
    let second = Command::new("/usr/bin/git")
        .args([
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--allow-empty",
            "-qm",
            "second",
        ])
        .current_dir(&harness.root)
        .output()
        .unwrap();
    assert!(second.status.success(), "git commit: {:?}", second);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![expected, harness.campaign_id],
        )
        .unwrap();
    let best = code_change::best_ref(&harness.campaign_id).unwrap();
    let tag = Command::new("/usr/bin/git")
        .args(["tag", &best])
        .current_dir(&harness.root)
        .output()
        .unwrap();
    assert!(tag.status.success(), "git tag: {:?}", tag);
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let policy = execution_policy_fixture::resolved_policy(
        harness.temp.path(),
        &[("decision-project", canonical_root.as_path(), Path::new("codex"))],
    );

    let report = DecisionCoordinator::new(&harness.db, &harness.pueue, policy.campaign_limits)
        .with_policy(&policy)
        .apply_ready(300, 10)
        .await
        .unwrap();

    assert_eq!(report.proposals_applied, 1);
    let proposal = ProposalRepository::new(&harness.db)
        .list_for_campaign(&harness.campaign_id, 10)
        .unwrap()
        .into_iter()
        .find(|proposal| proposal.kind == ProposalKind::CodeChange)
        .unwrap();
    let run = CodeChangeRepository::new(&harness.db)
        .find_by_proposal(&proposal.proposal_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.base_sha, expected);
}

#[cfg(unix)]
#[tokio::test]
async fn packed_best_branch_is_selected_for_code_change_base_resolution() {
    let harness = DecisionHarness::with_ready_code_change();
    let expected = initialize_clean_git(&harness.root);
    let second = Command::new("/usr/bin/git")
        .args([
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--allow-empty",
            "-qm",
            "second",
        ])
        .current_dir(&harness.root)
        .output()
        .unwrap();
    assert!(second.status.success(), "git commit: {:?}", second);
    let alternate = String::from_utf8(
        Command::new("/usr/bin/git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&harness.root)
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    assert_ne!(expected, alternate);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![alternate, harness.campaign_id],
        )
        .unwrap();
    let best = code_change::best_ref(&harness.campaign_id).unwrap();
    let best_ref = format!("refs/heads/{best}");
    let update = Command::new("/usr/bin/git")
        .args(["update-ref", &best_ref, &expected])
        .current_dir(&harness.root)
        .output()
        .unwrap();
    assert!(update.status.success(), "git update-ref: {:?}", update);
    let packed = Command::new("/usr/bin/git")
        .args(["pack-refs", "--all", "--prune"])
        .current_dir(&harness.root)
        .output()
        .unwrap();
    assert!(packed.status.success(), "git pack-refs: {:?}", packed);
    assert!(!harness
        .root
        .join(".git")
        .join("refs/heads")
        .join(&best)
        .exists());
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let policy = execution_policy_fixture::resolved_policy(
        harness.temp.path(),
        &[("decision-project", canonical_root.as_path(), Path::new("codex"))],
    );

    let report = DecisionCoordinator::new(&harness.db, &harness.pueue, policy.campaign_limits)
        .with_policy(&policy)
        .apply_ready(300, 10)
        .await
        .unwrap();

    assert_eq!(report.proposals_applied, 1);
    let proposal = ProposalRepository::new(&harness.db)
        .list_for_campaign(&harness.campaign_id, 10)
        .unwrap()
        .into_iter()
        .find(|proposal| proposal.kind == ProposalKind::CodeChange)
        .unwrap();
    let run = CodeChangeRepository::new(&harness.db)
        .find_by_proposal(&proposal.proposal_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.base_sha, expected);
}

#[cfg(unix)]
#[tokio::test]
async fn corrupt_packed_best_ref_rejects_without_campaign_base_fallback() {
    let harness = DecisionHarness::with_ready_code_change();
    let expected = initialize_clean_git(&harness.root);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![expected, harness.campaign_id],
        )
        .unwrap();
    let best = code_change::best_ref(&harness.campaign_id).unwrap();
    fs::write(
        harness.root.join(".git/packed-refs"),
        format!("# pack-refs with: peeled fully-peeled sorted\nnot-a-ref refs/heads/{best}\n"),
    )
    .unwrap();
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let policy = execution_policy_fixture::resolved_policy(
        harness.temp.path(),
        &[("decision-project", canonical_root.as_path(), Path::new("codex"))],
    );

    let report = DecisionCoordinator::new(&harness.db, &harness.pueue, policy.campaign_limits)
        .with_policy(&policy)
        .apply_ready(300, 10)
        .await
        .unwrap();

    assert_eq!(report.proposals_applied, 0);
    assert_eq!(report.deferred, 0);
    assert_eq!(harness.pueue.add_calls(), 0);
    let proposal = ProposalRepository::new(&harness.db)
        .list_for_campaign(&harness.campaign_id, 10)
        .unwrap()
        .into_iter()
        .find(|proposal| proposal.kind == ProposalKind::CodeChange)
        .unwrap();
    assert_eq!(proposal.status, ProposalStatus::Rejected);
    assert_eq!(
        proposal.reject_reason.as_deref(),
        Some("best_ref_invalid")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn duplicate_orphan_packed_best_ref_records_reject_without_campaign_base_fallback() {
    for record in ["duplicate", "orphan-peeled"] {
        let harness = DecisionHarness::with_ready_code_change();
        let expected = initialize_clean_git(&harness.root);
        harness
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
                rusqlite::params![expected, harness.campaign_id],
            )
            .unwrap();
        let best = code_change::best_ref(&harness.campaign_id).unwrap();
        let contents = if record == "duplicate" {
            format!("{expected} refs/heads/{best}\n{expected} refs/heads/{best}\n")
        } else {
            format!("^{}\n", "b".repeat(40))
        };
        fs::write(harness.root.join(".git/packed-refs"), contents).unwrap();
        let canonical_root = fs::canonicalize(&harness.root).unwrap();
        let policy = execution_policy_fixture::resolved_policy(
            harness.temp.path(),
            &[("decision-project", canonical_root.as_path(), Path::new("codex"))],
        );

        let report = DecisionCoordinator::new(&harness.db, &harness.pueue, policy.campaign_limits)
            .with_policy(&policy)
            .apply_ready(300, 10)
            .await
            .unwrap();

        assert_eq!(report.proposals_applied, 0, "record: {record}");
        assert_eq!(report.deferred, 0, "record: {record}");
        let proposal = ProposalRepository::new(&harness.db)
            .list_for_campaign(&harness.campaign_id, 10)
            .unwrap()
            .into_iter()
            .find(|proposal| proposal.kind == ProposalKind::CodeChange)
            .unwrap();
        assert_eq!(proposal.status, ProposalStatus::Rejected, "record: {record}");
        assert_eq!(
            proposal.reject_reason.as_deref(),
            Some("best_ref_invalid"),
            "record: {record}"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn duplicate_code_change_digest_reuses_durable_outcome() {
    let harness = DecisionHarness::with_ready_code_change();
    let expected = initialize_clean_git(&harness.root);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![expected, harness.campaign_id],
        )
        .unwrap();
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let policy = execution_policy_fixture::resolved_policy(
        harness.temp.path(),
        &[(
            "decision-project",
            canonical_root.as_path(),
            Path::new("codex"),
        )],
    );
    let coordinator = || {
        DecisionCoordinator::new(&harness.db, &harness.pueue, policy.campaign_limits)
            .with_policy(&policy)
    };

    let first = coordinator().apply_ready(300, 10).await.unwrap();
    assert_eq!(first.proposals_applied, 1);
    let reservations_before: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations WHERE dimension = 'code_change'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE decision_cycles
             SET state = 'pending', next_wake_at = NULL, consecutive_failed_attempts = 0
             WHERE cycle_id = ?1",
            [&harness.cycle_id],
        )
        .unwrap();
    harness.persist_ready_decision(&harness.code_change_decision(), "proposal", 400);
    let second = coordinator().apply_ready(500, 10).await.unwrap();

    assert_eq!(second.proposals_applied, 1);
    assert_eq!(harness.pueue.add_calls(), 0);
    let reservations_after: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations WHERE dimension = 'code_change'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reservations_after, reservations_before);
    assert_eq!(
        CodeChangeRepository::new(&harness.db)
            .list_recoverable(10)
            .unwrap()
            .len(),
        1
    );
}

#[cfg(unix)]
#[tokio::test]
async fn rejected_code_change_replay_is_not_reported_applied() {
    let harness = DecisionHarness::with_ready_code_change();
    let expected = initialize_clean_git(&harness.root);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![expected, harness.campaign_id],
        )
        .unwrap();
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let policy = execution_policy_fixture::resolved_policy(
        harness.temp.path(),
        &[("decision-project", canonical_root.as_path(), Path::new("codex"))],
    );

    let first = DecisionCoordinator::new(&harness.db, &harness.pueue, policy.campaign_limits)
        .with_policy(&policy)
        .apply_ready(300, 10)
        .await
        .unwrap();
    assert_eq!(first.proposals_applied, 1);
    let proposal = ProposalRepository::new(&harness.db)
        .list_for_campaign(&harness.campaign_id, 10)
        .unwrap()
        .into_iter()
        .find(|proposal| proposal.kind == ProposalKind::CodeChange)
        .unwrap();
    let run = CodeChangeRepository::new(&harness.db)
        .find_by_proposal(&proposal.proposal_id)
        .unwrap()
        .unwrap();
    CodeChangeRepository::new(&harness.db)
        .reject(&run.code_change_run_id, "check_failed", "checks failed", 350)
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE decision_cycles
             SET state = 'pending', next_wake_at = NULL, consecutive_failed_attempts = 0
             WHERE cycle_id = ?1",
            [&harness.cycle_id],
        )
        .unwrap();
    harness.persist_ready_decision(&harness.code_change_decision(), "proposal", 400);

    let second = DecisionCoordinator::new(&harness.db, &harness.pueue, policy.campaign_limits)
        .with_policy(&policy)
        .apply_ready(500, 10)
        .await
        .unwrap();
    assert_eq!(second.proposals_applied, 0);
    assert_eq!(second.deferred, 0);
    assert_eq!(harness.pueue.add_calls(), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn orphan_pending_code_change_replay_is_durably_rejected() {
    let harness = DecisionHarness::with_ready_code_change();
    let expected = initialize_clean_git(&harness.root);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![expected, harness.campaign_id],
        )
        .unwrap();
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let policy = execution_policy_fixture::resolved_policy(
        harness.temp.path(),
        &[("decision-project", canonical_root.as_path(), Path::new("codex"))],
    );
    DecisionCoordinator::new(&harness.db, &harness.pueue, policy.campaign_limits)
        .with_policy(&policy)
        .apply_ready(300, 10)
        .await
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute("DELETE FROM code_change_runs WHERE campaign_id = ?1", [&harness.campaign_id])
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE decision_cycles
             SET state = 'pending', next_wake_at = NULL, consecutive_failed_attempts = 0
             WHERE cycle_id = ?1",
            [&harness.cycle_id],
        )
        .unwrap();
    harness.persist_ready_decision(&harness.code_change_decision(), "proposal", 400);

    let second = DecisionCoordinator::new(&harness.db, &harness.pueue, policy.campaign_limits)
        .with_policy(&policy)
        .apply_ready(500, 10)
        .await
        .unwrap();
    assert_eq!(second.proposals_applied, 0);
    assert_eq!(second.deferred, 0);
    assert_eq!(harness.pueue.add_calls(), 0);
    let proposal = ProposalRepository::new(&harness.db)
        .list_for_campaign(&harness.campaign_id, 10)
        .unwrap()
        .into_iter()
        .find(|proposal| proposal.kind == ProposalKind::CodeChange)
        .unwrap();
    assert_eq!(proposal.status, ProposalStatus::Rejected);
    let event = EventRepository::new(&harness.db)
        .find_by_dedup_key(
            "decision-project",
            &format!("code-change-proposal:v1:{}:rejected", proposal.proposal_id),
        )
        .unwrap()
        .unwrap();
    assert_eq!(event.status, EventStatus::Completed);
}

#[test]
fn legacy_experiment_acceptance_wrapper_rejects_code_change_without_outcome() {
    let harness = DecisionHarness::with_terminal_source(ExperimentStatus::Succeeded, None);
    let proposal = proposals::validate(
        ProposalInput {
            kind: ProposalKind::CodeChange,
            hypothesis: "Change the training implementation".to_owned(),
            source_experiment_id: Some(harness.source_experiment_id.clone()),
            argv: vec!["python".to_owned(), "train.py".to_owned()],
            working_directory: ".".to_owned(),
            expected_evidence: vec!["validation loss".to_owned()],
        },
        &harness.objective_digest(),
    )
    .unwrap();
    let error = CampaignRepository::new(&harness.db)
        .accept_proposal(
            &harness.campaign_id,
            "legacy-code-change-proposal",
            "legacy-code-change-experiment",
            "legacy-code-change-submission",
            &proposal,
            &CampaignLimits::default(),
            200,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        AppError::Validation {
            field: "proposal.kind",
            ..
        }
    ));
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM proposals WHERE proposal_id = 'legacy-code-change-proposal'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn duplicate_proposal_from_a_different_decision_attempt_is_rejected_for_fresh_context() {
    let harness = DecisionHarness::with_ready_proposal(ExperimentStatus::Succeeded);
    harness.persist_duplicate_as_terminal();

    let report = harness.coordinator().apply_ready(300, 10).await.unwrap();

    assert_eq!(report.proposals_applied, 0);
    assert_eq!(harness.pueue.add_calls(), 0);
    assert_eq!(harness.child_experiment_count(), 1);
    assert_eq!(
        harness.decision_states(),
        (DecisionCycleState::Pending, DecisionAttemptState::Failed)
    );
}

#[tokio::test]
async fn malformed_decision_attempt_is_rejected_without_submission() {
    let harness = DecisionHarness::with_ready_malformed_decision();

    let report = harness.coordinator().apply_ready(300, 10).await.unwrap();

    assert_eq!(report.degraded, 0);
    assert_eq!(harness.campaign_state(), CampaignState::Active);
    assert_eq!(harness.pueue.add_calls(), 0);
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
    // managed baseline must be wrapped via /usr/bin/env + four derived vars
    let stored = SubmissionRepository::new(&harness.db)
        .find_by_id(&result.submission_id)
        .unwrap()
        .unwrap();
    assert_eq!(stored.argv, vec!["python".to_owned(), "train.py".to_owned()]);
    let add_args = harness.fake.last_add_args();
    let sep = add_args.iter().position(|a| a == "--").unwrap();
    let runtime = &add_args[sep + 1..];
    assert_eq!(runtime[0], OsString::from("/usr/bin/env"));
    assert_eq!(runtime.len(), 1 + 4 + 2);
    assert_eq!(&runtime[5..], &[OsString::from("python"), OsString::from("train.py")]);
    // verify durable group/root still used (not caller mutated) – wrapper correctness is covered by dedicated test
    assert_eq!(add_args[1], OsString::from("pa-project"));
}

#[cfg(unix)]
#[tokio::test]
async fn experiment_submit_with_root_anchor_persists_clean_head() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    for args in [
        ["init", "-q"].as_slice(),
        ["add", "."].as_slice(),
        [
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "baseline",
        ]
        .as_slice(),
    ] {
        let output = Command::new("/usr/bin/git")
            .args(args)
            .current_dir(&harness.root)
            .output()
            .unwrap();
        assert!(output.status.success(), "git {:?}: {:?}", args, output);
    }
    let expected = String::from_utf8(
        Command::new("/usr/bin/git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&harness.root)
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    let policy = execution_policy_fixture::resolved_policy(
        harness._temp.path(),
        &[("project-a", harness.root.as_path(), Path::new("codex"))],
    );
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let root_anchor = policy
        .project_root_anchor(&canonical_root)
        .map_err(AppError::from)
        .unwrap();
    let limits = policy.campaign_limits;
    submit::run_with_options_with_root_anchor(
        &harness.db,
        &harness.root,
        &[OsString::from("python"), OsString::from("train.py")],
        &submit::SubmitOptions::default(),
        &limits,
        &harness.fake,
        root_anchor,
        policy,
    )
    .await
    .unwrap();
    let campaign = CampaignRepository::new(&harness.db)
        .find_live_by_project("project-a")
        .unwrap()
        .unwrap();
    assert_eq!(
        campaign.base_revision_sha.as_deref(),
        Some(expected.as_str())
    );
}

#[cfg(unix)]
#[tokio::test]
async fn experiment_submit_with_root_anchor_dirty_git_preserves_null_base() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    initialize_clean_git(&harness.root);
    fs::write(
        harness.root.join(".git/config"),
        "[status]\n\tshowUntrackedFiles = no\n",
    )
    .unwrap();
    fs::write(harness.root.join("dirty.txt"), "working tree change\n").unwrap();
    let policy = execution_policy_fixture::resolved_policy(
        harness._temp.path(),
        &[("project-a", harness.root.as_path(), Path::new("codex"))],
    );
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let root_anchor = policy
        .project_root_anchor(&canonical_root)
        .map_err(AppError::from)
        .unwrap();
    let limits = policy.campaign_limits;
    submit::run_with_options_with_root_anchor(
        &harness.db,
        &harness.root,
        &[OsString::from("python"), OsString::from("train.py")],
        &submit::SubmitOptions::default(),
        &limits,
        &harness.fake,
        root_anchor,
        policy,
    )
    .await
    .unwrap();
    let campaign = CampaignRepository::new(&harness.db)
        .find_live_by_project("project-a")
        .unwrap()
        .unwrap();
    assert_eq!(campaign.base_revision_sha, None);
    assert_eq!(harness.fake.add_calls(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn experiment_submit_with_root_anchor_does_not_execute_a_local_clean_filter() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    initialize_clean_git(&harness.root);
    fs::write(
        harness.root.join(".gitattributes"),
        "tracked.txt filter=clean\n",
    )
    .unwrap();
    fs::write(harness.root.join("tracked.txt"), "baseline\n").unwrap();
    for args in [
        ["add", ".gitattributes", "tracked.txt"].as_slice(),
        [
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "filter-baseline",
        ]
        .as_slice(),
    ] {
        let output = Command::new("/usr/bin/git")
            .args(args)
            .current_dir(&harness.root)
            .output()
            .unwrap();
        assert!(output.status.success(), "git {:?}: {:?}", args, output);
    }
    let marker = harness._temp.path().join("clean-filter-executed");
    let filter = harness._temp.path().join("clean-filter.sh");
    fs::write(
        &filter,
        format!(
            "#!/bin/sh\n/bin/touch {}\n/bin/cat\n",
            marker.display()
        ),
    )
    .unwrap();
    let mut permissions = fs::metadata(&filter).unwrap().permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(&filter, permissions).unwrap();
    let config_path = harness.root.join(".git/config");
    let mut config = fs::read_to_string(&config_path).unwrap();
    config.push_str(&format!(
        "\n[filter \"clean\"]\n\tclean = {}\n",
        filter.display()
    ));
    fs::write(config_path, config).unwrap();
    fs::write(harness.root.join("tracked.txt"), "changed\n").unwrap();
    let policy = execution_policy_fixture::resolved_policy(
        harness._temp.path(),
        &[("project-a", harness.root.as_path(), Path::new("codex"))],
    );
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let root_anchor = policy
        .project_root_anchor(&canonical_root)
        .map_err(AppError::from)
        .unwrap();
    let limits = policy.campaign_limits;

    submit::run_with_options_with_root_anchor(
        &harness.db,
        &harness.root,
        &[OsString::from("python"), OsString::from("train.py")],
        &submit::SubmitOptions::default(),
        &limits,
        &harness.fake,
        root_anchor,
        policy,
    )
    .await
    .unwrap();

    let campaign = CampaignRepository::new(&harness.db)
        .find_live_by_project("project-a")
        .unwrap()
        .unwrap();
    assert_eq!(campaign.base_revision_sha, None);
    assert!(!marker.exists(), "local clean filter was executed");
}

#[cfg(unix)]
#[tokio::test]
async fn experiment_submit_with_root_anchor_cannot_redirect_status_with_local_worktree() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    initialize_clean_git(&harness.root);
    let outside = harness._temp.path().join("outside-worktree");
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("outside.txt"), "outside\n").unwrap();
    let config_path = harness.root.join(".git/config");
    let mut config = fs::read_to_string(&config_path).unwrap();
    config.push_str(&format!("\n[core]\n\tworktree = {}\n", outside.display()));
    fs::write(config_path, config).unwrap();
    fs::write(
        harness.root.join(".pueue-agent/STATE.md"),
        "changed objective state\n",
    )
    .unwrap();
    let policy = execution_policy_fixture::resolved_policy(
        harness._temp.path(),
        &[("project-a", harness.root.as_path(), Path::new("codex"))],
    );
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let root_anchor = policy
        .project_root_anchor(&canonical_root)
        .map_err(AppError::from)
        .unwrap();
    let limits = policy.campaign_limits;

    submit::run_with_options_with_root_anchor(
        &harness.db,
        &harness.root,
        &[OsString::from("python"), OsString::from("train.py")],
        &submit::SubmitOptions::default(),
        &limits,
        &harness.fake,
        root_anchor,
        policy,
    )
    .await
    .unwrap();

    let campaign = CampaignRepository::new(&harness.db)
        .find_live_by_project("project-a")
        .unwrap()
        .unwrap();
    assert_eq!(campaign.base_revision_sha, None);
}

#[cfg(unix)]
#[tokio::test]
async fn experiment_submit_with_root_anchor_non_git_preserves_null_base() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    let policy = execution_policy_fixture::resolved_policy(
        harness._temp.path(),
        &[("project-a", harness.root.as_path(), Path::new("codex"))],
    );
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let root_anchor = policy
        .project_root_anchor(&canonical_root)
        .map_err(AppError::from)
        .unwrap();
    let limits = policy.campaign_limits;
    submit::run_with_options_with_root_anchor(
        &harness.db,
        &harness.root,
        &[OsString::from("python"), OsString::from("train.py")],
        &submit::SubmitOptions::default(),
        &limits,
        &harness.fake,
        root_anchor,
        policy,
    )
    .await
    .unwrap();
    let campaign = CampaignRepository::new(&harness.db)
        .find_live_by_project("project-a")
        .unwrap()
        .unwrap();
    assert_eq!(campaign.base_revision_sha, None);
    assert_eq!(harness.fake.add_calls(), 1);
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
async fn experiment_admission_holds_lock_until_pueue_add() {
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
    // must use durable project group/root and wrapped runtime with original argv, ignoring caller mutations
    let add_args = harness.fake.last_add_args();
    let sep = add_args.iter().position(|a| a == "--").unwrap();
    let runtime = &add_args[sep + 1..];
    assert_eq!(runtime[0], OsString::from("/usr/bin/env"));
    assert_eq!(&runtime[5..], &[OsString::from("python"), OsString::from("train.py")]);
    assert_eq!(add_args[1], OsString::from("pa-project"));
    assert_eq!(
        add_args[3],
        fs::canonicalize(&harness.root).unwrap().into_os_string()
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

#[tokio::test]
async fn campaign_submit_managed_wraps_with_env_and_preserves_durable_argv() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    let result = harness.submit(&["python", "train.py"]).await.unwrap();

    // durable submission must store only original argv
    let stored = SubmissionRepository::new(&harness.db)
        .find_by_id(&result.submission_id)
        .unwrap()
        .unwrap();
    let expected_user: Vec<String> = vec!["python".to_owned(), "train.py".to_owned()];
    assert_eq!(
        stored.argv, expected_user,
        "durable argv must remain original user argv"
    );

    let exp_id = stored.metadata.get("experiment_id").and_then(|v| v.as_str()).unwrap().to_owned();
    let camp_id = stored.metadata.get("campaign_id").and_then(|v| v.as_str()).unwrap().to_owned();
    let experiment = ExperimentRepository::new(&harness.db)
        .find_by_id(&exp_id)
        .unwrap()
        .unwrap();
    let proposal = ProposalRepository::new(&harness.db)
        .find_by_id(&experiment.proposal_id)
        .unwrap()
        .unwrap();
    assert_eq!(proposal.argv, expected_user, "proposal argv must remain original user argv");
    let objective = pueue_agent::state::load_objective(&harness.root).unwrap();
    let expected_proposal = pueue_agent::proposals::validate_initial_baseline(
        pueue_agent::proposals::ProposalInput {
            kind: ProposalKind::Experiment,
            hypothesis: "Establish the initial campaign baseline".to_owned(),
            source_experiment_id: None,
            argv: expected_user.clone(),
            working_directory: ".".to_owned(),
            expected_evidence: Vec::new(),
        },
        &objective.digest,
    )
    .unwrap();
    assert_eq!(
        proposal.canonical_digest, expected_proposal.canonical_digest(),
        "proposal canonical digest must match original user argv"
    );

    let add_args = harness.fake.last_add_args();
    let sep = add_args.iter().position(|a| a == "--").expect("missing -- separator");
    let runtime = &add_args[sep + 1..];
    let canonical_root = fs::canonicalize(&harness.root).unwrap();
    let expected_runtime: Vec<OsString> = {
        let mut v = Vec::with_capacity(7);
        v.push(OsString::from("/usr/bin/env"));
        v.push(OsString::from(format!("PUEUE_AGENT_EXPERIMENT_ID={}", exp_id)));
        v.push(OsString::from(format!("PUEUE_AGENT_CAMPAIGN_ID={}", camp_id)));
        let mut result_path = OsString::from("PUEUE_AGENT_RESULT_PATH=");
        result_path.push(canonical_root.as_os_str().to_owned());
        result_path.push("/.pueue-agent/results/");
        result_path.push(OsString::from(format!("{}.json", exp_id)));
        v.push(result_path);
        let mut artifact_dir = OsString::from("PUEUE_AGENT_ARTIFACT_DIR=");
        artifact_dir.push(canonical_root.as_os_str().to_owned());
        artifact_dir.push("/.pueue-agent/artifacts/");
        artifact_dir.push(OsString::from(&exp_id));
        v.push(artifact_dir);
        v.push(OsString::from("python"));
        v.push(OsString::from("train.py"));
        v
    };
    assert_eq!(
        runtime, expected_runtime.as_slice(),
        "complete runtime vector must match hand-built expectations"
    );
}

#[tokio::test]
async fn campaign_submit_direct_control_remains_unwrapped() {
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
    let add_args = harness.fake.last_add_args();
    let sep = add_args.iter().position(|a| a == "--").unwrap();
    let runtime = &add_args[sep + 1..];
    assert!(
        !runtime.contains(&OsString::from("/usr/bin/env")),
        "direct/control must remain unwrapped, got {runtime:?}"
    );
    assert_eq!(
        runtime,
        &[OsString::from("python"), OsString::from("control.py")]
    );
    let stored = SubmissionRepository::new(&harness.db)
        .find_by_id(&result.submission_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.argv,
        vec!["python".to_owned(), "control.py".to_owned()]
    );
}

#[tokio::test]
async fn campaign_submit_post_add_identity_expects_runtime_command() {
    struct MismatchedPueue {
        inner: FakePueue,
        expected_original: Vec<OsString>,
    }
    #[async_trait]
    impl PueueApi for MismatchedPueue {
        async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
            // Return a task whose command is the *original* user argv display, not the wrapped runtime
            let mut tasks = self.inner.status_json().await?;
            if let Some(task) = tasks.first_mut() {
                let original_display = self
                    .expected_original
                    .iter()
                    .map(|s| {
                        let s = s.to_string_lossy();
                        if s.contains(' ') { format!("'{}'", s) } else { s.into_owned() }
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                task.command = original_display;
            }
            Ok(tasks)
        }
        async fn add(&self, args: &[OsString]) -> Result<i64, AppError> {
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

    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    // Reserve baseline via repository to get intent without involving pueue yet
    let intent = harness.reserve_baseline(&["python", "train.py"]);
    let project = harness.project();
    // Build a pueue double that will report mismatched command (original, not wrapped)
    let mismatched = MismatchedPueue {
        inner: FakePueue::new(),
        expected_original: vec![OsString::from("python"), OsString::from("train.py")],
    };
    let coordinator =
        CampaignCoordinator::new(&harness.db, &mismatched, CampaignLimits::default());
    let result = coordinator
        .submit_accepted_intent(&intent, &project, 101)
        .await;
    // Should fail identity validation and mark unreconciled, not succeed
    assert!(
        result.is_err(),
        "post-add identity must expect runtime command, mismatched original should fail"
    );
    let experiment = ExperimentRepository::new(&harness.db)
        .find_by_id(&intent.experiment.experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        experiment.status,
        ExperimentStatus::Unreconciled,
        "mismatched command should leave experiment unreconciled"
    );
}
