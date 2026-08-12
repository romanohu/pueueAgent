# Pueue Command Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every supervisor-launched Pueue command use a canonical service-trusted executable, fixed service-owned config, sanitized environment, mandatory process groups, bounded output, and bounded TERM/KILL cleanup while preserving all existing Pueue API callers and argv semantics.

**Architecture:** Keep PueueApi as the durable-operation boundary used by submit, batches, reconciliation, cancellation, termination, enable, and diagnostics. Add focused pueue_security and pueue_process modules for validation, verified-descriptor execution, timeout/output collection, and process-group cleanup; CommandPueue delegates to those modules and never builds a shell command.

**Tech Stack:** Rust 2021, Tokio process/time/io, Unix setsid/process-group signals and no-follow file APIs, rusqlite-backed existing repositories, serde/TOML, tempfile, generated fake-Pueue fixtures, Cargo integration tests, and Bats.

## Global Constraints

- CommandPueue receives a canonical absolute Pueue executable anchor resolved once by the daemon from the service-owned trusted PATH; it never resolves or searches ambient PATH at command time.
- The configured Pueue YAML is a core-owned pinned `PueueConfigAnchor`: startup lstat/open uses no-follow, records descriptor identity/owner/mode, rejects symlink components and project-root paths, and every command revalidates that identity immediately before launch.
- Pueue operations use direct argv (status, add, kill, remove, group) and preserve --escape, the -- separator, arbitrary argument boundaries, and all PueueApi implementations.
- `CommandPueue::new` and `Default` are `#[cfg(test)]` conveniences only; production has one policy-based factory and no bare executable construction.
- Each operation has a 30-second timeout and independent 64 KiB stdout and stderr caps. Timeout or overflow cleanup sends TERM to the recorded process group, waits boundedly, then sends KILL and reaps the child.
- Unix execution requires a successful setsid; failure is fatal before Pueue code runs. Non-Unix reports unsupported execution and never uses the old fallback.
- pueue_group is validated before callback registration, group lookup/add, or any Pueue argv use: [A-Za-z0-9][A-Za-z0-9._-]*.
- Policy failures are bounded policy_blocked:<code> diagnostics and do not become event retries; Pueue timeout/output errors remain bounded control errors and do not perform implicit event transitions.
- No credentials, environment maps/values, prompts, transcripts, or raw Pueue output are persisted in SQLite, logs, or diagnostics.
- Host-independent tests use generated temporary executables and fixtures; no real Pueue, Codex, network, or host /bin layout is required.
- Every task follows failing test -> RED command and expected failure -> minimal implementation -> GREEN command -> intentional commit.

## File map

- Create src/pueue_security.rs: bounded group grammar, canonical config-path ownership/mode/outside-root checks, bounded Pueue error projections.
- Create src/pueue_process.rs: core-plan verified-descriptor process adapter, mandatory setsid, independent output readers, 30-second timeout, TERM/KILL cleanup.
- Modify src/pueue.rs: CommandPueue construction, operation execution, typed timeout/overflow errors, group validation, and preserved argv shaping.
- Modify src/service.rs, src/main.rs, src/submit.rs, src/batches.rs: canonical policy construction and every production CommandPueue call site.
- Modify src/config.rs: retain existing configuration parsing while routing group validation through pueue_security.
- Modify src/termination.rs, src/cancel.rs, src/reconcile.rs: retain existing PueueApi flow while relying on bounded adapter errors.
- Modify src/diagnostics.rs: read-only Pueue path, bounds, and process-group capability checks.
- Test tests/integration/pueue_adapter.rs, tests/integration/config.rs, tests/integration/service.rs, tests/integration/termination.rs, tests/integration/diagnostics.rs, tests/support/fake_pueue.rs, and tests/test_shell_entrypoints.bats.

## Interfaces

This plan consumes the companion core execution-policy plan. The core plan must provide these exact public concepts before Task 2 is integrated; this plan does not redefine their fields or constructors:

    // src/execution_policy.rs
    pub struct ExecutableAnchor {
        pub canonical_path: std::path::PathBuf,
        pub identity: ExecutableIdentity,
        pub resolution_fingerprint: String,
    }
    impl ExecutableAnchor {
        pub fn verify_identity(&self) -> Result<VerifiedExecutable, PolicyViolation>;
    }
    pub struct VerifiedExecutable { pub file: std::fs::File, pub anchor: ExecutableAnchor }

    pub struct ResolvedExecutionPolicy {
        pub project_roots: Vec<std::path::PathBuf>,
        pub launcher_anchor: ExecutableAnchor,
        pub pueue_anchor: ExecutableAnchor,
        pub pueue_config_anchor: PueueConfigAnchor,
    }
    // PueueConfigAnchor is declared and implemented by the core plan; this
    // plan treats it as an opaque pinned descriptor/identity.

    // src/environment.rs
    pub struct SanitizedEnvironment;
    impl SanitizedEnvironment {
        pub fn for_pueue(policy: &ResolvedExecutionPolicy) -> Self;
        pub fn apply(&self, command: &mut tokio::process::Command);
    }

    // src/process.rs (generic marker-free mode required for Pueue)
    pub struct VerifiedCommandSpec {
        pub launcher: ExecutableAnchor,
        pub executable: ExecutableAnchor,
        pub argv: Vec<std::ffi::OsString>,
        pub cwd: Option<std::path::PathBuf>,
        pub environment: SanitizedEnvironment,
        pub process_group: ProcessGroupRequirement,
        pub start_suspended: bool,
        pub project_root: Option<VerifiedProjectRoot>,
        pub pueue_config: Option<VerifiedPueueConfig>,
        pub child_io: VerifiedChildIo,
    }
    pub fn spawn_verified_command(spec: VerifiedCommandSpec)
        -> Result<VerifiedChild, AppError>;
    pub struct VerifiedChild {
        pub child: tokio::process::Child,
        pub pid: i64,
        pub process_group_id: Option<i64>,
        pub start_gate: StartGate,
        pub exec_status: ExecStatusReceiver,
        pub ack: AckReceiver,
    }
    impl VerifiedChild {
        pub fn release(&mut self) -> Result<(), AppError>;
        pub async fn confirm_exec(&mut self) -> Result<(), AppError>;
        pub async fn wait_for_release_ack(&mut self) -> Result<(), AppError>;
        pub fn take_stdout(&mut self) -> Result<tokio::process::ChildStdout, AppError>;
        pub fn take_stderr(&mut self) -> Result<tokio::process::ChildStderr, AppError>;
    }
    pub async fn terminate_process_group(child: &mut VerifiedChild);

The core plan owns anchor resolution, project-root inventory, startup
environment capture, the pinned Pueue config anchor, platform-specific Linux/
macOS descriptor launch, mandatory setsid, and process-group cleanup. This plan
owns only the reusable Pueue adapter around those interfaces. It must call the
core `SanitizedEnvironment::for_pueue`, `spawn_verified_command`, and
`terminate_process_group`; it must not add a second launcher, setsid binding,
config-anchor struct, or process-group implementation.

### Task 1: Add Pueue security helpers and configuration validation

Files:
- Create: src/pueue_security.rs
- Modify: src/lib.rs, src/service.rs, src/config.rs
- Test: tests/integration/config.rs, tests/integration/service.rs, tests/integration/pueue_adapter.rs

Interfaces:
- Produces validate_group(&str) -> Result<(), AppError> and MAX_PUEUE_OUTPUT_BYTES: usize = 64 * 1024. It consumes the core-owned `PueueConfigAnchor`; it does not create a competing path-based config validator or return a `PathBuf` as a security proof.
- The production Pueue factory receives `ResolvedExecutionPolicy { project_roots, pueue_anchor, pueue_config_anchor }` and stores the opaque `PueueConfigAnchor`. At startup the core anchor has lstat/opened with O_NOFOLLOW, descriptor metadata/owner/mode, and outside-root validation recorded. Before every operation, call the anchor’s core revalidation/open-descriptor operation immediately before `spawn_verified_command`; pass the returned `VerifiedPueueConfig` as FD 9 and generate `--config /dev/fd/9`. Never validate one file and later let Pueue reopen the original path. Symlink input/components are rejected by the core anchor walk or pinned descriptor identity.

- [ ] Step 1: Write failing validation tests.

    #[test]
    fn pueue_group_accepts_bounded_grammar_and_rejects_shell_text() {
        for value in ["pa-project", "A9._-x"] { validate_group(value).unwrap(); }
        for value in ["", "-bad", "pa project", "pa/project", "pa;touch-x", &"a".repeat(129)] {
            assert!(validate_group(value).is_err(), "accepted {value:?}");
        }
    }

    #[tokio::test]
    async fn pueue_config_anchor_rejects_replacement_before_each_command() {
        let h = ServiceHarness::new_with_core_policy();
        let policy = h.policy();
        fs::rename(h.config_path(), h.replacement_path()).unwrap();
        fs::write(h.config_path(), "replacement").unwrap();
        let error = h.command_from_policy(policy).status_json().await.unwrap_err();
        assert!(matches!(error, AppError::PolicyViolation { violation:
            PolicyViolation { code: PolicyViolationCode::AnchorReplaced, .. } }));
    }

    #[cfg(unix)]
    #[test]
    fn pueue_config_anchor_rejects_weak_mode_and_project_root_config() {
        let weak = ServiceHarness::new_with_weak_config();
        assert!(weak.load_core_policy().is_err());
        let under_root = ServiceHarness::new_with_config_under_project_root();
        assert!(under_root.load_core_policy().is_err());
    }

- [ ] Step 2: Run RED. Run cargo test --test pueue_adapter pueue_group_accepts_bounded_grammar_and_rejects_shell_text. Expected failure: pueue_security and validate_group do not exist.
- [ ] Step 3: Implement minimum helpers. Use an ASCII byte predicate, require `1..=128` bytes, and reject empty/overlong groups or any byte outside the grammar. Return `AppError::Validation { field: "pueue_group", message: "has invalid characters or length" }`; this is a bounded CLI/service validation error, not `policy_blocked` and not an agent-event transition. Delegate config lstat/open O_NOFOLLOW, owner/mode, outside-root walk, startup identity recording, and pre-command revalidation to the core `PueueConfigAnchor`.

    pub fn validate_group(value: &str) -> Result<(), AppError> {
        let bytes = value.as_bytes();
        if bytes.is_empty() || bytes.len() > 128 || !bytes[0].is_ascii_alphanumeric()
            || !bytes.iter().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(b)) {
            return Err(AppError::Validation { field: "pueue_group", message: "has invalid characters" });
        }
        Ok(())
    }
- [ ] Step 4: Run GREEN. Run cargo test --test pueue_adapter --test service --test config. Expected PASS, including existing config parsing and service path fixtures.
- [ ] Step 5: Commit.

    git add src/lib.rs src/pueue_security.rs src/service.rs src/config.rs tests/integration/config.rs tests/integration/service.rs tests/integration/pueue_adapter.rs
    git commit -m "feat: validate Pueue groups and service config paths"

### Task 2: Build the bounded verified Pueue process adapter

Files:
- Create: src/pueue_process.rs
- Modify: src/pueue.rs, src/lib.rs
- Test: `src/pueue.rs` cfg(test) adapter tests, tests/integration/pueue_adapter.rs, tests/support/fake_pueue.rs, and shared `tests/support/native_process_fixture.rs`

Interfaces:
- `PueueProcessRunner::run(policy: &ResolvedExecutionPolicy, operation_argv: &[OsString]) -> Result<BoundedOutput, AppError>` calls core config revalidation and `spawn_verified_command` exactly once per operation. `AnchorReplaced` remains `AppError::PolicyViolation` for the caller; timeout/overflow/nonzero status map to bounded `PueueError` variants. None of these automatically transitions agent events.
- `BoundedOutput { status: ExitStatus, stdout: Vec<u8>, stderr: Vec<u8> }` caps each stream independently at 64 KiB. `PueueError::{Spawn,Timeout,OutputLimit,CommandFailed,Cleanup}` distinguishes control failures without raw output in its display text; anchor/config failures stay typed `AppError::PolicyViolation`.
- Production limits are `PUEUE_TIMEOUT = Duration::from_secs(30)` and `MAX_PUEUE_OUTPUT_BYTES = 64 * 1024`. A `cfg(test)` limits constructor accepts a short timeout (50 ms) and cap for generated fixtures; no production limit is changed by tests.

- [ ] Step 1: Add fake fixture controls and failing tests. Keep the existing shell `FakePueueCommand` only for direct non-native argv compatibility tests. Add `NativeFakePueue`, which compiles a small generated Rust source into a temporary executable and supports sleep_ms, stdout_bytes, stderr_bytes, argument/config-FD capture, and signal-observation files without `/bin/sh`, `/bin/cat`, or host layout. Put direct `CommandPueue::new`/`with_test_limits` tests in the `#[cfg(test)]` module in `src/pueue.rs`; native integration tests use the generated core-policy factory and `NativeFakePueue`. Add:

    #[tokio::test]
    async fn pueue_timeout_terminates_the_process_group() {
        let fixture = FakePueueCommand::sleeping(500);
        let adapter = CommandPueue::new(fixture.executable(), Vec::<OsString>::new())
            .with_test_limits(Duration::from_millis(50), 64 * 1024);
        let error = adapter.status_json().await.unwrap_err();
        assert!(matches!(error, AppError::Pueue(PueueError::Timeout { operation: "status" })));
        assert!(fixture.term_seen() && fixture.child_reaped());
    }

    #[tokio::test]
    async fn stdout_overflow_terminates_and_reaps_the_process_group() {
        let fixture = FakePueueCommand::output_sizes(65 * 1024, 0);
        let adapter = CommandPueue::new(fixture.executable(), Vec::<OsString>::new())
            .with_test_limits(Duration::from_millis(500), 64 * 1024);
        let error = adapter.status_json().await.unwrap_err();
        assert!(matches!(error, AppError::Pueue(PueueError::OutputLimit { operation: "status", stream: "stdout" })));
        assert!(fixture.term_seen() && fixture.child_reaped());
    }

    #[tokio::test]
    async fn stderr_overflow_terminates_and_reaps_the_process_group() {
        let fixture = FakePueueCommand::output_sizes(0, 65 * 1024);
        let adapter = CommandPueue::new(fixture.executable(), Vec::<OsString>::new())
            .with_test_limits(Duration::from_millis(500), 64 * 1024);
        let error = adapter.status_json().await.unwrap_err();
        assert!(matches!(error, AppError::Pueue(PueueError::OutputLimit { operation: "status", stream: "stderr" })));
        assert!(fixture.term_seen() && fixture.child_reaped());
    }

- [ ] Step 2: Run RED. Run `cargo test --test pueue_adapter pueue_timeout_terminates_the_process_group`; expected failure: no timeout/output variants and current output waits without a process group.
- [ ] Step 3: Implement the minimum runner. Revalidate the opaque `PueueConfigAnchor` to obtain `VerifiedPueueConfig`; build argv = [`--config`, `/dev/fd/9`, operation, operation_args...] without the original config path; obtain `SanitizedEnvironment::for_pueue` from the core policy; and call `spawn_verified_command` with `pueue_config: Some(verified_config)`, no project root, and `VerifiedChildIo::Capture`. Immediately call `verified.release()`, await `verified.confirm_exec()`, and require the fixed release ack before collecting output. Take stdout and stderr exactly once into separate tasks with `take(limit + 1)`, race child wait and both readers against the production 30-second limit (or cfg(test) limit), and on release/exec/ack failure, timeout, overflow, or reader failure call core `terminate_process_group(&mut verified)`. Join/abort both reader tasks after cleanup and reap the group. Never implement Linux/macOS fork/exec/setsid logic here or include captured bytes in Display.

    async fn run_one(
        policy: &ResolvedExecutionPolicy,
        operation: &'static str,
        argv: &[OsString],
    ) -> Result<BoundedOutput, AppError> {
        let config = policy.pueue_config_anchor.verify_identity(&policy.project_roots)?;
        let environment = SanitizedEnvironment::for_pueue(policy);
        let mut final_argv = vec!["--config".into(), "/dev/fd/9".into(), operation.into()];
        final_argv.extend_from_slice(argv);
        let mut verified = spawn_verified_command(VerifiedCommandSpec {
            launcher: policy.launcher_anchor.clone(),
            executable: policy.pueue_anchor.clone(), argv: final_argv,
            environment, cwd: None,
            process_group: ProcessGroupRequirement::Required, start_suspended: true,
            project_root: None, pueue_config: Some(config), child_io: VerifiedChildIo::Capture,
        })?;
        verified.release()?;
        verified.confirm_exec().await?;
        verified.wait_for_release_ack().await?;
        let output = tokio::time::timeout(PUEUE_TIMEOUT, collect_bounded(&mut verified, MAX_PUEUE_OUTPUT_BYTES)).await
            .map_err(|_| AppError::Pueue(PueueError::Timeout { operation }));
        if output.is_err() { terminate_process_group(&mut verified).await; }
        output
    }
- [ ] Step 4: Run GREEN. Run cargo test --test pueue_adapter. Expected PASS for timeout, independent caps, nonzero status capture, direct argv, and all existing JSON parsing tests.
- [ ] Step 5: Commit.

    git add src/lib.rs src/pueue_process.rs src/pueue.rs tests/integration/pueue_adapter.rs tests/support/fake_pueue.rs
    git commit -m "feat: bound Pueue process lifetime and output"

### Task 3: Harden CommandPueue while preserving every API caller

Files:
- Modify: src/pueue.rs, src/pueue_process.rs
- Modify: src/submit.rs, src/batches.rs, src/cancel.rs, src/reconcile.rs, src/termination.rs
- Test: tests/integration/pueue_adapter.rs, tests/integration/termination.rs

Interfaces:
- Add one production-only `CommandPueue` factory that consumes `ResolvedExecutionPolicy { project_roots, pueue_anchor, pueue_config_anchor }`, obtains the core `SanitizedEnvironment::for_pueue`, and retains `PueueApi` unchanged. Do not add a second config-anchor or executable-identity field type.
- Keep `CommandPueue::new` and `Default` only under `#[cfg(test)]`; test construction accepts an already-created fixture anchor/path and the cfg(test) limits override. Production code cannot call either with `"pueue"` or ambient PATH.

- [ ] Step 1: Add caller-preservation RED assertions. Assert add still emits --print-task-id, --escape, and the original -- boundary; assert batch, submit, cancellation, reconciliation, termination, and enable test doubles still receive the same PueueApi calls. Add a compile-time helper using fn accepts_api<P: PueueApi>(_: &P) {} for each fake.
- [ ] Step 2: Run RED. Run cargo test --test pueue_adapter --test termination. Expected failure after changing construction because main/submit still use CommandPueue::default and "pueue".
- [ ] Step 3: Implement policy construction and operation validation. Retain the opaque config anchor, revalidate it per operation, and route only its verified FD through fixed `/dev/fd/9`; never place its original path into target argv after validation. Call `validate_group` at the start of `ensure_group` and route every operation through `PueueProcessRunner`. Leave `PueueApi` signatures and fake implementations untouched. Keep `new`/`Default` behind `#[cfg(test)]`; expose only the single production factory selected by the core policy integration.
- [ ] Step 4: Run GREEN. Run cargo test --test pueue_adapter --test termination. Expected PASS with unchanged task signatures, termination leases, and Pueue argv.
- [ ] Step 5: Commit.

    git add src/pueue.rs src/pueue_process.rs src/submit.rs src/batches.rs src/cancel.rs src/reconcile.rs src/termination.rs tests/integration/pueue_adapter.rs tests/integration/termination.rs
    git commit -m "feat: route PueueApi callers through the verified adapter"

### Task 4: Wire canonical policy construction into services and CLI

Files:
- Modify: src/service.rs, src/main.rs, src/submit.rs
- Modify: tests/integration/service.rs, tests/integration/pueue_adapter.rs, tests/test_shell_entrypoints.bats

Interfaces:
- ServicePaths exposes the canonical service state directory and Pueue config path; daemon startup receives the core ResolvedExecutionPolicy before constructing Daemon.
- `configured_pueue(policy: Arc<ResolvedExecutionPolicy>) -> Result<CommandPueue, AppError>` is the only production factory. Enable and daemon may receive a create-capable policy prepared by the core boundary; disable, cancel, status, doctor, submit, and submit-batch use `load_existing_policy` and never create or repair policy state.

- [ ] Step 1: Add service/CLI RED tests. Extend service-definition tests to require an absolute canonical --pueue-config path and no bare pueue; add an integration assertion that a fake executable replacing the inherited PATH entry is never invoked after startup anchor resolution.
- [ ] Step 2: Run RED. Run cargo test --test service --test pueue_adapter. Expected failure because definitions and configured_pueue still construct CommandPueue::new("pueue", ...), while submit still uses Default.
- [ ] Step 3: Implement wiring. In `commands::daemon`, load/create the core policy before CommandPueue/AgentRunner/Daemon; on first enable, include the candidate canonical root in the inventory before policy validation and project registration. Replace all local constructors and `CommandPueue::default` paths with the one production factory. Other operator/submit/read-only paths load the existing policy only. Keep `submit::run_with`, `batches::run_with`, and all fake-injected APIs unchanged. Make `ServicePaths::from_environment` carry the policy/config location without embedding credentials in systemd/launchd. Invalid group/config returns a bounded validation/config or `AppError::PolicyViolation` error at this boundary, before callback or Pueue execution; it does not write `policy_blocked` or transition agent events.
- [ ] Step 4: Run GREEN. Run cargo test --test service --test pueue_adapter --test cli_help. Expected PASS; run bats tests/test_shell_entrypoints.bats and expect literal argument forwarding to pass.
- [ ] Step 5: Commit.

    git add src/service.rs src/main.rs src/submit.rs tests/integration/service.rs tests/integration/pueue_adapter.rs tests/test_shell_entrypoints.bats
    git commit -m "feat: construct Pueue from canonical service policy"

### Task 5: Add Pueue-specific diagnostics and policy error coverage

Files:
- Modify: src/diagnostics.rs, src/pueue.rs, src/pueue_security.rs
- Test: tests/integration/diagnostics.rs, tests/integration/pueue_adapter.rs

Interfaces:
- Core diagnostics retain ownership of project/root/executable/path/code/stage projections. This task adds only bounded Pueue checks: canonical config, output cap 65536, timeout 30, group grammar, and process-group capability.
- DoctorExternal continues to carry Result<Vec<PueueTask>, String>; raw command output is converted to bounded generic summaries.

- [ ] Step 1: Write failing doctor/error tests. Add a doctor report assertion for pueue.bounds with timeout_seconds = 30 and output_bytes = 65536, and assert a timeout error’s rendered text contains Pueue status timed out but not fixture output longer than 256 bytes or any credential marker. Add invalid-group/config tests through the existing enable fixture and assert they return bounded validation/config errors before `ensure_group`, callback installation, or any agent-event transition.

    #[test]
    fn doctor_reports_fixed_pueue_bounds_without_output() {
        let report = build_doctor_report(&harness.db, &harness.project, paths, external, 100).unwrap();
        let check = report.checks.iter().find(|check| check.name == "pueue.bounds").unwrap();
        assert_eq!(check.status, DoctorCheckStatus::Ok);
        assert!(check.summary.contains("30") && check.summary.contains("65536"));
        assert!(!check.summary.contains("credential"));
    }
- [ ] Step 2: Run RED. Run cargo test --test diagnostics pueue. Expected failure because the check and typed timeout projection do not exist.
- [ ] Step 3: Implement bounded projections. Add a static Pueue bounds report, map timeout/overflow to bounded `PueueError` display text, and ensure `enable_with` validates the registered group/config anchor before `ensure_group` or callback installation. Invalid group/config remains a bounded CLI/service validation error, never `policy_blocked:<code>` and never an implicit agent-event transition. Do not alter doctor’s read-only behavior.

    checks.push(doctor_ok(
        "pueue.bounds",
        "Pueue commands use timeout=30s and independent stdout/stderr caps=65536 bytes",
        "none",
    ));
- [ ] Step 4: Run GREEN. Run cargo test --test diagnostics --test pueue_adapter --test service. Expected PASS without raw output or credentials.
- [ ] Step 5: Commit.

    git add src/diagnostics.rs src/pueue.rs src/pueue_security.rs tests/integration/diagnostics.rs tests/integration/pueue_adapter.rs
    git commit -m "feat: expose bounded Pueue diagnostics and policy errors"

### Task 6: Full integration and host-independent verification

Files:
- Verify: src/pueue.rs, src/pueue_process.rs, src/pueue_security.rs, src/service.rs, src/main.rs, src/submit.rs, src/batches.rs, src/cancel.rs, src/reconcile.rs, src/termination.rs, src/diagnostics.rs
- Verify: all listed integration tests and tests/support/fake_pueue.rs

Interfaces:
- The finished adapter keeps PueueApi source-compatible for every fake and repository caller; only production construction changes to the canonical policy factory.
- Existing intervention, submission intent, batch lease, cancellation signature, termination confirmation, reconciliation, --escape, and fixed --config contracts remain unchanged.

- [ ] Step 1: Run focused suites.

    cargo test --test pueue_adapter --test service --test config --test termination --test diagnostics

    Expected: all Pueue, service, configuration, termination, and diagnostic tests pass with generated fixtures only.

- [ ] Step 2: Run shell-boundary tests.

    bats tests/test_shell_entrypoints.bats

    Expected: literal shell metacharacters remain one argv element; no Pueue command is assembled as shell source.

- [ ] Step 3: Run required host-independent checks.

    cargo fmt --all -- --check
    cargo test --all-targets
    git diff --check

    Expected: exit status 0 for each command; inspect failures rather than weakening caps, validation, or process cleanup.

- [ ] Step 4: Inspect the final diff.

    git status --short
    git diff --stat
    rg -n 'CommandPueue::default|CommandPueue::new\("pueue"|/bin/sh|LAUNCH_GATE_SCRIPT|command -v' src tests

    Expected: no production bare-Pueue/default construction and no Pueue shell launch; test-only fixture shell text may remain isolated.

- [ ] Step 5: Commit the verified integration.

    git add src tests Cargo.toml
    git commit -m "test: verify bounded secure Pueue command execution"

## Self-review checklist

- Trusted executable anchor, sanitized environment, and marker-free verified launcher are explicit cross-plan inputs; this plan does not duplicate their implementation.
- Canonical Pueue path, fixed config ownership/mode/root checks, group grammar, 30-second timeout, independent 64 KiB streams, mandatory setsid, TERM/KILL cleanup, and no PATH check/use race each have a task and test.
- submit, batches, termination, cancel, reconcile, enable, and diagnostics retain PueueApi; argv/--escape/fixed --config behavior is tested.
- Invalid group/config is bounded validation at the CLI/service boundary, never `policy_blocked` and never an implicit agent-event transition.
- The plan stays within Pueue control-command scope; agent/Codex/native-gate and log-read implementation remains with the companion plans.
- Every task has concrete test code, RED/GREEN commands with expected outcomes, minimal implementation guidance, and a commit command.
