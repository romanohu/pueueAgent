# Final Security Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the seven actionable whole-branch security findings while leaving non-escapable descendant containment as an explicit integration blocker.

**Architecture:** Production commands share one lexical, policy-anchored Pueue profile resolver; the installed callback carries only a numeric task ID; Pueue framing gets pre-persistence validation and one absolute deadline; and agent private temp becomes a fixed verified descriptor role in the native ABI. Existing agent marker semantics, DB finalization ordering, process-group cleanup, and redaction remain unchanged. The branch remains unintegrated because descendant containment is a separate design.

**Tech Stack:** Rust 2021, Tokio, rusqlite, serde/serde_json, Unix descriptor APIs, the existing native helper protocol, Cargo integration tests, and Bash fixtures.

## Global Constraints

- Scope changes to `docs/superpowers/specs/2026-08-15-final-security-review-fixes-design.md`.
- Do not add dependencies without approval.
- Never persist or render credentials, environment values, raw command output, prompts, or private-temp contents.
- Only enable and daemon startup may create policy; all other commands load existing policy.
- The 30-second Pueue limit is one absolute operation deadline. Cleanup keeps its independent safety grace.
- Preserve the five-method `PueueApi` seam unless a RED proves an extension is necessary.
- Preserve marker publication -> release -> exec proof -> exact acknowledgement.
- Use a private-temp FD role distinct from all existing protocol/target roles; no pathname or shell fallback.
- Run host-independent compile/link gates before completion. A pre-main dyld stall is not GREEN.
- Do not merge, push for release, or claim production readiness until descendant containment is separately approved and implemented.

---

### Task 1: Authoritative lexical Pueue profile resolution

**Files:**
- Modify: `src/service.rs`
- Modify: `src/main.rs`
- Modify: `src/upgrade.rs`
- Modify: `src/execution_policy.rs`
- Test: `tests/integration/{cli_help,service,execution_policy,diagnostics}.rs`

**Interfaces:**
- Produces `service::resolve_pueue_config_path(explicit, environment, installed, home) -> Result<PathBuf, AppError>`.
- Produces `ServicePaths::from_environment` that retains the lexical absolute config path until policy anchoring.
- Uses precedence explicit > `PUEUE_CONFIG` > installed service > verified-HOME default; invalid higher-precedence input never falls through.

- [ ] **Step 1: Write custom-profile RED tests**

Install a service definition pinned to `custom/pueue.yml`, leave the default absent or point it at a second fake, then invoke submit, submit-batch, status, and doctor without repeating the flag.

```rust
#[test]
fn submit_reuses_the_profile_pinned_by_enable_without_a_repeated_flag() {
    let harness = CliHarness::with_custom_installed_pueue_profile();
    let output = harness.run(&["submit", "--", "/usr/bin/true"]);
    assert!(output.success(), "{}", output.stderr());
    assert_eq!(harness.custom_pueue_calls(), 1);
    assert_eq!(harness.default_pueue_calls(), 0);
}
```

Also assert an explicit config conflicting with the loaded policy causes zero Pueue, DB, callback, and service mutation.

- [ ] **Step 2: Run the profile tests and capture RED**

```bash
cargo test --test cli_help custom_pueue_profile -- --nocapture --test-threads=1
cargo test --test service pueue_profile -- --nocapture --test-threads=1
```

Expected: submit paths select the default or fail before reaching the custom fake.

- [ ] **Step 3: Write symlink and degraded-doctor RED tests**

```rust
#[cfg(unix)]
#[test]
fn pueue_config_symlink_is_rejected_before_anchor_creation() {
    let harness = PolicyHarness::with_symlinked_pueue_config();
    let error = load_existing_policy(&harness.input()).unwrap_err();
    assert_eq!(error.code, PolicyViolationCode::AnchorMissing);
}

#[test]
fn doctor_reports_a_missing_pueue_config_instead_of_exiting_before_report() {
    let output = CliHarness::missing_pueue_config().run(&["doctor", "--json"]);
    let report: serde_json::Value = serde_json::from_slice(output.stdout()).unwrap();
    assert!(report["checks"].as_array().unwrap().iter().any(|check|
        check["name"] == "pueue.config" && check["status"] == "error"));
}
```

- [ ] **Step 4: Run these tests and capture RED**

```bash
cargo test --test execution_policy pueue_config_symlink -- --nocapture --test-threads=1
cargo test --test diagnostics missing_pueue_config -- --nocapture --test-threads=1
```

Expected: canonicalization accepts the symlink and doctor exits before rendering.

- [ ] **Step 5: Implement the shared resolver**

```rust
pub fn resolve_pueue_config_path(
    explicit: Option<&Path>,
    environment: Option<&Path>,
    installed: Option<&Path>,
    home: &Path,
) -> Result<PathBuf, AppError> {
    let selected = explicit.or(environment).or(installed)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| home.join(".config/pueue/pueue.yml"));
    validate_lexical_absolute_path("pueue_config", &selected)?;
    Ok(selected)
}
```

Move upgrade's precedence logic here and migrate every production command through `ServicePaths::from_environment`.

- [ ] **Step 6: Enforce lexical no-follow anchoring and degraded doctor**

Remove `canonicalize` from `ServicePaths`. Require root plus normal path components, open the original path with the existing no-follow walk, and require the opened canonical path to equal the lexical input. Doctor may retain a missing lexical path to render error checks but must not create policy or a Pueue adapter.

- [ ] **Step 7: Run focused GREEN**

```bash
cargo test --test cli_help custom_pueue_profile -- --nocapture --test-threads=1
cargo test --test service pueue_profile -- --nocapture --test-threads=1
cargo test --test execution_policy pueue_config -- --nocapture --test-threads=1
cargo test --test diagnostics missing_pueue_config -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 8: Review and commit**

Review precedence, policy creation classification, zero-mutation failure, no-follow identity, and doctor read-only behavior.

```bash
git add src/service.rs src/main.rs src/upgrade.rs src/execution_policy.rs tests/integration/cli_help.rs tests/integration/service.rs tests/integration/execution_policy.rs tests/integration/diagnostics.rs
git commit -m "fix: reuse the pinned Pueue profile"
```

---

### Task 2: Remove group text from the installed shell callback

**Files:**
- Modify: `src/{cli,main,service,events,pueue}.rs`
- Test: `tests/integration/{service,reconciliation,cli_help}.rs`

**Interfaces:**
- Produces `events::callback_group_for_task<'a>(tasks: &'a [PueueTask], task_id: i64) -> Result<&'a str, AppError>`.
- Produces installed callback `event callback --task-id '{{ id }}'` with no group placeholder.
- Consumes Task 1's profile resolver and configured Pueue adapter.

- [ ] **Step 1: Write callback injection/lookup RED tests**

```rust
#[test]
fn installed_callback_never_interpolates_group_text() {
    let command = callback_command(&service_paths());
    assert!(!command.contains("{{ group }}"));
    assert!(command.contains("--task-id '{{ id }}'"));
}

#[tokio::test]
async fn callback_resolves_and_validates_group_from_numeric_task_id() {
    let harness = CallbackHarness::with_task(41, "project-a");
    harness.run_installed_callback(41).await.unwrap();
    assert_eq!(harness.events_for("project-a", 41), 1);
}
```

Return a group `x'; touch injected; #` from fake status and assert no event and no injected marker.

- [ ] **Step 2: Run RED tests**

```bash
cargo test --test service installed_callback -- --nocapture --test-threads=1
cargo test --test reconciliation callback_resolves -- --nocapture --test-threads=1
```

Expected: the command contains `{{ group }}` and no-group lookup is unavailable.

- [ ] **Step 3: Implement numeric-only rendering and typed lookup**

```rust
pub fn callback_group_for_task<'a>(
    tasks: &'a [PueueTask],
    task_id: i64,
) -> Result<&'a str, AppError> {
    let mut matches = tasks.iter().filter(|task| task.id == task_id);
    let task = matches.next().ok_or(AppError::Validation {
        field: "callback.task_id",
        message: "was not found in the configured Pueue profile",
    })?;
    if matches.next().is_some() {
        return Err(AppError::Validation {
            field: "callback.task_id",
            message: "is ambiguous in the configured Pueue profile",
        });
    }
    validate_group(&task.group)?;
    Ok(&task.group)
}
```

Make `commands::event` async. If explicit `--group` is absent, load existing policy, query typed Pueue status, resolve/validate the group, then record the event. Do not mutate SQLite before successful lookup.

- [ ] **Step 4: Run focused GREEN**

```bash
cargo test --test service callback -- --nocapture --test-threads=1
cargo test --test reconciliation callback -- --nocapture --test-threads=1
cargo test --test cli_help event_callback -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 5: Review and commit**

Review shell rendering, validation before DB mutation, missing/ambiguous task behavior, deduplication, and redaction.

```bash
git add src/cli.rs src/main.rs src/service.rs src/events.rs src/pueue.rs tests/integration/service.rs tests/integration/reconciliation.rs tests/integration/cli_help.rs
git commit -m "fix: resolve callback groups without shell interpolation"
```

---

### Task 3: Apply one absolute deadline to a Pueue operation

**Files:**
- Modify: `src/process.rs`
- Modify: `src/pueue_process.rs`
- Modify: `src/pueue.rs`
- Test: `tests/integration/pueue_adapter.rs`
- Test support: `tests/support/native_process_fixture.rs`

**Interfaces:**
- Produces `process::spawn_verified_command_before(spec, deadline: Instant)`.
- Produces `VerifiedChild::{release_before, confirm_exec_before, wait_for_release_ack_before}`.
- Keeps existing agent APIs as wrappers with their current phase-specific deadlines.

- [ ] **Step 1: Write a cumulative-handshake RED test**

Extend the fixture with readiness, exec-proof, and ack delays. Under a debug-only 300 ms Pueue limit, use three 150 ms phases.

```rust
#[tokio::test]
async fn pueue_launch_phases_share_one_absolute_deadline() {
    let harness = NativePueueHarness::delays(150, 150, 150);
    let started = Instant::now();
    let error = harness.runner(Duration::from_millis(300)).status().await.unwrap_err();
    assert!(matches!(error, AppError::Pueue(PueueError::Timeout { .. })));
    assert!(started.elapsed() < Duration::from_millis(700));
    harness.assert_group_reaped();
}
```

- [ ] **Step 2: Run the test and capture RED**

```bash
cargo test --test pueue_adapter share_one_absolute_deadline -- --nocapture --test-threads=1
```

Expected: accumulated phase budgets exceed the limit or the error is misclassified as native-gate/spawn failure.

- [ ] **Step 3: Add deadline-aware lifecycle entry points**

Every blocking poll/read/write computes remaining time from the supplied `Instant`. Add one internal deadline-exhausted classification. Existing agent methods wrap these entry points with their existing deadlines.

- [ ] **Step 4: Thread the deadline through `PueueProcessRunner`**

Create the deadline before final config verification. Pass it through spawn/readiness, release, exec proof, acknowledgement, wait, and output collection. Any phase expiry performs checked cleanup and returns `PueueError::Timeout`; cleanup is not capped by the expired operation deadline.

- [ ] **Step 5: Run focused GREEN and regressions**

```bash
cargo test --test pueue_adapter timeout -- --nocapture --test-threads=1
cargo test --test pueue_adapter overflow -- --nocapture --test-threads=1
cargo test --lib process::tests -- --nocapture --test-threads=1
cargo test --test native_launcher -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 6: Review and commit**

Review deadline origin, error mapping, cancellation ownership, cleanup ordering, and unchanged agent marker timing.

```bash
git add src/process.rs src/pueue_process.rs src/pueue.rs tests/integration/pueue_adapter.rs tests/support/native_process_fixture.rs
git commit -m "fix: bound the complete Pueue operation"
```

---

### Task 4: Reject unframeable submissions and close small fail-open gaps

**Files:**
- Modify: `src/{pueue,pueue_process,submit,batches,diagnostics,execution_policy}.rs`
- Test: `tests/integration/{pueue_adapter,database,diagnostics,execution_policy}.rs`

**Interfaces:**
- Produces `pueue::validate_add_argv(args: &[OsString]) -> Result<(), AppError>` using the native protocol count and encoded-byte limits.
- Produces object-only Pueue group parsing.
- Preserves the five-method `PueueApi`; submit/batch call the pure validator before persistence.

- [ ] **Step 1: Write final-frame boundary RED tests**

Test the maximum and first rejected add request through direct submission and batch persistence.

```rust
#[tokio::test]
async fn oversized_native_add_argv_is_rejected_before_submission_insert() {
    let harness = SubmitHarness::new();
    let command = vec![OsString::from("x"); max_user_add_args() + 1];
    assert!(submit_with(&harness.db, &command, &harness.pueue).await.is_err());
    assert_eq!(harness.submission_count(), 0);
    assert_eq!(harness.pueue.add_calls(), 0);
}
```

- [ ] **Step 2: Write group JSON, anchor, and unlink RED tests**

Add `[]`, `null`, and string group responses; replace the Pueue executable/config before doctor; and force policy write/fsync failure while cwd contains a same-named sentinel.

- [ ] **Step 3: Run RED tests**

```bash
cargo test --test pueue_adapter group_json -- --nocapture --test-threads=1
cargo test --test database oversized_native_add -- --nocapture --test-threads=1
cargo test --test diagnostics execution_anchors -- --nocapture --test-threads=1
cargo test --test execution_policy publication_failure_cleanup -- --nocapture --test-threads=1
```

Expected: the submission row exists, non-object JSON is treated as absent, doctor omits Pueue anchors, or cleanup targets cwd.

- [ ] **Step 4: Implement exact add framing validation**

Build the exact operation vector used by `CommandPueue::add`, account for fixed native entries (`pueue`, `--config`, `/dev/fd/9`, `add`), and invoke the existing control-frame count/byte validator. Call it before `SubmissionRepository::insert_idempotent`, before batch mutation, and defensively in `CommandPueue::add`.

- [ ] **Step 5: Implement typed group parsing and four-anchor doctor**

Deserialize groups as `BTreeMap<String, serde_json::Value>`. Arrays/scalars map to `InvalidGroupJson`. Doctor verifies Codex, launcher, Pueue executable, and Pueue config identities.

- [ ] **Step 6: Replace cwd-relative policy-temp removal**

After temporary creation, every write/fsync/link failure calls `unlinkat(&state_dir.file, &temporary)` and syncs the modified parent. Remove every `fs::remove_file(&temporary)` call.

- [ ] **Step 7: Run focused GREEN**

```bash
cargo test --test pueue_adapter -- --nocapture --test-threads=1
cargo test --test database oversized_native_add -- --nocapture --test-threads=1
cargo test --test diagnostics execution_anchors -- --nocapture --test-threads=1
cargo test --test execution_policy publication_failure_cleanup -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 8: Review and commit**

Review argument math, byte/count source of truth, persistence order, typed JSON rejection, anchor coverage, and descriptor cleanup.

```bash
git add src/pueue.rs src/pueue_process.rs src/submit.rs src/batches.rs src/diagnostics.rs src/execution_policy.rs tests/integration/pueue_adapter.rs tests/integration/database.rs tests/integration/diagnostics.rs tests/integration/execution_policy.rs
git commit -m "fix: validate bounded Pueue framing before persistence"
```

---

### Task 5: Make private temp a verified target descriptor role

**Files:**
- Modify: `src/environment.rs`
- Modify: `src/codex_command.rs`
- Modify: `src/agent.rs`
- Modify: `src/native_launcher.rs`
- Modify: `src/process.rs`
- Test: `tests/integration/{codex_security,native_agent_gate,native_launcher,scheduler}.rs`

**Interfaces:**
- Produces crate-private `environment::VerifiedPrivateTemp` with a cloned directory descriptor and exact identity.
- Produces `PrivateRunTemp::verified_target() -> Result<VerifiedPrivateTemp, PolicyViolation>`.
- Produces one fixed `PRIVATE_TEMP_TARGET_FD`, frame flag, identity field, and launch-spec field.
- Uses the same `/dev/fd/<slot>` value for task temp variables and Codex's only extra writable root.

- [ ] **Step 1: Write pathname-replacement RED integration test**

Spawn a real blocked target that writes below `TMPDIR`. Before release, rename the numeric generation and create a new owner-0700 directory at the old name.

```rust
#[tokio::test]
async fn private_temp_path_replacement_cannot_redirect_target_writes() {
    let mut run = NativeAgentHarness::spawn_blocked_temp_writer().await;
    let old_generation = run.swap_private_temp_generation();
    run.authorize().await.unwrap();
    assert_eq!(fs::read(old_generation.join("target-write")).unwrap(), b"ok");
    assert!(!run.replacement_path().join("target-write").exists());
    run.wait_and_cleanup().await.unwrap();
}
```

Expected RED: the target follows the replacement pathname while cleanup retains the old directory descriptor.

- [ ] **Step 2: Write ABI/identity RED tests**

Cover missing right, duplicate role, wrong descriptor type/identity, role reordering/trailing data, target slot visibility, protocol-FD leakage, and CLOEXEC on every non-target role.

- [ ] **Step 3: Run RED tests**

```bash
cargo test --test native_agent_gate private_temp -- --nocapture --test-threads=1
cargo test --test native_launcher private_temp -- --nocapture --test-threads=1
cargo test --lib process::tests::private_temp -- --nocapture --test-threads=1
```

- [ ] **Step 4: Add the typed capability**

`PrivateRunTemp::verified_target` clones `self.directory`, revalidates owner/type/mode/device/inode, and returns a type with no public raw-FD or arbitrary-path constructor. Debug output contains only role/run ID.

- [ ] **Step 5: Extend frame and fixed-slot preparation**

Add the private-temp flag, expected identity, and bootstrap right. Reject missing/extra/reordered rights. Install it at `PRIVATE_TEMP_TARGET_FD`, verify immediately before target creation, and preserve only that slot across target exec. Keep Linux post-fork work async-signal-safe and use macOS spawn file actions.

- [ ] **Step 6: Remove private-temp pathname consumption**

Remove `temp.path()` from `AgentRunner::command_for`. Make `CodexArgvBuilder` accept a typed private-temp target reference and emit the fixed FD path. Make `SanitizedEnvironment::project_baseline` emit the same value for `TMPDIR`, `TMP`, and `TEMP`. Pass the verified capability in `NativeLaunchSpec`.

- [ ] **Step 7: Run focused GREEN and cleanup regressions**

```bash
cargo test --test native_agent_gate -- --nocapture --test-threads=1
cargo test --test native_launcher -- --nocapture --test-threads=1
cargo test --test codex_security private_temp -- --nocapture --test-threads=1
cargo test --test scheduler private_temp -- --nocapture --test-threads=1
cargo test --lib process::tests -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 8: Review and commit**

Review protocol exactness, ownership, no-follow identity, post-fork syscall safety, leakage, marker order, and cleanup generation consistency.

```bash
git add src/environment.rs src/codex_command.rs src/agent.rs src/native_launcher.rs src/process.rs tests/integration/codex_security.rs tests/integration/native_agent_gate.rs tests/integration/native_launcher.rs tests/integration/scheduler.rs
git commit -m "fix: bind private temp to the native target descriptor"
```

---

### Task 6: Cross-plan verification and containment-blocked handoff

**Files:**
- Modify ignored ledger: `.superpowers/sdd/2026-08-13-execution-policy-agent-launch/progress.md`
- Modify ignored ledger: `.superpowers/sdd/2026-08-13-pueue-command-hardening/progress.md`
- Test: all touched unit/integration targets

**Interfaces:**
- Consumes Tasks 1-5.
- Produces independent closure evidence for original findings 1 and 3-8 plus the three Minors.
- Does not produce a merge, push, release, or production-ready claim.

- [ ] **Step 1: Run focused runtime suites**

```bash
cargo test --lib -- --test-threads=1
cargo test --test cli_help -- --test-threads=1
cargo test --test service -- --test-threads=1
cargo test --test execution_policy -- --test-threads=1
cargo test --test diagnostics -- --test-threads=1
cargo test --test pueue_adapter -- --test-threads=1
cargo test --test native_agent_gate -- --test-threads=1
cargo test --test native_launcher -- --test-threads=1
cargo test --test codex_security -- --test-threads=1
cargo test --test scheduler -- --test-threads=1
```

If this host stalls before Rust main, terminate only the owned process, record the exact boundary, and do not claim runtime GREEN.

- [ ] **Step 2: Run mandatory host-independent gates**

```bash
cargo check --all-targets
cargo check --release --all-targets
cargo test --all-targets --no-run
git diff cb89c9f..HEAD --check
bash -n bin/pueue-agent install.sh tests/e2e/fake_experiments/train_ok.sh tests/e2e/run.sh tests/e2e/rust_supervisor.sh tests/support/fake_agent.sh tests/support/fake_codex.sh
```

If the documented build-script workaround is needed, restore both original Mach-O files and prove no Cargo lock holder remains.

- [ ] **Step 3: Run static security invariants**

```bash
rg -n 'CommandPueue::default|CommandPueue::new\("pueue"|Command::new\("pueue"|LAUNCH_GATE_SCRIPT|command -v' src
rg -n '\{\{ group \}\}' src assets
rg -n 'remove_file\(&temporary\)' src
rg -n 'load_or_create_policy\(' src
```

Expected: no forbidden production hit; policy creation appears only in its definition/tests plus enable and daemon startup.

- [ ] **Step 4: Request independent whole-fix review**

Provide the spec, this plan, `cb89c9f..HEAD`, RED/GREEN evidence, and host limitation. Require exact disposition of findings 1 and 3-8 and all three Minors. Require the verdict to retain descendant containment as open and the branch as not integration-ready.

- [ ] **Step 5: Apply at most one scoped fix round**

For each confirmed review finding, save a focused RED, make the minimal change, rerun affected tests and host-independent gates, and obtain a scoped re-review. Do not add containment under this plan.

- [ ] **Step 6: Update ignored ledgers and report**

Record commits, commands, verdicts, runtime limitations, and the unresolved containment gate in both ignored ledgers. Confirm no credentials/runtime state and a clean tracked worktree.

```bash
git status --short
git log --oneline cb89c9f..HEAD
```

The handoff must state: actionable findings complete if verified; descendant containment remains open; branch preserved and unintegrated.
