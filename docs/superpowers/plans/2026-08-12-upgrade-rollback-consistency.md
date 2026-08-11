# Upgrade Rollback Consistency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task with verification checkpoints.

**Goal:** Quiesce the supervisor before the SQLite upgrade snapshot, make launchd stop idempotent, and preserve SQLite plus binary rollback ordering and documentation guarantees.

**Architecture:** Keep `ServiceControl::stop` as the platform boundary. `UpgradeRunner::run` will stop the supervisor after its final active-agent check and before `VACUUM INTO` or install, while its existing upgrade lock remains in scope. Post-install rollback will continue to execute stop, SQLite restore, binary restore, restart, and health in that order.

**Tech Stack:** Rust, rusqlite, Tokio integration tests, fake service/command runners, Markdown documentation.

## Global Constraints

- Preserve the existing no-Pueue-task mutation policy.
- Treat only launchd `Could not find service` stop output as an idempotent success; propagate genuine stop failures.
- Do not use destructive git reset or stale commits.
- Run focused upgrade/service/CLI tests, `cargo test --all-targets`, and `git diff --check` before the implementation commit.

---

### Task 1: Lock the upgrade ordering and rollback behavior with failing tests

**Files:**
- Modify: `tests/integration/upgrade.rs`
- Test: `tests/integration/upgrade.rs`

**Interfaces:**
- Consumes: `UpgradeFixture`, `FakeServiceRecorder`, `UpgradeRunner`, `UpgradeRollback`.
- Produces: regression coverage that requires the service to stop before snapshot/install and verifies the restored SQLite marker, old binary, and call order.

- [ ] **Step 1: Extend the fake service with a one-shot database mutation on stop.** Add a `mutate_database_on_first_stop` flag and helper beside the existing restart mutation. When the first successful `stop()` occurs, update `upgrade_fixture_marker` to `quiesced`.
- [ ] **Step 2: Update post-install rollback call expectations.** Existing health/Pueue rollback tests must expect `stop` before the candidate `restart`, then the rollback `stop`, then the old-binary `restart`.
- [ ] **Step 3: Write the failing ordering/restoration test.** Add `snapshot_is_created_after_the_service_is_quiesced_and_rollback_restores_both_states`: set marker `old`, mutate on first stop, mutate candidate DB on first restart, trigger health failure, and assert marker `quiesced`, old binary, and `stop, restart, stop, restart` calls.
- [ ] **Step 4: Add the initial-stop failure test.** Configure the first stop to fail and assert that the installed binary and database remain unchanged, no restart occurs, and the failure report does not claim a completed rollback.
- [ ] **Step 5: Run the focused upgrade tests to verify RED.** Run `cargo test --test upgrade`; expect the new ordering/restoration assertions to fail because snapshot currently precedes quiescing and the existing call expectations lack the initial stop.

### Task 2: Lock launchd idempotency and documentation contracts with failing tests

**Files:**
- Modify: `tests/integration/service.rs`
- Modify: `tests/integration/cli_help.rs`
- Test: `tests/integration/service.rs`
- Test: `tests/integration/cli_help.rs`

**Interfaces:**
- Consumes: `ServiceManager::stop_with`, `ServiceCommandOutput`, existing documentation contract tests.
- Produces: tests requiring unloaded launchd stop success, genuine launchd stop error propagation, and the new README/operations text.

- [ ] **Step 1: Add the unloaded launchd stop test.** Feed `ServiceCommandOutput::failure(3, "Could not find service")` to `stop_with(ServicePlatform::Launchd, ...)`; assert success and exactly one `bootout` invocation.
- [ ] **Step 2: Add the genuine launchd stop failure test.** Feed a different non-zero result such as `permission denied`; assert an error containing the lifecycle status and no fallback command.
- [ ] **Step 3: Extend the README documentation contract.** Require stable phrases covering `VACUUM INTO`, service quiescing before the snapshot, SQLite plus binary rollback, and avoiding direct operator SQLite writes during the short window.
- [ ] **Step 4: Extend the Japanese operations documentation contract.** Add the same required guarantees for `docs/operations-ja.md`.
- [ ] **Step 5: Run focused service and CLI tests to verify RED.** Run `cargo test --test service --test cli_help`; expect the new launchd and documentation assertions to fail before production/docs changes.

### Task 3: Implement service idempotency and upgrade quiescing

**Files:**
- Modify: `src/service.rs:300-318`
- Modify: `src/upgrade.rs:650-690`

**Interfaces:**
- Consumes: `ServiceControl::stop`, `launchd_service_is_not_loaded`, existing upgrade lock and cleanup helpers.
- Produces: idempotent launchd stop and a quiesced SQLite snapshot/install boundary.

- [ ] **Step 1: Make launchd `stop_with` accept an unloaded service.** Capture the `bootout` result; return `Ok(())` for success or `launchd_service_is_not_loaded(&output.details)`, and call `lifecycle_command_error` for every other failure. Leave systemd behavior unchanged.
- [ ] **Step 2: Stop the supervisor after the final active-agent rejection.** In `UpgradeRunner::run`, call `self.service.stop()` immediately before `snapshot_database`; on failure return the existing cleanup-wrapped `UpgradeFailure` without taking a snapshot or installing the candidate.
- [ ] **Step 3: Preserve the coordination lock and rollback sequence.** Do not narrow `_lock` scope or bypass `rollback_after_post_install_failure`; keep rollback operations in stop, database restore, binary restore, restart, health order. Update only report/error text or test expectations required by the new stop boundary.
- [ ] **Step 4: Run the focused tests to verify GREEN.** Run `cargo test --test upgrade --test service`; fix implementation issues without weakening the new assertions.

### Task 4: Update operator documentation

**Files:**
- Modify: `README.md:94-110`
- Modify: `docs/operations-ja.md:55-73`

**Interfaces:**
- Consumes: the implemented upgrade sequence and rollback behavior.
- Produces: operator-facing documentation that describes the SQLite/binary consistency boundary.

- [ ] **Step 1: Update README.** State that service is stopped before the `VACUUM INTO` snapshot and install, that the short boundary should not contain direct operator SQLite writes, and that failed restart/health rollback restores SQLite snapshot and old binary before restart/health.
- [ ] **Step 2: Update Japanese operations guide.** State the same behavior in the existing upgrade section while retaining the no-Pueue-task mutation policy.
- [ ] **Step 3: Run documentation contract tests.** Run `cargo test --test cli_help`; confirm the new clauses are present in both files.

### Task 5: Final verification and implementation commit

**Files:**
- Verify: `src/service.rs`, `src/upgrade.rs`, `tests/integration/service.rs`, `tests/integration/upgrade.rs`, `tests/integration/cli_help.rs`, `README.md`, `docs/operations-ja.md`

**Interfaces:**
- Consumes: all preceding implementation and documentation changes.
- Produces: a verified implementation commit on `feat/periodic-lifecycle-upgrade`.

- [ ] **Step 1: Run formatting check.** Run `cargo fmt --all -- --check`; if the formatter is unavailable, record that fact and continue with compiler/tests.
- [ ] **Step 2: Run focused suites separately.** Run `cargo test --test upgrade`, `cargo test --test service`, and `cargo test --test cli_help`.
- [ ] **Step 3: Run the full suite.** Run `cargo test --all-targets` and inspect the complete result for failures.
- [ ] **Step 4: Check the diff.** Run `git diff --check` and `git status --short`; confirm only intended files are modified after the spec and plan commits.
- [ ] **Step 5: Commit the implementation.** Stage the source, tests, README, and operations guide, then commit with `fix: quiesce service during upgrade snapshot`.
