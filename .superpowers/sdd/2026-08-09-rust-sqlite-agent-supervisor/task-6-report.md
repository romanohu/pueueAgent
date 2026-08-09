# Task 6 Report: Bounded detector, log snapshots/fingerprints, and incident lifecycle

## Summary

- Added bounded log snapshots with byte size, modification time, tail-only evidence, and stable content fingerprinting.
- Added detector observations for configured regex patterns and extra logs, including confirmation counts, actions, bounded evidence, and project-root canonicalization for extra log paths.
- Added an incident store that opens, updates, leaves unchanged, or resolves incidents through the existing SQLite incident repository.
- Connected reconciliation terminal-task observations to resolve active task-scoped incidents without terminating tasks or starting agents.
- Preserved task-ID reuse semantics by fingerprinting task-scoped incidents with the full `task_signature`.

## Changed files

- `Cargo.lock`
- `Cargo.toml`
- `src/lib.rs`
- `src/detect.rs`
- `src/incidents.rs`
- `src/logs.rs`
- `src/reconcile.rs`
- `tests/integration/detection.rs`

## Commit

- `5233e4d` — `feat: add fingerprinted anomaly incidents`

## Commands and results

- `cargo test --test detection`
  - Initial red attempt failed because `cargo` was not on PATH.
- `nix shell nixpkgs#cargo nixpkgs#rustc -c cargo test --test detection`
  - Failed in sandbox because Nix daemon socket access was not permitted; escalated retry was interrupted after long initial fetch.
- `PATH=/nix/store/19jys72rq6a5i71lvpxqi84ja9gydvpk-rust-default-1.96.1/bin:$PATH cargo test --test detection`
  - Red after adding the test target and implementation: 4 passed, 1 failed.
  - Root cause: the bounded-tail confirmation test used a tail size too small to contain all three expected matches.
  - Final result: passed, 5 passed, 0 failed.
- `PATH=/nix/store/19jys72rq6a5i71lvpxqi84ja9gydvpk-rust-default-1.96.1/bin:$PATH cargo test --test detection`
  - After adding `regex-automata` as a direct dependency, failed in sandbox because Cargo needed registry/dependency metadata for `aho-corasick` and `regex-syntax`.
- `PATH=/nix/store/19jys72rq6a5i71lvpxqi84ja9gydvpk-rust-default-1.96.1/bin:$PATH cargo test --test detection` with network escalation
  - Passed, 5 passed, 0 failed.
- `PATH=/nix/store/19jys72rq6a5i71lvpxqi84ja9gydvpk-rust-default-1.96.1/bin:$PATH cargo fmt --check`
  - Passed.
- `PATH=/nix/store/19jys72rq6a5i71lvpxqi84ja9gydvpk-rust-default-1.96.1/bin:$PATH cargo test --all-targets`
  - Passed: all integration and unit test targets passed.
- `PATH=/nix/store/19jys72rq6a5i71lvpxqi84ja9gydvpk-rust-default-1.96.1/bin:$PATH cargo clippy --all-targets --all-features -- -D warnings`
  - Passed.
- `git diff --check -- . ':(exclude)target'`
  - Passed.

## Risks and follow-ups

- `Detector::inspect_task` supports bounded reads and observations but is not yet wired into the daemon loop; later scheduler tasks should pass `Detector::for_project` with the canonical project ID.
- Automatic termination remains data-only via the observation action and is intentionally not implemented in this task.
- The local shell PATH did not include `cargo`; verification used the existing Nix store Rust toolchain by explicitly prepending its `bin` directory.

## Fix round: reviewer incident identity findings

### Summary

- Split task-scoped incident identity from the full reconciliation `task_signature`.
- Added `task_incident_key` using stable run identity fields: Pueue group, task ID, `enqueued_at`, and `started_at`.
- Kept full `task_signature` for task observations and terminal event deduplication.
- Updated detector task observations and terminal reconciliation recovery to use the stable incident key, so incidents opened while a task is running are resolved when the same task reaches a terminal state.
- Added explicit task-less recovery identity for extra-log pattern observations by matching `project_id`, `kind`, `task_key IS NULL`, and exact fingerprint, where the fingerprint includes path, pattern name, and bounded snapshot fingerprint.
- Removed the broad `(?3 IS NULL OR task_key = ?3)` recovery condition so task-less recovery cannot resolve unrelated active incidents of the same kind in a project.

### Changed files

- `src/detect.rs`
- `src/incidents.rs`
- `src/reconcile.rs`
- `tests/integration/detection.rs`

### Commit

- `3d4560f` — `fix: stabilize detector incident recovery identity`

### Regression tests

- `terminal_task_observation_resolves_incident_opened_while_running`
  - Red: failed with `left: Unchanged`, `right: Resolved`.
  - Green: passed after switching task-scoped incidents to `task_incident_key`.
- `taskless_recovery_does_not_resolve_unrelated_extra_log_incident`
  - Red: failed to compile because `Observation::extra_log_pattern_recovered` did not exist.
  - Green: passed after adding explicit extra-log recovery fingerprinting and exact fingerprint matching for task-less recovery.

### Commands and results

- `cargo test --test detection terminal_task_observation_resolves_incident_opened_while_running`
  - Failed because `cargo` is not on PATH in the local shell.
- `PATH=/nix/store/19jys72rq6a5i71lvpxqi84ja9gydvpk-rust-default-1.96.1/bin:$PATH cargo test --test detection terminal_task_observation_resolves_incident_opened_while_running`
  - Red before fix: failed, 0 passed, 1 failed, assertion showed `Unchanged` instead of `Resolved`.
  - Green after fix: passed, 1 passed, 0 failed.
- `PATH=/nix/store/19jys72rq6a5i71lvpxqi84ja9gydvpk-rust-default-1.96.1/bin:$PATH cargo test --test detection taskless_recovery_does_not_resolve_unrelated_extra_log_incident`
  - Red before fix: failed to compile with missing `Observation::extra_log_pattern_recovered`.
  - Green after fix: passed, 1 passed, 0 failed.
- `PATH=/nix/store/19jys72rq6a5i71lvpxqi84ja9gydvpk-rust-default-1.96.1/bin:$PATH cargo test --test detection`
  - Passed, 7 passed, 0 failed.
- `PATH=/nix/store/19jys72rq6a5i71lvpxqi84ja9gydvpk-rust-default-1.96.1/bin:$PATH cargo fmt --check`
  - Initial check failed on `src/incidents.rs` indentation.
- `PATH=/nix/store/19jys72rq6a5i71lvpxqi84ja9gydvpk-rust-default-1.96.1/bin:$PATH cargo fmt`
  - Completed successfully.
- `PATH=/nix/store/19jys72rq6a5i71lvpxqi84ja9gydvpk-rust-default-1.96.1/bin:$PATH cargo fmt --check`
  - Passed.
- `PATH=/nix/store/19jys72rq6a5i71lvpxqi84ja9gydvpk-rust-default-1.96.1/bin:$PATH cargo test --all-targets`
  - Passed: lib 0, main 0, cli_help 1, config 24, database 17, detection 7, pueue_adapter 9, reconciliation 13.
- `PATH=/nix/store/19jys72rq6a5i71lvpxqi84ja9gydvpk-rust-default-1.96.1/bin:$PATH cargo clippy --all-targets --all-features -- -D warnings`
  - Passed.
- `git diff --check -- . ':(exclude)target'`
  - Passed.

### Risks and follow-ups

- `task_incident_key` separates reused IDs when Pueue exposes differing `enqueued_at` or `started_at`; if both timestamps are unavailable for reused IDs with the same group and numeric ID, the incident key cannot distinguish those runs.
- Extra-log recovered observations currently resolve only exact path/pattern/snapshot fingerprints. A future detector-level "log has recovered" flow may need a separate source-scoped recovery identity if it should resolve prior error snapshots after the log content changes.
