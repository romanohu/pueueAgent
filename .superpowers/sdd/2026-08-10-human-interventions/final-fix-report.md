# Final review remediation report

Date: 2026-08-10
Base review range: `2bb4fe8..c7b3ac1`

## Root causes and fixes

- A: periodic scheduler recovery only recovered event leases. `InterventionRepository::recover_expired_unattached` now runs on every scheduler tick and only requeues `reserved` rows whose `agent_run_id IS NULL`. Startup recovery for attached starting, failed, and live runs remains unchanged.
- B: the configured agent was previously spawned before the run/intervention transaction committed. Unix launches now use a fixed-argv `/bin/sh` gate over piped stdin; the configured program and arguments are positional parameters, and the release line is written only after the transaction succeeds. EOF or an invalid release exits without executing the configured program. Spawn/preflight failures still finish the run and release reservations. Non-Unix builds fail explicitly because the durable Unix gate is unavailable.
- C: the Phase A diagnostics handlers were no-ops. Events, task inspection, incident explanation, and read-only doctor reports are now implemented with project-scoped repositories, deterministic ordering, bounded summaries, typed doctor statuses, JSON/text rendering, and CLI wiring. Unknown and foreign task/incident records are rejected or excluded. Policy and approval are reported as `not_configured`.
- D: current-schema opens unconditionally rebuilt intervention indexes. Migration now compares normalized index definitions and repairs only missing/stale definitions. Fresh v6 and v5 migration behavior is preserved.
- E: Pueue timestamps were copied into JSON without bounds or control normalization. `enqueued_at` and `started_at` now use the bounded text sanitizer.
- F: text `steer list` output emitted stored control characters. Text-only escaping now covers newline, carriage return, tab, ANSI escape, and remaining controls; JSON and stored messages remain unchanged.
- G: diagnostics sorting now passes the comparator directly, removing the redundant closure.

## TDD evidence

- A RED: the later-tick daemon test observed `Reserved` instead of the expected `Pending`; the repository test initially failed to compile because `recover_expired_unattached` did not exist. GREEN: both focused tests passed.
- B RED: gate tests initially failed to compile because `tokio` lacked `io-util` and the gate helper/write API did not exist. A subsequent gate test caught the incorrect `shift` behavior (`exec: printf... not found`). GREEN: both gate tests passed after the fixed-argv implementation.
- C RED: diagnostics integration tests initially failed on the missing render APIs; the CLI event test then failed with `EOF while parsing a value` because the handler emitted no JSON. GREEN: 10 diagnostics integration tests and 5 CLI tests passed.
- D RED: `repeated_current_schema_open_does_not_rebuild_intervention_indexes` observed SQLite `schema_version` changing from 26 to 30. GREEN: the repeated-open, v5, and legacy-v6 migration tests passed.
- E RED: the oversized/control timestamp fixture failed `timestamp.len() <= 240` against the unbounded projection. GREEN: the sanitized fixture passed.
- F RED: text output did not contain the expected escaped `line\\n\\t\\x1b...` representation. GREEN: the text and JSON preservation test passed.

## Changed files

`Cargo.toml`, `README.md`, `src/agent.rs`, `src/db/migrations.rs`, `src/db/repositories.rs`, `src/diagnostics.rs`, `src/main.rs`, `src/scheduler.rs`, `tests/integration/cli_help.rs`, `tests/integration/daemon.rs`, `tests/integration/database.rs`, `tests/integration/diagnostics.rs`, `tests/integration/interventions.rs`, `tests/integration/scheduler.rs`, and `docs/superpowers/plans/2026-08-10-human-interventions-final-fix.md`.

The SDD ledger was not edited.

## Verification

All commands below were run in the assigned worktree after the remediation was restored:

- `cargo test --all-targets`: passed, 210 tests passed and 0 failed across all targets.
- `cargo fmt --check`: passed.
- `cargo clippy --all-targets --all-features -- -D warnings`: passed with no warnings.
- `bats tests/test_shell_entrypoints.bats`: passed, 3 tests.
- `shellcheck --shell=bash bin/pueue-agent tests/test_shell_entrypoints.bats`: passed, exit 0 with no output.
- `git diff --check`: passed.

The initial baseline focused run also exposed one `database::concurrent_first_opens_apply_migration_once` failure with `database is locked` while enabling SQLite WAL. The failure was in unchanged `Db::open`/`open_connection` WAL setup, before the migration-index changes. The same test passed in three repeated targeted runs after the changes and passed again in the final full `cargo test --all-targets` run. No WAL/open-connection code was changed; the evidence classifies the initial result as pre-existing/flaky rather than remediation-induced.

## Round 2: gate state, read-only doctor, and bounded diagnostics

### Root causes and fixes

- The launch gate had no durable boundary between “interventions applied” and “configured program confirmed released”. A new `agent_runs.launch_gate_state` state machine records `pending`, `release_requested`, `released`, or `failed`. The Unix launcher sends a fixed-argv release through stdin, waits for the gate ACK, and only then persists `released`. Confirmed pre-release write/EOF/ACK failures atomically fail the run and requeue both reserved and applied interventions. Startup recovery requeues attached work for pending/failed gates, while released gates retain applied interventions to preserve at-most-once behavior after confirmation. Non-Unix builds return an explicit unsupported-launcher error.
- Doctor previously opened the writable database path, allowing migration/index repair and WAL setup. `Db::open_read_only` now uses SQLite read-only flags, does not create parent directories or run migrations, and only configures connection-local busy timeout/foreign keys. Doctor project resolution uses this path exclusively.
- Doctor’s expired lease counts lacked `project_id` predicates, so foreign-project leases polluted the report. All three event, intervention, and termination queries now bind the requested project.
- Task inspection fetched the per-event run limit for every event before deduplication. It now enforces a global 64-run cap before each repository query.
- Pueue state summaries now use the same bounded/control-safe normalization as timestamps. Process-level CLI coverage now exercises inspect, explain, and doctor text/JSON behavior, including doctor’s nonzero error exit and no-repair guarantee.

### TDD evidence

- RED before production implementation: the focused database tests failed to compile because `Db::open_read_only` was missing at the read-only tests and `AgentRunRepository::fail_before_gate_release` was missing at the pre-release test. The current environment could not rerun these commands after edits because `cargo` is not installed (`zsh:1: command not found: cargo`).
- Additional RED tests were added for Pueue state sanitization, the global task-run cap, cross-project doctor lease isolation, and inspect/explain/doctor CLI processes. Their Rust execution is likewise blocked by the missing Cargo toolchain.
- GREEN evidence available in this environment: `bats tests/test_shell_entrypoints.bats` passed all 3 tests; ShellCheck and `git diff --check` passed with exit 0. Rust GREEN results are not claimable without Cargo.

### Round 2 verification

- `cargo fmt --check`: not run; exact result `zsh:1: command not found: cargo` (exit 127).
- `cargo test --all-targets`: not run; exact result `zsh:1: command not found: cargo` (exit 127).
- `cargo clippy --all-targets --all-features -- -D warnings`: not run; exact result `zsh:1: command not found: cargo` (exit 127).
- `bats tests/test_shell_entrypoints.bats`: passed, 3 tests.
- `shellcheck --shell=bash bin/pueue-agent tests/test_shell_entrypoints.bats`: passed, exit 0 with no output.
- `git diff --check`: passed, exit 0 with no output.

## Round 4: durable launch-gate acknowledgement

- RED: added database coverage for an absent marker requeue and a marker-confirmed `release_requested` run retaining applied interventions. Added Unix gate coverage for ACK ordering, EOF, and unavailable configured-program failure. Focused Rust commands were blocked before compilation by `zsh:1: command not found: cargo` (exit 127).
- Fix: the fixed-argv Unix gate now starts the configured child with redirected output, atomically commits `<log_path>.gate-started`, emits `released\n` only afterward, and waits/propagates the child status. It validates the configured executable before spawning and removes stale markers before a new run.
- Fix: startup recovery inspects `release_requested` marker paths before opening its transaction. Marker-confirmed runs are promoted to `released` and retain applied interventions; unconfirmed runs remain pre-execution and requeue reserved/applied interventions. ACK-following DB failure preserves marker-confirmed applied state and leaves recovery evidence instead of requeueing it.
- Documentation: the Unix-only plan now documents the log-derived marker and ACK ordering; non-Unix launchers remain explicit failures.
- `cargo test --test database startup_recovery_requeues_applied_interventions_after_release_request_before_ack startup_recovery_promotes_marker_confirmed_release_request_and_retains_applied_interventions`: blocked; exact result `zsh:1: command not found: cargo` (exit 127).
- `cargo test --lib launch_gate_exits_on_eof_without_executing_configured_agent launch_gate_acknowledges_only_after_child_spawn_and_marker_commit`: blocked; exact result `zsh:1: command not found: cargo` (exit 127).
- `cargo test --lib launch_gate_exits_without_ack_for_unavailable_configured_program`: blocked; exact result `zsh:1: command not found: cargo` (exit 127).
- `cargo fmt --check`: blocked; exact result `zsh:1: command not found: cargo` (exit 127).
- `cargo test --all-targets`: blocked; exact result `zsh:1: command not found: cargo` (exit 127).
- `cargo clippy --all-targets --all-features -- -D warnings`: blocked; exact result `zsh:1: command not found: cargo` (exit 127).
- `bats tests/test_shell_entrypoints.bats`: passed, 3 tests.
- `shellcheck --shell=bash bin/pueue-agent tests/test_shell_entrypoints.bats`: passed, exit 0 with no output.
- `git diff --check`: passed, exit 0 with no output.
- `bats tests/test_shell_entrypoints.bats`: passed, 3 tests.
- `shellcheck --shell=bash bin/pueue-agent tests/test_shell_entrypoints.bats`: passed, exit 0 with no output.

The required Rust verification is blocked by the execution environment’s missing Cargo/rustfmt toolchain, not by a reproduced repository failure. No reset or destructive cleanup was performed.

## Independent controller verification after `22d5f4e`

The assigned verification environment supplied the Rust toolchain through its task-specific PATH and reran the complete suite after correcting the doctor lease fixture:

- `cargo fmt --check`: passed.
- `cargo test --all-targets`: passed; all targets completed with 0 failures.
- `cargo clippy --all-targets --all-features -- -D warnings`: passed with no warnings.
- `bats tests/test_shell_entrypoints.bats`: passed, 3 tests.
- `shellcheck --shell=bash bin/pueue-agent tests/test_shell_entrypoints.bats`: passed.
- `git diff --check`: passed; worktree clean.

## Round 3: recover `release_requested` interventions

- RED: the new focused database tests were added for recovery of an applied intervention attached to a PID-bearing `release_requested` run and for rejecting `mark_gate_released` when no release request exists. Focused Rust execution was blocked before compilation by `zsh:1: command not found: cargo` (exit 127).
- Fix: startup recovery now treats `release_requested` as pre-execution, requeues attached reserved/applied interventions, and marks the interrupted run failed. An ACK-following `mark_gate_released` database failure now uses `fail_before_gate_release`, so applied interventions are requeued atomically.
- Documentation: the implementation plan now states Unix is the supported launch platform and non-Unix launchers fail explicitly.
- `cargo test --test database startup_recovery_requeues_applied_interventions_after_release_request_before_ack mark_gate_released_rejects_a_run_without_a_release_request`: blocked; exact result `zsh:1: command not found: cargo` (exit 127).
- `cargo fmt --check`: blocked; exact result `zsh:1: command not found: cargo` (exit 127).
- `git diff --check`: passed, exit 0 with no output.
- `bats tests/test_shell_entrypoints.bats`: passed, 3 tests.
- `shellcheck --shell=bash bin/pueue-agent tests/test_shell_entrypoints.bats`: passed, exit 0 with no output.
