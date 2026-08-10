# Final review remediation report

Date: 2026-08-10
Base review range: `2bb4fe8..c7b3ac1`

## Root causes and fixes

- A: periodic scheduler recovery only recovered event leases. `InterventionRepository::recover_expired_unattached` now runs on every scheduler tick and only requeues `reserved` rows whose `agent_run_id IS NULL`. Startup recovery for attached starting, failed, and live runs remains unchanged.
- B: the configured agent was previously spawned before the run/intervention transaction committed. Unix launches now use a fixed-argv `/bin/sh` gate over piped stdin; the configured program and arguments are positional parameters, and the release line is written only after the transaction succeeds. EOF or an invalid release exits without executing the configured program. Spawn/preflight failures still finish the run and release reservations. Non-Unix builds retain an explicit direct-launch fallback.
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
