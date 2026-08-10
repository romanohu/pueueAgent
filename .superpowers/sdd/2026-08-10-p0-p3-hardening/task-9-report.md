# Task 9 report — idempotent `submit-batch` CLI

## Commits

- Implementation and test commit: `3d49b8c` (`feat: add idempotent submit batch command`)
- This report is committed separately.

## Scope delivered

- Added `Command::SubmitBatch(SubmitBatchArgs)` with `--request-id`, `--manifest`,
  `--group`, `--json`, and optional `PROJECT_ROOT`.
- Added bounded manifest reading (1 MiB), strict `{ "jobs": [...] }` parsing,
  Task 8 bounds, metadata validation, unique job IDs/ordinals, and stable FNV-1a
  manifest hashing over normalized job data.
- Validated the registered project/configuration and exact group before any Pueue add.
- Persisted batch and per-job submission intent before external calls, claimed a
  BatchRepository dispatch lease, and passed its opaque lease token to every accepted
  or failed result update.
- Submitted jobs in ordinal order, stopped after an add failure, and resumed only
  pending jobs on a later invocation. Accepted jobs are not re-added.
- Added bounded, redacted shared human/JSON batch rendering without raw Pueue output,
  command arguments, environment, prompts, or metadata blobs.
- Kept Task 10 canonical state/doctor work out of scope.

## TDD and verification evidence

- RED before production changes:
  `cargo test --all-targets submit_batch_cli` — 6 failed, 0 passed. All failures
  were the expected missing `submit-batch` command/contract.
- Focused GREEN after implementation and adapter-double coverage:
  `cargo test --all-targets submit_batch_cli` — 7 passed, 0 failed
  (cli_help: 6; pueue_adapter: 1; other targets: 0 selected).
- Full GREEN:
  `cargo test --all-targets` — 307 passed, 0 failed.
  Breakdown: library 14; cli_help 21; config 28; daemon 13; database 67;
  detection 13; diagnostics 29; init 7; interventions 7; operator_commands 13;
  pueue_adapter 24; reconciliation 13; scheduler 29; service 10; termination 19.
- `git diff --check`: passed for the implementation diff.
- `cargo clippy --all-targets --all-features -- -D warnings`: blocked only by the
  inherited `unnecessary_map_or` lint at `src/runs.rs:82,322`.
- Clippy with the two inherited exceptions allowed
  (`unnecessary_map_or`, `needless_borrows_for_generic_args`): passed.
- `cargo fmt --all -- --check`: non-zero only for the inherited Task 1 difference
  at `tests/integration/database.rs:339-344`; Task 9 files are formatted.

## Changed paths in the implementation commit

- `src/batches.rs`
- `src/cli.rs`
- `src/db/repositories.rs`
- `src/main.rs`
- `src/submit.rs`
- `tests/integration/cli_help.rs`
- `tests/integration/pueue_adapter.rs`

The pre-existing modified plan file `docs/superpowers/plans/2026-08-10-p0-p3-hardening.md`
was left unstaged and is not part of either Task 9 commit.
