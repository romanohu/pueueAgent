# Task 8 report — durable batch repository/state machine

## Commits

- Implementation and test commit: `52868f5` (`feat: add durable batch submission state`)
- This report is committed separately.

## Scope delivered

- Added schema v9 tables `batch_requests` and `batch_jobs` with project foreign keys,
  request/job uniqueness, bounded manifest/error/argv/metadata fields, status checks,
  and lease/job indexes.
- Added `BatchStatus` and `BatchJobStatus` models plus bounded `NewBatchRequest` and
  `NewBatchJob` inputs.
- Added `BatchRepository::{create_or_get, find, claim, record_job_result,
  recover_expired}`.
- Persisted batch and job intent before any external add result is recorded. Accepted
  Pueue task IDs and submission IDs are durable; repeated accepted results are idempotent.
- Partial or ambiguous failure records the failed job and error, preserves accepted jobs,
  resets later unaccepted jobs to pending, and clears the dispatch lease.
- Expired leases reset only dispatching jobs. Accepted jobs and their external IDs are
  retained and are never re-submitted by recovery.
- Kept the SQLite/Pueue boundary explicit: this task does not claim a distributed atomic
  transaction and does not add the Task 9 `submit-batch` CLI.

## TDD RED → GREEN evidence

- Initial required RED: `cargo test --all-targets batch_` exited 101 before production
  batch code existed. Compilation failed on the missing `batches`, `BatchRepository`,
  and batch model APIs referenced by the new tests.
- A later idempotency regression RED failed only
  `batch_accepted_result_is_idempotent_after_request_completion` because a replayed
  accepted result was rejected after completion.
- Final focused GREEN: `cargo test --all-targets batch_` — 10 passed, 0 failed
  (database: 9; pueue_adapter: 1; all other targets: 0 selected).

## Verification

- `cargo test --all-targets` — 298 passed, 0 failed
  (library unit tests: 14; binary unit tests: 0; integration tests: 284).
- `git diff --cached --check` — passed before the implementation commit.
- `cargo clippy --all-targets --all-features -- -D warnings -A clippy::unnecessary_map_or -A clippy::needless_borrows_for_generic_args` — passed.
- Unmodified full clippy configuration still reports pre-existing lint differences in
  `src/runs.rs:82,322` and `tests/integration/operator_commands.rs:323,332`; those files
  were not changed by Task 8.
- `cargo fmt --check` remains non-zero only for the inherited Task 1 formatting difference
  in `tests/integration/database.rs:339-344`; no Task 8 formatting difference remains.

## Changed paths in the implementation commit

- `src/batches.rs`
- `src/db/migrations.rs`
- `src/db/mod.rs`
- `src/db/repositories.rs`
- `src/lib.rs`
- `src/models.rs`
- `tests/integration/database.rs`
- `tests/integration/pueue_adapter.rs`

The pre-existing modified plan file was left unstaged and is not part of either Task 8 commit.
