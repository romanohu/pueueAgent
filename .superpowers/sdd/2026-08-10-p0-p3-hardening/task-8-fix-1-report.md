# Task 8 fix loop 1 report — batch lease tokens

## Commits

- Implementation and test commit: `26891b1` (`fix: bind batch results to lease tokens`)
- This report is committed separately.

## Scope delivered

- Added SQLite schema v10 from v9 with a bounded nullable `batch_requests.lease_token`
  column. The v10 migration preserves the v9 migration body and is idempotent when a
  legacy test database already contains the column.
- Added `lease_token: Option<String>` to `BatchRequest`, including the batch SELECT and
  row mapping used by all repository reads.
- Generated a fresh UUID v4 token on every successful batch claim or reclaim.
- Required the token for `record_job_result` and guarded accepted/failed job updates by
  project, request, token, and an unexpired active lease.
- Cleared the token on lease recovery, failed/partial finalization, and completion.
  A late worker result from a recovered lease is rejected without mutating the job.
- Updated every existing `record_job_result` call site to use the token returned by
  `claim`.
- Made replay behavior explicit in tests: a completed or failed request has no active
  token, so replay with the old token is rejected; accepted-result idempotency remains
  available only while the same lease is active.

## TDD and verification evidence

- Initial fix-loop RED: `cargo test --all-targets batch_` exited 101 during compilation
  because `BatchRequest` row construction did not yet supply `lease_token`.
- Focused GREEN: `cargo test --all-targets batch_` — 12 passed, 0 failed
  (database: 11; pueue_adapter: 1; other targets: 0 selected).
- Full GREEN: `cargo test --all-targets` — 300 passed, 0 failed
  (library unit tests: 14; binary unit tests: 0; integration tests: 286).
- `git diff --check` — passed.
- `cargo fmt --all -- --check` remains non-zero only for the inherited Task 1 formatting
  difference in `tests/integration/database.rs:339-344`; no lease-token formatting
  difference remains.

## Changed paths in the implementation commit

- `src/db/migrations.rs`
- `src/db/repositories.rs`
- `src/models.rs`
- `tests/integration/database.rs`
- `tests/integration/pueue_adapter.rs`

The pre-existing modified plan file was left unstaged and is not part of either commit.
