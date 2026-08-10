# Task 8 fix loop 2 report — restore batch replay idempotency

## Commits

- Implementation and test commit: `bdcc7e0` (`fix: restore batch result replay idempotency`)
- This report is committed separately.

## Scope delivered

- `record_job_result` now reads the current batch job, including `last_error`, before
  enforcing the active lease token.
- An Accepted result with the same Pueue task ID and submission ID is returned from a
  read-only batch read even after the lease has been cleared.
- A Failed result with the same error is returned from a read-only batch read even after
  the lease has been cleared.
- Conflicting accepted or failed replays remain validation errors and do not mutate the
  stored job.
- Only a new mutation from a dispatching job requires the project-scoped active lease
  token. The stale worker A/B recovery regression remains covered.
- Replaced the new batch `unnecessary_map_or` use with `is_some_and`.

## TDD and verification evidence

- RED after updating replay expectations and before production changes:
  `cargo test --all-targets batch_` — 9 passed, 2 failed. The failures were the
  completed accepted replay and failed partial replay, both rejected by the premature
  active-lease check.
- Focused GREEN: `cargo test --all-targets batch_` — 12 passed, 0 failed
  (database: 11; pueue_adapter: 1; other targets: 0 selected).
- Full GREEN: `cargo test --all-targets` — 300 passed, 0 failed
  (library unit tests: 14; binary unit tests: 0; integration tests: 286).
- Raw clippy: blocked only by the pre-existing `unnecessary_map_or` lints in
  `src/runs.rs:82,322`.
- Clippy with the known pre-existing `unnecessary_map_or` and
  `needless_borrows_for_generic_args` lints allowed: passed.
- `git diff --check`: passed.
- `cargo fmt --all -- --check` remains non-zero only for the inherited Task 1 formatting
  difference in `tests/integration/database.rs:339-344`.

## Changed paths in the implementation commit

- `src/db/repositories.rs`
- `tests/integration/database.rs`

The pre-existing modified plan file was left unstaged and is not part of this fix loop
commit or its report commit.
