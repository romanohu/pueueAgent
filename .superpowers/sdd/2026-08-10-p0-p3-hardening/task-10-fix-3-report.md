# Task 10 Fix Loop 3 Report

## Commits

- Implementation: `a93e56e` (`fix: harden canonical state and scheduler prompt`)
- Report: separate documentation commit, created after this report was added

## Scope

Implementation and regression tests cover:

- `state.json` presence detection via `symlink_metadata`, including dangling symlinks
- duplicate active lineage submission/task IDs
- current-fact duplicate detection after sentinel normalization
- rejection of unknown canonical budget keys while preserving partial allowed budgets
- `MAX_STATE_DEPTH = 32`, including 32 accepted and 33 rejected
- scheduler event-evidence allowlist projection with bounded/redacted safe fields only
- doctor and scheduler dangling-symlink regressions
- safe-reason replacement for the overlength prompt fixture after arbitrary evidence removal

Changed implementation/test paths in the implementation commit:

- `src/state.rs`
- `src/scheduler.rs`
- `tests/integration/diagnostics.rs`
- `tests/integration/scheduler.rs`

The pre-existing working-tree change in `docs/superpowers/plans/2026-08-10-p0-p3-hardening.md` was not staged.

## TDD and focused verification

The loop-3 tests were added before the corresponding production changes. The observed RED cases were:

- `cargo test --all-targets canonical_state`: the depth-boundary test failed while `MAX_STATE_DEPTH` was 8.
- `cargo test --all-targets scheduler`: the allowlist projection test failed because the old prompt contained raw JSON and did not emit `task_id=41`.

After implementation:

- `cargo test --all-targets canonical_state`: 17 passed, 0 failed.
  - library: 1
  - diagnostics: 11
  - init: 2
  - scheduler: 3
- `cargo test --all-targets scheduler`: 3 passed, 0 failed.
  - daemon: 1
  - scheduler: 2
- `cargo test --test scheduler`: 33 passed, 0 failed.

## Verification checks

- `git diff --check`: passed.
- Targeted rustfmt check for the four changed implementation/test paths: passed.
- `cargo fmt --check`: reports only the inherited Task 1 difference at `tests/integration/database.rs:339-344`.
- Full `cargo test --all-targets`: intentionally interrupted after focused verification at the parent request; parent-side full verification remains required.
