# Task 6 report — P2 runs/lineage と `--follow`

## Commit

Implementation commit: `2e625700c3f19523bafb25173f2bc059622c1272` (`feat: expose agent run lineage`).

## Changes

- Added `pueue-agent runs [--json] [--follow] [--limit N]`; limits fail closed outside `1..=128`.
- Added a project-scoped lineage projection covering event, agent run, origin submission, and Pueue task ID. Missing event/run/origin/task links are represented explicitly rather than assumed.
- Added bounded/redacted human and JSON summaries. The projection excludes event payloads, submission argv/metadata, agent log paths, prompts, and transcripts.
- Added read-only SQLite polling for `--follow`, Ctrl-C cancellation, and cursor-based deduplication.
- Did not implement Task 7/P3 work.

## RED → GREEN evidence

1. RED: `cargo test --all-targets runs_` failed because the `runs` module, lineage repository, and follow cursor did not exist.
2. GREEN: the same focused runs test passed after the minimal CLI/repository/renderer implementation.
3. RED: the read-only polling test failed because `Iterator::any` short-circuited after recording only the first submission cursor.
4. GREEN: recording every cursor before deciding whether a lineage is fresh made the polling test pass.

## Verification

- Focused tests: `runs_`, `runs_repository_scopes_lineage_and_keeps_incomplete_submissions`, and `follow_cursor_deduplicates_orders_and_respects_limit` passed.
- `cargo test --all-targets`: 277 passed, 0 failed.
- `git diff --check`: passed before the implementation commit.
- `cargo fmt --check`: fails only at the pre-existing Task 1 formatting difference in `tests/integration/database.rs:338-340`; it was not modified by Task 6.

## Remaining concerns

- The requested global formatting check remains non-zero solely because of the inherited Task 1 formatting difference above. No Task 6 formatting differences remain.
- Follow polling is deliberately read-only and does not contact Pueue or control agent processes; it reports only persisted SQLite lineage.

## Review follow-up

- Selected runs are now limited before lineage expansion. Each selected run reads its primary event and bounded origin submissions independently, so an older submission cannot be hidden by unrelated newer submissions.
- Follow now consumes `FollowCursor::take_ordered` in bounded cursor batches and emits a lineage containing only the newly selected submissions; an unaccompanied run/event cursor emits the same lineage with no submissions.
- New RED/GREEN regressions cover `--limit 1` with an old selected-run submission and pending-cursor ordering, batching, partial lineage projection, and deduplication.
- Follow-up verification: `cargo test --all-targets` completed with 279 passed, 0 failed; `git diff --check` passed.
- Follow-up commit: `ddfdf4fcdf4f7b5f0694fe8d2cc39e2f43d66998` (`fix: preserve run lineage follow cursors`).
