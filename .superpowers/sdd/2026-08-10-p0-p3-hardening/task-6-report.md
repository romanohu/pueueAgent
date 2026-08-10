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
- `cargo fmt --check`: fails only at the pre-existing Task 1 formatting difference in `tests/integration/database.rs:339-344`; it was not modified by Task 6.

## Remaining concerns

- The requested global formatting check remains non-zero solely because of the inherited Task 1 formatting difference above. No Task 6 formatting differences remain.
- Follow polling is deliberately read-only and does not contact Pueue or control agent processes; it reports only persisted SQLite lineage.

## Review follow-up

- Selected runs are now limited before lineage expansion. Each selected run reads its primary event and bounded origin submissions independently, so an older submission cannot be hidden by unrelated newer submissions.
- Follow now consumes `FollowCursor::take_ordered` in bounded cursor batches and emits a lineage containing only the newly selected submissions; an unaccompanied run/event cursor emits the same lineage with no submissions.
- New RED/GREEN regressions cover `--limit 1` with an old selected-run submission and pending-cursor ordering, batching, partial lineage projection, and deduplication.
- Follow-up verification: `cargo test --all-targets` completed with 279 passed, 0 failed; `git diff --check` passed.
- Follow-up commit: `ddfdf4fcdf4f7b5f0694fe8d2cc39e2f43d66998` (`fix: preserve run lineage follow cursors`).
- Event-only cursor follow-up: `RunLineageCursor::event_only(started_at, event_id)` preserves the existing four-argument `new()` API while making same-second event-only lineages distinct.
- Event-only regression: `cargo test --all-targets` completed with 280 passed, 0 failed; `git diff --check` passed.
- Event-only commit: `35ab8d74857076107a29f8f0a46d1feb6fef1207` (`fix: distinguish event-only follow cursors`).
- Fresh review RED: repository polling with `--limit 1` returned only one of two submissions and the root cursor produced an empty submission projection.
- Fresh review GREEN: selected runs now page all origin submissions internally, while `collect_fresh` treats `--limit` as an output batch size and emits submission cursors before a root cursor for populated lineages. A 1000-row page boundary regression verifies no submission is stranded.
- Fresh review verification: `cargo test --all-targets` completed with 282 passed, 0 failed; `git diff --check` passed. `cargo fmt --check` still reports only the inherited Task 1 difference at `tests/integration/database.rs:339-344`.
- Fresh review implementation commit: `54ae1d99c595fa9e82bb842de7ec99545f8672f1` (`fix: page follow run submissions`).

## Fresh review fix loop 2

- Implementation commit: `9af8268ceb03de0a072766f7e418fe98bde14866` (`fix: bound follow lineage pagination`).
- Normal `RunLineageRepository::list_by_project` now fetches only a bounded per-run page; it no longer materializes all submissions for a selected run.
- Follow uses `list_by_project_follow` with per-run keyset continuation. `created_at < after_created_at` (with `submission_id` tie-break) advances toward older submissions, while a separate head cursor accepts newer submissions. Boundary rows are re-read with bounded single-row queries so `task_id` changes such as `None -> Some` remain observable.
- `follow_runs` passes cloned `after` and `head` maps on every read-only SQLite poll. `FollowCursor` uses bounded per-run maps and bounded pending/root state; submission history is not retained in an unbounded `seen` set. `--limit` remains the output batch limit, distinct from the internal page/cursor caps.
- RED: the 1001-submission repository regression returned 1001 rows from normal `list_by_project`, and the cursor-state regression left more than one pending cursor after a limit-one collection. GREEN: normal reads return one bounded page; the repository follow path emits `new -> old` with `--limit 1`, later emits a newer head submission, and eventually observes all 1001 submissions.
- Focused GREEN: `collect_fresh` tests (4 passed), the repository `new -> old`/head regression (1 passed), and the 1001-submission continuation regression (1 passed).
- Full verification: `cargo test --all-targets` — 284 passed, 0 failed; `git diff --check` — passed.
- `cargo fmt --check` remains non-zero only for the inherited Task 1 formatting difference in `tests/integration/database.rs:339-344`; Task 1 lines were not modified. Changed source files have no remaining formatting differences.

## Remaining concerns

- The follow page performs bounded boundary/head probes in addition to the one-row continuation page; the resulting internal lineage is capped by `MAX_FOLLOW_LINEAGE_SUBMISSIONS` and output remains capped by `--limit`.
- As above, the repository-wide format check cannot be zero without changing the explicitly excluded Task 1 formatting lines.

## Fresh review fix loop 3

- Implementation commit: `7fe16301489e987bcf865131085e7325b4ed4184` (`fix: page originless follow submissions`).
- Originless submissions (`origin_agent_run_id IS NULL`) now use the same bounded project-scoped keyset continuation as run submissions. Follow reserves stream key `0`; actual agent run IDs are positive.
- Added originless project page-after, page-since, and page-at repository queries. Normal `list_by_project` behavior remains unchanged, while follow groups the bounded originless page into one project-scoped lineage before applying the output `--limit`.
- `collect_fresh` retains stream key `0` whenever an originless lineage has submissions, preventing the continuation from being discarded between polls. Event-only cursor behavior remains unchanged.
- RED: the SQLite regression initially returned the newly inserted originless head on the second limit-one poll instead of the older submission because the incomplete projection still used the latest project query and truncated individual submissions by `remaining`.
- GREEN: the regression now observes `originless-new -> originless-old -> originless-old(task_id=88) -> originless-head` through the repository follow path.
- Full verification: `cargo test --all-targets` — 285 passed, 0 failed; `git diff --check` — passed.
- `cargo fmt --check` reports only the inherited Task 1 formatting difference at `tests/integration/database.rs:339-344`; Task 1 lines were not modified.

## Fresh review fix loop 4

- Implementation commit: `c23ffeeee06a50203f577fad840bbc4a3eac4e85` (`fix: keep originless follow candidates`).
- Follow candidate collection no longer applies normal-list `remaining` truncation. Runs, recent events, and the bounded originless page are all retained as bounded candidates; `collect_fresh(limit)` remains the output and pending-state control point. Normal `list_by_project` truncation is unchanged.
- Added the SQLite mixed-project regression: with one run and two originless submissions under `--limit 1`, the run is emitted first, the originless stream advances on the next poll, then its old continuation and newly inserted head are emitted.
- RED: the second poll was empty because `remaining = limit - lineages.len()` was zero after selecting the run.
- GREEN: the mixed run/originless regression passes, together with the existing originless task-update and event-only regressions.
- Full verification: `cargo test --all-targets` — 286 passed, 0 failed; `git diff --check` — passed.
- `cargo fmt --check` still reports only the inherited Task 1 formatting difference at `tests/integration/database.rs:339-344`; Task 1 lines were not modified.
