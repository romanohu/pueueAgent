# Task 3 report — submission kind, metadata, and agent origin

## Scope delivered

- Added `SubmissionKind::{Experiment, Control}` and schema v7 submission columns:
  `kind TEXT NOT NULL DEFAULT 'experiment'`, `metadata_json TEXT NOT NULL DEFAULT '{}'`,
  and nullable `origin_agent_run_id`.
- Made the v6-to-v7 migration preserve existing submissions as
  `experiment` with `{}` metadata. The migration also tolerates historical fixtures
  that already have some v7 columns or lack the submissions table.
- Added project/kind/status and project/origin-agent-run indexes.
- Extended submission insert/select parsing, origin-run query, and fail-closed metadata parsing.
- Limited `count_started_or_accepted` to experiment submissions; controls do not consume
  the experiment guardrail.
- Preserved existing `NewSubmission::new` and submit behavior as experiment plus empty metadata.
- Did not change CLI options or the existing status/events JSON schema or fields.

## TDD evidence

- RED: `cargo test --all-targets control_submissions_do_not_consume_experiment_guardrail`
  failed before implementation because `SubmissionKind`, submission fields,
  `NewSubmission::with_kind_metadata`, and `list_by_origin_agent_run` did not exist.
- GREEN focused coverage passed for the experiment count, v6 migration defaults,
  project/run origin scoping, and malformed database metadata.

## Verification

- `cargo test --all-targets --test database` — 49 passed.
- `cargo test --all-targets scheduler` — 26 scheduler tests passed.
- `cargo test --all-targets reconciliation` — 13 reconciliation tests passed.
- `cargo test --all-targets` — passed.
- `git diff --check` — passed.
- `cargo fmt --check` remains non-zero only because of the pre-existing Task 1 formatting
  diff at `tests/integration/database.rs:281`. It was not modified by Task 3.

## Remaining concerns

- Task 4 owns strict external metadata size/depth/input validation and CLI origin propagation.
- Metadata read failures are fail-closed; malformed persisted values return an error rather than
  being silently interpreted.

## Review follow-up

- `SubmissionRepository::insert_idempotent` now verifies every supplied
  `origin_agent_run_id` inside its insert transaction. Missing runs and runs owned by another
  project return `AppError::Validation { field: "origin_agent_run_id", .. }` and do not create a
  submission. A matching project/run remains valid.
- Current-schema v7 opens now read `user_version` before beginning a migration transaction and
  return without rebuilding indexes. SQLite WAL is only enabled when the current journal mode is
  not already WAL. A barrier-based eight-open test covers concurrent v7 reopen without sleep.
- Doctor now requires both submission kind/origin indexes and reports `schema.indexes` as an
  error when either is missing.
- RED: the new origin rejection tests initially failed because `AppError::Validation` did not
  exist. GREEN: the origin, v7 reopen/concurrent open, and doctor index focused tests pass.
- Final verification after the review follow-up: `cargo test --all-targets` — 259 passed; `git
  diff --check` passed. `cargo fmt --check` remains non-zero only for the unchanged Task 1 readonly
  assertion (originally `tests/integration/database.rs:281`, shifted to line 282 by added tests).

## Database foreign-key follow-up

- The v7 submissions schema now enforces `FOREIGN KEY(project_id, origin_agent_run_id)` against
  `agent_runs(project_id, run_id)`. `ON DELETE RESTRICT` is intentional: SQLite `SET NULL` on a
  composite key would also null the non-null `project_id` column.
- v6 and pre-constraint v7 databases rebuild `submissions` in a transaction, preserve existing
  submission fields, retain valid origins, clear invalid legacy origins, and recreate all three
  submission indexes. Current compliant v7 databases do not enter that migration transaction.
- The database integration coverage verifies the composite FK, v7 rebuild preservation, direct
  SQL rejection of invalid origins, and foreign-key/WAL-compatible reopen behavior.
