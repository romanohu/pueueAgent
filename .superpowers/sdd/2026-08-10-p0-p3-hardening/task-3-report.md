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

- `cargo test --all-targets --test database` — 45 passed.
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
- One final full-suite attempt hit a transient `database is locked` error while the existing
  concurrent-open test enabled WAL. Five subsequent focused runs passed; no Task 3 code touches
  that connection setup.
