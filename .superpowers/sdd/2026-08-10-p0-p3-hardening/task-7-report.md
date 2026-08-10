# Task 7 report — P2 common human CLI output

## Commits

- Implementation and test commit: `239501f` (`style: clarify human cli output`)
- This report is committed separately.

## Changes

- Added bounded shared human header and summary helpers in `src/output.rs`.
- Standardized human identifiers to `event=`, `run=`, `task=`, and `sub=` forms.
- Applied the shared contract to human `status`, `events`, `submit`, `wake`, and `runs` output.
- Added explicit state fields and bounded summaries while preserving the distinction between Pueue task state and supervisor event/run state.
- Kept human ANSI styling behind terminal detection and `NO_COLOR`; JSON branches remain undecorated.
- Preserved existing JSON object fields/schema versions, Pueue adapter behavior, and command error/exit behavior.
- Kept command, reason, project/group identity, and error summaries bounded/redacted through existing helpers.

## TDD RED → GREEN evidence

- RED was observed before production changes with `cargo test --all-targets cli_output_contract`. The first failing contract showed the old raw events line (`event 1 ...`) without the required header, `event=` ID, state field, or summary.
- The separate focused RED runs also failed the status contract on the old `task 41`/`event 1` forms and the submit contract on the old single-line `submission=...` form.
- GREEN: `cargo test --all-targets cli_output_contract --quiet` — 4 passed, 0 failed (2 in `cli_help`, 1 in `diagnostics`, 1 in `pueue_adapter`).
- Contract coverage includes human output for status/events/submit/wake/runs, JSON purity and parseability, pipe ANSI suppression, `NO_COLOR`, bounded/redacted values, and the terminal styling helper boundary.

## Verification

- `cargo test --all-targets --quiet` — 289 passed, 0 failed.
  - library unit tests: 14
  - binary unit tests: 0
  - integration tests: 275
- `git diff --check` — passed.
- `cargo fmt --check` — non-zero only for the inherited Task 1 formatting difference in `tests/integration/database.rs:339-344`; those lines were not modified by Task 7. All Task 7-touched Rust files were formatted and no other format diff remains.

## Changed paths in the implementation commit

- `src/diagnostics.rs`
- `src/main.rs`
- `src/output.rs`
- `src/runs.rs`
- `src/status.rs`
- `src/submit.rs`
- `tests/integration/cli_help.rs`
- `tests/integration/diagnostics.rs`
- `tests/integration/operator_commands.rs`
- `tests/integration/pueue_adapter.rs`

The inherited modified plan file was left unstaged and is not part of either Task 7 commit.
