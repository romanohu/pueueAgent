# Task 10 fix loop 2 report — NotFound-only canonical state fallback

## Commits

- Implementation and test commit: `7666a46` (`fix: fail closed on present canonical state`)
- This report is committed separately.

## Scope delivered

- Added `state::load_if_present`, which returns `Ok(None)` only when canonical
  `state.json` loading returns `io::ErrorKind::NotFound`; all other I/O and schema
  errors are returned as `AppError`.
- `load_effective_guardrails` now uses that helper. A directory, permission failure,
  read failure, or invalid canonical state cannot fall back to TOML budgets in the
  scheduler.
- Doctor now uses the same helper: truly missing `state.json` is a warning, while a
  present directory or read/parse/schema failure is a `state.schema` error.
- Added regressions for a `state.json` directory in doctor and scheduler. The scheduler
  regression verifies the event fails closed and no agent run starts; the existing
  `max_experiments=0` canonical-budget regression remains covered.

## TDD and verification evidence

- RED before production changes:
  `cargo test --all-targets canonical_state` —
  `canonical_state_doctor_reports_state_directory_as_error` failed because the
  directory was incorrectly reported as `warning`/missing.
- Focused GREEN after implementation and formatting:
  `cargo test --all-targets canonical_state` — 11 passed, 0 failed
  (diagnostics: 7; init: 2; scheduler: 2; other targets: 0 selected).
- Full GREEN:
  `cargo test --all-targets` — 318 passed, 0 failed.
  Breakdown: library 14; cli_help 21; config 28; daemon 13; database 67;
  detection 13; diagnostics 36; init 9; interventions 7; operator_commands 13;
  pueue_adapter 24; reconciliation 13; scheduler 31; service 10; termination 19.
- `git diff --check`: passed.
- Targeted `rustfmt --edition 2021 --check` for all implementation/test paths:
  passed.
- `cargo fmt --check`: non-zero only for the inherited Task 1 difference at
  `tests/integration/database.rs:339-344`.

## Changed paths in the implementation commit

- `src/state.rs`
- `src/diagnostics.rs`
- `tests/integration/diagnostics.rs`
- `tests/integration/scheduler.rs`

## Report path

- `.superpowers/sdd/2026-08-10-p0-p3-hardening/task-10-fix-2-report.md`

The pre-existing modified plan file
`docs/superpowers/plans/2026-08-10-p0-p3-hardening.md` remains unstaged and is not
part of either fix-loop commit.
