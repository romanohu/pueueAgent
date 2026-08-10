# Task 10 report — canonical structured state and doctor consistency

## Commits

- Implementation and test commit: `eeb3241` (`feat: validate structured experiment state`)
- This report is committed separately.

## Scope delivered

- Added bounded `CanonicalState` loading and validation in `src/state.rs` with strict top-level and lineage schemas, bounded facts/arrays/text/object depth, duplicate current-fact rejection, and validated non-negative budget values.
- Added `templates/state.json` and made `init` create it only when missing. Existing `STATE.md`, `state.json`, `instructions.md`, and `config.toml` contents are preserved.
- Integrated read-only doctor checks for missing, valid, and invalid canonical state; state schema errors are errors, while `STATE.md` sentinel contradictions are warnings. Doctor summaries expose bounded current-fact, historical-fact, next-action, budget, and active-lineage counts without prompt or transcript content.
- Replaced doctor’s stale SQLite schema literal with the exported migration `LATEST_SCHEMA_VERSION` constant.
- Updated the agent prompt and instructions template so `state.json` is canonical machine state and `STATE.md` remains supplementary context. `templates/STATE.md` was not modified.

## TDD and verification evidence

- RED before production changes:
  `cargo test --all-targets canonical_state` — 4 diagnostics tests failed before the missing state checks/schema-version implementation; the init tests were not reached because Cargo stopped after the diagnostics target failed.
- Focused GREEN after implementation:
  `cargo test --all-targets canonical_state` — 6 passed, 0 failed (diagnostics: 4; init: 2; other targets: 0 selected).
- Full GREEN after formatting:
  `cargo test --all-targets` — 313 passed, 0 failed.
  Breakdown: library 14; cli_help 21; config 28; daemon 13; database 67;
  detection 13; diagnostics 33; init 9; interventions 7;
  operator_commands 13; pueue_adapter 24; reconciliation 13; scheduler 29;
  service 10; termination 19.
- `git diff --check`: passed.
- `cargo fmt --check`: non-zero only for the inherited Task 1 difference at
  `tests/integration/database.rs:339-344`; all Task 10 files were formatted.

## Changed paths in the implementation commit

- `src/db/migrations.rs`
- `src/db/mod.rs`
- `src/diagnostics.rs`
- `src/init.rs`
- `src/lib.rs`
- `src/scheduler.rs`
- `src/state.rs`
- `templates/instructions.md`
- `templates/state.json`
- `tests/integration/diagnostics.rs`
- `tests/integration/init.rs`
- `tests/integration/scheduler.rs`

The pre-existing modified plan file `docs/superpowers/plans/2026-08-10-p0-p3-hardening.md`
was left unstaged and is not part of either Task 10 commit.
