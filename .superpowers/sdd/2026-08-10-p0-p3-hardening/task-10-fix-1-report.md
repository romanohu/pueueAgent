# Task 10 fix loop 1 report — canonical budgets and sentinel consistency

## Commits

- Implementation and test commit: `38ef749` (`fix: enforce canonical state guardrails`)
- This report is committed separately.

## Scope delivered

- Added `state::load_effective_guardrails` and `CanonicalState::effective_guardrails`.
  A missing `state.json` preserves TOML guardrails for compatibility; valid canonical
  `max_experiments`, `max_agent_runs`, and `max_consecutive_failures` entries override
  their corresponding TOML values, including zero.
- Scheduler now passes the effective guardrail configuration to `Guardrails::check`.
  Invalid present canonical state fails the claimed events closed; it is not silently
  treated as a missing state file.
- `check_consistency` now recognizes exact normalized `current_facts` sentinels and
  combines `campaign active` with active lineage. STATE.md checks accept only standalone
  or heading sentinel lines, so historical prose does not trigger a contradiction.
- Added focused regressions for canonical `max_experiments=0`, normalized current facts,
  heading sentinels, and historical markdown prose. Existing invalid-state doctor errors
  and contradiction warnings remain unchanged.

## TDD and verification evidence

- RED before production changes:
  `cargo test --all-targets canonical_state` — 2 diagnostics tests failed:
  `canonical_state_doctor_uses_normalized_current_fact_for_consistency` and
  `canonical_state_doctor_ignores_historical_markdown_prose`.
- Focused GREEN after implementation:
  `cargo test --all-targets canonical_state` — 9 passed, 0 failed
  (diagnostics: 6; init: 2; scheduler: 1; other targets: 0 selected).
- Full GREEN:
  `cargo test --all-targets` — 316 passed, 0 failed.
  Breakdown: library 14; cli_help 21; config 28; daemon 13; database 67;
  detection 13; diagnostics 35; init 9; interventions 7; operator_commands 13;
  pueue_adapter 24; reconciliation 13; scheduler 30; service 10; termination 19.
- `git diff --check`: passed.
- Targeted `rustfmt --edition 2021 --check` for all four implementation/test paths:
  passed.
- `cargo fmt --check`: non-zero only for the inherited Task 1 difference at
  `tests/integration/database.rs:339-344`; no Task 10 fix file is affected.

## Changed paths in the implementation commit

- `src/scheduler.rs`
- `src/state.rs`
- `tests/integration/diagnostics.rs`
- `tests/integration/scheduler.rs`

## Report path

- `.superpowers/sdd/2026-08-10-p0-p3-hardening/task-10-fix-1-report.md`

The pre-existing modified plan file
`docs/superpowers/plans/2026-08-10-p0-p3-hardening.md` remains unstaged and is not part
of either fix-loop commit.
