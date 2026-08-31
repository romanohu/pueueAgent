# Phase 4 final broad-review fix wave report

## Status

DONE

## Base, branch, and worktree

- Base: `ab38c03ddea1aa9e63469b311086bdeb719f3696`
- Head: `ecf1f10` (implementation commit; report commit follows)
- Branch: `codex/phase4-evaluation-goal-review`
- Worktree: `/Users/suzuki_f/project/pueueAgent/.worktrees/phase4-evaluation-goal-review`
- Date: 2026-08-31

## Commit hashes

- `ecf1f10` — `fix: close phase 4 broad review findings` (implementation and tests)
- report commit — `docs: record phase 4 broad-review fix wave` (this report)

## Files changed

- `src/cli.rs`
- `src/daemon.rs`
- `src/db/campaigns.rs`
- `src/main.rs`
- `src/models.rs`
- `src/project_logs.rs`
- `src/promotion.rs`
- `src/reconcile.rs`
- `src/result_manifest.rs`
- `tests/integration/promotion.rs`
- `tests/integration/reconciliation.rs`

## Finding 1: promotion campaign lineage

### RED evidence

The three real-SQLite regressions were added before the production checks and
were run against the pre-fix implementation:

- `evaluation_rejects_candidate_from_another_campaign_without_mutation` did
  not reject the campaign-B candidate; the expected
  `Validation { field: "experiment_id", .. }` match failed.
- `evaluation_rejects_cross_campaign_persisted_comparison_pointers_before_mutation`
  reached `Ok(NotImproved)` instead of the expected error from the corrupted
  current-best/baseline pointer.
- `defect_metric_value_cannot_promote_a_best_experiment` returned
  `Improved` for a numeric metric row carrying `artifact_defect`, instead of
  the expected `NotImproved`.

### Implementation decisions

- Validate the persisted objective, candidate experiment, and every non-null
  comparison pointer before any evaluation marker, plateau, best-pointer, or
  event mutation.
- Scope primary metric lookup through `experiments.campaign_id` and require
  `artifact_defect IS NULL`, including idempotent outcome lookup.
- Keep the existing transaction, promotion, plateau, and retry/idempotency
  behavior for valid same-campaign rows.

## Finding 2: startup-pinned manifest reads

### RED evidence

`terminal_projection_rejects_results_parent_symlink_outside_pinned_root` was
added before the reader change. With the old pathname implementation, the
reconciliation path followed the replaced `results` symlink and did not return
the expected `AppError::PolicyViolation`; the outside valid manifest was
therefore reachable. The regression then passed after descriptor-relative
opening was installed.

### Implementation decisions

- `Daemon::run_once` now passes its startup `Arc<ResolvedExecutionPolicy>` to
  `Reconciler`.
- Immediately before ingestion, reconciliation resolves the stored project
  root through that policy anchor, verifies its identity, and constructs a
  `ProjectRootLogReader` from the resulting `VerifiedProjectRoot` descriptor.
- Manifest candidates are exactly the relative paths
  `.pueue-agent/results/<experiment_id>.json` and then
  `.pueue-agent/results/<pueue_task_id>.json`. Every parent component uses
  descriptor-relative `openat` with no-follow flags; final entries use
  nonblocking no-follow opens and descriptor metadata checks.
- The 16 KiB bound, missing/invalid/retryable-I/O classification, final
  regular-file/FIFO/symlink handling, and first-terminal-evidence freeze are
  preserved. The existing public helper remains available for direct callers;
  production reconciliation uses only the verified-root reader path.

## Finding 3: non-negative minimum delta

### RED evidence

The negative-delta regressions were added before validation and were run
against the pre-fix implementation:

- The existing negative CLI case called `unwrap_err()` but received `Ok` with
  parsed `-0.5`.
- `campaign_rejects_negative_objective_metric_delta_before_insert` called
  `unwrap_err()` but campaign acceptance returned `Ok`.
- `persisted_negative_delta_fails_closed_before_worse_result_promotion`
  called `unwrap_err()` but promotion returned `Ok(Improved)` for a worse
  result.

### Implementation decisions

- `ObjectiveMetric::validate` rejects non-finite and negative `min_delta`.
- Clap parsing rejects finite values below zero while retaining `None`, `0.0`,
  and both metric directions.
- Campaign acceptance validates before insertion, and persisted objective JSON
  is validated before promotion can mutate any marker, plateau, pointer, or
  event.

## GREEN verification

- New promotion regressions, including the candidate-lineage, pointer-lineage,
  defect-metric, negative-acceptance, and persisted-negative cases: PASS as
  part of `cargo test --test promotion -- --test-threads=1` — 21 passed, 0
  failed.
- New parent-results-symlink regression plus existing manifest symlink, FIFO,
  nonregular, missing, invalid, and I/O-recovery coverage: PASS as part of
  `cargo test --test reconciliation -- --test-threads=1` — 53 passed, 0
  failed.
- `cargo test --test database --quiet -- --test-threads=1` — PASS, 224
  passed, 0 failed.
- `cargo test --test goal_review -- --test-threads=1` — PASS, 13 passed, 0
  failed.
- `cargo test --test daemon --quiet -- --test-threads=1` — PASS, 53 passed,
  0 failed.
- `cargo check --all-targets` — PASS.
- `git diff --check` — PASS.

## Self-review and concerns

- The implementation commit changes only the nine required source files and
  two focused integration test files; no schema, dependency, adapter, or
  unrelated formatting changes were made.
- The known repository-wide rustfmt baseline remains unchanged; no broad
  formatter rewrite was run.
- The full `pueue_adapter` suite was rerun in isolation: 86 passed and the
  unchanged `pueue_launch_phases_share_one_absolute_deadline` test failed its
  pre-existing `started.elapsed() < 700ms` timing assertion. Its focused rerun
  reproduced the same failure, and no edited file touches that process-launch
  path. The initial parallel run also had two additional timing/process-fixture
  failures; those passed in the isolated rerun.
- No merge, push, or roko access was performed.
