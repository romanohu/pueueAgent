# Phase 4 final-review fix wave report

## Status

DONE

## Commit hashes

- `9bb127a59001c02eff00a598fad277047682e836` — `fix: finalize evaluation goal review safeguards`

## Files changed

- `src/db/experiment_metrics.rs`
- `src/db/migrations.rs`
- `src/db/repositories.rs`
- `src/promotion.rs`
- `src/reconcile.rs`
- `src/result_manifest.rs`
- `tests/integration/database.rs`
- `tests/integration/promotion.rs`
- `tests/integration/reconciliation.rs`

## Implementation summary

- Terminal manifest classifications now freeze the first `valid`, `result_missing`, or `result_invalid` row. Only a retryable `result_io_error` row can be replaced by later terminal evidence.
- Reconciliation skips manifest ingestion when a terminal experiment already has metrics, then retries unsettled evaluation after an ingest/projection/evaluation crash window.
- Promotion evaluation fails closed when the metrics row is missing, settles existing rows exactly once for inactive/no-objective campaigns, and requires exactly one row update when marking evaluation.
- Schema v25 now uses a nullable `TEXT` `evaluated_at` marker. Canonical v24 migration backfills terminal rows, while legacy marker migrations preserve existing values including terminal `NULL` values. Current-schema verification rejects missing or malformed markers.
- Promotion audit events are inserted as completed passive events. Exact legacy pending events are converted in-transaction; mismatched kind, lineage, payload, or status collisions fail closed.
- Duplicate and contradictory promotion tests were removed; migration coverage now lives in `tests/integration/database.rs`.

## RED evidence

- The inherited promotion run exposed stale missing-metrics expectations, a legacy integer marker read against the final `TEXT` column, and migration tests that recreated tables already created by `Db::open`.
- After the test set was reduced to scenario-accurate cases, the pending legacy promotion audit regression remained red because the existing pending event was not converted to completed. The repository collision path was then implemented and the regression passed.
- The I/O recovery regression was corrected to require the intermediate `result_io_error` marker and later valid replacement; the real reconciliation path passed with those assertions.

## Verification

- `cargo test --test promotion -- --test-threads=1` — PASS, 16 passed, 0 failed.
- `cargo test --test reconciliation -- --test-threads=1` — PASS, 48 passed, 0 failed.
- `cargo test --test database -- --test-threads=1` — PASS, 221 passed, 0 failed.
- `cargo test --test goal_review -- --test-threads=1` — PASS, 13 passed, 0 failed.
- `cargo check --all-targets` — PASS.
- `git diff --check` — PASS.

## Self-review and remaining concerns

- No focused implementation concerns remain after the required suites and compile check.
- `cargo fmt --all -- --check` reports pre-existing formatting differences across unrelated repository files; no repository-wide formatting was run and no unrelated files were changed.
- No merge, push, or roko access was performed.
