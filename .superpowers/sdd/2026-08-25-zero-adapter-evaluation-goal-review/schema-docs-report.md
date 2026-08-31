# Phase 4 current-v25 schema verification and metrics-only documentation report

## Status

Complete. The current-v25 open path now fails closed for malformed
`experiment_metrics` schemas, while fresh-v25, canonical-v24 migration, and
legacy-v24 crash-window behavior remain green. The Phase 4 prose now treats a
persisted metrics row as the only goal evidence and defers artifact-digest
evidence persistence to a future schema migration.

## Base and head

- Branch: `codex/phase4-evaluation-goal-review`
- Base: `0cec818e976513c63c297337f81741353f95760e`
- Implementation head: `8aefe7f542a687e808725f4c7849662e6995b625`

## Files changed

- `src/db/migrations.rs`
  - Added the canonical v25 `experiment_metrics` table definition.
  - Added normalized table-SQL comparison plus exact column metadata and
    foreign-key checks to the current-v25 verifier.
  - Kept the existing v24/v25 migration flow and schema version unchanged.
- `tests/integration/database.rs`
  - Added real SQLite current-v25 regressions for a changed `source` CHECK and
    a non-canonical experiment foreign-key action.
- `docs/superpowers/specs/2026-08-25-zero-adapter-evaluation-goal-review-design.md`
  - Restricted `goal_reached.evidence_ref` to a persisted metrics row in the
    same project/campaign lineage and documented deferred artifact-digest
    persistence.
- `docs/superpowers/plans/2026-08-25-zero-adapter-evaluation-goal-review.md`
  - Applied the same evidence rule and removed `<digest-or-metrics-ref>`.

## TDD RED evidence

The malformed source-CHECK regression was written before production changes
and run with:

```text
cargo test --test database current_v25_schema_rejects_a_noncanonical_experiment_metrics_check -- --exact --test-threads=1
```

It failed under the old verifier because `Db::open` returned `Ok`:

```text
called `Result::unwrap_err()` on an `Ok` value: Db { ... }
test result: FAILED. 0 passed; 1 failed
```

After adding the minimal FK-shape regression, the pre-implementation combined
run also showed both malformed fixtures accepted by the weak verifier:

```text
cargo test --test database current_v25_schema_rejects -- --test-threads=1
```

```text
test result: FAILED. 0 passed; 2 failed
called `Result::unwrap_err()` on an `Ok` value: Db { ... }
```

## Implementation

The current-v25 verifier now requires the complete canonical table SQL,
including the `source` manifest-only CHECK, the `metrics_json` `{}` default,
all v24 columns, and the nullable TEXT `evaluated_at` column. It also compares
the complete ordered column/type/nullability/primary-key shape and the exact
`experiment_id` foreign key to `experiments(experiment_id)` with `ON DELETE
CASCADE`. The SQL compaction tolerates only SQLite's formatting differences
between an `ALTER TABLE`-produced v25 table and the v25 rebuild used by the
legacy crash-window migration.

The verification runs in the existing `version == LATEST_SCHEMA_VERSION`
fast path before any migration transaction, so malformed databases are
rejected without repair or version changes.

## GREEN verification

- `cargo test --test database current_v25_schema_rejects -- --test-threads=1`
  - 2 passed, 0 failed.
- `cargo test --test database -- --test-threads=1`
  - 223 passed, 0 failed.
- `cargo test --test goal_review -- --test-threads=1`
  - 13 passed, 0 failed.
- `cargo check --all-targets`
  - exited 0.
- `git diff --check`
  - exited 0.

The full database suite covered fresh v25, canonical v24 migration, and
legacy-v24 marker migration cases in addition to the new malformed-v25 tests.

## Self-review

- No schema version bump, artifact-digest schema/repository support, goal-review
  behavior change, dependency change, or unrelated cleanup was added.
- The verifier reuses the existing canonical column and foreign-key comparison
  helpers and rejects both listed load-bearing mutations.
- Documentation changes are limited to the contradictory goal-evidence prose;
  `<digest-or-metrics-ref>` no longer appears in the design or plan.
- The implementation commit contains only the four requested files and is
  followed by this separate report commit.

## Concerns

`cargo fmt --all -- --check` is already failing on the repository baseline with
large unrelated diffs across files such as `build.rs`, `src/agent.rs`,
`src/status.rs`, `src/service.rs`, `src/upgrade.rs`, and many integration tests.
Those unrelated files were not rewritten. Compilation, the required focused
and full suites, and `git diff --check` are green.
