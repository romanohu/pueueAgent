# Evaluation, Promotion & Goal Review Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record validated experiment metrics from optional result manifests, promote improvements against a declared objective metric, escalate plateaus to operators, and let decisions claim goal completion under operator review.

**Architecture:** Schema v24 adds structured objective fields on campaigns and an `experiment_metrics` table keyed by experiment. Terminal projection ingests manifests (idempotent), a comparison engine maintains `current_best_experiment_id` and a plateau counter emitting one strategy-refresh wake per round, and a new decision kind parks campaigns in `goal_reached_pending_review` behind an operator accept/reject CLI.

**Tech Stack:** Rust 2024 / rusqlite / serde_json / clap / existing daemon pass ordering / Bats + Linux real-Pueue E2E on roko.

**Spec:** `docs/superpowers/specs/2026-08-25-zero-adapter-evaluation-goal-review-design.md`

## Global Constraints

- Manifest-only promotion: log parsing never promotes or updates current_best.
- Manifest values must be finite JSON numbers; non-finite/missing id mismatch → row stored with `artifact_defect` set, experiment unaffected.
- Campaigns without `objective_metric_json` skip ingestion comparisons entirely (Phase 3 behaviour unchanged).
- Plateau wake is deduplicated per campaign+plateau round.
- Goal acceptance/rejection writes an operator-log entry and runs its state transition in one transaction.
- v24 migration follows forward-only style; LATEST_SCHEMA_VERSION becomes 24.
- Existing Phase 3 health machinery untouched except where this plan explicitly integrates (reconcile hook ordering after terminal projection).

---

### Task 1: Schema v24 + metrics repository

**Files:**
- Modify: `src/db/migrations.rs` (LATEST=24, migrate_to_v24, verify)
- Create: `src/db/experiment_metrics.rs`
- Modify: `src/db/mod.rs`
- Modify: `src/models.rs` (ObjectiveMetric, ExperimentMetricsRow)
- Test: `tests/integration/database.rs` (append)

**Interfaces:**
- Produces:
  - `pueue_agent::db::experiment_metrics::MetricsRepository`
    - `upsert(db,&ExperimentMetricsRow)->Result<(),AppError>`
    - `get(db,&experiment_id)->Result<Option<ExperimentMetricsRow>,AppError>`
  - `models::ObjectiveMetric { name:String, direction:MetricDirection, min_delta:Option<f64> }`, `MetricDirection::{Minimize,Maximize}` (serde snake_case)
  - `models::ExperimentMetricsRow { experiment_id, source, primary_metric_name:Option<String>, primary_metric_value:Option<f64>, metrics_json:String, artifact_defect:Option<String>, created_at, updated_at }`

- [ ] **Step 1: Failing migration test**

```rust
#[test]
fn schema_v24_adds_metrics_and_objective_columns() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
    let connection = db.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0)).unwrap();
    assert_eq!(version, 24);
    for column in ["objective_metric_json", "current_best_experiment_id", "plateau_count"] {
        let hit: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('campaigns') WHERE name=?1",
                [&column], |row| row.get(0)).unwrap();
        assert_eq!(hit, 1, "{column}");
    }
}
```

- [ ] **Step 2: RED run** (`cargo test --test database schema_v24 -- --exact` fails at 23).

- [ ] **Step 3: Implement v24** mirroring v22/v23 style:

```sql
ALTER TABLE campaigns ADD COLUMN objective_metric_json TEXT;
ALTER TABLE campaigns ADD COLUMN current_best_experiment_id TEXT;
ALTER TABLE campaigns ADD COLUMN plateau_count INTEGER NOT NULL DEFAULT 0;
CREATE TABLE experiment_metrics (
    experiment_id TEXT PRIMARY KEY REFERENCES experiments(experiment_id) ON DELETE CASCADE,
    source TEXT NOT NULL CHECK (source IN ('manifest')),
    primary_metric_name TEXT,
    primary_metric_value REAL,
    metrics_json TEXT NOT NULL DEFAULT '{}',
    artifact_defect TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
PRAGMA user_version = 24;
```

Add `models.rs` types (serde on ObjectiveMetric). Create repository file with upsert/get.

- [ ] **Step 4: Green + lifecycle repo test** (upsert twice → single row updated; get missing → None).

- [ ] **Step 5: Full database suite + commit** `feat: add evaluation schema v24 and metrics repository`

---

### Task 2: Submit flags, manifest env, terminal ingestion

**Files:**
- Modify: `src/campaign.rs` (submit accepts metric flags; persists objective_json), `src/cli.rs` (flags), `src/environment.rs` (env injection into agent task env), `src/reconcile.rs` (ingest hook after terminal projection), Create: `src/result_manifest.rs`
- Test: `tests/integration/reconciliation.rs` (append)

**Interfaces:**
- CLI (SubmitArgs): `--metric-name <n>`, `--metric-direction <minimize|maximize>`, `--metric-min-delta <f64>` (optional trio; all-or-nothing validation).
- Env on campaign experiment tasks: `PUEUE_AGENT_EXPERIMENT_ID`, `PUEUE_AGENT_CAMPAIGN_ID`, `PUEUE_AGENT_RESULT_PATH` (= `<root>/.pueue-agent/results/<task>.json`), `PUEUE_AGENT_ARTIFACT_DIR`.
- `result_manifest::ingest(db, project_root, project_id, experiment_id, pueue_task_id, now) -> Result<(),AppError>` — discovery/validation/persist per spec §4 rules.

- [ ] **Step 1: Failing tests**

```rust
// reconciliation.rs appends
#[tokio::test]
async fn terminal_projection_ingests_manifest_metrics() { ... }
// submit with --metric flags -> campaigns.objective_metric_json stored
// task env exposes PUEUE_AGENT_RESULT_PATH etc (existing env-capture fixture pattern)
// manifest with mismatched experiment_id -> artifact_defect='result_invalid', no metrics
// missing manifest -> artifact_defect='result_missing'
// non-finite value -> 'result_invalid'
```

(Full bodies mirror existing reconciliation harness patterns: fake pueue snapshot Done, run reconcile once, assert rows.)

- [ ] **Step 2: RED run.**

- [ ] **Step 3: Implement.**
  - `result_manifest.rs`: discovery order RESULT_PATH env of the finished task cannot be read post-exit → use fixed project-relative path first; validate via serde_json Value walk; store row via MetricsRepository.
  - environment.rs: extend campaign experiment env builder (same site as PUEUE_AGENT_RUN_ID).
  - reconcile.rs: after `project_terminal_experiment` success and only when campaign has `objective_metric_json`, call ingest.

- [ ] **Step 4: Green + full suite.**

- [ ] **Step 5: Commit** `feat: ingest result manifests on terminal projection`

---

### Task 3: Promotion engine

**Files:**
- Modify: `src/reconcile.rs` (post-ingest compare), Create: `src/promotion.rs`
- Test: `tests/integration/promotion.rs` (new)

**Interfaces:**
- `promotion::evaluate(db, campaign_id, experiment_id, now) -> Result<PromotionOutcome, AppError>`; `PromotionOutcome ∈ {Improved, NotImproved, SkippedNoMetric, SkippedNoObjective}`.
- Direction-aware boundaries: minimize improved iff `value < best - delta`; maximize iff `value > best + delta` (delta default 0).

- [ ] **Step 1: Failing tests**: improve-updates-current-best-and-resets-plateau; non-improve-increments; boundary exactly-at-delta-not-improved; metric-less-campaign-skips; baseline-first-comparison (no best yet).

- [ ] **Step 2: RED → implement → GREEN** (compare inside single Immediate tx wrapping current_best update + plateau reset).

- [ ] **Step 3: Suite green + commit** `feat: promote improving experiments against the objective metric`

---

### Task 4: Plateau counter + strategy refresh

**Files:**
- Modify: `src/promotion.rs`, `src/execution_policy.rs` (`CampaignLimits.plateau_threshold: u32` default 3 range 1..=20 + Raw), `src/events.rs` (StrategyRefresh operator wake reuse)
- Test: `tests/integration/promotion.rs` (append)

**Interfaces:**
- Non-improving completed experiments increment `plateau_count`; reaching threshold emits deduped operator wake event (`strategy-refresh:v1:<campaign>:<round>`), counter keeps counting; improvement resets.

- [ ] **Step 1: Failing tests**: third consecutive non-improvement emits exactly one wake; fourth emits none until an improvement resets; wake event visible to scheduler as OperatorWake.

- [ ] **Step 2: RED → implement → GREEN.**

- [ ] **Step 3: Commit** `feat: escalate experiment plateaus to strategy refresh wakes`

---

### Task 5: Goal-reached decisions + review CLI

**Files:**
- Modify: `src/decision_protocol.rs` (GoalReached variant + evidence_ref requirement), `src/db/campaigns.rs` (`transition_to_goal_review` + review transitions w/ operator log), `src/cli.rs` + `src/main.rs`/`src/campaign.rs` (`campaign review accept|reject [--note]`), `src/scheduler.rs` (goal campaign defers scheduling)
- Test: `tests/integration/goal_review.rs` (new)

**Interfaces:**
- Decision output adds `"decision":"goal_reached","evidence_ref":"<digest-or-metrics-ref>"`; validator requires referenced row exists.
- On apply: campaign → `goal_reached_pending_review`, decision event Completed, further claims for that campaign suppressed.
- CLI: accept → retired(`goal_accepted`) in same tx as operator-log entry; reject → active + claiming decision dead-lettered(`goal_claim_rejected`).

- [ ] **Step 1: Failing tests**: suspicious→diagnosis unaffected; goal_reached valid output parks campaign (status text shows it); scheduler stops dispatching that campaign; accept retires; reject resumes + dead-letter; invalid evidence ref degrades like malformed.

- [ ] **Step 2: RED → implement → GREEN** (reuse decision degradation counters).

- [ ] **Step 3: Full suite + commit** `feat: add goal reached decisions with operator review`

---

### Task 6: Status/diagnostics + E2E + docs

**Files:**
- Modify: `src/status.rs`, `src/diagnostics.rs`, `tests/e2e/rust_supervisor.sh`, `docs/commands-ja.md`, `docs/getting-started-ja.md`
- Test: golden status extension; bats unchanged

**Steps:**
- [ ] **Step 1:** status campaign section gains `best:` line (current_best experiment short-id + primary value when present) and `plateau:` count; diagnostics lists experiment_metrics rows (cap 50). Extend golden tests.
- [ ] **Step 2:** E2E additions (Linux): scenario C (manifest promotion): submit with `--metric-name loss --metric-direction minimize`, fake task writes result manifest then exits success; assert metrics row + current_best set + plateau reset. Scenario D (goal): fake codex returns goal_reached with valid evidence_ref; assert pending_review; operator accept via CLI; assert retired. Both bounded (retire/stop-chain at section end like phase-3 pattern).
- [ ] **Step 3:** bash -n; local suites; commit(s) `feat: surface evaluation state` / `test: exercise evaluation acceptance end to end` / `docs: document evaluation and goal review`.

---

## Final Verification

Full Linux gate on roko at merged HEAD (all five commands), evidence into SDD ledger, then merge/push upon user approval.
