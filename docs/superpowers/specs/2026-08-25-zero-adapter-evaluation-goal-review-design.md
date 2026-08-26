# Zero-adapter evaluation, promotion and goal review design

Date: 2026-08-25
Phase: 4 (baseline vs current-best evaluation, optional result-manifest
discovery, plateau escalation, goal review)
Base: `codex/phase3-running-health` at `591e70e` (schema v23)

## 1. Purpose

Phase 2 gave campaigns a terminal decision loop, but nothing in SQLite knows
whether an experiment *improved* anything. The objective is free text; metric
values are never recorded; "next experiment" is chosen by an LLM with no
quantitative anchor; goal claims cannot be expressed. Phase 4 closes the
evaluation loop so numeric exploration no longer depends on the LLM.

## 2. Requirements

1. Campaigns can declare a structured objective metric: name, direction
   (`minimize` | `maximize`), and optional minimum improvement delta.
2. Experiment tasks receive the environment needed to publish results and may
   write a JSON result manifest. On task terminal projection the manifest is
   discovered, validated, and its metrics persisted per experiment.
3. When the primary metric is present for a completed experiment, it is
   compared against the current best (baseline first). Improvements beyond
   `min_delta` update `current_best_experiment_id` and record a promotion.
4. Completed experiments without promotion increment a plateau counter;
   reaching the threshold emits one `strategy_refresh` operator wake event
   (campaign stays active).
5. The decision protocol gains a third kind, `goal_reached`, which requires an
   evidence reference. Accepting it transitions the campaign to
   `goal_reached_pending_review`; an operator CLI (`campaign review
   accept/reject`) finalizes retirement or resumes the campaign.
6. Manifest absence or invalidity records `artifact_defect:result_missing`
   (bounded) without failing the experiment; log parsing never promotes.
7. Campaigns without a declared metric keep today's behaviour end to end.

## 3. Non-goals

- Replication/holdout pipelines (spec §12.3) — later phase.
- HPO optimizer interfaces (master-spec Phase 4/HPO section) — later phase.
- Code worktrees (Phase 5). Log-parsing promotion.

## 4. Data model (schema v24)

```sql
ALTER TABLE campaigns ADD COLUMN objective_metric_json TEXT;
ALTER TABLE campaigns ADD COLUMN current_best_experiment_id TEXT
    REFERENCES experiments(experiment_id);
ALTER TABLE campaigns ADD COLUMN plateau_count INTEGER NOT NULL DEFAULT 0;

CREATE TABLE experiment_metrics (
    experiment_id TEXT PRIMARY KEY REFERENCES experiments(experiment_id)
                  ON DELETE CASCADE,
    source        TEXT NOT NULL CHECK (source IN ('manifest')),
    primary_metric_name  TEXT,
    primary_metric_value REAL,
    metrics_json  TEXT NOT NULL DEFAULT '{}',
    artifact_defect TEXT,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);
```

- `objective_metric_json`: `{"name","direction","min_delta":number|absent}`.
  Written at submit time when CLI flags are given; otherwise NULL.
- Manifest discovery order: `$PUEUE_AGENT_RESULT_PATH` if set, else
  `.pueue-agent/results/<pueue_task_id>.json` inside the project root.
- Validation: JSON object, `schema_version == 1`, `experiment_id` matches the
  projected experiment, every metric value finite numbers only, paths in the
  manifest are never followed. Invalid → row stored with
  `artifact_defect='result_invalid'` and empty metrics.
- Absent file → `artifact_defect='result_missing'`.

## 5. Promotion engine

On terminal projection of an experiment belonging to an active campaign with
a declared metric:

1. Load `experiment_metrics`. If primary value absent → no comparison; if the
   experiment status is `succeeded`, increment `plateau_count`.
2. Compare against current best (baseline experiment first): direction-aware
   delta must be `>= min_delta` (default 0 means strictly better by any real
   margin: minimize requires `value < best - min_delta`, maximize
   `value > best + min_delta`).
3. Improvement → set `current_best_experiment_id`, reset `plateau_count`,
   insert a promotion marker into the campaign audit trail (operator wake
   event kind reused, summary bounded).
4. No improvement → increment `plateau_count`; at `plateau_threshold`
   (new `CampaignLimits` field, default 3, range 1..=20) emit exactly one
   `strategy_refresh` operator wake event (dedup per campaign+plateau round)
   and leave the campaign active.

## 6. Goal review

- `DecisionKind::GoalReached { evidence_ref }` joins Proposal | Wait. The
  decision validator requires `evidence_ref` to reference a stored artifact
  digest or metrics row.
- Accepted parse transitions the campaign to
  `goal_reached_pending_review`, stops further scheduling for that campaign,
  and stores the claim.
- New CLI: `pueue-agent campaign review accept [--note]` /
  `campaign review reject [--note]` on the project:
  - accept: campaign → `retired` with reason `goal_accepted`; pending events
    suppressed through the retired-lineage path.
  - reject: campaign → `active`, the claiming decision event dead-lettered
    (`goal_claim_rejected`), plateau counter unchanged.

## 7. Security

Manifest contents are validated numbers/ids only; artifact paths are recorded
as digests, never followed. Env additions are names only — no values cross
from the supervisor beyond ids already present. Review CLI actions append
bounded operator-log entries like existing campaign mutations.

## 8. Recovery windows

| Crash point | Owner |
|---|---|
| Manifest written, not yet ingested | next reconcile pass ingests idempotently (PK on experiment_id) |
| Promotion decided, current_best update crashed | transaction wraps compare+update |
| Plateau event emitted twice | dedup key per campaign+round |
| Goal accepted but retire interrupted | retire transition is the same transaction as review log |

## 9. Testing

- Unit: manifest validation table (valid/invalid/missing/mismatched id/
  non-finite/traversal); direction-aware comparison boundaries including
  min_delta; plateau counter transitions.
- Integration: submit-with-metric flag persists objective_json; terminal
  ingestion stores metrics; promotion updates current_best and resets plateau;
  non-improvement increments and emits one strategy_refresh per round;
  goal_reached decision parks campaign and CLI accept/reject behave; metric-less
  campaigns skip everything.
- E2E (Linux): baseline completes without metric (no promotion churn);
  scenario with manifest-driven promotion; goal_reached → operator accept →
  retired.

## 10. Acceptance criteria

1. A campaign can declare a metric and have completed experiments carry
   persisted, validated metric rows.
2. Improvement beyond min_delta moves current_best exactly once per
   experiment and resets the plateau counter.
3. Non-improving completions escalate to exactly one strategy_refresh wake
   per plateau round.
4. A goal_reached decision freezes the campaign pending review; operator
   accept retires it, reject resumes it.
5. Metric-less campaigns are behaviourally identical to Phase 3.
