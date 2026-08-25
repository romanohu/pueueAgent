# Zero-adapter running health and resumable observer design

Date: 2026-08-25
Phase: 3 (running-health separation, signals, resumable periodic observer,
confirmed cancellation, finite live repair)
Base: `main` at `7c91780` (Phase 2 autonomous completion loop, schema v21)

## 1. Purpose

Running experiments currently have no durable health model. Detection is a
config-driven regex pass over running tasks, incidents are advisory, and the
only bounded responses are operator-configured kills. A stalled or failing
experiment is invisible to the campaign until the task happens to exit.

Phase 3 adds an experiment-scoped running-health state machine, structured
signal classification, a resumable periodic observer, a read-only diagnosis
agent, and finite confirmed actions on live experiments. It unifies the
existing detection/incident/termination machinery behind one health state
machine instead of replacing it.

## 2. Requirements

1. Each running experiment carries a durable health state that survives daemon
   restarts and is separate from the Pueue task state.
2. Signals are classified into built-in classes (`oom`, `numerical`,
   `worker_loss`, `staleness`, `exception`) plus config-declared classes.
3. A resumable observer evaluates each running experiment on a fixed interval
   (campaign policy `observer_interval_minutes`, default 30). Evaluation is
   service-side; no agent runs for healthy experiments.
4. Suspicion escalation runs one bounded, read-only diagnosis agent that
   returns a schema-validated recommendation.
5. Confirmed actions on live experiments are limited to: continue,
   kill-and-resume-resubmit, kill-and-escalate. Every destructive action
   requires a termination request in `Confirmed` state.
6. Live interventions are finite per experiment (`max_live_repairs`).
7. Existing `check.patterns` behaviour is preserved: `action="kill"` remains a
   pre-confirmed immediate path through the existing termination manager;
   `action="wake"` becomes a signal input.
8. All Phase 2 guarantees (decision cycles, budgets, restart safety) remain
   intact.

## 3. Non-goals (later phases)

- Goal review and quantitative promotion (later phase).
- Isolated code worktrees and automated code changes (Phase 5).
- True in-process live manipulation (signals to a running trainer beyond kill).
- Result manifest ingestion.

## 4. Design choice

```text
running experiment
  -> observer tick (every observer_interval_minutes)
       -> signal classification (built-in probes + check.patterns hits)
            |-- healthy          -> update counters, stay healthy
            `-- suspicion        -> diagnosing state
                 -> one read-only diagnosis agent (bounded evidence)
                      -> recommended_action
                           |-- continue            -> healthy (counters reset)
                           |-- kill_and_resume     -> Confirmed kill -> terminal
                           |                          projection -> same-spec
                           |                          resubmit with checkpoint
                           |                          metadata (max_live_repairs)
                           `-- kill_and_escalate   -> Confirmed kill -> campaign
                                                      degraded + operator wake
```

The unified machine absorbs the existing flows:

- `check.patterns` with `action="kill"` produce an immediately confirmed
  signal that skips diagnosis and drives the existing termination manager.
- `action="wake"` patterns and stall observations become classified signals.
- Incidents remain the human-facing record; they are opened by confirmed
  actions and resolved when the experiment reaches a terminal state.

## 5. Data model (schema v22)

One new table plus two nullable columns on `experiments` (checkpoint-resume
lineage); no other existing tables change.

```sql
ALTER TABLE experiments ADD COLUMN resume_of_experiment_id
    TEXT REFERENCES experiments(experiment_id);
ALTER TABLE experiments ADD COLUMN checkpoint_note TEXT;
```

```sql
CREATE TABLE running_health (
    experiment_id     TEXT PRIMARY KEY REFERENCES experiments(experiment_id)
                      ON DELETE CASCADE,
    campaign_id       TEXT NOT NULL REFERENCES campaigns(campaign_id)
                      ON DELETE CASCADE,
    project_id        TEXT NOT NULL REFERENCES projects(project_id)
                      ON DELETE CASCADE,
    pueue_task_id     INTEGER NOT NULL,
    state             TEXT NOT NULL CHECK (state IN (
                          'healthy', 'suspicious', 'diagnosing',
                          'action_pending')),
    observation_count INTEGER NOT NULL DEFAULT 0,
    last_observed_at  INTEGER NOT NULL,
    signal_summary_json TEXT NOT NULL DEFAULT '[]',
    diagnosis_json    TEXT,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL
);
CREATE INDEX running_health_due_idx
    ON running_health (last_observed_at);
CREATE INDEX running_health_campaign_state_idx
    ON running_health (campaign_id, state);
```

- `signal_summary_json`: bounded array (last 32 entries) of
  `{class, source, evidence_digest, observed_at}`.
- `diagnosis_json`: latest diagnosis `{root_cause_class, confidence,
  recommended_action, summary}` (bounded).
- The row is created lazily when an experiment first enters running state and
  deleted when the experiment reaches a terminal status.
- `last_observed_at` is the observer session anchor: restarts resume from it.

Migration v22 follows the repository pattern used by v18-v21 (forward-only,
transactional, verified via `PRAGMA user_version`).

## 6. Signal classification

`SignalClass` enum: `oom`, `numerical`, `worker_loss`, `staleness`,
`exception`, plus config-declared names.

Sources:

1. Built-in log probes over the bounded tail already collected by the
   detector:
   - `oom`: `CUDA out of memory`, `OutOfMemoryError`, `OOMKilled`.
   - `numerical`: `NaN`, `inf`, `divergence` on metric lines.
   - `worker_loss`: dataloader worker / rank process terminated messages.
   - `exception`: Python traceback markers not matched by a configured
     pattern.
2. `staleness`: existing log-mtime stall logic, re-labelled as a signal.
3. Config patterns: each hit contributes its configured class. New optional
   `PatternConfig.class` field; unset patterns keep legacy wake/kill actions
   without contributing a class.

Signals are derived inside the existing detector walk so there is still one
log read per observation pass.

## 7. Observer session

- Interval: `CampaignLimits.observer_interval_minutes` (existing field,
  default 30, range 1..1440). This phase makes it authoritative for running
  experiments; the legacy deep-check scheduler keeps operating unchanged for
  projects without active campaigns.
- Due condition: `now >= last_observed_at + interval`.
- One observation updates: signal set, counters, `last_observed_at`.
- Suspicion rules (service-side, deterministic):
  - two consecutive observations with the same non-staleness class;
  - any staleness signal beyond `stall_minutes`;
  - a Kill-action pattern match (immediate confirmation, skip diagnosis).

Suspicion escalation transitions the row to the `suspicious` state and then
spawns one diagnosis agent, which moves it to `diagnosing` until its output is
persisted. Kill-action pattern matches skip both states and drive the
confirmed action directly.

## 8. Diagnosis agent

- Same execution envelope as the decision agent: startup-pinned codex,
  read-only sandbox, network policy inherited, sanitized environment,
  schema-validated JSON output persisted before cleanup.
- Input evidence bundle: bounded signal history, current log tail excerpt,
  experiment/campaign identifiers, objective digest. No credentials.
- Output schema:
  `{root_cause_class, confidence, recommended_action, summary}` where
  `recommended_action` is one of `continue`, `kill_and_resume`,
  `kill_and_escalate`.
- Invalid output follows the decision-protocol degradation pattern: bounded
  retries then dead-letter with `health_diagnosis_missing`.
- Diagnosis runs are recorded as agent runs (`execution_kind = 'diagnosis'`)
  and consume the campaign `agent_run` rolling budget.

## 9. Confirmed action execution

- `kill_and_resume` / `kill_and_escalate` create a termination request and may
  only dispatch after the request reaches `Confirmed` using the existing
  termination manager (dispatch lease, confirmation grace, TimedOut/Failed
  handling unchanged).
- After the killed task projects terminal:
  - `kill_and_resume`: insert checkpoint-resume metadata on the successor
    experiment (`resume_of_experiment_id`, `checkpoint_note` bounded text),
    submit the identical argv through the existing coordinator, and count one
    live repair. When `max_live_repairs` (new `CampaignLimits` field, default
    2, range 0..=8) is exhausted the action degrades to `kill_and_escalate`.
  - `kill_and_escalate`: campaign transitions to `degraded` with an operator
    wake event; no automatic resubmission.
- `continue` resets the signal window and returns the row to `healthy`.

## 10. Legacy integration

- `PatternConfig.action = "kill"`: signal severity is immediate; the health
  machine skips suspicion/diagnosis and calls the termination manager exactly
  once per incident key (existing single-kill guarantee preserved).
- `PatternConfig.action = "wake"`: contributes a classified signal; if the
  accumulated suspicion escalates, the normal diagnosis flow runs instead of
  the legacy direct Crash-event enqueue.
- Stall handling keeps its notify/kill configuration; kill goes through the
  same confirmed-termination gate.

## 11. Daemon integration

`run_once` order becomes:

1. startup recovery
2. reserved submission dispatch
3. decision recovery
4. reconciler
5. detection (unchanged walk; feeds classifier)
6. **observer + health transitions (new)**
7. termination processing (unchanged)
8. periodic deep-check scheduler (unchanged, non-campaign projects)
9. agent poll / decision apply / campaign wake
10. scheduler tick

Single-writer discipline is preserved; all health writes happen inside this
pass under the existing project admission rules.

## 12. CLI surface

- `status` gains a health section per running experiment: state, top signal
  class, observation age, last diagnosis action.
- `diagnostics` lists `running_health` rows with bounded signal summaries.
- No new subcommands.

## 13. Security

- Diagnosis agents reuse the decision-agent envelope verbatim: pinned
  executable, read-only sandbox, filtered environment, credential-name
  stripping, bounded persisted output.
- Health rows store digests and class labels only; raw log lines never enter
  SQLite.
- Confirmed-kill authority remains operator/config owned; the health machine
  can never extend budgets or retry limits.

## 14. Recovery windows

| Crash point | Recovery owner |
|---|---|
| Before health row insert | next observer pass recreates it |
| After insert, before diagnosis spawn | row stays `suspicious`; next pass re-runs diagnosis |
| Diagnosis agent spawned, output unpersisted | startup marker evidence dead-letters the attempt; row returns to `suspicious`; attempt counted |
| Action decided, kill dispatched | termination manager owns confirmation; restart-safe |
| Kill confirmed, before terminal projection | reconcile observes Killed task; AutoKilled flow unchanged |
| Resume resubmit add ambiguity | existing unreconciled quarantine semantics |

## 15. Testing

- Unit: classifier mapping table, suspicion thresholds, leaf validation of
  diagnosis schema, action-constraint checks (Confirmed gate,
  max_live_repairs exhaustion).
- Integration: observer due/resume across daemon restart; escalation spawns
  exactly one diagnosis agent; each recommended action executes through the
  confirmed gate; budget consumption; paused/disabled/halted campaigns defer
  everything; legacy kill-pattern parity (fatal-pattern scenario unchanged).
- E2E additions on Linux: stall-kill path; OOM-log simulation producing
  kill_and_resume with checkpoint metadata; daemon restart while diagnosing.
- Existing suites must remain green; the fatal-pattern E2E section asserts the
  same single-incident/single-kill guarantees.

## 16. Acceptance criteria

1. A running experiment accumulates durable, restart-surviving health state.
2. Healthy experiments never trigger agent invocations from the observer.
3. Suspicion escalation produces exactly one bounded diagnosis agent run and a
   validated recommendation.
4. Destructive recommendations execute only through a Confirmed termination.
5. Live repairs are finite per experiment and degrade the campaign when
   exhausted.
6. Legacy kill-pattern scenarios behave identically to Phase 2.
7. Full local suites and the Linux real-Pueue gate pass.
