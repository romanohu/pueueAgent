# Zero-Adapter Running Health & Resumable Observer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every running experiment a durable health state machine fed by structured signal classification and a resumable periodic observer, escalating through one read-only diagnosis agent to finite confirmed actions (continue / kill_and_resume / kill_and_escalate).

**Architecture:** New `running_health` table (schema v22) keyed by experiment; a `HealthEngine` inside the existing daemon `run_once` pass consumes classified signals from the extended detector and drives state transitions healthy→suspicious→diagnosing→action_pending; destructive actions route through the existing termination manager under a `Confirmed` gate and are bounded by `CampaignLimits.max_live_repairs`.

**Tech Stack:** Rust 2024 / Tokio / rusqlite (bundled SQLite), serde_json, SHA-256 digests, existing decision-agent envelope for diagnosis runs, Bats + Linux real-Pueue E2E on `roko`.

**Spec:** `docs/superpowers/specs/2026-08-25-zero-adapter-running-health-design.md`

## Global Constraints

- Schema is forward-only; v22 migration must set `PRAGMA user_version = 22` transactionally and verify canonically (mirror v21 style in `src/db/migrations.rs`).
- Raw log lines never enter SQLite; health rows store class labels, digests, and bounded JSON only.
- `signal_summary_json` holds at most 32 entries; oldest entries are dropped.
- Legacy parity: `check.patterns` with `action="kill"` keeps producing exactly one incident + exactly one Pueue kill per incident key, skipping suspicion/diagnosis.
- Destructive actions require a termination request whose status is exactly `Confirmed`.
- Live repairs per experiment are bounded by `max_live_repairs` (default 2, range 0..=8); exhaustion degrades to escalation.
- Daemon `run_once` order stays: recovery → reserved dispatch → decision recovery → reconcile → detection → **observer/health** → termination → deep-check → poll → apply → wake → tick.
- All new public surface lives behind `pueue_agent::` paths; integration tests may call `pueue_agent::agent::relative_log_path`-style pub items.
- Every task ends with `cargo check --all-targets` clean and its tests green before commit.

---

### Task 1: Schema v22 + HealthRepository

**Files:**
- Modify: `src/db/migrations.rs` (add v22; bump LATEST)
- Create: `src/db/running_health.rs`
- Modify: `src/db/mod.rs` (register module)
- Modify: `src/models.rs` (enums)
- Test: `tests/integration/database.rs` (append)

**Interfaces:**
- Produces:
  - `pueue_agent::db::running_health::HealthRepository`
    - `ensure_running(db:&Db, project_id:&str, campaign_id:&str, experiment_id:&str, pueue_task_id:i64, now:i64) -> Result<(),AppError>`
    - `get(db,&experiment_id) -> Result<Option<RunningHealthRow>,AppError>`
    - `due_observations(db, now, interval_minutes:u32, limit) -> Result<Vec<RunningHealthRow>,AppError>`
    - `record_observation(db,&experiment_id, observed_at, SignalSummaryEntry) -> Result<(),AppError>`
    - `set_state(db,&experiment_id, HealthState, now) -> Result<(),AppError>`
    - `store_diagnosis(db,&experiment_id, &serde_json::Value, now) -> Result<(),AppError>`
    - `reset_to_healthy(db,&experiment_id, now) -> Result<(),AppError>`
    - `delete_for_experiment(db,&experiment_id) -> Result<(),AppError>` (called on terminal projection)
  - `models::{HealthState}` enum (`Healthy/Suspicious/Diagnosing/ActionPending` ↔ strings), `RunningHealthRow`, `SignalSummaryEntry{class,source,evidence_digest,observed_at}`.

- [ ] **Step 1: Write failing migration test**

Append to `tests/integration/database.rs`:

```rust
#[test]
fn schema_v22_creates_running_health_and_survives_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
    let connection = db.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 22);
    let columns: Vec<String> = connection
        .prepare("SELECT name FROM pragma_table_info('running_health')")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(columns.contains(&"state".to_string()));
    assert!(columns.contains(&"last_observed_at".to_string()));
}
```

- [ ] **Step 2: Run it — expect failure**

`cargo test --test database schema_v22 -- --exact` fails (version is 21).

- [ ] **Step 3: Implement v22**

In `src/db/migrations.rs`: set `LATEST_SCHEMA_VERSION = 22`; add `migrate_to_v22(connection)` after the v21 function following the file's established shape:

```rust
fn migrate_to_v22(connection: &Connection) -> Result<(), AppError> {
    let already = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='running_health'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(database_error("probe SQLite v22 running_health"))?;
    if already == 0 {
        connection
            .execute_batch(
                "CREATE TABLE running_health (
                    experiment_id     TEXT PRIMARY KEY REFERENCES experiments(experiment_id)
                                      ON DELETE CASCADE,
                    campaign_id       TEXT NOT NULL,
                    project_id        TEXT NOT NULL,
                    pueue_task_id     INTEGER NOT NULL,
                    state             TEXT NOT NULL CHECK (state IN (
                                          'healthy','suspicious','diagnosing','action_pending')),
                    observation_count INTEGER NOT NULL DEFAULT 0,
                    last_observed_at  INTEGER NOT NULL,
                    signal_summary_json TEXT NOT NULL DEFAULT '[]',
                    diagnosis_json    TEXT,
                    created_at        INTEGER NOT NULL,
                    updated_at        INTEGER NOT NULL
                );
                CREATE INDEX running_health_due_idx ON running_health (last_observed_at);
                CREATE INDEX running_health_campaign_state_idx
                    ON running_health (campaign_id, state);",
            )
            .map_err(database_error("create SQLite v22 running_health"))?;
    }
    transaction_execute_user_version(connection, 22)
}
```

(Reuse whatever private helper the file uses to stamp `PRAGMA user_version`; wire `migrate_to_v22` into the dispatch chain after v21.)

Add to `src/models.rs`:

```rust
database_enum!(HealthState {
    Healthy => "healthy",
    Suspicious => "suspicious",
    Diagnosing => "diagnosing",
    ActionPending => "action_pending",
});
```

- [ ] **Step 4: Run migration test — expect pass**

- [ ] **Step 5: Write repository tests**

```rust
#[test]
fn health_repository_lifecycle_is_bounded_and_resumable() {
    // harness: create project+campaign+experiment rows via existing helpers
    // ensure_running twice -> still one row, state healthy
    // record_observation with class oom -> summary len 1, count 1
    // record 32 more -> summary capped at 32, count grows
    // set_state Suspicious -> get reflects it
    // store_diagnosis -> diagnosis_json parses
    // delete_for_experiment -> gone
}
```

(Full assertions mirror `decisions` repo tests; use `serde_json::json!`.)

- [ ] **Step 6: Create `src/db/running_health.rs`** implementing the interface above against `running_health` table; register module in `src/db/mod.rs`.

- [ ] **Step 7: Full database suite**

`cargo test --test database -- --test-threads=1` → all green.

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "feat: add running health schema v22 and repository"
```

---

### Task 2: Signal classifier

**Files:**
- Create: `src/signals.rs`
- Modify: `src/lib.rs` (module), `src/config.rs` (`PatternConfig.class`), `src/detect.rs` (emit signals), `templates/config.toml` (class hints)
- Test: `tests/integration/detection.rs` (append), unit tests in `signals.rs`

**Interfaces:**
- Produces:
  - `pueue_agent::signals::SignalClass` enum: `Oom | Numerical | WorkerLoss | Staleness | Exception | Configured(String)`
  - `SignalObservation { class: SignalClass, source: SignalSource, evidence_digest: String, observed_at: i64 }`, `SignalSource ∈ {BuiltinProbe, ConfigPattern, Stall}`
  - `classify_log_tail(tail:&str) -> Vec<SignalClass>` (built-in probe rules)
  - detector: `Detector::signal_observations_for(task, tail) -> Vec<SignalObservation>` merging built-in probes, staleness, and configured-pattern classes (`PatternConfig.class: Option<String>`)
- Consumes: existing detector walk (`inspect_task_at`) and `PatternConfig`.

- [ ] **Step 1: Failing classifier unit tests (in `signals.rs` `#[cfg(test)]`)**

```rust
#[test]
fn builtin_probes_map_canonical_markers() {
    assert!(classify_log_tail("torch.cuda.OutOfMemoryError: CUDA out of memory")
        .contains(&SignalClass::Oom));
    assert!(classify_log_tail("epoch 3 loss=NaN").contains(&SignalClass::Numerical));
    assert!(classify_log_tail("RuntimeError: DataLoader worker (pid 12) is killed")
        .contains(&SignalClass::WorkerLoss));
    assert!(classify_log_tail("Traceback (most recent call last):")
        .contains(&SignalClass::Exception));
    assert!(classify_log_tail("all good here").is_empty());
}

#[test]
fn classification_is_case_insensitive_and_deduplicated() {
    let out = classify_log_tail("nan\nNAN\nCUDA OUT OF MEMORY");
    assert_eq!(
        out.iter().filter(|c| **c == SignalClass::Numerical).count(),
        1
    );
}
```

- [ ] **Step 2: RED run → implement `signals.rs`** with case-insensitive substring rules per class (rules as `const &[(&SignalClass, &[&str])]`), dedup via sort/dedup. Register module.

- [ ] **Step 3: Wire into detector**

In `detect.rs`, extend the per-task walk so each observation carries `Vec<SignalObservation>` derived from (a) built-in tail probes and (b) matched config patterns that declare `class`. Add `pub(crate) class: Option<String>` to `PatternConfig` (serde default None; validate value against `[a-z_]{1,32}` when present). Update `config.toml` template: give `numerical-instability` → `class="numerical"`, `cuda-error` → `class="oom"`.

- [ ] **Step 4: Integration test (detection.rs append)**

```rust
#[test]
fn pattern_hits_carry_declared_class_into_signal_observations() {
    // configure pattern name="fatal-loss" action=wake class="exception"
    // write matching log; run detection walk; assert emitted signal has
    // class exception and source config_pattern
}
```

- [ ] **Step 5: Green + full suite + commit**

```bash
git add -A && git commit -m "feat: classify running-experiment signals"
```

---

### Task 3: Observer session + health transitions

**Files:**
- Create: `src/health.rs`
- Modify: `src/daemon.rs` (call site after detection), `src/db/running_health.rs` (due query), lifecycle hooks in `src/reconcile.rs` (create/delete rows on running↔terminal transitions)
- Test: `tests/integration/health_observer.rs` (new file)

**Interfaces:**
- Produces:
  - `HealthEngine::run_once(db:&Db, projects:&[Project], pueue_snapshot:&[PueueTask], limits:&CampaignLimits, now:i64) -> Result<HealthReport, AppError>` where `HealthReport { observed:usize, escalated:usize, executed_actions:usize }`
  - Row creation hook: reconciler terminal-projection path calls `delete_for_experiment`; a running-task observation calls `ensure_running`.
- Behavior:
  - Due rows (`now >= last_observed_at + interval*60`) get one observation.
  - Healthy: record signal(s); suspicion rule (same non-staleness class twice consecutively OR staleness beyond stall threshold) → `set_state(Suspicious)` and spawn diagnosis next pass (Task 4 picks up `Suspicious` rows).
  - Paused/disabled/halted campaigns: observations deferred (no state change).

- [ ] **Step 1: Failing integration tests**

```rust
// health_observer.rs
#[tokio::test]
async fn observer_records_healthy_experiments_without_agents() {
    // campaign A with running baseline (fake pueue snapshot Running)
    // limits.observer_interval_minutes = 0-padded small? use last_observed_at backdated
    // run_once -> running_health row exists healthy, zero agent_runs spawned
}

#[tokio::test]
async fn repeated_same_class_signals_escalate_to_suspicious() {
    // two due passes with oom tail -> state suspicious
}

#[tokio::test]
async fn paused_project_defers_observations() {
    // paused project: due row untouched (last_observed_at unchanged)
}

#[tokio::test]
async fn terminal_projection_deletes_health_row() {
    // reconcile Done -> row deleted
}
```

- [ ] **Step 2: RED run.**

- [ ] **Step 3: Implement `HealthEngine`** (pure service logic; no agent spawns yet — `Suspicious` rows simply persist; Task 4 consumes them). Wire call into `daemon.rs` between detection and termination; add reconciler hooks (`ensure_running` when a campaign experiment's task is first seen running; `delete_for_experiment` inside `project_terminal_submission` success path).

- [ ] **Step 4: Green all four tests + full suite.**

- [ ] **Step 5: Commit** `feat: add resumable running-health observer sessions`

---

### Task 4: Diagnosis agent

**Files:**
- Modify: `src/models.rs` (`AgentRunRole::Diagnosis { experiment_id: String }`), `src/agent.rs` (spawn path + finalize persistence), `src/db/repositories.rs` (execution_kind accepts `"diagnosis"`), `src/decision_evidence.rs` or new `src/health_diagnosis.rs` (evidence bundle + output schema), `src/health.rs` (Suspicious rows spawn once), `tests/support/fake_codex.sh` (`diagnose` mode)
- Test: `tests/integration/health_diagnosis.rs` (new)

**Interfaces:**
- `AgentRunRole::Diagnosis { experiment_id: String }`
- Output schema constant `HEALTH_DIAGNOSIS_SCHEMA`: strict object `{root_cause_class:string, confidence:number 0..1, recommended_action:enum[continue|kill_and_resume|kill_and_escalate], summary:string≤512}`.
- `AgentHandle` finalize for `Diagnosis` persists validated JSON to `running_health.diagnosis_json` and sets state `ActionPending`; invalid output → bounded retry (reuse decision degradation counters pattern) then dead-letter + row back to `Suspicious`.
- Evidence bundle (bounded): experiment/campaign ids, objective digest, signal_summary, log tail excerpt ≤4KiB digest-only lines.

- [ ] **Step 1: Failing tests**

```rust
// health_diagnosis.rs
#[tokio::test]
async fn suspicious_row_spawns_one_diagnosis_agent_with_schema_argv() {
    // seed suspicious row; fake codex diagnose mode; run_once
    // assert: agent_run row exists execution_kind='diagnosis',
    //         codex argv contains '--output-schema' + '/dev/fd/11/health-diagnosis.json'
    //         state == diagnosing while running
}

#[tokio::test]
async fn valid_diagnosis_persists_and_moves_action_pending() {
    // fake codex writes {"root_cause_class":"oom","confidence":0.9,
    //   "recommended_action":"kill_and_resume","summary":"gpu exhausted"}
    // after completion pass: diagnosis_json stored, state action_pending
}

#[tokio::test]
async fn malformed_diagnosis_retries_then_dead_letters_row_to_suspicious() {
    // three malformed outputs -> attempts 3, state back to suspicious,
    // event/run dead-lettered with health_diagnosis_missing
}
```

Extend `fake_codex.sh`: after the decision branches add a `diagnose` mode branch triggered by argv containing `--output-last-message` AND env `PUEUE_AGENT_TEST_DIAGNOSE_MODE=1` writing either valid or malformed JSON per `$PUEUE_AGENT_TEST_DIAGNOSE_MODE`.

- [ ] **Step 2: RED run.**

- [ ] **Step 3: Implement.** Key pieces mirroring the decision flow:
  - `spawn_with_role` gains `AgentRunRole::Diagnosis { .. }` arm: preflight via same `CodexArgvBuilder::preflight_decision`, evidence bundle replaces campaign context, `execution_kind` string `"diagnosis"` accepted by `NewAgentRun::with_execution` validation.
  - Finalize: parse output with `HEALTH_DIAGNOSIS_SCHEMA`; on Ok → `store_diagnosis` + `set_state(ActionPending)`; on parse Err → `fail_attempt`-style bounded counter on the health row (reuse `consecutive_failed_attempts` column semantics by storing attempt count inside `diagnosis_json` wrapper `{attempt, ...}`) and revert to `Suspicious` after limit 3.
  - Exhaustive-match fixes across role matches (enumerate compiler errors; each arm mirrors the Decision arm but targets running-health instead of decision cycles).

- [ ] **Step 4: Green three tests + whole suite.**

- [ ] **Step 5: Commit** `feat: add bounded health diagnosis agent`

---

### Task 5: Confirmed action execution

**Files:**
- Modify: `src/health.rs` (action dispatcher), `src/execution_policy.rs` (`CampaignLimits.max_live_repairs: u32` + Raw field + range 0..=8 + default 2), `src/db/campaigns.rs` (resume-resubmit insert helper reuse), `src/db/repositories.rs` (checkpoint metadata columns via ALTER in v22 migration — add `resume_of_experiment_id TEXT` + `checkpoint_note TEXT` to experiments within the same v22 batch), `src/termination.rs` (expose `Confirmed` gate check helper `require_confirmed(project_id, pueue_task_id)`)
- Test: `tests/integration/health_actions.rs` (new)

**Interfaces:**
- `HealthEngine::execute_pending(db, pueue, projects, limits, now) -> usize` — for `ActionPending` rows with stored diagnosis:
  - `continue` → `reset_to_healthy`
  - `kill_and_resume` → require Confirmed; dispatch kill; on Killed projection insert successor experiment (same argv, `parent_experiment_id=self`, `resume_of=self`, checkpoint note) via coordinator budget path; increment live-repair counter (derived: `COUNT(*) FROM experiments WHERE resume_of_experiment_id = self`); ≥ max → escalate instead.
  - `kill_and_escalate` → require Confirmed; kill; on terminal projection mark campaign `degraded` + enqueue operator wake event.
- Spec addition inside Task-5 scope: extend the Task-1 migration block to also `ALTER TABLE experiments ADD COLUMN resume_of_experiment_id TEXT REFERENCES experiments(experiment_id)` and `ADD COLUMN checkpoint_note TEXT` (v22 ships both changes atomically).

- [ ] **Step 1: Failing tests**

```rust
// continue resets to healthy without any kill
// kill_and_resume WITHOUT confirmed request -> refused (row stays action_pending, error logged, no kill dispatched)
// kill_and_resume happy path: request confirmed manually via TerminationManager; kill dispatched exactly once;
//   after killed projection a NEW experiment exists with resume_of set + identical argv; live repairs counted =1
// exhausting max_live_repairs (=0) turns kill_and_resume into escalation: campaign degraded + wake event, no resubmit
```

- [ ] **Step 2: RED → implement → GREEN** (mirror auto-kill confirmation fixtures from termination.rs tests for the Confirmed gate).

- [ ] **Step 3: Suite green + commit** `feat: execute finite confirmed health actions`

---

### Task 6: CLI surface + E2E extensions

**Files:**
- Modify: `src/status.rs` (health section), `src/diagnostics.rs` (running_health listing), `tests/e2e/rust_supervisor.sh` (two scenarios), `docs/commands-ja.md` + `docs/getting-started-ja.md` (short sections)
- Test: bats untouched (existing suites must stay green); E2E validated on Linux gate

**Steps:**

- [ ] **Step 1:** status render adds, for each running experiment of active campaigns:
  `health: <state> signals=<top-class>x<count> age=<s> action=<last recommended>` (bounded line, redaction rules unchanged). Unit-test via existing status_text_output stable-projection test pattern (extend golden text).

- [ ] **Step 2:** diagnostics subcommand lists rows ordered by updated_at desc, capped 50.

- [ ] **Step 3: E2E scenario additions** (inside existing PROJECT_A fatal section neighborhood):
  1. After auto-kill asserts: craft OOM scenario on a fresh campaign project (submit `/bin/sleep 20`, write `torch.cuda.OutOfMemoryError` into its log, confirm observer escalates → diagnosis (fake codex diagnose mode) → recommended `kill_and_resume` → Confirmed gate → exactly one kill → successor experiment with resume metadata → completed).
  2. Restart-while-diagnosing: stop_daemon during `diagnosing`, start again, assert exactly one diagnosis agent total (recovery dedupe) and eventual action.

- [ ] **Step 4:** `bash -n` all scripts; local suites green; docs commits separate (`docs: document running health surface`).

- [ ] **Step 5: Commit** `test: exercise running health acceptance end to end`

---

## Final Verification (Linux gate, exact merged tree on roko)

```bash
cargo test --all-targets -- --test-threads=1
cargo check --all-targets
cargo check --release --all-targets
bats tests/test_shell_entrypoints.bats
tests/e2e/run.sh
```

Record versions/counts/warnings into the SDD ledger; then (with user approval) ff-merge `codex/phase3-running-health` into `main` and push.
