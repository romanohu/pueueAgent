# Zero-adapter Autonomous Completion Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one managed baseline submit autonomously produce the next safe non-code experiment after terminal success or failure, while preserving finite waits, restart safety, and Phase 1 submission invariants.

**Architecture:** A schema-v18 decision-cycle domain owns one durable cycle per terminal experiment and one active attempt per campaign. A Linux-only, built-in Codex decision runner receives a bounded SQLite-backed evidence bundle in a forced read-only sandbox, returns one schema-validated proposal or wait, and delegates every external submission to the existing `CampaignCoordinator`.

**Tech Stack:** Rust 2024, Tokio, rusqlite/SQLite, serde/serde_json, SHA-256, existing native agent launcher and `PrivateRunTemp`, Pueue 4, Bats, Linux real-Pueue E2E on `roko`.

## Global Constraints

- Preserve Phase 1's project admission lock, immutable objective, rolling budget, `reserved/submitting/unreconciled`, and descriptor-relative private-temp boundaries.
- Phase 2 decisions use pinned built-in Codex only; custom executables remain experiment-only.
- The project root is read-only; the verified private run temp is the only writable decision-run capability.
- Network follows service policy and is enabled by default; credentials and authentication environment variables remain excluded.
- One campaign has at most one active decision agent; one terminal experiment has at most one decision cycle.
- `proposal` and finite `wait` are the only accepted decision variants. `code_change` is rejected.
- `repair` requires the source experiment's trusted failure fingerprint.
- Complete context and decision payloads are at most 128 KiB and reject unknown fields and control characters.
- `max_decision_attempts_per_cycle` defaults to 3 and ranges from 1 through 10.
- `max_decision_wait_minutes` defaults to 1,440 and ranges from 1 through 10,080.
- A valid proposal or wait resets consecutive failed attempts; every analysis run consumes the existing hourly agent-run budget.
- Running health, periodic observer, automatic cancellation, goal review, and code worktrees are outside this plan.
- Linux real-Pueue acceptance must run on `roko`; macOS cannot satisfy the Phase 2 runtime gate.
- Do not add a second Pueue submission implementation or perform filesystem/Pueue/process work inside SQLite writer transactions.

---

## File and Responsibility Map

- Create `src/decision_protocol.rs`: strict decision JSON schema, validation, canonical digest, and bounded constants.
- Create `src/db/decisions.rs`: decision-cycle and attempt repositories, state transitions, recovery queries, and doctor projection.
- Create `src/decision_evidence.rs`: bounded deterministic context construction and project-relative artifact hints.
- Create `src/decision.rs`: ready-decision application, proposal/wait handling, and coordinator integration.
- Modify `src/models.rs`: decision states/models and the `campaign_decision` event kind.
- Modify `src/db/migrations.rs`: schema 18 DDL, verification, and 17-to-18 migration.
- Modify `src/execution_policy.rs`: service-owned decision limits and read-only/output capability requirements.
- Modify `src/codex_command.rs`: forced decision-specific Codex argv.
- Modify `src/environment.rs`: descriptor-relative decision schema/output preparation and validated readback.
- Modify `src/agent.rs`: decision launch role and pre-cleanup output persistence.
- Modify `src/reconcile.rs`: idempotent cycle creation after terminal experiment projection.
- Modify `src/scheduler.rs`: deterministic decision event admission and decision-agent dispatch.
- Modify `src/daemon.rs`: recovery and ready-decision processing order.
- Modify `src/diagnostics.rs`, `src/status.rs`, and `src/output.rs`: bounded decision projections.
- Modify `README.md`, `docs/architecture-ja.md`, `docs/getting-started-ja.md`, `docs/workflows-ja.md`, `docs/troubleshooting-ja.md`, and `templates/instructions.md`: truthful Phase 2 operation and remaining Phase 3 boundaries.
- Modify existing integration suites rather than adding duplicate harnesses.

---

### Task 1: Decision Protocol and Service-owned Limits

**Files:**
- Create: `src/decision_protocol.rs`
- Modify: `src/lib.rs`
- Modify: `src/execution_policy.rs`
- Test: `src/decision_protocol.rs`
- Test: `tests/integration/execution_policy.rs`

**Interfaces:**
- Produces: `DecisionInput`, `ValidatedDecision`, `ValidatedWait`, `parse_and_validate_decision(bytes, objective_digest, limits)`.
- Produces: `CampaignLimits::{max_decision_attempts_per_cycle,max_decision_wait_minutes}`.
- Consumes: existing `ProposalInput`, `ValidatedProposal`, and `proposals::validate`.

- [ ] **Step 1: Write protocol and policy RED tests**

Add tests that require the missing API before implementation:

```rust
#[test]
fn decision_protocol_accepts_one_proposal_or_finite_wait() {
    let limits = CampaignLimits::default();
    let proposal = br#"{"schema_version":1,"decision":"proposal","proposal":{"kind":"experiment","hypothesis":"lower lr","source_experiment_id":"exp-1","argv":["python","train.py","--lr","0.001"],"working_directory":".","expected_evidence":["validation loss"]}}"#;
    assert!(matches!(
        parse_and_validate_decision(proposal, "objective-digest", limits).unwrap(),
        ValidatedDecision::Proposal(_)
    ));

    let wait = br#"{"schema_version":1,"decision":"wait","reason":"artifact pending","requested_wait_minutes":30,"expected_evidence":["checkpoint"]}"#;
    assert!(matches!(
        parse_and_validate_decision(wait, "objective-digest", limits).unwrap(),
        ValidatedDecision::Wait(ValidatedWait { requested_wait_minutes: 30, .. })
    ));
}

#[test]
fn decision_protocol_rejects_code_change_unknown_fields_and_unbounded_wait() {
    let limits = CampaignLimits::default();
    let rejected: [&[u8]; 4] = [
        br#"{"schema_version":1,"decision":"proposal","proposal":{"kind":"code_change","hypothesis":"edit source","source_experiment_id":"exp-1","argv":["python","train.py"],"working_directory":".","expected_evidence":[]}}"#,
        br#"{"schema_version":1,"decision":"wait","reason":"later","requested_wait_minutes":30,"expected_evidence":[],"extra":true}"#,
        br#"{"schema_version":1,"decision":"wait","reason":"later","requested_wait_minutes":0,"expected_evidence":[]}"#,
        br#"{"schema_version":1,"decision":"wait","reason":"later","requested_wait_minutes":10081,"expected_evidence":[]}"#,
    ];
    for document in rejected {
        assert!(parse_and_validate_decision(document, "objective-digest", limits).is_err());
    }
}
```

In `tests/integration/execution_policy.rs`, extend the table-driven policy cases to assert defaults
`3` and `1_440`, reject attempts `0`/`11`, and reject wait limits `0`/`10_081`.

- [ ] **Step 2: Run the RED tests**

Run:

```bash
cargo test --lib decision_protocol::tests -- --nocapture --test-threads=1
cargo test --test execution_policy decision_ -- --nocapture --test-threads=1
```

Expected: compile failure for missing decision types and missing `CampaignLimits` fields.

- [ ] **Step 3: Implement the strict protocol types**

Create the tagged schema and validate before exposing a decision:

```rust
pub const MAX_DECISION_BYTES: usize = 128 * 1024;
pub const MAX_WAIT_REASON_BYTES: usize = 4 * 1024;
pub const MAX_WAIT_EVIDENCE_ITEMS: usize = 16;
pub const MAX_WAIT_EVIDENCE_BYTES: usize = 512;

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionInput {
    Proposal { schema_version: u8, proposal: ProposalInput },
    Wait {
        schema_version: u8,
        reason: String,
        requested_wait_minutes: u32,
        expected_evidence: Vec<String>,
    },
}

#[derive(Debug)]
pub enum ValidatedDecision {
    Proposal(ValidatedProposal),
    Wait(ValidatedWait),
}
```

`parse_and_validate_decision` must reject empty/oversize input before serde, require schema version 1,
call `proposals::validate`, reject `ProposalKind::CodeChange`, bound every wait string, validate the
requested wait against `max_decision_wait_minutes`, canonicalize the accepted form, and compute a
SHA-256 digest.

- [ ] **Step 4: Extend policy parsing and fixtures**

Add both raw optional TOML fields, defaults, range checks, equality/debug coverage, and default policy
rendering. Preserve v1 compatibility by applying defaults when the fields are absent.

- [ ] **Step 5: Run focused GREEN tests and compile**

Run:

```bash
cargo test --lib decision_protocol::tests -- --nocapture --test-threads=1
cargo test --test execution_policy decision_ -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 6: Commit Task 1**

```bash
git add src/decision_protocol.rs src/lib.rs src/execution_policy.rs tests/integration/execution_policy.rs
git commit -m "feat: validate autonomous campaign decisions"
```

---

### Task 2: Schema 18 Decision Domain and Atomic Repository

**Files:**
- Create: `src/db/decisions.rs`
- Modify: `src/db/mod.rs`
- Modify: `src/db/migrations.rs`
- Modify: `src/models.rs`
- Test: `tests/integration/database.rs`

**Interfaces:**
- Consumes: `Campaign`, `Experiment`, `CampaignState`, `AgentRun`, and `Db`.
- Produces: `DecisionRepository`, `DecisionCycle`, `DecisionAttempt`, `DecisionReservation`, `DecisionRecovery`, and `DecisionDoctorProjection`.
- Produces repository methods used by Tasks 3, 5, 6, and 7.

- [ ] **Step 1: Write migration and repository RED tests**

Add exact tests:

```rust
#[test]
fn v17_migrates_decision_cycles_and_attempts_atomically() {
    let (_temp, path) = canonical_v17_campaign_fixture();
    let db = Db::open(&path).unwrap();
    let connection = db.connect().unwrap();
    assert_eq!(connection.pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0)).unwrap(), 18);
    assert_eq!(
        table_columns(&connection, "decision_cycles").iter().map(|column| column.0.as_str()).collect::<Vec<_>>(),
        vec!["cycle_id", "campaign_id", "source_experiment_id", "state", "next_wake_at", "consecutive_failed_attempts", "last_decision_kind", "last_failure_code", "last_failure_summary", "created_at", "updated_at"]
    );
    assert_eq!(table_foreign_keys(&connection, "decision_attempts").len(), 2);
}

#[test]
fn terminal_experiment_creates_one_cycle_and_one_active_attempt() {
    let harness = CampaignDbHarness::with_terminal_experiment(ExperimentStatus::Succeeded);
    let first = Db::open(harness.db.path()).unwrap();
    let second = Db::open(harness.db.path()).unwrap();
    let cycle = DecisionRepository::new(&first)
        .ensure_cycle_for_terminal(&harness.campaign_id, &harness.experiment_id, 200)
        .unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let results = std::thread::scope(|scope| {
        [&first, &second].into_iter().map(|db| {
            let barrier = std::sync::Arc::clone(&barrier);
            let project_id = harness.project_id.clone();
            let cycle_id = cycle.cycle_id.clone();
            scope.spawn(move || {
                barrier.wait();
                DecisionRepository::new(db).reserve_next_attempt(&project_id, &cycle_id, 201).unwrap()
            })
        }).collect::<Vec<_>>().into_iter().map(|thread| thread.join().unwrap()).collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|result| result.is_some()).count(), 1);
    assert_eq!(harness.scalar("SELECT COUNT(*) FROM decision_attempts"), 1);
}

#[test]
fn cross_campaign_or_nonterminal_decision_lineage_rolls_back() {
    let harness = CampaignDbHarness::with_experiment(ExperimentStatus::Accepted);
    let error = DecisionRepository::new(&harness.db)
        .ensure_cycle_for_terminal(&harness.campaign_id, &harness.experiment_id, 200)
        .unwrap_err();
    assert!(matches!(error, AppError::Validation { field: "source_experiment_id", .. }));
    assert_eq!(harness.scalar("SELECT COUNT(*) FROM decision_cycles"), 0);
}

#[test]
fn valid_wait_resets_failed_attempts_and_requires_finite_wake() {
    let harness = CampaignDbHarness::with_terminal_experiment(ExperimentStatus::Failed);
    let (cycle, attempt) = harness.reserved_decision_attempt();
    let waiting = DecisionRepository::new(&harness.db)
        .mark_waiting(&cycle.cycle_id, attempt.attempt_number, 260, 200)
        .unwrap();
    assert_eq!(waiting.state, DecisionCycleState::Waiting);
    assert_eq!(waiting.next_wake_at, Some(260));
    assert_eq!(waiting.consecutive_failed_attempts, 0);
}
```

Add `canonical_v17_campaign_fixture`, `CampaignDbHarness::with_experiment`,
`CampaignDbHarness::with_terminal_experiment`, `CampaignDbHarness::reserved_decision_attempt`, and
`CampaignDbHarness::scalar` as test-only helpers in `tests/integration/database.rs`; each helper must
insert canonical linked project/campaign/proposal/submission/experiment rows through repository APIs
except the version-17 fixture, which must use independent canonical v17 DDL.

The race test must use two independently opened `Db` values and a barrier so exactly one reservation
succeeds and no duplicate attempt row exists.

- [ ] **Step 2: Run database RED tests**

Run:

```bash
cargo test --test database decision_schema -- --nocapture --test-threads=1
cargo test --test database decision_cycle -- --nocapture --test-threads=1
```

Expected: failures for schema version 17 and missing repository APIs.

- [ ] **Step 3: Add model enums and structs**

Add database enums with exact persisted values:

```rust
database_enum!(DecisionCycleState {
    Pending => "pending",
    Analyzing => "analyzing",
    Waiting => "waiting",
    Completed => "completed",
    Degraded => "degraded",
});

database_enum!(DecisionAttemptState {
    Reserved => "reserved",
    EvidenceReady => "evidence_ready",
    Running => "running",
    Decided => "decided",
    Failed => "failed",
});
```

Add `EventKind::CampaignDecision => "campaign_decision"`. Define row models without raw prompt or
environment fields.

- [ ] **Step 4: Add schema 18 DDL and verification**

Create tables with these keys and constraints:

```sql
CREATE TABLE decision_cycles (
  cycle_id TEXT PRIMARY KEY,
  campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id),
  source_experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
  state TEXT NOT NULL CHECK (state IN ('pending','analyzing','waiting','completed','degraded')),
  next_wake_at INTEGER,
  consecutive_failed_attempts INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failed_attempts >= 0),
  last_decision_kind TEXT,
  last_failure_code TEXT,
  last_failure_summary TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(campaign_id, source_experiment_id)
);

CREATE TABLE decision_attempts (
  cycle_id TEXT NOT NULL REFERENCES decision_cycles(cycle_id),
  attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
  state TEXT NOT NULL CHECK (state IN ('reserved','evidence_ready','running','decided','failed')),
  context_schema_version INTEGER,
  context_json TEXT,
  context_digest TEXT,
  agent_run_id INTEGER REFERENCES agent_runs(run_id),
  decision_json TEXT,
  decision_digest TEXT,
  decision_kind TEXT,
  failure_code TEXT,
  failure_summary TEXT,
  created_at INTEGER NOT NULL,
  started_at INTEGER,
  finished_at INTEGER,
  PRIMARY KEY(cycle_id, attempt_number),
  UNIQUE(agent_run_id)
);
```

Add indexes on `(state,next_wake_at,updated_at)`, `(campaign_id,state,updated_at)`, and
`decision_attempts(state,created_at)`. Verify canonical SQL, columns, indexes, and FKs on every v18
open; malformed current schemas fail without repair.

- [ ] **Step 5: Implement the atomic repository API**

Expose these exact methods:

```rust
impl<'a> DecisionRepository<'a> {
    pub fn ensure_cycle_for_terminal(&self, campaign_id: &str, experiment_id: &str, now: i64)
        -> Result<DecisionCycle, AppError>;
    pub fn reserve_next_attempt(&self, project_id: &str, cycle_id: &str, now: i64)
        -> Result<Option<DecisionReservation>, AppError>;
    pub fn store_evidence(&self, reservation: &DecisionReservation, context_json: &str,
        context_digest: &str, now: i64) -> Result<(), AppError>;
    pub fn bind_agent_run(&self, reservation: &DecisionReservation, run_id: i64, now: i64)
        -> Result<(), AppError>;
    pub fn store_decision(&self, run_id: i64, decision_json: &str, decision_digest: &str,
        decision_kind: &str, now: i64) -> Result<DecisionAttempt, AppError>;
    pub fn fail_attempt(&self, run_id: Option<i64>, cycle_id: &str, attempt_number: i64,
        code: &str, summary: &str, limits: CampaignLimits, now: i64)
        -> Result<DecisionCycle, AppError>;
    pub fn mark_waiting(&self, cycle_id: &str, attempt_number: i64, next_wake_at: i64,
        now: i64) -> Result<DecisionCycle, AppError>;
    pub fn mark_completed(&self, cycle_id: &str, attempt_number: i64, now: i64)
        -> Result<DecisionCycle, AppError>;
    pub fn due_cycles(&self, now: i64, limit: usize) -> Result<Vec<DecisionCycle>, AppError>;
    pub fn recoverable_attempts(&self, limit: usize) -> Result<Vec<DecisionRecovery>, AppError>;
}
```

Every mutation re-reads project/campaign/source lineage under `BEGIN IMMEDIATE`. Reservation requires
enabled, unpaused, unhalted project and active campaign. Waiting cycles become pending only when due.

- [ ] **Step 6: Run Task 2 GREEN gates**

```bash
cargo test --test database decision_schema -- --nocapture --test-threads=1
cargo test --test database decision_cycle -- --nocapture --test-threads=1
cargo test --test database migration_ -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 7: Commit Task 2**

```bash
git add src/db/decisions.rs src/db/mod.rs src/db/migrations.rs src/models.rs tests/integration/database.rs
git commit -m "feat: persist autonomous decision cycles"
```

---

### Task 3: Bounded Evidence Builder

**Files:**
- Create: `src/decision_evidence.rs`
- Modify: `src/lib.rs`
- Modify: `src/environment.rs`
- Test: `tests/integration/database.rs`
- Test: `tests/integration/codex_security.rs`

**Interfaces:**
- Consumes: `DecisionReservation`, campaign/project/experiment repositories, `ProjectRootAnchor`, and bounded Pueue task projection.
- Produces: `DecisionEvidenceBuilder::build(&DecisionEvidenceRequest) -> DecisionContextBundle`.
- Produces: `DecisionContextBundle::{json,digest}` for `DecisionRepository::store_evidence`.

- [ ] **Step 1: Write evidence RED tests**

Add tests proving deterministic output and bounded filesystem behavior:

```rust
#[test]
fn decision_context_is_deterministic_bounded_and_uses_persisted_objective() {
    let first = harness.build_decision_context("experiment-1");
    let second = harness.build_decision_context("experiment-1");
    assert_eq!(first.digest, second.digest);
    assert!(first.json.len() <= MAX_DECISION_CONTEXT_BYTES);
    assert!(first.json.contains("persisted objective"));
    assert!(!first.json.contains("edited STATE objective"));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn decision_artifact_hints_do_not_follow_symlinks_or_escape_the_pinned_root() {
    let harness = EvidenceHarness::new();
    std::fs::create_dir_all(harness.project_root.join("metrics")).unwrap();
    std::fs::write(harness.project_root.join("metrics/epoch.json"), "{\"loss\":1.0}\n").unwrap();
    std::fs::write(harness.external_root.join("secret.txt"), "EVIDENCE_SECRET").unwrap();
    std::os::unix::fs::symlink(
        harness.external_root.join("secret.txt"),
        harness.project_root.join("metrics/external"),
    ).unwrap();
    let bundle = harness.build();
    let value: serde_json::Value = serde_json::from_str(&bundle.json).unwrap();
    let hints = value["artifact_hints"].as_array().unwrap();
    assert!(hints.iter().any(|hint| hint["path"] == "metrics/epoch.json"));
    assert!(!hints.iter().any(|hint| hint["path"] == "metrics/external"));
    assert!(!bundle.json.contains("EVIDENCE_SECRET"));
}
```

Also assert credential-like environment values, full logs, and proposal raw metadata never appear.

- [ ] **Step 2: Run RED tests**

```bash
cargo test --test database decision_context -- --nocapture --test-threads=1
cargo test --test codex_security decision_artifact -- --nocapture --test-threads=1
```

Expected: compile failure for missing builder and artifact-hint API.

- [ ] **Step 3: Implement the serialized context schema**

Define a versioned `Serialize`-only context with typed sections for objective, source experiment,
terminal observation, recent outcomes, budgets, intervention, and artifact hints. Use constants:

```rust
pub const DECISION_CONTEXT_SCHEMA_VERSION: u8 = 1;
pub const MAX_DECISION_CONTEXT_BYTES: usize = 128 * 1024;
pub const MAX_ARTIFACT_HINTS: usize = 64;
pub const MAX_ARTIFACT_HINT_DEPTH: usize = 4;
pub const MAX_ARTIFACT_HINT_FIELD_BYTES: usize = 4 * 1024;
```

Sort recent records and artifact hints by stable keys before serialization. Serialize once, reject
oversize, and hash those exact bytes.

- [ ] **Step 4: Implement descriptor-relative artifact hints**

Extend the existing pinned-root descriptor traversal rather than using `walkdir`, `canonicalize`, or
ambient reopen. Inspect regular-file metadata only, refuse mount/root identity changes, skip symlinks
and special files, enforce depth/entry limits before allocation, and revalidate the root after the
scan. Record relative path, size, and mtime only; do not ingest file content in this task.

- [ ] **Step 5: Run Task 3 GREEN gates**

```bash
cargo test --test database decision_context -- --nocapture --test-threads=1
cargo test --test codex_security decision_artifact -- --nocapture --test-threads=1
cargo test --lib environment:: -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 6: Commit Task 3**

```bash
git add src/decision_evidence.rs src/lib.rs src/environment.rs tests/integration/database.rs tests/integration/codex_security.rs
git commit -m "feat: build bounded campaign decision evidence"
```

---

### Task 4: Linux Read-only Decision Runner and Secure Output Ingestion

**Files:**
- Modify: `src/codex_command.rs`
- Modify: `src/execution_policy.rs`
- Modify: `src/environment.rs`
- Modify: `src/agent.rs`
- Modify: `src/models.rs`
- Test: `tests/integration/codex_security.rs`
- Test: `tests/integration/native_agent_gate.rs`
- Test: `tests/integration/scheduler.rs`

**Interfaces:**
- Consumes: `DecisionReservation`, `DecisionContextBundle`, `DecisionRepository`, and pinned Codex/private-temp capabilities.
- Produces: `AgentRunRole::Decision { cycle_id, attempt_number }` and `AgentRunner::spawn_decision`.
- Produces: decision bytes persisted before `PrivateRunTemp` cleanup.

- [ ] **Step 1: Write runner security RED tests**

Add tests that require:

```rust
#[cfg(target_os = "linux")]
#[tokio::test]
async fn decision_runner_is_read_only_network_enabled_and_persists_output_before_cleanup() {
    let mut harness = DecisionRunnerHarness::new(OutputMutation::Valid);
    harness.run_to_terminal().await.unwrap();
    assert!(!harness.project_root.join("forbidden-write").exists());
    assert_eq!(harness.capture("network_access"), "true");
    assert!(!harness.captured_environment_names().iter().any(|name| {
        matches!(name.as_str(), "OPENAI_API_KEY" | "AWS_SECRET_ACCESS_KEY" | "SSH_AUTH_SOCK")
    }));
    assert_eq!(harness.stored_decision_kind(), Some("proposal"));
    assert_eq!(harness.cleanup_order(), vec!["decision_commit", "agent_terminal", "temp_cleanup"]);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn replaced_symlinked_or_weak_decision_output_is_rejected_without_project_mutation() {
    for mutation in [
        OutputMutation::Replaced,
        OutputMutation::Symlink,
        OutputMutation::Mode0644,
        OutputMutation::SecondHardLink,
        OutputMutation::Oversize,
    ] {
        let mut harness = DecisionRunnerHarness::new(mutation);
        harness.run_to_terminal().await.unwrap();
        assert_eq!(harness.stored_decision_kind(), None);
        assert_eq!(harness.attempt_failure_code(), Some("decision_missing"));
        assert!(!harness.project_root.join("forbidden-write").exists());
    }
}
```

Add `DecisionRunnerHarness` and `OutputMutation` to `tests/integration/codex_security.rs`. The generated
Codex fixture must expose only the named capture fields and must never capture environment values,
prompt text, or decision contents.

Add a macOS/non-Linux test that the decision capability preflight returns a typed unsupported policy
error before AgentRun allocation.

- [ ] **Step 2: Run RED tests**

```bash
cargo test --test codex_security decision_runner -- --nocapture --test-threads=1
cargo test --test native_agent_gate decision_output -- --nocapture --test-threads=1
```

Expected: failures for missing read-only/output capability and missing decision launch role.

- [ ] **Step 3: Extend Codex capability detection and argv**

Add positive capability bits for read-only sandbox, JSON output schema, and output-last-message. Add a
decision builder that forces the pinned Codex executable, ignores user config/rules, uses approval
never, uses the resolved network boolean, selects the read-only sandbox, and supplies supervisor-
owned schema/output paths under verified private temp.

```rust
pub(crate) fn build_decision_with_private_temp(
    &self,
    config: &AgentConfig,
    prompt: &str,
    private_tmp: &VerifiedPrivateTemp,
) -> Result<Vec<OsString>, PolicyViolation>;
```

Do not use the project-configured custom executable for this method. Missing capability is a
pre-binding policy violation.

- [ ] **Step 4: Add secure schema/output file operations**

Add Phase 2-specific `PrivateRunTemp` methods:

```rust
pub(crate) fn prepare_decision_schema(&self, schema: &[u8]) -> Result<(), PolicyViolation>;
pub(crate) fn read_decision_output(&self) -> Result<Vec<u8>, PolicyViolation>;
```

Create the schema descriptor-relative with `0600`, fsync file and parent, and revalidate the temp
identity. Read exactly `decision.json` with no-follow, owner/euid, regular-file, mode `0600`, link
count one, same mount, and size cap checks before allocation. Recheck identity immediately after read.

- [ ] **Step 5: Persist output before terminal cleanup**

Add:

```rust
pub(crate) enum AgentRunRole {
    Standard,
    Decision { cycle_id: String, attempt_number: i64 },
}
```

Factor common spawn logic into a private role-aware method and expose `spawn_decision`. In
`AgentHandle` terminal finalization, after checked process-group reap and before AgentRun terminal
persistence/private-temp cleanup, read and validate the decision output and call
`DecisionRepository::store_decision`. On read/parse failure call `fail_attempt`, then persist the
AgentRun outcome; do not lose the retained temp capability until both DB operations finish.

- [ ] **Step 6: Run Task 4 GREEN gates**

```bash
cargo test --test codex_security decision_runner -- --nocapture --test-threads=1
cargo test --test native_agent_gate decision_output -- --nocapture --test-threads=1
cargo test --test scheduler decision_agent -- --nocapture --test-threads=1
cargo check --all-targets
cargo check --release --all-targets
git diff --check
```

Run the Linux-only focused tests on `roko` if the local host is not Linux.

- [ ] **Step 7: Commit Task 4**

```bash
git add src/codex_command.rs src/execution_policy.rs src/environment.rs src/agent.rs src/models.rs tests/integration/codex_security.rs tests/integration/native_agent_gate.rs tests/integration/scheduler.rs
git commit -m "feat: run read-only campaign decision agents"
```

---

### Task 5: Terminal Cycle Creation and Scheduler Dispatch

**Files:**
- Modify: `src/reconcile.rs`
- Modify: `src/scheduler.rs`
- Modify: `src/agent.rs`
- Modify: `src/db/decisions.rs`
- Test: `tests/integration/reconciliation.rs`
- Test: `tests/integration/scheduler.rs`

**Interfaces:**
- Consumes: Task 2 repository, Task 3 evidence builder, and Task 4 decision runner.
- Produces: idempotent terminal-cycle events and deterministic scheduler admission.

- [ ] **Step 1: Write terminal and scheduler RED tests**

Add exact scenarios:

```rust
#[test]
fn terminal_success_and_failure_each_create_one_decision_cycle_event() {
    for terminal in ["Done", "Failed"] {
        let harness = Harness::new();
        let experiment_id = harness.accepted_campaign_experiment_at(41, "2026-08-21T00:00:00Z");
        harness.reconcile_task(terminal_task(41, "2026-08-21T00:00:00Z", serde_json::json!(terminal)));
        harness.reconcile_task(terminal_task(41, "2026-08-21T00:00:00Z", serde_json::json!(terminal)));
        assert_eq!(harness.decision_cycle_count(&experiment_id), 1);
        assert_eq!(harness.decision_event_count(&experiment_id), 1);
    }
}

#[tokio::test]
async fn scheduler_runs_only_the_oldest_campaign_decision_and_binds_one_attempt() {
    let mut harness = SchedulerHarness::with_two_terminal_campaign_experiments();
    let report = harness.scheduler.tick().await.unwrap();
    assert_eq!(report.started.len(), 1);
    assert_eq!(harness.running_decision_attempts(), 1);
    assert_eq!(harness.pending_decision_cycles(), 1);
    assert_eq!(harness.started_source_experiment_id(), harness.oldest_experiment_id());
}

#[tokio::test]
async fn paused_disabled_retired_or_budget_waiting_campaign_never_starts_a_decision_agent() {
    for state in [CampaignState::Paused, CampaignState::Retired, CampaignState::BudgetWaiting] {
        let mut harness = SchedulerHarness::with_due_decision(state);
        let report = harness.scheduler.tick().await.unwrap();
        assert!(report.started.is_empty());
        assert_eq!(harness.agent_run_count(), 0);
    }
    let mut disabled = SchedulerHarness::with_due_decision(CampaignState::Active);
    disabled.disable_project();
    assert!(disabled.scheduler.tick().await.unwrap().started.is_empty());
    assert_eq!(disabled.agent_run_count(), 0);
}
```

Extend the existing reconciliation and scheduler harnesses with the named helpers in the snippet.
`reconcile_task` must invoke the production `Reconciler`; count helpers must query only linked
campaign/experiment rows, and scheduler helpers must construct events through `EventRepository`.

Assert legacy unlineaged terminal events and `unreconciled` experiments do not create cycles.

- [ ] **Step 2: Run RED tests**

```bash
cargo test --test reconciliation decision_cycle -- --nocapture --test-threads=1
cargo test --test scheduler campaign_decision -- --nocapture --test-threads=1
```

- [ ] **Step 3: Create cycles during terminal projection**

After `ExperimentRepository` commits a uniquely correlated terminal state, call
`DecisionRepository::ensure_cycle_for_terminal` and insert one idempotent `campaign_decision` event
with exact campaign/experiment lineage. Never create a cycle from provisional task identity,
ambiguous reconciliation, or a terminal observation that lost project/campaign lineage.

- [ ] **Step 4: Dispatch decision events in deterministic order**

Add `campaign_decision` priority ahead of generic idle/deep-check work but after termination safety
work. Under global then project admission locks:

1. reload project and live campaign;
2. reserve the oldest pending/due cycle;
3. build and persist evidence;
4. reserve the existing hourly agent-run budget with an idempotency key derived from cycle/attempt;
5. call `spawn_decision`;
6. bind the AgentRun before releasing admission.

If lock, budget, pause, or authority changes, defer the claimed event without an AgentRun. A waiting
cycle is not due until its absolute wake.

- [ ] **Step 5: Run Task 5 GREEN gates**

```bash
cargo test --test reconciliation decision_cycle -- --nocapture --test-threads=1
cargo test --test scheduler campaign_decision -- --nocapture --test-threads=1
cargo test --test scheduler campaign_agent_budget -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 6: Commit Task 5**

```bash
git add src/reconcile.rs src/scheduler.rs src/agent.rs src/db/decisions.rs tests/integration/reconciliation.rs tests/integration/scheduler.rs
git commit -m "feat: schedule terminal campaign decisions"
```

---

### Task 6: Apply Proposal/Wait Decisions and Recover Daemon State

**Files:**
- Create: `src/decision.rs`
- Modify: `src/lib.rs`
- Modify: `src/daemon.rs`
- Modify: `src/campaign.rs`
- Modify: `src/db/decisions.rs`
- Test: `tests/integration/daemon.rs`
- Test: `tests/integration/pueue_adapter.rs`

**Interfaces:**
- Consumes: persisted `DecisionAttempt`, `ValidatedDecision`, `CampaignCoordinator`, and existing Pueue API.
- Produces: `DecisionCoordinator::apply_ready(limit, now) -> DecisionLoopReport`.
- Produces: daemon startup and per-tick decision recovery.

- [ ] **Step 1: Write proposal, failure, wait, and crash-window RED tests**

Add tests:

```rust
#[tokio::test]
async fn successful_terminal_decision_adds_exactly_one_next_experiment() {
    let harness = DecisionHarness::with_ready_proposal(ExperimentStatus::Succeeded);
    let first = harness.coordinator().apply_ready(300, 10).await.unwrap();
    let second = harness.coordinator().apply_ready(301, 10).await.unwrap();
    assert_eq!(first.proposals_applied, 1);
    assert_eq!(second.proposals_applied, 0);
    assert_eq!(harness.pueue.add_calls(), 1);
    assert_eq!(harness.child_experiment_count(), 1);
}

#[tokio::test]
async fn failed_terminal_allows_trusted_repair_and_rejects_untrusted_repair() {
    let trusted = DecisionHarness::with_ready_repair(Some("trusted-fingerprint"));
    assert_eq!(trusted.coordinator().apply_ready(300, 10).await.unwrap().proposals_applied, 1);
    assert_eq!(trusted.pueue.add_calls(), 1);

    let untrusted = DecisionHarness::with_ready_repair(None);
    assert!(matches!(
        untrusted.coordinator().apply_ready(300, 10).await.unwrap_err(),
        AppError::Validation { field: "source_experiment_id", .. }
    ));
    assert_eq!(untrusted.pueue.add_calls(), 0);
}

#[tokio::test]
async fn wait_decision_adds_no_task_and_wakes_same_cycle_at_the_finite_deadline() {
    let harness = DecisionHarness::with_ready_wait(1);
    assert_eq!(harness.coordinator().apply_ready(300, 10).await.unwrap().waits_scheduled, 1);
    assert_eq!(harness.pueue.add_calls(), 0);
    assert!(DecisionRepository::new(&harness.db).due_cycles(359, 10).unwrap().is_empty());
    assert_eq!(DecisionRepository::new(&harness.db).due_cycles(360, 10).unwrap().len(), 1);
}

#[tokio::test]
async fn restart_after_decision_or_pueue_add_never_duplicates_the_external_add() {
    for point in [DecisionFailpoint::AfterDecisionCommit, DecisionFailpoint::AfterPueueAdd] {
        let harness = DecisionHarness::with_ready_proposal(ExperimentStatus::Succeeded);
        harness.enable_failpoint(point);
        assert!(harness.coordinator().apply_ready(300, 10).await.is_err());
        harness.disable_failpoints();
        harness.restart_daemon_once().await.unwrap();
        assert_eq!(harness.pueue.add_calls(), 1);
        assert_eq!(harness.child_experiment_count(), 1);
    }
}
```

Add `DecisionHarness` and `DecisionFailpoint` to `tests/integration/pueue_adapter.rs`. The harness must
use the production repositories and coordinator, while its fake Pueue records add calls and can fail
only at the two named boundaries.

Also assert code-change output is rejected, duplicate proposal digest does not add, and three
consecutive invalid attempts move the campaign to degraded.

- [ ] **Step 2: Run RED tests**

```bash
cargo test --test pueue_adapter decision_ -- --nocapture --test-threads=1
cargo test --test daemon decision_recovery -- --nocapture --test-threads=1
```

- [ ] **Step 3: Implement `DecisionCoordinator`**

Expose:

```rust
pub struct DecisionLoopReport {
    pub proposals_applied: usize,
    pub waits_scheduled: usize,
    pub deferred: usize,
    pub degraded: usize,
}

impl<'a, P: PueueApi + ?Sized> DecisionCoordinator<'a, P> {
    pub async fn apply_ready(&self, now: i64, limit: usize)
        -> Result<DecisionLoopReport, AppError>;
}
```

For proposal decisions, reload durable context, parse the stored JSON again, require the exact source
experiment and objective digest, generate supervisor IDs, acquire the project admission lock, and
call the existing `CampaignCoordinator`. Mark the cycle complete only after the coordinator has a
durable accepted intent. Preserve original Pueue errors after storing `unreconciled` state.

For waits, compute `next_wake_at = now + requested_wait_minutes * 60` with checked arithmetic, cap it
by service policy, persist waiting atomically, and create no submission or reservation.

- [ ] **Step 4: Integrate daemon ordering and startup recovery**

Use this per-tick order:

1. existing AgentRun and submission-intent recovery;
2. decision attempt recovery;
3. existing Pueue reconciliation and terminal cycle creation;
4. poll retained agent/cleanup ownership;
5. apply persisted ready decisions;
6. wake due decision/budget cycles;
7. scheduler tick;
8. post-scheduler ownership poll.

Startup must never restart a linked running agent until existing ownership recovery resolves. A
terminal AgentRun without provable decision output becomes `decision_missing`; it never implies a
proposal.

- [ ] **Step 5: Run Task 6 GREEN gates**

```bash
cargo test --test pueue_adapter decision_ -- --nocapture --test-threads=1
cargo test --test daemon decision_recovery -- --nocapture --test-threads=1
cargo test --test daemon campaign_ -- --nocapture --test-threads=1
cargo test --test reconciliation campaign_ -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 6: Commit Task 6**

```bash
git add src/decision.rs src/lib.rs src/daemon.rs src/campaign.rs src/db/decisions.rs tests/integration/daemon.rs tests/integration/pueue_adapter.rs
git commit -m "feat: apply and recover campaign decisions"
```

---

### Task 7: Status, Doctor, Templates, and User Documentation

**Files:**
- Modify: `src/diagnostics.rs`
- Modify: `src/status.rs`
- Modify: `src/output.rs`
- Modify: `tests/integration/diagnostics.rs`
- Modify: `tests/integration/cli_help.rs`
- Modify: `README.md`
- Modify: `docs/architecture-ja.md`
- Modify: `docs/getting-started-ja.md`
- Modify: `docs/workflows-ja.md`
- Modify: `docs/troubleshooting-ja.md`
- Modify: `templates/instructions.md`

**Interfaces:**
- Consumes: `DecisionDoctorProjection` and bounded cycle/attempt projections.
- Produces: stable human and JSON fields for current Phase 2 state.

- [ ] **Step 1: Write diagnostics and documentation RED tests**

Require JSON and human output fields:

```rust
#[test]
fn campaign_status_projects_active_decision_without_raw_evidence() {
    let value = harness.status_json();
    assert_eq!(value["campaign"]["decision"]["state"], "waiting");
    assert_eq!(value["campaign"]["decision"]["attempt_count"], 1);
    assert!(value["campaign"]["decision"]["next_wake_at"].is_number());
    assert!(!value.to_string().contains("raw-log-secret"));
}
```

Add doctor cases for orphan lineage, duplicate active attempt, overdue run, waiting without wake,
digest mismatch, and healthy absence on legacy projects. Add a docs contract requiring the automatic
terminal loop and retaining explicit Phase 3 observer/OOM limitations.

- [ ] **Step 2: Run RED tests**

```bash
cargo test --test diagnostics decision_ -- --nocapture --test-threads=1
cargo test --test cli_help phase_2_ -- --nocapture --test-threads=1
```

- [ ] **Step 3: Implement bounded projections**

Add at most one current decision object to status. Include cycle ID, source experiment ID, state,
attempt count, last decision kind, next wake, and bounded failure code/summary. Do not expose context
JSON/digest, decision JSON, prompt, transcript, environment, complete argv, or log excerpts.

Doctor queries must be read-only, project/campaign scoped, indexed, and bounded. A malformed row is a
typed error check, not a migration/repair attempt.

- [ ] **Step 4: Update templates and docs truthfully**

Document the four-command zero-adapter start, automatic terminal proposal/wait behavior, budget and
degraded remediation, and decision status fields. Remove statements that say the terminal completion
loop is unavailable. Keep explicit statements that running OOM/stall detection, periodic observer,
goal review, and code worktrees remain unavailable.

Update `templates/instructions.md` so a Phase 2 decision agent returns exactly one structured decision
and never directly invokes Pueue or edits source.

- [ ] **Step 5: Run Task 7 GREEN gates**

```bash
cargo test --test diagnostics decision_ -- --nocapture --test-threads=1
cargo test --test cli_help phase_2_ -- --nocapture --test-threads=1
cargo test --test cli_help command_reference_covers_every_public_cli_without_exposing_internal_launch -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 6: Commit Task 7**

```bash
git add src/diagnostics.rs src/status.rs src/output.rs tests/integration/diagnostics.rs tests/integration/cli_help.rs README.md docs/architecture-ja.md docs/getting-started-ja.md docs/workflows-ja.md docs/troubleshooting-ja.md templates/instructions.md
git commit -m "docs: explain autonomous campaign decisions"
```

---

### Task 8: Linux Real-Pueue Acceptance and Final Verification

**Files:**
- Modify: `tests/e2e/rust_supervisor.sh`
- Modify: `tests/test_shell_entrypoints.bats`
- Modify: `tests/support/fake_codex.sh`
- Modify: `tests/integration/cli_help.rs`
- Create: `.superpowers/sdd/2026-08-21-zero-adapter-autonomous-completion-loop/final-report.md`

**Interfaces:**
- Consumes: the complete Phase 2 feature.
- Produces: supported-Linux evidence for success, failure, wait, restart, no duplicate add, network enabled, and credentials absent.

- [ ] **Step 1: Extend the Linux E2E fixture before production adjustments**

The synthetic repository must not provide a result manifest or controller. Extend the generated
Codex fixture with deterministic decision modes:

- terminal success emits an experiment proposal;
- terminal failure with trusted fingerprint emits a repair proposal;
- terminal failure without fingerprint emits a non-repair experiment proposal;
- wait emits a 1-minute wait and later emits a proposal;
- invalid output emits malformed JSON for exactly three attempts.

Record decision invocations, network config, filtered environment names, Pueue add argv, and task IDs
without recording credential values or prompts.

- [ ] **Step 2: Add Bats structural and runtime contracts**

Require the shell harness to assert:

```bash
run grep -F 'sandbox_read_only=true' "$capture"
[ "$status" -eq 0 ]
run grep -F 'network_access=true' "$capture"
[ "$status" -eq 0 ]
run grep -E 'OPENAI_API_KEY|AWS_SECRET_ACCESS_KEY|SSH_AUTH_SOCK' "$captured_env_names"
[ "$status" -ne 0 ]
```

Add exact SQL assertions for one cycle per source experiment, one accepted child per proposal cycle,
zero add on wait, finite `next_wake_at`, and degraded after three invalid attempts.

- [ ] **Step 3: Run local static and compile gates**

```bash
bash -n install.sh bin/pueue-agent tests/e2e/rust_supervisor.sh tests/e2e/run.sh
bats tests/test_shell_entrypoints.bats
cargo check --all-targets
cargo check --release --all-targets
git diff --check
```

Expected: all commands exit 0. If the local host cannot run Linux process tests, do not claim E2E
GREEN locally.

- [ ] **Step 4: Run the exact merged tree on `roko`**

Create an owner-only isolated worktree or bundle clone on `roko`, set `umask 077`, and use owner-only
`TMPDIR` and `CARGO_TARGET_DIR` paths whose names do not contain output-contract sentinel strings.

Run:

```bash
cargo test --all-targets -- --test-threads=1
cargo check --all-targets
cargo check --release --all-targets
bats tests/test_shell_entrypoints.bats
tests/e2e/run.sh
```

Record OS/kernel, architecture, Pueue version, Rust version, exact commit, exit codes, test counts,
and any warnings. A failure stops integration; do not classify it as flaky without reproducing from
the pre-task commit under the same environment.

- [ ] **Step 5: Write the final report**

Document task commits, RED/GREEN evidence, schema version, crash-window coverage, security boundaries,
Linux real-Pueue output, unsupported platforms, remaining Phase 3/4/5 work, and known pre-existing
warnings. Do not include credentials, raw prompts, captured agent output, or temporary host paths.

- [ ] **Step 6: Run final verification from a clean tree**

```bash
git diff --check
git status --short
git log --oneline --decorate -12
```

Expected: only the intentionally ignored report may remain outside tracked status; all production and
test changes are committed.

- [ ] **Step 7: Commit Task 8 tracked changes**

```bash
git add tests/e2e/rust_supervisor.sh tests/test_shell_entrypoints.bats tests/support/fake_codex.sh tests/integration/cli_help.rs
git commit -m "test: verify autonomous campaign completion"
```

Keep the `.superpowers/sdd/` report unstaged when it is ignored by repository policy. If
`git check-ignore` confirms it is not ignored, stage it in a separate documentation commit rather
than changing the Task 8 test commit's file set.

---

## Final Review Checklist

- [ ] Every design requirement maps to one of Tasks 1 through 8.
- [ ] No task introduces running health, periodic observer, goal evaluation, or code worktrees.
- [ ] All new DB mutations are transactionally scoped and side-effect free.
- [ ] All external effects reuse existing AgentRunner, CampaignCoordinator, and Pueue boundaries.
- [ ] Decision output is persisted before private-temp cleanup.
- [ ] Pause/disable/halt/retire and decision admission share the project lock.
- [ ] Waits are finite, indexed, restart-safe, and create no task.
- [ ] Invalid output is bounded and diagnosably degraded.
- [ ] Linux real-Pueue evidence covers success, failure, wait, restart, network, and credentials.
- [ ] Documentation states both the new Phase 2 behavior and remaining Phase 3/4/5 limitations.
