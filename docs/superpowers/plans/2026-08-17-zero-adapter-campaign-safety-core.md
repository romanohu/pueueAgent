# Zero-adapter Campaign Safety Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 最初の `submit` を durable managed campaign と baseline experiment に変換し、hard policy、objective、proposal、rolling budget、Pueue side effect を supervisor が安全かつ冪等に所有する Phase 1 を構築する。

**Architecture:** `.pueue-agent/STATE.md` を bounded objective snapshot として読み、service-owned execution policy から campaign limits を取得する。新しい campaign repository が campaign、proposal、experiment、budget reservation、既存 submission intent を一つの SQLite transaction で作り、application coordinator が commit 後の Pueue add を `submitting`/`accepted`/`unreconciled` として追跡する。active campaign 中の一般 `submit`/`submit-batch` は拒否し、read-only CLI と diagnostics で状態を可視化する。

**Tech Stack:** Rust 2021、Tokio、rusqlite/SQLite、serde/serde_json、TOML、SHA-256、Clap、既存の `PueueApi`、Cargo integration tests、Linux real-Pueue shell acceptance。

## Global Constraints

- 規範設計は `docs/superpowers/specs/2026-08-17-zero-adapter-continuous-ml-campaign-design.md` とする。
- 正式な実行対象は Linux。macOS の campaign agent execution 対応は主張しない。
- network の service default は `enabled`。credential/environment value の継承は既存 allowlist 外では拒否する。
- objective は最初の submit 時の `.pueue-agent/STATE.md` snapshot で固定し、active campaign 中のファイル変更では更新しない。
- 初期 hard limits は parallel=1、new experiments/24h=24、agent runs/hour=6、code changes/24h=10、same-spec retries=2、repairs/fingerprint=2、accepted proposals/cycle=1、observer interval=30分とする。
- `state.json` schema v2 から agent-writable budget を除き、v1 budget を service limit へ昇格させない。
- SQLite commit と Pueue add を一つの transaction とみなさない。外部 add 開始後の不明結果は `unreconciled` とし、自動再 add しない。
- active campaign 中は human/agent の一般 `submit` と `submit-batch` を拒否する。内部 coordinator だけが managed submission を作る。
- prompt、raw transcript、credential、environment value を SQLite、diagnostics、error、Debug に保存・表示しない。
- Phase 1 では result interpretation、periodic observer、health diagnosis、goal promotion、code worktree 実行を実装しない。
- 新しい runtime dependency は persistent canonical digest に必要な `sha2 = "0.10"` だけとする。
- 各 task は RED、最小 GREEN、focused verification、review、commit の順に完了する。

---

## File responsibility map

- `src/state.rs`: state.json v1/v2 decode と objective snapshot の bounded validation。
- `src/execution_policy.rs`: service-owned `CampaignLimits` の parse、bounds、immutable projection。
- `src/models.rs`: campaign/proposal/experiment/reservation の typed states と records。
- `src/db/migrations.rs`: schema v16 の作成、v15 migration、current-schema verification。
- `src/db/campaigns.rs`: campaign domain の query と atomic transitions。外部 Pueue call は行わない。
- `src/campaign.rs`: baseline/proposal intent と `PueueApi` を結ぶ application coordinator、campaign command operations。
- `src/proposals.rs`: untrusted structured proposal の validation と canonical SHA-256 digest。
- `src/submit.rs`, `src/batches.rs`: user-facing submit boundary と managed campaign admission。
- `src/reconcile.rs`: accepted/unreconciled submission から experiment state への idempotent projection。
- `src/status.rs`, `src/diagnostics.rs`: bounded campaign projections と invariant checks。
- `src/cli.rs`, `src/main.rs`: campaign/proposal/experiment command routing。
- `templates/*`, `docs/*`: SQLite ownershipと zero-adapter workflow の user-facing contract。

後続の autonomous loop、running health/observer、goal evaluation、isolated code changes は、この
Phase 1 が GREEN になった後にそれぞれ別の implementation plan を作る。

---

### Task 1: Canonical state v2 と objective snapshot

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/state.rs`
- Modify: `src/scheduler.rs`
- Modify: `src/init.rs`
- Modify: `templates/state.json`
- Modify: `templates/STATE.md`
- Modify: `templates/instructions.md`
- Test: `tests/integration/init.rs`
- Test: `tests/integration/diagnostics.rs`
- Test: `tests/integration/scheduler.rs`

**Interfaces:**
- Produces `state::ObjectiveSnapshot { text: String, digest: String }`.
- Produces `state::load_objective(project_root: &Path) -> Result<ObjectiveSnapshot, AppError>`.
- Produces `CanonicalState` schema v2 without a `budgets` field.
- Keeps `state::load` able to read schema v1, but discards v1 budget values.
- Scheduler consumes project `GuardrailsConfig` directly; state.json cannot widen or narrow it.

- [ ] **Step 1: Write state v2 and objective RED tests**

Add tests that parse v2 without budgets, normalize CRLF to LF, and reject the generated unedited
`STATE.md`, whitespace-only goals, NUL/control characters, and content over 16 KiB.

```rust
#[test]
fn objective_snapshot_is_bounded_normalized_and_stable() {
    let temp = tempfile::TempDir::new().unwrap();
    let root = temp.path().join("project");
    std::fs::create_dir_all(root.join(".pueue-agent")).unwrap();
    std::fs::write(
        root.join(".pueue-agent/STATE.md"),
        "# Goal\r\nReach validation loss below 0.20\r\n",
    ).unwrap();
    let first = state::load_objective(&root).unwrap();
    let second = state::load_objective(&root).unwrap();
    assert_eq!(first.text, "# Goal\nReach validation loss below 0.20\n");
    assert_eq!(first.digest, second.digest);
    assert_eq!(first.digest.len(), 64);
}

#[test]
fn untouched_state_template_is_not_a_campaign_objective() {
    let temp = tempfile::TempDir::new().unwrap();
    let root = temp.path().join("project");
    std::fs::create_dir_all(root.join(".pueue-agent")).unwrap();
    std::fs::write(root.join(".pueue-agent/STATE.md"), include_str!("../../templates/STATE.md")).unwrap();
    let error = state::load_objective(&root).unwrap_err();
    assert!(matches!(error, AppError::Validation { field: "STATE.md", .. }));
}
```

Add a schema migration test proving a v1 `budgets.max_experiments = 1_000_000` value does not
alter configured guardrails.

- [ ] **Step 2: Run the focused tests and capture RED**

Run:

```bash
cargo test --test init objective_ -- --nocapture --test-threads=1
cargo test --test scheduler legacy_state_budget -- --nocapture --test-threads=1
```

Expected: compile failure for missing `ObjectiveSnapshot`/`load_objective`, then the legacy budget
test shows the current state value overriding project guardrails.

- [ ] **Step 3: Add SHA-256 and implement objective validation**

Add `sha2 = "0.10"`. Implement the exact public boundary:

```rust
pub const MAX_OBJECTIVE_BYTES: usize = 16 * 1024;

#[derive(Clone, PartialEq, Eq)]
pub struct ObjectiveSnapshot {
    pub text: String,
    pub digest: String,
}

impl std::fmt::Debug for ObjectiveSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectiveSnapshot")
            .field("bytes", &self.text.len())
            .field("digest", &self.digest)
            .finish()
    }
}

pub fn load_objective(project_root: &Path) -> Result<ObjectiveSnapshot, AppError> {
    let path = project_root.join(".pueue-agent/STATE.md");
    let bytes = read_bounded(&path, MAX_OBJECTIVE_BYTES, "read campaign objective")?;
    let source = String::from_utf8(bytes).map_err(|source| AppError::Io {
        operation: "decode campaign objective",
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })?;
    let text = source.replace("\r\n", "\n");
    validate_objective_text(&text)?;
    use sha2::{Digest, Sha256};
    let digest = format!("{:x}", Sha256::digest(text.as_bytes()));
    Ok(ObjectiveSnapshot { text, digest })
}
```

`validate_objective_text` must reject NUL and non-whitespace control characters, the exact generated
template, the sentinel `(目的をここに書く)`, and content without one non-comment/non-table
meaningful line.

- [ ] **Step 4: Decode v1 safely and remove budget authority**

Deserialize through a private raw type with an optional legacy budget map. For schema v1, validate
the old map only to bound parsing, discard it, and return schema v2 in memory. For schema v2, reject
a present `budgets` field. Remove `CanonicalState::effective_guardrails`; change
`load_effective_guardrails` to validate state when present and return `configured.clone()`.

The v2 template must be exactly this shape:

```json
{
  "schema_version": 2,
  "current_facts": ["campaign not started"],
  "historical_facts": [],
  "next_action": "define experiment objective",
  "active_lineage": {
    "event_id": null,
    "run_id": null,
    "submission_ids": [],
    "task_ids": []
  }
}
```

- [ ] **Step 5: Update templates and scheduler wording**

State that SQLite owns campaign/objective/budget/lineage, `STATE.md` owns the human goal, and
state.json is a bounded agent scratch projection. Remove instructions telling agents to modify
budgets or directly call `pueue-agent submit` during a managed campaign.

- [ ] **Step 6: Run focused GREEN**

```bash
cargo test --lib state:: -- --nocapture --test-threads=1
cargo test --test init -- --nocapture --test-threads=1
cargo test --test diagnostics state_ -- --nocapture --test-threads=1
cargo test --test scheduler legacy_state_budget -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 7: Review and commit**

Review byte bounds, Debug redaction, v1 budget non-authority, template preservation, and no implicit
rewrite of existing STATE.md.

```bash
git add Cargo.toml Cargo.lock src/state.rs src/scheduler.rs src/init.rs templates/state.json templates/STATE.md templates/instructions.md tests/integration/init.rs tests/integration/diagnostics.rs tests/integration/scheduler.rs
git commit -m "feat: define immutable campaign objectives"
```

---

### Task 2: Service-owned campaign hard policy

**Files:**
- Modify: `src/execution_policy.rs`
- Modify: `tests/integration/execution_policy.rs`
- Modify: `tests/integration/codex_security.rs`
- Modify: `tests/support/execution_policy_fixture.rs`

**Interfaces:**
- Produces `execution_policy::CampaignLimits`.
- Adds `ResolvedExecutionPolicy::campaign_limits: CampaignLimits`.
- `CampaignLimits::default()` returns the exact approved defaults.
- Later tasks consume an immutable cloned value from the startup-resolved policy.

- [ ] **Step 1: Write policy RED tests**

```rust
#[test]
fn campaign_limits_have_safe_service_defaults() {
    let harness = PolicyHarness::new();
    let policy = load_or_create_policy(&harness.input()).unwrap();
    assert_eq!(policy.campaign_limits, CampaignLimits {
        max_parallel_experiments: 1,
        max_new_experiments_per_24h: 24,
        max_agent_runs_per_hour: 6,
        max_code_change_proposals_per_24h: 10,
        max_same_spec_retries: 2,
        max_repairs_per_failure_fingerprint: 2,
        max_proposals_per_cycle: 1,
        observer_interval_minutes: 30,
    });
    assert_eq!(policy.default_network, NetworkMode::Enabled);
}
```

Add table-driven rejection for zero parallel/experiment/agent/proposal/observer values, parallel
above 64, daily experiment above 10,000, hourly agent above 1,000, code proposal above 1,000,
retry/repair above 100, accepted proposals per cycle above 32, and observer interval above 1,440.
Allow zero only for code-change proposals, same-spec retries, and repairs to disable those actions.

- [ ] **Step 2: Run RED**

```bash
cargo test --test execution_policy campaign_limits -- --nocapture --test-threads=1
```

Expected: compile failure because `CampaignLimits` and `campaign_limits` do not exist.

- [ ] **Step 3: Implement the immutable policy projection**

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CampaignLimits {
    pub max_parallel_experiments: u32,
    pub max_new_experiments_per_24h: u32,
    pub max_agent_runs_per_hour: u32,
    pub max_code_change_proposals_per_24h: u32,
    pub max_same_spec_retries: u32,
    pub max_repairs_per_failure_fingerprint: u32,
    pub max_proposals_per_cycle: u32,
    pub observer_interval_minutes: u32,
}
```

Add a `[campaign]` table to `RawPolicy`, parse it with `deny_unknown_fields`, enforce the test bounds,
and add the following to `DEFAULT_POLICY`:

```toml
[campaign]
max_parallel_experiments = 1
max_new_experiments_per_24h = 24
max_agent_runs_per_hour = 6
max_code_change_proposals_per_24h = 10
max_same_spec_retries = 2
max_repairs_per_failure_fingerprint = 2
max_proposals_per_cycle = 1
observer_interval_minutes = 30
```

Do not add project-level overrides. Existing project `agent.execution.network = "disabled"` may only
narrow the service default.

- [ ] **Step 4: Prove credential and network separation**

Extend fixture tests so default network is enabled while an unlisted `AWS_SECRET_ACCESS_KEY`,
`WANDB_API_KEY`, and `SSH_AUTH_SOCK` remain absent from both agent and task sanitized environments.

- [ ] **Step 5: Run focused GREEN**

```bash
cargo test --test execution_policy campaign_limits -- --nocapture --test-threads=1
cargo test --test codex_security environment -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 6: Review and commit**

```bash
git add src/execution_policy.rs tests/integration/execution_policy.rs tests/integration/codex_security.rs tests/support/execution_policy_fixture.rs
git commit -m "feat: add service-owned campaign limits"
```

---

### Task 3: SQLite schema v16 and typed campaign models

**Files:**
- Modify: `src/models.rs`
- Modify: `src/db/migrations.rs`
- Test: `tests/integration/database.rs`

**Interfaces:**
- Produces typed enums `CampaignState`, `ProposalKind`, `ProposalStatus`, `ExperimentStatus`, and `BudgetReservationStatus`.
- Produces records `Campaign`, `Proposal`, `Experiment`, and `BudgetReservation`.
- Produces schema v16 tables and indexes without changing legacy submission rows.

- [ ] **Step 1: Write migration RED tests**

Add tests for a fresh v16 database, v15-to-v16 migration preserving projects/submissions, exact CHECK
constraints, required indexes, foreign keys, and rejection of a malformed current-v16 schema.
Define a local `V15Fixture` in `tests/integration/database.rs`; it owns a `TempDir` and database path,
creates the exact canonical v15 DDL from the existing migration fixture, inserts one project and one
submission, sets `PRAGMA user_version = 15`, and opens the path through `Db::open` in
`open_and_migrate`.

```rust
#[test]
fn v15_migrates_campaign_tables_without_claiming_legacy_submissions() {
    let fixture = V15Fixture::with_submission("legacy-submission");
    let _db = Db::open(&fixture.path).unwrap();
    let connection = rusqlite::Connection::open(&fixture.path).unwrap();
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0)).unwrap();
    let campaigns: i64 = connection.query_row("SELECT COUNT(*) FROM campaigns", [], |row| row.get(0)).unwrap();
    let legacy: i64 = connection.query_row(
        "SELECT COUNT(*) FROM submissions WHERE submission_id = 'legacy-submission'",
        [],
        |row| row.get(0),
    ).unwrap();
    assert_eq!(version, 16);
    assert_eq!(campaigns, 0);
    assert_eq!(legacy, 1);
}
```

- [ ] **Step 2: Run migration RED**

```bash
cargo test --test database campaign_schema -- --nocapture --test-threads=1
cargo test --test database v15_migrates_campaign -- --nocapture --test-threads=1
```

Expected: missing table/index assertions fail and `LATEST_SCHEMA_VERSION` remains 15.

- [ ] **Step 3: Add typed model enums**

Use the existing `database_enum!` macro with these exact persisted values:

```rust
database_enum!(CampaignState {
    Active => "active",
    BudgetWaiting => "budget_waiting",
    GoalReachedPendingReview => "goal_reached_pending_review",
    Paused => "paused",
    Degraded => "degraded",
    Halted => "halted",
    Retired => "retired",
});

database_enum!(ProposalStatus {
    Pending => "pending",
    Accepted => "accepted",
    Rejected => "rejected",
});

database_enum!(ExperimentStatus {
    Reserved => "reserved",
    Submitting => "submitting",
    Accepted => "accepted",
    Unreconciled => "unreconciled",
    Succeeded => "succeeded",
    Failed => "failed",
    Cancelled => "cancelled",
});
```

`ProposalKind` values are `experiment`, `repair`, `broader_search`, `recipe`, `code_change`, and
`data_evaluation`. `BudgetReservationStatus` values are `reserved`, `consumed`, and `released`.
Add `BudgetDimension::{Experiment, AgentRun, CodeChange}` with persisted values `experiment`,
`agent_run`, and `code_change`.

- [ ] **Step 4: Create the schema v16 tables**

Implement a transactional v16 migration using these columns and constraints:

```sql
CREATE TABLE campaigns (
    campaign_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    objective_text TEXT NOT NULL,
    objective_digest TEXT NOT NULL,
    initial_argv_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'active','budget_waiting','goal_reached_pending_review','paused',
        'degraded','halted','retired'
    )),
    state_reason TEXT,
    baseline_experiment_id TEXT REFERENCES experiments(experiment_id) ON DELETE RESTRICT,
    next_eligible_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX campaigns_one_live_project_idx
ON campaigns(project_id) WHERE state <> 'retired';

CREATE TABLE proposals (
    proposal_id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN (
        'experiment','repair','broader_search','recipe','code_change','data_evaluation'
    )),
    status TEXT NOT NULL CHECK (status IN ('pending','accepted','rejected')),
    hypothesis TEXT NOT NULL,
    source_experiment_id TEXT REFERENCES experiments(experiment_id) ON DELETE RESTRICT,
    argv_json TEXT NOT NULL,
    working_directory TEXT NOT NULL,
    expected_evidence_json TEXT NOT NULL,
    canonical_digest TEXT NOT NULL,
    reject_reason TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(campaign_id, canonical_digest)
);

CREATE TABLE experiments (
    experiment_id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id) ON DELETE CASCADE,
    proposal_id TEXT NOT NULL REFERENCES proposals(proposal_id) ON DELETE RESTRICT,
    submission_id TEXT NOT NULL UNIQUE REFERENCES submissions(submission_id) ON DELETE RESTRICT,
    parent_experiment_id TEXT REFERENCES experiments(experiment_id) ON DELETE RESTRICT,
    attempt INTEGER NOT NULL CHECK (attempt >= 0),
    status TEXT NOT NULL CHECK (status IN (
        'reserved','submitting','accepted','unreconciled',
        'succeeded','failed','cancelled'
    )),
    pueue_task_id INTEGER,
    task_signature TEXT,
    failure_code TEXT,
    failure_fingerprint TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    finished_at INTEGER,
    UNIQUE(proposal_id, attempt),
    CHECK ((pueue_task_id IS NULL) = (task_signature IS NULL))
);

CREATE TABLE budget_reservations (
    reservation_id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id) ON DELETE CASCADE,
    experiment_id TEXT REFERENCES experiments(experiment_id) ON DELETE RESTRICT,
    dimension TEXT NOT NULL CHECK (dimension IN ('experiment','agent_run','code_change')),
    subject_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('reserved','consumed','released')),
    window_started_at INTEGER NOT NULL,
    window_ends_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(campaign_id, dimension, subject_key),
    CHECK (window_ends_at > window_started_at),
    CHECK (
        (dimension = 'experiment' AND experiment_id IS NOT NULL)
        OR (dimension <> 'experiment' AND experiment_id IS NULL)
    )
);
```

SQLite accepts the forward references from campaign/proposal creation to the later experiments table.
Add indexes for campaign state/wake, proposal campaign/status/created time, experiment
campaign/status/created time, exact Pueue task lookup, and reservation campaign/dimension/window. Set
`PRAGMA user_version = 16` only after `verify_campaign_schema_v16` succeeds inside the transaction.

- [ ] **Step 5: Implement current-schema verification**

Verify exact table SQL invariants by `PRAGMA table_info`, `PRAGMA foreign_key_list`, and canonical
index SQL. A v16 database with missing CHECK, wrong nullability, missing partial unique index, or
missing FK must fail with bounded static operation `verify SQLite v16 campaign schema` and perform
no repair outside an IMMEDIATE transaction.

- [ ] **Step 6: Run database GREEN**

```bash
cargo test --test database campaign_schema -- --nocapture --test-threads=1
cargo test --test database migration -- --nocapture --test-threads=1
cargo test --test database -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 7: Review and commit**

Review FK delete semantics, one-live-campaign index, malformed fast-path rejection, rollback, and
absence of secrets/raw environment fields.

```bash
git add src/models.rs src/db/migrations.rs tests/integration/database.rs
git commit -m "feat: add campaign schema v16"
```

---

### Task 4: Structured proposal validation and canonical identity

**Files:**
- Create: `src/proposals.rs`
- Modify: `src/lib.rs`
- Modify: `src/models.rs`
- Test: unit tests in `src/proposals.rs`

**Interfaces:**
- Produces `proposals::ProposalInput` as the only deserializable agent-facing proposal type.
- Produces `proposals::ValidatedProposal` with private fields and read-only accessors.
- Produces `proposals::validate(input, objective_digest) -> Result<ValidatedProposal, AppError>`.
- Canonical digest is SHA-256 over versioned deterministic JSON and is stable across retries.

- [ ] **Step 1: Write validation RED tests**

Test valid baseline/repair/code-change proposals; reject unknown fields, empty argv, absolute or
parent-traversing working directories, control characters, more than 16 evidence items, evidence
items over 512 bytes, hypothesis over 4 KiB, argv item over the native field limit, and a source
experiment missing for repair/code-change/data-evaluation.

```rust
#[test]
fn semantically_identical_proposals_have_one_digest() {
    let input = ProposalInput {
        kind: ProposalKind::Experiment,
        hypothesis: "Reduce learning rate after the baseline".to_owned(),
        source_experiment_id: Some("exp-baseline".to_owned()),
        argv: vec!["python".to_owned(), "train.py".to_owned(), "--lr".to_owned(), "0.001".to_owned()],
        working_directory: ".".to_owned(),
        expected_evidence: vec!["validation loss".to_owned()],
    };
    assert_eq!(
        proposals::validate(input.clone(), "objective-digest").unwrap().canonical_digest(),
        proposals::validate(input, "objective-digest").unwrap().canonical_digest(),
    );
}
```

- [ ] **Step 2: Run RED**

```bash
cargo test --lib proposals::tests -- --nocapture --test-threads=1
```

Expected: compile failure for missing proposal module and types.

- [ ] **Step 3: Implement the bounded untrusted input**

```rust
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposalInput {
    pub kind: ProposalKind,
    pub hypothesis: String,
    pub source_experiment_id: Option<String>,
    pub argv: Vec<String>,
    pub working_directory: String,
    pub expected_evidence: Vec<String>,
}
```

Validate IDs as 1..=128 bytes of control-free UTF-8. Working directory must be `.` or a relative
path of normal components only. Validate argv through `pueue::validate_add_argv` after the caller
adds `-g <verified-group> --`; the module itself enforces count/field/control bounds without
inventing a group. Keep `ValidatedProposal` fields private so serde cannot bypass validation.

- [ ] **Step 4: Implement versioned canonical digest**

Serialize a private struct with fixed field order and a literal `schema_version: 1`. Include the
objective digest, proposal kind, hypothesis, source experiment, argv, normalized working directory,
and expected evidence in their validated order.

```rust
use sha2::Digest;

let canonical = serde_json::to_vec(&CanonicalProposal {
    schema_version: 1,
    objective_digest,
    kind: input.kind.as_str(),
    hypothesis: &input.hypothesis,
    source_experiment_id: input.source_experiment_id.as_deref(),
    argv: &input.argv,
    working_directory: &normalized_working_directory,
    expected_evidence: &input.expected_evidence,
}).map_err(|source| AppError::Serialization {
    operation: "serialize canonical campaign proposal",
    source,
})?;
let canonical_digest = format!("{:x}", sha2::Sha256::digest(canonical));
```

- [ ] **Step 5: Add digest and source-shape boundary tests**

Validate the same input twice and assert one digest. Change only the objective digest and assert a
different digest. Require a nonempty source ID for repair/code-change/data-evaluation, and reject a
source ID on the initial baseline constructor. Cross-campaign ownership is tested in Task 5 where
the repository exists.

- [ ] **Step 6: Run focused GREEN**

```bash
cargo test --lib proposals::tests -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 7: Review and commit**

```bash
git add src/proposals.rs src/lib.rs src/models.rs
git commit -m "feat: validate campaign proposals"
```

---

### Task 5: Atomic campaign, proposal, experiment, and budget repository

**Files:**
- Create: `src/db/campaigns.rs`
- Modify: `src/db/mod.rs`
- Modify: `src/models.rs`
- Test: `tests/integration/database.rs`

**Interfaces:**
- Produces `CampaignRepository`, `ProposalRepository`, and `ExperimentRepository`.
- Produces `CampaignRepository::start_with_baseline` and `accept_proposal` as IMMEDIATE transactions.
- Produces `ManagedSubmissionIntent { campaign, proposal, experiment, submission }`.
- Produces strict transition methods `mark_submitting`, `mark_accepted`, `mark_unreconciled`, and `project_terminal_submission`.

- [ ] **Step 1: Write atomicity and race RED tests**

Add tests for two connections starting one project simultaneously, parallel=1 proposal acceptance,
24-hour rolling boundary, duplicate digest, rollback on injected submission insert failure, and
project/campaign state revalidation inside the same transaction. Add a cross-campaign source
experiment attempt and assert rejection before proposal, experiment, reservation, or submission
insertion.
Define `CampaignDbHarness` next to the tests as a wrapper around the existing `TestDatabase`; its
concurrent method uses an `Arc<Barrier>` and two cloned `Db` handles against the same path, matching
the existing concurrent migration/admission test style.

```rust
#[test]
fn parallel_baseline_start_creates_exactly_one_live_campaign_and_reservation() {
    let harness = CampaignDbHarness::new();
    let results = harness.concurrent_start_same_project();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(harness.count("campaigns"), 1);
    assert_eq!(harness.count("proposals"), 1);
    assert_eq!(harness.count("experiments"), 1);
    assert_eq!(harness.count("budget_reservations"), 1);
    assert_eq!(harness.count("submissions"), 1);
}
```

- [ ] **Step 2: Run RED**

```bash
cargo test --test database campaign_atomic -- --nocapture --test-threads=1
cargo test --test database rolling_budget -- --nocapture --test-threads=1
```

Expected: repository methods are missing.

- [ ] **Step 3: Implement atomic start and acceptance**

`start_with_baseline` must begin IMMEDIATE, verify project enabled/not paused/not halted, verify no
live campaign, insert campaign and accepted baseline proposal, enforce limits, insert pending
submission, reserved experiment, reservation, update baseline experiment ID, read all owned rows,
then commit.

`accept_proposal` must verify campaign `active`, source lineage, unique digest, proposal-per-cycle,
parallel count, rolling 24-hour experiment count, same-spec retry count, and fingerprint repair
count before it inserts any row. Return an existing matching accepted intent for the same digest;
never create a second experiment.

For Phase 1, one decision cycle is identified by `(campaign_id, source_experiment_id)`. The baseline
uses a null source and is created only by `start_with_baseline`. Repair counts derive the fingerprint
from the referenced source experiment; untrusted proposal JSON cannot supply or replace it.
`CodeChange` proposals remain `pending` and cannot create an experiment until the isolated-worktree
phase is implemented. Recording such a pending proposal still consumes the daily code-change
proposal budget, so repeated agent output cannot bypass the configured hard cap.

Use this request boundary:

```rust
pub struct StartCampaignRequest<'a> {
    pub campaign_id: &'a str,
    pub project_id: &'a str,
    pub objective: &'a ObjectiveSnapshot,
    pub initial_argv: &'a [String],
    pub baseline: &'a ValidatedProposal,
    pub submission_id: &'a str,
    pub experiment_id: &'a str,
    pub proposal_id: &'a str,
    pub now: i64,
}
```

- [ ] **Step 4: Implement strict external-side-effect transitions**

```rust
pub fn mark_submitting(&self, experiment_id: &str, now: i64) -> Result<Experiment, AppError>;

pub fn mark_accepted(
    &self,
    experiment_id: &str,
    task_id: i64,
    task_signature: &str,
    now: i64,
) -> Result<Experiment, AppError>;

pub fn mark_unreconciled(
    &self,
    experiment_id: &str,
    reason_code: &'static str,
    now: i64,
) -> Result<Experiment, AppError>;
```

`mark_submitting` accepts only `reserved`; `mark_accepted` accepts only `submitting` and updates the
existing submission plus experiment atomically; `mark_unreconciled` accepts `submitting` and sets
both rows to `unreconciled`. Repeating the exact terminal transition is idempotent; conflicting task
IDs or transitions fail validation.

- [ ] **Step 5: Implement terminal projection and reservation accounting**

`project_terminal_submission` maps successful Pueue results to `succeeded`, failed results to
`failed`, and killed/cancelled results to `cancelled`. It consumes the reservation in the same
transaction. It accepts only the task ID already bound to the experiment and is idempotent for an
identical terminal result.

- [ ] **Step 6: Run database GREEN**

```bash
cargo test --test database campaign_ -- --nocapture --test-threads=1
cargo test --test database rolling_budget -- --nocapture --test-threads=1
cargo test --test database -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 7: Review and commit**

Review BEGIN IMMEDIATE scope, no filesystem/Pueue work in writer transactions, retry boundaries,
one-live-campaign enforcement, exact task identity, and rollback behavior.

```bash
git add src/db/campaigns.rs src/db/mod.rs src/models.rs tests/integration/database.rs
git commit -m "feat: reserve campaign experiments atomically"
```

---

### Task 6: Managed Pueue coordinator and first-submit boundary

**Files:**
- Create: `src/campaign.rs`
- Modify: `src/lib.rs`
- Modify: `src/submit.rs`
- Modify: `src/batches.rs`
- Modify: `src/main.rs`
- Modify: `src/reconcile.rs`
- Test: `tests/integration/pueue_adapter.rs`
- Test: `tests/integration/reconciliation.rs`
- Test: `tests/integration/cli_help.rs`

**Interfaces:**
- Produces `campaign::CampaignCoordinator<'a, P: PueueApi>`.
- `start_baseline` consumes validated project/objective/policy and performs DB intent before add.
- `submit_accepted_intent` is the only campaign path that calls `PueueApi::add`.
- User-facing `submit::run_with_options` consumes `&CampaignLimits`.

- [ ] **Step 1: Write first-submit and rejection RED tests**

Extend the existing `SubmitHarness` with `with_objective`, `with_active_campaign`, row-count helpers,
and a fake `PueueApi` call counter. These helpers must use public repository operations rather than
directly changing campaign rows, except in tests that explicitly target crash-boundary states.

```rust
#[tokio::test]
async fn first_experiment_submit_creates_campaign_baseline_and_one_pueue_task() {
    let harness = SubmitHarness::with_objective("Reach validation loss below 0.20");
    let result = harness.submit(&["python", "train.py"]).await.unwrap();
    assert_eq!(harness.live_campaigns(), 1);
    assert_eq!(harness.experiments(), 1);
    assert_eq!(harness.pueue_add_calls(), 1);
    assert_eq!(result.pueue_task_id, Some(41));
}

#[tokio::test]
async fn active_campaign_rejects_human_and_agent_direct_submit_before_persistence() {
    let harness = SubmitHarness::with_active_campaign();
    assert!(harness.submit(&["python", "other.py"]).await.is_err());
    assert!(harness.submit_from_agent_run(&["python", "other.py"]).await.is_err());
    assert_eq!(harness.submissions(), 0);
    assert_eq!(harness.pueue_add_calls(), 0);
}
```

Add batch rejection with zero batch/submission rows and zero Pueue calls. Preserve a legacy
one-off `--kind control` submission only when no live campaign exists.

- [ ] **Step 2: Run RED**

```bash
cargo test --test pueue_adapter campaign_submit -- --nocapture --test-threads=1
cargo test --test cli_help active_campaign_submit -- --nocapture --test-threads=1
```

Expected: first submit creates only a legacy submission and later direct submits are accepted.

- [ ] **Step 3: Implement the coordinator**

```rust
pub struct CampaignCoordinator<'a, P: PueueApi + ?Sized> {
    db: &'a Db,
    pueue: &'a P,
    limits: CampaignLimits,
}

impl<'a, P: PueueApi + ?Sized> CampaignCoordinator<'a, P> {
    pub fn new(db: &'a Db, pueue: &'a P, limits: CampaignLimits) -> Self {
        Self { db, pueue, limits }
    }
}
```

`start_baseline` builds a fixed baseline proposal from the initial argv, validates the exact Pueue
add argv including verified group and `--working-directory <verified-project-root>`, creates the
atomic intent through `CampaignRepository`, marks it
`submitting`, calls `pueue.add`, and marks accepted. Any `pueue.add` error after `submitting` must first
persist `unreconciled`; return the original bounded Pueue error only after that persistence succeeds.

- [ ] **Step 4: Route user submit through campaign admission**

Change `submit::run` to retain `policy.campaign_limits` and pass it into `run_with_options`. After
validated project/config lookup, query live campaign before any submission insert. Default experiment with no
live campaign loads `STATE.md` and calls `start_baseline`. Any submit with a live campaign returns:

```rust
Err(AppError::Validation {
    field: "submit",
    message: "a managed campaign is active; use pueue-agent steer",
})
```

Keep control submissions only outside a live campaign and keep the existing origin validation.

- [ ] **Step 5: Reject batch before manifest persistence**

After project/policy validation but before `BatchRepository::create_or_get`, query the live campaign.
Return the same actionable error with field `submit-batch`; do not create a batch request, job,
submission, or Pueue task.

- [ ] **Step 6: Project terminal reconciliation into experiments**

In `reconcile`, after a submission observation is durably classified terminal, look up its optional
experiment. Call `project_terminal_submission` in the same repository transaction used for the
experiment transition. Legacy submissions without an experiment remain unchanged.

- [ ] **Step 7: Add crash-boundary tests**

Inject failures after intent commit/before `mark_submitting`, after `mark_submitting`/before add,
after successful add/before acceptance, and on add timeout. Assert reserved can be resumed without
duplicate, every submitting case becomes unreconciled, and no automatic second add occurs.

- [ ] **Step 8: Run focused GREEN**

```bash
cargo test --test pueue_adapter campaign_submit -- --nocapture --test-threads=1
cargo test --test reconciliation campaign_experiment -- --nocapture --test-threads=1
cargo test --test cli_help active_campaign_submit -- --nocapture --test-threads=1
cargo test --test pueue_adapter -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 9: Review and commit**

```bash
git add src/campaign.rs src/lib.rs src/submit.rs src/batches.rs src/main.rs src/reconcile.rs tests/integration/pueue_adapter.rs tests/integration/reconciliation.rs tests/integration/cli_help.rs
git commit -m "feat: start managed campaigns from submit"
```

---

### Task 7: Campaign, proposal, and experiment CLI

**Files:**
- Modify: `src/cli.rs`
- Modify: `src/main.rs`
- Modify: `src/campaign.rs`
- Modify: `src/db/campaigns.rs`
- Modify: `src/output.rs`
- Test: `tests/integration/cli_help.rs`
- Test: `tests/integration/operator_commands.rs`

**Interfaces:**
- Produces `campaign status|pause|resume|retire`.
- Produces bounded `proposal list|inspect` and `experiment list|inspect`.
- List queries use project/campaign scope, stable ordering, limit <= 100, and cursor-ready IDs.

- [ ] **Step 1: Write Clap and behavior RED tests**

Assert help exposes the three top-level commands and their eight actions, JSON output never contains
objective text/argv by default, pause/resume transitions only campaign state, and retire rejects
accepted/unreconciled experiments.
Extend the existing `OperatorHarness` with repository-backed campaign/experiment constructors and a
`campaign_state` query helper.

```rust
#[test]
fn campaign_retire_requires_no_nonterminal_experiment() {
    let harness = OperatorHarness::with_accepted_experiment();
    let output = harness.run(&["campaign", "retire"]);
    assert!(!output.status.success());
    assert_eq!(harness.campaign_state(), "active");
}
```

- [ ] **Step 2: Run RED**

```bash
cargo test --test cli_help campaign_command -- --nocapture --test-threads=1
cargo test --test operator_commands campaign_ -- --nocapture --test-threads=1
```

Expected: Clap rejects unknown commands.

- [ ] **Step 3: Add exact CLI types**

```rust
#[derive(Debug, Args)]
pub struct CampaignArgs {
    #[command(subcommand)]
    pub action: CampaignAction,
}

#[derive(Debug, Subcommand)]
pub enum CampaignAction {
    Status(CampaignStatusArgs),
    Pause(CampaignMutationArgs),
    Resume(CampaignMutationArgs),
    Retire(CampaignMutationArgs),
}
```

Add equivalent `ProposalAction::{List, Inspect}` and `ExperimentAction::{List, Inspect}`. Every
action supports `--json` and optional project root; list supports `--limit` default 20 and max 100.

- [ ] **Step 4: Implement strict campaign transitions**

`pause`: active/budget_waiting/degraded -> paused. `resume`: paused/budget_waiting/degraded/
goal_reached_pending_review -> active only after project enabled/not paused/not halted and no
unreconciled/termination-unknown state. `retire`: paused/goal-review/active -> retired only when all
experiments are terminal and reservations are consumed/released. Repeating the same operation is
idempotent; invalid transitions return typed validation errors.

- [ ] **Step 5: Implement bounded renderers**

Human and JSON status expose IDs, states, reason codes, objective digest, counts, timestamps, budget
usage, and task IDs. Proposal inspect may render hypothesis and expected-evidence names through
bounded text but not raw argv. Experiment inspect renders argv digest, not argv, plus submission/task
identity and lineage IDs.

- [ ] **Step 6: Run CLI GREEN**

```bash
cargo test --test cli_help campaign_command -- --nocapture --test-threads=1
cargo test --test operator_commands campaign_ -- --nocapture --test-threads=1
cargo test --test operator_commands proposal_ -- --nocapture --test-threads=1
cargo test --test operator_commands experiment_ -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 7: Review and commit**

```bash
git add src/cli.rs src/main.rs src/campaign.rs src/db/campaigns.rs src/output.rs tests/integration/cli_help.rs tests/integration/operator_commands.rs
git commit -m "feat: add campaign inspection commands"
```

---

### Task 8: Restart recovery, budget wake, status, and doctor

**Files:**
- Modify: `src/db/campaigns.rs`
- Modify: `src/daemon.rs`
- Modify: `src/reconcile.rs`
- Modify: `src/scheduler.rs`
- Modify: `src/status.rs`
- Modify: `src/diagnostics.rs`
- Test: `tests/integration/daemon.rs`
- Test: `tests/integration/reconciliation.rs`
- Test: `tests/integration/scheduler.rs`
- Test: `tests/integration/diagnostics.rs`

**Interfaces:**
- Produces `CampaignRepository::recover_submission_boundaries(now)`.
- Produces `CampaignRepository::list_reserved_submission_intents(limit)`.
- Produces `CampaignRepository::wake_eligible_campaigns(now)`.
- Produces `CampaignRepository::reserve_agent_decision(campaign_id, decision_key, limits, now)`.
- Daemon runs recovery before scheduling work.
- Status/doctor use read-only bounded repository queries.

- [ ] **Step 1: Write restart RED tests**

Create durable rows at `reserved`, `submitting`, `unreconciled`, and accepted states, restart daemon,
and assert: reserved remains resumable, submitting becomes unreconciled, unreconciled is not re-added,
accepted is reconciled only by exact task signature, and no duplicate experiment appears.
Extend the existing `DaemonHarness` with repository-backed state constructors, a fake Pueue call
counter, and a restart operation that recreates the daemon while preserving the same database.

```rust
#[tokio::test]
async fn startup_quarantines_submitting_without_readding() {
    let mut harness = DaemonHarness::with_campaign_experiment("submitting");
    harness.restart().await.unwrap();
    assert_eq!(harness.experiment_state(), "unreconciled");
    assert_eq!(harness.pueue_add_calls(), 0);
}
```

- [ ] **Step 2: Run RED**

```bash
cargo test --test daemon campaign_recovery -- --nocapture --test-threads=1
cargo test --test reconciliation campaign_unreconciled -- --nocapture --test-threads=1
```

Expected: campaign rows are ignored by startup and submitting remains ambiguous.

- [ ] **Step 3: Implement startup recovery**

Before normal event scheduling, transactionally change stale `submitting` to `unreconciled` while
retaining reservations. Then list reserved intents in bounded ID order and pass each through the same
`CampaignCoordinator::submit_accepted_intent` used by first submit; reserved is the only state that
may begin a new add. Reconstruct the explicit working directory from the verified project root and
stored normalized relative directory. For accepted experiments, reconcile only a unique Pueue task
matching stored task ID/signature. Never infer a task from command text alone. Keep zero/multiple
candidates unreconciled.

- [ ] **Step 4: Implement rolling wake**

For `budget_waiting` campaigns with `next_eligible_at <= now`, recalculate all active/reserved window
usage in one IMMEDIATE transaction. Transition to active only if every exceeded dimension is now
available; otherwise move `next_eligible_at` to the earliest exact expiry. Phase 1 only makes the
durable state runnable again; the Phase 2 idle watchdog creates the next decision event.

- [ ] **Step 5: Enforce the hourly agent-run limit atomically**

Before `AgentRunner::spawn`, derive `decision_key` from the sorted event IDs plus their persisted
attempt number. In one IMMEDIATE transaction, `reserve_agent_decision` returns the existing exact
reservation for a crash retry, or counts consumed/reserved `agent_run` reservations in the preceding
hour and inserts one consumed reservation. At the limit it changes the live campaign to
`budget_waiting`, stores the earliest reservation expiry, and returns that time without creating an
agent run. Scheduler defers the claimed events to that time. This conservative budget counts a
pre-binding launch attempt as an agent run and therefore cannot undercount or exceed the hard cap.

Add a two-connection race test asserting six allowed unique decision keys and a seventh budget wait,
plus a same-key crash retry test asserting one reservation and no double charge.

- [ ] **Step 6: Add status projections**

Extend human, compact, and JSON status with campaign state/reason, experiment counts, rolling usage,
next eligible time, unreconciled count, and objective digest. Keep output absent for projects without
a campaign so legacy status shape remains compatible where possible.

- [ ] **Step 7: Add doctor invariant checks**

Add read-only checks for one live campaign, baseline linkage, orphan reservation, experiment/
submission task disagreement, submitting/unreconciled state, budget wait without a finite wake time,
and on-disk STATE.md digest drift. Digest drift is a warning and never rewrites the active snapshot.

- [ ] **Step 8: Run focused GREEN**

```bash
cargo test --test daemon campaign_ -- --nocapture --test-threads=1
cargo test --test reconciliation campaign_ -- --nocapture --test-threads=1
cargo test --test scheduler campaign_agent_budget -- --nocapture --test-threads=1
cargo test --test diagnostics campaign_ -- --nocapture --test-threads=1
cargo test --test daemon -- --nocapture --test-threads=1
cargo check --all-targets
git diff --check
```

- [ ] **Step 9: Review and commit**

```bash
git add src/db/campaigns.rs src/daemon.rs src/reconcile.rs src/scheduler.rs src/status.rs src/diagnostics.rs tests/integration/daemon.rs tests/integration/reconciliation.rs tests/integration/scheduler.rs tests/integration/diagnostics.rs
git commit -m "feat: recover and diagnose campaign intents"
```

---

### Task 9: User documentation and Linux real-Pueue acceptance

**Files:**
- Modify: `README.md`
- Modify: `docs/getting-started-ja.md`
- Modify: `docs/commands-ja.md`
- Modify: `docs/workflows-ja.md`
- Modify: `docs/architecture-ja.md`
- Modify: `docs/troubleshooting-ja.md`
- Modify: `tests/e2e/rust_supervisor.sh`
- Modify: `tests/test_shell_entrypoints.bats`
- Test: `tests/integration/init.rs`
- Test: `tests/integration/cli_help.rs`

**Interfaces:**
- Documents the four-command zero-adapter start and Phase 1 limitation: autonomous next-proposal generation is delivered by Phase 2.
- Pins Linux support, network/credential separation, objective immutability, direct-submit rejection, and unreconciled recovery.
- Extends the existing isolated real-pueued acceptance harness instead of creating a second controller.

- [ ] **Step 1: Write documentation contract RED tests**

Assert README/getting-started contain the exact quick start, commands guide contains every new Clap
subcommand, workflows explains retire/new objective/new submit, architecture contains durable intent
ordering, and troubleshooting contains `unreconciled` with no “retry submit” advice.

- [ ] **Step 2: Run documentation RED**

```bash
cargo test --test cli_help campaign_documentation -- --nocapture --test-threads=1
cargo test --test init objective_template -- --nocapture --test-threads=1
```

Expected: new command and workflow strings are absent.

- [ ] **Step 3: Extend the real-Pueue harness**

In the existing isolated Linux fixture, write a real goal into STATE.md, run the first default submit,
and assert via SQLite and Pueue:

- exactly one campaign, baseline proposal, experiment, reservation, submission, and task;
- a second direct submit and batch create no row/task;
- restart at reserved/submitting/accepted boundaries creates no duplicate;
- Pueue add uncertainty stays unreconciled;
- budget waiting has a finite next wake and becomes active after the window is advanced;
- network mode is enabled in policy while fake task capture has no credential values.

Use SQL counts and unique IDs, not sleeps alone. Continue to use the isolated real `pueued` and its
dedicated config/socket from `rust_supervisor.sh`.

- [ ] **Step 4: Update the user and architecture docs**

Document:

```bash
pueue-agent init
# edit .pueue-agent/STATE.md
pueue-agent enable
pueue-agent submit -- python train.py
```

Explain that Phase 1 safely establishes the campaign/baseline/control plane; the completion-driven
autonomous proposal loop, periodic observer, goal evaluation, and code worktrees are subsequent
phases and must not be advertised as implemented until their acceptance tests pass.

- [ ] **Step 5: Run final Phase 1 verification**

```bash
cargo fmt --all -- --check
cargo check --all-targets
cargo check --release --all-targets
cargo test --all-targets -- --test-threads=1
bash -n install.sh bin/pueue-agent tests/e2e/run.sh tests/e2e/rust_supervisor.sh
bats tests/test_shell_entrypoints.bats
tests/e2e/run.sh
git diff --check
```

Expected: every command exits 0 on the supported Linux runner. A host that stalls before a Rust test
harness starts is inconclusive and cannot be reported as GREEN.

- [ ] **Step 6: Review Phase 1 against the design**

Confirm objective immutability, service limit ownership, one live campaign, one baseline, atomic
reservation, external-side-effect unknown quarantine, direct-submit rejection, bounded output,
restart uniqueness, and Linux real-Pueue evidence. Confirm no observer/goal/code-worktree claim was
added to user docs.

- [ ] **Step 7: Commit documentation and acceptance**

```bash
git add README.md docs/getting-started-ja.md docs/commands-ja.md docs/workflows-ja.md docs/architecture-ja.md docs/troubleshooting-ja.md tests/e2e/rust_supervisor.sh tests/test_shell_entrypoints.bats tests/integration/init.rs tests/integration/cli_help.rs
git commit -m "docs: explain managed campaign startup"
```

---

## Phase 1 completion gate

Phase 1 is complete only when all nine task commits exist, supported Linux verification is GREEN,
the real-Pueue test proves no duplicate external add across restart boundaries, and an independent
review finds no Critical or Important violation of the design invariants.

After that gate, write separate implementation plans in this order:

1. Autonomous completion loop: evidence collection, analysis agent structured output, coordinator,
   `decision_missing`, idle watchdog.
2. Running health and periodic observer: health state, resumable per-experiment session, diagnosis,
   cancel/terminal proof, finite repair.
3. Evaluation and goal review: discovered evidence, optional manifest, baseline/current-best,
   plateau strategy, `goal_reached_pending_review`.
4. Isolated code changes: managed worktree, required tests, candidate commit, immutable experiment SHA,
   no main merge.

Do not pre-create the later phase tables, CLI flags, or unused abstractions in Phase 1 beyond the
explicit schema and interfaces in this plan.
