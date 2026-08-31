# Phase 5 Isolated Code Changes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn a validated `code_change` campaign decision into an isolated, checked candidate commit and a Pueue experiment pinned to that commit without changing `main`, the original checkout, or a remote.

**Architecture:** Schema v26 connects the existing pending code-change proposal and rolling budget to a durable coordinator. One new source module owns private worktree, editor-protocol, check, Git-ref, and recovery components; one new DB module owns all lifecycle transitions. Existing verified process/agent launch, campaign submission, Phase 3 health, and Phase 4 evaluation paths are extended only where a candidate root or revision must be carried.

**Tech Stack:** Rust 2021 / Tokio / rusqlite / serde_json / existing verified native launcher and process-group lifecycle / startup-anchored Git, Cargo, uv, and Python tools / Pueue 4 / Bats + Linux real-Pueue E2E on roko.

**Spec:** `docs/superpowers/specs/2026-09-01-phase5-isolated-code-changes-design.md`

## Global Constraints

- Linux is the supported runtime/security target; roko is a test host only.
- `main`, the original working tree, unrelated worktrees, remote refs, and remote configuration stay unchanged.
- Never run an editor or candidate check as root.
- Git, checks, agents, and Pueue receive validated argv arrays; no shell command strings.
- One fresh editor session per proposal and one same-session correction: two editor attempts total.
- `git diff --check` plus at least one project-specific check must pass against the final diff digest.
- Shipped hard bounds: 50 changed files, 500000 diff bytes, 8 project checks, 30 minutes per check, 64 KiB combined check output.
- Network is enabled by default. Credentials, SSH agent variables, Git authentication/signing variables, and ambient secrets stay excluded.
- Code-change budget is consumed at proposal acceptance; every editor start consumes agent-run budget; candidate submission consumes experiment budget.
- Persist bounded summaries/digests only, never raw diffs, child output, prompts, conversations, or credential values.
- Built-in Codex and enrolled custom agents share one editor JSON protocol. Custom executables are trusted until Phase 6 adds OS/container containment.
- Existing non-code proposals and campaigns whose `base_revision_sha` is NULL keep working unchanged.
- Product implementation and required code reviews use Luna at maximum reasoning, as requested by the user.
- Follow `AGENTS.md`: smallest complete diff, existing patterns, no unrelated refactor.

## File and responsibility map

Only two production modules are created:

- `src/code_change.rs`: private `WorktreeManager`, `EditorProtocol`, `CandidateValidator`, `CheckRunner`, `CandidateRepository`, and public `CodeChangeCoordinator`; inline unit tests cover pure parsing and validation.
- `src/db/code_changes.rs`: typed reads and transactional Phase 5 state transitions.

Existing files change only at required integration boundaries:

- `src/db/migrations.rs`, `src/models.rs`, `src/db/campaigns.rs`, `src/db/mod.rs`: schema/model/campaign lineage.
- `src/execution_policy.rs`, `src/process.rs`, `src/native_launcher.rs`, `src/environment.rs`: pinned tools, candidate-root execution, bounded process ownership.
- `src/agent.rs`, `src/codex_command.rs`: dedicated editor role, private output, session resume.
- `src/decision_protocol.rs`, `src/decision.rs`, `src/campaign.rs`, `src/submit.rs`, `src/main.rs`: code-change decision admission, production baseline policy wiring, and candidate submission.
- `src/daemon.rs`, `src/reconcile.rs`, `src/promotion.rs`: durable advancement, terminal verification, promotion, cleanup.
- `src/status.rs`, `src/output.rs`, `src/diagnostics.rs`: bounded visibility.
- Existing integration targets receive tests; `Cargo.toml` and test topology do not change.

---

### Task 1: Add schema v26 and typed lifecycle persistence

**Files:**
- Modify: `src/db/migrations.rs`
- Create: `src/db/code_changes.rs`
- Modify: `src/db/mod.rs`
- Modify: `src/models.rs`
- Modify: `src/db/campaigns.rs`
- Modify: `src/events.rs`
- Modify: `src/scheduler.rs`
- Modify: `src/db/repositories.rs`
- Modify: `tests/integration/database.rs`

**Interfaces:**
- Produces `CodeChangeState`, `CodeChangeRun`, `CodeChangeEditorAttempt`, `CodeChangeCheck`, `CodeChangeCheckStatus`, and the bounded persisted promotion fields `promotion_outcome`, `promotion_expected_best_experiment_id`, `promotion_expected_old_sha`, and `promotion_target_sha`.
- Adds nullable `Campaign.base_revision_sha`, `Experiment.code_change_run_id`, and `Experiment.code_revision_sha`.
- Adds `EventKind::CodeChange`; migration rebuilds the canonical `events` CHECK without losing v25 rows. Code-change transitions emit bounded completed audit events, and editor launches use a coordinator-claimed event of the same kind.
- Produces `CodeChangeRepository` methods:
  - `create_pending(&NewCodeChangeRun) -> Result<CodeChangeRun, AppError>`
  - `find_by_id(&str) -> Result<Option<CodeChangeRun>, AppError>`
  - `find_by_proposal(&str) -> Result<Option<CodeChangeRun>, AppError>`
  - `list_recoverable(usize) -> Result<Vec<CodeChangeRun>, AppError>`
  - `transition(&str, CodeChangeState, CodeChangeState, i64) -> Result<CodeChangeRun, AppError>`
  - `reserve_editor_attempt`, `finish_editor_attempt`, `replace_attempt_checks`, `finish_check`
  - `record_candidate`, `bind_experiment`, `record_evaluation`, `reject`, `require_recovery`, `finish_cleanup`.
- Adds `CampaignRepository::start_with_baseline_at_revision(request, limits, Option<&str>)`; existing `start_with_baseline` delegates with `None` so existing fixtures remain focused.

- [ ] **Step 1: Write the failing v26 schema test**

Append to `tests/integration/database.rs`:

```rust
#[test]
fn schema_v26_adds_isolated_code_change_state() {
    let temporary = tempfile::tempdir().unwrap();
    let db = Db::open(&temporary.path().join("state.sqlite3")).unwrap();
    let connection = db.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 26);
    for table in [
        "code_change_runs",
        "code_change_editor_attempts",
        "code_change_checks",
    ] {
        let present: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [table],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(present, 1, "{table}");
    }
    for (table, column) in [
        ("campaigns", "base_revision_sha"),
        ("experiments", "code_change_run_id"),
        ("experiments", "code_revision_sha"),
        ("code_change_runs", "promotion_outcome"),
        ("code_change_runs", "promotion_expected_best_experiment_id"),
        ("code_change_runs", "promotion_expected_old_sha"),
        ("code_change_runs", "promotion_target_sha"),
    ] {
        let present: i64 = connection
            .query_row(
                &format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name=?1"),
                [column],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(present, 1, "{table}.{column}");
    }
}
```

Add a migration test that opens an actual v25 fixture, inserts an ordinary campaign/experiment/event, migrates, and asserts every existing ID/value survives while new columns remain NULL.

- [ ] **Step 2: Run RED**

```bash
cargo test --test database schema_v26_adds_isolated_code_change_state -- --exact --test-threads=1
```

Expected: FAIL because `LATEST_SCHEMA_VERSION` is 25.

- [ ] **Step 3: Implement canonical schema v26**

Use the approved tables and this exact state enum shape:

```rust
database_enum!(CodeChangeState {
    Reserved => "reserved",
    PreparingWorktree => "preparing_worktree",
    Editing => "editing",
    Checking => "checking",
    Committing => "committing",
    CandidateReady => "candidate_ready",
    ExperimentSubmitted => "experiment_submitted",
    Evaluated => "evaluated",
    CleanupPending => "cleanup_pending",
    Completed => "completed",
    Rejected => "rejected",
    RecoveryRequired => "recovery_required",
});
```

The one-live partial index excludes only `completed` and `rejected`; a `recovery_required` row blocks another code-change pipeline. The verifier checks tables, columns, foreign keys, unique/partial indexes, CHECK SQL, and extended event kind before setting `PRAGMA user_version = 26`.

Extend exhaustive event classification so `CodeChange` is coordinator-owned:
the legacy scheduler never turns it into a Standard/Decision run, incident
counting does not classify it as a task failure, and completed transition
events remain queryable audit facts. Keep only `created_at`, `updated_at`, and
`cleanup_completed_at` on the run; attempt/check tables retain their own
start/finish times and bounded completed `CodeChange` events provide transition
history.

- [ ] **Step 4: Write the failing compare-and-set repository test**

Extend the existing pending-code-change database fixture. Create one run, transition `reserved -> preparing_worktree`, then attempt `reserved -> editing` and assert the second transition fails. Reopen `Db`, call `list_recoverable(10)`, and assert the preparing row remains first in stable `updated_at, code_change_run_id` order.

- [ ] **Step 5: Implement transactional repository methods**

Every transition uses an immediate transaction and exactly one affected row:

```rust
let changed = transaction.execute(
    "UPDATE code_change_runs
     SET state=?1, updated_at=?2
     WHERE code_change_run_id=?3 AND state=?4",
    rusqlite::params![next, now, run_id, expected],
).map_err(database_error("transition code-change run"))?;
if changed != 1 {
    return Err(AppError::Validation {
        field: "code_change.state",
        message: "changed concurrently or does not match the expected state",
    });
}
```

Apply existing bounded JSON/text helpers. Do not persist changed path lists, raw output, or raw diff. Extend `CAMPAIGN_SELECT`, `EXPERIMENT_SELECT`, row decoders, and experiment insertion once; keep legacy wrappers nullable.

Every successful lifecycle mutation inserts one idempotent completed
`CodeChange` event in the same transaction. Its dedup key is
`code-change:v1:<run-id>:<state>:<attempt-or-zero>` and its payload contains
only run/campaign/proposal IDs, state, attempt, bounded reason code, and event
time. It contains no SHA, diff/check output, prompt, argv, or environment.

- [ ] **Step 6: Run GREEN and database regressions**

```bash
cargo test --test database schema_v26 -- --test-threads=1
cargo test --test database pending_code_change -- --test-threads=1
cargo test --test database -- --test-threads=1
git diff --check
```

- [ ] **Step 7: Commit**

```bash
git add src/db/migrations.rs src/db/code_changes.rs src/db/mod.rs src/models.rs src/db/campaigns.rs src/events.rs src/scheduler.rs src/db/repositories.rs tests/integration/database.rs
git commit -m "feat: add durable code change state"
```

---

### Task 2: Add service policy, accept decisions, pin campaign base, and validate pure protocol data

**Files:**
- Create: `src/code_change.rs`
- Modify: `src/lib.rs`
- Modify: `src/execution_policy.rs`
- Modify: `src/decision_protocol.rs`
- Modify: `src/agent.rs`
- Modify: `src/decision.rs`
- Modify: `src/campaign.rs`
- Modify: `src/submit.rs`
- Modify: `src/main.rs`
- Modify: `src/db/campaigns.rs`
- Modify: `tests/support/execution_policy_fixture.rs`
- Modify: `tests/integration/execution_policy.rs`
- Modify: `tests/integration/pueue_adapter.rs`

**Interfaces:**
- `ResolvedExecutionPolicy` retains a private `ServiceStateRootAnchor { canonical_path, identity, resolution_fingerprint, directory: Arc<File> }` and optional startup-resolved anchors for Git, Cargo, uv, and Python.
- Produces `CodeChangeTool::{Git,Cargo,Uv,Python}` plus `ResolvedExecutionPolicy::code_change_tool(CodeChangeTool) -> Option<&ExecutableAnchor>` and the convenience `code_change_git_anchor()` used by admission.
- `CampaignLimits` gains `max_code_change_changed_files`, `max_code_change_diff_bytes`, `max_code_change_checks`, `code_change_check_timeout_minutes`.
- `CampaignCoordinator::with_execution_policy(&ResolvedExecutionPolicy)` stores a borrow for pinned baseline/admission Git operations; `DecisionCoordinator` calls it whenever its existing policy is present.
- `code_change.rs` initially produces pure helpers for full SHA, internal ref, owned relative path, editor JSON, proposed checks, discovery, protected paths, and diff limits.
- `CampaignProposalAdmission::{Experiment(AdmittedCampaignProposal),CodeChange(CodeChangeRun),CodeChangeRejected(Proposal),Deferred}` replaces `Option<AdmittedCampaignProposal>` at the sole `DecisionCoordinator` consumer.

- [ ] **Step 1: Write failing policy tests**

Assert shipped values and optional anchors:

```rust
assert_eq!(policy.campaign_limits.max_code_change_changed_files, 50);
assert_eq!(policy.campaign_limits.max_code_change_diff_bytes, 500_000);
assert_eq!(policy.campaign_limits.max_code_change_checks, 8);
assert_eq!(policy.campaign_limits.code_change_check_timeout_minutes, 30);
assert!(policy.code_change_git_anchor().is_some());
```

Add fixtures with only Git present, with Git absent, and with each bound outside its accepted range. Policy load succeeds when Cargo/uv/Python are absent, but Git-backed code-change admission later fails closed when Git is absent.

- [ ] **Step 2: Implement defaults and startup tool resolution**

Add these service-owned defaults:

```toml
max_code_change_changed_files = 50
max_code_change_diff_bytes = 500000
max_code_change_checks = 8
code_change_check_timeout_minutes = 30
```

Extend `[executables]` with `git`, `cargo`, `uv`, and `python`. Resolve optional tools only through the already opened trusted-PATH descriptors during policy load. A missing optional tool is stored as absent and is never looked up from ambient PATH later. Move the already opened state-directory path, identity, resolution fingerprint, and descriptor into `ResolvedExecutionPolicy::state_root`; update every fixture constructor explicitly rather than reopening the state path in tests.

Add a code-change preflight that rejects effective UID 0 before any editor,
check, or Git mutation. The ordinary non-code service remains startable so an
operator can inspect and retire campaigns even when this code-change preflight
fails. Keep the UID classification as a pure helper and test `0 -> reject`,
nonzero -> continue without changing the process identity during tests.

- [ ] **Step 3: Write failing decision/protocol tests**

Replace the current hard rejection with:

```rust
#[test]
fn decision_protocol_accepts_bounded_code_change() {
    let bytes = br#"{"schema_version":1,"decision":"proposal","proposal":{"kind":"code_change","hypothesis":"reduce allocator pressure","source_experiment_id":"exp-1","argv":["python","train.py"],"working_directory":".","expected_evidence":["lower peak memory"]}}"#;
    let decision = parse_and_validate_decision(bytes, "objective", CampaignLimits::default()).unwrap();
    let ValidatedDecision::Proposal(proposal) = decision else {
        panic!("expected proposal");
    };
    assert_eq!(proposal.kind(), ProposalKind::CodeChange);
}
```

In `code_change.rs` add inline RED tests for: canonical lowercase full SHA, ref derivation, worktree relative path, editor unknown/oversized fields, shell argv, absolute/traversing cwd, too many checks, deterministic Cargo/Python discovery, protected paths, file/diff caps, and at least one project check.

- [ ] **Step 4: Implement pure validation in one module**

Use private component structs inside `code_change.rs`; do not split more files. Parse editor output with deny-unknown-fields serde and explicit byte/count/path/tool checks. Initial discovery produces only:

```rust
const RUST_CHECK: &[&str] = &["cargo", "test", "--all-targets", "--", "--test-threads=1"];
const UV_PYTEST_CHECK: &[&str] = &["uv", "run", "pytest"];
const PYTHON_PYTEST_CHECK: &[&str] = &["python", "-m", "pytest"];
```

`pytest.ini` or parsed `[tool.pytest.ini_options]` establishes pytest; `uv.lock` selects uv. Repository text may select a profile but never inject argv.

- [ ] **Step 5: Write failing production-start and admission-ownership tests**

Add focused integration tests named
`experiment_submit_with_root_anchor_persists_clean_head`,
`code_change_admission_releases_lock_without_pueue_add`,
`experiment_admission_holds_lock_until_pueue_add`, and
`duplicate_code_change_digest_reuses_durable_outcome`. Drive the real public
path `submit::run -> run_with_options_with_root_anchor ->
run_with_options_inner ->
CampaignCoordinator::start_baseline`; assert that the clean full `HEAD` is
stored, a dirty/non-Git project stores NULL and still submits, and no test-only
coordinator construction is needed.

```bash
cargo test --test pueue_adapter experiment_submit_with_root_anchor_persists_clean_head -- --exact --test-threads=1
cargo test --test pueue_adapter code_change_admission -- --test-threads=1
```

Expected: FAIL because `src/submit.rs` does not pass the resolved execution
policy and `admit_proposal` still returns `Option<AdmittedCampaignProposal>`.

- [ ] **Step 6: Capture campaign base and make admission ownership explicit**

Add `CampaignCoordinator::with_execution_policy(&ResolvedExecutionPolicy)`. Extend `submit::run_with_options_with_root_anchor` with an `Arc<ResolvedExecutionPolicy>` argument and pass it into `run_with_options_inner` as `Some(policy)`; the fixture-oriented `run_with_options` wrapper passes `None` and retains legacy NULL-base behavior. Update both production callers, `submit::run` and `main::submit`, to pass the already loaded `Arc` policy. In the experiment branch, call both `.with_root_anchor(root_anchor)` and `.with_execution_policy(policy.as_ref())` before `start_baseline`; in `DecisionCoordinator`, add `.with_execution_policy(policy)` beside the existing root-anchor binding. Production campaign start inspects exact `HEAD^{commit}` and `git status --porcelain=v1 -z` through the pinned Git anchor. Clean Git stores the canonical full SHA via `start_with_baseline_at_revision`; dirty/non-Git/Git-absent stores NULL and still submits the baseline.

Remove both `code_change decisions are not permitted` errors and include `code_change` in `DECISION_OUTPUT_SCHEMA`. Before the admission transaction, resolve the exact local `campaign/<campaign-id>/best` commit when that ref exists; otherwise use the persisted campaign-start `base_revision_sha`. Reverify that full SHA while preparing the worktree.

Code-change admission requires active campaign, terminal same-campaign source, no live code-change row, and current code-change budget. In one transaction it consumes the existing code-change reservation exactly once. With a usable base it inserts the pending proposal and `reserved` run; it creates no experiment/submission/Pueue task. With NULL/invalid base or missing Git it inserts a rejected proposal plus a completed audit event keyed by proposal ID, returns `CodeChangeRejected`, and still retains the consumed rolling slot.

Use these ownership rules at every return site:

```rust
enum CampaignProposalAdmission {
    Experiment(AdmittedCampaignProposal), // owns CampaignAdmission until add
    CodeChange(CodeChangeRun),             // transaction committed; lock dropped
    CodeChangeRejected(Proposal),          // consumed rejection committed; lock dropped
    Deferred,                              // no durable acceptance; lock dropped
}
```

`DecisionCoordinator` exhaustively handles all four variants. It calls
`submit_admitted_proposal` only for `Experiment`, marks the decision attempt
completed for both durable code-change variants, and leaves `Deferred`
eligible for the existing wake path. A repeated proposal/digest returns the
already committed `CodeChange` or `CodeChangeRejected` result without consuming
another code-change slot, inserting another run, or calling Pueue. A proposal
ID whose digest differs is rejected as a durable identity conflict.

- [ ] **Step 7: Run GREEN and non-code regressions**

```bash
cargo test --lib code_change::tests -- --test-threads=1
cargo test --lib decision_protocol:: -- --test-threads=1
cargo test --test execution_policy code_change -- --test-threads=1
cargo test --test pueue_adapter experiment_submit_with_root_anchor_persists_clean_head -- --exact --test-threads=1
cargo test --test pueue_adapter code_change -- --test-threads=1
cargo test --test pueue_adapter -- --test-threads=1
git diff --check
```

- [ ] **Step 8: Commit**

```bash
git add src/code_change.rs src/lib.rs src/execution_policy.rs src/decision_protocol.rs src/agent.rs src/decision.rs src/campaign.rs src/submit.rs src/main.rs src/db/campaigns.rs tests/support/execution_policy_fixture.rs tests/integration/execution_policy.rs tests/integration/pueue_adapter.rs
git commit -m "feat: admit validated code changes"
```

---

### Task 3: Add verified Git/worktree execution and owned cleanup

**Files:**
- Modify: `src/code_change.rs`
- Modify: `src/process.rs`
- Modify: `src/pueue_process.rs`
- Modify: `src/native_launcher.rs`
- Modify: `src/environment.rs`
- Modify: `src/execution_policy.rs`
- Modify: `tests/integration/execution_policy.rs`
- Modify: `tests/integration/native_launcher.rs`
- Modify: `tests/integration/codex_security.rs`
- Modify: `tests/integration/daemon.rs`

**Interfaces:**
- Private `WorktreeManager` owns only `inspect_base`, `prepare`, `verify`, and `cleanup`; it is the sole creator/remover of Phase 5 worktrees.
- Private `CandidateValidator` owns protected-path, repository/ref/remote, diff-bound, and final-tree validation.
- Private `CandidateRepository` owns `diff_facts`, temporary-index tree construction, `commit_candidate`, `ensure_candidate_ref`, `verify_candidate_ref`, and `update_best_ref_cas`; it is the sole commit and `refs/heads/campaign/<campaign-id>/` mutator.
- Private `CheckRunner` owns project-check execution; its private `BoundedToolRunner` reuses `VerifiedCommandSpec`, startup anchors, process-group requirement, retained ownership, timeout, and caller-selected output cap.
- `ResolvedExecutionPolicy::for_code_change_worktree(&self, project: &Project, original: &ResolvedProjectExecutionPolicy, candidate: &VerifiedProjectRoot) -> Result<ResolvedProjectExecutionPolicy, PolicyViolation>` proves the candidate is below the retained state-root `worktrees` directory, revalidates both descriptors/identities, preserves agent/tool/network/environment authority, and replaces only `root_anchor`.
- `VerifiedWorkingDirectory::open_descendant(&VerifiedProjectRoot, &Path) -> Result<VerifiedWorkingDirectory, PolicyViolation>` opens normalized relative components descriptor-by-descriptor with `O_NOFOLLOW`; `VerifiedCommandSpec` carries this descriptor instead of trusting a cwd string.
- `DiffFacts` contains sorted transient paths, file count, diff bytes, tree SHA, and SHA-256 digest; only counts/digest/tree are persisted.

- [ ] **Step 1: Write failing worktree lifecycle tests**

In `tests/integration/daemon.rs`, add a disposable Git repository helper that creates branch `main`, one commit, and one tracked `model.py`. Test preparation/edit/commit/ref/cleanup and assert:

```rust
assert_eq!(fixture.rev_parse("refs/heads/main"), original_main);
assert_eq!(fixture.read_original("model.py"), "score = 1\n");
assert_eq!(fixture.rev_parse(&candidate_ref), candidate_sha);
assert_eq!(fixture.remote_config_digest(), original_remote_digest);
```

Add exact failures for dirty base, non-Git base, service-root symlink replacement, protected-ref mismatch, remote-config change, `.git`/`.pueue-agent`/credential path, submodule/nested repo, excess files/bytes, and cleanup ownership mismatch. Add focused security tests named `code_change_valid_nested_cwd_is_descriptor_verified`, `code_change_sibling_cwd_is_rejected`, `code_change_symlink_descendant_is_rejected`, and `code_change_replaced_candidate_root_is_rejected`.

- [ ] **Step 2: Run RED**

```bash
cargo test --test daemon code_change_worktree -- --test-threads=1
cargo test --test execution_policy code_change_worktree_root -- --test-threads=1
cargo test --test native_launcher code_change_cwd -- --test-threads=1
```

- [ ] **Step 3: Minimally extend existing verified execution**

Reuse `VerifiedCommandSpec` and `spawn_verified_command_before_classified`; add only the candidate-root/captured-output support missing from the current API. Replace `VerifiedCommandSpec.cwd: Option<PathBuf>` with `working_directory: Option<VerifiedWorkingDirectory>`. Agent/tool mode requires both a verified project root and a working-directory descriptor that is that root or a verified descendant; Pueue-control mode continues to carry neither. Extend the native control frame with a working-directory identity/descriptor right and have the helper call `fchdir` on that descriptor. It must never compare a nested cwd by pathname or reopen it in the child.

Keep `NativeLaunchSpec.cwd` source-compatible for existing agent callers, but
consume it inside `NativeLauncher::spawn_verified`: require it to strip exactly
under the verified project-root canonical path, convert the remainder with
`VerifiedWorkingDirectory::open_descendant`, and pass only that descriptor to
`VerifiedCommandSpec`. A root-equal cwd becomes a clone of the verified root
descriptor. No unverified cwd reaches the native control frame.

The four valid launch shapes are explicit:

```rust
match (&spec.project_root, &spec.working_directory, &private_temp, &spec.child_io) {
    (Some(_), Some(_), Some(_), VerifiedChildIo::AgentLog { .. }) => LaunchMode::Agent,
    (Some(_), Some(_), None, VerifiedChildIo::Capture) => LaunchMode::OwnedTool,
    (None, None, None, VerifiedChildIo::Capture) if spec.pueue_config.is_some() => LaunchMode::Pueue,
    _ => return Err(native_gate_error(PolicyViolationStage::NativeGate).into()),
}
```

A tool invocation verifies its startup executable anchor immediately before
spawn, requires a process group, starts suspended, uses sanitized env, and
retains cleanup when timeout/cancellation/exec acknowledgement is uncertain.
Never call `std::process::Command` directly from `code_change.rs`. The caller
selects one service-owned cap: 1 MiB for internal Git metadata/diff parsing and
64 KiB combined output for project checks.

The private result may hold bounded child bytes only until the owning operation
parses or hashes them:

```rust
struct BoundedToolOutput {
    success: bool,
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    output_digest: String,
    summary: &'static str,
}
```

No caller copies `stdout`/`stderr` into a model, SQLite row, event, diagnostic,
status projection, or user-facing error. Git parsing consumes at most 1 MiB in
memory; check execution hashes and drops at most 64 KiB.

- [ ] **Step 4: Implement owned Git lifecycle**

Use pinned Git with `core.hooksPath=/dev/null`, no pager, no global/system config, no signing, and sanitized auth variables. `WorktreeManager` creates detached worktrees only under `<state-dir>/worktrees/<campaign-id>/<proposal-id>`, returns a `VerifiedProjectRoot`, and removes only that DB-derived target. `CandidateValidator` validates no-follow containment, repository common-dir identity, exact base/HEAD, protected refs, and remote digest before/after every editor/tool boundary. Never call `git worktree prune`.

Enumerate tracked and untracked changes with NUL-delimited status output; plain
`git diff` alone must not omit a new file. Build the candidate tree through a
supervisor-owned temporary index under the owned state directory:

```text
GIT_INDEX_FILE=<owned-temp-index> git read-tree <base-sha>
GIT_INDEX_FILE=<owned-temp-index> git add -A -- <validated-paths>
GIT_INDEX_FILE=<owned-temp-index> git write-tree
git diff-tree --binary <base-sha> <tree-sha>
```

The bounded binary diff supplies byte count/digest; before commit,
`CandidateRepository` rebuilds the temporary index and requires the same
tree/diff digest that passed checks. It uses fixed pueue-agent
author/committer identity with `git commit-tree <tree-sha> -p <base-sha>`,
verifies parent/tree, updates the candidate ref conditionally, then moves only
the owned detached worktree to the candidate SHA and verifies it is clean.
Create refs only with `git update-ref <ref> <new> <expected-old-or-zero>`.
Clean the temporary index through its retained owner descriptor on every
outcome.

- [ ] **Step 5: Implement descriptor-owned cleanup**

Verify DB ownership, exact state-root relative path, repository identity, no live experiment/process, and expected candidate HEAD before `git worktree remove --force <exact-path>`. Reopen the parent descriptor and verify absence. Missing target after verified ownership is idempotent success; a replacement/mismatch becomes `recovery_required` and is never recursively deleted.

- [ ] **Step 6: Run GREEN and process/security regressions**

```bash
cargo test --test daemon code_change_worktree -- --test-threads=1
cargo test --test execution_policy code_change_worktree_root -- --test-threads=1
cargo test --test native_launcher -- --test-threads=1
cargo test --test native_launcher code_change_cwd -- --test-threads=1
cargo test --test codex_security code_change_tool -- --test-threads=1
cargo test process:: -- --test-threads=1
git diff --check
```

- [ ] **Step 7: Commit**

```bash
git add src/code_change.rs src/process.rs src/pueue_process.rs src/native_launcher.rs src/environment.rs src/execution_policy.rs tests/integration/execution_policy.rs tests/integration/native_launcher.rs tests/integration/codex_security.rs tests/integration/daemon.rs
git commit -m "feat: manage verified candidate worktrees"
```

---

### Task 4: Run dedicated editors and persist one same-session correction

**Files:**
- Modify: `src/models.rs`
- Modify: `src/code_change.rs`
- Modify: `src/agent.rs`
- Modify: `src/codex_command.rs`
- Modify: `src/environment.rs`
- Modify: `src/db/code_changes.rs`
- Modify: `src/db/repositories.rs`
- Modify: `src/db/campaigns.rs`
- Modify: `src/daemon.rs`
- Modify: `tests/support/fake_codex.sh`
- Modify: `tests/support/fake_agent.sh`
- Modify: `tests/integration/codex_security.rs`
- Modify: `tests/integration/daemon.rs`

**Interfaces:**
- Adds `AgentRunRole::CodeChangeEditor { code_change_run_id: String, attempt: i64 }`.
- Adds `AgentRunner::spawn_code_change_editor` using the verified candidate-root policy from Task 3 and stores `ExecutionProjection.execution_kind = "code_change_editor"` before any native launch.
- `code_change_editor_attempts.agent_run_id` binds the durable attempt to that run before marker creation/release. Generic interrupted-run recovery recognizes the execution kind plus binding and preserves it for `CodeChangeCoordinator` instead of applying Standard/Decision recovery.
- Introduces `CodeChangeCoordinator::advance_ready(now, limit)` and `CodeChangeReport`; at this task boundary it advances only through worktree/editor terminal persistence, and later tasks extend the same method for checks, submission, evaluation, and cleanup.
- Editor result is `{schema_version,status,summary,proposed_checks}` with `status=ready|cannot_apply`.
- First built-in attempt is fresh; the exact candidate-root-owned Codex session ID is persisted before private-temp cleanup. Attempt 2 uses `Resume { session_id }`.
- Custom editors receive the same supervisor-generated session token with `fresh` then `resume` environment modes.

- [ ] **Step 1: Write failing fresh/resume tests**

Extend fake Codex/custom agent fixtures to write `editor.json` and record argv/env. In daemon tests drive one check failure and assert:

```rust
assert_eq!(attempts.len(), 2);
assert_eq!(attempts[0].attempt, 1);
assert_eq!(attempts[1].attempt, 2);
assert_eq!(attempts[0].editor_session_id, attempts[1].editor_session_id);
assert_eq!(runs[0].context_mode, AgentContextMode::Fresh);
assert_eq!(runs[1].context_mode.session_id(), Some(attempts[0].editor_session_id.as_str()));
```

Add `cannot_apply`, malformed/oversized output, timeout, first agent failure, second failure, restart after terminal output persistence, and attempted third launch. Assert third launch is rejected and restart never resets attempts.

Add `code_change_editor_is_preserved_from_generic_startup_recovery`: seed a
bound attempt plus a Starting/Running `agent_runs` row with
`execution_kind=code_change_editor`, invoke generic recovery, and assert the
agent/event/attempt rows are unchanged, `editor_attempts` is unchanged, and no
replacement agent is inserted. Add the inverse fixtures: an editor execution
kind without a valid code-change binding and a binding to a non-editor
execution kind both fail closed as database identity contradictions.

- [ ] **Step 2: Run RED**

```bash
cargo test --test daemon code_change_editor -- --test-threads=1
cargo test --test daemon code_change_editor_is_preserved_from_generic_startup_recovery -- --exact --test-threads=1
cargo test --test codex_security code_change_editor -- --test-threads=1
```

- [ ] **Step 3: Add editor command/output transport**

Extend `PrivateRunTemp` with `prepare_editor_schema`/`read_editor_output` and add `CodexArgvBuilder::build_editor_with_private_temp(config, prompt, private_tmp)`. The builder is constructed only from `ResolvedExecutionPolicy::for_code_change_worktree`; it uses workspace-write, approval never, configured network, the candidate root for `-C` and project trust, editor output schema/last-message, and only the candidate root plus the inherited private-temp descriptor as writable authorities. Session discovery/ownership is keyed to that candidate root, never the registered/original root. Second attempt accepts only `Resume { session_id }`, verifies that exact candidate-root-owned session, and then appends `resume <exact-id>`. `ResumeLatest` is rejected for this role.

Custom agents receive only:

```text
PUEUE_AGENT_EDITOR_MODE=fresh|resume
PUEUE_AGENT_EDITOR_SESSION_ID=<supervisor-id>
PUEUE_AGENT_EDITOR_SCHEMA=/run/pueue-agent/private/editor-schema.json
PUEUE_AGENT_EDITOR_OUTPUT=/run/pueue-agent/private/editor.json
```

Credentials, SSH/Git auth/signing variables, unrelated state, and user-selected output paths remain absent.

- [ ] **Step 4: Extend retained agent ownership**

Add the editor role to every exhaustive `AgentRunRole` match without changing Standard/Decision/Diagnosis behavior. Extend `ExecutionProjection::new` with the exact fourth value `code_change_editor`. `spawn_code_change_editor` inserts the agent row with that execution kind, binds its run ID to the already reserved attempt, and only then creates/releases native launch state. A crash between row insertion and binding remains distinguishable by execution kind and is handled as a code-change recovery contradiction, never as Standard work.

It reuses launch gate, process group, timeout, log, cleanup, and terminal persistence. Before private-temp cleanup, validate editor JSON, resolve/persist the exact candidate-root-owned session ID, store result digest/status/summary/proposed checks, and finish the attempt transactionally. If attempt 1 reached execution but no owned session ID can be proven, do not launch a fresh attempt 2; reject or require recovery according to whether failure is certain or contradictory.

Generalize the existing hourly agent reservation helper for key `code-change-editor:v1:<run-id>:<attempt>`. Budget exhaustion leaves the run eligible until existing finite wake; it does not increment attempts.

- [ ] **Step 5: Integrate daemon editor ownership**

`CodeChangeCoordinator` advances `reserved -> preparing_worktree -> editing`, starts at most one editor, and exposes its `AgentHandle` to the daemon's existing retained-owner collections. Poll terminal ownership before advancing to checks. On editor/check failure, persist bounded feedback and resume once; second failure or `cannot_apply` rejects and schedules owned cleanup.

Modify `AgentRunRepository::recover_interrupted_with_marker_evidence` to left
join active runs to `code_change_editor_attempts`. Rows with both the exact
execution kind and binding are validated and counted in
`AgentRunRecovery.preserved_code_change_editors`, but are not failed, requeued,
dead-lettered, or detached from their attempt. All other active rows keep the
existing recovery behavior byte-for-byte. Task 6 performs the editor-specific
startup decision before normal daemon advancement.

- [ ] **Step 6: Run GREEN and agent regressions**

```bash
cargo test --test daemon code_change_editor -- --test-threads=1
cargo test --test daemon code_change_editor_is_preserved_from_generic_startup_recovery -- --exact --test-threads=1
cargo test --test codex_security code_change_editor -- --test-threads=1
cargo test agent:: -- --test-threads=1
cargo test --test native_agent_gate -- --test-threads=1
cargo test --test scheduler -- --test-threads=1
git diff --check
```

- [ ] **Step 7: Commit**

```bash
git add src/models.rs src/code_change.rs src/agent.rs src/codex_command.rs src/environment.rs src/db/code_changes.rs src/db/repositories.rs src/db/campaigns.rs src/daemon.rs tests/support/fake_codex.sh tests/support/fake_agent.sh tests/integration/codex_security.rs tests/integration/daemon.rs
git commit -m "feat: run resumable code editors"
```

---

### Task 5: Enforce checks, publish the candidate, and submit its revision-pinned experiment

**Files:**
- Modify: `src/code_change.rs`
- Modify: `src/db/code_changes.rs`
- Modify: `src/db/campaigns.rs`
- Modify: `src/campaign.rs`
- Modify: `src/daemon.rs`
- Modify: `src/environment.rs`
- Modify: `tests/integration/daemon.rs`
- Modify: `tests/integration/pueue_adapter.rs`
- Modify: `tests/integration/reconciliation.rs`

**Interfaces:**
- Private `CheckRunner::run_all` persists supervisor/discovered/editor checks and returns `CheckRoundResult { git_diff_passed, project_check_count, all_project_checks_passed, final_diff_matches }` bound to one final diff digest.
- `CampaignRepository::accept_code_change_candidate(run_id, experiment_id, submission_id, now, limits) -> ManagedSubmissionIntent` atomically accepts proposal, creates candidate experiment/submission/budget, stores revision, and binds run.
- `CampaignCoordinator::submit_candidate_intent` uses verified candidate worktree plus proposal-relative cwd; no fallback to original root.

- [ ] **Step 1: Write failing check-round tests**

In daemon tests use pinned fake tools for pass, non-zero, timeout, 64 KiB overflow, and tracked-file mutation. Assert `git diff --check` runs first, execution stops on the first failed required check, each check row is durable, at least one project check is required, and a post-check diff digest change fails the round.

Test deterministic discovery: Cargo profile, uv pytest profile, Python pytest profile, pyproject without pytest configuration, missing pinned tool, malicious TOML text, duplicate proposed check, shell/absolute/traversing proposal. Add exact test `git_diff_only_is_rejected`: a clean `git diff --check` with no discovered or valid editor project check must reject before commit and leave `candidate_sha` NULL.

- [ ] **Step 2: Run RED**

```bash
cargo test --test daemon code_change_check -- --test-threads=1
```

- [ ] **Step 3: Implement durable checks and candidate commit**

Before each command insert a reserved check row. The mandatory diff check is
`GIT_INDEX_FILE=<owned-temp-index> git diff --cached --check <base-sha>` so new
files in the temporary candidate tree are covered without trusting or mutating
the worktree index. Run every command through the bounded verified tool path
with candidate cwd and sanitized env; persist passed/failed/timed_out, exit
class, output digest, and fixed summary. Rerun diff enumeration after all
checks and require identical tree/digest/counts. On attempt 1 failure return
bounded feedback to Task 4; on attempt 2 reject.

Compute the project-specific count before committing and accept a round only
with this exact predicate:

```rust
let passed = result.git_diff_passed
    && result.project_check_count >= 1
    && result.all_project_checks_passed
    && result.final_diff_matches;
```

`git diff --check` is the supervisor check and never contributes to
`project_check_count`. Missing pinned tools, an empty discovery/proposal set,
or every proposed check being deduplicated/invalid yields count zero and
rejects; it must not silently publish a candidate.

For a passing round transition `checking -> committing`, revalidate protected refs/remotes, stage only validated paths, create one fixed-identity commit, verify parent/tree, create `campaign/<campaign-id>/candidate/<proposal-id>` with expected-old zero/exact, persist candidate SHA, and transition to `candidate_ready`. Restart verifies an existing exact commit/ref instead of creating another.

- [ ] **Step 4: Write failing atomic candidate-submission tests**

Assert candidate acceptance produces one reservation/experiment/submission and:

```rust
assert_eq!(experiment.code_change_run_id.as_deref(), Some(run.code_change_run_id.as_str()));
assert_eq!(experiment.code_revision_sha.as_deref(), run.candidate_sha.as_deref());
assert_eq!(proposal.status, ProposalStatus::Accepted);
assert_eq!(submission.argv, proposal.argv);
```

Fake Pueue must receive `--working-directory <candidate-root>/<proposal-cwd>`, while durable argv remains user argv and the existing four campaign environment variables remain present. Add restart at reserved/submitting/unreconciled boundaries and assert no duplicate `pueue add`. Add focused submission tests for a valid nested cwd, sibling escape, symlink component, and candidate-root replacement; only the descriptor-verified nested cwd may reach fake Pueue.

- [ ] **Step 5: Implement candidate intent and Pueue identity**

In one immediate transaction re-read `candidate_ready`, require exact SHA/ref and normal campaign/parallel/rolling budgets, insert deterministic experiment/submission IDs, normal experiment reservation, accepted proposal, candidate linkage, then move the run. Budget exhaustion uses existing `budget_waiting` and leaves candidate ready.

Before add, `WorktreeManager::verify` returns the exact candidate root and `VerifiedWorkingDirectory::open_descendant` resolves the proposal-relative cwd. Hold both descriptors through `pueue add`, pass only the verified canonical cwd in `--working-directory`, and reverify root/cwd identity after add before accepting its task identity. A sibling, absolute path, parent component, symlink component, or replaced descriptor fails before Pueue. Extend `ManagedSubmissionIntent` identity checks with code-change run/revision. After add, keep existing accepted/unreconciled task-signature protocol. A timeout after possible add is never automatically repeated.

- [ ] **Step 6: Run GREEN and non-code regressions**

```bash
cargo test --test daemon code_change_check -- --test-threads=1
cargo test --test daemon git_diff_only_is_rejected -- --exact --test-threads=1
cargo test --test pueue_adapter code_change_candidate -- --test-threads=1
cargo test --test pueue_adapter code_change_candidate_cwd -- --test-threads=1
cargo test --test pueue_adapter -- --test-threads=1
cargo test --test reconciliation code_change_candidate -- --test-threads=1
git diff --check
```

- [ ] **Step 7: Commit**

```bash
git add src/code_change.rs src/db/code_changes.rs src/db/campaigns.rs src/campaign.rs src/daemon.rs src/environment.rs tests/integration/daemon.rs tests/integration/pueue_adapter.rs tests/integration/reconciliation.rs
git commit -m "feat: submit checked candidate revisions"
```

---

### Task 6: Verify terminal source, CAS-promote best, recover, clean, and surface state

**Files:**
- Modify: `src/code_change.rs`
- Modify: `src/db/code_changes.rs`
- Modify: `src/reconcile.rs`
- Modify: `src/promotion.rs`
- Modify: `src/daemon.rs`
- Modify: `src/status.rs`
- Modify: `src/output.rs`
- Modify: `src/diagnostics.rs`
- Modify: `tests/integration/daemon.rs`
- Modify: `tests/integration/reconciliation.rs`
- Modify: `tests/integration/promotion.rs`
- Modify: `tests/integration/evaluation_surface.rs`
- Modify: `tests/integration/diagnostics.rs`

**Interfaces:**
- Extends `CodeChangeCoordinator` with `recover_interrupted(now, limit)` and completes `advance_ready(now, limit)` so both process a bounded stable list and one external action per row.
- Phase 4 keeps numeric comparison authority through `promotion::preview_code_candidate(&Transaction, campaign_id, experiment_id, terminal_status, limits, expected_old_sha, candidate_sha) -> Result<CodePromotionPlan, AppError>` and `promotion::finalize_code_candidate(&Transaction, &CodePromotionPlan, now) -> Result<PromotionOutcome, AppError>`.
- `CodePromotionPlan { outcome, expected_current_best_experiment_id, expected_old_sha, candidate_sha }` is bounded and reconstructible from the four persisted promotion fields added in Task 1. Preview does not change `campaigns.current_best_experiment_id`, plateau state, or `experiment_metrics.evaluated_at`; finalize performs those Phase 4 mutations only after ref verification.
- Existing `promotion::evaluate` remains the unchanged entry point for non-code experiments.
- Status/doctor expose bounded code-change projections and never mutate Git/worktrees.

- [ ] **Step 1: Write failing terminal/promotion tests**

Cover unchanged successful candidate, tracked-source mutation, untracked result file, OOM/failed/cancelled task, invalid/missing manifest, non-improvement, improvement, best-ref conflict, crash after update-ref before DB projection, rejected cleanup, and cleanup path replacement.

Improvement must satisfy:

```rust
assert_eq!(promotion, PromotionOutcome::Improved);
assert_eq!(fixture.rev_parse(&best_ref), candidate_sha);
assert_eq!(fixture.rev_parse("refs/heads/main"), original_main);
assert_eq!(fixture.remote_config_digest(), original_remote_digest);
```

Add `code_change_cas_failure_keeps_database_best` and assert both sides of the
boundary:

```rust
assert_eq!(campaign.current_best_experiment_id, Some(old_best_experiment_id));
assert_eq!(fixture.rev_parse(&best_ref), observed_conflicting_sha);
assert_ne!(fixture.rev_parse(&best_ref), candidate_sha);
assert!(metrics.evaluated_at.is_none());
```

Add `code_change_restart_after_ref_cas_finishes_database_projection`: persist
the intent, move the best ref to the candidate, simulate restart before
finalize, then assert recovery changes DB best/evaluated state exactly once and
does not issue another ref mutation.

- [ ] **Step 2: Run RED**

```bash
cargo test --test promotion code_change -- --test-threads=1
cargo test --test promotion code_change_cas_failure_keeps_database_best -- --exact --test-threads=1
cargo test --test promotion code_change_restart_after_ref_cas_finishes_database_projection -- --exact --test-threads=1
cargo test --test reconciliation code_change_terminal -- --test-threads=1
```

- [ ] **Step 3: Gate evaluation and best ref**

Before Phase 4 promotion of a code candidate, require exact candidate HEAD/index/tracked tree/ref. Tracked mutation, missing/mismatched worktree, runtime failure, invalid evidence, or cancellation records non-promotable evaluation and never runs CAS. Untracked result/artifact files do not change revision identity.

Do not call existing mutating `promotion::evaluate` for an experiment whose
`code_change_run_id` is non-NULL. Use this sequence instead:

```text
1. CandidateRepository verifies candidate HEAD/tree/ref and reads best_ref.
2. CodeChangeRepository::prepare_code_promotion opens IMMEDIATE transaction.
3. preview_code_candidate computes Phase 4 outcome without mutations.
4. The same transaction stores outcome, expected DB best experiment,
   expected old ref SHA (nullable means ref absent), and candidate target SHA.
5. Commit the SQLite transaction.
6. For Improved only, CandidateRepository performs update-ref CAS and verifies.
7. CodeChangeRepository::finalize_code_promotion opens IMMEDIATE transaction,
   reconstructs the exact plan, rechecks expected DB best, calls finalize, and
   records evaluated state atomically.
```

For `Improved`, the target is exactly the persisted candidate SHA. A CAS error
is followed by an exact ref read: expected-old still present is safely
retryable on a later bounded pass, candidate already present continues to DB
finalize, and any unrelated value becomes `recovery_required` without
overwrite. Until exact candidate ref verification, DB current-best, plateau,
and evaluated marker remain unchanged. Non-improvement stores a plan with no
ref target, finalizes Phase 4 directly, retains the candidate ref, and leaves
the best ref unchanged.

`finalize_code_candidate` uses a compare-and-set predicate on the persisted
`expected_current_best_experiment_id` (including NULL). A mismatch never
blindly rewrites DB state; before Git CAS it discards/re-previews the intent,
and after Git CAS it becomes `recovery_required` for explicit operator
inspection.

- [ ] **Step 4: Recover every external boundary and clean owned worktrees**

Startup recovery follows DB state plus exact Git/agent/check/Pueue evidence. It never infers from branch names, repeats uncertain Pueue add, creates a second commit, resets attempts, or overwrites refs. Contradiction becomes `recovery_required` and blocks further code changes in that campaign.

Use this startup order in `Daemon::run_once`; do not let the current generic
recovery at `src/daemon.rs` terminalize editor rows first:

```text
1. load startup marker evidence;
2. generic AgentRunRepository recovery preserves validated code-change editors;
3. CodeChangeCoordinator::recover_startup_editors consumes those exact rows;
4. CampaignRepository::recover_submission_boundaries reconciles durable adds;
5. CodeChangeCoordinator::recover_interrupted reconciles Git/check/commit/ref/
   promotion state, including CAS-before-DB crashes;
6. clear startup_recovery_pending;
7. continue normal dispatch and Reconciler evaluation.
```

Editor-specific startup recovery first reuses a terminal validated output. A
certain pre-marker failure finishes that attempt once and permits the same
finite retry rules; post-release uncertainty without durable validated output
becomes `recovery_required`. It never decrements attempt count, creates a new
session, requeues its coordinator event as Standard work, or launches an agent
during the startup reconciliation call.

After durable evaluation, transition `evaluated -> cleanup_pending`, prove no live task/process, call descriptor-owned cleanup, verify absence, and mark completed. Pre-candidate rejection uses the same cleanup authority. Commit/ref/check/lineage metadata remains.

- [ ] **Step 5: Write failing status and doctor tests**

Status JSON includes state, attempts, abbreviated base/candidate SHA, linked experiment/task, failed check summary, next action, and cleanup flag. Assert absence of `prompt`, `conversation`, `raw_diff`, raw `argv`, `environment`, `credentials`, complete output, and full digests.

Expose a bounded recent transition list from completed `CodeChange` audit
events so editor/check/commit/submission/evaluation/promotion/cleanup history is
inspectable without reading raw SQLite payloads. Cap it with the existing
status history limit and render fixed stage/reason labels only.

Doctor adds independent read-only checks: `code_change.rows`, `.lineage`, `.single_live`, `.worktrees`, `.refs`, `.experiments`, `.stale`, `.cleanup`. Seed each invalid relation and assert only bounded counts/fixed messages; doctor never repairs/prunes/removes/updates.

- [ ] **Step 6: Run GREEN and Phase 3/4 regressions**

```bash
cargo test --test promotion -- --test-threads=1
cargo test --test promotion code_change_cas_failure_keeps_database_best -- --exact --test-threads=1
cargo test --test promotion code_change_restart_after_ref_cas_finishes_database_projection -- --exact --test-threads=1
cargo test --test reconciliation -- --test-threads=1
cargo test --test daemon code_change_editor_is_preserved_from_generic_startup_recovery -- --exact --test-threads=1
cargo test --test evaluation_surface code_change -- --test-threads=1
cargo test --test diagnostics code_change -- --test-threads=1
cargo test --test health_observer -- --test-threads=1
cargo test --test health_actions -- --test-threads=1
git diff --check
```

- [ ] **Step 7: Commit**

```bash
git add src/code_change.rs src/db/code_changes.rs src/reconcile.rs src/promotion.rs src/daemon.rs src/status.rs src/output.rs src/diagnostics.rs tests/integration/daemon.rs tests/integration/reconciliation.rs tests/integration/promotion.rs tests/integration/evaluation_surface.rs tests/integration/diagnostics.rs
git commit -m "feat: evaluate and recover code candidates"
```

---

### Task 7: Document Phase 5, add real-Pueue acceptance, and verify on Linux

**Files:**
- Modify: `README.md`
- Modify: `docs/getting-started-ja.md`
- Modify: `docs/commands-ja.md`
- Modify: `docs/workflows-ja.md`
- Modify: `docs/architecture-ja.md`
- Modify: `docs/troubleshooting-ja.md`
- Modify: `tests/e2e/rust_supervisor.sh`
- Modify: `tests/e2e/run.sh` only if the existing entrypoint must register the scenario
- Modify: `tests/support/fake_codex.sh`
- Modify: `tests/support/fake_agent.sh`

**Interfaces:**
- No new mandatory CLI command, repository adapter, or controller.
- Documentation explains the internal pipeline, policy, status/doctor facts, refs, recovery, custom-agent trust, and Phase 6 boundary.
- E2E proves ordinary submit through candidate evaluation on real Pueue.

- [ ] **Step 1: Update documentation to match implemented behavior**

Document zero-adapter flow; 50/500000/8/30 bounds; two attempts/one session; code-change/agent/experiment budget accounting; Git/Cargo/uv/Python discovery; local candidate/best refs; main/remote prohibition; dirty/non-Git/legacy base rejection; OOM/internal failure/tracked mutation; recovery/cleanup diagnostics; candidate inspection commands; trusted custom-agent/native execution limitation. Remove stale prose saying Phase 3, Phase 4, goal review, or isolated worktrees are future work.

- [ ] **Step 2: Verify and commit documentation**

```bash
cargo test --test cli_help -- --test-threads=1
cargo test --test evaluation_surface code_change -- --test-threads=1
git diff --check
git add README.md docs/getting-started-ja.md docs/commands-ja.md docs/workflows-ja.md docs/architecture-ja.md docs/troubleshooting-ja.md
git commit -m "docs: explain isolated code change campaigns"
```

- [ ] **Step 3: Add the real-Pueue acceptance scenario**

Use a disposable Git Python ML repository with clean `main`, deterministic pytest smoke check, result manifest metric, fake editor first-check failure, and same-session successful correction. Record before values for main SHA, original file digest, remote config, and unrelated worktree registrations.

Assert:

```text
one pending then accepted code_change proposal
two editor attempts with one session ID
one candidate commit and candidate ref
one successful project check bound to final diff digest
one Pueue experiment at persisted candidate SHA/cwd
best ref moves only after valid improvement
original main/file/remote/unrelated worktrees stay unchanged
owned worktree is absent after completed cleanup
```

Add second-check failure and runtime OOM/internal-error cases; neither may create/move best.

- [ ] **Step 4: Run local syntax/focused gates and commit E2E**

```bash
bash -n tests/e2e/rust_supervisor.sh
bash -n tests/e2e/run.sh
cargo test --test daemon code_change -- --test-threads=1
cargo test --test pueue_adapter code_change -- --test-threads=1
cargo test --test reconciliation code_change -- --test-threads=1
cargo test --test promotion code_change -- --test-threads=1
git diff --check
git add tests/e2e/rust_supervisor.sh tests/e2e/run.sh tests/support/fake_codex.sh tests/support/fake_agent.sh
git commit -m "test: exercise isolated code changes end to end"
```

- [ ] **Step 5: Run local compilation and all-target verification**

```bash
cargo fmt --check
cargo check --all-targets
cargo check --all-targets --release
cargo test --all-targets -- --test-threads=1
bats tests/test_shell_entrypoints.bats
git diff --check
```

macOS unsupported Linux security-path failures are not acceptance failures. Fix every compilation/cross-platform unit regression and record exact unsupported test names; never weaken a Linux invariant to make macOS pass.

- [ ] **Step 6: Sync exact clean HEAD to roko and run Linux gates**

Confirm local `git status --short` is empty and record `git rev-parse HEAD`. Sync that exact source to `/home/romanohu/project/pueueAgent`; roko remains test-only. Verify matching SHA/clean checkout, then run:

```bash
cargo fmt --check
cargo check --all-targets
cargo check --all-targets --release
cargo test --all-targets -- --test-threads=1
bats tests/test_shell_entrypoints.bats
tests/e2e/run.sh
```

Record roko OS/tool versions, exact SHA, exit codes, Rust/Bats counts, real-Pueue result, candidate/best/main invariants, warnings, and macOS-only observations.

- [ ] **Step 7: Request two-stage Luna-max review before integration**

Use a fresh Luna-max spec-compliance reviewer, then a separate Luna-max code-quality/security reviewer. Resolve findings with focused TDD commits and rerun affected Linux gates. Do not merge or push until all tasks pass, reviews are clear, and the user authorizes integration.

---

## Final verification checklist

- [ ] Every approved spec section maps to a task above.
- [ ] Placeholder scan finds no deferred implementation or unnamed error handling.
- [ ] Interfaces and type names remain consistent between tasks.
- [ ] `git diff --check` passes and the feature worktree is clean.
- [ ] Exact Linux HEAD passes Rust, Bats, and real-Pueue gates on roko.
- [ ] `main`, original checkout, unrelated worktrees, remote refs/config, and credentials remain unchanged/unexposed.
- [ ] No automatic merge, rebase, push, PR creation, broad worktree pruning, or raw-log/diff persistence exists.
