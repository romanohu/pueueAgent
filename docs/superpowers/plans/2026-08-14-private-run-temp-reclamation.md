# Private Run Temp Reclamation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reclaim descriptor-owned private-run temporary contents after process and database finalization, while retaining retry authority, bounding work/disk exposure, and preventing only the affected project from launching another agent.

**Architecture:** `PrivateRunTemp` performs a two-phase, descriptor-relative, no-follow audit and post-order cleanup of its retained generation; the top-level run directory is never renamed or removed. Agent handles persist the terminal outcome before cleanup and retain policy/temp ownership across cleanup retries. Daemon and scheduler carry cleanup-blocked project IDs explicitly, while startup/pre-admission inventory detects crash-retained trees without mutating them.

**Tech Stack:** Rust 2021, Tokio, `libc` Unix descriptor APIs, rusqlite, existing immutable execution policy/native launcher, tempfile-based integration fixtures.

## Global Constraints

- Cleanup begins only after checked process-group drain/reap and successful terminal SQLite persistence.
- The retained run-directory FD is the only cleanup root; never reopen `PrivateRunTemp::path()` or construct a child pathname.
- Never follow symlinks or read leaf contents. Use `openat`/`fstatat(AT_SYMLINK_NOFOLLOW)`/`unlinkat` relative to verified directory descriptors.
- Never rename or remove the top-level `.pueue-agent/tmp/<run-id>` directory. `Drop` remains non-mutating.
- Fixed bounds are depth 32, 4096 visited entries, 1 GiB allocated bytes (`st_blocks * 512`), and 4096 generation entries during inventory.
- Audit the complete bounded tree before the first mutation. Bound failures leave the tree untouched; mutation failures retain authority and are retryable.
- Cleanup failure never rewrites a terminal run/event/intervention outcome and never repeats process signaling after successful reap.
- A live cleanup owner blocks only its project; other projects continue. Claimed events for an in-memory cleanup block are deferred without attempt consumption or intervention reservation.
- Crash-retained nonempty trees are observed through the verified project-root descriptor and fail closed as `TempUnsafe/PreBinding`; startup inventory never deletes them.
- No raw path, entry name, file contents, prompt, argv, environment, credential, or unbounded filesystem error is persisted or rendered.
- Preserve the existing one-second TERM grace and daemon global shutdown deadline.
- Linux and macOS are supported. Non-Unix public boundaries fail closed.

---

### Task 1: Descriptor-Owned Audit, Cleanup, and Inventory

**Files:**
- Modify: `src/environment.rs`
- Modify: `src/execution_policy.rs`
- Test: `tests/integration/codex_security.rs`

**Interfaces:**
- Consumes: `VerifiedProjectRoot`, retained `PrivateRunTemp::{parent,directory,identity}`, and existing `PolicyViolation` redaction.
- Produces:

```rust
pub const MAX_PRIVATE_TEMP_CLEANUP_DEPTH: usize = 32;
pub const MAX_PRIVATE_TEMP_CLEANUP_ENTRIES: usize = 4096;
pub const MAX_PRIVATE_TEMP_ALLOCATED_BYTES: u64 = 1024 * 1024 * 1024;
pub const MAX_PRIVATE_TEMP_GENERATIONS: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TempUnsafeReason {
    DepthLimit,
    EntryLimit,
    ByteLimit,
    GenerationLimit,
    IdentityChanged,
    InvalidEntry,
    IoFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TempCleanupReport {
    pub entries_removed: usize,
    pub allocated_bytes_reclaimed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TempInventoryReport {
    pub generations: usize,
    pub retained_nonempty_generations: usize,
    pub retained_allocated_bytes: u64,
}

impl PrivateRunTemp {
    pub fn cleanup_contents_before(
        &mut self,
        deadline: Option<std::time::Instant>,
    ) -> Result<TempCleanupReport, PolicyViolation>;

    pub fn inspect_capacity(
        root: &VerifiedProjectRoot,
    ) -> Result<TempInventoryReport, PolicyViolation>;
}
```

`PolicyViolationDetail` gains `TempUnsafe(TempUnsafeReason)`. Its `Display` and SQLite projection remain only `policy_blocked:private_temp_unsafe`; the reason is local typed control flow.

- [ ] **Step 1: Write RED boundary tests**

Add integration tests with these exact names:

```rust
private_temp_cleanup_removes_bounded_contents_but_retains_run_directory
private_temp_cleanup_does_not_follow_symlink_or_fifo_targets
private_temp_cleanup_rejects_depth_entry_and_allocated_byte_overflow_before_mutation
private_temp_cleanup_never_touches_a_replacement_run_generation
private_temp_inventory_allows_empty_generations_and_rejects_nonempty_or_unsafe_generations
private_temp_inventory_rejects_generation_overflow_without_mutation
```

Use `symlink`, `mkfifo`, Unix sockets, small allocated fixture files, nested
directories, and a renamed/replaced visible run path. Exercise the production
1 GiB constant through checked-arithmetic unit tests and a private
`#[cfg(test)]` audit-limit seam rather than allocating 1 GiB in an integration
test. The successful test must assert that the retained run directory still
exists and is empty. Each overflow test must assert a typed
`TempUnsafeReason` and byte-for-byte fixture preservation.

- [ ] **Step 2: Run RED**

Run:

```bash
cargo test --test codex_security private_temp_cleanup -- --nocapture
cargo test --test codex_security private_temp_inventory -- --nocapture
```

Expected: compile failure for the missing types/methods, then behavior failure
if the API scaffold is introduced without traversal.

- [ ] **Step 3: Implement bounded descriptor iteration and audit**

Add private Unix helpers in `environment.rs`:

```rust
struct AuditedEntry {
    name: OsString,
    identity: (u64, u64),
    kind: AuditedEntryKind,
    allocated_bytes: u64,
    children: Vec<AuditedEntry>,
}

enum AuditedEntryKind { Directory, Leaf }

fn audit_directory(
    directory: &File,
    depth: usize,
    state: &mut AuditState,
    deadline: Option<Instant>,
) -> Result<Vec<AuditedEntry>, PolicyViolation>;

fn remove_audited_entries(
    directory: &File,
    entries: &[AuditedEntry],
    report: &mut TempCleanupReport,
    deadline: Option<Instant>,
) -> Result<(), PolicyViolation>;
```

Duplicate FDs before `fdopendir`; close each exactly once. Count/check every
entry and `st_blocks * 512` with checked arithmetic. Open directories with
`O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC|O_NONBLOCK`; do not open leaves.
Revalidate type/dev/inode immediately before unlink. Remove directories
post-order, fsync modified parents, and never unlink the run root.

- [ ] **Step 4: Add deterministic mutation and deadline unit seams**

Inside `environment.rs` under `#[cfg(test)]`, add a private callback/failpoint
between audit and removal. Prove an entry replacement produces
`IdentityChanged`, the replacement remains, the handle remains retryable, and
an expired deadline performs no new mutation. Do not add a public raw-FD or
callback API.

- [ ] **Step 5: Implement observational startup/admission inventory**

Open only the fixed `.pueue-agent/tmp` components relative to
`VerifiedProjectRoot`. Missing fixed directories mean an empty inventory and
must not be created. Require numeric positive run-directory names, owner-only
`0700` real directories, no symlinks, and the generation bound. Reuse the
audit walker without calling removal. Any nonempty valid generation returns
`TempUnsafe(InvalidEntry)` at the policy boundary; expose only aggregate counts
inside `TempInventoryReport` for tests/internal flow.

- [ ] **Step 6: Run GREEN and platform checks**

```bash
cargo test --test codex_security private_temp_cleanup -- --nocapture
cargo test --test codex_security private_temp_inventory -- --nocapture
cargo test --lib environment -- --nocapture
cargo check --all-targets
git diff --check
```

- [ ] **Step 7: Commit**

```bash
git add src/environment.rs src/execution_policy.rs tests/integration/codex_security.rs
git commit -m "feat: reclaim private temp contents safely"
```

---

### Task 2: Terminal-Persisted Cleanup Ownership

**Files:**
- Modify: `src/agent.rs`
- Modify: `src/environment.rs` only if Task 1 exposes a narrow helper needed by lifecycle code
- Test: `src/agent.rs` unit tests
- Test: `tests/integration/scheduler.rs`

**Interfaces:**
- Consumes: `PrivateRunTemp::cleanup_contents_before`, cached `TerminalOutcome`, `RetainedLaunchAuthority`, `BoundCleanupHandle`.
- Produces:

```rust
enum TerminalPersistence {
    Pending,
    Persisted(AgentRunStatus),
}

impl AgentHandle {
    pub(crate) fn cleanup_pending(&self) -> bool;
    pub(crate) fn cleanup_blocked_project(&self) -> Option<&str>;
}
```

The public `poll` shape stays `Result<Option<AgentRunStatus>, AppError>`:
`Ok(None)` means running or terminal-persisted cleanup pending; daemon uses the
private predicate to distinguish them. `wait`/`timeout_now` return a bounded
cleanup error while retaining the same handle when terminal persistence has
succeeded but cleanup has not.

- [ ] **Step 1: Write RED lifecycle-order tests**

Add real DB/native-child tests with these names:

```rust
terminal_temp_cleanup_waits_for_process_reap_and_database_commit
terminal_database_failure_does_not_begin_temp_cleanup
terminal_cleanup_failure_keeps_persisted_outcome_and_same_handle_for_retry
bound_spawn_cleanup_finalizes_database_before_reclaiming_temp
```

Use existing SQLite failure triggers and current-test-binary native child
fixtures. The cleanup failure fixture exceeds a bound or injects an identity
change. Assert first retry leaves the DB terminal exactly once, retains temp
authority, and does not signal/reap again; after removing the fault, the same
handle cleans and finishes.

- [ ] **Step 2: Run RED**

```bash
cargo test --lib terminal_temp_cleanup -- --nocapture
cargo test --lib terminal_cleanup_failure -- --nocapture
cargo test --test scheduler bound_spawn_cleanup -- --nocapture
```

Expected: cleanup is currently never invoked and retained authority is dropped
immediately after terminal persistence.

- [ ] **Step 3: Separate terminal persistence from authority release**

Add `terminal_persistence` to `AgentHandle`. Refactor
`finalize_stored_outcome` into:

```rust
fn persist_terminal_outcome(&mut self, db: &Db, now: i64)
    -> Result<AgentRunStatus, AppError>;

fn retry_terminal_cleanup(&mut self, deadline: Option<Instant>)
    -> Result<AgentRunStatus, AppError>;
```

Set `Persisted(status)` immediately after the DB transaction succeeds. Only
`retry_terminal_cleanup` may replace `RetainedLaunchAuthority` with `Released`.
When cleanup fails, leave both `terminal_outcome` and `Persisted(status)` in
place so DB and process work are not repeated.

- [ ] **Step 4: Apply the same rule to `BoundCleanupHandle`**

Extend its live-child state with a `finalized: bool`. `retry` order is:
terminate/reap once -> cached finalizer once -> temp cleanup until success ->
release. Pending-marker handles without a child/temp remain unchanged. Ensure
the inline spawn-failure resolver returns a cleanup owner when DB finalization
succeeds but temp cleanup fails.

- [ ] **Step 5: Preserve shutdown deadlines**

`timeout_now_before` and `BoundCleanupHandle::retry_before` pass the existing
absolute deadline to temp cleanup after process/DB work. An expired deadline
retains ownership and returns a bounded Runtime/TempUnsafe error. Do not change
TERM grace, lifecycle readiness, or SQLite scoped busy timeout.

- [ ] **Step 6: Run GREEN**

```bash
cargo test --lib agent::tests -- --nocapture
cargo test --test scheduler finalizer -- --nocapture
cargo test --test scheduler bound_spawn_cleanup -- --nocapture
cargo check --all-targets
git diff --check
```

- [ ] **Step 7: Commit**

```bash
git add src/agent.rs src/environment.rs tests/integration/scheduler.rs
git commit -m "feat: retain terminal temp cleanup ownership"
```

---

### Task 3: Fair Project-Scoped Admission Blocking

**Files:**
- Modify: `src/daemon.rs`
- Modify: `src/scheduler.rs`
- Modify: `src/agent.rs`
- Test: `tests/integration/daemon.rs`
- Test: `tests/integration/scheduler.rs`

**Interfaces:**
- Consumes: `AgentHandle::cleanup_blocked_project` and daemon `active_agents`.
- Produces:

```rust
impl Scheduler {
    pub fn with_cleanup_blocked_projects(
        self,
        project_ids: std::collections::BTreeSet<String>,
    ) -> Self;
}
```

The daemon derives the set immediately before each scheduler tick. Scheduler
checks it before config loading/reservation/binding and calls
`EventRepository::defer_claimed` for that project's grouped event IDs.

- [ ] **Step 1: Write RED two-project fairness tests**

```rust
cleanup_pending_project_defers_without_attempt_while_other_project_dispatches
cleanup_retry_is_fair_across_projects_and_finishes_after_fault_removal
shutdown_retains_temp_cleanup_when_deadline_expires
```

Project A must have a terminal-persisted cleanup fault. Project B must use a
generated native target and dispatch in the same daemon tick. Assert A event
returns to Pending with unchanged attempts, no intervention reservation and no
new run; B reaches Dispatched. The shutdown test keeps the existing absolute
deadline assertion.

- [ ] **Step 2: Run RED**

```bash
cargo test --test daemon cleanup_pending_project -- --nocapture
cargo test --test daemon cleanup_retry_is_fair -- --nocapture
cargo test --test daemon shutdown_retains_temp_cleanup -- --nocapture
```

Expected: current poll error returns before scheduler or scheduler attempts a
new binding because it has no cleanup-blocked project set.

- [ ] **Step 3: Keep cleanup pending non-fatal to unrelated projects**

Update daemon polling so a typed temp-cleanup-pending result retains the agent
without becoming the tick's generic first error. Continue treating process,
DB, and ownership errors as errors. Attempt every owner once per pass, as in
the existing fairness contract.

- [ ] **Step 4: Add scheduler admission exclusion**

Store a private `BTreeSet<String>` in `Scheduler`, default empty. At the start
of each grouped project branch, if blocked, defer its claimed events and
continue before config, guardrail, reservation, prompt, or spawn work. Add a
defensive active-run check before private-temp inventory in Task 4 so the live
run's own nonempty temp is never classified as crash-retained evidence.

- [ ] **Step 5: Run GREEN and repeat concurrency suites**

```bash
cargo test --test daemon cleanup_ -- --nocapture
cargo test --test scheduler cleanup_ -- --nocapture
cargo test --test daemon
cargo test --test daemon
cargo test --test scheduler
cargo check --all-targets
git diff --check
```

- [ ] **Step 6: Commit**

```bash
git add src/agent.rs src/daemon.rs src/scheduler.rs tests/integration/daemon.rs tests/integration/scheduler.rs
git commit -m "feat: block projects with pending temp cleanup"
```

---

### Task 4: Crash-Retained Inventory and Acceptance

**Files:**
- Modify: `src/agent.rs`
- Modify: `src/scheduler.rs`
- Modify: `src/daemon.rs` only if startup inventory is cached there
- Test: `tests/integration/daemon.rs`
- Test: `tests/integration/scheduler.rs`
- Test: `tests/integration/codex_security.rs`
- Update: `.superpowers/sdd/2026-08-13-execution-policy-agent-launch/progress.md` (ignored ledger only)

**Interfaces:**
- Consumes: `PrivateRunTemp::inspect_capacity`, pinned
  `ResolvedProjectExecutionPolicy::root_anchor`, scheduler active-run and
  cleanup-blocked checks.
- Produces:

```rust
impl AgentRunner {
    pub fn preflight_private_temp_capacity(
        &self,
        policy: &ResolvedProjectExecutionPolicy,
    ) -> Result<TempInventoryReport, PolicyViolation>;
}
```

- [ ] **Step 1: Write RED restart/admission tests**

```rust
crash_retained_temp_dead_letters_before_reservation_or_run
empty_retained_generations_allow_the_next_agent
active_agent_temp_is_deferred_not_classified_as_crash_retained
startup_temp_inventory_rejects_symlink_weak_and_over_limit_without_mutation
```

Construct crash-retained entries before daemon/scheduler creation. Assert
`TempUnsafe/PreBinding`, direct DeadLetter/no run/no reservation for a nonempty
or unsafe orphan; empty generations allow normal dispatch. For an active DB
run, the new event must defer without attempts and must not invoke inventory
against the live temp generation.

- [ ] **Step 2: Run RED**

```bash
cargo test --test scheduler crash_retained_temp -- --nocapture
cargo test --test scheduler empty_retained_generations -- --nocapture
cargo test --test daemon startup_temp_inventory -- --nocapture
```

- [ ] **Step 3: Add pinned inventory preflight at the correct boundary**

`AgentRunner::preflight_private_temp_capacity` verifies the retained project
root anchor and calls descriptor-only inventory. Scheduler order per project is:

```text
claimed -> explicit cleanup-block set -> active-run defer -> config and
immutable policy -> guardrails -> private-temp inventory -> intervention
reservation -> actual-prompt preflight -> bind/spawn
```

Map inventory violations through existing pre-binding policy dead-letter
handling. Do not canonicalize/open a stored path and do not create missing temp
directories during inventory.

- [ ] **Step 4: Add bounded startup coverage**

Run the same descriptor inventory on the first admission attempt after daemon
startup. It may be cached per project, but an unsafe tree must block only that
project and must not abort recovery or scheduling for unrelated projects. The
actual event transition remains in the scheduler's typed pre-binding policy
path. Inventory itself leaves database state unchanged. Do not auto-delete a
crash-retained tree and do not signal a historical PID/process group.

- [ ] **Step 5: Full security and host-independent verification**

```bash
cargo test --test codex_security
cargo test --test scheduler
cargo test --test daemon
cargo test --test database
cargo test --test native_agent_gate --test native_launcher
cargo check --all-targets
cargo test --all-targets
git diff --check
```

`cargo fmt --all -- --check` is required only when the rustfmt component is
available; otherwise record the environment limitation and perform manual
format/diff review. Do not add credentials or runtime artifacts.

- [ ] **Step 6: Independent task review and fix loop**

Generate a review package from the pre-Task-1 base through current HEAD. The
reviewer must return both spec-compliance and quality verdicts. Fix every
Critical/Important finding with a new RED/GREEN cycle and request a scoped
re-review until CLEAN.

- [ ] **Step 7: Commit acceptance and ledger**

```bash
git add src tests
git commit -m "test: verify private temp reclamation lifecycle"
```

Record exact commits, RED/GREEN evidence, final review verdict, and any truly
unresolved portability limitation in the ignored plan ledger. Do not push;
the overarching roadmap is not complete.

---

## Plan self-review

- Every spec requirement maps to a task: descriptor cleanup (Task 1), exact
  lifecycle ownership (Task 2), project-scoped fairness/deadline (Task 3), and
  crash inventory/acceptance (Task 4).
- Public/private type names and constants are defined before later tasks use
  them. No task relies on an unstated ambient path or shell fixture.
- The plan intentionally does not remove top-level run directories, implement
  a hard filesystem quota, auto-delete crash-retained trees, persist raw temp
  metadata, or expand general diagnostics.
- Task boundaries each have an independent RED/GREEN and reviewable commit.
