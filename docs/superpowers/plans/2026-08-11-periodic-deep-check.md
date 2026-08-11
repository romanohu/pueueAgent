# Periodic DeepCheck Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 実行中の Pueue task がある project を一定周期で軽量に観測し、重複なしの `DeepCheck` event から agent を起動して状態を記録できるようにする。

**Architecture:** daemon の reconciliation 後、既存 scheduler の前に project 単位の periodic scheduler を追加する。周期判定は純粋関数でテストし、SQLite の event dedup と active agent 検査で一枠制約を守る。既存の `EventKind::DeepCheck` と `deep_check` prompt を再利用する。

**Tech Stack:** Rust 2021、Tokio、rusqlite bundled、Serde/TOML、既存の PueueApi abstraction、Rust integration tests。

## Global Constraints

- `check.deep_check_interval_minutes = 0` は定期起動を無効にする。
- `check.deep_check_every` は既存設定との互換性のため受理するが、定期起動の条件には使わない。
- task ごとではなく project ごとに最大1つの定期 event を作る。
- event payload は task ID、件数、時刻だけの bounded projection とし、command、env、prompt、transcript を保存しない。
- Pueue に dummy task を投入しない。
- 実装後の標準検証は `cargo test --all-targets` と `cargo fmt --check` で行う。

---

### Task 1: 定期起動判定を純粋関数として追加

**Files:**
- Create: `src/periodic.rs`
- Modify: `src/lib.rs`
- Test: `src/periodic.rs` 内の `#[cfg(test)]` module

**Interfaces:**
- Produces `DeepCheckScheduleInput`、`should_schedule_deep_check`、`periodic_bucket`。
- Later tasks use `should_schedule_deep_check` without accessing SQLite or Pueue.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn periodic_check_requires_active_running_task_and_enabled_interval() {
    let input = DeepCheckScheduleInput {
        interval_minutes: 30,
        now: 3_600,
        oldest_running_task_started_at: Some(1_801),
        last_scheduled_at: None,
        project_active: true,
        has_running_task: true,
        has_active_agent: false,
        has_open_event: false,
    };

    assert!(should_schedule_deep_check(&input));
    assert!(!should_schedule_deep_check(&DeepCheckScheduleInput {
        has_running_task: false,
        ..input
    }));
}

#[test]
fn periodic_check_waits_for_interval_and_skips_active_or_open_work() {
    let base = DeepCheckScheduleInput {
        interval_minutes: 30,
        now: 2_000,
        oldest_running_task_started_at: Some(1_000),
        last_scheduled_at: Some(1_900),
        project_active: true,
        has_running_task: true,
        has_active_agent: false,
        has_open_event: false,
    };

    assert!(!should_schedule_deep_check(&base));
    assert!(!should_schedule_deep_check(&DeepCheckScheduleInput {
        has_active_agent: true,
        ..base
    }));
    assert!(!should_schedule_deep_check(&DeepCheckScheduleInput {
        has_open_event: true,
        ..base
    }));
    assert!(should_schedule_deep_check(&DeepCheckScheduleInput {
        now: 3_701,
        ..base
    }));
}

#[test]
fn periodic_bucket_is_stable_for_the_same_interval() {
    assert_eq!(periodic_bucket(3_599, 1_800), 1);
    assert_eq!(periodic_bucket(3_600, 1_800), 2);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test periodic::tests --lib`
Expected: FAIL because `src/periodic.rs` and its exported functions do not exist.

- [ ] **Step 3: Implement the minimal policy**

Add the following public types and functions to `src/periodic.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeepCheckScheduleInput {
    pub interval_minutes: u32,
    pub now: i64,
    pub oldest_running_task_started_at: Option<i64>,
    pub last_scheduled_at: Option<i64>,
    pub project_active: bool,
    pub has_running_task: bool,
    pub has_active_agent: bool,
    pub has_open_event: bool,
}

pub fn should_schedule_deep_check(input: &DeepCheckScheduleInput) -> bool {
    if input.interval_minutes == 0
        || !input.project_active
        || !input.has_running_task
        || input.has_active_agent
        || input.has_open_event
    {
        return false;
    }
    let interval = i64::from(input.interval_minutes) * 60;
    let anchor = input
        .last_scheduled_at
        .or(input.oldest_running_task_started_at)
        .unwrap_or(input.now);
    input.now.saturating_sub(anchor) >= interval
}

pub fn periodic_bucket(now: i64, interval_seconds: i64) -> i64 {
    now.div_euclid(interval_seconds.max(1))
}
```

Export the module from `src/lib.rs` with `pub mod periodic;`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test periodic::tests --lib`
Expected: PASS with all new unit tests green.

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs src/periodic.rs
git commit -m "feat: add periodic deep check policy"
```

### Task 2: SQLite の periodic event query と event 生成を追加

**Files:**
- Modify: `src/db/repositories.rs`
- Modify: `src/periodic.rs`
- Test: `tests/integration/periodic.rs`
- Modify: `Cargo.toml` to register the integration test target

**Interfaces:**
- Produces `PeriodicDeepCheckScheduler::schedule(&[PueueTask]) -> Result<usize, AppError>`.
- Adds `EventRepository::latest_periodic_deep_check_at` and `EventRepository::has_open_periodic_deep_check`.
- Step 1 of this task defines a test-only `PeriodicHarness` with a temp `Db`, one registered project, configurable `CheckConfig`, event-count helpers, and `running_task(task_id)` constructors.

- [ ] **Step 1: Add failing repository and scheduler integration tests**

Create a temp-db harness with one registered project and a running `PueueTask`. Add tests with these names and assertions:

```rust
#[test]
fn scheduler_creates_one_bounded_periodic_event_for_multiple_running_tasks() {
    let harness = PeriodicHarness::with_interval(30);
    let count = harness.schedule(&[harness.running_task(41), harness.running_task(42)]);

    assert_eq!(count, 1);
    let event = harness.only_event();
    assert_eq!(event.kind, EventKind::DeepCheck);
    assert_eq!(event.payload["source"], "periodic");
    assert_eq!(event.payload["task_count"], 2);
    assert!(event.payload.get("command").is_none());
}

#[test]
fn scheduler_is_idempotent_for_the_same_periodic_bucket() {
    let harness = PeriodicHarness::with_interval(30);
    assert_eq!(harness.schedule(&[harness.running_task(41)]), 1);
    assert_eq!(harness.schedule(&[harness.running_task(41)]), 0);
    assert_eq!(harness.event_count(), 1);
}

#[test]
fn scheduler_skips_active_agent_and_open_periodic_event() {
    let harness = PeriodicHarness::with_interval(30);
    harness.insert_active_agent_run();
    assert_eq!(harness.schedule(&[harness.running_task(41)]), 0);
    harness.finish_active_agent_run();
    harness.insert_open_periodic_event();
    assert_eq!(harness.schedule(&[harness.running_task(41)]), 0);
}
```

- [ ] **Step 2: Run the focused integration tests to verify they fail**

Run: `cargo test --test periodic`
Expected: FAIL because the repository query methods and scheduler implementation are missing.

- [ ] **Step 3: Implement repository queries and bounded event insertion**

Use the stable prefix `periodic-deep-check:v1:` for dedup keys. Add repository methods that filter `events.kind = 'deep_check'` and `dedup_key LIKE 'periodic-deep-check:v1:%'`; the open query must include `pending`, `claimed`, and `retry_wait`.

Implement `PeriodicDeepCheckScheduler` so it:

1. Lists enabled projects and loads each project config.
2. Filters the supplied Pueue snapshot by the project group and `task.is_running()`.
3. Gets the latest periodic event time and active agent state.
4. Calls `should_schedule_deep_check`.
5. Inserts `NewEvent` with `source`, bounded task IDs, task count, and `scheduled_at` only.
6. Uses `insert_idempotent` and counts only newly inserted events.

Make the task start-time parser reusable as `pub(crate)` from `src/reconcile.rs` rather than duplicating timestamp parsing. If a task has no parseable `started_at`, use the current reconciliation time as its first anchor.

- [ ] **Step 4: Run focused tests and inspect payload bounds**

Run: `cargo test --test periodic`
Expected: PASS; no payload contains command text, environment values, prompt text, or transcript text.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml src/reconcile.rs src/periodic.rs src/db/repositories.rs tests/integration/periodic.rs
git commit -m "feat: schedule periodic deep check events"
```

### Task 3: daemon loop と scheduler report に接続する

**Files:**
- Modify: `src/reconcile.rs`
- Modify: `src/daemon.rs`
- Modify: `tests/integration/daemon.rs`
- Modify: `tests/integration/reconciliation.rs`

**Interfaces:**
- `Reconciler::run_once_at(now: i64)` becomes the deterministic entry point; existing `run_once()` delegates to the system clock.
- `DaemonReport` gains `scheduled_deep_checks: usize`.
- The test step extends the existing daemon harness with `run_once_at(now)` and helpers for inserting an active run and counting project events.

- [ ] **Step 1: Add failing daemon tests**

Add tests with these names:

```rust
#[tokio::test]
async fn daemon_schedules_deep_check_after_interval_for_running_task() {
    let harness = DaemonHarness::running_task_with_deep_check_interval(30);
    let report = harness.run_once_at(3_700).await;

    assert_eq!(report.scheduled_deep_checks, 1);
    assert_eq!(harness.pending_event_count(EventKind::DeepCheck), 1);
}

#[tokio::test]
async fn daemon_does_not_schedule_deep_check_while_agent_is_active() {
    let harness = DaemonHarness::running_task_with_deep_check_interval(30);
    harness.insert_active_agent_run();
    let report = harness.run_once_at(3_700).await;

    assert_eq!(report.scheduled_deep_checks, 0);
    assert_eq!(harness.pending_event_count(EventKind::DeepCheck), 0);
}
```

- [ ] **Step 2: Run focused tests to verify they fail**

Run: `cargo test --test daemon daemon_schedules_deep_check -- --exact`
Expected: FAIL because the daemon does not expose a periodic scheduling count or deterministic reconciliation clock.

- [ ] **Step 3: Wire scheduling into `Daemon::run_once`**

Compute `let now = self.now()?` once. Call `Reconciler::run_once_at(now)`, run detection and termination, then call `PeriodicDeepCheckScheduler::schedule(&reconciliation.observed_tasks)` before constructing `Scheduler`. Store its count in `DaemonReport.scheduled_deep_checks`; preserve the existing scheduler ordering so crash, stalled, completion, and operator events retain priority over `DeepCheck`.

- [ ] **Step 4: Run daemon, reconciliation, and scheduler tests**

Run: `cargo test --test daemon --test reconciliation --test scheduler`
Expected: PASS with existing event recovery and scheduler behavior unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/reconcile.rs src/daemon.rs tests/integration/daemon.rs tests/integration/reconciliation.rs
git commit -m "feat: run periodic checks from daemon loop"
```

### Task 4: 設定 template と日本語ドキュメントを整備する

**Files:**
- Modify: `templates/config.toml`
- Modify: `README.md`
- Modify: `templates/instructions.md`
- Test: `tests/integration/config.rs`

- [ ] **Step 1: Add the failing configuration/documentation assertions**

Add config tests that load `deep_check_interval_minutes = 0` and `30`, assert both parse, and assert that `deep_check_every = 6` does not enable the interval when the interval is zero. Add a text assertion that the README contains `deep_check_interval_minutes`, `0` disables it, and the agent records healthy progress in `STATE.md`.

- [ ] **Step 2: Run tests to verify the documentation/config assertions fail**

Run: `cargo test --test config`
Expected: FAIL only for the new README/template assertions.

- [ ] **Step 3: Update the Japanese user-facing documentation**

Set the template default to `deep_check_interval_minutes = 0`. Mark `deep_check_every` as legacy. Add a Japanese section explaining the difference between reconciliation and agent DeepCheck, the token-cost condition, the project-level coalescing rule, and the `STATE.md` recording behavior. Add a `deep_check` instruction that asks for a short health record and forbids inventing metrics not found in the project.

- [ ] **Step 4: Run documentation/config tests**

Run: `cargo test --test config`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add templates/config.toml templates/instructions.md README.md tests/integration/config.rs
git commit -m "docs: explain periodic deep checks in Japanese"
```

## Final Verification

- Run `cargo fmt --all` and then `cargo fmt --check`.
- Run `cargo test --all-targets`.
- Run `git diff --check`.
- Manually verify that `deep_check_interval_minutes = 0` causes no agent run and `30` causes one project-level `deep_check` run for a long-running fake task.
