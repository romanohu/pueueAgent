# Event Run Ack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** agent process の実行結果と durable SQLite 更新を event の ack とし、spawn 成功だけでは completed にせず、failure/timeout/restart interruption を retry または dead-letter として project-scoped に解決する。

**Architecture:** `EventStatus` に `in_flight`、`dispatched`、`dead_letter` を追加する。claim、run/event binding、launch gate ack、process terminal result の各境界を repository の immediate transaction に集約し、`AgentHandle` と daemon recovery は同じ finalizer を呼ぶ。project config から解決した `RetryPolicy` を repository に渡し、grouped event も event ごとの attempts で判定する。

**Tech Stack:** Rust 2021、Tokio process lifecycle、rusqlite WAL/immediate transactions、Serde/TOML、既存の `agent_run_events` 複合 foreign key、既存 integration tests。

## Global Constraints

- 外部 Slack/webhook 通知と外部 delivery worker は作らない。
- event は agent spawn 成功だけでは completed にせず、process 正常終了と永続更新成功の後だけ completed にする。
- `agent.max_retries` は初回以外の retry 回数とし、`max_retries=0` の最初の実行失敗は dead-letter にする。
- retry delay は base 60 秒、`60 * 2^(attempts - 1)`、exponent 6 cap の bounded exponential backoff とする。
- event/run 解決は project-scoped な immediate transaction とし、既存 `agent_run_events`、launch gate、leases、recovery を再利用する。
- durable submission/proposal の idempotency 経路は再利用し、この slice で全面再設計しない。
- SQLite の `events.status='dead_letter'` を唯一の dead-letter source of truth とし、専用 delivery worker/table は追加しない。
- batch lineage、budget reservation、goal state machine、token accounting は変更しない。
- 実装後は `cargo fmt --all -- --check`、`cargo test --all-targets`、`git diff --check` を成功させる。

---

### Task 1: Event status 契約、retry policy、schema 13 migration

**Files:**
- Modify: `src/models.rs:75-105` (`EventStatus`)
- Create: `src/retry.rs`
- Modify: `src/lib.rs` (`pub mod retry`)
- Modify: `src/db/migrations.rs:7-12, 58-105, 360-390` (`LATEST_SCHEMA_VERSION`, CREATE SQL、`migrate_events_to_v13`)
- Modify: `tests/integration/database.rs` (`create_legacy_schema_without_active_agent_index` と schema/status tests)

**Interfaces:**
- Produces `EventStatus::{InFlight,Dispatched,DeadLetter}` with DB values `in_flight`, `dispatched`, `dead_letter`.
- Produces `retry::{RetryPolicy,RetryDecision,retry_backoff_seconds}`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_retries: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    Retry { not_before: i64 },
    DeadLetter,
}

pub fn retry_decision(attempts: i64, now: i64, policy: RetryPolicy) -> RetryDecision;
pub fn retry_backoff_seconds(attempts: i64) -> i64;
```
- Consumes no later task interfaces.

- [ ] **Step 1: Write the failing model and retry tests.** Add a test that iterates over `EventStatus::{Pending,Claimed,InFlight,Dispatched,Completed,RetryWait,Failed,DeadLetter}`, writes each through SQLite, and reads the same value. Add `retry_policy_uses_attempt_number_and_zero_retry_is_dead_letter` with exact assertions:

```rust
assert_eq!(retry_decision(1, 1_000, RetryPolicy { max_retries: 0 }), RetryDecision::DeadLetter);
assert_eq!(retry_decision(1, 1_000, RetryPolicy { max_retries: 2 }), RetryDecision::Retry { not_before: 1_060 });
assert_eq!(retry_decision(2, 1_000, RetryPolicy { max_retries: 2 }), RetryDecision::Retry { not_before: 1_120 });
assert_eq!(retry_decision(3, 1_000, RetryPolicy { max_retries: 2 }), RetryDecision::DeadLetter);
assert_eq!(retry_backoff_seconds(20), 3_840);
```

- [ ] **Step 2: Run the tests to verify RED.** Run `cargo test --test database all_event_kind_and_status_values_round_trip_through_sqlite` and `cargo test --lib retry_policy_uses_attempt_number_and_zero_retry_is_dead_letter`; expect compile failure because the new enum variants and `src/retry.rs` do not exist.

- [ ] **Step 3: Implement the minimal status and retry policy.** Extend the `database_enum!(EventStatus { ... })` declaration. Implement `retry_backoff_seconds` with `attempts.saturating_sub(1).clamp(0, 6)` and `60_i64.saturating_mul(2_i64.saturating_pow(exponent))`; implement `retry_decision` as `attempts <= max_retries` => `Retry { now.saturating_add(delay) }`, otherwise `DeadLetter`. Export `pub mod retry` from `src/lib.rs`.

- [ ] **Step 4: Write the migration RED test.** Create a schema-12 fixture with the current events table and one event, open it through `Db::open`, assert `PRAGMA user_version == 13`, assert `sqlite_master.sql` contains all eight event statuses, and execute an invalid status insert expecting a SQLite CHECK error. Keep the existing event row and all event indexes in the assertions.

- [ ] **Step 5: Implement schema 13.** Set `LATEST_SCHEMA_VERSION` to 13, add the three values to the version-0 `events` CHECK, call `migrate_events_to_v13` when `version <= 12`, and use one exact old status-list replacement in `PRAGMA writable_schema` with a replacement-count assertion. Run `PRAGMA integrity_check` before committing the migration transaction. Do not change event rows, foreign keys, payloads, attempts, leases, or timestamps.

- [ ] **Step 6: Run the focused GREEN tests.** Run `cargo test --test database all_event_kind_and_status_values_round_trip_through_sqlite`, `cargo test --test database schema_v12_migration_adds_event_run_ack_states_and_rejects_unknown_status`, and `cargo test --lib retry`; expect PASS.

### Task 2: Atomic run binding, dispatch ack、terminal event resolution

**Files:**
- Modify: `src/db/repositories.rs:471-1120` (`EventRepository` transitions)
- Modify: `src/db/repositories.rs:2820-3300` (`AgentRunRepository` binding, gate ack, recovery, finalizer)
- Modify: `tests/integration/database.rs:2755-3175` (repository transaction tests)

**Interfaces:**
- `AgentRunRepository::insert_with_events_and_reservation` keeps its existing arguments and atomically changes every project-owned `claimed` event in `event_ids` to `in_flight`.
- Add `AgentRunRepository::acknowledge_dispatch(&self, project_id: &str, run_id: i64) -> Result<usize, AppError>`; it must atomically change the run gate from `release_requested` to `released` and all linked `in_flight` events to `dispatched`.
- Add `AgentRunRepository::finish_and_resolve_events(&self, project_id: &str, run_id: i64, status: AgentRunStatus, finished_at: i64, exit_code: Option<i64>, last_error: Option<&str>, policy: RetryPolicy) -> Result<AgentRun, AppError>`; it updates the run, interventions, and every linked event in one transaction.
- Change `fail_before_gate_release` to accept `RetryPolicy` and resolve linked `in_flight` events with the same terminal policy.
- Add `EventRepository::resolve_claimed_without_run(project_id: &str, event_ids: &[i64], now: i64, reason: &str, policy: RetryPolicy)` for the claim-to-binding crash window.

- [ ] **Step 1: Write RED tests for the first ack.** Add `agent_run_binding_moves_claimed_events_to_in_flight_atomically`: claim two events, call `insert_with_events_and_reservation`, assert both event rows are `in_flight`, lease NULL, the run is `starting`, and both `agent_run_events` rows carry the same project. Add a trigger that rejects the second attachment and assert the run, links, and status changes all roll back to the pre-call `claimed` rows.

- [ ] **Step 2: Write RED tests for dispatch ack and project scope.** Add `dispatch_ack_moves_only_project_owned_inflight_events`: create one project-owned run/event and one foreign event, call `acknowledge_dispatch("project-a", run_id)`, assert one event becomes `dispatched`, the gate becomes `released`, and a foreign run/event ID returns `AppError::Validation` without changing either project.

- [ ] **Step 3: Write RED tests for final ack and per-event retry.** Add `finish_and_resolve_events_completes_only_after_run_success` and `failed_group_resolves_events_independently_by_attempt`: bind two events with attempts 1 and 3, finish a failed run with `RetryPolicy { max_retries: 2 }`, assert the first is `retry_wait` with `not_before=now+60`, the second is `dead_letter`, and both have `lease_until=NULL`; finish a successful run in a separate fixture and assert all linked events become `completed` only in the same committed read as run=`completed` and reserved interventions released.

- [ ] **Step 4: Write the rollback and ownership tests.** Add `finish_and_resolve_events_rolls_back_run_events_and_interventions_on_sql_failure` with a trigger rejecting event update; assert run remains active, event remains `dispatched`, and intervention remains reserved. Add `cross_project_dispatch_and_finish_are_rejected` and assert no row outside the requested project changes.

- [ ] **Step 5: Run the repository tests to verify RED.** Run `cargo test --test database agent_run_binding_moves_claimed_events_to_in_flight_atomically`; then run the complete `cargo test --test database` suite to expose the remaining missing methods and old status assertions.

- [ ] **Step 6: Implement the repository transactions.** In the existing immediate transactions, validate each event with `WHERE project_id=? AND status='claimed'`, insert links, then update statuses. In `acknowledge_dispatch`, require `release_requested`, update the run and linked events together, and return the changed event count. In `finish_and_resolve_events`, select linked event IDs using both project columns, compute `RetryDecision` for each event inside the transaction, update event status/not-before/completed-at/error, update run terminal fields, and release reserved interventions. Return an error before commit when an event/run is missing, foreign, or in an unexpected state.

- [ ] **Step 7: Run the repository tests to verify GREEN.** Run `cargo test --test database`; expect all repository and migration tests to pass.

### Task 3: Launch gate and AgentHandle finalization

**Files:**
- Modify: `src/agent.rs:100-560` (`AgentHandle`, `AgentRunner::spawn`)
- Modify: `tests/integration/scheduler.rs:563-900,1361-1424` (process lifecycle tests)
- Modify: `src/db/repositories.rs:3150-3280` only where the launch-gate calls need the Task 2 interfaces

**Interfaces:**
- `AgentHandle` adds `project_id: String`, `retry_policy: RetryPolicy`, and a stored terminal child outcome so a failed finalizer can be retried without waiting on/reaping the child a second time.
- `AgentHandle::poll`, `wait`, and `timeout_now` all call `finish_and_resolve_events`; none calls `finish` directly.
- `AgentRunner::spawn` returns a handle only after `acknowledge_dispatch` commits, and calls the policy-aware `fail_before_gate_release` for every pre-dispatch failure.

- [ ] **Step 1: Write the spawn/terminal RED tests.** Update `crash_and_deep_check_for_one_project_start_one_crash_run` and `operator_wake_uses_the_existing_scheduler_dispatch_path` to assert `EventStatus::Dispatched` immediately after scheduler start, then call `handle.wait` and assert `Completed`. Add `spawn_success_leaves_events_dispatched_until_process_exit` with a sleeping fake agent and assert the event remains dispatched while the child is alive.

- [ ] **Step 2: Write failure/timeout RED tests.** Add `agent_nonzero_exit_retries_event_after_backoff` using a fake executable that exits 7; call `wait`, assert run=`failed`, event=`retry_wait`, `attempts=1`, and `not_before=now+60`. Add `agent_timeout_dead_letters_when_max_retries_zero` with a sleeping executable and config `max_retries=0`; force the deadline, call `timeout_now`, and assert run=`timed_out`, event=`dead_letter`, and a bounded timeout reason.

- [ ] **Step 3: Write finalizer retry RED test.** Add `failed_finalizer_is_retried_without_losing_terminal_process_outcome`: install an update trigger that rejects the first event finalization, observe `poll` return an error while the event remains dispatched, remove the trigger, poll again, and assert one terminal run plus one event resolution and no second child process.

- [ ] **Step 4: Run lifecycle tests to verify RED.** Run `cargo test --test scheduler spawn_success_leaves_events_dispatched_until_process_exit`; then run `cargo test --test scheduler` to expose the remaining old immediate-completed behavior and missing policy-aware finalizer.

- [ ] **Step 5: Implement launch-gate dispatch ack.** Preserve the existing shell marker and `released\n` acknowledgement. After reading the acknowledgement, call `acknowledge_dispatch(&project.project_id, run.run_id)`; do not return a handle before it succeeds. If that transaction fails, terminate the child and call the policy-aware pre-release failure path. Preserve marker inspection for startup recovery; never use marker existence to complete an event.

- [ ] **Step 6: Implement the shared AgentHandle finalizer.** Convert process exit, wait error, and timeout into one stored outcome. On the first outcome, call `finish_and_resolve_events`; on a database failure, retain the outcome and return the error. On the next `poll`, retry the same transaction. Remove the handle only after the finalizer returns successfully.

- [ ] **Step 7: Run lifecycle tests to verify GREEN.** Run `cargo test --test scheduler`; expect process tree cleanup and all finalization assertions to pass.

### Task 4: Scheduler dispatch/error paths and grouped retry semantics

**Files:**
- Modify: `src/scheduler.rs:190-320,369-371` (`tick`, failure paths, shared retry policy)
- Modify: `tests/integration/scheduler.rs:446-1040` (scheduler expectations and grouped tests)

**Interfaces:**
- `Scheduler::tick` loads `project_config.agent.max_retries` into `RetryPolicy` and passes it to `AgentRunner::spawn`/repository failure paths.
- A successful `spawn` appends `StartedAgent` without changing events to completed; AgentHandle owns the final ack.
- Config/guardrail/project lookup errors continue to use `EventStatus::Failed`; `UpgradeInProgress` continues to use `defer_claimed` without consuming attempts.

- [ ] **Step 1: Update existing RED expectations.** Change `operator_intervention_delivery_releases_rows_when_process_spawn_fails`, `event_attachment_failure_rolls_back_the_agent_run`, `log_open_failure_finishes_the_inserted_agent_run`, `process_spawn_failure_finishes_the_inserted_agent_run`, and `mark_running_failure_finishes_the_run_and_terminates_the_spawned_process` to assert retry_wait/dead_letter through the policy instead of relying on the old direct `transition_many` path. Keep the existing run/intervention cleanup assertions.

- [ ] **Step 2: Add grouped per-attempt RED test.** Add `grouped_event_failure_applies_per_event_retry_limit`: claim two same-project events, set one to attempts=1 and the other to attempts=3, run a failing fake agent, call `wait`, and assert exactly one retry_wait and one dead_letter. Assert the next scheduler tick claims only the retry_wait event after its `not_before`.

- [ ] **Step 3: Run scheduler tests to verify RED.** Run `cargo test --test scheduler`; expect failures because `tick` still marks all started events completed and the spawn failure paths do not use the new resolver.

- [ ] **Step 4: Remove premature completion.** Delete the successful-spawn `transition_many(... Completed ...)` call. Keep `StartedAgent.event_ids` for prompt/reporting, but let AgentHandle’s finalizer resolve those event IDs through the run binding.

- [ ] **Step 5: Route every non-upgrade spawn error through policy-aware resolution.** For pre-binding errors, call `EventRepository::resolve_claimed_without_run`; for a run that was inserted, use `fail_before_gate_release`. Use each event’s attempts to choose retry_wait/dead_letter, and release intervention reservations in the same transaction. Preserve first-error reporting and project loop continuation.

- [ ] **Step 6: Run scheduler tests to verify GREEN.** Run `cargo test --test scheduler`; then run `cargo test --test pueue_adapter --test periodic` to ensure durable submission/reconciliation and periodic scheduling continue to use the same event lease boundary.

### Task 5: Startup recovery with project config policy

**Files:**
- Modify: `src/db/repositories.rs:Projects repository` (add `ProjectRepository::list_all`)
- Modify: `src/db/repositories.rs:2860-3095` (`recover_interrupted` signature and project-scoped transactions)
- Modify: `src/daemon.rs:80-115` (`startup_retry_policies` and recovery ordering)
- Modify: `tests/integration/daemon.rs:607-880`
- Modify: `tests/integration/database.rs:2258-2534` (marker/recovery expectations)

**Interfaces:**
- Add `ProjectRepository::list_all() -> Result<Vec<Project>, AppError>` ordered by project ID.
- Add `Daemon::load_startup_retry_policies() -> Result<BTreeMap<String, RetryPolicy>, AppError>`; it reads every project config and returns an error before opening a recovery transaction if any config is invalid.
- Change `AgentRunRepository::recover_interrupted(finished_at: i64, reason: &str, policies: &BTreeMap<String, RetryPolicy>) -> Result<AgentRunRecovery, AppError>`; each project’s recovery is one immediate transaction and uses only that project’s policy.

- [ ] **Step 1: Write RED tests for interrupted event classes.** Add `startup_recovery_retries_inflight_and_dispatched_events`, `startup_recovery_dead_letters_when_attempt_exhausted`, and `startup_recovery_recovers_claimed_event_without_run_binding`. Create starting/running fixtures and assert all runs become failed, in_flight/dispatched/unbound claimed events become retry_wait/dead_letter by attempts, leases clear, and interventions follow existing marker rules.

- [ ] **Step 2: Write RED tests for policy responsibility.** Add `startup_recovery_loads_disabled_project_config` with a disabled project and an interrupted run; assert recovery still uses its config. Add `startup_recovery_invalid_project_config_makes_no_mutations`; corrupt one config, call `Daemon::run_once`, assert the call errors, `recovered_agent_runs==0`, `startup_recovery_pending` remains effective for the next tick, and all run/event statuses are unchanged.

- [ ] **Step 3: Run daemon/database recovery tests to verify RED.** Run `cargo test --test daemon startup_recovery_retries_inflight_and_dispatched_events`; then run `cargo test --test daemon` and `cargo test --test database` to expose old recovery that requeues only claimed linked rows and ignores policy/dead-letter.

- [ ] **Step 4: Implement policy loading before recovery.** In `Daemon::run_once`, resolve all project configs before calling `recover_interrupted`; set `startup_recovery_pending=false` only after the recovery call succeeds. Use `list_all`, not `list_active`, so paused/disabled projects are covered.

- [ ] **Step 5: Implement project-scoped recovery.** Preserve marker inspection for `release_requested` runs, but treat marker confirmation as launch evidence only. In each project transaction, fail active runs, resolve their in_flight/dispatched links and claimed rows without a run using `RetryDecision`, clear leases, and retain unchanged rows when the transaction fails. Aggregate `AgentRunRecovery` counts across successful project transactions.

- [ ] **Step 6: Run recovery tests to verify GREEN.** Run the focused daemon/database commands from Step 3, then `cargo test --test daemon`; expect existing restart, intervention, and atomic recovery tests to pass with updated status assertions.

### Task 6: Bounded local observability for ack and dead-letter

**Files:**
- Modify: `src/db/repositories.rs:EventRepository` (add project-scoped latest run projection)
- Modify: `src/diagnostics.rs:57-120,926-1130,780-890` (`events`, status JSON, doctor checks)
- Modify: `src/status.rs:95-225,301-360` (human and compact status counts)
- Modify: `tests/integration/diagnostics.rs:713-905,1241-1290,1500-1675`

**Interfaces:**
- Add `EventRepository::latest_run_id(project_id: &str, event_id: i64) -> Result<Option<i64>, AppError>` with both project predicates in the join and deterministic `started_at DESC, run_id DESC` ordering.
- Extend diagnostics `EventCounts` with `in_flight`, `dispatched`, and `dead_letter`; preserve existing `failed` and JSON schema version 1.
- Extend human/compact event count lines to print `retry_wait`, `in_flight`, `dispatched`, `failed`, and `dead_letter` separately.
- Add read-only doctor checks named `events.ack_consistency` and `events.dead_letter`.

- [ ] **Step 1: Write RED event projection tests.** Extend `events_projection_filters_project_events_and_emits_bounded_fields` with a dispatched event linked to a run and a dead-letter event. Assert JSON has bounded `status`, `attempts`, `not_before`, and `run_id`, contains no payload/prompt, and `--status dead_letter` returns only the dead-letter row.

- [ ] **Step 2: Write RED status/doctor tests.** Extend `status_json_counts_project_interventions_without_exposing_message_bodies` with one row of each new state and assert `events.counts.in_flight`, `.dispatched`, `.retry_wait`, `.dead_letter`. Add `doctor_reports_dead_letter_and_ack_consistency_without_repair`: assert dead-letter produces a warning, an in_flight row without a link produces an error, and the database statuses/leases are unchanged after `build_doctor_report`.

- [ ] **Step 3: Run diagnostics tests to verify RED.** Run `cargo test --test diagnostics events_projection_filters_project_events_and_emits_bounded_fields`; then run `cargo test --test diagnostics` to expose missing fields/checks and old counts.

- [ ] **Step 4: Implement bounded projections.** Use the existing project-scoped `EventRepository::list_filtered` output and latest run query; do not include event payload or log text. Add the new enum states to human and JSON renderers, keep `failed` distinct from `dead_letter`, and use `bounded_redacted_text` for errors/remediation.

- [ ] **Step 5: Implement read-only doctor checks.** Count dead-letter events for the requested project and emit warning/remediation `events --status dead-letter`. Count in_flight/dispatched rows with no same-project `agent_run_events` link and emit an error. Do not call migration, recovery, transition, or retry methods.

- [ ] **Step 6: Run observability tests to verify GREEN.** Run `cargo test --test diagnostics --test cli_help`; then inspect `cargo run -- events --help` and `cargo run -- status --help` to ensure the enum values remain discoverable.

### Task 7: Existing expectation updates, full verification, and handoff

**Files:**
- Modify: `tests/integration/database.rs:4014-4044` (all enum round-trip values)
- Modify: `tests/integration/scheduler.rs:563-1040` (post-spawn `Dispatched`, terminal finalizer)
- Modify: `tests/integration/daemon.rs:446-880` (finished/recovery counts)
- Modify: `tests/integration/diagnostics.rs:423-450,713-905,1241-1290` (new counts/projections)
- Verify: `src/models.rs`, `src/retry.rs`, `src/db/migrations.rs`, `src/db/repositories.rs`, `src/agent.rs`, `src/scheduler.rs`, `src/daemon.rs`, `src/status.rs`, `src/diagnostics.rs`

**Interfaces:**
- Consumes all previous tasks.
- Produces a host-independent, fully tested implementation with no changes to batch/budget/goal/token subsystems.

- [ ] **Step 1: Update all existing expected values.** Change `crash_and_deep_check_for_one_project_start_one_crash_run`, `operator_wake_uses_the_existing_scheduler_dispatch_path`, and `retry_wait_events_obey_not_before_backoff` to observe `dispatched` before `handle.wait` and `completed` after finalization. Change migration and enum tests to include `in_flight`, `dispatched`, and `dead_letter`. Preserve tests that expect pre-dispatch config/guardrail errors to remain `failed`.

  Before calling the stricter run-binding transaction, update these existing fixtures to claim their event IDs (or use `AgentRunRepository::insert` when they are testing a run without event delivery): `agent_run_insert_with_events_persists_the_run_and_all_event_links`, `agent_run_insert_with_events_rolls_back_when_an_event_attachment_is_invalid`, `interventions_bind_to_runs_and_apply_or_release_in_agent_run_transactions`, `interventions_mark_running_and_application_are_atomic`, `pre_release_gate_failure_requeues_applied_interventions`, `startup_recovery_requeues_applied_interventions_after_release_request_before_ack`, `startup_recovery_promotes_marker_confirmed_release_request_and_retains_applied_interventions`, `startup_recovery_requeues_an_applied_intervention_before_gate_release`, and `confirmed_gate_release_does_not_requeue_applied_interventions_on_recovery`. Keep `mark_gate_released_rejects_a_run_without_a_release_request` as a gate-state rejection test, and add assertions that every event passed to a production binding call is `claimed` before the call.

- [ ] **Step 2: Run formatting and focused suites.** Run `cargo fmt --all -- --check`; run `cargo test --test database`, `cargo test --test scheduler`, `cargo test --test daemon`, and `cargo test --test diagnostics`. Expected result: all PASS with no ignored host-dependent tests added.

- [ ] **Step 3: Run all host-independent tests.** Run `cargo test --all-targets`. Confirm the existing shell/Pueue integration tests remain unchanged and no test requires Slack, webhook, network, or a live daemon service.

- [ ] **Step 4: Review crash windows and scope.** Run `rg -n "transition_many\(.*Completed|dead_letter|in_flight|dispatched|recover_interrupted|finish_and_resolve_events" src tests`; confirm no scheduler path completes an event on spawn, all terminal process paths use the shared finalizer, and all new SQL joins include project predicates. Run `git diff --check`.

- [ ] **Step 5: Commit the implementation separately from this docs commit.** Stage only the source and test files listed above with `git add`, create the implementation commit `feat: acknowledge event runs durably`, and report the focused/full test commands and their outputs. Do not add external notification code or unrelated roadmap slices.

## Self-review checklist

- EventStatus names and DB CHECK values are defined once in Task 1 and used consistently by repositories, scheduler, recovery, diagnostics, and tests.
- `max_retries=0`, attempts 1-based semantics, grouped per-event resolution, and bounded backoff are covered by Tasks 1, 2, and 4.
- The claim-to-binding, gate marker/ack, SQLite dispatch ack, process-exit, and startup-recovery crash windows are covered by Tasks 2, 3, and 5.
- Config loading responsibility and invalid-config no-mutation behavior are covered by Task 5.
- Dead-letter visibility in `events`, `runs`, `status`, and read-only `doctor` is covered by Task 6.
- No task introduces Slack/webhook delivery, batch lineage, budget reservation, goal state, or token accounting.
- Every production change has a named file/function and every behavior has a named RED/GREEN test command; no step relies on an unspecified future action.
