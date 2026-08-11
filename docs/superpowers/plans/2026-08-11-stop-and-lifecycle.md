# Stop and Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 自律 dispatch、supervisor service、Pueue task、project 登録の停止経路を分離し、`pause`、`start`、`stop`、`cancel`、`disable` を誤解なく使えるようにする。

**Architecture:** Service lifecycle は `ServiceControl` の start/stop/restart API に集約する。project の pause/halt は SQLite state、task の cancel は現在の Pueue signature を再検証してから Pueue kill、登録解除は既存 disable flow のまま責務を分ける。status は service、automation、Pueue、agent を別フィールドで表示する。

**Tech Stack:** Rust 2021、Clap、Tokio、rusqlite migrations、既存 ServiceManager/PueueApi abstractions、integration tests、既存 systemd/launchd adapters。

## Global Constraints

- `pause` は新規 agent、定期 DeepCheck、自動 termination だけを止め、Pueue task と実行中 agent を止めない。
- `stop` は supervisor service だけを止め、Pueue task と project state を変更しない。
- `cancel --task-id ID` は明示した project group の queued/running task だけを対象にし、初期実装では `--all` を提供しない。
- stale task ID、別 project group、terminal task は kill しない。
- `disable` と `disable --remove` の既存の reservation semantics を維持する。
- 既存の SQLite data を破壊せず、operator log の cancel action は migration で追加する。
- 実装後の標準検証は `cargo test --all-targets` と `cargo fmt --check` で行う。

---

### Task 1: service start/stop/restart API を追加する

**Files:**
- Modify: `src/service.rs`
- Test: `tests/integration/service.rs`

**Interfaces:**
- `ServiceControl` gains `start()`, `stop()`, and `restart()`.
- `ServiceManager` maps these operations to systemd user service and launchd user agent without changing Pueue state.
- The test step defines `RecordingService` with `calls: Vec<String>`, a configurable `ServiceStatus`, and an injectable failure result.

- [ ] **Step 1: Add failing fake-service tests**

Extend the existing fake `ServiceControl` implementation and add tests that assert each method invokes exactly one platform operation and propagates a non-zero service-manager status as `AppError`.

```rust
#[test]
fn service_control_exposes_start_stop_and_restart_without_reinstalling() {
    let mut fake = RecordingService::default();
    fake.start().unwrap();
    fake.stop().unwrap();
    fake.restart().unwrap();
    assert_eq!(fake.calls, vec!["start", "stop", "restart"]);
}
```

- [ ] **Step 2: Run focused service tests to verify they fail**

Run: `cargo test --test service service_control_exposes_start_stop_and_restart -- --exact`
Expected: FAIL because the trait methods do not exist.

- [ ] **Step 3: Implement platform commands**

For systemd use `systemctl --user start|stop|restart pueue-agent.service`. For launchd use the existing GUI domain and label: `kickstart -k` for restart, `bootstrap` when start reports the plist is not loaded, and `bootout` for stop. Keep command execution argument-vector based; do not invoke a shell.

- [ ] **Step 4: Run service tests**

Run: `cargo test --test service`
Expected: PASS, including existing rendered service and callback tests.

- [ ] **Step 5: Commit**

```bash
git add src/service.rs tests/integration/service.rs
git commit -m "feat: add supervisor service lifecycle controls"
```

### Task 2: `start` / `stop` CLI commandsと help を追加する

**Files:**
- Modify: `src/cli.rs`
- Modify: `src/main.rs`
- Test: `tests/integration/cli_help.rs`
- Test: `tests/integration/operator_commands.rs`

**Interfaces:**
- Add `Command::Start(ServiceLifecycleArgs)` and `Command::Stop(ServiceLifecycleArgs)`.
- `ServiceLifecycleArgs` contains `--json`; these commands do not resolve a project and do not require a project root.
- Add `commands::start` and `commands::stop` that call `ServiceManager` and print a bounded human/JSON result.

- [ ] **Step 1: Add failing CLI help and command tests**

Assert that `--help` lists `start` and `stop`, each help output contains `--json`, and the command result distinguishes service state from project state.

```rust
#[test]
fn help_lists_service_lifecycle_commands() {
    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .arg("--help")
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("start"));
    assert!(text.contains("stop"));
}
```

- [ ] **Step 2: Run focused tests to verify they fail**

Run: `cargo test --test cli_help help_lists_service_lifecycle_commands -- --exact`
Expected: FAIL because the subcommands are not defined.

- [ ] **Step 3: Implement CLI dispatch**

Use `ServiceManager.start()` and `ServiceManager.stop()`. `start` must verify `ServiceStatus::Running` before returning success. `stop` must report `stopped` only after the service manager command succeeds. JSON output must contain `schema_version`, `operation`, and `service` without project or Pueue task claims.

- [ ] **Step 4: Run CLI and operator tests**

Run: `cargo test --test cli_help --test operator_commands`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cli.rs src/main.rs tests/integration/cli_help.rs tests/integration/operator_commands.rs
git commit -m "feat: add start and stop commands"
```

### Task 3: operator cancel の persistence と SQLite migration を追加する

**Files:**
- Modify: `src/db/migrations.rs`
- Modify: `src/db/repositories.rs`
- Modify: `src/diagnostics.rs`
- Modify: `tests/integration/database.rs`

**Interfaces:**
- Raise `LATEST_SCHEMA_VERSION` from 10 to 11.
- Add the `cancel` action to `operator_logs` while preserving all existing rows.
- Add a repository method that records project, group, task ID, task signature, requested state, final state, and reason as bounded JSON.
- The test step adds `open_v10_operator_log_fixture()`, a test-only helper that creates a schema-v10 SQLite database with five legacy operator log rows.

- [ ] **Step 1: Add failing migration tests**

Create a schema-v10 database containing pause/resume/halt/disable/remove operator logs, open it with the new code, and assert that migration reaches version 11 and preserves all rows. Add a second test inserting `action = 'cancel'` and assert it succeeds.

```rust
#[test]
fn operator_log_migration_preserves_rows_and_allows_cancel() {
    let (database, path) = open_v10_operator_log_fixture();
    let db = Db::open(&path).unwrap();
    let connection = db.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 11);
    assert_eq!(operator_log_count(&db), 5);
    insert_cancel_log(&db);
}
```

- [ ] **Step 2: Run database tests to verify they fail**

Run: `cargo test --test database operator_log_migration_preserves_rows_and_allows_cancel -- --exact`
Expected: FAIL because schema version 11 and the new action are absent.

- [ ] **Step 3: Implement the v11 migration**

Update the base `operator_logs` action check and add `migrate_operator_logs_to_v11`. Rename the existing table inside the migration transaction, create the same columns with actions `pause`, `resume`, `halt`, `disable`, `remove`, and `cancel`, copy rows, drop the legacy table, recreate the project/created index, and set `PRAGMA user_version = 11`. Update the early-return invariant check so a v11 database returns without rebuilding.

Keep cancel details bounded and redacted before serialization. Add `cancel` to the required schema checks in `doctor` only if the current schema check can inspect the action definition; otherwise the migration version check is sufficient.

- [ ] **Step 4: Run migration and doctor tests**

Run: `cargo test --test database --test diagnostics`
Expected: PASS with old schema fixtures and current database fixtures.

- [ ] **Step 5: Commit**

```bash
git add src/db/migrations.rs src/db/repositories.rs src/diagnostics.rs tests/integration/database.rs
git commit -m "feat: persist operator task cancellation"
```

### Task 4: project-scoped `cancel --task-id` を実装する

**Files:**
- Create: `src/cancel.rs`
- Modify: `src/lib.rs`
- Modify: `src/cli.rs`
- Modify: `src/main.rs`
- Test: `tests/integration/operator_commands.rs`
- Test: `tests/integration/pueue_adapter.rs`

**Interfaces:**
- Add `CancelArgs { task_id: i64, json: bool, pueue_config: Option<PathBuf>, project_root: Option<PathBuf> }`.
- Add `pub async fn cancel_task_with(db: &Db, project: &Project, pueue: &impl PueueApi, task_id: i64, now: i64) -> Result<CancelResult, AppError>`.
- `CancelResult` contains task ID, signature, request state, final observed state, and whether `kill` was sent.
- The test step defines `CancelHarness` with a temp project database, an in-memory Pueue task list, kill-call recording, and operator-log query helpers.

- [ ] **Step 1: Add failing cancel tests**

Cover the required safety cases:

```rust
#[tokio::test]
async fn cancel_kills_only_a_running_task_in_the_project_group() {
    let harness = CancelHarness::with_running_task(41, "pa-project");
    let result = harness.cancel(41).await.unwrap();
    assert!(result.kill_sent);
    assert_eq!(harness.kill_calls(), vec![41]);
    assert!(harness.operator_log_contains("cancel"));
}

#[tokio::test]
async fn cancel_refuses_other_group_stale_id_and_terminal_task() {
    let harness = CancelHarness::with_running_task(41, "other-group");
    assert!(harness.cancel(41).await.is_err());
    assert!(harness.kill_calls().is_empty());
}
```

- [ ] **Step 2: Run focused tests to verify they fail**

Run: `cargo test --test operator_commands cancel_ -- --nocapture`
Expected: FAIL because the command and cancel service do not exist.

- [ ] **Step 3: Implement signature revalidation and kill**

Call `pueue.status_json()` first. Find exactly one task with the requested ID and require `task.group == project.pueue_group` and a non-terminal state. Compute the same stable task signature used by reconciliation, record an operator cancel request, call `pueue.kill(task_id)`, query status once more for the final observed state, and record the result. Do not call `kill` after status failure, group mismatch, terminal state, or ambiguous task ID. Do not use a raw OS signal.

- [ ] **Step 4: Wire the CLI and run tests**

Resolve the project like `status` and `disable`, create the configured Pueue adapter, call `cancel_task_with`, and render human output with `task=`, `state=`, and `summary:`. JSON must include `schema_version`, `project_id`, `task_id`, `kill_sent`, and final state. Run:

```bash
cargo test --test operator_commands --test pueue_adapter
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cancel.rs src/lib.rs src/cli.rs src/main.rs tests/integration/operator_commands.rs tests/integration/pueue_adapter.rs
git commit -m "feat: add explicit project task cancellation"
```

### Task 5: status の automation/service/task 表示と日本語運用 docs を追加する

**Files:**
- Modify: `src/status.rs`
- Modify: `src/diagnostics.rs`
- Test: `tests/integration/operator_commands.rs`
- Create: `docs/operations-ja.md`
- Modify: `README.md`
- Modify: `templates/instructions.md`

- [ ] **Step 1: Add failing output assertions**

Extend the existing status tests to require `service:`, `automation:`, `project: enabled=... paused=... halted=...`, `pueue:`, and `agent_runs:` as separate lines. Add JSON assertions for `service` and `automation` fields.

- [ ] **Step 2: Run status tests to verify they fail**

Run: `cargo test --test operator_commands status_ -- --nocapture`
Expected: FAIL because the automation projection is not yet rendered.

- [ ] **Step 3: Implement status projections**

Use `service_status_label` for `service`. Derive automation as `disabled` when `!enabled`, `halted` when `halted_reason.is_some()`, `paused` when `paused`, otherwise `active`. Do not infer Pueue task state from the automation label. Preserve bounded redaction and existing JSON schema version.

- [ ] **Step 4: Write the Japanese operations guide**

Create `docs/operations-ja.md` with a command matrix, concrete workflows for “自律動作だけ止める”, “supervisor だけ止める”, “実験を止める”, and “project 登録を解除する”, plus the warning that `stop` and `disable` do not kill Pueue tasks. Add the same concise section to README and tell agents in `templates/instructions.md` not to substitute `stop`, `pause`, and `cancel`.

- [ ] **Step 5: Run output and documentation tests**

Run: `cargo test --test operator_commands --test diagnostics` and `git diff --check`
Expected: PASS and no whitespace errors.

- [ ] **Step 6: Commit**

```bash
git add src/status.rs src/diagnostics.rs tests/integration/operator_commands.rs docs/operations-ja.md README.md templates/instructions.md
git commit -m "docs: clarify stop and lifecycle operations"
```

## Final Verification

- Run `cargo fmt --all` and then `cargo fmt --check`.
- Run `cargo test --all-targets`.
- Run `git diff --check`.
- Verify manually that `stop` leaves a fake Pueue task running, `pause` leaves both the task and current agent running, and `cancel --task-id` sends exactly one kill only for the registered group.
