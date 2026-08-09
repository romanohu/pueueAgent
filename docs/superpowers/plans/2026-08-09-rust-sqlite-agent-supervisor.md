# Rust + SQLite Agent Supervisor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fragile per-project Bash coordination layer with one Rust supervisor per Pueue daemon, backed by SQLite, while preserving the simple `pueue-agent submit -- command...` workflow and enabling policy-controlled termination of clearly failed Pueue tasks.

**Architecture:** A single Rust binary provides both short-lived CLI commands and a long-running `daemon` subcommand. Callback ingestion and periodic reconciliation write idempotent events to a shared SQLite database; the scheduler claims events with leases, coalesces them per project, applies guardrails, and starts at most one agent per project. Pueue remains the task scheduler and the only task-control boundary.

**Tech Stack:** Rust stable, edition 2021, `clap` for CLI parsing, `rusqlite` with the bundled SQLite build, `serde`/`serde_json` for data, `toml` for configuration, `thiserror` for typed errors, `uuid` for stable project IDs, `tokio`/`tokio-util` for the supervisor loop and child-process lifecycle, `async-trait` for injectable async adapters, `tempfile` and `assert_cmd` for tests.

## Global Constraints

- One supervisor runs per Pueue daemon/profile; projects are isolated by stable `project_id`.
- SQLite is the source of truth for projects, events, incidents, submissions, termination requests, and agent runs.
- SQLite uses WAL mode, foreign keys, a busy timeout, and explicit transactions.
- Pueue remains the task scheduler; the supervisor may invoke `pueue add` and explicitly configured `pueue kill`, but must not use raw OS signals for task termination.
- Automatic termination is opt-in per detector action; default behavior is `wake` for strong configured patterns and `notify` for soft signals such as stalled output.
- The default agent launcher uses an executable plus argument vector and never interpolates an arbitrary command into `bash -c`.
- `STATE.md` and `instructions.md` remain the agent-facing project files.
- Normal health checks must not start an agent.
- Every event claim has a lease and is recoverable after supervisor restart.
- A project may have at most one active agent run unless a future configuration explicitly permits more.
- The migration must preserve the current Bash implementation until Rust parity is verified.
- Linux systemd user services and macOS launchd must be supported; interactive shell environment variables are not service configuration.

---

## Repository Map

The Rust implementation is added at the repository root while the existing Bash implementation remains available during migration.

```text
Cargo.toml
src/
  main.rs
  cli.rs
  error.rs
  paths.rs
  config.rs
  models.rs
  db/
    mod.rs
    migrations.rs
    repositories.rs
  pueue.rs
  project.rs
  submit.rs
  events.rs
  reconcile.rs
  detect.rs
  incidents.rs
  termination.rs
  guardrails.rs
  agent.rs
  scheduler.rs
  daemon.rs
  service.rs
tests/
  integration/
  support/
assets/
  systemd/pueue-agent.service
  launchd/com.pueue-agent.plist
```

The current `lib/*.sh`, `bin/pueue-agent`, and Bash tests remain unchanged
until the migration task explicitly switches the entry point. New Rust tests
must cover concurrency and restart behavior that the current sequential Bash
tests cannot express.

## Task 1: Rust workspace and typed CLI skeleton

**Files:**
- Create: `Cargo.toml`
- Create: `Cargo.lock`
- Create: `src/main.rs`
- Create: `src/cli.rs`
- Create: `src/error.rs`
- Test: `tests/integration/cli_help.rs`

**Interfaces:**
- `Cli` parses `init`, `enable`, `disable`, `submit`, `event`, `status`, `pause`, `resume`, and `daemon`.
- `AppError` is the crate-wide error type and renders actionable context without leaking command secrets.
- `main` returns exit code 0 for successful commands, 2 for invalid CLI input, and 1 for runtime errors.

- [ ] **Step 1: Write the failing CLI tests.**

```rust
#[test]
fn help_lists_submit_and_daemon_commands() {
    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("submit"));
    assert!(text.contains("daemon"));
}
```

- [ ] **Step 2: Run the focused test and confirm it fails because the Rust binary does not exist.**

Run: `cargo test --test cli_help`

Expected: FAIL because the crate and binary have not been created.

- [ ] **Step 3: Add the crate manifest and CLI dispatch.**

Use a binary target named `pueue-agent`. Define the dependencies listed in the plan header and keep command handlers as modules returning `Result<(), AppError>`.

- [ ] **Step 4: Run the focused test and the formatter.**

Run: `cargo fmt --check && cargo test --test cli_help`

Expected: PASS.

- [ ] **Step 5: Commit the scaffold.**

```bash
git add Cargo.toml Cargo.lock src/main.rs src/cli.rs src/error.rs tests/integration/cli_help.rs
git commit -m "feat: add Rust CLI skeleton"
```

## Task 2: Paths, TOML configuration, and project identity

**Files:**
- Create: `src/paths.rs`
- Create: `src/config.rs`
- Create: `src/project.rs`
- Modify: `src/cli.rs`
- Test: `tests/integration/config.rs`

**Interfaces:**
- `paths::state_db_path() -> Result<PathBuf, AppError>` resolves the central state database using `XDG_STATE_HOME` with a macOS fallback.
- `project::find_root(start: &Path) -> Result<PathBuf, AppError>` walks upward for `.pueue-agent/config.toml`.
- `config::load(path: &Path) -> Result<ProjectConfig, AppError>` parses TOML and validates numeric ranges and detector actions.
- `ProjectConfig` contains `project_id`, `pueue_group`, agent executable/args, check configuration, and guardrails.
- `PatternAction` is the enum `Notify`, `Wake`, or `Kill`.

- [ ] **Step 1: Write tests for valid TOML, invalid ranges, project-root discovery, and stable group generation.**

```rust
#[test]
fn command_is_an_argument_vector_not_a_shell_string() {
    let config = load_fixture("valid-config.toml").unwrap();
    assert_eq!(config.agent.program, "codex");
    assert_eq!(config.agent.args, vec!["exec", "{prompt}"]);
}

#[test]
fn zero_stall_interval_is_rejected() {
    let error = load_fixture("stall-zero.toml").unwrap_err();
    assert!(error.to_string().contains("stall_minutes"));
}
```

- [ ] **Step 2: Run the focused tests and confirm they fail.**

Run: `cargo test --test config`

Expected: FAIL because configuration and path modules do not exist.

- [ ] **Step 3: Implement path resolution and TOML validation.**

Reject missing agent program, empty Pueue group, non-positive intervals, negative retry counts, invalid pattern actions, a pattern `Kill` action without a non-empty pattern name, and a stalled `Kill` action without a positive `kill_after_minutes`. Generate default groups from the stable project ID suffix rather than the directory basename alone.

- [ ] **Step 4: Run tests, formatting, and Clippy.**

Run: `cargo fmt --check && cargo test --test config && cargo clippy --all-targets --all-features -- -D warnings`

Expected: PASS.

- [ ] **Step 5: Commit configuration and identity support.**

```bash
git add src/paths.rs src/config.rs src/project.rs src/cli.rs tests/integration/config.rs
git commit -m "feat: add validated project configuration"
```

## Task 3: SQLite schema, migrations, and repositories

**Files:**
- Create: `src/models.rs`
- Create: `src/db/mod.rs`
- Create: `src/db/migrations.rs`
- Create: `src/db/repositories.rs`
- Test: `tests/integration/database.rs`

**Interfaces:**
- `Db::open(path: &Path) -> Result<Db, AppError>` opens SQLite, enables WAL, foreign keys, and a busy timeout, then runs migrations.
- `ProjectRepository::register` enforces unique canonical root and Pueue group.
- `EventRepository::insert_idempotent` inserts by `(project_id, dedup_key)` and returns the existing event when duplicated.
- `EventRepository::claim_batch` claims eligible events in one transaction and assigns a lease.
- `EventRepository::recover_expired_claims` returns expired claims to `pending`.
- `IncidentRepository::upsert_active` maintains one active incident per fingerprint; resolved incidents may recur as new rows.
- `EventKind` includes `TaskFinished`, `TaskFailed`, `Crash`, `Stalled`, `DeepCheck`, `AutoKilled`, and `TerminationFailed`.
- `EventStatus` includes `Pending`, `Claimed`, `Completed`, `RetryWait`, and `Failed`.
- `IncidentTransition` includes `Opened`, `Updated`, `Unchanged`, and `Resolved`.

- [ ] **Step 1: Write repository tests for schema constraints and event idempotency.**

```rust
#[test]
fn duplicate_group_registration_is_rejected() {
    let db = test_db();
    register_project(&db, "project-a", "/work/a", "pa-shared").unwrap();
    let error = register_project(&db, "project-b", "/work/b", "pa-shared").unwrap_err();
    assert!(error.to_string().contains("pueue_group"));
}

#[test]
fn duplicate_event_key_returns_one_event() {
    let db = test_db();
    let first = insert_event(&db, "project-a", "task_finished:41").unwrap();
    let second = insert_event(&db, "project-a", "task_finished:41").unwrap();
    assert_eq!(first.event_id, second.event_id);
}
```

- [ ] **Step 2: Run the database tests and confirm failure before migrations exist.**

Run: `cargo test --test database`

Expected: FAIL because the repositories and schema are not implemented.

- [ ] **Step 3: Add migrations for `projects`, `events`, `incidents`, `agent_runs`, `agent_run_events`, `submissions`, `termination_requests`, and `task_observations`.**

Add foreign keys, indexes for pending events and active incidents, and a partial unique index for active incident fingerprints.

- [ ] **Step 4: Implement repository transactions and lease recovery.**

Use `BEGIN IMMEDIATE` for claims and state transitions. Never hold a SQLite transaction while starting an external process.

- [ ] **Step 5: Run database tests, including a two-connection claim race.**

Run: `cargo test --test database -- --nocapture`

Expected: PASS with one claimant receiving each event and the other receiving an empty batch.

- [ ] **Step 6: Commit the database layer.**

```bash
git add src/models.rs src/db tests/integration/database.rs
git commit -m "feat: add SQLite event and project repositories"
```

## Task 4: Safe Pueue process adapter and `submit`

**Files:**
- Create: `src/pueue.rs`
- Create: `src/submit.rs`
- Modify: `src/cli.rs`
- Test: `tests/integration/pueue_adapter.rs`
- Create: `tests/support/fake_pueue.rs`

**Interfaces:**
- `trait PueueApi` exposes `status_json`, `add`, and `kill` without shell interpolation.
- `CommandPueue` stores the executable and fixed configuration arguments separately.
- `PueueTask` preserves task ID, group, command, state, enqueue/start/end timestamps, and result.
- `submit::run(project_root: &Path, args: &[OsString]) -> Result<Submission, AppError>` records submission intent before calling Pueue.

- [ ] **Step 1: Write tests for argument preservation and submit intent recovery.**

```rust
#[tokio::test]
async fn submit_preserves_spaces_and_shell_metacharacters_as_arguments() {
    let fake = FakePueue::new();
    let result = submit_with(&fake, ["python", "train.py", "--name", "a b; echo bad"]).await.unwrap();
    assert_eq!(fake.last_add_args(), vec!["-g", "pa-project", "--", "python", "train.py", "--name", "a b; echo bad"]);
    assert_eq!(result.status, SubmissionStatus::Accepted);
}
```

- [ ] **Step 2: Run the focused adapter tests and confirm failure.**

Run: `cargo test --test pueue_adapter`

Expected: FAIL because the adapter and submit command are not implemented.

- [ ] **Step 3: Implement `CommandPueue` with `tokio::process::Command`.**

Pass Pueue configuration arguments as individual arguments. Capture stdout/stderr and return a typed integration error on non-zero exit or invalid JSON.

- [ ] **Step 4: Implement submission intent and task adoption.**

Insert a `submissions` row before `pueue add`, store the returned task ID after success, and let reconciliation adopt an unlinked task only when project group, command, and creation window identify exactly one candidate.

- [ ] **Step 5: Run adapter tests and full Rust tests.**

Run: `cargo test --all-targets`

Expected: PASS.

- [ ] **Step 6: Commit the adapter and submit path.**

```bash
git add src/pueue.rs src/submit.rs src/cli.rs tests/integration/pueue_adapter.rs tests/support/fake_pueue.rs
git commit -m "feat: add safe Pueue adapter and submit command"
```

## Task 5: Callback ingestion and authoritative reconciliation

**Files:**
- Create: `src/events.rs`
- Create: `src/reconcile.rs`
- Modify: `src/pueue.rs`
- Modify: `src/cli.rs`
- Test: `tests/integration/reconciliation.rs`

**Interfaces:**
- `events::record_callback(group: &str, task_id: i64, metadata: CallbackMetadata) -> Result<EventId, AppError>` only writes an event and returns quickly.
- `Reconciler::run_once(&mut self) -> Result<ReconcileReport, AppError>` queries all enabled groups once and updates task observations.
- `task_signature(task: &PueueTask) -> TaskSignature` includes group, task ID, enqueue/start/end data, and state information.

- [ ] **Step 1: Write tests for duplicate callbacks, missed callbacks, task ID reuse, and malformed Pueue responses.**

```rust
#[tokio::test]
async fn duplicate_callback_and_reconciliation_create_one_completion_event() {
    let harness = Harness::new();
    harness.record_callback("pa-project", 41).await.unwrap();
    harness.record_callback("pa-project", 41).await.unwrap();
    harness.reconcile_success(41).await.unwrap();
    assert_eq!(harness.pending_event_count("project-a", EventKind::TaskFinished), 1);
}

#[tokio::test]
async fn malformed_status_is_an_integration_error_not_idle() {
    let harness = Harness::with_status_bytes(b"not-json");
    let error = harness.reconcile_once().await.unwrap_err();
    assert!(error.to_string().contains("Pueue status JSON"));
}
```

- [ ] **Step 2: Run reconciliation tests and confirm failure.**

Run: `cargo test --test reconciliation`

Expected: FAIL because event ingestion and reconciliation are not implemented.

- [ ] **Step 3: Implement callback ingestion and task signatures.**

Resolve the project from the unique group in SQLite. Unknown groups produce a visible integration event rather than mutating a project.

- [ ] **Step 4: Implement the all-project reconciliation pass.**

Fetch Pueue status once, update observations, materialize authoritative task completion/failure events, and recover submissions. A failed status query must not be interpreted as an empty task list.

- [ ] **Step 5: Run the focused and full test suites.**

Run: `cargo test --test reconciliation && cargo test --all-targets`

Expected: PASS.

- [ ] **Step 6: Commit event ingestion and reconciliation.**

```bash
git add src/events.rs src/reconcile.rs src/pueue.rs src/cli.rs tests/integration/reconciliation.rs
git commit -m "feat: add durable callback and Pueue reconciliation"
```

## Task 6: Detector fingerprints and incident lifecycle

**Files:**
- Create: `src/detect.rs`
- Create: `src/incidents.rs`
- Create: `src/logs.rs`
- Modify: `src/reconcile.rs`
- Test: `tests/integration/detection.rs`

**Interfaces:**
- `Detector::inspect_task(task: &PueueTask, config: &CheckConfig) -> Result<Vec<Observation>, AppError>` bounds reads to the configured tail size.
- `LogSnapshot` contains byte size, modification time, and a bounded content fingerprint.
- `IncidentStore::observe(observation) -> Result<IncidentTransition, AppError>` returns `Opened`, `Updated`, `Unchanged`, or `Resolved`.
- Error-pattern observations carry pattern name, action, confirmation count, and bounded evidence.

- [ ] **Step 1: Write tests for repeated `NaN`, repeated stalled snapshots, log growth, extra logs, and pattern confirmation counts.**

```rust
#[test]
fn identical_nan_observations_update_one_incident() {
    let store = test_incident_store();
    let first = store.observe(nan_observation("task-41", "nan-loss")).unwrap();
    let second = store.observe(nan_observation("task-41", "nan-loss")).unwrap();
    assert_eq!(first, IncidentTransition::Opened);
    assert_eq!(second, IncidentTransition::Unchanged);
    assert_eq!(store.active_count(), 1);
}
```

- [ ] **Step 2: Run detector tests and confirm failure.**

Run: `cargo test --test detection`

Expected: FAIL because detector and incident modules are not implemented.

- [ ] **Step 3: Implement bounded log readers and fingerprints.**

Use the configured Pueue task-log directory and project-relative extra paths. Reject extra paths that escape the project root after canonicalization.

- [ ] **Step 4: Implement incident transitions and confirmation counts.**

Do not create a new active incident for an unchanged observation. Resolve an incident only after the task or log state demonstrates recovery, or when the task reaches a terminal state.

- [ ] **Step 5: Run focused tests and full Rust tests.**

Run: `cargo test --test detection && cargo test --all-targets`

Expected: PASS.

- [ ] **Step 6: Commit detection and incidents.**

```bash
git add src/detect.rs src/incidents.rs src/logs.rs src/reconcile.rs tests/integration/detection.rs
git commit -m "feat: add fingerprinted anomaly incidents"
```

## Task 7: Policy-controlled Pueue termination

**Files:**
- Create: `src/termination.rs`
- Modify: `src/pueue.rs`
- Modify: `src/incidents.rs`
- Modify: `src/reconcile.rs`
- Test: `tests/integration/termination.rs`

**Interfaces:**
- `TerminationPolicy` maps an observation to `Notify`, `Wake`, or `Kill`.
- `TerminationManager::request(incident_id, task_signature) -> Result<TerminationRequestId, AppError>` is idempotent.
- `TerminationManager::execute(request_id) -> Result<TerminationOutcome, AppError>` revalidates the task before invoking `pueue kill`.
- `TerminationOutcome` is `Confirmed`, `TimedOut`, `Failed`, or `AlreadyTerminal`.

- [ ] **Step 1: Write tests for default non-kill behavior, explicit fatal-pattern kill, task-signature revalidation, one kill per incident, and failed kill handling.**

```rust
#[tokio::test]
async fn explicit_kill_policy_kills_only_the_matching_running_task() {
    let harness = Harness::running_task("project-a", "task-41");
    harness.configure_pattern("cuda-oom", PatternAction::Kill);
    harness.observe_fatal_pattern("task-41", "cuda-oom").await.unwrap();
    harness.run_termination_cycle().await.unwrap();
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);
    assert_eq!(harness.pending_event_kinds(), vec![EventKind::AutoKilled]);
}

#[tokio::test]
async fn stalled_detection_does_not_kill_with_default_zero_grace() {
    let harness = Harness::stalled_task("project-a", "task-41");
    harness.run_termination_cycle().await.unwrap();
    assert!(harness.fake_pueue.kill_calls().is_empty());
}
```

- [ ] **Step 2: Run termination tests and confirm failure.**

Run: `cargo test --test termination`

Expected: FAIL because termination policy and requests are not implemented.

- [ ] **Step 3: Implement idempotent termination requests.**

Create one request per active incident and task signature. Store the reason, matched pattern, and bounded evidence before invoking Pueue.

- [ ] **Step 4: Revalidate and invoke Pueue kill.**

Read authoritative status again, require the exact signature to still be `Running` in the registered group, then invoke `pueue kill` with the numeric task ID. Never kill based only on a stale callback.

- [ ] **Step 5: Reconcile post-kill state.**

Create `auto_killed` and schedule agent work only after a terminal Pueue state is observed. On timeout or failure, create `termination_failed`, preserve the incident, and do not start a second agent automatically.

- [ ] **Step 6: Run termination tests and the complete Rust suite.**

Run: `cargo test --test termination && cargo test --all-targets`

Expected: PASS.

- [ ] **Step 7: Commit policy-controlled termination.**

```bash
git add src/termination.rs src/pueue.rs src/incidents.rs src/reconcile.rs tests/integration/termination.rs
git commit -m "feat: add policy-controlled Pueue task termination"
```

## Task 8: Guardrails, scheduler leases, and agent process lifecycle

**Files:**
- Create: `src/guardrails.rs`
- Create: `src/agent.rs`
- Create: `src/scheduler.rs`
- Modify: `src/db/repositories.rs`
- Modify: `src/models.rs`
- Test: `tests/integration/scheduler.rs`

**Interfaces:**
- `Scheduler::tick(&mut self) -> Result<SchedulerReport, AppError>` claims eligible events, coalesces them per project, and starts no more than one run per project.
- `AgentRunner::spawn(project, prompt) -> Result<AgentHandle, AppError>` uses `tokio::process::Command` and an argument vector.
- `Guardrails::check(project, event_batch) -> Result<DispatchDecision, AppError>` returns `Allow`, `Pause`, or `Halt(reason)`.
- `AgentHandle` exposes run ID, child process, timeout deadline, and log path.

- [ ] **Step 1: Write tests for priority coalescing, one active agent, lease expiry, cooldown, retry backoff, and guardrail halts.**

```rust
#[tokio::test]
async fn crash_and_deep_check_for_one_project_start_one_crash_run() {
    let harness = SchedulerHarness::new();
    harness.enqueue(EventKind::DeepCheck, "project-a");
    harness.enqueue(EventKind::Crash, "project-a");
    harness.tick().await.unwrap();
    assert_eq!(harness.agent_runs(), 1);
    assert_eq!(harness.last_prompt_mode(), "crash");
}

#[tokio::test]
async fn expired_claim_is_requeued_after_restart() {
    let harness = SchedulerHarness::new();
    let event_id = harness.enqueue(EventKind::TaskFinished, "project-a");
    harness.claim_without_completion(event_id).await.unwrap();
    harness.advance_time_past_lease();
    harness.restart_scheduler().await.unwrap();
    assert_eq!(harness.event_status(event_id), EventStatus::Pending);
}
```

- [ ] **Step 2: Run scheduler tests and confirm failure.**

Run: `cargo test --test scheduler`

Expected: FAIL because scheduling and agent lifecycle are not implemented.

- [ ] **Step 3: Implement event priority and per-project coalescing.**

Claim all eligible events in one transaction, group by project, select the highest-priority reason, and associate all claimed events through `agent_run_events`.

- [ ] **Step 4: Implement direct agent process execution.**

Replace `{prompt}` only inside an argument entry. Write stdout/stderr to a timestamped project log. Track timeout and exit code in SQLite. Use Unix process groups when available so timeout cleanup can terminate the full agent process tree, while keeping a platform-specific fallback for macOS.

- [ ] **Step 5: Implement retry and guardrail transitions.**

Count accepted or started Pueue tasks for `max_experiments`, intervention events for consecutive failures, and agent invocations for `max_agent_runs`. Use bounded exponential backoff and preserve failed event context.

- [ ] **Step 6: Run focused tests and full Rust tests.**

Run: `cargo test --test scheduler && cargo test --all-targets`

Expected: PASS.

- [ ] **Step 7: Commit the scheduler and agent runner.**

```bash
git add src/guardrails.rs src/agent.rs src/scheduler.rs src/db/repositories.rs src/models.rs tests/integration/scheduler.rs
git commit -m "feat: add leased agent scheduler and guardrails"
```

## Task 9: Supervisor loop, service integration, and callback installation

**Files:**
- Create: `src/daemon.rs`
- Create: `src/service.rs`
- Create: `assets/systemd/pueue-agent.service`
- Create: `assets/launchd/com.pueue-agent.plist`
- Modify: `src/cli.rs`
- Test: `tests/integration/daemon.rs`
- Test: `tests/integration/service.rs`

**Interfaces:**
- `Daemon::run(shutdown: CancellationToken) -> Result<(), AppError>` executes reconciliation, detection, termination, scheduling, and child-process polling at configured intervals.
- `ServiceManager::install` and `ServiceManager::status` are platform-specific wrappers with a common typed result.
- `enable` registers the project, installs one callback command for the selected Pueue profile, and verifies daemon health before returning success.

- [ ] **Step 1: Write tests for one reconciliation loop, graceful shutdown, daemon restart recovery, callback command generation, and partial enable failure.**

```rust
#[tokio::test]
async fn daemon_restart_recovers_expired_claims() {
    let harness = DaemonHarness::new();
    harness.enqueue_running_event("project-a");
    harness.stop_after_claim().await;
    harness.restart().await.unwrap();
    assert_eq!(harness.pending_events("project-a"), 1);
}
```

- [ ] **Step 2: Run daemon and service tests and confirm failure.**

Run: `cargo test --test daemon --test service`

Expected: FAIL because the supervisor loop and service integration are not implemented.

- [ ] **Step 3: Implement the daemon loop with bounded intervals.**

Run one Pueue reconciliation per interval, process all projects in that result, then run detector, termination, scheduler, and child-process polling passes. Treat integration errors as visible failures rather than idle state.

- [ ] **Step 4: Add systemd and launchd service definitions.**

Use explicit binary path, Pueue configuration path, PATH, state directory, working directory, and restart policy. Do not rely on the interactive shell environment.

- [ ] **Step 5: Replace per-project cron registration with one supervisor service.**

Keep callback ingestion as a fast entry point. `enable` must reject a conflicting existing callback instead of reporting success, and must leave a recoverable registration if service installation fails.

- [ ] **Step 6: Build the release binary and run service tests without changing the production entry point yet.**

Build the release binary for service-command generation. Verify the old Bash entry point is still available through the migration test path until Task 11.

Run: `cargo test --test daemon --test service && cargo build --release`

Expected: PASS and a release binary at `target/release/pueue-agent`.

- [ ] **Step 7: Commit daemon and service integration.**

```bash
git add src/daemon.rs src/service.rs assets/systemd assets/launchd src/cli.rs tests/integration/daemon.rs tests/integration/service.rs
git commit -m "feat: add supervisor daemon and user service integration"
```

## Task 10: Status, pause/resume, and operator visibility

**Files:**
- Create: `src/status.rs`
- Modify: `src/cli.rs`
- Modify: `src/project.rs`
- Test: `tests/integration/operator_commands.rs`

**Interfaces:**
- `status` reports daemon health, project state, active Pueue tasks, pending events, open incidents, termination requests, agent runs, and guardrail counters.
- `pause` prevents new agent and automatic termination actions but does not delete events.
- `resume` clears the project halt/pause state and makes pending events eligible again.
- `disable` refuses to release a group while unresolved Pueue tasks remain unless an explicit remove operation is requested.

- [ ] **Step 1: Write operator-command tests.**

```rust
#[test]
fn status_shows_failed_termination_without_marking_project_idle() {
    let output = run_status_fixture("termination_failed");
    assert!(output.contains("termination_failed"));
    assert!(!output.contains("events: none"));
}
```

- [ ] **Step 2: Run focused tests and confirm failure.**

Run: `cargo test --test operator_commands`

Expected: FAIL because status and pause/resume commands are not implemented.

- [ ] **Step 3: Implement read-only status queries and state transitions.**

All state-changing transitions must use repository transactions and produce a runtime log entry.

- [ ] **Step 4: Run focused tests and full Rust tests.**

Run: `cargo test --test operator_commands && cargo test --all-targets`

Expected: PASS.

- [ ] **Step 5: Commit operator commands.**

```bash
git add src/status.rs src/cli.rs src/project.rs tests/integration/operator_commands.rs
git commit -m "feat: add supervisor status and pause controls"
```

## Task 11: Compatibility migration, documentation, and end-to-end coverage

**Files:**
- Rename: `bin/pueue-agent` to `bin/pueue-agent-legacy`
- Create: `bin/pueue-agent` as a development launcher for the built Rust binary
- Modify: `install.sh`
- Modify: `README.md`
- Modify: `templates/config.yml`
- Modify: `templates/instructions.md`
- Modify: `tests/helpers/setup.bash`
- Modify: `tests/e2e/run.sh`
- Create: `tests/e2e/rust_supervisor.sh`
- Create: `tests/support/fake_agent.sh`

**Interfaces:**
- Existing command examples continue to work with TOML-backed Rust state.
- `pueue-agent submit -- command...` remains the supported submission path for both humans and agents.
- The old Bash implementation is removed only after the Rust E2E suite covers callback, missed callback, repeated anomaly, auto-kill, agent retry, halt, resume, and restart recovery.

- [ ] **Step 1: Write the Rust E2E scenario before switching the entry point.**

The scenario must create two projects with the same basename, register different groups, submit a successful fake experiment, inject a persistent fatal log into a Running fake task, verify exactly one Pueue kill, verify exactly one agent run, restart the supervisor, and verify a pending event remains recoverable.

- [ ] **Step 2: Run the E2E test and confirm it fails before the compatibility switch.**

Run: `tests/e2e/rust_supervisor.sh`

Expected: FAIL because the Rust supervisor is not yet the installed entry point.

- [ ] **Step 3: Update templates and instructions.**

Replace YAML configuration examples with TOML. Instruct agents to use `pueue-agent submit` rather than raw `pueue add`, and document that explicit `action = "kill"` policies can terminate Pueue tasks.

- [ ] **Step 4: Update install and entry-point behavior.**

Build the Rust release binary in `install.sh`, make the installed symlink point to `target/release/pueue-agent`, and make the development launcher execute `target/debug/pueue-agent` with a clear error if it has not been built. Keep the renamed Bash implementation only for migration comparison; do not leave two production implementations with the same command path.

- [ ] **Step 5: Run the complete verification suite before removing the legacy implementation.**

Run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
tests/e2e/rust_supervisor.sh
bats tests
shellcheck bin/pueue-agent lib/*.sh install.sh
git diff --check
```

Expected: Rust tests and E2E pass; `tests/helpers/setup.bash` points the legacy Bash suite at `bin/pueue-agent-legacy`, so all existing Bash tests remain green during the comparison window.

- [ ] **Step 6: Remove obsolete coordination code after parity is demonstrated.**

Remove the global text registry, PID lock, handcrafted YAML parser, per-project cron entries, direct Bash agent launcher, and obsolete tests. Preserve only compatibility code that is still part of the documented user workflow.

- [ ] **Step 7: Commit the migration and documentation.**

```bash
git add Cargo.toml Cargo.lock src bin install.sh templates README.md tests
git commit -m "feat: switch pueue-agent to Rust SQLite supervisor"
```

## Spec Coverage Map

- Project identity and multi-project isolation: Tasks 2, 3, 5, and 11.
- SQLite schema, idempotency, leases, and restart recovery: Tasks 3, 8, and 9.
- Callback ingestion and missed-callback recovery: Task 5.
- Bounded anomaly detection and incident fingerprints: Task 6.
- Explicit automatic termination and kill-failure handling: Task 7.
- Agent execution, prompt construction, retries, and guardrails: Task 8.
- User service, environment propagation, and callback installation: Task 9.
- Status, pause, resume, and visible operator state: Task 10.
- CLI compatibility, templates, installation, and end-to-end acceptance: Task 11.

## Verification Checklist

Before declaring the migration complete, verify each requirement against a test or command:

- [ ] Two same-basename projects have distinct groups and isolated events.
- [ ] Duplicate callback plus reconciliation produces one completion event and one agent run.
- [ ] Repeated identical anomaly observations produce one active incident.
- [ ] Explicit `action = "kill"` revalidates the task and invokes one `pueue kill`.
- [ ] A failed or timed-out kill is visible and does not start a second agent automatically.
- [ ] Normal monitoring starts zero agents.
- [ ] `submit` preserves argument boundaries and records task accounting.
- [ ] SQLite leases recover after supervisor restart.
- [ ] Agent timeout and retry do not leave an unrecoverable claim.
- [ ] Pause/resume preserves pending events.
- [ ] Service execution has explicit PATH and Pueue configuration.
- [ ] Rust unit, integration, E2E, Bash compatibility, ShellCheck, and diff checks pass.
