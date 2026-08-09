# Observability and Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** SQLite の event、incident、task、termination、agent run を CLI と bounded JSON で追跡できる Phase A の診断機能を追加する。

**Architecture:** 既存の `status` と repository を読み取り専用 query 層へ拡張し、`src/diagnostics.rs` が CLI 用の bounded read model と JSON serialization を担当する。既存の text status と supervisor の動作は維持し、`events`、`inspect`、`explain`、`doctor` は同じ project/root 解決と Pueue snapshot を利用する。

**Tech Stack:** Rust 2021、Tokio、Clap、Serde/serde_json、rusqlite、既存の Pueue adapter と service abstraction

## Global Constraints

- 対象は Phase A の可視化・診断だけ。安全 policy、approval、resource admission は空の将来セクションとして扱う。
- 現行 SQLite schema v5 と project/event の境界を維持し、Phase A では migration を追加しない。
- 既存の text `status` 出力、event 処理、agent 起動、termination の挙動を変更しない。
- task command、incident payload、agent prompt、Codex transcript の全文は出力しない。
- すべての query に既定値と最大値を持つ limit を設定し、payload と error summary を bounded にする。
- JSON の最上位に `schema_version = 1` を含め、出力順を time と ID の複合順序で決定的にする。
- Web UI、Slack/Discord 通知、全文ログ表示、自動修復は実装しない。
- 既存の Rust test、Bats、ShellCheck を最終検証で実行する。

---

### Task 1: Diagnostics CLI surface and typed filters

**Files:**
- Modify: `src/cli.rs`
- Modify: `src/main.rs`
- Create: `src/diagnostics.rs`
- Modify: `src/lib.rs`
- Test: `tests/integration/cli_help.rs`

**Interfaces:**
- Consumes: existing `ProjectArgs`, `Status` command, `Db`, and `StatusInput` boundaries.
- Produces: `EventsArgs`, `InspectArgs`, `ExplainArgs`, `DoctorArgs`, `StatusArgs`, and the public `diagnostics` module with bounded filter types.

- [ ] **Step 1: Add failing CLI help assertions**

  Extend `tests/integration/cli_help.rs` with assertions that `--help` contains `events`, `inspect`, `explain`, `doctor`, and `status --json`.

  ```rust
  assert!(text.contains("events"));
  assert!(text.contains("inspect"));
  assert!(text.contains("explain"));
  assert!(text.contains("doctor"));
  ```

- [ ] **Step 2: Run the focused CLI test and confirm it fails**

  Run: `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test cli_help`

  Expected: FAIL because the new subcommands and options are not yet present.

- [ ] **Step 3: Define the Clap command types**

  Add these command variants and arguments without changing existing argument parsing:

  ```rust
  Status(StatusArgs); // project_root, pueue_config, json
  Events(EventsArgs); // project_root, pueue_config, kind, status, limit, json
  Inspect(InspectArgs); // project_root, pueue_config, task_id, json
  Explain(ExplainArgs); // project_root, pueue_config, incident_id, json
  Doctor(DoctorArgs); // project_root, pueue_config, json
  ```

  Use `i64` for task/incident IDs, `usize` for limits, and bounded defaults of 8 for status/event summaries and 100 for explicit event listing. Reject zero and values above the hard maximum in the command handler.

- [ ] **Step 4: Add typed diagnostics filter and public module declarations**

  Define in `src/diagnostics.rs`:

  ```rust
  pub const JSON_SCHEMA_VERSION: u32 = 1;

  pub struct EventFilter {
      pub kind: Option<EventKind>,
      pub status: Option<EventStatus>,
      pub limit: usize,
  }

  impl EventFilter {
      pub fn new(
          kind: Option<EventKind>,
          status: Option<EventStatus>,
          limit: usize,
      ) -> Self;
  }
  ```

  Add `mod diagnostics` to `src/lib.rs` and route the new `Command` variants from `src/main.rs` to fallible handlers. Keep all output rendering out of the Clap definitions.

- [ ] **Step 5: Run the focused CLI test and confirm it passes**

  Run: `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test cli_help`

  Expected: PASS with the new command names present.

- [ ] **Step 6: Commit the CLI surface**

  ```bash
  git add src/cli.rs src/main.rs src/diagnostics.rs src/lib.rs tests/integration/cli_help.rs
  git commit -m "feat: add diagnostics command surface"
  ```

### Task 2: Bounded repository read models

**Files:**
- Modify: `src/db/repositories.rs`
- Modify: `src/db/mod.rs` only if a shared query error helper is required
- Modify: `src/models.rs` only if a serializable bounded view type is required
- Test: `tests/integration/database.rs`

**Interfaces:**
- Consumes: `EventRepository::recent_events`, `IncidentRepository::find_by_id`, `SubmissionRepository`, `AgentRunRepository`, `TerminationRequestRepository`, and `TaskObservationRepository`.
- Produces: filtered event listing, latest task observation by Pueue task ID, incident timeline data, and project-scoped bounded relation queries for `src/diagnostics.rs`.

- [ ] **Step 1: Add failing repository tests for filters and task inspection**

  Add tests that insert events with different kinds/statuses, task observations with a reused Pueue task ID but different signatures, and related submission/incident records. Assert that filters, deterministic ordering, and project isolation are preserved.

  ```rust
  let events = EventRepository::new(&db)
      .list_filtered("project-a", &EventFilter::new(Some(EventKind::Crash), None, 10))
      .unwrap();
  assert!(events.iter().all(|event| event.kind == EventKind::Crash));
  ```

- [ ] **Step 2: Run the focused database tests and confirm the new cases fail**

  Run: `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test database diagnostics`

  Expected: FAIL because the query methods do not exist.

- [ ] **Step 3: Implement bounded event and relation queries**

  Add read-only methods with deterministic `ORDER BY created_at DESC, event_id DESC` or the corresponding stable ID tie-breaker:

  ```rust
  EventRepository::list_filtered(
      &self,
      project_id: &str,
      filter: &EventFilter,
  ) -> Result<Vec<Event>, AppError>
  TaskObservationRepository::find_by_pueue_task(
      &self,
      project_id: &str,
      pueue_task_id: i64,
      limit: usize,
  ) -> Result<Vec<TaskObservation>, AppError>
  ```

  Add project-scoped query helpers for submissions, incidents, termination requests, and agent runs. Enforce the maximum limit in one place and never construct SQL from user-provided strings except validated enum values.

- [ ] **Step 4: Run the focused database tests and confirm they pass**

  Run: `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test database diagnostics`

  Expected: PASS, including project isolation and task-ID reuse cases.

- [ ] **Step 5: Commit the read models**

  ```bash
  git add src/db/repositories.rs src/db/mod.rs src/models.rs tests/integration/database.rs
  git commit -m "feat: add bounded diagnostic queries"
  ```

### Task 3: JSON and text status projection

**Files:**
- Modify: `src/status.rs`
- Modify: `src/diagnostics.rs`
- Modify: `src/main.rs`
- Test: `tests/integration/operator_commands.rs`
- Test: `tests/integration/diagnostics.rs`

**Interfaces:**
- Consumes: `StatusInput`, `Project`, repository read models, and `ServiceStatus`.
- Produces: `render_project_status_json(db, project, input) -> Result<String, AppError>` while preserving the existing `render_project_status(db, project, input)` text function.

- [ ] **Step 1: Add failing JSON projection tests**

  Create `tests/integration/diagnostics.rs` with a project fixture containing one active task, one open incident, one failed termination, and one agent run. Assert that JSON contains `schema_version`, project identity, counts, and bounded summaries, while not containing the fixture's transcript payload.

  ```rust
  let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
  assert_eq!(value["schema_version"], 1);
  assert_eq!(value["project"]["project_id"], "project-a");
  assert!(rendered.contains("open_incidents"));
  assert!(!rendered.contains("hidden transcript"));
  ```

- [ ] **Step 2: Run the focused diagnostics test and confirm it fails**

  Run: `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test diagnostics status_json`

  Expected: FAIL because the diagnostics integration test and JSON renderer are not implemented.

- [ ] **Step 3: Implement a serializable status read model**

  Define bounded `Serialize` DTOs in `src/diagnostics.rs` for project, daemon, Pueue task summaries, event counts, incident counts, termination counts, agent-run counts, and future `policy`/`resource` sections. Serialize only sanitized task command summaries and existing count/error fields; do not serialize raw database payload JSON.

  Keep the existing text renderer and add a separate JSON renderer so the default CLI output remains byte-compatible.

- [ ] **Step 4: Route `status --json` through the projection**

  Resolve project and Pueue status exactly as the existing status command does. On a Pueue error, include a typed `pueue.status = "error"` section rather than treating the project as idle. Print JSON to stdout and return the existing error behavior for database/configuration failures.

- [ ] **Step 5: Run the focused diagnostics and operator tests**

  Run: `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test diagnostics --test operator_commands`

  Expected: PASS with text status regression coverage and bounded JSON output coverage.

- [ ] **Step 6: Commit the status projection**

  ```bash
  git add src/status.rs src/diagnostics.rs src/main.rs tests/integration/diagnostics.rs tests/integration/operator_commands.rs
  git commit -m "feat: add bounded JSON status diagnostics"
  ```

### Task 4: Events, task inspection, and incident explanation

**Files:**
- Modify: `src/diagnostics.rs`
- Modify: `src/main.rs`
- Modify: `src/db/repositories.rs` if an unimplemented relation query remains
- Test: `tests/integration/diagnostics.rs`

**Interfaces:**
- Consumes: `EventFilter`, bounded repository queries, existing project resolution, and `StatusInput` Pueue snapshots.
- Produces: `events`, `inspect`, and `explain` handlers with text and JSON output.

- [ ] **Step 1: Add failing event filter and causal-chain tests**

  Cover kind/status filters, maximum limits, task-ID reuse, unknown incident IDs, and a deterministic explanation chain from observation through event. Assert that a project cannot inspect another project's task or incident.

- [ ] **Step 2: Run the focused tests and confirm the new behavior fails**

  Run: `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test diagnostics events`

  Expected: FAIL because the handlers and query composition are not implemented.

- [ ] **Step 3: Implement `events` output**

  Parse validated `EventKind` and `EventStatus` values, enforce the default/max limit, call the repository query, and render either deterministic text rows or the bounded JSON list.

- [ ] **Step 4: Implement `inspect` output**

  Resolve the project from the current root, query task observations and related records by stable signature, and render the current Pueue task state alongside persisted history. If no matching observation exists, return a typed runtime error without broadening the search to other projects.

- [ ] **Step 5: Implement `explain` output**

  Load the incident, its related event and task key, then attach matching termination and agent-run records. If policy tables are not present, emit `policy: not_configured` and `approval: not_configured` rather than inventing a decision.

- [ ] **Step 6: Run the focused diagnostics tests and confirm they pass**

  Run: `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test diagnostics`

  Expected: PASS for text/JSON output, filters, project isolation, task reuse, and explanation ordering.

- [ ] **Step 7: Commit event and inspection commands**

  ```bash
  git add src/diagnostics.rs src/main.rs src/db/repositories.rs tests/integration/diagnostics.rs
  git commit -m "feat: add event and incident inspection commands"
  ```

### Task 5: Read-only doctor checks

**Files:**
- Modify: `src/diagnostics.rs`
- Modify: `src/main.rs`
- Modify: `src/db/repositories.rs`
- Test: `tests/integration/diagnostics.rs`

**Interfaces:**
- Consumes: `Db`, project config path, `ServiceControl`, `PueueApi`, and existing lease/recovery query APIs.
- Produces: `doctor` report with `ok`/`warning`/`error` checks and deterministic text/JSON rendering.

- [ ] **Step 1: Add failing doctor tests**

  Cover healthy SQLite/config/Pueue/service fixtures, invalid config, Pueue status error, missing callback/service, and an expired event/termination lease. Assert that doctor never writes a repair or changes project state.

- [ ] **Step 2: Run the focused doctor tests and confirm they fail**

  Run: `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test diagnostics doctor`

  Expected: FAIL because the doctor report and checks are not implemented.

- [ ] **Step 3: Implement independent read-only checks**

  Use a typed `DoctorCheck { name, status, summary, remediation }` and `DoctorReport { schema_version, checks }`. Each check must catch and classify its own integration error so one failed check does not hide the remaining checks. Do not call migration, recovery, transition, install, or repair methods.

- [ ] **Step 4: Implement text/JSON doctor rendering and exit behavior**

  Render all checks in stable name order. JSON includes every check; text includes one line per check and remediation for warnings/errors. Return a non-zero command result only when at least one `error` exists, while warnings remain inspectable success output.

- [ ] **Step 5: Run doctor and full integration tests**

  Run:

  ```bash
  PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test diagnostics --test daemon --test service --test pueue_adapter
  ```

  Expected: PASS with no mutation of the test database or fake Pueue/service state.

- [ ] **Step 6: Commit doctor checks**

  ```bash
  git add src/diagnostics.rs src/main.rs src/db/repositories.rs tests/integration/diagnostics.rs
  git commit -m "feat: add read-only supervisor doctor"
  ```

### Task 6: Full verification and documentation

**Files:**
- Modify: `README.md`
- Modify: `templates/config.toml`
- Modify: `templates/instructions.md`
- Modify: `tests/integration/cli_help.rs` if final help output needs a stable assertion

**Interfaces:**
- Consumes: completed Phase A commands and JSON schema.
- Produces: documented CLI usage and a verified Phase A release candidate without changing Phase B/C behavior.

- [ ] **Step 1: Document the new commands and bounded-output guarantees**

  Add examples for `status --json`, `events`, `inspect`, `explain`, and `doctor`. Explain that doctor is read-only and that payloads/transcripts are bounded and omitted.

- [ ] **Step 2: Run formatting, lint, and all tests**

  ```bash
  PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo fmt --check
  PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo clippy --all-targets --all-features -- -D warnings
  PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --all-targets
  bats tests
  shellcheck bin/pueue-agent install.sh tests/e2e/rust_supervisor.sh tests/support/*.sh tests/test_shell_entrypoints.bats
  git diff --check
  ```

- [ ] **Step 3: Verify CLI and output boundaries manually**

  ```bash
  cargo run -- --help
  cargo run -- status --help
  cargo run -- events --help
  cargo run -- inspect --help
  cargo run -- explain --help
  cargo run -- doctor --help
  ```

  Confirm that every new command has a bounded limit option where applicable and that no command advertises an unimplemented policy/resource operation.

- [ ] **Step 4: Commit documentation and final Phase A verification**

  ```bash
  git add README.md templates/config.toml templates/instructions.md tests/integration/cli_help.rs
  git commit -m "docs: document supervisor diagnostics"
  ```
