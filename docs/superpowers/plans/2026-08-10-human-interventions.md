# Human Intervention Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** SQLiteキューに保存した人の自然言語介入を、次回のagent runへFIFOで一度だけ渡し、失敗時には再試行できるようにする。

**Architecture:** `pueue-agent steer` はproject-scoped interventionをSQLiteへ保存するだけとし、schedulerが次回runのpromptを組み立てる前にboundedなpending介入をreservation lease付きで取得する。agent runの作成、PID確定、介入のapplied遷移をSQLite transactionで結び、spawn failureとdaemon recoveryでは安全にpendingへ戻す。既存のtext status、Pueue操作、実行中agent processへの入力は変更しない。

**Tech Stack:** Rust 2021、Clap、Tokio、rusqlite bundled SQLite、Serde/serde_json、既存のuuid crate、既存のPueue adapterとscheduler/agent abstraction

## Global Constraints

- 介入は実行中の agent process へ直接送信せず、SQLite に保存したキューを次回の agent run に渡す。
- `steer` は現在の project に対する介入を一件登録する。
- 介入は project 単位で扱う。task/incident への絞り込みは後続拡張とする。
- pause/disable 中でも登録でき、resume 後の次回 agent run で消費する。
- 1件あたり最大 4,096 bytes とする。
- 1回の agent run へ渡す合計は最大 16,384 bytes とする。
- 超過分は pending のまま次回 run へ繰り越す。
- FIFO 順を維持する。
- JSON診断出力に介入本文を含めない。
- 空白だけの本文は拒否する。
- 1件の最大長を超える入力は拒否し、暗黙に切り詰めない。
- `steer` は agent 起動、Pueue 操作、実行中 process への入力を発生させない。
- 実行中 agent process へのリアルタイム入力は実装しない。
- Web UI、Slack/Discord、外部通知は実装しない。
- 人の介入によって安全ポリシー、承認、resource admissionを自動上書きしない。
- 既存の text status、agent起動、Pueue処理、Rust test、Bats、ShellCheckを維持する。

---

## File and module map

- `src/interventions.rs`: interventionのbounded validation、status/model、prompt section、list projectionの型と定数。
- `src/models.rs`: SQLite enum macroで読む `InterventionStatus` と永続モデルが既存モデルとの境界を保つ。
- `src/db/migrations.rs`: schema v5からv6へのinterventions table/index migration。FIFOを保証するproject-scoped insertion sequenceを含める。
- `src/db/repositories.rs`: project-scoped insert/list/count、FIFO reservation、apply/release、lease recoveryのtransaction API。
- `src/db/mod.rs`: repositoryの公開再export。
- `src/cli.rs`: `steer` と `steer list` のtyped Clap surface。
- `src/main.rs`: 既存のproject/root/config解決を使うenqueue/list handler。
- `src/scheduler.rs`: promptのbase部分とoperator intervention部分を分離する。reservationとagent runの接続はTask 4で行う。
- `src/agent.rs`: reservationとagent runを結び、spawn成功・失敗をrepositoryへ通知する。
- `src/daemon.rs`: daemon restart時のreserved intervention recoveryを既存agent recoveryと同じcycleで行う。
- `src/diagnostics.rs`: status JSONへ件数だけを追加し、本文を出力しない。
- `README.md`: 日本語の使用手順、FIFO、失敗時再試行、bounded limits、非目標を追記する。
- `tests/integration/database.rs`: migration、FIFO、project isolation、reservation/apply/release、lease recoveryを検証する。
- `tests/integration/cli_help.rs`: steer command surfaceを検証する。
- `tests/integration/interventions.rs`: CLI handler/listのbounded出力と入力検証を検証する。
- `tests/integration/scheduler.rs`: prompt順序、budget overflow、successful deliveryを検証する。
- `tests/integration/daemon.rs`: spawn failure、restart recovery、重複配送防止を検証する。
- `tests/integration/diagnostics.rs`: status JSONの件数のみの表示と本文秘匿を検証する。

## Task 1: Add schema, bounded models, and repository queue API

**Files:**
- Create: `src/interventions.rs`
- Modify: `src/lib.rs`
- Modify: `src/models.rs`
- Modify: `src/db/migrations.rs`
- Modify: `src/db/repositories.rs`
- Modify: `src/db/mod.rs`
- Test: `tests/integration/database.rs`

**Interfaces:**
- Produces `InterventionStatus`, `Intervention`, `InterventionCounts`, `InterventionReservation`, `MAX_INTERVENTION_BYTES`, `MAX_INTERVENTIONS_PER_RUN`, and `MAX_INTERVENTION_BYTES_PER_RUN` for later tasks. `Intervention` carries a monotonic project-scoped insertion sequence used for FIFO.
- Produces `InterventionRepository::insert_pending`, `list`, `count_by_project`, `reserve_pending`, `mark_applied_for_run`, `release_for_run`, and `recover_expired`.

- [ ] **Step 1: Add failing migration and repository tests**

Extend the database integration fixture with the schema-v5-to-v6 migration case and tests for the public repository contract. Use two registered projects and timestamps with equal values so that the ID tie-breaker is observable.

```rust
let first = InterventionRepository::new(&db)
    .insert_pending("project-a", "first instruction", 100)
    .unwrap();
let second = InterventionRepository::new(&db)
    .insert_pending("project-a", "second instruction", 100)
    .unwrap();
let other = InterventionRepository::new(&db)
    .insert_pending("project-b", "foreign instruction", 100)
    .unwrap();

let listed = InterventionRepository::new(&db)
    .list("project-a", InterventionStatus::Pending, 8)
    .unwrap();
assert_eq!(listed.iter().map(|item| item.intervention_id), vec![first.intervention_id, second.intervention_id]);
assert!(listed.iter().all(|item| item.project_id == "project-a"));
assert!(!listed.iter().any(|item| item.intervention_id == other.intervention_id));
```

Add tests for empty/too-long messages, per-run message count/byte bounds, overflow left pending, reservation token ownership, applied/release transitions, and expired reservation recovery.

- [ ] **Step 2: Run the focused tests and confirm they fail**

Run:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test database interventions
```

Expected: FAIL because the v6 migration, model, repository, and test APIs do not exist.

- [ ] **Step 3: Add bounded intervention model and constants**

Create the public module and re-export it from `src/lib.rs`:

```rust
pub const MAX_INTERVENTION_BYTES: usize = 4 * 1024;
pub const MAX_INTERVENTIONS_PER_RUN: usize = 16;
pub const MAX_INTERVENTION_BYTES_PER_RUN: usize = 16 * 1024;

pub struct InterventionReservation {
    pub token: String,
    pub items: Vec<Intervention>,
}

pub fn validate_message(message: &str) -> Result<(), AppError>;
```

Use the existing `database_enum!` macro for `InterventionStatus { Pending, Reserved, Applied }`. Reject trimmed-empty input and input whose UTF-8 byte length exceeds `MAX_INTERVENTION_BYTES`; do not truncate at insertion time.

- [ ] **Step 4: Add schema v6 migration**

Change `LATEST_SCHEMA_VERSION` to `6`. Add a `version == 5` branch that creates:

```sql
CREATE TABLE interventions (
    intervention_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    insertion_sequence INTEGER NOT NULL,
    message TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'reserved', 'applied')),
    created_at INTEGER NOT NULL,
    reserved_at INTEGER,
    applied_at INTEGER,
    agent_run_id INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_expires_at INTEGER,
    reservation_token TEXT,
    FOREIGN KEY (project_id, agent_run_id)
        REFERENCES agent_runs(project_id, run_id) ON DELETE SET NULL,
    CHECK (
        (status = 'pending' AND reserved_at IS NULL AND applied_at IS NULL AND agent_run_id IS NULL AND lease_expires_at IS NULL AND reservation_token IS NULL)
        OR (status = 'reserved' AND reserved_at IS NOT NULL AND applied_at IS NULL AND lease_expires_at IS NOT NULL AND reservation_token IS NOT NULL)
        OR (status = 'applied' AND reserved_at IS NOT NULL AND applied_at IS NOT NULL AND agent_run_id IS NOT NULL)
    )
);
CREATE UNIQUE INDEX interventions_project_sequence_idx
    ON interventions(project_id, insertion_sequence);
CREATE INDEX interventions_project_status_created_idx
    ON interventions(project_id, status, insertion_sequence, intervention_id);
CREATE INDEX interventions_reservation_lease_idx
    ON interventions(status, lease_expires_at, reservation_token);
PRAGMA user_version = 6;
```

Keep migration execution transactional and add a test that opens a v5 database, migrates it, and confirms existing projects/events plus the new table survive. In the fresh `version == 0` path, apply the interventions DDL before committing instead of leaving the new database at v5; a fresh `Db::open` must finish at v6 in one open.

- [ ] **Step 5: Implement parameterized repository methods**

Implement fixed-SQL methods with project predicates and deterministic ordering:

```rust
pub fn insert_pending(
    &self,
    project_id: &str,
    message: &str,
    created_at: i64,
) -> Result<Intervention, AppError>;

pub fn list(
    &self,
    project_id: &str,
    status: InterventionStatus,
    limit: usize,
) -> Result<Vec<Intervention>, AppError>;

pub fn count_by_project(&self, project_id: &str) -> Result<InterventionCounts, AppError>;

pub fn reserve_pending(
    &self,
    project_id: &str,
    token: &str,
    now: i64,
    lease_until: i64,
    max_count: usize,
    max_bytes: usize,
) -> Result<InterventionReservation, AppError>;

pub fn mark_applied_for_run(
    &self,
    project_id: &str,
    run_id: i64,
    applied_at: i64,
) -> Result<usize, AppError>;

pub fn release_for_run(
    &self,
    project_id: &str,
    run_id: i64,
) -> Result<usize, AppError>;

pub fn recover_expired(&self, now: i64) -> Result<usize, AppError>;
```

`insert_pending` must allocate the next project-scoped `insertion_sequence` inside the same Immediate transaction as the insert. `reserve_pending` and `list` must select pending rows in `insertion_sequence ASC` order, using `created_at, intervention_id` only as deterministic display tie-breakers. Stop before either bound would be exceeded and update only rows still pending. `list`, `count_by_project`, and all transition methods must enforce project scope. Clamp internal limits to the declared maxima and never interpolate user input into SQL.

- [ ] **Step 6: Run focused tests and commit**

Run:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo fmt --check
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test database interventions
```

Expected: PASS, including migration, project isolation, FIFO, bounds, apply/release, and lease recovery. Commit:

```bash
git add src/interventions.rs src/lib.rs src/models.rs src/db/migrations.rs src/db/repositories.rs src/db/mod.rs tests/integration/database.rs
git commit -m "feat: add human intervention queue storage"
```

## Task 2: Add the steer CLI and bounded list command

**Files:**
- Modify: `src/cli.rs`
- Modify: `src/main.rs`
- Modify: `tests/integration/cli_help.rs`
- Create: `tests/integration/interventions.rs`
- Modify: `Cargo.toml`

**Interfaces:**
- Consumes: `ProjectArgs` resolution, `InterventionRepository::insert_pending`, `InterventionRepository::list`, `validate_message`, and the shared intervention limits.
- Produces: `pueue-agent steer -- <message>` and `pueue-agent steer list [--json]` without starting an agent or invoking Pueue.

- [ ] **Step 1: Add failing Clap and handler tests**

Add help assertions for `steer`, `steer list`, `--json`, and the message argument. Add integration cases that invoke the command against a temporary project/database fixture and assert that registration returns an ID, stores the exact bounded message, rejects whitespace-only and over-limit messages, preserves CLI registration order when two messages share one timestamp, and lists only the selected project's rows.

```rust
let output = assert_cmd::Command::cargo_bin("pueue-agent")
    .unwrap()
    .args(["steer", "--", "次の実験では learning rate を下げる"])
    .env("PUEUE_AGENT_STATE_DIR", state_dir)
    .current_dir(project_root)
    .output()
    .unwrap();
assert!(output.status.success());
assert!(String::from_utf8_lossy(&output.stdout).contains("queued intervention"));
```

Verify that the `steer` command does not create an agent run and does not call the Pueue binary. Verify JSON list output contains IDs/status/timestamps and bounded message text but never returns more than the list maximum.

- [ ] **Step 2: Run focused tests and confirm they fail**

Run:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test cli_help --test interventions steer
```

Expected: FAIL because the subcommand, handler, and integration target do not exist.

- [ ] **Step 3: Define typed CLI arguments**

Add a `Command::Steer(SteerArgs)` variant. Support the approved syntax and an explicit list subcommand:

```rust
pub struct SteerArgs {
    pub action: Option<SteerAction>,
    pub message: Vec<String>,
    pub json: bool,
    pub pueue_config: Option<PathBuf>,
    pub project_root: Option<PathBuf>,
}

pub enum SteerAction {
    List(SteerListArgs),
}
```

Use Clap validation so a message is required when no action is present, `steer list` accepts no message, and `--` permits natural-language text beginning with a hyphen. Keep project/config resolution identical to `status` and `events`.

- [ ] **Step 4: Implement enqueue and list handlers**

Add `commands::steer` in `src/main.rs`. Resolve `(Db, Project, ServicePaths)` through the existing helper, but use only the database for enqueue/list. Join trailing message arguments with a single space, validate before insertion, generate an intervention ID with the existing `uuid` dependency, and print either:

```text
queued intervention: <id>
```

or a bounded JSON object containing `schema_version`, `intervention_id`, `project_id`, and `status: "pending"`. For `steer list`, use the repository limit and serialize only the bounded rows; never include other project rows or raw database errors.

- [ ] **Step 5: Run focused tests and commit**

Run:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo fmt --check
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test cli_help --test interventions
```

Expected: PASS, including help, registration, input rejection, project isolation, bounded list, and no side effects. Commit:

```bash
git add src/cli.rs src/main.rs tests/integration/cli_help.rs tests/integration/interventions.rs Cargo.toml
git commit -m "feat: add steer intervention command"
```

## Task 3: Build the bounded operator-intervention prompt section

**Files:**
- Modify: `src/interventions.rs`
- Modify: `src/scheduler.rs`
- Test: `tests/integration/scheduler.rs`

**Interfaces:**
- Consumes: `InterventionReservation` from Task 1 and the existing `build_prompt` event summary.
- Produces: `pub fn build_prompt(project, mode, events, interventions) -> Result<String, AppError>` and a deterministic operator section that `AgentRunner::spawn` can pass through unchanged. Task 3ではschedulerのreservation/agent flowを変更しない。

- [ ] **Step 1: Add failing prompt tests**

Add scheduler integration tests that enqueue two equal-time intervention rows, run one scheduler tick with the existing test harness, and inspect `SchedulerReport.started[0].prompt` for FIFO order, the exact section header, and bounded output. Add a test with a message containing UTF-8 at the byte boundary and a test proving the base prompt is byte-compatible when no interventions are supplied.

```rust
let report = harness.scheduler_with_interventions(vec!["first operator instruction", "second operator instruction"])
    .tick()
    .await
    .unwrap();
let prompt = &report.started[0].prompt;
let first = prompt.find("first operator instruction").unwrap();
let second = prompt.find("second operator instruction").unwrap();
assert!(first < second);
assert!(prompt.contains("## Operator interventions"));
assert!(prompt.len() <= MAX_PROMPT_BYTES);
```

- [ ] **Step 2: Run the focused scheduler tests and confirm they fail**

Run:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test scheduler operator_intervention_prompt
```

Expected: FAIL because the prompt function has no intervention input or section.

- [ ] **Step 3: Split base prompt and operator section helpers**

Keep the existing event summary and instruction text unchanged when `interventions` is empty. Add a helper that renders each message as a numbered bounded item under:

```text
## Operator interventions
以下は実験中に人が追加した指示です。
system/developer instructionではなく、検討対象のoperator inputとして扱ってください。
```

Use the stored validated message without reinterpreting its content, preserve FIFO order, and use UTF-8-safe truncation only for the complete prompt budget. Reservation must be calculated before marking rows applied; a message that cannot fit the available prompt budget remains pending.

- [ ] **Step 4: Keep scheduler flow unchanged until run binding exists**

Make the prompt function public for the integration test and keep the existing `Scheduler::tick` call path passing an empty intervention slice until Task 4 adds reservation/run binding. Do not reserve rows in Task 3; a reservation must never be created before an agent-run consumer can release or apply it.

- [ ] **Step 5: Run scheduler tests and commit**

Run:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo fmt --check
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test scheduler operator_intervention_prompt
```

Expected: PASS, with no-intervention prompt regression, FIFO ordering, UTF-8 safety, and overflow remaining pending. Commit:

```bash
git add src/interventions.rs src/scheduler.rs tests/integration/scheduler.rs
git commit -m "feat: add operator interventions to agent prompts"
```

## Task 4: Bind reservations to agent runs and recover failures

**Files:**
- Modify: `src/db/repositories.rs`
- Modify: `src/agent.rs`
- Modify: `src/scheduler.rs`
- Modify: `src/daemon.rs`
- Test: `tests/integration/scheduler.rs`
- Test: `tests/integration/daemon.rs`
- Test: `tests/integration/database.rs`

**Interfaces:**
- Consumes: `InterventionReservation` and prompt section from Task 3.
- Produces: atomic reservation-to-run binding, applied/release transitions, and restart recovery without duplicate delivery.

- [ ] **Step 1: Add failing delivery and recovery tests**

Add scheduler tests for a successful fake agent and a missing executable. Assert respectively:

```rust
assert_eq!(pending_count(&db, "project-a"), 0);
assert_eq!(applied_run_id(&db, intervention_id), Some(run_id));
```

and:

```rust
assert_eq!(status(&db, intervention_id), InterventionStatus::Pending);
assert_eq!(applied_run_id(&db, intervention_id), None);
```

Add daemon recovery tests for an expired reserved row, a reserved row attached to a live run, and a reserved row attached to a failed pre-spawn run. The live-run case must become applied; only the pre-spawn case may return to pending.

- [ ] **Step 2: Run focused tests and confirm they fail**

Run:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test scheduler operator_intervention_delivery --test daemon intervention_recovery
```

Expected: FAIL because the existing agent repository does not know about intervention reservations.

- [ ] **Step 3: Add transaction APIs for run binding**

Extend `AgentRunRepository` with transaction methods that use the same SQLite connection:

```rust
pub fn insert_with_events_and_reservation(
    &self,
    run: &NewAgentRun,
    event_ids: &[i64],
    reservation_token: Option<&str>,
) -> Result<AgentRun, AppError>;

pub fn mark_running_and_apply_interventions(
    &self,
    project_id: &str,
    run_id: i64,
    pid: i64,
    applied_at: i64,
) -> Result<AgentRun, AppError>;

pub fn finish_and_release_interventions(
    &self,
    project_id: &str,
    run_id: i64,
    status: AgentRunStatus,
    finished_at: i64,
    exit_code: Option<i64>,
    last_error: Option<&str>,
) -> Result<AgentRun, AppError>;
```

The first method attaches reserved rows to the new run in the insertion transaction. The second transaction updates PID/status and marks those rows applied. If that transaction fails, terminate the spawned child before releasing the reservation. Failed startup paths use the third method and return rows to pending; normal completion leaves applied rows immutable.

- [ ] **Step 4: Pass reservations through scheduler and AgentRunner**

Change `AgentRunner::spawn` to accept `Option<&InterventionReservation>`. It must use the reservation token when inserting the run and call `mark_running_and_apply_interventions` immediately after obtaining the child PID. `Scheduler::tick` must release a reservation if `spawn` returns an error, while preserving the existing event retry behavior.

- [ ] **Step 5: Integrate daemon startup recovery**

Update the startup recovery transaction to inspect `agent_runs` and intervention reservations together. A reservation attached to a run with a live PID is applied; a reservation attached to a failed/starting run with no live PID is returned to pending. Recovery must retain project predicates and must not requeue an intervention twice.

- [ ] **Step 6: Run focused tests and commit**

Run:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo fmt --check
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test database interventions --test scheduler operator_intervention --test daemon intervention_recovery
```

Expected: PASS for successful delivery, failed spawn retry, lease recovery, live-run application, project isolation, and no duplicate delivery. Commit:

```bash
git add src/db/repositories.rs src/agent.rs src/scheduler.rs src/daemon.rs tests/integration/database.rs tests/integration/scheduler.rs tests/integration/daemon.rs
git commit -m "feat: make intervention delivery durable"
```

## Task 5: Expose bounded intervention diagnostics

**Files:**
- Modify: `src/diagnostics.rs`
- Modify: `src/main.rs`
- Modify: `tests/integration/diagnostics.rs`
- Modify: `tests/integration/interventions.rs`

**Interfaces:**
- Consumes: `InterventionRepository::count_by_project`, bounded list rows, and existing `render_project_status_json`.
- Produces: status JSON intervention counts and bounded `steer list` text/JSON output without changing existing text status output.

- [ ] **Step 1: Add failing projection tests**

Create fixtures with pending, reserved, and applied interventions containing a hidden prompt-like value. Assert:

```rust
let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
assert_eq!(value["interventions"]["counts"]["pending"], 1);
assert_eq!(value["interventions"]["counts"]["applied"], 1);
assert!(!rendered.contains("hidden prompt-like value"));
```

Add list tests proving that the CLI list is bounded, project-scoped, ordered by creation time and ID, and that JSON contains bounded message text only in the explicit `steer list` response.

- [ ] **Step 2: Run focused diagnostics tests and confirm they fail**

Run:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test diagnostics intervention --test interventions steer_list
```

Expected: FAIL because status JSON and list projection have no intervention section.

- [ ] **Step 3: Add intervention counts to status JSON**

Extend the existing typed status DTO with:

```rust
#[derive(Serialize)]
struct InterventionStatusProjection {
    counts: InterventionCountsProjection,
}
```

Serialize only counts and bounded metadata. Keep `render_project_status` byte-compatible and do not include intervention bodies in either status text or status JSON.

- [ ] **Step 4: Implement bounded steer list rendering**

Add a text/JSON projection for `steer list`. Use a fixed maximum list limit and bounded message fields. On database/configuration errors, return the existing short CLI error path and do not echo the submitted message. Ensure other projects are never included.

- [ ] **Step 5: Run focused tests and commit**

Run:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo fmt --check
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test diagnostics --test interventions
```

Expected: PASS with status text regression, status JSON secrecy, count projection, bounded list, and project isolation. Commit:

```bash
git add src/diagnostics.rs src/main.rs tests/integration/diagnostics.rs tests/integration/interventions.rs
git commit -m "feat: expose intervention diagnostics"
```

## Task 6: Document operator workflow and run full verification

**Files:**
- Modify: `README.md`
- Modify: `tests/integration/cli_help.rs`
- Modify: `docs/superpowers/specs/2026-08-10-human-interventions-design.md` only if implementation made an approved interface clarification
- Test: existing Rust integration targets, Bats, ShellCheck

**Interfaces:**
- Consumes: the complete `steer` workflow and the approved human-intervention design.
- Produces: Japanese user documentation that explains enqueue, FIFO delivery, retry behavior, bounded limits, and non-goals.

- [ ] **Step 1: Add documentation regression checks**

Add `readme_documents_human_intervention_workflow` to `tests/integration/cli_help.rs`. Read `README.md` from `env!("CARGO_MANIFEST_DIR")` and assert that it contains the exact examples `pueue-agent steer --` and `pueue-agent steer list`, plus the phrases that registration is queued for the next agent run and running agents are not interrupted.

- [ ] **Step 2: Update the Japanese README**

Add a section after the operator commands showing:

```bash
pueue-agent steer -- "次は learning rate を半分にして"
pueue-agent steer list
pueue-agent status --json
```

Explain that registration is SQLite-only, messages are FIFO and one-shot, spawn failure returns them to pending, pause/disable queues without delivery, and status JSON omits message bodies. State clearly that running agents are not interrupted and that safety policies cannot be overridden.

- [ ] **Step 3: Run the complete verification suite**

Run:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo fmt --check
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --all-targets
bats tests/test_shell_entrypoints.bats
shellcheck --shell=bash bin/pueue-agent tests/test_shell_entrypoints.bats
```

Expected: all Rust tests, Bats tests, and ShellCheck checks pass with no warnings. Also run `git diff --check` and verify that only the planned files changed.

- [ ] **Step 4: Commit documentation and verification record**

```bash
git add README.md tests/integration/cli_help.rs
git commit -m "docs: document human intervention workflow"
```
