# P0〜P3 運用強化 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 外部利用レポートで見つかった read-only、安全性、分類、介入、可観測性、batch 再実行、STATE 整合性の問題を、P0 → P1 → P2 → P3 の順で本体へ取り込む。

**Architecture:** SQLite source of truth を維持し、read-only CLI、typed submission、operator event、lineage、batch state machine、canonical state を既存の event/scheduler/guardrail 経路へ接続する。TTY 表示は共通 formatter に集約し、JSON と外部 Pueue 操作の契約を分離する。

**Tech Stack:** Rust stable、Tokio、rusqlite、SQLite WAL、Clap、Serde/serde_json、assert_cmd、tempfile、Bats、ShellCheck。

## Global Constraints

- `submit --kind` 省略時は `experiment`。既存 command の挙動を壊さない。
- `control` は `max_experiments` に数えない。
- read-only CLI は migration、directory creation、WAL pragma を実行しない。
- `immutable=1` は使わない。WAL を無視した古い snapshot を作り得るため。
- Pueue と SQLite をまたぐ完全な distributed transaction は主張しない。intent、lease、result で追跡する。
- prompt、transcript、raw payload、credential 値は既定の CLI 出力へ出さない。
- TTY の装飾は pipe/redirect で無効にし、JSON 出力へ header/ANSI/説明文を混ぜない。
- 既存 `status`、`events`、`inspect`、`explain`、`doctor` の JSON `schema_version` と既存 field を維持する。
- 各タスクは failing test → 失敗確認 → 最小実装 → 成功確認 → commit の順で行う。
- Rust コマンドは `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin` を先頭に付ける。

---

## Task 1: P0 read-only status と compact status

**Files:** `src/cli.rs`, `src/main.rs`, `src/status.rs`, `tests/integration/cli_help.rs`, `tests/integration/diagnostics.rs`, `tests/integration/database.rs`

**Interfaces:** `StatusArgs.compact: bool`、`status::render_project_status_compact(&Db, &Project, &StatusInput) -> Result<String, AppError>`。

- [x] **Step 1: failing tests を書く。** `status --help` の `--compact`、compact output の daemon/task/event/agent/guardrail summary、read-only connection の `UPDATE/CREATE/PRAGMA user_version` 拒否を検証する。compact output に payload/prompt-like text がないことも確認する。
- [x] **Step 2: RED を確認する。** `cargo test --all-targets compact_status` と `cargo test --all-targets readonly_open` を実行し、option/renderer/契約が未実装で失敗することを確認する。
- [x] **Step 3: 最小実装。** `commands::status` を `resolve_project_read_only` に切り替える。`status --json` は既存 full JSON のまま、`--compact` は `pueue-agent` header、daemon、pueue counts、experiments、agent runs、events、summary だけを表示する。
- [x] **Step 4: GREEN を確認する。** focused test と `cargo test --all-targets` を実行する。
- [x] **Step 5: commit。** `git add src/cli.rs src/main.rs src/status.rs tests/integration/cli_help.rs tests/integration/diagnostics.rs tests/integration/database.rs && git commit -m "feat: make status read-only and compact"`。

## Task 2: P0 共通 formatter と redaction

**Files:** Create `src/output.rs`; modify `src/lib.rs`, `src/status.rs`, `src/diagnostics.rs`, `src/main.rs`; test `tests/integration/diagnostics.rs`, `tests/integration/cli_help.rs`。

**Interfaces:** `OutputMode::{Human, Json}`、`redact_sensitive_text(&str) -> String`、`render_id(kind, id)`、`format_state(state)`。

- [x] **Step 1: failing tests を書く。** `--token very-secret`、`--api-key abc123`、`Authorization: Bearer secret`、`AWS_SECRET_ACCESS_KEY=hidden` を redaction し、通常の `--lr 0.001` は残す。pipe/JSON に ANSI がないことを検証する。
- [x] **Step 2: RED を確認する。** `cargo test --all-targets redact_sensitive_text` を実行し、secret が残る失敗を確認する。
- [x] **Step 3: 最小実装。** `std::io::IsTerminal` と `NO_COLOR` で装飾を制御し、credential key、`--token`、`--api-key`、`--password`、`--secret`、Bearer 値を `[REDACTED]` に置換する。status/diagnostics の command、reason、error summary に適用する。
- [x] **Step 4: GREEN を確認する。** focused test、既存 diagnostics test、`cargo test --all-targets` を実行する。
- [x] **Step 5: commit。** `git add src/output.rs src/lib.rs src/status.rs src/diagnostics.rs src/main.rs tests/integration/diagnostics.rs tests/integration/cli_help.rs && git commit -m "feat: add bounded cli output and redaction"`。

## Task 3: P1 submission kind、metadata、agent origin の DB 基盤

**Files:** `src/models.rs`, `src/db/migrations.rs`, `src/db/repositories.rs`, `src/guardrails.rs`, `src/status.rs`; tests `tests/integration/database.rs`, `tests/integration/scheduler.rs`, `tests/integration/reconciliation.rs`。

**Interfaces:** `SubmissionKind::{Experiment, Control}`、`Submission.kind`、`Submission.metadata: Value`、`Submission.origin_agent_run_id: Option<i64>`、`NewSubmission::with_kind_metadata(...)`、`SubmissionRepository::list_by_origin_agent_run(...)`。

- [x] **Step 1: failing tests を書く。** v6 database migration 後の既存 submission が `experiment/{}` になること、control/experiment を各1件作ると `count_started_or_accepted` が1になること、origin run query が project+run 単位で分離されることを検証する。
- [x] **Step 2: RED を確認する。** `cargo test --all-targets control_submissions_do_not_consume_experiment_guardrail` を実行する。
- [x] **Step 3: migration/model を実装する。** v6→v7 で `submissions.kind TEXT NOT NULL DEFAULT 'experiment'`、`metadata_json TEXT NOT NULL DEFAULT '{}'`、`origin_agent_run_id INTEGER` と kind/origin index を追加する。既存 `NewSubmission::new` は experiment/empty object を default にする。
- [x] **Step 4: repository/guardrail を実装する。** select/insert/parser を更新し、guardrail SQL に `kind = 'experiment'` を追加する。壊れた metadata JSON は serialization error とする。
- [x] **Step 5: GREEN と commit。** `cargo test --all-targets database`、`cargo test --all-targets scheduler`、`cargo test --all-targets reconciliation` を通し、`git commit -m "feat: classify submissions and store metadata"` する。

## Task 4: P1 submit CLI、metadata validation、origin propagation

**Files:** `src/cli.rs`, `src/main.rs`, `src/submit.rs`, `src/agent.rs`; tests `tests/integration/cli_help.rs`, `tests/integration/pueue_adapter.rs`, `tests/integration/database.rs`。

**Interfaces:** `SubmitArgs.kind` default experiment、`--metadata <PATH>`、`--metadata-json <JSON>`、`--json`、`SubmitOptions { kind, metadata, origin_agent_run_id }`。

- [x] **Step 1: failing tests を書く。** help に options があること、metadata が保存されること、不正/過大/同時指定では Pueue `add` を呼ばないこと、既存 argv boundary が変わらないことを検証する。
- [x] **Step 2: RED を確認する。** `cargo test --all-targets submit_` を実行する。
- [x] **Step 3: metadata loader を実装する。** object のみ、最大16 KiB、depth 8、key 32個、key 64 bytes、string 1024 bytes、array 64要素で検証する。ファイル/inline は相互排他的にする。
- [x] **Step 4: agent origin を実装する。** `AgentRunner::spawn` が child に `PUEUE_AGENT_RUN_ID` と `PUEUE_AGENT_PROJECT_ID` を渡し、submit 側で project と active run の組を検証して origin を保存する。無効な環境変数は fail closed とする。
- [x] **Step 5: output と GREEN。** human は submission/task/kind/group/state の5項目、JSON は bounded projection を表示する。`cargo test --all-targets pueue_adapter` と全テストを通す。
- [x] **Step 6: commit。** `git add src/cli.rs src/main.rs src/submit.rs src/agent.rs tests/integration/cli_help.rs tests/integration/pueue_adapter.rs tests/integration/database.rs && git commit -m "feat: add typed submission metadata"`。

## Task 5: P2 operator `wake`

**Files:** `src/cli.rs`, `src/main.rs`, `src/models.rs`, `src/db/migrations.rs`, `src/events.rs`, `src/scheduler.rs`, `templates/instructions.md`; tests `tests/integration/cli_help.rs`, `tests/integration/daemon.rs`, `tests/integration/scheduler.rs`, `tests/integration/database.rs`。

**Interfaces:** `Command::Wake(WakeArgs)`、`EventKind::OperatorWake`、dispatch mode `operator_wake`。

- [x] **Step 1: failing tests を書く。** `wake --reason` が Pueue API を呼ばず event を作ること、scheduler が operator_wake agent を起動すること、pause 中は pending のままになること、help/JSON output が bounded であることを検証する。
- [x] **Step 2: RED を確認する。** `cargo test --all-targets operator_wake` を実行する。
- [x] **Step 3: event migration を実装する。** events.kind の CHECK constraint に `operator_wake` を追加する migration を行い、reason を bounded payload に保存する。dedup key は `operator-wake:v1:<uuid>` とする。
- [x] **Step 4: CLI/scheduler を実装する。** wake は SQLite insert のみ実行し、既存 claim/lease/guardrail/one-slot を再利用する。`templates/instructions.md` に operator_wake の判断規則を追加する。
- [x] **Step 5: GREEN と commit。** `cargo test --all-targets operator_wake`、全 Rust test、`git commit -m "feat: add operator wake events"` を実行する。

## Task 6: P2 runs/lineage と `--follow`

**Files:** Create `src/runs.rs`; modify `src/lib.rs`, `src/cli.rs`, `src/main.rs`, `src/db/repositories.rs`, `src/output.rs`; tests `tests/integration/diagnostics.rs`, `tests/integration/cli_help.rs`, `tests/integration/database.rs`。

**Interfaces:** `RunsArgs { json, follow, limit, ... }`、`render_runs(&Db, &Project, limit, json)`、`RunLineage { event_id, run_id, mode, run_status, submission_ids, task_ids }`、`FollowCursor`。

- [x] **Step 1: failing tests を書く。** event→agent run→submission(origin_agent_run_id)→task の query、bounded text/JSON、`FollowCursor` の重複排除、help を検証する。
- [x] **Step 2: RED を確認する。** `cargo test --all-targets runs_` を実行する。`FollowCursor` の unit test は同じ focused run に含める。
- [x] **Step 3: repository/renderer を実装する。** project-scoped `LIMIT` query で event/run/submission の summary だけを結ぶ。prompt、transcript、full metadata/command は返さない。ID は `event=`, `run=`, `task=`, `sub=` prefix を使う。
- [x] **Step 4: follow を実装する。** read-only SQLite を短い interval で polling し、cursor `(started_at, run_id, submission_id)` より新しいものだけ表示する。`tokio::select!` と Ctrl-C で停止する。Pueue API は呼ばない。
- [x] **Step 5: GREEN と commit。** focused/full tests を通し、`git commit -m "feat: expose agent run lineage"` を実行する。

## Task 7: P2 共通 human CLI output を適用する

**Files:** `src/main.rs`, `src/diagnostics.rs`, `src/status.rs`, `src/runs.rs`, `src/output.rs`, `src/submit.rs`; tests `tests/integration/cli_help.rs`, `tests/integration/diagnostics.rs`, `tests/integration/pueue_adapter.rs`。

- [x] **Step 1: failing contract tests を書く。** status/events/submit/wake/runs の human output に `pueue-agent` header、ID prefix、state、summary があり、JSON stdout が `{` で始まり装飾を含まないことを検証する。
- [x] **Step 2: RED を確認する。** `cargo test --all-targets cli_output_contract` を実行する。
- [x] **Step 3: renderer を適用する。** Pueue task state と supervisor event/run state を別列で表示し、TTY のみ状態記号/色を有効にする。long command/reason は bounded/redacted にする。
- [x] **Step 4: GREEN と commit。** 全 Rust test と `git commit -m "style: clarify human cli output"` を実行する。

表示例:

```text
pueue-agent runs  project=vision-lab  showing=2
RUN     EVENT     MODE        STATE  RESULT
run=31  event=18  completion  RUN    waiting for agent
run=30  event=17  crash       DONE   submitted task=42
```

## Task 8: P3 durable batch repository/state machine

**Files:** Create `src/batches.rs`; modify `src/lib.rs`, `src/models.rs`, `src/db/migrations.rs`, `src/db/repositories.rs`; tests `tests/integration/database.rs`, `tests/integration/pueue_adapter.rs`。

**Interfaces:** `BatchStatus::{Pending, Dispatching, Accepted, Partial, Failed, Completed}`、`BatchRepository::{create_or_get, claim, record_job_result, recover_expired, find}`。

- [x] **Step 1: failing tests を書く。** 同一 request ID の重複防止、job 途中失敗時の partial、dispatch lease expiry 後の未処理 job だけの回収を検証する。
- [x] **Step 2: RED を確認する。** `cargo test --all-targets batch_` を実行する。
- [x] **Step 3: schema v8 を実装する。** `batch_requests(request_id, project_id, manifest_hash, status, lease_until, timestamps, last_error)` と `batch_jobs(request_id, job_id, ordinal, kind, argv_json, metadata_json, status, pueue_task_id, submission_id, last_error)` を追加し、`request_id + job_id` を一意にする。
- [x] **Step 4: durable external result を実装する。** submission intent を先に保存し、Pueue add 後に task ID を記録する。曖昧な add response は failed とし、後続 job を投入しない。accepted job は再実行しない。
- [x] **Step 5: GREEN と commit。** `cargo test --all-targets batch_` と全 Rust test を通し、`git commit -m "feat: add durable batch submission state"` を実行する。

## Task 9: P3 `submit-batch` CLI

**Files:** `src/cli.rs`, `src/main.rs`, `src/batches.rs`, `src/output.rs`; tests `tests/integration/cli_help.rs`, `tests/integration/pueue_adapter.rs`。

**Interfaces:** `Command::SubmitBatch(SubmitBatchArgs)`、`--request-id <UUID>`、`--manifest <PATH>`、`--group <GROUP>`、`--json`。

- [x] **Step 1: failing tests を書く。** help、manifest parse、same request no duplicate add、partial JSON result、group mismatch rejection を検証する。
- [x] **Step 2: RED を確認する。** `cargo test --all-targets submit_batch_cli` を実行する。
- [x] **Step 3: manifest validation を実装する。** `{jobs:[...]}`、job 数128以下、unique job ID、non-empty argv、kind/metadata validation、stable manifest hash、bounded file read を実装する。group 省略時は登録 group、指定時は一致必須とする。
- [x] **Step 4: human/JSON output を実装する。** batch status と accepted/failed/pending 件数を表示し、JSON には job 単位の result と accepted task IDs を返す。raw Pueue output は混ぜない。
- [x] **Step 5: GREEN と commit。** focused/full test を通し、`git commit -m "feat: add idempotent submit batch command"` を実行する。

## Task 10: P3 canonical state と doctor consistency check

**Files:** Create `src/state.rs`, `templates/state.json`; modify `src/lib.rs`, `src/init.rs`, `src/diagnostics.rs`, `src/main.rs`, `templates/instructions.md`; tests `tests/integration/init.rs`, `tests/integration/diagnostics.rs`。

**Interfaces:** `CanonicalState { schema_version, current_facts, historical_facts, next_action, budgets, active_lineage }`、`state::load`、`state::check_consistency`、`StateWarning`。

- [x] **Step 1: failing tests を書く。** init が `.pueue-agent/state.json` を作ること、duplicate current fact、active state と `STATE.md` の bounded sentinel `campaign stopped` の矛盾、invalid budget を検証する。
- [x] **Step 2: RED を確認する。** `cargo test --all-targets canonical_state` を実行する。doctor consistency の test もこの prefix に含める。
- [x] **Step 3: state schema/init を実装する。** 新規 project だけ state.json を作り、既存 STATE.md/state.json は上書きしない。未知 field、size、depth、array length を reject する。
- [x] **Step 4: doctor/prompt を実装する。** schema error は exit failure、文章との不一致は warning とする。instructions と prompt は state.json を canonical、STATE.md を補足として扱う。
- [x] **Step 5: GREEN と commit。** focused/full test を通し、`git commit -m "feat: validate structured experiment state"` を実行する。

## Task 11: docs と最終回帰

**Files:** `README.md`, `templates/config.toml`, `templates/instructions.md`, `tests/integration/cli_help.rs`, `tests/test_shell_entrypoints.bats`（launcher output を変えた場合のみ）。

- [x] **Step 1: failing documentation contract test を書く。** README に `status --compact`、`wake`、`runs --follow`、`submit-batch`、`--kind control`、`state.json` が記載されていることを検証する。
- [x] **Step 2: RED を確認する。** `cargo test --test cli_help documentation_contract` で、README に `pueue-agent status --compact` がないため失敗することを確認した。
- [x] **Step 3: README/templates を更新する。** command syntax、human/JSON output、control/experiment count、batch idempotency、canonical state、raw Pueue と supervisor output の違いを日本語で記載する。
- [x] **Step 4: 全検証を実行する。** Rust test 326件、Bats 3件、ShellCheck、targeted rustfmt、diff-check は成功。`cargo fmt --check` は既知の Task1 差分のみ、clippy は既存コード3件で失敗した。

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo fmt --check
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --all-targets
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo clippy --all-targets --all-features -- -D warnings
bats tests/test_shell_entrypoints.bats
shellcheck --shell=bash bin/pueue-agent tests/test_shell_entrypoints.bats
git diff --check
```

- [x] **Step 5: commit。** 実装 `6569232` (`docs: document p0-p3 operations`) と、report `8a8bbbb` (`docs: report task 11 documentation verification`) を分離して作成した。

## 完了時の統合

- [ ] feature branch の clean status、全 commit、全検証結果を確認する。
- [ ] `superpowers:requesting-code-review` でレビューする。
- [ ] main へ merge した後、main 上で全検証を再実行する。
- [ ] 検証後に worktree と branch を整理する。
