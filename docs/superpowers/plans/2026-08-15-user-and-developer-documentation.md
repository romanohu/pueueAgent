# User and Developer Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize `pueue-agent` documentation into a concise README plus complete Japanese guides for setup, commands, workflows, architecture, and troubleshooting.

**Architecture:** Keep Markdown as the only documentation format. Treat `README.md` as a navigation hub and place detailed, responsibility-focused guides under `docs/`; use lightweight Rust integration tests to keep links, command coverage, support claims, and safety wording synchronized with the implementation.

**Tech Stack:** Markdown, Mermaid, Rust integration tests, Cargo, Bash syntax checks

## Global Constraints

- Documentation is Japanese-first; CLI names, option names, config keys, paths, environment variables, types, and state names retain their implementation spelling.
- Do not change CLI behavior, configuration behavior, SQLite schema, scheduler behavior, agent lifecycle, service behavior, or runtime dependencies.
- `src/cli.rs`, config types, `templates/config.toml`, models, repositories, scheduler, and daemon are the source of truth; do not document inferred or planned behavior.
- State that Linux is validated on Ubuntu GitHub Actions and requires kernel 5.8 or newer for strict private-temp mount checks.
- State that macOS has a known `/dev/fd/11` descendant-path limitation and is not claimed to have Linux-equivalent agent execution support.
- Do not include credentials, prompt text, transcripts, raw environment values, or unbounded payload examples.
- Preserve `docs/operations-ja.md` as a compatibility path containing links to its replacement sections.
- Do not rewrite existing historical files under `docs/superpowers/` except this implementation plan and the already-approved design spec.
- Do not add mdBook, a site generator, a Markdown runtime dependency, or generated documentation artifacts.

---

### Task 1: Getting Started Guide

**Files:**
- Create: `docs/getting-started-ja.md`
- Modify: `tests/integration/cli_help.rs`

**Interfaces:**
- Consumes: installation behavior from `install.sh`, CLI arguments from `src/cli.rs`, generated files from `src/init.rs`, service setup from `src/service.rs`, and current configuration keys from `templates/config.toml`.
- Produces: the canonical step-by-step setup guide later linked by `README.md` and the troubleshooting guide.

- [ ] **Step 1: Add a failing documentation contract test**

Add this test near `documentation_contract_covers_current_operator_surface`:

```rust
#[test]
fn getting_started_documents_a_complete_first_run_and_platform_boundary() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let guide = std::fs::read_to_string(root.join("docs/getting-started-ja.md")).unwrap();
    for required in [
        "pueue-agent init",
        ".pueue-agent/config.toml",
        ".pueue-agent/state.json",
        "pueue-agent enable",
        "pueue-agent submit --",
        "pueue-agent status",
        "kernel 5.8",
        "Ubuntu GitHub Actions",
        "/dev/fd/11",
    ] {
        assert!(guide.contains(required), "getting started guide is missing {required:?}");
    }
}
```

- [ ] **Step 2: Run the focused test and verify the RED state**

Run:

```bash
cargo test --test cli_help getting_started_documents_a_complete_first_run_and_platform_boundary -- --exact --test-threads=1
```

Expected: FAIL because `docs/getting-started-ja.md` does not exist.

- [ ] **Step 3: Write the complete getting-started guide**

Create `docs/getting-started-ja.md` with these exact top-level sections and content:

```markdown
# 導入ガイド

## 対応環境
## 必要条件
## インストール
## Pueue profile を確認する
## プロジェクトを初期化する
## 生成ファイルを確認する
## プロジェクトを有効化する
## 最初の実験を投入する
## 状態を確認する
## 次に読むガイド
```

The commands must form one runnable sequence:

```bash
git clone <repository-url>
cd pueueAgent
./install.sh
cd /absolute/path/to/project
pueue-agent init
$EDITOR .pueue-agent/config.toml
$EDITOR .pueue-agent/STATE.md
pueue-agent enable
pueue-agent submit -- python train.py --lr 0.001
pueue-agent status
```

Explain that monitored jobs must use `pueue-agent submit` instead of raw `pueue add`, list all files created by `init`, explain the Pueue config precedence used by normal commands, and copy the exact Linux/macOS support boundary from the approved design.

- [ ] **Step 4: Run the focused test and inspect the guide**

Run:

```bash
cargo test --test cli_help getting_started_documents_a_complete_first_run_and_platform_boundary -- --exact --test-threads=1
git diff --check
```

Expected: PASS; no whitespace errors.

- [ ] **Step 5: Commit the guide**

```bash
git add docs/getting-started-ja.md tests/integration/cli_help.rs
git commit -m "docs: add getting started guide"
```

### Task 2: Complete Command Reference

**Files:**
- Create: `docs/commands-ja.md`
- Modify: `tests/integration/cli_help.rs`

**Interfaces:**
- Consumes: public command variants and argument structs from `src/cli.rs`; output and side-effect behavior from `src/main.rs`, `src/submit.rs`, `src/batches.rs`, `src/diagnostics.rs`, `src/runs.rs`, and `src/service.rs`.
- Produces: the canonical command reference used by README, workflows, and troubleshooting links.

- [ ] **Step 1: Add a failing command-coverage test**

```rust
#[test]
fn command_reference_covers_every_public_cli_without_exposing_internal_launch() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let commands = std::fs::read_to_string(root.join("docs/commands-ja.md")).unwrap();
    for command in [
        "init", "enable", "disable", "cancel", "submit", "submit-batch", "event",
        "status", "events", "runs", "inspect", "explain", "doctor", "pause",
        "resume", "steer", "wake", "version", "upgrade", "start", "stop", "daemon",
    ] {
        let heading = format!("### `pueue-agent {command}");
        assert!(commands.contains(&heading), "command reference is missing {command}");
    }
    assert!(commands.contains("### `pueue-agent steer list`"));
    assert!(!commands.contains("### `pueue-agent internal-launch`"));
}
```

- [ ] **Step 2: Run the command contract test and verify failure**

Run:

```bash
cargo test --test cli_help command_reference_covers_every_public_cli_without_exposing_internal_launch -- --exact --test-threads=1
```

Expected: FAIL because `docs/commands-ja.md` does not exist.

- [ ] **Step 3: Write the command reference**

Use this document structure:

```markdown
# コマンドリファレンス

## 共通ルール
## セットアップ
## 投入
## 状態確認
## 運用制御
## 人による介入
## 保守
## 内部・連携用
## コマンド選択早見表
```

Give every command its own `###` heading beginning with the exact invocation. Each entry must include `構文`, `目的`, `状態変更`, `主なオプション`, `例`, and `失敗時の確認`. Document these option contracts explicitly:

- `status`: `--json`, `--compact`, `--pueue-config`, optional `PROJECT_ROOT`
- `events`: `--kind`, `--status`, `--limit`, `--json`
- `runs`: `--follow`, `--limit`, `--json`
- `submit`: `--kind`, `--metadata`, `--metadata-json`, `--json`, trailing command argv
- `submit-batch`: `--request-id`, `--manifest`, optional `--group`, `--json`
- `disable`: the semantic difference between default and `--remove`
- `cancel`: one verified `--task-id`; no group-wide cancellation
- `steer`: bounded message queue and `steer list`
- `upgrade`: `--source`, `--pueue-config`, `--json`

Mark `event` as the installed Pueue callback interface and `daemon` as the service entry point. State that users do not call hidden `internal-launch`.

- [ ] **Step 4: Verify command coverage**

Run:

```bash
cargo test --test cli_help command_reference_covers_every_public_cli_without_exposing_internal_launch -- --exact --test-threads=1
cargo test --test cli_help help_lists_diagnostics_commands_and_status_options -- --exact --test-threads=1
git diff --check
```

Expected: both tests PASS.

- [ ] **Step 5: Commit the command reference**

```bash
git add docs/commands-ja.md tests/integration/cli_help.rs
git commit -m "docs: add complete command reference"
```

### Task 3: Operational Workflows and Compatibility Redirect

**Files:**
- Create: `docs/workflows-ja.md`
- Modify: `docs/operations-ja.md`
- Modify: `tests/integration/cli_help.rs`

**Interfaces:**
- Consumes: current operator semantics from `README.md`, `docs/operations-ja.md`, `src/main.rs`, `src/daemon.rs`, `src/termination.rs`, `src/interventions.rs`, and `src/upgrade.rs`.
- Produces: purpose-oriented runbooks and a stable redirect from the old operations path.

- [ ] **Step 1: Add failing workflow safety assertions**

```rust
#[test]
fn workflow_guide_distinguishes_every_stop_boundary_and_preserves_operations_link() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let workflows = std::fs::read_to_string(root.join("docs/workflows-ja.md")).unwrap();
    for required in [
        "pueue-agent pause", "pueue-agent stop", "pueue-agent cancel --task-id",
        "pueue-agent disable", "pueue-agent disable --remove", "pueue-agent resume",
        "pueue-agent start", "pueue-agent upgrade", "VACUUM INTO",
        "SQLite snapshot と旧 binary", "Pueue task を kill しません",
    ] {
        assert!(workflows.contains(required), "workflow guide is missing {required:?}");
    }
    let old_path = std::fs::read_to_string(root.join("docs/operations-ja.md")).unwrap();
    assert!(old_path.contains("workflows-ja.md"));
    assert!(old_path.contains("troubleshooting-ja.md"));
}
```

- [ ] **Step 2: Run the workflow test and verify failure**

Run:

```bash
cargo test --test cli_help workflow_guide_distinguishes_every_stop_boundary_and_preserves_operations_link -- --exact --test-threads=1
```

Expected: FAIL because `docs/workflows-ja.md` does not exist.

- [ ] **Step 3: Write purpose-oriented workflows**

Create these top-level sections:

```markdown
# 運用ワークフロー

## プロジェクトを登録して最初の実験を投入する
## 単発実験を投入する
## batch を冪等に投入・再開する
## 状態を監視する
## Periodic DeepCheck を有効化する
## 次の agent run に指示を渡す
## automation だけを停止・再開する
## supervisor service を停止・起動する
## Pueue task 1件を停止する
## project を無効化・登録解除する
## 異常検知から安全に対応する
## supervisor を更新・rollbackする
## daemon 再起動後を確認する
```

Include the stop-boundary matrix from the existing operations guide. Preserve the exact warnings about active-agent drain, `VACUUM INTO`, avoiding direct SQLite writes during upgrade, snapshot/binary rollback, and Pueue tasks not being killed by `stop` or `disable`.

- [ ] **Step 4: Replace the old operations body with a compatibility redirect**

Use this complete body:

```markdown
# 運用ガイドの移転

運用手順は [運用ワークフロー](workflows-ja.md) へ移動しました。
症状から復旧手順を探す場合は [トラブルシューティング](troubleshooting-ja.md) を参照してください。

既存リンクとの互換性を保つため、このファイルは残しています。
```

- [ ] **Step 5: Verify and commit workflows**

```bash
cargo test --test cli_help workflow_guide_distinguishes_every_stop_boundary_and_preserves_operations_link -- --exact --test-threads=1
git diff --check
git add docs/workflows-ja.md docs/operations-ja.md tests/integration/cli_help.rs
git commit -m "docs: add operational workflows"
```

Expected: focused test PASS and clean diff.

### Task 4: Architecture and Security Boundaries

**Files:**
- Create: `docs/architecture-ja.md`
- Modify: `tests/integration/cli_help.rs`

**Interfaces:**
- Consumes: `src/pueue.rs`, `src/pueue_process.rs`, `src/process.rs`, `src/scheduler.rs`, `src/agent.rs`, `src/daemon.rs`, `src/db/repositories.rs`, `src/native_launcher.rs`, and `src/environment.rs`.
- Produces: the current developer-facing architecture reference used by README and troubleshooting explanations.

- [ ] **Step 1: Add a failing architecture contract test**

```rust
#[test]
fn architecture_guide_covers_submission_dispatch_terminal_and_recovery_flows() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let architecture = std::fs::read_to_string(root.join("docs/architecture-ja.md")).unwrap();
    for required in [
        "```mermaid", "submission intent", "callback", "reconciliation", "pending event",
        "policy preflight", "agent run bind", "native gate", "exec proof", "exact ack",
        "終端状態", "private temp cleanup", "startup recovery", "descriptor",
        "Ubuntu GitHub Actions", "/dev/fd/11",
    ] {
        assert!(architecture.contains(required), "architecture guide is missing {required:?}");
    }
}
```

- [ ] **Step 2: Run the architecture test and verify failure**

```bash
cargo test --test cli_help architecture_guide_covers_submission_dispatch_terminal_and_recovery_flows -- --exact --test-threads=1
```

Expected: FAIL because `docs/architecture-ja.md` does not exist.

- [ ] **Step 3: Write the architecture guide**

Use these top-level sections:

```markdown
# 内部アーキテクチャ

## コンポーネント
## 所有する状態と設定
## Submission から event まで
## Scheduler と agent dispatch
## Native launch gate
## Agent 終了と private temp cleanup
## 状態モデル
## Daemon startup recovery
## 診断と redaction
## セキュリティ境界
## プラットフォーム対応
## コードを追うための入口
```

Add Mermaid flowcharts for submission/event, scheduler/dispatch, terminal/cleanup, and startup recovery. Add tables for component ownership, event states, agent-run states, launch-gate states, and command stop boundaries. Explain transaction ordering without claiming that PGID is a complete descendant-containment boundary. Include direct source links to the owning Rust modules and the exact Linux/macOS support statement.

- [ ] **Step 4: Verify and commit architecture**

```bash
cargo test --test cli_help architecture_guide_covers_submission_dispatch_terminal_and_recovery_flows -- --exact --test-threads=1
git diff --check
git add docs/architecture-ja.md tests/integration/cli_help.rs
git commit -m "docs: explain internal architecture"
```

Expected: focused test PASS.

### Task 5: Troubleshooting Guide and README Hub

**Files:**
- Create: `docs/troubleshooting-ja.md`
- Modify: `README.md`
- Modify: `tests/integration/cli_help.rs`

**Interfaces:**
- Consumes: all guides from Tasks 1–4, bounded diagnostic behavior from `src/diagnostics.rs`, status rendering from `src/status.rs`, run lineage from `src/runs.rs`, and policy errors from `src/execution_policy.rs`.
- Produces: the final navigation hub, symptom-oriented recovery guide, and complete cross-document contract.

- [ ] **Step 1: Add failing README/link/troubleshooting contracts**

Add these tests and replace the README-only assertions in `documentation_contract_covers_current_operator_surface` with assertions against the responsible guide:

```rust
#[test]
fn readme_links_every_user_and_developer_guide() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let readme = std::fs::read_to_string(root.join("README.md")).unwrap();
    for link in [
        "docs/getting-started-ja.md", "docs/commands-ja.md", "docs/workflows-ja.md",
        "docs/architecture-ja.md", "docs/troubleshooting-ja.md",
    ] {
        assert!(readme.contains(link), "README is missing link {link}");
        assert!(root.join(link).is_file(), "README link target does not exist: {link}");
    }
}

#[test]
fn troubleshooting_uses_bounded_read_only_diagnostics_and_safe_recovery() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let guide = std::fs::read_to_string(root.join("docs/troubleshooting-ja.md")).unwrap();
    let status = guide.find("pueue-agent status").unwrap();
    let doctor = guide.find("pueue-agent doctor").unwrap();
    let events = guide.find("pueue-agent events").unwrap();
    let runs = guide.find("pueue-agent runs").unwrap();
    assert!(status < doctor && doctor < events && events < runs);
    for forbidden in ["UPDATE agent_runs", "DELETE FROM", "kill -9", "chmod 777"] {
        assert!(!guide.contains(forbidden), "unsafe recovery advice: {forbidden}");
    }
}
```

Refactor the existing documentation contract to read `README.md`, `docs/commands-ja.md`, `docs/workflows-ja.md`, and `templates/instructions.md`; keep every existing required phrase, but assert it in the document that now owns it.

Replace its README and operations loops with these concrete ownership checks; leave the existing `templates/instructions.md` and `templates/config.toml` loops unchanged:

```rust
let readme = std::fs::read_to_string(root.join("README.md")).unwrap();
for required in [".pueue-agent/state.json", "supervisor", "Ubuntu GitHub Actions"] {
    assert!(readme.contains(required), "README.md is missing {required:?}");
}

let commands = std::fs::read_to_string(root.join("docs/commands-ja.md")).unwrap();
for required in [
    "pueue-agent status --compact", "pueue-agent status --json",
    "pueue-agent wake --reason", "pueue-agent runs --follow",
    "pueue-agent submit-batch", "pueue-agent submit --kind control",
    "人間向け出力", "JSON 出力", "max_experiments", "request-id", "冪等",
    "raw Pueue", "`status --json` には submission の一覧を含めず",
    "runs --json", "polling 単位",
] {
    assert!(commands.contains(required), "docs/commands-ja.md is missing {required:?}");
}

let workflows = std::fs::read_to_string(root.join("docs/workflows-ja.md")).unwrap();
for required in [
    "Pueue task は kill しないが、active agent は drain 対象で、shutdown timeout 後に process tree を終了して timed_out と記録され得る",
    "VACUUM INTO", "service を停止して SQLite の整合性境界",
    "SQLite snapshot と旧 binary", "operator による SQLite の直接書き込み",
] {
    assert!(workflows.contains(required), "docs/workflows-ja.md is missing {required:?}");
}
```

- [ ] **Step 2: Run focused tests and verify the RED state**

```bash
cargo test --test cli_help readme_links_every_user_and_developer_guide -- --exact --test-threads=1
cargo test --test cli_help troubleshooting_uses_bounded_read_only_diagnostics_and_safe_recovery -- --exact --test-threads=1
```

Expected: FAIL because troubleshooting does not exist and README is not yet the new hub.

- [ ] **Step 3: Write the symptom-oriented troubleshooting guide**

Use this structure:

```markdown
# トラブルシューティング

## 最初に行う4段階の確認
## Service が起動していない
## Pueue profile または config が一致しない
## Execution policy を読み込めない
## Project が paused または halted
## Event が retry または dead-letter になった
## Native gate で agent が起動しない
## Private temp admission または cleanup が失敗する
## Callback を取りこぼしたように見える
## Upgrade が失敗または rollback した
## 安全のため案内しない操作
```

The first section must run diagnostics in this order:

```bash
pueue-agent status --compact
pueue-agent doctor
pueue-agent events --limit 100
pueue-agent runs --limit 100
```

Every symptom section uses the table columns `症状`, `まず確認`, `想定原因`, and `安全な復旧`. Recovery steps use supported CLI commands only and never recommend direct SQLite writes, permission weakening, raw policy rewriting, or unverified PID signals.

- [ ] **Step 4: Rewrite README as the concise hub**

Use these exact top-level sections:

```markdown
# pueue-agent

## 何をするツールか
## 全体像
## 対応環境
## クイックスタート
## よく使うコマンド
## ガイド
## セキュリティ上の重要事項
## 開発と検証
## ライセンス
```

Keep the top-level architecture diagram, a seven-command quick start, the Linux/macOS support table, and links to all five guides. Move detailed configuration, batch, upgrade, intervention, context-resume, detector, and state-directory explanations to their owning guides rather than duplicating them. Link `templates/config.toml` for the complete current config template.

- [ ] **Step 5: Run the complete documentation contract tests**

```bash
cargo test --test cli_help documentation_ -- --test-threads=1
cargo test --test cli_help readme_links_every_user_and_developer_guide -- --exact --test-threads=1
cargo test --test cli_help troubleshooting_uses_bounded_read_only_diagnostics_and_safe_recovery -- --exact --test-threads=1
git diff --check
```

Expected: all documentation tests PASS and no broken required text.

- [ ] **Step 6: Commit troubleshooting and README**

```bash
git add README.md docs/troubleshooting-ja.md tests/integration/cli_help.rs
git commit -m "docs: make README a documentation hub"
```

### Task 6: Cross-Document Audit and Host-Independent Verification

**Files:**
- Modify only if verification exposes a documentation mismatch: `README.md`, `docs/getting-started-ja.md`, `docs/commands-ja.md`, `docs/workflows-ja.md`, `docs/architecture-ja.md`, `docs/troubleshooting-ja.md`, `docs/operations-ja.md`, `tests/integration/cli_help.rs`

**Interfaces:**
- Consumes: all deliverables from Tasks 1–5.
- Produces: a verified documentation set ready for review and integration.

- [ ] **Step 1: Audit public command coverage against `src/cli.rs`**

Run:

```bash
rg -n '^    [A-Z][A-Za-z]+\(' src/cli.rs
rg -n '^### `pueue-agent ' docs/commands-ja.md
```

Compare the two lists manually. The only CLI variant omitted as a public heading must be hidden `InternalLaunch`; `SteerAction::List` must appear as `pueue-agent steer list`.

- [ ] **Step 2: Audit links and placeholders**

Run:

```bash
rg -n 'T[B]D|T[O]DO|F[I]XME|implement[ ]later' README.md docs/getting-started-ja.md docs/commands-ja.md docs/workflows-ja.md docs/architecture-ja.md docs/troubleshooting-ja.md docs/operations-ja.md
rg -n '\]\([^)]*\.md' README.md docs/getting-started-ja.md docs/commands-ja.md docs/workflows-ja.md docs/architecture-ja.md docs/troubleshooting-ja.md docs/operations-ja.md
git diff --check
```

Expected: no placeholders; every relative Markdown target exists; diff check succeeds.

- [ ] **Step 3: Run focused documentation and CLI tests**

```bash
cargo test --test cli_help documentation_ -- --test-threads=1
cargo test --test cli_help help_ -- --test-threads=1
```

Expected: PASS.

- [ ] **Step 4: Run all host-independent verification gates**

```bash
cargo check --all-targets
cargo check --release --all-targets
cargo test --all-targets -- --test-threads=1
bash -n install.sh bin/pueue-agent tests/e2e/run.sh tests/e2e/rust_supervisor.sh tests/support/fake_agent.sh tests/support/fake_codex.sh
git diff --check
```

Expected: every command exits 0. If macOS executable launch stalls before Rust test output, do not claim runtime GREEN; run the same four gates on Ubuntu GitHub Actions and record that run as the host-independent acceptance evidence.

- [ ] **Step 5: Review the final diff for scope and secret safety**

```bash
git status --short
git diff --stat HEAD~5..HEAD
rg -n 'AWS_SECRET|TOKEN=|PASSWORD=|BEGIN (RSA|OPENSSH) PRIVATE KEY' README.md docs/*.md
```

Expected: only documentation and `tests/integration/cli_help.rs` changed; no credential-like example values.

- [ ] **Step 6: Commit any verification-only corrections**

If Step 1–5 required a correction, stage only the affected documentation/test files and commit:

```bash
git add README.md docs/getting-started-ja.md docs/commands-ja.md docs/workflows-ja.md docs/architecture-ja.md docs/troubleshooting-ja.md docs/operations-ja.md tests/integration/cli_help.rs
git commit -m "docs: verify user and developer guides"
```

If no correction was required, do not create an empty commit.

## Plan Self-Review

- Every approved design section maps to a task: information architecture (Tasks 1–5), command list (Task 2), workflows (Task 3), internal architecture (Task 4), troubleshooting (Task 5), and maintenance verification (Task 6).
- Every created guide has a failing contract test before content is written.
- Existing README contract text is migrated rather than silently deleted.
- The old operations path remains valid.
- Linux and macOS support claims are consistent and tested.
- No task changes production behavior or adds a dependency.
- No placeholder steps or undefined helper APIs remain.
