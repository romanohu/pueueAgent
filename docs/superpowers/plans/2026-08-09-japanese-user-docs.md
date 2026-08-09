# Japanese User Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `README.md` と `templates/` を日本語化し、日本語話者が現行の Rust + SQLite supervisor を導入・設定・運用できる利用者向け資料にする。

**Architecture:** 既存の文書構成と実装を保ったまま、利用者向け説明文だけを日本語へ置き換える。CLI、TOML キー、環境変数、パス、正規表現、コード例は機械的に変えず、README と生成テンプレートの説明責務を分離する。

**Tech Stack:** Markdown、TOML、Bash、既存 Rust CLI (`pueue-agent`)

## Global Constraints

- 変更対象は `README.md`、`templates/config.toml`、`templates/instructions.md`、`templates/STATE.md` に限定する。
- `docs/superpowers/specs/`、`docs/superpowers/plans/`、`.superpowers/sdd/` の既存履歴資料は変更しない。ただし、この計画書と承認済み設計書は追加する。
- `pueue-agent`、`pueue`、`codex`、サブコマンド、設定キー、環境変数、パス、正規表現、SQL、コード例は原文表記を維持する。
- Rust 実装、CLI の挙動、SQLite スキーマ、設定キー、テンプレートの TOML 値は変更しない。
- 説明は日本語を主言語にし、技術用語は初出で意味が分かる短い説明を添える。

---

### Task 1: README を日本語化

**Files:**
- Modify: `README.md`
- Reference: `src/main.rs`, `src/config.rs`, `templates/config.toml`

**Interfaces:**
- Consumes: 現行 CLI のサブコマンド、設定項目、README に記載済みの運用仕様。
- Produces: 日本語で導入から移行までを説明する単一の `README.md`。コマンドブロックとリンクのパスは利用可能なままにする。

- [ ] **Step 1: 現行の CLI と設定表記を照合する**

  `README.md` のコマンド・設定キー・環境変数を `src/main.rs`、`src/config.rs`、`templates/config.toml` と照合し、翻訳中に名前や値を変えない箇所を確定する。

- [ ] **Step 2: README の説明文を日本語へ置き換える**

  次の順序を維持する。

  1. 概要と設計上の要点
  2. 動作の流れ
  3. 必要条件とインストール
  4. Quick Start
  5. プロジェクトファイルと共有実験コンテキスト
  6. TOML 設定リファレンス
  7. Codex の `fresh` / `resume` / `resume_latest`
  8. 異常検知と Pueue タスクの自動終了
  9. サービスと状態ファイル
  10. Bash/YAML 版からの移行

  `pueue-agent submit -- python train.py --lr 0.001`、TOML、ASCII 図、リンク先は維持する。自動終了の説明には、`notify` / `wake` / `kill`、Pueue 境界での kill、再検証、重複防止、失敗の可視化を含める。

- [ ] **Step 3: README の参照先とコード例を確認する**

  次のコマンドでリンク先と主要な固定表記を確認する。

  ```bash
  test -f templates/config.toml
  test -f templates/instructions.md
  test -f templates/STATE.md
  rg -n 'pueue-agent (init|enable|disable|status|pause|resume|submit)|agent.context|action = "(notify|wake|kill)"' README.md
  ```

- [ ] **Step 4: README の文書差分を確認する**

  ```bash
  git diff -- README.md
  git diff --check
  ```

### Task 2: 生成テンプレートを日本語化

**Files:**
- Modify: `templates/config.toml`
- Modify: `templates/instructions.md`
- Modify: `templates/STATE.md`
- Reference: `src/init.rs`, `src/config.rs`, `README.md`

**Interfaces:**
- Consumes: `pueue-agent init` が生成するファイルの構造と scheduler/detector が読む設定名。
- Produces: 日本語のコメント、エージェント指示、実験ノート。ファイル名、TOML 構造、キー、プレースホルダー、既定値は変更しない。

- [ ] **Step 1: `config.toml` のコメントだけを日本語化する**

  `project_id`、`pueue_group`、`agent.program`、`agent.args`、`agent.context`、`check`、`guardrails` のキーと値を保持する。`fresh` が既定であること、`resume` には `session_id` が必要であること、`resume_latest` が project-scoped であることを日本語コメントで説明する。

- [ ] **Step 2: `instructions.md` を日本語の運用指示に置き換える**

  起動時に `instructions.md` と `STATE.md` を読むこと、関連プロジェクトだけを調査すること、終了前に状態を記録すること、`pueue-agent submit` を使うこと、Codex context を勝手に変更しないこと、`kill` 済みタスクを再投入する前に Pueue 状態を確認することを残す。

- [ ] **Step 3: `STATE.md` の見出しとコメントを日本語で整理する**

  目的・制約・実験履歴・現在の状況・次の計画・ヘルスチェック履歴を残し、表の列名と記入例を日本語化する。agent が追記できる Markdown の形は維持する。

- [ ] **Step 4: テンプレートの構造を確認する**

  ```bash
  rg -n 'project_id|pueue_group|agent\.context|session_id|extra_log_paths|guardrails|pueue-agent submit' templates
  git diff -- templates
  git diff --check
  ```

### Task 3: 文書品質と回帰を検証

**Files:**
- Test: `README.md`, `templates/config.toml`, `templates/instructions.md`, `templates/STATE.md`

**Interfaces:**
- Consumes: Task 1 と Task 2 の日本語化された利用者向け資料。
- Produces: リンク切れ、設定表記の不一致、空白エラー、既存テスト回帰のない文書変更。

- [ ] **Step 1: 変更対象が想定範囲内か確認する**

  ```bash
  git status --short
  git diff --name-only HEAD
  ```

  出力には利用者向け4ファイルと承認済みの計画・設計書以外を含めない。

- [ ] **Step 2: Markdown のローカルリンクを確認する**

  ```bash
  for path in templates/config.toml templates/instructions.md templates/STATE.md; do
    test -e "$path"
  done
  rg -o '\]\([^)#]+' README.md | sed 's/^](//' | while read -r path; do
    case "$path" in
      http://*|https://*) ;;
      *) test -e "$path" || { echo "missing README link: $path" >&2; exit 1; } ;;
    esac
  done
  ```

- [ ] **Step 3: 既存の文書境界テストを実行する**

  ```bash
  bats tests
  shellcheck bin/pueue-agent install.sh tests/e2e/rust_supervisor.sh tests/support/*.sh tests/test_shell_entrypoints.bats
  git diff --check
  ```

- [ ] **Step 4: 必要に応じて Rust の全テストを実行する**

  ```bash
  PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --all-targets
  ```

  文書だけの変更でも、テンプレートと README の CLI 表記を照合した後に全体テストを実行し、最終状態の回帰がないことを確認する。

- [ ] **Step 5: 変更をコミットする**

  ```bash
  git add README.md templates/config.toml templates/instructions.md templates/STATE.md
  git commit -m "docs: translate user documentation into Japanese"
  ```
