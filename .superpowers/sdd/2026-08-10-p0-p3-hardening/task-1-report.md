# Task 1 Report: P0 read-only status と compact status

## 実装内容

- `StatusArgs.compact: bool` と `status --compact` を追加した。
- `commands::status` の DB 解決を `resolve_project` から既存の `resolve_project_read_only` へ切り替えた。
- `status --json` の分岐と既存の JSON renderer は変更していない。
- `status::render_project_status_compact(&Db, &Project, &StatusInput) -> Result<String, AppError>` を追加した。
- compact output は次の bounded summary のみを含む。
  - `pueue-agent` header
  - daemon status
  - project-scoped Pueue total/active/queued counts
  - experiments count
  - agent run active/failed counts
  - event pending/failed counts
  - guardrail summary
  - project enabled/paused/halted summary
- compact renderer は Pueue command、event payload、agent log path、prompt-like text を出力しない。
- read-only DB の `UPDATE`、`CREATE TABLE`、`PRAGMA user_version = ...` 拒否を統合テストへ追加した。
- CLI help と compact output の回帰テストを追加した。

## Commit

`5bdba04` (`feat: make status read-only and compact`)

## 実行したテスト

### TDD RED

- `cargo test --all-targets compact_status`
  - 実行結果: `cargo` が環境に存在せず、シェルが exit 127。
- `cargo test --all-targets readonly_open`
  - 実行結果: `cargo` が環境に存在せず、シェルが exit 127。

### 実行できた検証

- `git diff --check`
  - 成功。
- `command -v cargo`、`command -v rustc`、Rust toolchain の探索
  - `cargo` と `rustc` は見つからなかった。

したがって、この環境では focused test および `cargo test --all-targets` の GREEN 確認を実行できていない。

## 懸念点

1. Rust toolchain 不在のため、コンパイル、focused test、全 Rust test の実行結果は未確認である。Rust toolchain が利用可能な環境で、まず `cargo test --all-targets compact_status` と `cargo test --all-targets readonly_open`、続いて `cargo test --all-targets` を実行する必要がある。
2. compact の Pueue 表示は command 本文を意図的に捨て、project group に限定した total/active/queued counts のみを表示する。これは payload/prompt-like text を含めない brief の契約に合わせたもの。
3. `--json --compact` の同時指定時は既存契約を優先し、`--json` の full JSON を出力する。
