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

### Review fix round 1

以下のコマンドを、指定された Rust toolchain PATH で実行した。

- `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --all-targets compact_status`
  - 成功。対象テスト 1 件成功。
- `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --all-targets readonly_open`
  - 成功。対象テスト 2 件成功。
- `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --all-targets --quiet`
  - 成功。全テスト成功。失敗 0 件。

あわせて、CLI help test の名前を `help_lists_diagnostics_commands_and_status_options` に変更し、`--json` と `--compact` の両方を表す名前に更新した。

## 懸念点

1. compact の Pueue 表示は command 本文を意図的に捨て、project group に限定した total/active/queued counts のみを表示する。これは payload/prompt-like text を含めない brief の契約に合わせたもの。
2. `--json --compact` の同時指定時は既存契約を優先し、`--json` の full JSON を出力する。
