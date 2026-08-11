# Task 11 report — 日本語ドキュメントと最終回帰

## Commits

- 実装コミット: `6569232` (`docs: document p0-p3 operations`)
- このレポートは別コミットで記録する。

## 変更内容

- README に `status --compact` / `status --json`、`wake`、`runs --follow`、`submit-batch`、`submit --kind control` の使用例を追加した。
- human output と JSON output の違い、raw Pueue output と supervisor output の責務の違いを説明した。
- `control` submission が履歴には残るが `max_experiments` を消費しないことを明記した。
- batch の `request-id` 冪等性、accepted job の二重投入防止、未確定 job の再開境界を説明した。
- `.pueue-agent/state.json` を canonical machine state として説明し、`STATE.md` を補足ノートとして位置付けた。
- `templates/config.toml` と `templates/instructions.md` に state、control/experiment、operator intervention の最小限の運用手順を追加した。
- `tests/integration/cli_help.rs` に README/templates の documentation contract test を追加した。launcher の出力は変更していない。

## TDD evidence

- RED:
  `cargo test --test cli_help documentation_contract` — `documentation_contract_covers_current_operator_surface` が、更新前 README に `pueue-agent status --compact` がないため失敗した。
- GREEN:
  同じ focused command — 1 passed, 0 failed。
- CLI help target:
  `cargo test --all-targets` の `cli_help` target — 22 passed, 0 failed。

## Verification

- `cargo test --all-targets` — **326 passed, 0 failed**。
  - library 15、cli_help 22、config 28、daemon 13、database 67、detection 13、diagnostics 40、init 9、interventions 7、operator_commands 13、pueue_adapter 24、reconciliation 13、scheduler 33、service 10、termination 19。
- `rustfmt --edition 2021 --check tests/integration/cli_help.rs` — passed。
- `cargo fmt --check` — non-zero。既知の Task 1 由来 `tests/integration/database.rs:343` のみ。
- `cargo clippy --all-targets --all-features -- -D warnings` — non-zero。既存コードの `src/runs.rs` の `map_or` 2件と `src/state.rs` の不要な `?` 1件。Task 11 の変更箇所ではない。
- `bats tests/test_shell_entrypoints.bats` — 3 passed。
- `shellcheck --shell=bash bin/pueue-agent tests/test_shell_entrypoints.bats` — passed。
- `git diff --check` — passed。

実装コミット後の未コミット変更は、進捗管理のための計画書だけである。
