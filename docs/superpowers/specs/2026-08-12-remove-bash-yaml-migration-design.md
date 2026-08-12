# Bash/YAML 移行痕跡の削除 設計書

日付: 2026-08-12

ステータス: 承認済み

## 目的

現行の Rust + SQLite supervisor を唯一の `pueue-agent` 実装として説明できる状態にし、旧 Bash/YAML 版および Rust 移行期だけに必要だった設計・計画・設定互換の痕跡を削除する。SQLite の現行 schema migration や、運用に必要な Bash entrypoint/E2E は移行痕跡とは扱わず、引き続き保持する。

## 範囲と不変条件

- README の移行手順を削除し、通常の導入・設定・運用説明だけを残す。
- 旧 Bash/YAML 版または Rust 移行期の設計・計画である4ファイルは、内容を編集せずファイルごと削除する。
- 日本語利用者向け文書、Periodic DeepCheck 文書、運用文書はファイルを保持し、旧移行や `deep_check_every` の説明だけを現行仕様へ修正する。
- runtime で参照されていない `check.deep_check_every` は、Rust の public/raw config model、validation、template、利用者向け文書、fixture、assertion から完全に除去する。
- `RawCheckConfig` の `deny_unknown_fields` は維持する。削除後に未定義の `[check]` key は黙って無視せず拒否する。
- `install.sh`、`bin/pueue-agent`、E2E/support Bash、README の Bash code fence、Pueue 自身の `.yml` profile と `--pueue-config` は保持する。
- SQLite schema migration、legacy table repair、関連する database test は変更しない。
- credentials、runtime state、秘密情報を追加・生成・移動しない。

## 削除・修正・保持マトリクス

| 対象 | 扱い | 完了条件 |
| --- | --- | --- |
| `README.md` の `## Bash/YAML 版からの移行` 節 | 節を削除 | 移行手順、旧 registry/PID lock/cron/sentinel/YAML backup の説明を除去する。その他の Bash code fence、`./install.sh`、`bin/pueue-agent`、Pueue profile の説明は残す。 |
| `docs/superpowers/specs/2026-08-04-pueue-agent-design.md` | ファイル削除 | 旧 Bash supervisor と Rust 版を並行して移行する設計書を履歴から除去する。 |
| `docs/superpowers/plans/2026-08-04-pueue-agent.md` | ファイル削除 | 旧 Bash/YAML 実装からの移行計画を履歴から除去する。 |
| `docs/superpowers/specs/2026-08-09-rust-sqlite-agent-supervisor-design.md` | ファイル削除 | Rust 移行期の設計書を履歴から除去する。 |
| `docs/superpowers/plans/2026-08-09-rust-sqlite-agent-supervisor.md` | ファイル削除 | Rust 移行期の実装計画を履歴から除去する。 |
| `docs/superpowers/specs/2026-08-09-japanese-user-docs-design.md`、`docs/superpowers/plans/2026-08-09-japanese-user-docs.md` | 保持して修正 | 旧 Bash/YAML の移行手順を対象にする記述と README 構成中の移行節を削除する。日本語化の目的・現行 CLI・テンプレート仕様は残す。 |
| `docs/superpowers/specs/2026-08-11-periodic-deep-check-design.md`、`docs/superpowers/plans/2026-08-11-periodic-deep-check.md` | 保持して修正 | `deep_check_interval_minutes` だけを Periodic DeepCheck の設定とし、`deep_check_every` の互換受理、deprecated 表記、legacy compatibility test/plan step を削除する。project 単位の coalescing、token 消費、`STATE.md` 記録の現行仕様は残す。 |
| `docs/operations-ja.md` | 保持して修正 | `deep_check_every` と legacy 設定の説明を削除し、`deep_check_interval_minutes = 0` が無効、正の値だけが opt-in という現行表現に統一する。更新手順、Pueue profile、`--pueue-config` は残す。 |
| `src/config.rs` | 修正 | `CheckConfig.deep_check_every`、`RawCheckConfig.deep_check_every`、その `positive` validation と参照を削除する。`deep_check_interval_minutes` の non-negative validation と `RawCheckConfig` の `deny_unknown_fields` は維持する。 |
| `templates/config.toml` | 修正 | `deep_check_every` と legacy コメントを削除する。`deep_check_interval_minutes = 0` と、その無効化/opt-in の説明は残す。 |
| `tests/integration/config.rs` | 修正・追加 | 旧キーの値・zero frequency test・legacy frequency test・legacy 文書 assertion を削除し、一般的な未定義 `[check]` key を含む config が `config.toml` エラーで拒否される negative test を追加する。削除対象キー名を test fixture/assertion の literal として再導入しない。interval の `0`/正値と既存 unknown-key rejection の挙動は維持する。 |
| `tests/integration/daemon.rs`、`detection.rs`、`operator_commands.rs`、`periodic.rs`、`pueue_adapter.rs`、`scheduler.rs`、`service.rs` | fixture/struct literal だけ修正 | `deep_check_every` の TOML fixture 行または `CheckConfig` field を削除する。各テストの scheduler、detector、service、Pueue 動作は変更しない。 |
| `tests/e2e/rust_supervisor.sh` | fixture だけ修正し、script は保持 | embedded config の旧キーを削除する。E2E の Bash 手順・検証は削除しない。 |
| `install.sh`、`bin/pueue-agent`、`tests/e2e/*.sh`、`tests/support/*.sh`、`tests/test_shell_entrypoints.bats` | 保持 | 現行 Rust binary の install/development launcher、E2E/support の Bash 経路を残す。fixture の旧キー除去以外の変更は行わない。 |
| Pueue の `.yml` profile と `--pueue-config` | 保持 | Pueue 自身の設定形式を `pueue-agent` の旧 project YAML と混同しない。README/operations の profile path と option の例は残す。 |
| `src/db/migrations.rs`、`tests/integration/database.rs` | 保持 | SQLite schema migration、legacy table repair、migration 回帰テストは Bash/YAML 移行と無関係なので削除・改名しない。 |

## 設定互換性の変更

`check.deep_check_every` は runtime で読まれていないため、新しい config schema には存在しない。新しい `src/config.rs` は `deep_check_interval_minutes` のみを Periodic DeepCheck の周期設定として扱い、`0` は無効、正の値は opt-in とする。

旧 schema の field が残っている既存 `.pueue-agent/config.toml` は、新 binary の config load が `RawCheckConfig` の `deny_unknown_fields` により失敗し、既存の parser mapping に従って `config.toml` configuration error を返す。これは意図した compatibility break であり、alias、serde の ignore、既定値による受理、旧 field からの自動変換は実装しない。Pueue の `.yml` profile や `--pueue-config` の解釈には影響しない。

新規生成される template には旧キーを出力しない。Periodic DeepCheck の設計・運用文書は、旧設定を移行する手順を追加せず、現行の interval 設定と unknown key rejection の事実だけを記載する。

## 実装単位

1. **config schema 単位** — `src/config.rs` の public/raw model と validation から field を除去し、`templates/config.toml` を現行 schema に合わせる。`deep_check_interval_minutes` の validation と `deny_unknown_fields` を変更しない。
2. **fixture/test 単位** — 全 fixture と `CheckConfig` struct literal から field を除去し、`tests/integration/config.rs` に一般的な未定義 `[check]` key の拒否テストを追加する。旧 frequency の意味を検証する assertion は削除し、interval の opt-in と未知キー拒否を検証する。拒否テストに削除対象キーの literal は使用しない。

   拒否テストは、たとえば既存 fixture の `stall_minutes` の直後に `unknown_check_key = 1` を挿入して load error を取得し、既存の parser mapping が返す `config.toml` を assertion する。削除対象だった field 名を test source に書かないことで、runtime schema と最終 negative grep の両方を検証する。
3. **利用者文書単位** — README の移行節を削除し、config table/Periodic DeepCheck と `docs/operations-ja.md` を interval のみの表現へ修正する。日本語 user-doc spec/plan と periodic DeepCheck spec/plan は保持したまま旧移行・legacy frequency 記述を修正する。
4. **履歴整理単位** — 指定された4つの旧 design/plan ファイルを削除する。Bash launcher、E2E/support、Pueue profile、SQLite migration はこの単位の対象外として残す。

## テストと受け入れ条件

実装後は、最終 cleanup（この設計書と、この設計から作成した implementation plan を削除する処理）を完了した tree に対して、次をすべて満たすこと。cleanup は通常の差分で行い、git history の rewrite は行わない。

- `cargo test --all-targets` が成功する。
- `cargo fmt --check` と `git diff --check` が成功する。
- `deep_check_interval_minutes = 0` と正の値の config test が成功し、未定義の `[check]` key を含む config が成功しない。拒否テストは既存の `deny_unknown_fields` 経路を通ることを確認する。削除対象キー名は test fixture/assertion に現れない。
- 次の旧ファイルが存在しない。

  ```text
  docs/superpowers/specs/2026-08-04-pueue-agent-design.md
  docs/superpowers/plans/2026-08-04-pueue-agent.md
  docs/superpowers/specs/2026-08-09-rust-sqlite-agent-supervisor-design.md
  docs/superpowers/plans/2026-08-09-rust-sqlite-agent-supervisor.md
  ```

- 設計書や implementation plan の除外指定なしで、次の negative grep が無出力になる。

  ```bash
  rg -n 'deep_check_every' README.md templates src tests docs
  rg -n 'Bash/YAML 版からの移行|旧 Bash supervisor|pueue-agent sentinel|global text registry|PID lock|cron/sentinel|config\.yml' \
    README.md docs/superpowers/specs/2026-08-09-japanese-user-docs-design.md \
    docs/superpowers/plans/2026-08-09-japanese-user-docs.md docs/operations-ja.md
  ```

- `test -f install.sh`, `test -f bin/pueue-agent`、E2E/support Bash の存在確認が成功し、README/operations の `--pueue-config` 例が残る。
- `src/db/migrations.rs` と `tests/integration/database.rs` の SQLite migration/legacy table repair test が差分に含まれない。
- 実装差分のファイル一覧が、この設計の修正・削除マトリクス以外を含まない。

設計作成中は本文が削除対象語を説明するため、この negative grep を実行しても一致が残る。実装完了時に設計書と implementation plan を削除してから再実行し、最終 tree 全体で一致がないことを確認する。削除は履歴の rewrite ではなく、通常の cleanup commit とする。

## リスクと対策

- **既存 project が起動不能になる。** 旧キーを含む config は意図的に拒否される。config error を既存の形式で返し、template と通常の設定説明から旧キーを除去して新規発生を防ぐ。
- **移行痕跡の削除が SQLite の保守コードへ波及する。** Bash/YAML の具体的な語句を negative grep の対象に限定し、`src/db/migrations.rs` と database migration test を保持対象として明記する。
- **現役 Bash を誤って削除する。** 削除対象を4つの design/plan と README の1節に限定し、launcher/E2E/support と Bash code fence の存在を受け入れ条件で確認する。embedded fixture の旧キー1行だけは schema 整合のため除去する。
- **Pueue の YAML 設定まで消える。** project config の TOML と Pueue の `.yml` profile を別物として扱い、`--pueue-config` と profile path を保持する。
- **Periodic DeepCheck の意味が文書間でずれる。** README、operations、periodic spec/plan のすべてを `deep_check_interval_minutes`、`0` disabled、positive opt-in に揃え、legacy frequency の互換説明を残さない。
