# Bash/YAML 移行痕跡の削除 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 現行の Rust + SQLite supervisor だけを `pueue-agent` の実装として説明できるよう、旧 Bash/YAML 移行痕跡と未使用の設定キーを、現役の Bash 経路・Pueue profile・SQLite migration を壊さずに削除する。

**Architecture:** Task 1 で public/raw config model、validation、template、全 fixture を同じ schema にそろえ、未定義の `[check]` key を `deny_unknown_fields` 経由で拒否する。Task 2 で README と保持対象の利用者/履歴文書を現行の interval-only 仕様へそろえ、指定された4つの旧 design/plan をファイル単位で削除する。Task 3 でこの spec とこの plan を読み終えた後に削除し、最終 tree 全体の negative grep と host-independent verification を通す。

**Tech Stack:** Rust 2021、Cargo、Serde/TOML、Markdown、Bash、SQLite（schema migration は変更しない）。

## Global Constraints

- README の移行手順を削除し、通常の導入・設定・運用説明だけを残す。
- 旧 Bash/YAML 版または Rust 移行期の設計・計画である4ファイルは、内容を編集せずファイルごと削除する。
- 日本語利用者向け文書、Periodic DeepCheck 文書、運用文書はファイルを保持し、旧移行や `deep_check_every` の説明だけを現行仕様へ修正する。
- runtime で参照されていない `check.deep_check_every` は、Rust の public/raw config model、validation、template、利用者向け文書、fixture、assertion から完全に除去する。
- `RawCheckConfig` の `deny_unknown_fields` は維持する。削除後に未定義の `[check]` key は黙って無視せず拒否する。
- `install.sh`、`bin/pueue-agent`、E2E/support Bash、README の Bash code fence、Pueue 自身の `.yml` profile と `--pueue-config` は保持する。
- SQLite schema migration、legacy table repair、関連する database test は変更しない。
- credentials、runtime state、秘密情報を追加・生成・移動しない。
- `deep_check_interval_minutes` だけを Periodic DeepCheck の周期設定として扱い、`0` は無効、正の値は opt-in とする。
- 旧 schema の field を含む config は `RawCheckConfig` の `deny_unknown_fields` で load に失敗し、既存の parser mapping に従って `config.toml` configuration error を返す。alias、serde の ignore、既定値による受理、旧 field からの自動変換は実装しない。
- `deep_check_every` の文字列を最終 tree の test fixture、assertion、production source、保持文書へ再導入しない。Task 3 の spec/plan 削除後に全 tree の negative grep を実行する。
- `cargo fmt --check` は実行し、`cargo fmt`/rustfmt が利用できない場合は unavailable と明記する。利用不可を成功として扱わず、`git diff --check` と `cargo test --all-targets` の結果とは分けて記録する。

---

## File Map

### Task 1で変更するファイル

- Modify: `src/config.rs` — `CheckConfig`、`RawCheckConfig`、`RawCheckConfig::validate` から未使用 field と positive validation を除去する。
- Modify: `templates/config.toml` — interval-only の生成 template にする。
- Modify: `tests/integration/config.rs` — valid fixture と config regression test を更新する。
- Modify: `tests/integration/daemon.rs` — embedded TOML fixture の該当行だけを削除する。
- Modify: `tests/integration/detection.rs` — `CheckConfig` struct literal の該当 field だけを削除する。
- Modify: `tests/integration/operator_commands.rs` — embedded TOML fixture の該当行だけを削除する。
- Modify: `tests/integration/periodic.rs` — embedded TOML fixture の該当行だけを削除する。
- Modify: `tests/integration/pueue_adapter.rs` — embedded TOML fixture の該当行だけを削除する。
- Modify: `tests/integration/scheduler.rs` — 5か所の embedded TOML fixture の該当行だけを削除する。
- Modify: `tests/integration/service.rs` — embedded TOML fixture の該当行だけを削除する。
- Modify: `tests/e2e/rust_supervisor.sh` — embedded config の該当行だけを削除し、Bash 手順は保持する。

### Task 2で変更・削除するファイル

- Modify: `README.md` —移行節、旧 key の表行/説明を削除し、現行の interval、token、coalescing、`STATE.md` 説明を保持する。
- Modify: `docs/operations-ja.md` — Periodic DeepCheck を interval-only 表現にする。更新手順、Pueue profile、`--pueue-config` は保持する。
- Modify: `docs/superpowers/specs/2026-08-09-japanese-user-docs-design.md` — README 構成と旧移行方針だけを除去する。
- Modify: `docs/superpowers/plans/2026-08-09-japanese-user-docs.md` — README の旧移行項目だけを除去する。
- Modify: `docs/superpowers/specs/2026-08-11-periodic-deep-check-design.md` —旧設定の互換受理/deprecated 記述を除去する。
- Modify: `docs/superpowers/plans/2026-08-11-periodic-deep-check.md` —旧設定の compatibility test/plan step を interval-only の記述へ置換する。
- Delete: `docs/superpowers/specs/2026-08-04-pueue-agent-design.md`
- Delete: `docs/superpowers/plans/2026-08-04-pueue-agent.md`
- Delete: `docs/superpowers/specs/2026-08-09-rust-sqlite-agent-supervisor-design.md`
- Delete: `docs/superpowers/plans/2026-08-09-rust-sqlite-agent-supervisor.md`

### Task 3で最後に削除するファイル

- Delete: `docs/superpowers/specs/2026-08-12-remove-bash-yaml-migration-design.md` —実装者が spec を読み終えてから `git rm` する。
- Delete: `docs/superpowers/plans/2026-08-12-remove-bash-yaml-migration.md` —実装者がこの plan の全 task を読み終えてから `git rm` する。

### 変更しないファイル/経路

- Preserve: `install.sh`、`bin/pueue-agent`、`tests/e2e/*.sh`、`tests/support/*.sh`、`tests/test_shell_entrypoints.bats`。ただし `tests/e2e/rust_supervisor.sh` の embedded fixture から該当行を1行削除する。
- Preserve: README と `docs/operations-ja.md` の Pueue profile path、および `--pueue-config` の例。
- Preserve: Pueue 自身の `.yml` profile の解釈。project の TOML と Pueue の YAML profile を同一視しない。
- Preserve: `src/db/migrations.rs`、`tests/integration/database.rs` の SQLite schema migration、legacy table repair、回帰 test。

## Interfaces

- Config loader entrypoint: `config::load(path: &Path) -> Result<ProjectConfig, AppError>`。
- Task 1後の public config shape: `CheckConfig { interval_minutes: u32, deep_check_interval_minutes: u32, stall_minutes: u32, log_tail_bytes: u32, extra_log_paths: Vec<PathBuf>, patterns: Vec<PatternConfig>, stall: StallConfig }`。
- Task 1後の raw conversion: `RawCheckConfig::validate(self) -> Result<CheckConfig, AppError>` は `deep_check_interval_minutes` に `non_negative` validation を適用し、他の validation と `#[serde(default, deny_unknown_fields)]` を保持する。
- Task 1後の schema contract: `[check]` に `unknown_check_key = 1` のような未定義 key がある場合、`config::load` は `Err(AppError::Configuration { field: "config.toml" })` 相当の既存表示を返す。
- Periodic consumer: `src/periodic.rs` は `check.deep_check_interval_minutes` だけを読む。Task 1では periodic scheduler の型、計算、SQLite query、dispatch 順序を変更しない。
- Task 2後の documentation contract: README、operations、日本語 user-doc/Periodic DeepCheck の保持文書は `deep_check_interval_minutes = 0` disabled、正の値 opt-in、project coalescing、token 消費、`STATE.md` 記録を説明し、旧移行手順と旧 key の説明を含まない。

---

### Task 1: Config schema、unknown-key regression、全 fixture/template の整合

**Files:**

- Modify: `tests/integration/config.rs` (`valid_config`、旧 frequency test の削除、zero/positive interval test、`unknown_check_key_is_rejected`)
- Modify: `src/config.rs` (`CheckConfig`、`RawCheckConfig`、`RawCheckConfig::validate`)
- Modify: `templates/config.toml`
- Modify: `tests/integration/daemon.rs`
- Modify: `tests/integration/detection.rs`
- Modify: `tests/integration/operator_commands.rs`
- Modify: `tests/integration/periodic.rs`
- Modify: `tests/integration/pueue_adapter.rs`
- Modify: `tests/integration/scheduler.rs`
- Modify: `tests/integration/service.rs`
- Modify: `tests/e2e/rust_supervisor.sh`

**Interfaces:**

- Consumes: 現行の `config::load`、`ProjectConfig.check.deep_check_interval_minutes`、`RawCheckConfig` の `deny_unknown_fields`。
- Produces: 旧 field のない `CheckConfig`/`RawCheckConfig`、interval の `0`/正値を受理する `zero_deep_check_interval_is_disabled`/`positive_deep_check_interval_is_opt_in`、未定義 `[check]` key を `config.toml` error として拒否する `unknown_check_key_is_rejected`、template を実際に load/validate する `generated_template_loads_with_current_check_schema`、旧 field のない全 fixture。
- Does not change: `deep_check_interval_minutes` の `non_negative` validation、既存の positive interval/stall/guardrail validation、Periodic scheduler、Pueue adapter、SQLite code。

- [ ] **Step 1: Write the RED config tests and remove the obsolete config assertions**

  `tests/integration/config.rs` の `valid_config()` から旧 field の TOML 行を削除する。`zero_deep_check_frequency_is_rejected`、`deep_check_interval_is_opt_in_and_legacy_frequency_does_not_enable_it`、`periodic_deep_check_template_and_readme_explain_opt_in_health_records` は test function と本文を全体削除する。既存の `negative_deep_check_interval_is_rejected` は維持する。README の文面 assertion は追加しない。

  ```rust
  #[test]
  fn zero_deep_check_interval_is_disabled() {
      let config = load_config(valid_config()).unwrap();

      assert_eq!(config.check.deep_check_interval_minutes, 0);
  }

  #[test]
  fn positive_deep_check_interval_is_opt_in() {
      let config = load_config(
          valid_config().replace(
              "deep_check_interval_minutes = 0",
              "deep_check_interval_minutes = 30",
          ),
      )
      .unwrap();

      assert_eq!(config.check.deep_check_interval_minutes, 30);
  }

  #[test]
  fn unknown_check_key_is_rejected() {
      let config = valid_config().replace(
          "stall_minutes = 30",
          "stall_minutes = 30\nunknown_check_key = 1",
      );

      let error = load_config(config).unwrap_err();

      assert!(error.to_string().contains("config.toml"));
  }

  #[test]
  fn generated_template_loads_with_current_check_schema() {
      let root = Path::new(env!("CARGO_MANIFEST_DIR"));
      let template = fs::read_to_string(root.join("templates/config.toml")).unwrap();
      let template = template
          .replace("{{PROJECT_ID}}", "project-template-1234567890")
          .replace("{{PUEUE_GROUP}}", "pa-template-123456");

      let config = load_config(template).unwrap();

      assert_eq!(config.check.deep_check_interval_minutes, 0);
  }
  ```

- [ ] **Step 2: Run the config test suite to prove RED before production edits**

  Run:

  ```bash
  cargo test --test config
  ```

  Expected: FAIL。`valid_config()` が旧 field を含まなくなった一方で、現行 `RawCheckConfig::validate` が未指定 field の default `0` に positive validation を適用するため、valid fixture を使う test が `check.deep_check_every` の configuration error になる。新しい generated-template test はこの時点では現行 raw model が template の旧 field を受理するため PASS するが、config test 全体の RED は valid fixture の failure で確認する。

- [ ] **Step 3: Remove the field from the Rust public/raw config model after RED**

  `src/config.rs` で次の形になるように変更する。

  ```rust
  pub struct CheckConfig {
      pub interval_minutes: u32,
      pub deep_check_interval_minutes: u32,
      pub stall_minutes: u32,
      pub log_tail_bytes: u32,
      pub extra_log_paths: Vec<PathBuf>,
      pub patterns: Vec<PatternConfig>,
      pub stall: StallConfig,
  }

  #[derive(Debug, Deserialize, Default)]
  #[serde(default, deny_unknown_fields)]
  struct RawCheckConfig {
      interval_minutes: i64,
      deep_check_interval_minutes: i64,
      stall_minutes: i64,
      #[serde(default = "default_log_tail_bytes")]
      log_tail_bytes: i64,
      extra_log_paths: Vec<PathBuf>,
      patterns: Vec<RawPatternConfig>,
      stall: RawStallConfig,
  }
  ```

  `RawCheckConfig::validate` では `interval_minutes`、`deep_check_interval_minutes`、`stall_minutes` の順に既存 validation を残し、interval だけを次の形で変換する。`positive(self.deep_check_every, ...)` の呼び出しは存在しない状態にする。

  ```rust
  deep_check_interval_minutes: non_negative(
      self.deep_check_interval_minutes,
      "check.deep_check_interval_minutes",
  )?,
  ```

  `#[serde(default, deny_unknown_fields)]` は削除、緩和、alias 追加をせず、そのまま維持する。

- [ ] **Step 4: Run the generated-template test after the schema change to prove the template is RED**

  Step 3 の production source 変更後、template はまだ更新せず、次の test だけを実行する。

  ```bash
  cargo test --test config generated_template_loads_with_current_check_schema -- --exact
  ```

  Expected: FAIL。`RawCheckConfig` の `deny_unknown_fields` が、未更新の `templates/config.toml` に残る旧 field を unknown field として `config.toml` configuration error にする。この failure を確認してから template を変更する。

- [ ] **Step 5: Update the generated template without changing current interval semantics**

  `templates/config.toml` の `[check]` 部分を次の順序にする。旧 key と互換コメントはなくし、interval の既定値と opt-in 説明は残す。

  ```toml
  [check]
  interval_minutes = 10
  # 0 は Periodic DeepCheck を無効化する（正の分数を指定した場合だけ opt in）。
  deep_check_interval_minutes = 0
  stall_minutes = 30
  ```

- [ ] **Step 6: Remove the obsolete field from every fixture and literal**

  次の各ファイルで、指定した embedded TOML 行または struct field だけを削除する。scheduler、detector、service、Pueue の他の fixture 値、Bash の起動手順、assertion は変更しない。

  - `tests/integration/daemon.rs` — embedded `[check]` fixture の旧行。
  - `tests/integration/detection.rs` — `check_config(log_tail_bytes: u32) -> CheckConfig` の struct literal field。
  - `tests/integration/operator_commands.rs` — embedded `[check]` fixture の旧行。
  - `tests/integration/periodic.rs` — embedded `[check]` fixture の旧行。
  - `tests/integration/pueue_adapter.rs` — embedded `[check]` fixture の旧行。
  - `tests/integration/scheduler.rs` — embedded fixture 5か所（現在の約76、255、1105、1516、1972行）。
  - `tests/integration/service.rs` — embedded `[check]` fixture の旧行。
  - `tests/e2e/rust_supervisor.sh` — embedded config の旧行だけ。E2E の Bash 手順・検証は保持する。

  変更後、次の検索が production、template、全 test fixture に一致しないことを確認する。

  ```bash
  rg -n 'deep_check_every' templates src tests
  ```

  Expected: no output。`rg` の no-match exit status 1 はこの negative check の成功条件であり、通常の test failure とは区別する。

- [ ] **Step 7: Run the focused config and affected integration tests to verify GREEN**

  Run each command separately:

  ```bash
  cargo test --test config
  cargo test --test daemon
  cargo test --test detection
  cargo test --test operator_commands
  cargo test --test periodic
  cargo test --test pueue_adapter
  cargo test --test scheduler
  cargo test --test service
  ```

  Expected: すべて PASS。特に `zero_deep_check_interval_is_disabled`、`positive_deep_check_interval_is_opt_in`、`unknown_check_key_is_rejected`、`generated_template_loads_with_current_check_schema` が PASS し、未定義 key の error は `config.toml` を含む。旧 field を含む fixture が残っていれば `deny_unknown_fields` による error になるため、commit 前に Step 6 の検索結果を確認する。

- [ ] **Step 8: Commit the schema/fixture review gate**

  ```bash
  git add src/config.rs templates/config.toml \
    tests/integration/config.rs tests/integration/daemon.rs \
    tests/integration/detection.rs tests/integration/operator_commands.rs \
    tests/integration/periodic.rs tests/integration/pueue_adapter.rs \
    tests/integration/scheduler.rs tests/integration/service.rs \
    tests/e2e/rust_supervisor.sh
  git commit -m "refactor: remove legacy check config field"
  ```

  Review gate: commit の差分は Task 1 の一覧だけで、`src/db/migrations.rs` と `tests/integration/database.rs` を含まず、config test と影響を受けた integration test が PASS していること。

---

### Task 2: 利用者/現行履歴文書の整合、4つの旧ファイル削除、focused negative grep

**Files:**

- Modify: `README.md`
- Modify: `docs/operations-ja.md`
- Modify: `docs/superpowers/specs/2026-08-09-japanese-user-docs-design.md`
- Modify: `docs/superpowers/plans/2026-08-09-japanese-user-docs.md`
- Modify: `docs/superpowers/specs/2026-08-11-periodic-deep-check-design.md`
- Modify: `docs/superpowers/plans/2026-08-11-periodic-deep-check.md`
- Delete: `docs/superpowers/specs/2026-08-04-pueue-agent-design.md`
- Delete: `docs/superpowers/plans/2026-08-04-pueue-agent.md`
- Delete: `docs/superpowers/specs/2026-08-09-rust-sqlite-agent-supervisor-design.md`
- Delete: `docs/superpowers/plans/2026-08-09-rust-sqlite-agent-supervisor.md`

**Interfaces:**

- Consumes: Task 1 の interval-only config schema、`templates/config.toml`、保持対象の current runtime behavior。
- Produces: 旧移行節と旧設定の説明がない README/operations/current historical docs、旧 design/plan 4ファイルの不存在。
- Preserves: README の通常の Bash code fence、`./install.sh`、`bin/pueue-agent`、Pueue profile path、`--pueue-config`、Periodic DeepCheck の project coalescing/token/`STATE.md` 説明。

- [ ] **Step 1: Record the documentation RED baseline before editing**

  Task 1 の commit 後に、次の focused negative grep を実行する。旧記述が残っているため一致が出ることが RED の証拠になる。

  ```bash
  rg -n 'deep_check_every' README.md templates src tests \
    docs/superpowers/specs/2026-08-09-japanese-user-docs-design.md \
    docs/superpowers/plans/2026-08-09-japanese-user-docs.md \
    docs/superpowers/specs/2026-08-11-periodic-deep-check-design.md \
    docs/superpowers/plans/2026-08-11-periodic-deep-check.md \
    docs/operations-ja.md
  rg -n 'Bash/YAML 版からの移行|旧 Bash supervisor|pueue-agent sentinel|global text registry|PID lock|cron/sentinel|config\.yml' \
    README.md \
    docs/superpowers/specs/2026-08-09-japanese-user-docs-design.md \
    docs/superpowers/plans/2026-08-09-japanese-user-docs.md \
    docs/operations-ja.md
  ```

  Expected: 1つ以上の一致が旧 README/current docs にあり、まだ削除対象の design/plan もあるため、no-output 条件を満たさない。Task 2 ではこの出力以外の runtime/config behavior を変更しない。

- [ ] **Step 2: Remove README の移行節と旧設定の説明**

  `README.md` の設定表から旧 key の行を削除する。Periodic DeepCheck の段落は、次の interval-only 内容にする。

  ```markdown
  agent DeepCheck は別の opt-in 機能です。`check.deep_check_interval_minutes` に正の分数を設定したときだけ、長時間実行中の実験について agent を起動し、metric と artifact から進行の健全性を確認します。この run は agent のトークンを消費します。
  ```

  `periodic DeepCheck` の project-level coalescing、pending/claimed/retry 待ちの扱い、正常進行を `STATE.md` に記録し canonical `state.json` を上書きしない説明は残す。`## Bash/YAML 版からの移行` 見出しから旧 registry、PID lock、cron/sentinel、旧 YAML backup、sentinel command の説明までを節全体で削除する。移行節以外の Bash code fence、`./install.sh`、`bin/pueue-agent`、Pueue profile の説明は変更しない。

- [ ] **Step 3: Update operations and retained Japanese user-document history**

  `docs/operations-ja.md` の Periodic DeepCheck 冒頭を、`check.deep_check_interval_minutes = 0` が既定で無効、正の値だけが opt-in である表現にそろえる。正常 tick が token を消費しない条件、event dispatch 時だけ token を消費する条件、task を kill しないこと、project coalescing、実際に取得した値だけを `STATE.md` に記録する方針はそのまま残す。更新コマンド、`~/.config/pueue/experiments.yml`、`--pueue-config`、Pueue profile 優先順位は変更しない。

  `docs/superpowers/specs/2026-08-09-japanese-user-docs-design.md` では旧移行手順を日本語化する方針の bullet と README 構成の旧移行項目を削除する。`docs/superpowers/plans/2026-08-09-japanese-user-docs.md` では README の構成リストから旧移行項目を削除し、「導入から移行まで」のような旧移行範囲を「導入・設定・運用」へ直す。日本語化の目的、現行 CLI、template 仕様、リンク/テスト検証方針は保持する。

- [ ] **Step 4: Remove obsolete compatibility wording from retained Periodic DeepCheck records**

  `docs/superpowers/specs/2026-08-11-periodic-deep-check-design.md` の設定節から旧設定を読み取り可能なまま残す、定期起動には使わない、deprecated と明記するという bullet を削除する。interval の TOML example、`0` disabled、`1` 以上 enabled、`interval_minutes` が reconciliation 周期である説明は保持する。

  `docs/superpowers/plans/2026-08-11-periodic-deep-check.md` では Global Constraints の旧 compatibility bullet を削除し、Task 4 の config test を `deep_check_interval_minutes = 0` と `30` の parse/behavior test、および README/template の interval-only assertion として記述する。template/doc の step は、interval の default、0 disabled、positive opt-in、reconciliation と agent DeepCheck の差、token 条件、project coalescing、`STATE.md` の bounded health record を説明する内容にする。旧 field を受理する test や plan step は残さない。

- [ ] **Step 5: Delete the four historical design/plan files without editing their contents**

  次の4パスだけをファイル単位で削除する。README、operations、保持対象の2組の design/plan は削除しない。

  ```bash
  git rm \
    docs/superpowers/specs/2026-08-04-pueue-agent-design.md \
    docs/superpowers/plans/2026-08-04-pueue-agent.md \
    docs/superpowers/specs/2026-08-09-rust-sqlite-agent-supervisor-design.md \
    docs/superpowers/plans/2026-08-09-rust-sqlite-agent-supervisor.md
  ```

- [ ] **Step 6: Run focused negative grep and retention checks to verify GREEN**

  Run each negative search separately; no output is required. `rg` の no-match exit status 1 は成功条件として扱う。

  ```bash
  rg -n 'deep_check_every' README.md templates src tests \
    docs/superpowers/specs/2026-08-09-japanese-user-docs-design.md \
    docs/superpowers/plans/2026-08-09-japanese-user-docs.md \
    docs/superpowers/specs/2026-08-11-periodic-deep-check-design.md \
    docs/superpowers/plans/2026-08-11-periodic-deep-check.md \
    docs/operations-ja.md
  rg -n 'Bash/YAML 版からの移行|旧 Bash supervisor|pueue-agent sentinel|global text registry|PID lock|cron/sentinel|config\.yml' \
    README.md \
    docs/superpowers/specs/2026-08-09-japanese-user-docs-design.md \
    docs/superpowers/plans/2026-08-09-japanese-user-docs.md \
    docs/operations-ja.md
  test -f install.sh
  test -f bin/pueue-agent
  test -f tests/e2e/run.sh
  test -f tests/e2e/rust_supervisor.sh
  test -f tests/e2e/fake_experiments/train_ok.sh
  test -f tests/support/fake_agent.sh
  test -f tests/support/fake_codex.sh
  test -f tests/test_shell_entrypoints.bats
  rg -n -- '--pueue-config' README.md docs/operations-ja.md
  test -f src/db/migrations.rs
  test -f tests/integration/database.rs
  git diff --quiet -- src/db/migrations.rs tests/integration/database.rs
  ```

  Expected: negative grep は無出力、保持対象 path はすべて存在、`--pueue-config` の例は README/operations に残り、SQLite files の working-tree diff はない。README の通常 Bash code fence と Pueue/YAML profile の説明を削除してはいけない。

- [ ] **Step 7: Commit the documentation/history review gate**

  ```bash
  git add README.md docs/operations-ja.md \
    docs/superpowers/specs/2026-08-09-japanese-user-docs-design.md \
    docs/superpowers/plans/2026-08-09-japanese-user-docs.md \
    docs/superpowers/specs/2026-08-11-periodic-deep-check-design.md \
    docs/superpowers/plans/2026-08-11-periodic-deep-check.md
  git diff --cached --name-status -- \
    docs/superpowers/specs/2026-08-04-pueue-agent-design.md \
    docs/superpowers/plans/2026-08-04-pueue-agent.md \
    docs/superpowers/specs/2026-08-09-rust-sqlite-agent-supervisor-design.md \
    docs/superpowers/plans/2026-08-09-rust-sqlite-agent-supervisor.md
  git commit -m "docs: remove Bash YAML migration documentation"
  ```

  Review gate: README/current historical docs は interval-only、focused negative grep は無出力、4つの旧ファイルは不存在、現役 Bash/Pueue/SQLite 経路は存在し、Task 2 の commit に無関係なファイルがないこと。

---

### Task 3: Spec/plan の final cleanup、全-tree negative grep、host-independent verification

**Files:**

- Delete: `docs/superpowers/specs/2026-08-12-remove-bash-yaml-migration-design.md`
- Delete: `docs/superpowers/plans/2026-08-12-remove-bash-yaml-migration.md`
- Test/Verify: `README.md`、`templates/`、`src/`、`tests/`、保持された `docs/`
- Preserve/Verify: `src/db/migrations.rs`、`tests/integration/database.rs`、現役 Bash/Pueue 経路

**Interfaces:**

- Consumes: Task 1/2 の3つの review gate を通った working tree、および実装者が読み終えた spec/plan。
- Produces: spec/plan を含まず、旧 key/移行語の対象範囲内一致がなく、全 host-independent test が通る最終 tree。
- Cleanup boundary: spec/plan の削除は通常の Task 3 cleanup commit とし、git history の rewrite は行わない。

- [ ] **Step 1: Read and acknowledge the documents before cleanup deletion**

  `git rm` より前に、実装者は承認済み spec とこの plan を末尾まで読み、Task 1/2 の review gate、保持対象、最終検証、Task 3 の削除手順を確認する。

  ```bash
  sed -n '1,360p' docs/superpowers/specs/2026-08-12-remove-bash-yaml-migration-design.md
  sed -n '1,520p' docs/superpowers/plans/2026-08-12-remove-bash-yaml-migration.md
  ```

  Expected: 2つのファイルを読み終え、Task 3 の cleanup commit にこの2パスを含めることを確認する。読み終える前に削除してはならない。

- [ ] **Step 2: Prove the full-tree RED is limited to the cleanup documents**

  ```bash
  rg -n 'deep_check_every' README.md templates src tests docs
  ```

  Expected: 一致はこの spec、またはこの plan の説明だけに残る。README、template、Rust source、tests、Task 2 で保持した docs に一致があれば cleanup を進めず、Task 1/2 の該当 review gate に戻る。

- [ ] **Step 3: Delete the spec and implementation plan as the final cleanup change**

  ```bash
  git rm \
    docs/superpowers/specs/2026-08-12-remove-bash-yaml-migration-design.md \
    docs/superpowers/plans/2026-08-12-remove-bash-yaml-migration.md
  ```

  Expected: `git status --short` が上記2ファイルの deletion と、Task 1/2 commit 済みの clean state だけを示す。この削除は通常の差分であり、既存 commit の rewrite、広い glob による削除、runtime state の削除を行わない。

- [ ] **Step 4: Run the final full-tree negative grep and file-retention checks**

  ```bash
  rg -n 'deep_check_every' README.md templates src tests docs
  rg -n 'Bash/YAML 版からの移行|旧 Bash supervisor|pueue-agent sentinel|global text registry|PID lock|cron/sentinel|config\.yml' \
    README.md \
    docs/superpowers/specs/2026-08-09-japanese-user-docs-design.md \
    docs/superpowers/plans/2026-08-09-japanese-user-docs.md \
    docs/operations-ja.md
  test ! -e docs/superpowers/specs/2026-08-04-pueue-agent-design.md
  test ! -e docs/superpowers/plans/2026-08-04-pueue-agent.md
  test ! -e docs/superpowers/specs/2026-08-09-rust-sqlite-agent-supervisor-design.md
  test ! -e docs/superpowers/plans/2026-08-09-rust-sqlite-agent-supervisor.md
  test ! -e docs/superpowers/specs/2026-08-12-remove-bash-yaml-migration-design.md
  test ! -e docs/superpowers/plans/2026-08-12-remove-bash-yaml-migration.md
  test -f install.sh
  test -f bin/pueue-agent
  test -f tests/e2e/run.sh
  test -f tests/e2e/rust_supervisor.sh
  test -f tests/e2e/fake_experiments/train_ok.sh
  test -f tests/support/fake_agent.sh
  test -f tests/support/fake_codex.sh
  test -f tests/test_shell_entrypoints.bats
  rg -n -- '--pueue-config' README.md docs/operations-ja.md
  git diff --quiet -- src/db/migrations.rs tests/integration/database.rs
  ```

  Expected: 両方の negative grep は無出力、6つの旧/spec/plan path は不存在、現役 Bash path は存在、Pueue の `--pueue-config` 例は出力され、SQLite files の diff はない。`rg` の no-match exit status 1 は negative check の成功として扱う。

- [ ] **Step 5: Run cargo formatting check without hiding tool availability**

  ```bash
  cargo fmt --check
  ```

  Expected: rustfmt が利用可能なら PASS。`cargo fmt` または rustfmt が利用できない場合は、コマンドの実際の非ゼロ結果を `cargo fmt --check: unavailable` と記録し、PASS と報告しない。その場合も次の whitespace check を独立に実行する。

  ```bash
  git diff --check
  ```

  Expected: PASS。format tool unavailable と whitespace check PASS は別の結果として handoff に残す。

- [ ] **Step 6: Run the full host-independent Rust verification**

  ```bash
  cargo test --all-targets
  ```

  Expected: PASS。全 integration target、config unknown-key rejection、interval zero/positive behavior、SQLite migration regression test を含む。失敗した場合は Task 3 commit を作らず、失敗した target と出力を review gate の blocker として扱う。

- [ ] **Step 7: Verify the pre-commit net diff against the fixed base**

  Task 3 の cleanup deletion が index に staged された状態で、固定 base `1e3d7f4` と現在の index/working tree の net diff を確認する。commit 数や HEAD の相対位置を前提にしない。

  ```bash
  git diff --name-only 1e3d7f4
  git status --short
  ```

  Expected: `git diff --name-only 1e3d7f4` は Task 1/2 の実装変更だけを次のファイルとして列挙する。base に存在せず final tree にも存在しない cleanup spec/plan の2パスは一覧に含めない。

  ```text
  README.md
  docs/operations-ja.md
  docs/superpowers/plans/2026-08-04-pueue-agent.md
  docs/superpowers/plans/2026-08-09-japanese-user-docs.md
  docs/superpowers/plans/2026-08-09-rust-sqlite-agent-supervisor.md
  docs/superpowers/plans/2026-08-11-periodic-deep-check.md
  docs/superpowers/specs/2026-08-04-pueue-agent-design.md
  docs/superpowers/specs/2026-08-09-japanese-user-docs-design.md
  docs/superpowers/specs/2026-08-09-rust-sqlite-agent-supervisor-design.md
  docs/superpowers/specs/2026-08-11-periodic-deep-check-design.md
  src/config.rs
  templates/config.toml
  tests/e2e/rust_supervisor.sh
  tests/integration/config.rs
  tests/integration/daemon.rs
  tests/integration/detection.rs
  tests/integration/operator_commands.rs
  tests/integration/periodic.rs
  tests/integration/pueue_adapter.rs
  tests/integration/scheduler.rs
  tests/integration/service.rs
  ```

  `git status --short` は Task 3 commit 前なら上記2 deletion の staged state だけを追加で示す。`src/db/migrations.rs` と `tests/integration/database.rs`、install/launcher/E2E/support の無関係な変更が一覧にあれば commit を止める。

- [ ] **Step 8: Commit the final cleanup gate, including spec and plan deletion**

  ```bash
  git diff --cached --name-status -- \
    docs/superpowers/specs/2026-08-12-remove-bash-yaml-migration-design.md \
    docs/superpowers/plans/2026-08-12-remove-bash-yaml-migration.md
  git commit -m "chore: remove migration cleanup records"
  ```

  Review gate: Task 3 commit はこの spec とこの plan の削除を含み、full-tree negative grep、retention check、`git diff --check`、`cargo test --all-targets` を確認済みであること。cargo fmt が unavailable だった場合だけ、その事実を成功結果と混同せず handoff に明記する。

- [ ] **Step 9: Verify the committed final net diff and clean status**

  Task 3 cleanup commit の直後に、同じ固定 base から HEAD までの committed diff と working tree を確認する。

  ```bash
  git diff --name-only 1e3d7f4..HEAD
  git status --short
  ```

  Expected: range diff は Step 7 の21個の Task 1/2 path と完全に一致し、cleanup spec/plan の2パスは含まれない。`git status --short` は無出力であること。これにより、cleanup deletion を含む commit 後の final tree が plan の対象範囲だけであることを確認する。

---

## Plan self-review before the plan commit

- [ ] **Spec coverage:** spec の修正・削除マトリクス全行が File Map と Task 1/2/3 のいずれかに対応し、保持対象（現役 Bash、Pueue YAML/profile、`--pueue-config`、SQLite migration/database test）が Global Constraints、File Map、verification に明記されていることを確認する。
- [ ] **Placeholder review:** 全 step が対象 file、具体的な変更内容、実行 command、期待結果、または commit message を持ち、実装者が追加判断なしで実行できる粒度であることを確認する。
- [ ] **Type consistency:** `config::load(path: &Path) -> Result<ProjectConfig, AppError>`、Task 1後の `CheckConfig` field 一覧、`RawCheckConfig::validate` の return type、Task 1 test の `u32` assertions が interfaces と各 task で一致することを確認する。
- [ ] **Literal cleanup:** 実装後の test source/assertion に旧 key の文字列を残さず、Task 2 focused grep と Task 3 full-tree grep が削除後に no output になることを確認する。
