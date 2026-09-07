# Continuous ML Experiment Campaign 実装作業まとめ

> これは2026-08-17時点の作業記録です。未実装範囲、network の既定値、commit/test 状態などは当時の記述であり、現行版の仕様ではありません。現在の機能と制約は [README](../../README.md)、実際のセットアップは [導入ガイド](../getting-started-ja.md) を参照してください。

更新日: 2026-08-17<br>
対象リポジトリ: `romanohu/pueueAgent`<br>
作業ブランチ: `agent/continuous-campaign-core`<br>
基準コミット: `c0fea271d80ff5252a8ea91fa76d182372c1965d`

## 1. 結論

添付仕様の全機能を一度に実装するのではなく、既存の安全な agent 実行基盤を維持したまま、次の最小縦断スライスを実装した。

- Phase 0 相当: hard policy の所有権修正、Pueue 外部副作用の不明状態の隔離、原子的な予算予約
- Phase 1 相当: campaign / proposal / experiment の永続化、CLI、daemon coordinator、再調停、基本的な liveness
- 回帰対策: v1 state/policy 互換、unmanaged project の既存 submit 互換、batch の曖昧 add 対策

現在の変更はローカル作業ツリーにあり、まだ commit / push / PR 作成はしていない。この環境には `gh` と Rust toolchain がないため、PR 公開と実コンパイルは未実施である。

## 2. 実装した内容

### 2.1 Service-owned hard policy

- `execution-policy.toml` schema v2 を追加した。
- 新規 v2 policy は network を既定で無効にした。
- campaign hard limits を service-owned policy に移した。
- project 側の設定や legacy state は上限を狭めることだけができ、拡大できない。
- campaign 開始時の snapshot と現在の service policy の小さい方を常に採用し、運用中の policy tightening を反映する。
- v1 policy は互換読み込みを維持した。

対象となる主な上限:

- 並列 experiment 数
- 24 時間あたりの新規 experiment 数
- 1 時間あたりの agent run 数
- 1 cycle あたりの proposal 数
- 24 時間あたりの code-change proposal 数
- same-spec retry 数
- failure fingerprint あたりの repair 数
- GPU 秒上限用フィールド

注意: GPU 秒フィールドは将来の resource-accounting backend 用であり、今回の実装では実測・予約・強制を行わない。

### 2.2 `state.json` schema v2

- agent-writable な `budgets` を schema v2 から削除した。
- campaign / proposal / experiment の現在位置を表す bounded なフィールドを追加した。
- v1 state は読み込み時に v2 へ正規化する。
- v1 の budget 値は configured limit を安全側に狭める用途にだけ使う。
- campaign 系 ID の空白・制御文字を拒否する。
- `templates/state.json` と関連テストを更新した。

### 2.3 SQLite schema v16 と campaign domain

次の durable domain を追加した。

- `campaigns`
- `proposals`
- `experiments`
- `budget_reservations`
- `experiment_results`
- `failures`
- `promotions`

追加した主な invariant:

- project ごとの live campaign は最大 1 件
- `(campaign_id, source_event_id, proposal_slot)` の一意性
- canonical proposal digest / idempotency key の一意性
- campaign を跨ぐ proposal / experiment / failure / promotion lineage の禁止
- experiment tuple の一意性
- 必須 index、column、foreign key、unique constraint の migration 後検証

v15 から v16 への migration では、過去に外部 add が始まった可能性がある batch job を `pending` に戻さず `unreconciled` に隔離する。

### 2.4 Structured proposal protocol

- 128 KiB 上限、unknown field 拒否の proposal JSON schema を追加した。
- canonical JSON と SHA-256 digest を生成する。
- source event、origin agent run、proposal kind、precondition、execution tuple を検証する。
- repair / retry / replication / full evaluation / holdout evaluation の source 要件を検証する。
- proposal replay は digest と idempotency key で同一結果を返す。
- slot conflict や異なる内容の replay は fail-closed にする。
- proposal 数、code change、same-spec retry、repair fingerprint の上限を `BEGIN IMMEDIATE` transaction 内で検査する。
- proposal の保存は DB-only とし、agent 自身は Pueue add を行わない。

### 2.5 Atomic reservation と coordinator

- daemon に project-scoped `CampaignCoordinator` を接続した。
- proposal acceptance、rolling budget reservation、experiment intent 作成を原子的に行う。
- `reserved` を外部副作用開始前、`submitting` を開始済み marker として扱う。
- `submitting` 以降の timeout、出力上限、task ID 不明などは `unreconciled` とし、自動再 add しない。
- spawn 前に確実に失敗した場合だけ reservation を解放する。
- Pueue add 直前に campaign / project の active 状態を transaction 内で再確認する。
- working directory を project 内に canonicalize し、command identity と Pueue 引数の両方へ反映する。
- lineage attempt と parent experiment は caller 値を信用せず DB transaction 内で導出する。
- 永続的に不正な proposal は daemon tick 全体を停止させず rejected に遷移させる。

### 2.6 Reconciliation と terminal projection

- 一意に照合できた Pueue task だけを submission / batch / experiment に adopt する。
- 候補 0 件または複数件の場合は再投入せず `unreconciled` のまま保留する。
- terminal Pueue task を `succeeded` / `failed` / `cancelled` experiment に投影する。
- terminal 遷移時に budget reservation を consumed にする。
- capacity が空いた `budget_waiting` campaign を active に戻し、deduplicated wake event を作る。

### 2.7 Campaign liveness

- campaign start / resume の wake event を monotonic epoch で一意化した。
- agent が proposal を残さず終了した場合の `decision_missing` watchdog を追加した。
- runnable work、active agent、active experiment、未処理 event がない active campaign に `campaign_idle` wake を再投入する。
- rolling budget 待機には有限の `next_wake_at` を設定し、hot loop を避ける。
- operator transition は `BEGIN IMMEDIATE` と compare-and-set で競合を閉じる。

### 2.8 Managed submit boundary

- non-retired campaign が存在する project では、`pueue-agent submit` と `submit-batch` を origin environment に依存せず拒否する。
- これにより agent が環境変数を除去して coordinator を迂回する経路を閉じた。
- campaign のない unmanaged project では従来の operator submit を維持する。

### 2.9 CLI と documentation

追加した command family:

- `campaign init/start/status/pause/resume/halt/retire`
- `proposal validate/submit/list/inspect/reject`

更新した主な文書:

- `README.md`
- `docs/architecture-ja.md`
- `docs/commands-ja.md`
- `docs/workflows-ja.md`
- `templates/instructions.md`

agent instruction は、直接 submit ではなく、成功・失敗どちらの cycle でも exactly one structured proposal を保存する契約へ変更した。

## 3. 主な変更ファイル

| 領域 | 主なファイル |
|---|---|
| Campaign CLI | `src/cli.rs`, `src/main.rs`, `src/campaign_commands.rs` |
| Coordinator | `src/campaign_coordinator.rs`, `src/daemon.rs`, `src/scheduler.rs` |
| Proposal protocol | `src/proposal.rs`, `src/digest.rs` |
| Domain / repository | `src/models.rs`, `src/db/campaigns.rs`, `src/db/repositories.rs` |
| Migration | `src/db/migrations.rs` |
| Policy / state | `src/execution_policy.rs`, `src/state.rs`, `src/guardrails.rs` |
| Pueue boundary | `src/submit.rs`, `src/batches.rs`, `src/reconcile.rs` |
| Tests | `tests/integration/campaign.rs` と既存 integration tests |
| CI | `.github/workflows/ci.yml` |

## 4. 追加・更新した検証

campaign integration test では、少なくとも次を対象にした。

- fresh v16 schema と live campaign uniqueness
- tampered current schema の fail-closed 検出
- same-second resume の wake event 一意性
- proposal idempotent replay と slot conflict
- parallel reservation race
- rolling 24-hour budget race
- terminal projection と capacity wake
- ambiguous add の quarantine
- empty active campaign の idle watchdog
- managed project での direct single / batch submit 拒否

追加 CI は次を実行する。

```bash
cargo check --all-targets
cargo check --release --all-targets
cargo test --all-targets -- --test-threads=1
bash -n install.sh bin/pueue-agent
bats tests/test_shell_entrypoints.bats
```

## 5. この環境で実施済みの確認

- `git diff --check`
- `bash -n install.sh bin/pueue-agent`
- `templates/state.json` の JSON parse
- `Cargo.toml` の TOML parse
- `.github/workflows/ci.yml` の YAML parse
- SQLite fresh schema、required index、foreign key、cross-campaign DML の smoke test
- 関数署名、match、module export、SQL の静的レビュー

## 6. 未実施・未完了

### 環境上の未実施

- `cargo`, `rustc`, `rustfmt` がないため、実コンパイルと Rust test は未実施
- `gh` がないため、commit / push / draft PR 作成は未実施
- GitHub Actions は未起動

### 仕様上の後続フェーズ

今回の縦断スライスには、次の本実装を含めていない。

- result manifest の ingest、path / identity / digest / finite metric 検証
- immutable objective evaluator と promotion decision
- failure classifier、fingerprint、quarantine、fallback
- deterministic grid / random HPO state machine
- git worktree / revision isolation と code-change validation
- sandbox / resource class enforcement と GPU 秒 accounting
- replication / plateau / promotion の完全な policy
- 12 scenario の real-Pueue E2E
- failpoint matrix と 24-hour soak

`experiment_results`、`failures`、`promotions` の schema は先行して存在するが、上記の完全な lifecycle は後続実装である。

## 7. PR 化する前の必須手順

1. Rust toolchain がある環境で CI 相当 command をすべて実行する。
2. compile error と failing test を修正する。
3. 必要なら baseline 全体の rustfmt を別 commit / PR に分離する。
4. `gh auth status` を確認する。
5. 変更を意図的に stage / commit し、branch を push する。
6. この文書の「未実施・未完了」を PR 本文にも明記して draft PR を作成する。

## 8. 推奨する次の PR 分割

1. 今回の campaign core / safe submission vertical slice
2. Result ingest と objective evaluation
3. Failure classification、repair、quarantine
4. Deterministic HPO と promotion / plateau
5. Worktree isolation と revision verification
6. Sandbox / resource accounting / GPU enforcement
7. Production E2E、failpoint、soak、release hardening

この順番なら、外部副作用の安全性と hard-policy ownership を先に固定し、評価・最適化・resource enforcement を独立してレビューできる。
