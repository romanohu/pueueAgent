# pueue-agent

## 何をするツールか

`pueue-agent` は、Pueue で実行する長時間の実験を監視する Rust + SQLite 製の supervisor です。最初の通常 `submit` を managed campaign と baseline experiment として記録し、通常の監視は coding agent を起動せず、永続化された event が処理対象になったときだけ agent を起動します。投入、状態確認、人による介入、停止、診断、更新を一つの CLI から行えます。

## 全体像

```text
operator / coding agent
        |
        | pueue-agent submit
        v
SQLite campaign / proposal / experiment
        | durable submission intent
        +------------------------> Pueue project group
        ^                            |
        |                            | callback / status reconciliation
        +-------- event / incident <-+
                     |
                     v
               Rust supervisor
                     |
          execution policy + native gate
                     |
                     v
                 agent run
```

SQLite は project、campaign、proposal、experiment、budget reservation、submission、event、incident、agent run の durable な関連を保持します。Pueue task の実行と agent の判断は分離され、1つの supervisor は1つの Pueue daemon または profile を担当します。

現在の Phase 2 は、campaign、baseline、hard budget、外部投入の復旧境界に加え、実験の成功・失敗を起点にする `terminal completion loop` を持ちます。Linux では supervisor が SQLite の bounded evidence だけを read-only の built-in Codex decision agent に渡し、返された exactly one structured decision を `proposal` または `finite wait` として検証します。proposal は既存の campaign coordinator から次の非 code experiment を投入し、wait は Pueue task を追加せず有限の `next_wake_at` まで待ちます。

Phase 3 の `running OOM/stall observer` と実行中 experiment の `periodic observer` による campaign health-decision loop は実装済みです。同じ class の信号が繰り返されるか log が stall すると experiment は `suspicious` になり、1 回の read-only diagnosis agent が bounded な証拠から原因と推奨 action（`continue` / `kill_and_resume` / `kill_and_escalate`）を返します。破壊的な action は確認済みの termination request を必要とし、`kill_and_resume` は live repair 予算（`max_live_repairs`）が残る場合に限り同一 argv の後継 experiment を再投入します。既存の pattern/stall detector と Periodic DeepCheck は別機能であり、legacy の kill pattern は running health を経由せず従来どおり incident と termination request を直接作ります。`goal review` は後続 phase、隔離された `code worktree` は Phase 5 の範囲です。

## 対応環境

| 環境 | 対応状況 |
| --- | --- |
| Linux | 正式な対応対象です。Ubuntu の検証に加え、Phase 2 の完了判定では隔離した real `pueued` による `tests/e2e/run.sh` の成功が必要です。private temp の mount 境界確認には kernel 5.8 以降が必要です。 |
| macOS | launchd 経路はありますが、private temp を `/dev/fd/11` の子パスとして利用できない既知制約があり、Linux と同等の agent 実行対応は主張しません。 |
| その他 | 安全側に停止します。対応済み環境ではありません。 |

必要条件と Pueue profile の選択規則は[導入ガイド](docs/getting-started-ja.md)を参照してください。

## クイックスタート

インストール後、ML リポジトリのルートで次の4段階を実行します。`STATE.md` には具体的な目標、成功条件、変更してよい範囲を書きます。

```bash
pueue-agent init
# edit .pueue-agent/STATE.md
pueue-agent enable
pueue-agent submit -- python train.py
```

現在の完全な設定テンプレートは [`templates/config.toml`](templates/config.toml) です。

## よく使うコマンド

| 目的 | コマンド |
| --- | --- |
| 短い状態確認 | `pueue-agent status --compact` |
| campaign と decision の確認 | `pueue-agent status --json` |
| 総合診断 | `pueue-agent doctor` |
| 単発実験の投入 | `pueue-agent submit -- <command...>` |
| automation の停止・再開 | `pueue-agent pause` / `pueue-agent resume` |
| 次回 agent run への指示 | `pueue-agent steer -- "<MESSAGE>"` |
| 明示的な wake | `pueue-agent wake --reason "<REASON>"` |
| agent run の追跡 | `pueue-agent runs --follow` |

全コマンドの構文、状態変更の有無、失敗時の確認先はコマンドリファレンスにあります。

## ガイド

- [導入ガイド](docs/getting-started-ja.md): インストール、profile、初期化、登録、最初の投入
- [コマンドリファレンス](docs/commands-ja.md): 公開 CLI の構文、効果、オプション
- [運用ワークフロー](docs/workflows-ja.md): batch、監視、介入、停止、更新、再起動復旧
- [内部アーキテクチャ](docs/architecture-ja.md): 状態所有、scheduler、native gate、復旧、安全境界
- [トラブルシューティング](docs/troubleshooting-ja.md): 読み取り専用診断と症状別の安全な復旧

## セキュリティ上の重要事項

- 監視対象は raw `pueue add` ではなく `pueue-agent submit` から投入してください。submission intent と project ownership の記録を迂回しないためです。
- 最初の通常 `submit` は campaign と baseline を作ります。live campaign 中の二回目の `submit` / `submit-batch` は拒否されるため、追加指示は `steer` を使います。
- campaign の objective は最初の `submit` 時の `STATE.md` snapshot で固定されます。新しい目的へ移るときは既存 campaign を安全に `retire` してから `STATE.md` を編集し、新しい最初の `submit` を実行します。
- Pueue add の結果が不明な experiment は `unreconciled` のまま隔離され、自動で同じ task を追加しません。`campaign status` と `doctor` で確認してください。
- execution policy、実行ファイル、project root、Pueue config、agent log、private temp の検証に失敗した場合は安全側に起動を拒否します。検証を弱めて通さないでください。
- service policy の network 既定値は enabled ですが、network 利用許可と credential 継承許可は別です。allowlist にない credential/environment value は agent や agent task へ継承されません。
- `status`、`events`、`runs`、`doctor` などの診断投影は bounded / redacted です。ただし、SQLite には submission の argv と任意 metadata、`steer` の intervention message が保存されます。これらの入力に credential や secret を含めないでください。
- service、automation、agent run、Pueue task は別の lifecycle です。停止や取消は、対象に対応する `stop`、`pause`、`resume`、`cancel --task-id` を使ってください。
- 障害時も SQLite や immutable execution policy を直接修復せず、[トラブルシューティング](docs/troubleshooting-ja.md)の診断順序と supported CLI を使ってください。

## 開発と検証

開発用 binary は `cargo build` 後に `bin/pueue-agent` から実行できます。変更前後の基本検証は次のとおりです。

```bash
cargo fmt --check
cargo check --all-targets
cargo test --all-targets -- --test-threads=1
bash -n install.sh bin/pueue-agent
```

実装の入口と状態遷移は[内部アーキテクチャ](docs/architecture-ja.md)にまとめています。

## ライセンス

このリポジトリには現在、ライセンスファイルが同梱されていません。利用・再配布条件は、ライセンスが明記されるまでリポジトリ管理者に確認してください。
