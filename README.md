# pueue-agent

## 何をするツールか

`pueue-agent` は、Pueue で実行する長時間の実験を監視する Rust + SQLite 製の supervisor です。通常の監視は coding agent を起動せず、永続化された event が処理対象になったときだけ agent を起動します。投入、状態確認、人による介入、停止、診断、更新を一つの CLI から行えます。

## 全体像

```text
operator / coding agent
        |
        | pueue-agent submit
        v
SQLite submission intent -----> Pueue project group
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

SQLite は project、submission、event、incident、agent run の durable な関連を保持します。Pueue task の実行と agent の判断は分離され、1つの supervisor は1つの Pueue daemon または profile を担当します。

## 対応環境

| 環境 | 対応状況 |
| --- | --- |
| Linux | Ubuntu GitHub Actions で debug check、release check、全ターゲットの serial test、shell syntax を検証済み。private temp の mount 境界確認には kernel 5.8 以降が必要です。 |
| macOS | launchd 経路はありますが、private temp を `/dev/fd/11` の子パスとして利用できない既知制約があり、Linux と同等の agent 実行対応は主張しません。 |
| その他 | 安全側に停止します。対応済み環境ではありません。 |

必要条件と Pueue profile の選択規則は[導入ガイド](docs/getting-started-ja.md)を参照してください。

## クイックスタート

次の7コマンドでインストール、project 初期化、登録、最初の投入まで進めます。`init` 後、`enable` の前に生成ファイルを確認する手順は導入ガイドにあります。

```bash
git clone <repository-url>
cd pueueAgent
./install.sh
cd /path/to/experiment-project
pueue-agent init
pueue-agent enable
pueue-agent submit -- python train.py --lr 0.001
```

現在の完全な設定テンプレートは [`templates/config.toml`](templates/config.toml) です。

## よく使うコマンド

| 目的 | コマンド |
| --- | --- |
| 短い状態確認 | `pueue-agent status --compact` |
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
- execution policy、実行ファイル、project root、Pueue config、agent log、private temp の検証に失敗した場合は安全側に起動を拒否します。検証を弱めて通さないでください。
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
