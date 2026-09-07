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

Phase 3 の `running OOM/stall observer` と実行中 experiment の `periodic observer` による campaign health-decision loop は実装済みです。同じ class の信号が繰り返されるか log が stall すると experiment は `suspicious` になり、1 回の read-only diagnosis agent が bounded な証拠から原因と推奨 action（`continue` / `kill_and_resume` / `kill_and_escalate`）を返します。破壊的な action は確認済みの termination request を必要とし、`kill_and_resume` は live repair 予算（`max_live_repairs`）が残る場合に限り同一 argv の後継 experiment を再投入します。既存の pattern/stall detector と Periodic DeepCheck は別機能であり、legacy の kill pattern は running health を経由せず従来どおり incident と termination request を直接作ります。Phase 4 の evaluation と `goal review` も実装済みです。

Phase 5 の隔離された `code worktree` は、`code_change` proposal を専用 coordinator が受け取り、固定した Git base から候補を作り、editor、check、candidate commit、candidate experiment、evaluation、cleanup までを内部で進める zero-adapter pipeline です。通常の `submit` に新しい adapter や controller を追加する必要はなく、main、checkout 中の source branch、remote、無関係な worktree を merge、push、書き換えません。後続 phase に残るのは、trusted native editor を OS レベルで containment する Phase 6 です。

## code_change proposal の動作（Phase 5）

通常の campaign の decision agent が `code_change` proposal を返すと、SQLite の durable state を起点に次の順序で処理します。最初の admission で Git executable、project root、campaign 開始時の clean な committed `HEAD` または local `campaign/<campaign-id>/best` ref を検証します。dirty、非 Git、Git が利用できない、legacy campaign に `base_revision_sha` がない、または存在する best ref が不正な場合は code-change だけを fail closed で reject し、通常の非 code workflow はこの判定から推測して再投入しません。

1. service-owned state directory の `.pueue-agent/worktrees/<campaign-id>/<proposal-id>` に、完全な base SHA の detached candidate worktree を作ります。
2. 許可済みの native editor を candidate root に対して起動します。初回は fresh session、editor または必須 check の失敗時だけ同じ session を一度 resume し、合計 **2 回**を超えません。再起動しても attempt counter はリセットされません。
3. `git diff --check` と発見した project check を実行し、候補の変更ファイル **50 以下**、diff bytes **500000 以下**、check **8 以下**、各 check の timeout **30 分以下**、check 出力合計 **64 KiB 以下**を検証します。
4. 最終 diff の digest が一致したときだけ service identity で candidate commit を作り、candidate ref を durable にしてから、その commit SHA と candidate worktree を実験へ渡します。experiment の通常の Pueue task / evaluation / promotion 経路が成功し、objective metric の改善が確認できたときだけ best ref を local CAS で更新します。

`code_change` proposal の受理は code-change budget reservation を 1 つ消費します。reject や後続失敗でその slot は戻りません。editor の各 attempt は通常の agent-run hourly budget を消費し、candidate experiment は通常の rolling experiment budget と parallelism guardrail を使うため、`budget_waiting` になることがあります。候補を作る前の admission、editor、check、commit、experiment、promotion の各段階は一つの durable run として status/doctor から追跡できます。

project check は Rust の `Cargo.toml` があれば `cargo test --all-targets -- --test-threads=1`、Python の `pytest.ini` または `pyproject.toml` の `[tool.pytest.ini_options]` があれば pytest を発見します。`uv.lock` がある場合は `uv run pytest`、それ以外は `python -m pytest` を使います（対応する executable が policy にない場合は check を作れません）。editor が提案する check は発見済み check を削除できず、argv 配列でのみ追加できます。

候補 ref は `campaign/<campaign-id>/candidate/<proposal-id>`、best ref は `campaign/<campaign-id>/best` で、いずれも local ref です。自動 merge、rebase、push、PR 作成、remote ref の変更は行いません。candidate worktree は実験と cleanup が終わるまで保持され、tracked file の mutation、OOM、internal failure、timeout、cancel、無効な結果は候補を promotion 可能にしません。

custom agent/editor は shell command としてではなく、execution policy に登録・検証された trusted native executable として扱います。Phase 5 は argv、cwd、identity、credential 継承を検証しますが、OS namespace/container/VM/cgroup などの強制 containment は提供しません。editor、check、candidate experiment は root で起動せず、強い OS containment は Phase 6 の境界です。

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
- code-change の candidate は local ref と service-owned worktree に閉じ、main、source branch、remote、無関係な worktree を変更しません。候補の状態、abbreviated SHA、attempt、experiment/task ID、failed check、cleanup は `status --json` の `code_changes` で確認し、整合性は `doctor --json` の `code_change.*` checks で読み取り専用に診断します。
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
