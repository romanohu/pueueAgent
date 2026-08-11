# pueue-agent

`pueue-agent` は、[Pueue](https://github.com/Nukesor/pueue) で管理する長時間の実験を監視する Rust + SQLite 製の supervisor です。通常の監視ではトークンを消費せず、永続化されたイベントが発生したときだけ coding agent を起動します。設定した致命的な条件が確認された場合は、Pueue にタスクの終了を依頼することもできます。

1つの supervisor は、1つの Pueue daemon または profile を担当します。プロジェクトごとに生成された `project_id` と専用の Pueue group を使うため、同じディレクトリ名を持つリポジトリでもイベントやタスクの所有権が混ざりません。

## 仕組み

```text
人または agent
  └─ pueue-agent submit -- <command...>
       └─ pueue add -g <project-group> --escape -- <command...>

pueued
  ├─ callback ───────────────┐
  └─ status reconciliation ──┼─> SQLite events/incidents/submissions
                             │
Rust supervisor              │
  ├─ ログ末尾の範囲限定検知 ──┤
  ├─ 任意の pueue kill ──────┤
  └─ lease 付きイベント scheduler ┴─> プロジェクトごとに agent は最大1つ
```

SQLite が、プロジェクト登録、callback、再照合したタスク、incident、終了要求、イベント lease、agent run の source of truth です。callback を取りこぼしても Pueue の status から復旧でき、再起動後には期限切れのイベント lease が再び pending になります。

## 必要条件とインストール

- Rust stable と Cargo
- `pueue` と `pueued`
- Linux では systemd user service、macOS では launchd

```bash
git clone <repo>
cd pueueAgent
./install.sh
```

インストーラーは lock された依存関係で `target/release/pueue-agent` をビルドし、`~/.local/bin/pueue-agent` からそのバイナリへの symlink を作ります。別の bin ディレクトリを使う場合は `PA_INSTALL_PREFIX` を設定してください。

リポジトリを開発する場合は、最初にビルドして `bin/` の launcher を使います。

```bash
cargo build
bin/pueue-agent --help
```

開発用 launcher は `target/debug/pueue-agent` が存在しない場合、必要な build command を表示して終了します。

## クイックスタート

```bash
cd your-ml-repo
pueue-agent init
$EDITOR .pueue-agent/STATE.md
$EDITOR .pueue-agent/config.toml
pueue-agent enable
pueue-agent submit -- python train.py --lr 0.001
```

`submit` は `pueue add` を実行する前に SQLite へ submission intent を記録します。引数の境界も保持するため、人と agent の両方で使う正式な投入経路です。監視対象の実験では raw の `pueue add` を使わないでください。submission の記録を迂回してしまいます。

submission には `experiment` と `control` の2種類があります。通常の学習・評価ジョブは `experiment`（既定値）として投入し、bootstrap、診断、後片付けなど実験数に含めたくないジョブは明示的に `control` にします。

```bash
pueue-agent submit --kind experiment -- python train.py --lr 0.001
pueue-agent submit --kind control -- /usr/bin/true
```

`control` は SQLite の履歴と Pueue task として記録されますが、`guardrails.max_experiments` を消費しません。`max_agent_runs` や Pueue group の制約まで無効にする機能ではないため、control を無制限の実験枠として使うことはできません。

主な operator command は次のとおりです。

```bash
pueue-agent status
pueue-agent status --compact
pueue-agent status --json
pueue-agent pause
pueue-agent resume
pueue-agent start
pueue-agent stop
pueue-agent cancel --task-id <ID>
pueue-agent disable
pueue-agent disable --remove   # 登録と group の予約を明示的に解放する
pueue-agent wake --reason "variant-2 の分析を始める"
pueue-agent runs --follow
```

`pause` は pending event を保持したまま、新しい agent の起動と自動終了を停止します。`resume` で保持していた event を再び処理対象にできます。通常の `disable` は Pueue group の予約を維持します。`--remove` は明示的な登録解除であり、Pueue の status を取得できない場合は実行しません。

`stop` は supervisor service を止め、`pause` は automation だけを止め、`cancel --task-id` は確認済みの Pueue task 1件を止めます。`disable` と `disable --remove` は project の automation / 登録を変更します。`stop` と `disable` は Pueue task を kill しません。Pueue task は kill しないが、active agent は drain 対象で、shutdown timeout 後に process tree を終了して timed_out と記録され得る。操作対象と再開方法を含む日本語の手順は [運用ガイド](docs/operations-ja.md) を参照してください。

`wake` は Pueue にダミー task を投入せず、operator wake event を SQLite に記録して supervisor の次の scheduler loop の処理対象にします。`runs --follow` は新しい agent run と submission の lineage を監視し、Ctrl-C まで追加分を表示します。`--follow` は端末での追跡用で、`--json` を併用すると新しいデータを検出した polling 単位で、複数 run を含み得る bounded JSON report を出力します。

### 人間向け出力と JSON 出力

既定の人間向け出力は、`pueue-agent status` のような見出し、`key=value` の状態行、最後の `summary:` で構成されます。`service:` は supervisor service、`automation:` は project automation、`project:` は enabled/paused/halted、`pueue:` は task snapshot、`agent_runs:` は agent run を別々に示します。`status --compact` はこれらと experiment、event、guardrail を短く確認するための表示です。`status --json` は同じプロジェクト範囲の機械可読な report で、パイプや自動処理に使えます。JSON 出力には ANSI escape を入れず、上限を超える本文や secret らしい値を展開しません。

`pueue-agent` の human/JSON output は supervisor の投影です。`pueue status --json` の raw Pueue output とは形式も責務も異なり、前者は SQLite の event、incident、termination、agent run と Pueue の最新 snapshot を project scope でまとめます。`status --json` には submission の一覧を含めず、submission と task の lineage は `runs --json` で確認します。後者は Pueue daemon が持つ task の生データです。raw Pueue output が必要な低レベル調査では `pueue` を直接使えますが、pueue-agent の accounting や guardrail の確認には supervisor output と `events` / `runs` を使用してください。

### batch submission

複数の job を一つの request として投入する場合は、JSON manifest を作り、UUID の `request-id` を指定します。

```json
{
  "jobs": [
    {
      "id": "variant-1",
      "argv": ["python", "train.py", "--lr", "0.001"],
      "kind": "experiment",
      "metadata": {"variant": 1, "seed": 11}
    },
    {
      "id": "prepare-variant-1",
      "argv": ["python", "prepare.py"],
      "kind": "control"
    }
  ]
}
```

```bash
pueue-agent submit-batch \
  --request-id 00000000-0000-4000-8000-000000000001 \
  --manifest jobs.json \
  --json
```

同じ `request-id` を再度使うと、SQLite に保存された batch の状態を再利用します。この request-id 冪等性により、すでに accepted の job は二重投入せず、部分失敗で未確定の job だけを再開できます。ネットワーク障害や supervisor 再起動後の再送に使えます。request ID と manifest の組み合わせは一つの durable request として扱い、別の内容を同じ request ID に載せてはいけません。human output は job ごとの状態を行で表示し、`--json` は request、counts、accepted task ID、失敗 job を含む機械可読な結果を返します。

## 診断

```bash
pueue-agent events --json --limit 100
pueue-agent inspect <pueue-task-id> --json
pueue-agent explain <incident-id> --json
pueue-agent doctor --json
```

`events` は現在のプロジェクトの event を kind、status、件数で絞り込みます。`inspect` は task ID の最新 observation と、同じ stable signature に結び付く履歴を表示します。`explain` は observation、incident、event、policy、approval、Pueue action の因果順で表示し、Phase A で未設定の policy と approval は `not_configured` として示します。`doctor` は読み取り専用で、error check がある場合だけ非ゼロ終了します。すべての診断出力は上限付きで、prompt、transcript、payload 本文は出力しません。

## 人による介入を次回の agent run に渡す

```bash
pueue-agent steer -- "次は learning rate を半分にして"
pueue-agent steer list
pueue-agent status --json
```

`steer` は現在のプロジェクトに対するメッセージを SQLite へ登録するだけで、agent の起動、Pueue 操作、実行中 process への入力は行いません。各メッセージは最大 `4,096 bytes` です。1回の run には最大 `16 messages`、合計 `16,384 intervention bytes` までを、残りの prompt budget に収まる範囲で配信します。登録したメッセージは FIFO 順で一度だけ、次回の agent run の prompt に渡されますが、1回ですべての pending メッセージを配信するとは限りません。上限または残りの prompt budget を超える FIFO の後続メッセージ（超過分）は pending のまま、後続の run へ繰り越されます。agent の spawn に失敗した場合、メッセージは pending に戻されるため、次回の run で再試行されます。

`pause` または `disable` 中でもメッセージはキューへ登録できますが、配信はせず、resume または enable 後の次回 run まで保持されます。`status --json` はキューの件数などの診断情報を返しますが、メッセージ本文は含めません。実行中の agent は中断しません。介入メッセージによって、安全ポリシーや既存の制約を上書きすることはできません。

Unix では、agent process は prompt を受け取る前に起動 gate で待機します。run の PID 記録と intervention 適用 transaction が commit された後だけ親 process が release byte を送り、gate が EOF や異なる入力を受けた場合は設定済み agent を実行せず終了します。

## プロジェクトファイルと共有実験コンテキスト

`pueue-agent init` は次のファイルを作成します。

```text
.pueue-agent/
  config.toml
  state.json
  STATE.md
  instructions.md
  logs/
```

`.pueue-agent/state.json` は supervisor と agent が読む canonical machine state です。`current_facts`、`historical_facts`、`next_action`、`budgets`、`active_lineage` を構造化して保存し、特に budget と lineage の判断ではこのファイルを正とします。`pueue-agent init` は新規 project に template を作りますが、既存の `state.json` は上書きしません。壊れた、読めない、またはファイルではない canonical state は安全側に倒して dispatch を止めます。

`STATE.md` は agent run をまたいで共有する人間向けの実験ノートです。目的、制約、実験履歴、発見、成果物のパス、次の計画を記録してください。文章の履歴は補足情報であり、canonical state の budget や active lineage を上書きしません。

`instructions.md` は agent の作業手順を定義します。supervisor は、トリガーになった event の範囲を制限した要約と、これら3つのファイルへの参照を prompt に追加します。会話 transcript 全体を SQLite にコピーすることはありません。人間から次の run へ自然言語を渡す場合は `pueue-agent steer -- "指示"` を使い、実行中 process の stdin や prompt を直接書き換えないでください。

## 設定

設定ファイルは `.pueue-agent/config.toml` です。生成されるテンプレートは [`templates/config.toml`](templates/config.toml) にあります。

| キー | 役割 |
| --- | --- |
| `project_id` | 安定したプロジェクト識別子。 |
| `pueue_group` | プロジェクト名と ID suffix から作られる専用 Pueue group。 |
| `agent.program` / `agent.args` | 実行ファイルと引数ベクトル。`{prompt}` は各引数の中で置換される。 |
| `agent.timeout_minutes` | agent process の timeout。子 process を含む process group を終了する。 |
| `agent.max_retries` | 起動に失敗した場合の retry 上限。 |
| `agent.context.mode` | `fresh`、`resume`、`resume_latest` のいずれか。既定値は `fresh`。 |
| `check.interval_minutes` | supervisor が reconciliation を行う間隔。 |
| `check.deep_check_every` | legacy の互換設定。`deep_check_interval_minutes` が `0` のままでは agent DeepCheck を有効にしない。 |
| `check.deep_check_interval_minutes` | agent DeepCheck の間隔（分）。既定値の `0` は無効で、正の値を明示した場合だけ opt in する。 |
| `check.log_tail_bytes` | 各ログから読み取る末尾の最大 byte 数。 |
| `check.extra_log_paths` | 追加で検査する、プロジェクトからの相対パスのログ。 |
| `check.patterns` | 名前付き regex、確認回数、`notify` / `wake` / `kill` action。 |
| `check.stall` | 出力が停滞した場合の action。既定値は `notify`。 |
| `guardrails.*` | 連続失敗数、実験数、agent run 数の上限。 |

未知のキーや不正な範囲の値は無視せず、エラーとして拒否します。

`guardrails.max_experiments` は `kind = experiment` の submission だけを数えます。control submission は制御・準備用の履歴として残りますが、この実験 budget からは除外されます。機械的な判断を `STATE.md` の自由文へ移さず、canonical `.pueue-agent/state.json` の `budgets` と現在の設定を確認してください。

### Periodic DeepCheck

`check.interval_minutes` の reconciliation は、Pueue の status を再照合し、callback の取りこぼしを復旧して、範囲を制限した detector を実行する supervisor の機械的な処理です。正常な reconciliation は agent を起動しないため、agent のトークンを消費しません。

agent DeepCheck は別の opt-in 機能です。`check.deep_check_interval_minutes` に正の分数を設定したときだけ、長時間実行中の実験について agent を起動し、metric と artifact から進行の健全性を確認します。この run は agent のトークンを消費します。`deep_check_every` は legacy の互換設定であり、`deep_check_interval_minutes = 0` のまま agent run を有効にすることはありません。

periodic DeepCheck は task ごとではなく project ごとに coalesce します。同じ project に pending、claimed、または retry 待ちの periodic DeepCheck がある間は、長時間 task が複数あっても新しい periodic DeepCheck を追加しません。正常な進行を `STATE.md` に短い health record として残し、確認できない metric、値、進捗を記録しません。`STATE.md` は補足ノートなので、canonical `state.json` の budget や lineage を上書きしません。

### Codex の会話コンテキストを明示的に継続する

既定では fresh context を使います。

```toml
[agent]
program = "codex"
args = ["exec", "{prompt}"]

[agent.context]
mode = "fresh"
```

既存の特定の Codex session を続ける場合は、明示的に指定します。

```toml
[agent.context]
mode = "resume"
session_id = "019..."
```

これは `codex exec -C <project-root> resume <session-id> <prompt>` に変換されます。`session_id` は指定したプロジェクトに属する、確認可能な既存 session でなければなりません。

プロジェクトに属する最新の session を使う場合は、次のように opt in します。

```toml
[agent.context]
mode = "resume_latest"
```

これは `codex exec -C <project-root> resume --last <prompt>` に変換されます。継続モードは `agent.program = "codex"` の場合だけ利用できます。指定した session が存在しない、壊れている、または別プロジェクトのものだった場合は agent-run failure として記録され、fresh session へ暗黙に fallback することはありません。

## 異常検知と Pueue タスクの自動終了

pattern は、範囲を制限したログ末尾に対して確認されます。`notify` は incident を記録し、`wake` は agent が介入すべき event として記録し、`kill` は idempotent な termination request を作成します。

```toml
[[check.patterns]]
name = "fatal-loss"
regex = "NaN loss persisted|FATAL_LOSS"
action = "kill"
confirm_matches = 2
```

自動終了は opt in です。`pueue kill <task-id>` を呼び出す前に、supervisor は新しい Pueue status を取得し、プロジェクトの group と task の完全な signature を再検証します。実験 task に対して OS signal を直接送ることはありません。繰り返し同じ内容が観測されても、active incident と termination request はそれぞれ1件に保たれます。kill の失敗や timeout は状態として残り、agent が重複して自動起動することもありません。

出力の停滞は `check.stall` で検査できます。既定では `notify` だけを行います。停滞を理由に終了させる場合は、設定で明示的な action と確認待ち時間を指定してください。

既定では、task に対応するログを `.pueue-agent/logs/<task-id>.log` または `.pueue-agent/logs/task_<task-id>.log` から読み取ります。安定したプロジェクト相対パスの training log を監視する場合は `check.extra_log_paths` を使います。

## サービスと状態の場所

`enable` はプロジェクトを登録し、Pueue group を作成し、daemon 単位の Pueue callback を1つ設定し、user service をインストールして、service が正常であることを確認します。service file には binary path、Pueue config path、`PATH`、state directory、working directory が明示されます。対話式 shell の startup file には依存しません。

SQLite database は、`XDG_STATE_HOME` が絶対パスの場合は `XDG_STATE_HOME/pueue-agent/state.sqlite3` に置かれます。それ以外の場合は platform の state directory を使います。service の state directory を指定する場合は `PUEUE_AGENT_STATE_DIR` を設定してください。

## Bash/YAML 版からの移行

Rust supervisor は、旧 global text registry、PID lock、cron entry、`.pueue-agent/config.yml` を自動では取り込みません。

旧 Bash supervisor と cron/sentinel の test suite は、Rust E2E scenario が同等の動作を確認した後に削除されました。現在の `bin/pueue-agent` は Rust binary 用の開発 launcher であり、別の supervisor 実装ではありません。

1. 旧版を使っている各プロジェクトで disable を実行するか、`pueue-agent sentinel` の cron entry を削除します。
2. `./install.sh` で Rust release をインストールします。
3. 既存プロジェクトごとに `pueue-agent init` を実行します。既存の `STATE.md` と `instructions.md` は保持され、新しい `config.toml` が作成されます。
4. 旧 agent command を `agent.program` と `agent.args` に移し、detector action を確認します。`kill` は引き続き opt in です。
5. すべてのプロジェクトで `pueue-agent enable` を実行し、`pueue-agent status` を確認します。

SQLite-backed status の表示で各プロジェクトが正しく現れるまで、旧 YAML と registry のバックアップは保持してください。
