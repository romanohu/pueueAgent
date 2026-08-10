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

主な operator command は次のとおりです。

```bash
pueue-agent status
pueue-agent pause
pueue-agent resume
pueue-agent disable
pueue-agent disable --remove   # 登録と group の予約を明示的に解放する
```

`pause` は pending event を保持したまま、新しい agent の起動と自動終了を停止します。`resume` で保持していた event を再び処理対象にできます。通常の `disable` は Pueue group の予約を維持します。`--remove` は明示的な登録解除であり、Pueue の status を取得できない場合は実行しません。

## 人による介入を次回の agent run に渡す

```bash
pueue-agent steer -- "次は learning rate を半分にして"
pueue-agent steer list
pueue-agent status --json
```

`steer` は現在のプロジェクトに対するメッセージを SQLite へ登録するだけで、agent の起動、Pueue 操作、実行中 process への入力は行いません。登録したメッセージは FIFO 順で一度だけ、次回の agent run の prompt に渡されます。agent の spawn に失敗した場合、メッセージは pending のまま戻されるため、次回の run で再試行されます。

`pause` または `disable` 中でもメッセージはキューへ登録できますが、配信はせず、resume または enable 後の次回 run まで保持されます。`status --json` はキューの件数などの診断情報を返しますが、メッセージ本文は含めません。実行中の agent は中断しません。介入メッセージによって、安全ポリシーや既存の制約を上書きすることはできません。

## プロジェクトファイルと共有実験コンテキスト

`pueue-agent init` は次のファイルを作成します。

```text
.pueue-agent/
  config.toml
  STATE.md
  instructions.md
  logs/
```

`STATE.md` は agent run をまたいで共有する永続的な実験ノートです。目的、制約、実験履歴、発見、成果物のパス、次の計画を記録してください。

`instructions.md` は agent の作業手順を定義します。supervisor は、トリガーになった event の範囲を制限した要約と、これら2つのファイルへの参照を prompt に追加します。会話 transcript 全体を SQLite にコピーすることはありません。

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
| `check.log_tail_bytes` | 各ログから読み取る末尾の最大 byte 数。 |
| `check.extra_log_paths` | 追加で検査する、プロジェクトからの相対パスのログ。 |
| `check.patterns` | 名前付き regex、確認回数、`notify` / `wake` / `kill` action。 |
| `check.stall` | 出力が停滞した場合の action。既定値は `notify`。 |
| `guardrails.*` | 連続失敗数、実験数、agent run 数の上限。 |

未知のキーや不正な範囲の値は無視せず、エラーとして拒否します。

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
