# 導入ガイド

このガイドでは、リポジトリを取得してプロジェクトを初期化し、最初の実験を投入するまでを説明します。

## 対応環境

- Linux: Ubuntu GitHub Actions で debug check、release check、全ターゲットの serial test、shell syntax を検証済み。private temp の mount 境界確認には kernel 5.8 以降を要求する。
- macOS: launchd 経路は存在するが、private temp を `/dev/fd/11` の子パスとして利用できない既知制約があるため、Linux と同等の agent 実行対応を主張しない。
- その他: fail closed とし、対応済みとは記載しない。

## 必要条件

- Rust toolchain（`cargo`）
- 実行時に利用する Pueue（`pueue`）と、選択した profile で起動済みの `pueued`
- Linux の systemd user service または macOS の launchd user service を利用できるユーザー環境
- `agent.program` に設定する agent executable。既定テンプレートでは `codex` が利用でき、`CODEX_HOME`（未指定時は `$HOME/.codex`）に必要な Codex 環境が用意されていること
- 実験を実行するプロジェクトディレクトリ

## インストール

リポジトリを取得し、インストールスクリプトを実行します。スクリプトは release binary を build し、`PA_INSTALL_PREFIX`（既定は `$HOME/.local/bin`）に `pueue-agent` のシンボリックリンクを作成します。この prefix が `PATH` にない場合は、以後の手順の前に `PATH` へ追加するか、表示された絶対パスで実行してください。

```bash
git clone <repository-url>
cd pueueAgent
./install.sh
```

## Pueue profile を確認する

通常のコマンドは、次の優先順位で Pueue profile を選び、すべて同じ profile を使います。

1. コマンドラインの `--pueue-config <path>`
2. 環境変数 `PUEUE_CONFIG`
3. インストール済み user service 定義にある `--pueue-config <path>`
4. 既定値 `~/.config/pueue/pueue.yml`

指定するパスは絶対パスで、`.` や `..` を含めないでください。インストール済み service の profile と異なる profile を明示すると、設定の混在を防ぐためコマンドは失敗します。

## プロジェクトを初期化する

実験プロジェクトのルートへ移動して `init` を実行し、設定を確認します。`STATE.md` は人が記入する実験方針・制約の入口です。

```bash
cd /absolute/path/to/project
pueue-agent init
$EDITOR .pueue-agent/config.toml
$EDITOR .pueue-agent/STATE.md
```

## 生成ファイルを確認する

`init` はプロジェクト直下に `.pueue-agent/` を作成し、次のファイルとディレクトリを生成します。

- `.pueue-agent/config.toml`: project ID、Pueue group、agent、check、guardrails の設定
- `.pueue-agent/STATE.md`: 人が管理する実験方針、履歴、現在の状況、次の計画
- `.pueue-agent/state.json`: supervisor の機械的な状態（canonical state）
- `.pueue-agent/instructions.md`: agent に渡すプロジェクト指示のテンプレート
- `.pueue-agent/logs/`: プロジェクトログのディレクトリ

既存の `config.toml` があるプロジェクトでは、初期化は上書きせず失敗します。

現在の全設定は [`templates/config.toml`](../templates/config.toml) を参照してください。未知のキーや不正な値は無視されず、設定エラーになります。

## Agent context を選ぶ

`agent.context.mode` の既定値は `fresh` です。通常は run ごとに新しい context を使います。

```toml
[agent.context]
mode = "fresh"
```

特定の Codex session を明示的に継続する場合だけ `resume` と `session_id` を指定します。project に属する最新 session を選ぶ場合は `resume_latest` を opt in します。

```toml
[agent.context]
mode = "resume"
session_id = "<SESSION_ID>"
```

```toml
[agent.context]
mode = "resume_latest"
```

継続モードは `agent.program = "codex"` の場合だけ利用できます。session が存在しない、壊れている、または別 project に属する場合は agent run を bind する前に拒否され、event が `dead_letter`、`last_error` が `policy_blocked:session_missing` または `policy_blocked:session_not_owned` になります。`fresh` へ暗黙に fallback しません。

## Detector を設定する

detector は `check.log_tail_bytes` で制限した task log 末尾と、`check.extra_log_paths` の project 相対 log を確認します。pattern ごとに regex、action、必要な一致回数を設定します。

```toml
[[check.patterns]]
name = "training-failure"
regex = "FATAL"
action = "wake"
confirm_matches = 2

[check.stall]
action = "notify"
kill_after_minutes = 0
```

`notify` は incident の記録、`wake` は agent event の記録、`kill` は検証済み Pueue task への termination request です。自動終了は opt in です。pattern の `kill` には名前が必要で、stall の `kill` には正の `kill_after_minutes` が必要です。OS process へ直接 signal を送る設定ではありません。

## プロジェクトを有効化する

設定を保存したら user service をインストールして起動します。

```bash
pueue-agent enable
```

Linux では systemd user service、macOS では launchd 経路を使います。対応環境の制約は「対応環境」を確認してください。

## Service の state directory を確認する

SQLite database は次の優先順位で解決した directory の `state.sqlite3` です。

1. 空でない絶対パスの `PUEUE_AGENT_STATE_DIR`
2. 空でない絶対パスの `XDG_STATE_HOME` 配下の `pueue-agent/`
3. Linux などでは `$HOME/.local/state/pueue-agent/`、macOS では `$HOME/Library/Application Support/pueue-agent/`

相対パスや空の override は採用されません。`enable` は解決済みの state directory を user service 定義へ固定するため、対話 CLI と service で別の state database を参照しないよう、同じ環境と profile で `pueue-agent status` と `pueue-agent doctor` を確認してください。

## 最初の実験を投入する

監視対象の job は、raw `pueue add` ではなく必ず `pueue-agent submit` で投入してください。これにより project の設定、state、guardrails、agent supervisor の管理対象として登録されます。

```bash
pueue-agent submit -- python train.py --lr 0.001
```

`--` より後ろが実行するコマンドです。ここでは例として `train.py` を実行します。

## 状態を確認する

投入後は service、automation、project、Pueue、agent run の状態をまとめて確認できます。

```bash
pueue-agent status
```

表示された各項目を個別に確認し、automation の表示だけから Pueue task の状態を推測しないでください。

## 次に読むガイド

- [運用: 停止、再開、更新](operations-ja.md): pause、stop、cancel、disable、upgrade の手順
- 設定の詳細: `.pueue-agent/config.toml` と `templates/config.toml`
