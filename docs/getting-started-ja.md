# 導入ガイド

このガイドでは、リポジトリを取得してプロジェクトを初期化し、最初の実験を投入するまでを説明します。

## 対応環境

- Linux: 正式な対応対象。Phase 2 の完了判定では、通常の build/test に加えて隔離した real `pueued` を使う `tests/e2e/run.sh` の成功を要求する。private temp の mount 境界確認には kernel 5.8 以降を要求する。
- macOS: launchd 経路は存在するが、private temp を `/dev/fd/11` の子パスとして利用できない既知制約があるため、Linux と同等の agent 実行対応を主張しない。
- その他: fail closed とし、対応済みとは記載しない。

## 必要条件

- Rust toolchain（`cargo`）
- 実行時に利用する Pueue（`pueue`）と、選択した profile で起動済みの `pueued`
- Linux の systemd user service または macOS の launchd user service を利用できるユーザー環境
- `agent.program` に設定する agent executable。既定テンプレートでは `codex` が利用でき、`CODEX_HOME`（未指定時は `$HOME/.codex`）に必要な Codex 環境が用意されていること
- 実験を実行するプロジェクトディレクトリ
- `code_change` を使う場合は、clean な committed `HEAD` を持つ Git project と、execution policy から検証できる Git executable。Rust/Python の project check は、検出された構成に応じて `cargo`、`uv`、または `python` が必要です。
- `code_change` の editor/check/candidate experiment は root で実行できません。custom editor を指定する場合は、execution policy に登録した trusted native executable を用意してください。

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

インストール済みの環境で campaign を開始する最短手順は次の4段階です。

```bash
pueue-agent init
# edit .pueue-agent/STATE.md
pueue-agent enable
pueue-agent submit -- python train.py
```

## 生成ファイルを確認する

`init` はプロジェクト直下に `.pueue-agent/` を作成し、次のファイルとディレクトリを生成します。

- `.pueue-agent/config.toml`: project ID、Pueue group、agent、check、guardrails の設定
- `.pueue-agent/STATE.md`: 人が管理する campaign の目的、成功条件、変更可能範囲。STATE.md に credential や secret を書かないでください
- `.pueue-agent/state.json`: agent 用の bounded scratch projection。campaign、objective、budget、lineage の正本は SQLite
- `.pueue-agent/instructions.md`: agent に渡すプロジェクト指示のテンプレート
- `.pueue-agent/logs/`: プロジェクトログのディレクトリ

Git project では、`init` が tracked な `.gitignore` を変更せず、Git の common `info/exclude` に `/.pueue-agent/` を追加します。これにより service state が code-change の clean な基準に混ざりません。check と candidate runtime の生成物は service-owned な bounded scope に置かれ、再起動時に所有権を証明できない残存 scope は再利用せず保全して停止します。

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

監視対象の job は、raw `pueue add` ではなく必ず `pueue-agent submit` で投入してください。live campaign がない場合、最初の通常 `submit` は `STATE.md` の bounded snapshot、campaign、baseline proposal、baseline experiment、rolling budget reservation、submission intent を SQLite に作成してから Pueue へ追加します。

```bash
pueue-agent submit -- python train.py --lr 0.001
```

`--` より後ろが実行するコマンドです。ここでは例として `train.py` を実行します。

目的は最初の `submit` 時に immutable snapshot と digest として固定されます。その後に on-disk の `STATE.md` を編集しても、active campaign の objective snapshot は変更されません。新しい目的を開始するには、現在の experiment がすべて終端・照合済みであることを確認し、`pueue-agent campaign retire` 後に `STATE.md` を編集して、新しい最初の `submit` を実行します。

live campaign 中の追加 `submit` と `submit-batch` は、別 campaign や別 task の重複作成を防ぐため副作用前に拒否されます。現在の目的への追加指示は `pueue-agent steer -- "<MESSAGE>"` を使います。

## Phase 2 で自動化される範囲

上の4コマンド以外に controller script や project 固有 adapter は不要です。Phase 2 は baseline/control plane と安全な復旧に加え、terminal experiment から次の非 code experiment へ進む `terminal completion loop` を提供します。成功・失敗が一意に reconciliation されると decision cycle が作られ、Linux の read-only decision agent が bounded evidence から exactly one structured decision を返します。`proposal` は既存 coordinator と rolling experiment budget を通して投入され、`finite wait` は task を追加せず有限の `next_wake_at` を保存します。

decision analysis も agent-run hourly budget を消費します。1 cycle の連続失敗は service-owned `max_decision_attempts_per_cycle`（既定 3）、wait は `max_decision_wait_minutes`（既定 1,440 分）で制限されます。上限まで失敗すると cycle と campaign は `degraded` になり、自動 proposal は止まります。`status --json` の `campaign.decision` で `cycle_id`、`source_experiment_id`、`state`、`attempt_count`、`last_decision_kind`、`next_wake_at`、bounded な failure code/summary を確認し、raw evidence や decision body を期待しないでください。

実行中 experiment は periodic observer によって継続評価されます。同じ class の信号が繰り返されるか log が stall すると experiment は `suspicious` になり、1 回の read-only diagnosis agent が bounded な証拠から原因と推奨 action（`continue` / `kill_and_resume` / `kill_and_escalate`）を返します。破壊的な action は確認済みの termination request を必要とし、`kill_and_resume` は live repair 予算が残る場合に限り同一 argv の後継 experiment を 1 つだけ再投入します。diagnosis の試行回数と signal 要約は上限付きで、生の log 行が SQLite に保存されることはありません。現在の状態は `pueue-agent status` の `health:` 行と `status --json` の `health.recent` で確認できます。

Phase 3 の running health は、実行中 experiment を `running OOM/stall observer` 付きの `periodic observer` で継続評価します。これは既存の terminal completion loop と併用され、実行中の異常を `suspicious`、diagnosing、action pending として bounded に投影します。

Phase 4 の evaluation は、campaign が `--metric-name` / `--metric-direction`（任意に `--metric-min-delta`）で宣言した objective metric に対して experiment task の `PUEUE_AGENT_RESULT_PATH` に書き出された result manifest（`schema_version:1`, `experiment_id`, `metrics`）を terminal projection 時に発見・検証し、`experiment_metrics` に永続化します。検証では有限数値のみを受理し、欠損や不一致は `artifact_defect`（`result_missing` / `result_invalid`）として記録して experiment の成否には影響しません。`current_best_experiment_id` と `plateau_count` は promotion で更新され、`minimize` は `value < best - delta`、`maximize` は `value > best + delta` で改善とみなし、改善で best を更新して plateau をリセット、非改善の `succeeded` では plateau をインクリメントし、`plateau_threshold`（既定 3, 1..20）到達で `strategy-refresh` の operator wake を round ごとに一度だけ発火します。決定は `goal_reached`（`evidence_ref` 必須、metrics row を参照）を返すと campaign を `goal_reached_pending_review` に遷移させ、`pueue-agent campaign review accept|reject [--note]` で確定（accept は `retired:goal_accepted`、reject は `active` に戻し該当 `goal_reached` 決定を dead-letter 化）します。いずれも `pueue-agent status` の `best:` / `plateau:` 行と `status --json` の `campaign.best_*` / `plateau_count` / `evaluation.recent`（最大 50 件）で観測できます。Managed experiment の Pueue 追加は `/usr/bin/env` で4つの派生変数（`PUEUE_AGENT_EXPERIMENT_ID` 等）が `NAME=value` 形式で注入された後に user argv が続き、Pueue の生コマンド表示はラップを含みます（direct/control はラップされません）。ログ解析は promotion しません。

`goal review` は Phase 4 で `goal_reached` 決定を operator が承認/拒否するフローとして提供済みです。隔離された `code worktree` を使う Phase 5 の `code_change` pipeline も実装済みで、通常の decision agent が返した proposal を内部 coordinator が処理します。後続 phase に残るのは trusted native editor の OS レベル containment を扱う Phase 6 です。既存 detector/Periodic DeepCheck は別機能であり、legacy の kill pattern は running health を経由せず従来どおり incident と termination request を直接作ります。

## code_change を使う場合の前提と流れ

`code_change` は `pueue-agent submit --kind` の公開 submission kind ではありません。通常の campaign の decision agent が返す proposal kind であり、新しい project 固有 adapter や controller を追加せず、既存の submit、campaign、Pueue、evaluation 経路に接続されます。受理時は code-change budget を 1 slot 消費し、reject になっても戻りません。editor の各 attempt は通常の agent-run hourly budget、candidate experiment は通常の rolling experiment budget と parallelism guardrail を消費し、空きがないと `budget_waiting` になります。
既定の service policy は `max_code_change_proposals_per_24h=10`、`max_agent_runs_per_hour=6`、`max_new_experiments_per_24h=24`、`max_parallel_experiments=1` です。実効値は immutable execution policy の bounded budget として適用され、proposal、agent run、experiment の reservation を同じ意図で二重作成しません。

1. admission で project root、Git repository、campaign 開始時の clean な committed `HEAD` を確認します。既存の local best ref があればそれを、なければ `campaign.base_revision_sha` を完全な base SHA として使います。dirty、非 Git、Git executable 不在、legacy campaign に `base_revision_sha` がない、または既存 best ref が不正なら code-change proposal だけを reject します。
2. service-owned state directory の `.pueue-agent/worktrees/<campaign-id>/<proposal-id>` に detached candidate worktree を作り、editor を起動します。初回は fresh session、editor または必須 check の失敗時だけ同じ session を一度 resume し、最大 **2 attempts / 1 session** です。daemon の再起動はこの上限をリセットしません。
3. `git diff --check` を常に実行し、Cargo/Python の構成を発見して project check を追加します。`Cargo.toml` は `cargo test --all-targets -- --test-threads=1`、`pytest.ini` または `pyproject.toml` の `[tool.pytest.ini_options]` は pytest を対象にし、`uv.lock` があれば `uv run pytest`、なければ `python -m pytest` を選びます。editor の提案 check は発見済み check を削除できず、argv 配列でのみ追加されます。
4. 変更ファイルは **50 以下**、diff bytes は **500000 以下**、check は **8 以下**、各 check は **30 分以下**、check 出力合計は **64 KiB 以下**です。最終 diff が同じ digest のまま通過した場合だけ candidate commit と local candidate ref を確定し、その commit SHA の worktree を通常の experiment として Pueue に投入します。

candidate ref は `campaign/<campaign-id>/candidate/<proposal-id>`、best ref は `campaign/<campaign-id>/best` です。どちらも local ref であり、main、checkout 中の source branch、remote、無関係な worktree に merge、rebase、push、削除、書き換えを行いません。candidate experiment の OOM、internal failure、timeout、cancel、tracked file mutation、無効な result は promotion 不可で、best ref は変更されません。objective metric の改善が確認できた場合だけ best ref を local CAS で更新します。

custom agent/editor は trusted native executable として execution policy に登録し、argv、candidate cwd、実行ファイル identity、credential 継承を検証します。Phase 5 は process が OS の外へ逃げないことを保証する sandbox ではなく、namespace/container/VM 等の強制 containment は後続の Phase 6 の範囲です。

候補が live の間は、まず `pueue-agent status --json` の `code_changes`、`pueue-agent proposal inspect <proposal-id> --json`、`pueue-agent experiment inspect <experiment-id> --json`、`pueue-agent events --kind code_change --json`、`pueue-agent doctor --json` を読み取り専用で確認します。candidate worktree を直接調べる必要がある場合も、表示済みの base/candidate SHA と照合し、`git status --short`、`git rev-parse --verify HEAD^{commit}`、`git diff --check <base-sha> --`、`git show-ref --verify <campaign-ref>`、`git worktree list --porcelain` の読み取りだけを使います。`git update-ref`、`git checkout`、`git merge`、`git push`、`git worktree prune` や、未知の path の削除は行わないでください。

service-owned execution policy では network が既定で enabled です。ただし network access と credential access は別の権限であり、明示 allowlist にない credential/environment value は agent や agent task に継承されません。

## 状態を確認する

投入後は service、automation、project、Pueue、agent run の状態をまとめて確認できます。

```bash
pueue-agent status
```

表示された各項目を個別に確認し、automation の表示だけから Pueue task の状態を推測しないでください。

## 次に読むガイド

- [運用: 停止、再開、更新](operations-ja.md): pause、stop、cancel、disable、upgrade の手順
- 設定の詳細: `.pueue-agent/config.toml` と `templates/config.toml`
