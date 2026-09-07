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

インストール済みの環境で campaign を開始する最短手順は次の4段階です。自動評価まで使う場合は、投入前に後述の[結果出力](#評価結果を出力する)を準備し、`submit` に `--metric-name` / `--metric-direction` を付けてください。

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

`agent.context.mode` の既定値は `fresh` です。この設定は通常の agent run と Periodic DeepCheck に適用されます。campaign の decision / diagnosis agent は常に fresh、code-change editor は初回 fresh・修正時に同じ session を一度 resume という別の規則です。

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

自動評価を使う場合は、最初の投入時に指標も指定します。`minimize` は小さいほど良い指標、`maximize` は大きいほど良い指標です。

```bash
pueue-agent submit --metric-name validation_loss --metric-direction minimize --metric-min-delta 0.001 -- python train.py --lr 0.001
```

`--metric-min-delta` は「改善と認める最小の差」であり、目標値ではありません。「validation_loss が 0.20 未満」などの成功条件は `STATE.md` に書きます。例では、直前の best より **0.001 を超えて** loss が下がった場合に改善と判定します。

目的は最初の `submit` 時に immutable snapshot と digest として固定されます。その後に on-disk の `STATE.md` を編集しても、active campaign の objective snapshot は変更されません。新しい目的を開始するには、現在の experiment がすべて終端・照合済みであることを確認し、`pueue-agent campaign retire` 後に `STATE.md` を編集して、新しい最初の `submit` を実行します。

live campaign 中の追加 `submit` と `submit-batch` は、別 campaign や別 task の重複作成を防ぐため副作用前に拒否されます。現在の目的への追加指示は `pueue-agent steer -- "<MESSAGE>"` を使います。

## 評価結果を出力する

学習コードは、評価終了時に `PUEUE_AGENT_RESULT_PATH` が指すファイルへ次の JSON を書き、正常終了します。`experiment_id` は必ずその実行の `PUEUE_AGENT_EXPERIMENT_ID`、metric 名は `submit --metric-name` と一致させます。単にログへ loss を print するだけでは、自動 best 更新の根拠になりません。

```json
{
  "schema_version": 1,
  "experiment_id": "実行時の PUEUE_AGENT_EXPERIMENT_ID の値",
  "metrics": {"validation_loss": 0.18}
}
```

Python の例です。`write_result` を学習コードに組み込み、評価で得た実測値を渡してください。これは結果を書くだけの関数で、中間 controller ではありません。

```python
import json
import os
from pathlib import Path


def write_result(validation_loss: float) -> None:
    payload = {
        "schema_version": 1,
        "experiment_id": os.environ["PUEUE_AGENT_EXPERIMENT_ID"],
        "metrics": {"validation_loss": float(validation_loss)},
    }
    # NaN / Infinity はファイルを開く前に拒否する。
    document = json.dumps(payload, allow_nan=False)
    result_path = Path(os.environ["PUEUE_AGENT_RESULT_PATH"])
    result_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    # 候補実験では service が先に作ったファイルの identity を維持する。
    fd = os.open(result_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as output:
        output.write(document)
```

- 結果 JSON は **16 KiB 以下**、metric は有限の数値にします。別実験の ID、文字列の数値、NaN / Infinity は受理されません。
- candidate の結果ファイルは一時ファイルからの rename / replace で差し替えず、既存のファイルへ書きます。チェックポイント等の成果物は `PUEUE_AGENT_ARTIFACT_DIR` を利用し、candidate の tracked source を学習中に変更しないでください。
- この環境変数は managed experiment に渡されます。手動実行や `--kind control` では存在を前提にできません。
- 結果の欠損・不正は `result_missing` / `result_invalid` として記録されます。task の正常終了とは別の判定であり、有効な数値がなければ best 更新は行いません。

コード変更も任せる場合は、この結果出力と実行可能な project check を最初の `submit` 前に用意し、Git に commit して clean な状態にします。リポジトリに合わせたテスト・依存関係・データの準備は必要です。「adapter 不要」はこれらが不要という意味ではありません。

## 現在の自動化の範囲

プロジェクト固有の controller script や adapter は不要です。Phase 2〜5 は、実験の終了→結果の検証→次の判断→実験またはコード変更というループを内部で進めます。成功・失敗が一意に照合されると、Linux の read-only decision agent が一つの structured decision を返します。`proposal` は予算を確認して次の処理へ、`wait` は有限の `next_wake_at` まで待機、`goal_reached` は根拠付きで人の承認待ちへ進みます。

decision analysis も agent-run hourly budget を消費します。1 cycle の連続失敗は service-owned `max_decision_attempts_per_cycle`（既定 3）、wait は `max_decision_wait_minutes`（既定 1,440 分）で制限されます。上限まで失敗すると cycle と campaign は `degraded` になり、自動 proposal は止まります。`status --json` の `campaign.decision` で `cycle_id`、`source_experiment_id`、`state`、`attempt_count`、`last_decision_kind`、`next_wake_at`、bounded な failure code/summary を確認し、raw evidence や decision body を期待しないでください。

実行中 experiment は既定30分の周期で信号を観測します。毎周期 agent を起動するのではなく、信号の反復やログ停止で `suspicious` になった場合に read-only diagnosis agent を起動します。診断は `continue` / `kill_and_resume` / `kill_and_escalate` を返し、停止確認と予算を満たした場合だけ後継実験を一つ投入します。`kill_and_resume` は同じ argv の再実行であり、自動的な batch size 変更や checkpoint 再開を意味しません。

観測間隔、通常 agent の Periodic DeepCheck、会話の引き継ぎは別の設定です。詳しくは[監視とエージェントの起動](workflows-ja.md#監視とエージェントの起動を区別する)を参照してください。

終了後は結果 JSON の数値を評価し、有効な改善なら best を更新します。改善しない成功実験が続くと plateau を数え、閾値（既定3回）で方針の見直しを促します。`goal_reached` の申告には検証済み metrics row を指す根拠が必要で、campaign は `goal_reached_pending_review` になります。人が[goal review を承認または拒否](workflows-ja.md#目標達成を確認する)するまで、最終達成扱いにはしません。

`goal review` は Phase 4 で `goal_reached` 決定を operator が承認/拒否するフローとして提供済みです。隔離された `code worktree` を使う Phase 5 の `code_change` pipeline も実装済みで、通常の decision agent が返した proposal を内部 coordinator が処理します。後続 phase に残るのは trusted native editor の OS レベル containment を扱う Phase 6 です。既存 detector/Periodic DeepCheck は別機能であり、legacy の kill pattern は running health を経由せず従来どおり incident と termination request を直接作ります。

## 予算と自動化の上限

予算の正本は SQLite と service-owned `execution-policy.toml` です。`.pueue-agent/state.json` の編集で予算を増やしたり、予約をリセットしたりはできません。新規 service policy の主な既定値は次のとおりです。既存インストールの実効値は設定・予約状況によって異なります。

| 上限 | 既定値 | 消費する処理 |
| --- | --- | --- |
| 同時実験 | 1 | managed experiment |
| 新規実験 / 24時間 | 24 | candidate を含む実験の受理 |
| agent 起動 / 1時間 | 6 | decision や editor などの agent run |
| code-change proposal / 24時間 | 10 | code-change の受理。後続失敗でも返却しない |
| decision の連続失敗 / cycle | 3 | 判断の失敗。上限到達で degraded |
| decision の有限待機 | 最大1440分 | task を増やさず次の判断を待つ |

project の `guardrails` は別の停止条件で、service の上限を拡大しません。`budget_waiting` は期限後の再開を待つ状態であり、手動で追加 `submit` する必要はありません。`degraded` / `recovery_required` は単なる予算待ちとは異なり、[診断と介入](troubleshooting-ja.md)が必要です。

## code_change を使う場合の前提と流れ

`code_change` は `pueue-agent submit --kind` の公開 submission kind ではありません。通常の campaign の decision agent が返す proposal kind であり、新しい project 固有 adapter や controller を追加せず、既存の submit、campaign、Pueue、evaluation 経路に接続されます。受理時は code-change budget を 1 slot 消費し、reject になっても戻りません。editor の各 attempt は通常の agent-run hourly budget、candidate experiment は通常の rolling experiment budget と parallelism guardrail を消費し、空きがないと `budget_waiting` になります。
予算の既定値は[予算と自動化の上限](#予算と自動化の上限)を参照してください。

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
