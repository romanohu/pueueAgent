# 運用ワークフロー

このガイドは目的ごとの手順です。`pueue-agent status` の `service:`、`automation:`、`project:`、`pueue:`、`agent_runs:` は別の対象を表します。automation の表示から Pueue task の状態を推測しないでください。

## プロジェクトを登録して最初の実験を投入する

プロジェクトのルートで初期化、設定、登録、投入を順に実施します。

```bash
pueue-agent init
$EDITOR .pueue-agent/config.toml
$EDITOR .pueue-agent/STATE.md
pueue-agent enable
pueue-agent submit -- python train.py --lr 0.001
pueue-agent status
```

監視対象の job は raw の `pueue add` ではなく `pueue-agent submit` で投入します。`submit` は Pueue へ追加する前に submission intent を SQLite に記録します。

## 単発実験を投入する

有効なプロジェクトのルートで、`--` の後ろに実行する argv を渡します。

```bash
pueue-agent submit --kind experiment -- python train.py --epochs 5
pueue-agent runs --json
```

通常の学習・評価は既定の `experiment` を使います。bootstrap、診断、後片付けなどを実験数に数えない場合だけ `--kind control` を指定します。`control` も SQLite と Pueue task に記録され、ほかの guardrail を無効化しません。

## batch を冪等に投入・再開する

複数 job は JSON manifest と UUID の request ID を一組の durable request として投入します。

```bash
pueue-agent submit-batch \
  --request-id 00000000-0000-4000-8000-000000000001 \
  --manifest jobs.json \
  --json
```

ネットワーク障害や supervisor 再起動後に再送するときは、**同じ manifest に同じ request ID** を使います。すでに accepted の job は二重投入せず、部分失敗で未確定の job だけを再開します。別の内容を同じ request ID に載せないでください。出力の job ごとの状態、accepted task ID、失敗 job を確認します。

manifest の JSON schema と上限、および `--group` の一致条件は[コマンドリファレンス](commands-ja.md#pueue-agent-submit-batch)を確認してください。

## 状態を監視する

まず supervisor の project-scoped な投影を確認し、必要に応じて原因を絞り込みます。

```bash
pueue-agent status
pueue-agent status --compact
pueue-agent events --limit 20 --json
pueue-agent runs --follow
pueue-agent doctor --json
```

`status --json` は service、automation、project、Pueue snapshot、agent run などをまとめますが、submission の一覧は含みません。submission と task の lineage は `runs --json`、特定 task は `inspect <TASK_ID>`、incident の判断根拠は `explain <INCIDENT_ID>` で確認します。低レベル調査で raw Pueue data が必要な場合だけ `pueue status --json` を使い、supervisor の accounting と guardrail の確認には `pueue-agent` の出力を使います。

## Periodic DeepCheck を有効化する

`.pueue-agent/config.toml` で明示的に opt-in します。

```toml
[check]
deep_check_interval_minutes = 60
```

`0`（既定値）は無効です。正の値では、通常の reconciliation が周期条件を確認し、必要なときだけ設定済みの `agent.context.mode` を使う新しい agent run を起動します。`fresh` は既定値ですが、明示的に設定した `resume` / `resume_latest` もそのまま適用されます。正常な tick の確認や異常検知だけでは agent token を消費しません。Periodic DeepCheck event が dispatch されたときだけ token を消費します。

同じ project では pending、claimed、retry 待ちの periodic DeepCheck がある間、新しい event は追加されません。複数の長時間 task があっても project ごとに coalesce されます。`STATE.md` には確認できた task、metric、短い判断だけを記録し、値を補完しません。

## 次の agent run に指示を渡す

次回の agent run に渡す短い指示は `steer` で登録します。

```bash
pueue-agent steer -- "<MESSAGE>"
pueue-agent steer list
pueue-agent status --json
```

`steer` は SQLite に登録するだけで、agent の起動、Pueue 操作、実行中 process への入力を行いません。各メッセージは最大 4 KiB、1 run では最大 16 件かつ合計 16 KiB までを残りの prompt budget に収まる範囲で FIFO 配信します。超過分は pending のまま後続 run へ繰り越されます。spawn に失敗したメッセージも pending に戻ります。

`pause` または `disable` 中でも登録できますが、resume または enable 後の次回 run まで配信されません。実行中の agent は中断せず、指示によって安全ポリシーや既存の制約を上書きすることもできません。

## automation だけを停止・再開する

新しい agent 起動と自動 termination だけを止めるときは `pause` を使います。

```bash
pueue-agent pause
pueue-agent status
pueue-agent resume
pueue-agent status
```

`automation: paused` を確認します。pending event は保持され、実行中の Pueue task と agent run は継続します。再開後は保持されていた event が再び dispatch 対象になります。

## supervisor service を停止・起動する

user service だけを停止・起動します。

```bash
pueue-agent stop
pueue-agent status
pueue-agent start
pueue-agent status
```

`service: stopped` を確認します。`stop` は scheduler service を停止しますが、Pueue task を kill しません。Pueue task は kill しないが、active agent は drain 対象で、shutdown timeout 後に process tree を終了して timed_out と記録され得る。`start` は service を起動しますが、project の pause/halt 状態を変更しません。

## Pueue task 1件を停止する

停止対象の stable identity を確認してから task ID を 1 件だけ指定します。

```bash
pueue-agent status
pueue-agent inspect 42
pueue-agent cancel --task-id 42
pueue-agent status
```

`cancel` は現在の project group の登録済み task 1件だけを対象にします。running task には `kill`、queued task には `remove` を依頼します。要求後に running task が終端状態へ遷移したことを確認できない場合は成功扱いにせず、termination failure として記録します。group 全体の停止、project の disable、service の stop の代用ではありません。

## project を無効化・登録解除する

automation を止めて登録を残す場合と、登録を明示的に解除する場合を分けます。

```bash
pueue-agent disable
pueue-agent status

# 登録と group の予約を解除するときだけ使う
pueue-agent disable --remove
```

通常の `disable` は project の automation を無効化し、登録と group の予約を残します。`disable --remove` は project 登録と group の予約を解除します。いずれも Pueue task を kill しません。task も止める必要がある場合は、先に対象ごとに `pueue-agent cancel --task-id <ID>` を実行してください。

> 注意: `stop` と `disable` は Pueue task を kill しません。`pause` も実行中 task や current agent run を中断しません。停止対象を混同しないでください。

| 目的 | コマンド | 影響する対象 | 影響しない対象 |
| --- | --- | --- | --- |
| 自律動作だけ止める | `pueue-agent pause` | 新しい agent 起動と自動 termination | 実行中 Pueue task、実行中 agent run、pending event |
| 自律動作を再開する | `pueue-agent resume` | pause/halt の解除と automation の dispatch | project の enabled/disabled、service、実行中 Pueue task の状態 |
| supervisor だけ止める | `pueue-agent stop` | user service と active agent の graceful shutdown | Pueue task、project 登録 |
| supervisor を起動する | `pueue-agent start` | user service | Pueue task、project の pause/halt 状態 |
| 実験 task を止める | `pueue-agent cancel --task-id <ID>` | 指定 project group の登録済み Pueue task 1件 | 別 task、supervisor service、project 登録 |
| project を無効化する | `pueue-agent disable` | project の automation | Pueue task、group の予約 |
| project 登録を解除する | `pueue-agent disable --remove` | project 登録と group の予約 | Pueue task |

## 異常検知から安全に対応する

異常を見つけたら、まず event と incident を読み取り専用で確認し、停止対象をこのガイドの matrix に照らして決めます。

```bash
pueue-agent status --json
pueue-agent events --limit 20 --json
pueue-agent runs --json
pueue-agent inspect <TASK_ID>
pueue-agent explain <INCIDENT_ID>
pueue-agent doctor --json
```

pattern の `notify` は incident を記録し、`wake` は agent が介入すべき event を記録し、`kill` は idempotent な termination request を作成します。`kill` 要求の確認が timeout したり失敗した場合は、出力と event を確認してから明示的に再調査します。確認前に group 全体の停止や service の stop を代用しません。

## supervisor を更新・rollbackする

通常の更新は `pueue-agent upgrade` です。

```bash
pueue-agent upgrade
pueue-agent upgrade --json
pueue-agent upgrade --pueue-config ~/.config/pueue/experiments.yml
```

source は `target/release/pueue-agent` の下から project checkout を自動検出します。検出できない場合は `PUEUE_AGENT_SOURCE_ROOT`、特定の checkout を使う場合は `--source <path>` を使います。更新対象は clean な `main` branch が `origin/main` を upstream とし、fast-forward 可能な checkout に限ります。dirty worktree、branch または upstream の不一致、diverged checkout は拒否されます。

enabled project に active agent run がある場合、upgrade は source の fetch や binary の置換をせずに拒否します。agent run の完了または停止を確認し、他の operator が更新していないことを確認してから再試行してください。upgrade は fetch、fast-forward、test、release build、binary の atomic install、service restart、health check の順に実行します。現在の revision なら no-op で service は restart しません。

binary install の前に service を停止して SQLite の整合性境界を作り、停止後に `VACUUM INTO` で snapshot を取得します。この短い窓では operator による SQLite の直接書き込みを避けてください。snapshot または binary install の前段で失敗した場合も、変更前の service を再起動して recovery 結果を記録します。更新後の restart または health check に失敗すると、SQLite snapshot と旧 binary を復元してから service を再起動し、health check を行う rollback を試みます。report の rollback 状態と表示された診断コマンドを確認し、原因を直して `pueue-agent upgrade` を再実行してください。

upgrade は supervisor service と binary だけを扱います。Pueue daemon、group、実験 task を kill、stop、cancel しません。失敗時は report の rollback 状態と next diagnostic を確認し、安全な orchestration を迂回せずに原因を解消して同じ `pueue-agent upgrade` を再実行します。

## daemon 再起動後を確認する

daemon は起動時に中断された agent run を recovery し、marker evidence に基づいて event を requeue または dead-letter にします。service を再起動した後は、project ごとに状態と履歴を確認します。

```bash
pueue-agent status --json
pueue-agent runs --json
pueue-agent events --limit 20 --json
pueue-agent doctor --json
```

`agent_runs:`、pending/retry event、termination の結果、Pueue task snapshot をそれぞれ確認します。service 再起動は Pueue task を停止する手順ではありません。再起動の原因や recovery 結果が不明な場合は `runs`、`events`、`inspect`、`explain` の範囲で調査してから次の操作を選びます。
