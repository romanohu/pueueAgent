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

監視対象の job は raw の `pueue add` ではなく `pueue-agent submit` で投入します。最初の通常 `submit` は Pueue へ追加する前に campaign、baseline proposal、experiment、budget reservation、submission intent を SQLite に一度だけ記録します。live campaign 中の二回目の `submit` と `submit-batch` は副作用前に拒否されるため、追加指示には `steer` を使います。

Phase 2 はこの baseline/control plane と安全な復旧に加え、terminal experiment 後の `terminal completion loop` を提供します。Linux の decision agent は bounded evidence から `proposal` または `finite wait` を一つ返し、proposal は既存 coordinator から次の非 code experiment へ進みます。Phase 3 の `running OOM/stall observer` と実行中の `periodic observer` による campaign health-decision loop、Phase 4 の evaluation と `goal review` は実装済みです。隔離された `code worktree` を使う Phase 5 の `code_change` pipeline も実装済みで、後続 phase に残るのは trusted native editor の OS レベル containment を扱う Phase 6 です。

## code_change proposal のライフサイクルを追跡する

通常の campaign の decision agent が `code_change` proposal を返した場合も、project 固有 adapter や追加 controller は不要です。decision coordinator が proposal を SQLite に durable に受理し、専用 code-change coordinator が次の順序を所有します。

1. Git executable、canonical project root、campaign 開始時の clean な committed `HEAD` を確認します。既存の `campaign/<campaign-id>/best` があればその完全な commit SHA、なければ `campaign.base_revision_sha` を base にします。dirty/non-Git/Git 不在、legacy campaign の `base_revision_sha` 欠落、または不正な best ref は code-change proposal だけを reject します。
2. service-owned state directory の `.pueue-agent/worktrees/<campaign-id>/<proposal-id>` に、固定 base SHA の detached candidate worktree を作ります。main、checkout 中の source branch、remote、無関係な worktree は触りません。
3. policy で検証した trusted native editor を candidate root に起動します。初回は fresh session、editor/必須 check の失敗時だけ同じ session を一度 resume し、合計 **2 attempts / 1 session** です。restart recovery は attempt 上限をリセットしません。
4. `git diff --check` と発見した project check（Rust は `Cargo.toml` → `cargo test --all-targets -- --test-threads=1`、Python は pytest 設定 → `uv.lock` があれば `uv run pytest`、なければ `python -m pytest`）を実行します。editor の提案 check は discovered check を削除できず、argv でのみ追加できます。
5. 変更ファイル **50 以下**、diff bytes **500000 以下**、check **8 以下**、各 check **30 分以下**、check 出力合計 **64 KiB 以下**を満たした同一 diff digest だけを commit します。candidate ref `campaign/<campaign-id>/candidate/<proposal-id>` を local に固定してから、candidate SHA の worktree を通常の experiment として Pueue に投入します。
6. candidate experiment は通常の experiment budget/parallelism guardrail で `budget_waiting` になり得ます。code-change proposal の受理は code-change budget を 1 slot、editor の各 attempt は agent-run hourly budget を 1 run 消費します。reject や失敗で code-change slot は返却されません。評価で objective metric の改善が証明できた場合だけ `campaign/<campaign-id>/best` を local CAS で更新します。

candidate experiment の OOM、internal failure、timeout、cancel、tracked file mutation、result/metric 不備は promotion 不可です。best ref はそのまま残り、merge、rebase、push、PR 作成、remote ref の変更は行いません。candidate worktree は実験が live の間と cleanup が完了するまで保持されます。custom editor は shell command ではなく policy に登録・identity 検証された trusted native executable ですが、Phase 5 は OS namespace/container/VM 等の強制 containment ではありません。editor/check/candidate は root で実行せず、強制 containment は Phase 6 の境界です。

候補を確認するときは、まず `pueue-agent status --json` の `code_changes`、`proposal inspect <proposal-id> --json`、`experiment inspect <experiment-id> --json`、`events --kind code_change --json`、`doctor --json` を使います。必要な場合だけ、status の SHA と所有 path を照合する読み取り専用 Git 操作（`git -C <candidate-root> status --short`、`git -C <candidate-root> rev-parse --verify HEAD^{commit}`、`git -C <candidate-root> diff --check <base-sha> --`、`git -C <project-root> show-ref --verify refs/heads/campaign/<campaign-id>/candidate/<proposal-id>`、`git -C <project-root> show-ref --verify refs/heads/campaign/<campaign-id>/best`、`git -C <project-root> worktree list --porcelain`）に限定します。`update-ref`、checkout、merge、rebase、push、`worktree prune`、未知 path の削除は行いません。

## Campaign を retire して新しい目的を開始する

現在の campaign に running、reserved、submitting、`unreconciled` の experiment がなく、すべて終端・照合済みであることを `campaign status` と `experiment list` で確認します。その後だけ次の順序で新しい objective を開始します。

```bash
pueue-agent campaign retire
# edit .pueue-agent/STATE.md
pueue-agent submit -- python train.py
```

`STATE.md` を先に編集しても active campaign の immutable objective snapshot は変わりません。`campaign retire` は既存 task を停止するコマンドではなく、retired campaign の履歴も SQLite に残ります。新しい `submit` が新しい snapshot と baseline を作成します。

## Campaign 外の単発 control task を投入する

live campaign がない有効なプロジェクトで、bootstrap、診断、後片付けなどを direct submission として投入する場合だけ `control` を使います。

```bash
pueue-agent submit --kind control -- python prepare_data.py
pueue-agent runs --json
```

通常の学習・評価に既定の `experiment` を使うと managed campaign と baseline が始まります。以後は `control` を含む direct `submit` も拒否されます。`control` は SQLite と Pueue task に記録され、ほかの guardrail を無効化しません。

## batch を冪等に投入・再開する

live campaign がない direct workflow でのみ、複数 job を JSON manifest と UUID の request ID を一組の durable request として投入します。managed campaign 開始後は `submit-batch` も副作用前に拒否されます。

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

## Terminal decision loop を確認する

terminal experiment が reconciliation されると、supervisor は一意な `decision cycle` を作ります。通常の遷移は `pending` → `analyzing` → `completed` または `waiting` です。bounded failure が service-owned 上限に達した場合は `degraded` になります。

```bash
pueue-agent status --json
pueue-agent campaign status --json
pueue-agent doctor --json
```

`status --json` の `campaign.decision` は現在の cycle を最大1件だけ表示します。運用上の field は `cycle_id`、`source_experiment_id`、`state`、`attempt_count`、`last_decision_kind`、`next_wake_at`、`failure_code`、`failure_summary` です。context/decision JSON、prompt、transcript、environment、完全な argv、log excerpt は表示されません。

`waiting` の `next_wake_at` までは Pueue task を追加せず、daemon が期限後に同じ cycle の新しい analysis を起動します。decision analysis は hourly agent-run budget、proposal は rolling experiment budget を消費します。budget 待ちは `next_eligible_at` まで自動で保持されるため、同じ task を手動投入しないでください。

`degraded` と `decision_attempts_exhausted` が表示された場合、`pueue-agent campaign pause` で自律動作を保持し、`status --json` と `doctor --json` の bounded failure facts を確認します。`campaign resume` は exhausted decision cycle を消去せず、SQLite を直接編集して retry してはいけません。継続を断念する場合は nonterminal/unreconciled experiment と reservation がないことを確認し、paused campaign を `campaign retire` してから、新しい objective と baseline を開始します。

## Periodic DeepCheck を有効化する

`.pueue-agent/config.toml` で明示的に opt-in します。

```toml
[check]
deep_check_interval_minutes = 60
```

`0`（既定値）は無効です。正の値では、通常の reconciliation が周期条件を確認し、必要なときだけ設定済みの `agent.context.mode` を使う新しい agent run を起動します。`fresh` は既定値ですが、明示的に設定した `resume` / `resume_latest` もそのまま適用されます。正常な tick の確認や異常検知だけでは agent token を消費しません。Periodic DeepCheck event が dispatch されたときだけ token を消費します。

この既存 Periodic DeepCheck は project 単位の event を起こす機能であり、実行中 experiment を継続観測して改善見込みや棄却を判断する campaign health-decision loop ではありません。Phase 3 の running health/OOM observer は別の reconciliation 経路として実装済みです。`code_change` の editor/check はさらに別の bounded pipeline で、Periodic DeepCheck の event と同一視しません。

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

code-change run が再起動をまたぐ場合は、`status --json` の `code_changes[].state`、`attempts`、`candidate_sha`、`experiment_id`、`next_action`、`cleanup_pending` と `doctor --json` の `code_change.*` を確認します。worktree が publication 前にない場合は所有 descriptor から bounded に再作成し、予期しない path、ref、identity の置換は `recovery_required` にして停止します。editor は同じ session の未完了 attempt、candidate commit/ref、Pueue submission identity を再利用し、重複 editor、commit、task を作りません。terminal result 後の `cleanup_pending` は live process/task と所有権を確認した cleanup coordinator が処理し、unknown path を手動削除したり `git worktree prune` を実行したりしないでください。
