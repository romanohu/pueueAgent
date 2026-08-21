# コマンドリファレンス

## 共通ルール

`PROJECT_ROOT` を省略したコマンドは通常はカレントディレクトリからプロジェクトを解決します。`--pueue-config PUEUE_CONFIG` は使用する Pueue 設定を明示します。`--json` は機械可読出力を選びます。失敗時は、まず対象プロジェクトで `pueue-agent doctor`、状態の確認に `pueue-agent status` を実行してください。

## 人間向け出力と JSON 出力

既定の人間向け出力は見出し、状態行、`summary:` で構成されます。`--json` は同じ project scope の機械可読な JSON 出力を返し、ANSI escape を含めません。診断投影は件数と本文が bounded / redacted ですが、投入時の argv、metadata、intervention message そのものを secret-safe に変換する機能ではありません。

`pueue-agent` の出力は SQLite の event、incident、termination、agent run と最新の Pueue snapshot をまとめた supervisor の投影です。raw Pueue data の `pueue status --json` とは形式も責務も異なります。accounting と guardrail は `pueue-agent` の `status`、`events`、`runs` で確認し、raw Pueue data は低レベル調査に限って使います。

## セットアップ

### `pueue-agent init`

- **構文:** `pueue-agent init [PROJECT_ROOT]`
- **目的:** プロジェクトを初期化し、agent 用設定を作成します。
- **状態変更:** プロジェクト内の初期化済み設定を作成します。
- **主なオプション:** 任意の `PROJECT_ROOT`。
- **例:** `pueue-agent init .`
- **失敗時の確認:** 書込み権限と、指定ディレクトリが意図したプロジェクトかを確認します。

### `pueue-agent enable`

- **構文:** `pueue-agent enable [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** プロジェクトを登録し、Pueue のグループとコールバックを有効化します。
- **状態変更:** agent の状態 DB、プロジェクト登録、Pueue 設定およびサービス設定を更新します。
- **主なオプション:** `--pueue-config`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent enable --pueue-config ~/.config/pueue.yml .`
- **失敗時の確認:** `pueue-agent doctor` を実行し、設定、実行ポリシー、Pueue 接続を確認します。

### `pueue-agent disable`

- **構文:** `pueue-agent disable [--remove] [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** プロジェクトの管理を停止します。
- **状態変更:** 既定ではプロジェクトを無効化しますが、予約済み Pueue グループは残します。`--remove` は登録を削除し、グループ予約も解放します。
- **主なオプション:** `--remove`、`--pueue-config`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent disable --remove .`
- **失敗時の確認:** `--remove` では Pueue 状態の取得も必要です。接続と、解放してよいグループかを確認します。

## 投入

### `pueue-agent submit`

- **構文:** `pueue-agent submit [--kind KIND] [--metadata PATH | --metadata-json JSON] [--json] COMMAND...`
- **目的:** 現在の有効プロジェクトへ 1 件のコマンドを投入します。
- **状態変更:** 提出記録と Pueue タスクを作成します。
- **主なオプション:** `--kind` は提出種別、`--metadata PATH` はメタデータファイル、`--metadata-json JSON` はインライン JSON（両者は排他）、`--json` は結果を JSON にします。末尾の `COMMAND...` は必須で、そのままコマンド argv として渡されます。
- **例:** `pueue-agent submit --kind experiment --metadata-json '{"dataset":"a"}' -- python train.py --epochs 5`
- **失敗時の確認:** プロジェクトが有効であること、コマンド argv とメタデータ JSON、Pueue 接続を確認します。

`experiment` は既定の submission kind で、live campaign がなければ managed campaign と baseline を開始します。`control` は campaign 外の bootstrap、診断、後片付け用の direct submission です。どちらも live campaign 中は拒否され、追加指示には `steer` を使います。`control` も SQLite と Pueue task に記録され、guardrail や group 制約を迂回しません。argv と任意 metadata は SQLite に保存されるため、credential や secret を含めないでください。

### `pueue-agent submit-batch`

- **構文:** `pueue-agent submit-batch --request-id UUID --manifest PATH [--group GROUP] [--json] [PROJECT_ROOT]`
- **目的:** マニフェストで定義したバッチを冪等な要求 ID として投入します。
- **状態変更:** バッチの提出記録と Pueue タスクを作成します。
- **主なオプション:** `--request-id` は必須 UUID、`--manifest` は必須、`--group` は任意のグループ指定、`--json`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent submit-batch --request-id 550e8400-e29b-41d4-a716-446655440000 --manifest batch.json`
- **失敗時の確認:** UUID とマニフェストの形式、グループ名、対象プロジェクトの有効状態を確認します。

manifest は未知の field を許さない JSON object で、次の形式です。

```json
{
  "jobs": [
    {
      "id": "train-01",
      "argv": ["python", "train.py", "--epochs", "5"]
    },
    {
      "id": "evaluate-01",
      "argv": ["python", "evaluate.py"],
      "kind": "control",
      "metadata": {"dataset": "validation"}
    }
  ]
}
```

`jobs` は 1〜128 件です。各 job の `id` は空でない 128 byte 以下の文字列で、manifest 内で一意でなければなりません。`argv` は空でない文字列配列で、JSON 直列化後 64 KiB 以下です。任意の `kind` は `experiment`（既定）または `control`、任意の `metadata` は JSON object（既定 `{}`）で直列化後 16 KiB 以下です。manifest 全体は 1 MiB 以下です。`--group` は登録済み project group と完全一致する場合だけ受理され、登録値を上書きしません。

`submit-batch` は live campaign がない direct workflow 専用です。managed campaign 開始後は副作用前に拒否され、campaign 内の次 experiment を作る interface ではありません。

## 状態確認

### `pueue-agent status`

- **構文:** `pueue-agent status [--json] [--compact] [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** プロジェクト、Pueue、サービスの現在状態を表示します。
- **状態変更:** ありません（読み取り専用）。
- **主なオプション:** `--json`、短い人間向け表示の `--compact`、`--pueue-config`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent status --compact .`
- **失敗時の確認:** Pueue の状態を取得できない場合も表示内容を確認し、`pueue-agent doctor` を実行します。

`pueue-agent status --json` には submission の一覧を含めません。submission と task の lineage は `pueue-agent runs --json`、特定 task の詳細は `inspect <TASK_ID>` で確認します。

`status --json` の `campaign.decision` は、実行中の analysis があればその cycle、なければ scheduler と同じ due 条件と source terminal 順で次の cycle を bounded に投影します。field は `cycle_id`、`source_experiment_id`、`state`、`attempt_count`、`last_decision_kind`、`next_wake_at`、`failure_code`、`failure_summary` です。raw prompt、objective、decision JSON、environment、argv、log excerpt は含めません。live campaign に decision cycle がなければ `campaign.decision` は明示的な `null` です。

### `pueue-agent campaign`

- **構文:** `pueue-agent campaign <status|pause|resume|retire> [--json] [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** 対象プロジェクトの最新 campaign の状態を確認、または operator による状態遷移を実行します。
- **状態変更:** `status` は読み取り専用です。`pause`、`resume`、`retire` は campaign の状態だけを変更し、project や既存 Pueue task を直接変更しません。
- **主なオプション:** `--json`、`--pueue-config`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent campaign status --json .`、`pueue-agent campaign pause .`
- **失敗時の確認:** project に campaign があることを確認します。`resume` は project が有効で pause/halt されておらず、未照合または termination 状態不明の experiment がない場合だけ実行できます。`retire` はすべての experiment が終端かつ照合済みの場合だけ実行できます。

`status` は campaign ID、状態、objective digest、proposal / experiment / budget の集計、task ID と時刻を表示します。`campaign status --json` の `decision` も project status と同じ current cycle と 8 field を投影し、cycle がなければ `null` です。objective 本文、raw argv、raw decision evidence は既定出力と JSON に含めません。

公開 action は次のとおりです。

```text
pueue-agent campaign status
pueue-agent campaign pause
pueue-agent campaign resume
pueue-agent campaign retire
```

### `pueue-agent proposal`

- **構文:** `pueue-agent proposal <list|inspect> [OPTIONS] [PROJECT_ROOT]`
- **目的:** 対象プロジェクトの最新 campaign に属する proposal を一覧または 1 件確認します。
- **状態変更:** ありません（読み取り専用）。
- **主なオプション:** `list` は `--limit N`（既定 20、1〜100）、`--json`、`--pueue-config` を受け付けます。`inspect` は `PROPOSAL_ID`、`--json`、`--pueue-config` を受け付けます。
- **例:** `pueue-agent proposal list --limit 20 --json .`、`pueue-agent proposal inspect PROPOSAL_ID --json .`
- **失敗時の確認:** proposal ID が対象 project の最新 campaign に属すること、`--limit` が 1〜100 であることを確認します。

一覧は campaign scope 内で安定順に最大 100 件を返します。inspect の hypothesis と expected evidence は bounded / redacted で表示され、raw argv は表示しません。

```text
pueue-agent proposal list
pueue-agent proposal inspect <proposal-id>
```

### `pueue-agent experiment`

- **構文:** `pueue-agent experiment <list|inspect> [OPTIONS] [PROJECT_ROOT]`
- **目的:** 対象プロジェクトの最新 campaign に属する experiment を一覧または 1 件確認します。
- **状態変更:** ありません（読み取り専用）。
- **主なオプション:** `list` は `--limit N`（既定 20、1〜100）、`--json`、`--pueue-config` を受け付けます。`inspect` は `EXPERIMENT_ID`、`--json`、`--pueue-config` を受け付けます。
- **例:** `pueue-agent experiment list --limit 20 --json .`、`pueue-agent experiment inspect EXPERIMENT_ID --json .`
- **失敗時の確認:** experiment ID が対象 project の最新 campaign に属すること、`--limit` が 1〜100 であることを確認します。

一覧と inspect は campaign / proposal / submission / task の identity と状態を表示します。inspect は raw argv ではなく argv digest を表示します。

```text
pueue-agent experiment list
pueue-agent experiment inspect <experiment-id>
```

### `pueue-agent events`

- **構文:** `pueue-agent events [--kind KIND] [--status STATUS] [--limit N] [--json] [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** 記録済みイベントを絞り込んで表示します。
- **状態変更:** ありません（読み取り専用）。
- **主なオプション:** `--kind`、`--status`、`--limit`（1〜1000）、`--json`、`--pueue-config`。
- **例:** `pueue-agent events --kind task-failed --status pending --limit 20 --json`
- **失敗時の確認:** 種別・状態の列挙値、`--limit` の範囲、対象プロジェクトを確認します。

### `pueue-agent runs`

- **構文:** `pueue-agent runs [--follow] [--limit N] [--json] [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** agent 実行履歴を表示し、必要なら追跡します。
- **状態変更:** ありません（読み取り専用）。
- **主なオプション:** 更新を追跡する `--follow`、表示上限 `--limit`（1〜128）、`--json`、`--pueue-config`。
- **例:** `pueue-agent runs --follow --limit 20`
- **失敗時の確認:** `--limit` の範囲、接続を維持できる端末、プロジェクト解決を確認します。

`pueue-agent runs --follow --json` は Ctrl-C まで読み取り専用で追跡し、新しい lineage を検出した polling 単位ごとに1つの bounded JSON report を出力します。1 report の `runs` には複数 run が含まれることがあります。行全体を単一 JSON document として連結しないでください。

### `pueue-agent inspect`

- **構文:** `pueue-agent inspect TASK_ID [--json] [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** 1 つの Pueue タスクに関連する記録と診断情報を表示します。
- **状態変更:** ありません（読み取り専用）。
- **主なオプション:** 必須 `TASK_ID`、`--json`、`--pueue-config`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent inspect 42 --json`
- **失敗時の確認:** タスク ID が対象プロジェクトのものかを確認し、`events` と `runs` も参照します。

### `pueue-agent explain`

- **構文:** `pueue-agent explain INCIDENT_ID [--json] [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** 1 件のインシデントについて判断根拠と関連情報を表示します。
- **状態変更:** ありません（読み取り専用）。
- **主なオプション:** 必須 `INCIDENT_ID`、`--json`、`--pueue-config`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent explain 7`
- **失敗時の確認:** インシデント ID と対象プロジェクトを確認し、`doctor` の報告も確認します。

### `pueue-agent doctor`

- **構文:** `pueue-agent doctor [--json] [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** 状態 DB、設定、サービス、Pueue、コールバックを診断します。
- **状態変更:** ありません（読み取り専用）。エラー診断がある場合は非ゼロ終了します。
- **主なオプション:** `--json`、`--pueue-config`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent doctor --json .`
- **失敗時の確認:** 出力の各チェックの remediation を実行し、実行ポリシーとサービス状態を再確認します。

decision 診断は live campaign に限定した policy 上限 + 1 の read-only probe です。`decision.rows` は cycle / attempt の件数上限、SQLite storage class、payload byte 上限を、`decision.lineage` は terminal source experiment との同一 campaign lineage を確認します。`decision.active_attempts` は active attempt と agent binding の単一 owner、`decision.running_attempts` は run ownership と timeout、`decision.wait_wake` は有限 wake、`decision.digests` は current cycle の bounded payload digest、`decision.degraded_diagnostics` は degraded cycle の bounded failure facts を確認します。doctor は row を移行、削除、修復せず、raw prompt、objective、decision body を出力しません。

## 運用制御

状態変更コマンドは lifecycle 境界をまたぎません。停止・再開の全体表は[運用ワークフローの停止境界 matrix](workflows-ja.md#project-を無効化登録解除する)も参照してください。

| コマンド | 影響する対象 | 影響しない対象 |
| --- | --- | --- |
| `enable` | disabled project の有効化、登録済み group/callback と user service の整合 | 既存 Pueue task の実行状態 |
| `disable` | project の automation | Pueue task、project 登録、group の予約 |
| `disable --remove` | project 登録と group の予約 | Pueue task |
| `pause` | 新しい agent dispatch と自動 termination | 実行中 Pueue task、実行中 agent run、pending event |
| `resume` | pause/halt の解除 | disabled project、user service、Pueue task |
| `cancel --task-id` | 検証済み Pueue task 1件 | 別 task、project 登録、user service |
| `start` | user service | project の pause/halt、Pueue task |
| `stop` | user service と active agent の graceful shutdown | Pueue task、project 登録 |

### `pueue-agent pause`

- **構文:** `pueue-agent pause [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** プロジェクトの agent 処理を一時停止します。
- **状態変更:** プロジェクトを paused にします。
- **主なオプション:** `--pueue-config`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent pause .`
- **失敗時の確認:** `status` で対象プロジェクトを確認し、再開が必要なら `resume` を使います。

### `pueue-agent resume`

- **構文:** `pueue-agent resume [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** プロジェクトの pause/halt を解除し、agent dispatch を再開可能にします。
- **状態変更:** `paused` を解除し、`halted_reason` を消去します。disabled project の `enabled` は変更しないため、その管理を再開するには `enable` を使います。
- **主なオプション:** `--pueue-config`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent resume .`
- **失敗時の確認:** `status` と `doctor` でプロジェクトおよびサービスが利用可能か確認します。

### `pueue-agent cancel`

- **構文:** `pueue-agent cancel --task-id TASK_ID [--json] [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** 検証された単一の Pueue タスクを取り消します。
- **状態変更:** 対応する 1 タスクの取消状態と記録を更新します。グループ全体を取り消す機能はありません。
- **主なオプション:** 必須の `--task-id`、`--json`、`--pueue-config`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent cancel --task-id 42 --json`
- **失敗時の確認:** タスク ID を `inspect` で確認し、意図したプロジェクトのタスクだけを指定します。

### `pueue-agent start`

- **構文:** `pueue-agent start [--json]`
- **目的:** OS の agent サービスを開始し、実行中であることを確認します。
- **状態変更:** サービスを開始します。
- **主なオプション:** `--json`。
- **例:** `pueue-agent start --json`
- **失敗時の確認:** `status` と `doctor` でサービス状態を確認します。

### `pueue-agent stop`

- **構文:** `pueue-agent stop [--json]`
- **目的:** OS の agent サービスを停止します。
- **状態変更:** user service を停止し、active agent を graceful shutdown の対象にします。shutdown timeout 後は process tree を終了して `timed_out` と記録され得ます。Pueue task と project 登録は変更しません。
- **主なオプション:** `--json`。
- **例:** `pueue-agent stop`
- **失敗時の確認:** 停止対象のサービスを確認し、必要なら `start` で再開します。

## 人による介入

### `pueue-agent wake`

- **構文:** `pueue-agent wake --reason TEXT [--json] [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** オペレーターによる wake イベントをキューへ追加します。
- **状態変更:** pending の `operator_wake` イベントを記録します。service や Pueue task を直接起動・変更しません。
- **主なオプション:** 必須の `--reason`、`--json`、`--pueue-config`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent wake --reason "<REASON>"`
- **失敗時の確認:** 理由と対象プロジェクトを確認し、`events --kind operator-wake` で記録を確認します。

### `pueue-agent steer`

- **構文:** `pueue-agent steer [--json] [--pueue-config PUEUE_CONFIG] [--project-root PROJECT_ROOT] MESSAGE...`
- **目的:** 次の実行に渡す人による介入メッセージをキューに追加します。
- **状態変更:** pending の介入メッセージを追加します。キューは実行あたり最大 16 件、合計 16 KiB、各メッセージは最大 4 KiB に制限されます。
- **主なオプション:** 必須の `MESSAGE...`、`--json`、`--pueue-config`、`--project-root`。
- **例:** `pueue-agent steer '<MESSAGE>'`
- **失敗時の確認:** 空でないメッセージと上限を確認し、`steer list` で pending の内容を確認します。

### `pueue-agent steer list`

- **構文:** `pueue-agent steer [--pueue-config PUEUE_CONFIG] [--project-root PROJECT_ROOT] list [--json]`
- **目的:** pending の人による介入メッセージを一覧します。
- **状態変更:** ありません（読み取り専用）。
- **主なオプション:** `--json`。`--pueue-config` と `--project-root` は `list` より前に指定します。
- **例:** `pueue-agent steer list --json`
- **失敗時の確認:** 正しいプロジェクトを指定し、投入済みなら実行により消費されることを確認します。

## 保守

### `pueue-agent version`

- **構文:** `pueue-agent version [--json]`
- **目的:** パッケージ版、リビジョン、ソース、サービス状態を表示します。
- **状態変更:** ありません（読み取り専用）。
- **主なオプション:** `--json`。
- **例:** `pueue-agent version --json`
- **失敗時の確認:** 出力できない場合は実行ファイルとサービス検出環境を確認します。

### `pueue-agent upgrade`

- **構文:** `pueue-agent upgrade [--source SOURCE] [--pueue-config PUEUE_CONFIG] [--json]`
- **目的:** 検証済みのソースから agent を更新し、必要なサービス操作を実行します。
- **状態変更:** リリース成果物とサービス状態を更新する可能性があります。
- **主なオプション:** `--source` はソースルート、`--pueue-config` は Pueue 設定、`--json` は報告を JSON にします。
- **例:** `pueue-agent upgrade --source /path/to/source --pueue-config ~/.config/pueue.yml --json`
- **失敗時の確認:** エラーに示される `pueue-agent version` を実行し、ソース、実行ポリシー、Pueue 設定を確認します。

## 内部・連携用

### `pueue-agent event`

- **構文:** `pueue-agent event EVENT [--group GROUP] [--task-id TASK_ID] [--metadata JSON]`
- **目的:** インストール済み Pueue コールバック用インターフェースです。通常の利用者が手動でイベントを投入する用途ではありません。
- **状態変更:** `callback` イベントでは対応するタスクのコールバックイベントを記録します。
- **主なオプション:** `EVENT`、`--group`、`--task-id`、`--metadata`（既定 `{}`）。`callback` には非負の `--task-id` が必要です。
- **例:** `pueue-agent event callback --task-id 42 --group project-a --metadata '{"status":"done"}'`
- **失敗時の確認:** Pueue 側のコールバック設定、タスク ID、グループの登録、および JSON を確認します。

### `pueue-agent daemon`

- **構文:** `pueue-agent daemon [--foreground] [--pueue-config PUEUE_CONFIG]`
- **目的:** サービスが起動する agent のエントリポイントです。通常はサービス管理から起動します。
- **状態変更:** 状態 DB を開き、イベント処理ループを開始します。
- **主なオプション:** `--foreground`、`--pueue-config`。
- **例:** `pueue-agent daemon --foreground`
- **失敗時の確認:** `doctor`、Pueue 設定、サービスログと停止シグナルを確認します。

`internal-launch` は継承したブートストラップソケットのための非公開エントリポイントです。利用者は呼び出しません。

## コマンド選択早見表

| 目的 | コマンド |
| --- | --- |
| 初期化・登録 | `init`、`enable` |
| 単発・バッチ投入 | `submit`、`submit-batch` |
| 状態と原因の確認 | `status`、`events`、`runs`、`inspect`、`explain`、`doctor` |
| 停止・再開・取消 | `pause`、`resume`、`cancel`、`wake` |
| 人の指示を渡す | `steer`、`steer list` |
| サービスと更新 | `start`、`stop`、`version`、`upgrade` |
