# コマンドリファレンス

## 共通ルール

`PROJECT_ROOT` を省略したコマンドは通常はカレントディレクトリからプロジェクトを解決します。`--pueue-config PUEUE_CONFIG` は使用する Pueue 設定を明示します。`--json` は機械可読出力を選びます。失敗時は、登録済みの対象プロジェクトならまず `pueue-agent doctor`、状態の確認に `pueue-agent status` を実行してください。`enable` の登録前解決で失敗した場合は、doctor/status は対象を解決できないため、表示されたエラーと config、policy、Pueue profile の入力を確認して原因を修正し、同じコマンドを再実行します。

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

### `pueue-agent instructions`

- **構文:** `pueue-agent instructions update [PROJECT_ROOT]` または `pueue-agent instructions update --apply PREVIEW_TOKEN [PROJECT_ROOT]`
- **目的:** 既存プロジェクトの、既知の旧版配布部分だけを差分確認後に更新します。`init`、`upgrade`、daemon 起動から自動適用されることはありません。
- **状態変更:** `update` は引数なしなら読み取り専用の preview です。`--apply` を指定した場合だけ、承認済み token に一致する配布部分をバックアップ付きで置き換えます。config、`STATE.md`、`state.json`、SQLite、campaign、budget、service、既存 agent run は変更しません。
- **使い方:** まず `pueue-agent instructions update .` を実行し、`status: update_available`、差分、`preview_token` を確認します。内容を承認した同じプロジェクトで、表示された token を `--apply` に渡します。token はプロジェクトの canonical path と更新前後の全 bytes に結び付くため、手動編集後は再 preview が必要です。
- **既知版の判定:** 更新対象は基準 commit `5d1a8e0` の旧版全体と完全一致する配布部分がちょうど1回ある場合だけです。前後の独自 bytes は保持されます。新版 marker の本文が完全一致する場合は `current`、本文編集、未知版、重複・混在・壊れた marker、欠落ファイルは `conflict` です。CLI は推測 merge や独自文章の破棄をしません。
- **バックアップ:** apply 前に原本を `.pueue-agent/instructions.backups/<OLD_SHA256>.md` へ保存します。新規 backup directory は mode `0700`、backup と更新後 instructions は `0600` です。同名の既存 backup は安全性と bytes が一致する場合だけ再利用され、上書きされません。
- **復旧:** conflict の場合は更新を適用せず、marker の外側に独自文章を残す形で人が `.pueue-agent/instructions.md` を整理してから再 preview します。backup から戻す場合は、同時に editor や update を走らせないことを確認し、backup bytes を人が確認してから手動で復元します。自動 rollback や backup の自動削除は行いません。
- **適用タイミング:** 実行中の agent prompt は遡及変更されません。apply 後、次に instructions を読む新しい run から反映されます。必要なら先に `pause` で新規 automation を止め、別の editor が同時にファイルを編集していないことを確認してください。
- **終了コード:** `current`、`update_available`、`updated` は `0`、conflict・unsafe path・I/O 失敗は `1`、`--apply` の値欠落など clap の構文不正は `2` です。

### `pueue-agent enable`

- **構文:** `pueue-agent enable [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** プロジェクトを登録し、Pueue のグループとコールバックを有効化します。
- **状態変更:** agent の状態 DB、プロジェクト登録、Pueue 設定およびサービス設定を更新します。
- **主なオプション:** `--pueue-config`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent enable --pueue-config ~/.config/pueue.yml .`
- **失敗時の確認:** config、policy、Pueue の解決で登録前に失敗した場合は、doctor/status を実行せず、表示されたエラーと `--pueue-config`、config、policy の入力を確認して原因を修正し、同じ `enable` を再実行します。登録後の失敗は、同じ project/profile の `pueue-agent doctor` で設定、実行ポリシー、Pueue 接続を確認します。

### `pueue-agent disable`

- **構文:** `pueue-agent disable [--remove] [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** プロジェクトの管理を停止します。
- **状態変更:** 既定ではプロジェクトを無効化しますが、予約済み Pueue グループは残します。`--remove` は登録を削除し、グループ予約も解放します。
- **主なオプション:** `--remove`、`--pueue-config`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent disable --remove .`
- **失敗時の確認:** `--remove` では Pueue 状態の取得も必要です。接続と、解放してよいグループかを確認します。

## 投入

### `pueue-agent submit`

- **構文:** `pueue-agent submit [--kind KIND] [--metadata PATH | --metadata-json JSON] [--metric-name NAME --metric-direction minimize|maximize [--metric-min-delta DELTA]] [--json] COMMAND...`
- **目的:** 現在の有効プロジェクトへ 1 件のコマンドを投入します。
- **状態変更:** 提出記録と Pueue タスクを作成します。`--metric-*` を指定すると campaign の objective metric として SQLite に保存され、後続の result manifest 評価に使われます。
- **主なオプション:** `--kind` は提出種別、`--metadata PATH` はメタデータファイル、`--metadata-json JSON` はインライン JSON（両者は排他）、`--metric-name` と `--metric-direction` はセットで指定（`--metric-min-delta` は任意の有限数）、`--json` は結果を JSON にします。末尾の `COMMAND...` は必須で、そのままコマンド argv として渡されます。
- **例:** `pueue-agent submit --kind experiment --metadata-json '{"dataset":"a"}' -- python train.py --epochs 5`
- **例（評価）:** `pueue-agent submit --metric-name loss --metric-direction minimize --metric-min-delta 0.01 -- python train.py`
- **失敗時の確認:** プロジェクトが有効であること、コマンド argv とメタデータ JSON、metric 指定の完全性（name と direction は同時必須、delta は finite）、Pueue 接続を確認します。

`experiment` は既定の submission kind で、live campaign がなければ managed campaign と baseline を開始します。`control` は campaign 外の bootstrap、診断、後片付け用の direct submission です。どちらも live campaign 中は拒否され、追加指示には `steer` を使います。`control` も SQLite と Pueue task に記録され、guardrail や group 制約を迂回しません。argv と任意 metadata は SQLite に保存されるため、credential や secret を含めないでください。Managed experiment の Pueue 追加は `/usr/bin/env` でラップされ、4つの派生変数（`PUEUE_AGENT_EXPERIMENT_ID`、`PUEUE_AGENT_CAMPAIGN_ID`、`PUEUE_AGENT_RESULT_PATH`、`PUEUE_AGENT_ARTIFACT_DIR`）が `NAME=value` 形式で付与された後に durable な user argv が続きます。Direct/control 投入はラップされず、Pueue の生コマンド表示はラップされた形式を含みます。

`code_change` は `submit --kind` で直接指定する submission kind ではなく、terminal experiment 後の decision agent が返す proposal kind です。通常の `pueue-agent submit` と既存の campaign coordinator がこの proposal を内部の code-change pipeline に渡すため、project 固有 adapter や追加 controller は必要ありません。

`--metric-min-delta` は改善幅であって目標値ではありません。成功条件は最初の投入前に `STATE.md` に記入します。学習コードは `PUEUE_AGENT_RESULT_PATH` に、その実行の ID と有限な metric を含む JSON を出力する必要があります。[出力形式とPython例](getting-started-ja.md#評価結果を出力する)を参照してください。ログへの表示だけでは自動 best 更新の根拠になりません。

### `pueue-agent trial`

- **構文:** `pueue-agent trial [--timeout-seconds 60] [--metric-name NAME --metric-direction minimize|maximize [--metric-min-delta DELTA]] [--json] -- COMMAND...`
- **目的:** campaign を開始せず、現在の登録済み project で command を一度実行して、実 argv、runtime 環境変数、結果 manifest を確認します。対象 project はカレントディレクトリから解決します。
- **状態変更:** 専用 nonce group に Pueue task を作り、`.pueue-agent/trials/<trial-id>/` に private output を作ります。task、group、output の cleanup が確認された場合に削除します。campaign、proposal、experiment、submission、budget reservation、agent run、research review、通常 Event、task observation は作成しません。trial 成功後の自動 submit もありません。
- **主なオプション:** `--timeout-seconds` は1〜300秒、既定60秒です。Pueue control と cleanup の確認には実行 timeout 後も最大30秒かかる場合があります。`--metric-name` と `--metric-direction` はセットで指定し、`--metric-min-delta` は finite かつ0以上の数値です。`--json` は固定 schema の report を返します。
- **例:** `pueue-agent trial --timeout-seconds 60 --metric-name loss --metric-direction minimize -- python train.py`
- **group と manifest:** trial は `pueue-agent-trial-<trial_id.simple()>` という未登録 group を毎回作ります。suffix は32桁のhexで、reportのcanonical UUID `trial_id` は36文字です。command は `PUEUE_AGENT_EXPERIMENT_ID`、`PUEUE_AGENT_CAMPAIGN_ID`、`PUEUE_AGENT_RESULT_PATH`、`PUEUE_AGENT_ARTIFACT_DIR` を受け取ります。結果ファイルは16 KiB以下の `{"schema_version":1,"experiment_id":"<PUEUE_AGENT_EXPERIMENT_ID>","metrics":{"loss":0.18}}` 形式で、`metrics` は有限な数値を1つ以上含めます。metric を指定した場合は指定名が必要です。
- **report と終了コード:** report は `schema_version`、`trial_id`、`task_id`、`group`、`outcome`、`terminal`、`manifest`、`metric_count`、`selected_metric_name`、`selected_metric_value`、`task_cleanup`、`group_cleanup`、`output_cleanup` を含みます。成功は task 成功、manifest 検証、3つの cleanup 確認をすべて満たした場合だけです。成功は0、command failure、timeout、manifest failure、cleanup 不確実は1で終了します。admission後のfailure reportには確認できた trial/task/group ID と cleanup 状態が残ります。preflight failureはreport作成前に固定errorを返すためJSON reportはなく、Clap parse errorは通常のexit behaviorに従います。argv、環境値、manifest bytes は出力しません。
- **失敗時の確認:** `task_cleanup`、`group_cleanup`、`output_cleanup` のいずれかが `confirmed` でなければ、report の `trial_id`、`task_id`、`group` を保存します。同じ command をすぐ再実行したり、表示されていない ID を削除したりせず、対象 profile の task と group を ID で調査してください。

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

active campaign の実行中 experiment がある場合、`status` は experiment ごとに `health:` 行を表示します。行には experiment ID、状態（`healthy` / `suspicious` / `diagnosing` / `action_pending`）、最多 signal class と観測回数（signal がなければ `none`）、最後の観測からの経過秒、最新 diagnosis の推奨 action（なければ `none`）が入ります。最大 8 行までで、各値は bounded / redacted です。

`status --json` の `health.recent` は `running_health` 行を `updated_at` の降順で最大 50 件列挙します。各行には state、観測回数、最終観測・更新時刻、bounded な signal 要約（class / source / evidence digest / 観測時刻）、推奨 action（格納されていれば）が含まれます。raw log 行は含まれません。

campaign が objective metric を宣言している場合、`status` の人間向け出力には `best:` 行（current_best experiment ID と primary metric 値、存在すれば metric 名）と `plateau:` 行（plateau counter）が表示されます。`status --json` の `campaign` には `best_experiment_id`、`best_metric_name`、`best_metric_value`、`plateau_count` が、`evaluation.recent` には直近の `experiment_metrics` 行が `updated_at` 降順で最大 50 件含まれます。

#### `status --json` の code-change projection

`status --json` には live/最近の code-change run を最大 8 件投影する `code_changes` 配列があります。各要素は `state`、`attempts`、8 文字に省略した `base_sha` と任意の `candidate_sha`、`experiment_id`、`task_id`、失敗した check の `failed_check`（`status`、bounded `summary`）、`next_action`、`cleanup_pending`、最大 8 件の `transitions`（`stage`、`reason`）を含みます。`state` は `reserved`、`preparing_worktree`、`editing`、`checking`、`committing`、`candidate_ready`、`experiment_submitted`、`evaluated`、`cleanup_pending`、`completed`、`rejected`、`recovery_required` のいずれかです。attempt は最大 2 で、raw prompt、diff、argv、environment、credential、full SHA、check output は表示されません。

#### 候補を調べる読み取り専用コマンド

candidate worktree は service-owned state directory にあり、cleanup 後は存在しないことがあります。まず状態投影を確認し、live と示された場合だけ次を使います。

```bash
pueue-agent status --json
pueue-agent proposal inspect <proposal-id> --json
pueue-agent experiment inspect <experiment-id> --json
pueue-agent events --kind code_change --limit 20 --json
pueue-agent doctor --json
```

管理者が status の SHA と所有 path を照合する必要がある場合の Git 操作も読み取り専用に限ります。

```bash
git -C <candidate-root> status --short
git -C <candidate-root> rev-parse --verify HEAD^{commit}
git -C <candidate-root> diff --check <base-sha> --
git -C <project-root> show-ref --verify refs/heads/campaign/<campaign-id>/candidate/<proposal-id>
git -C <project-root> show-ref --verify refs/heads/campaign/<campaign-id>/best
git -C <project-root> worktree list --porcelain
```

`update-ref`、`checkout`、`merge`、`rebase`、`push`、`worktree prune`、未知 path の削除は候補調査には使いません。candidate ref と best ref は local ref で、サービスが main、checkout 中の source branch、remote、無関係な worktree を変更することはありません。

### `pueue-agent campaign`

- **構文:** `pueue-agent campaign <status|pause|resume|retire|review> [--json] [--pueue-config PUEUE_CONFIG] [PROJECT_ROOT]`
- **目的:** 対象プロジェクトの最新 campaign の状態を確認、または operator による状態遷移を実行します。
- **状態変更:** `status` は読み取り専用です。通常の `pause`、`resume`、`retire` は campaign の状態を変更し、project や既存 Pueue task を直接変更しません。research が blocked の campaign で preflight 付き `resume` が成功した場合だけ、研究状態の `blocked_reason` を消し、`next_due_at` を現在時刻に設定して更新時刻を記録し、新しい review を予定します。過去の review 履歴、budget、event、session とその所有権は保持されます。
- **主なオプション:** `--json`、`--pueue-config`、任意の `PROJECT_ROOT`。
- **例:** `pueue-agent campaign status --json .`、`pueue-agent campaign pause .`
- **失敗時の確認:** project に campaign があることを確認します。`resume` は project が有効で pause/halt されておらず、未照合または termination 状態不明の experiment がない場合だけ実行できます。`retire` はすべての experiment が終端かつ照合済みの場合だけ実行できます。

`status` は campaign ID、状態、objective digest、proposal / experiment / budget の集計、task ID と時刻を表示します。`campaign status --json` の `decision` も project status と同じ current cycle と 8 field を投影し、cycle がなければ `null` です。objective 本文、raw argv、raw decision evidence は既定出力と JSON に含めません。

同じ campaign scope の研究担当の状態も bounded に表示します。`campaign status --json` の `research` には `state`、`next_due_at`、`last_review_id`、`experiment_id`、`last_action`、`blocked_reason`、`discarded_reason`、`session_generation`、`session_rebuilt`、`checkpoint_confirmation` の10フィールドを出し、別の `research_history` には同じ campaign の review を新しい順に最大32件出します。`session_rebuilt` は summary では session generation が 0 より大きく、現在の campaign generation と一致する `last_review_id` の直近 review でセッション再構成が確認された事実だけを示し、後続の通常の resume を含む session 全体の由来を示すものではありません。history の各行ではその review 自身の 0 より大きい session generation でセッション再構成が確認された事実を示し、後の session generation が変わっても過去の history 行の事実を書き換えません。研究が存在しない campaign は idle として扱われ、次回予定日を作りません。値は bounded / redacted で、raw session ID、prompt、transcript、credential は表示しません。

研究 review の履歴を広域の新しい history CLI で取得することはできません。通常の `status --json` は `campaign.research` だけを出し、履歴を出しません。`doctor --json` は該当 campaign がある場合に top-level の任意フィールド `research` だけを出し、履歴を出しません。対象 experiment の `pueue-agent experiment inspect <experiment-id> --json` には、その experiment と同じ campaign に属する review だけを新しい順に最大32件の `research_history` として出します。いずれも別 campaign の review を混ぜません。

`review accept` は `goal_reached_pending_review` の campaign を `retired`（`goal_accepted`）へ、`review reject` は `active` へ戻し、該当 `goal_reached` 決定イベントを `dead_letter` 化します。いずれも同一トランザクションで operator log を残し、非終端 experiment や予約が残る場合は失敗します。`accept` は idempotent で、`reject` は pending-review 以外では失敗します。

公開 action は次のとおりです。

```text
pueue-agent campaign status
pueue-agent campaign pause
pueue-agent campaign resume
pueue-agent campaign retire
pueue-agent campaign review accept [--note TEXT]
pueue-agent campaign review reject [--note TEXT]
```

research state が blocked になった場合は、原因確認なしに `wake` や daemon restart で解除しません。supervisor が campaign の自律処理を明示的に止め、runtime、policy、instructions、campaign に結び付いた research session を確認してから再開します。

```bash
pueue-agent campaign pause
# 原因を修正し、必要なら binary / service policy / instructions を更新する
pueue-agent doctor
pueue-agent campaign status --json
pueue-agent campaign resume
```

`campaign resume` は supervisor の preflight が成功した場合だけ research state の blocked を解除して新しい review を予定します。supervisor は安全に欠落した session だけを保存済みの bounded notes から再構成し、所有権を確認できない session は fresh に置き換えません。表示された `blocked_reason` は原因を説明するための bounded な表示で、原因が別の表示から補われる場合でも、それだけで durable な block を解除する権限にはなりません。過去 review の失敗、試行数、消費済み budget、未解決の停止要求、event、session とその所有権、terminal decision の exhausted cycle は消去しません。

研究担当の action は `continue`、`stop_and_next`、`resume_from_checkpoint` の三つです。`continue` は新しい task を作らず、`stop_and_next` は対象 task の停止確認と terminal projection の後に fresh な terminal decision へ渡し、`resume_from_checkpoint` は互換性・scope・サイズ・retention を検証できる checkpoint だけを対象にします。停止コマンドの終了だけでは後継投入の前提を満たさず、fresh decision が待機・拒否を返すこともあります。研究の timeout や不正回答だけで learning task を kill することはありません。

研究担当の周期は service policy の `[campaign].research_interval_minutes` で、既定30、`0..=1440` の範囲、`0` は無効です。`observer_interval_minutes` と `[check].deep_check_interval_minutes` とは別の値です。timeout、不正回答、通常の runtime failure と、安全に Missing と判定できた session の再構成は supervisor が `max_decision_attempts_per_cycle`（既定3）の有限上限と既存 agent-run budget の範囲で扱います。session の所有権・path、policy/credential、unsupported runtime の問題は retry や fresh fallback ではなく research state を blocked にします。上限到達後は bounded な blocked reason を残し、無限 retry や budget の返却は行いません。Research reviewer は blocked enum を返さず、三つの action だけを返します。

checkpoint は、対象コマンドの loader と保存物の対応、source support proof、検証可能な scope/サイズ、所有権・identity・内容 digest、後継が terminal になり live reader がいなくなるまでの retention を確認できる supported path に限ります。campaign ごとの retained research checkpoint の合計は **1 GiB** までです。任意の framework や candidate を一律に扱う機能ではなく、argv の resume flag や研究メモだけでは「再開確認済み」になりません。要求された `resume_from_checkpoint` は `last_action` に残りますが、support が proved unavailable なら `checkpoint_confirmation` は `unsupported` となり、適用も後継もありません。対応する load evidence が確認できない場合は `checkpoint_confirmation` を `unconfirmed` として扱います。要求された action、unsupported / unconfirmed の結果、後継の未受理、実際の load confirmation を別々に確認してください。

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
- **確認点:** active campaign がない場合の `state.objective` は `STATE.md` が初回 submit に使えるかを、`execution.agent_runtime` は pinned Codex が research runtime を実行できるかを示します。どちらも読み取り専用 check です。
- **失敗時の確認:** 出力の各チェックの remediation を実行し、実行ポリシーとサービス状態を再確認します。

decision 診断は live campaign に限定した policy 上限 + 1 の read-only probe です。`decision.rows` は cycle / attempt の件数上限、SQLite storage class、payload byte 上限を、`decision.lineage` は terminal source experiment との同一 campaign lineage を確認します。`decision.active_attempts` は active attempt と agent binding の単一 owner、`decision.running_attempts` は run ownership と timeout、`decision.wait_wake` は有限 wake、`decision.digests` は current cycle の bounded payload digest、`decision.degraded_diagnostics` は degraded cycle の bounded failure facts を確認します。doctor は row を移行、削除、修復せず、raw prompt、objective、decision body を出力しません。

code-change の read-only probe は次の8 checksです: `code_change.cleanup`、`code_change.experiments`、`code_change.lineage`、`code_change.refs`、`code_change.rows`、`code_change.single_live`、`code_change.stale`、`code_change.worktrees`。各件数は上限付きで、doctor は cleanup、ref、worktree、lineage、single-live invariant を修復・削除しません。`code_change.stale` は stale row の有無を知らせる warning check であり、検出しただけで自動 recovery や editor 再起動を行いません。

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

既存の active campaign も binary / service policy の更新後に `research_interval_minutes` の対象になります。新しい config key を拒否する旧 binary から更新する場合は、先に `pueue-agent campaign pause` を実行し、binary と policy を更新し、必要なら `pueue-agent instructions update .` の preview/token/apply を完了してから `pueue-agent doctor` と `pueue-agent campaign status --json` を確認します。その後だけ `pueue-agent campaign resume` を意識的に実行してください。upgrade、daemon restart、`wake` は research blocked を自動解除せず、実行中 task の停止確認や後継の受理も保証しません。

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
