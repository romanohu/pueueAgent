# トラブルシューティング

## 最初に行う4段階の確認

```bash
pueue-agent status --compact
pueue-agent doctor
pueue-agent events --limit 100
pueue-agent runs --limit 100
```

この順序は、project と service の概要、設定・Pueue・policy の診断、bounded な event 履歴、bounded な agent-run 履歴を読み取り専用で確認します。`doctor` は error check があると非ゼロで終了しますが、それ自体が状態変更を意味するものではありません。出力を共有するときは必要な行だけを使い、project の設定ファイル、環境、agent log を丸ごと貼り付けないでください。

個別の task は `pueue-agent inspect <TASK_ID>`、incident は `pueue-agent explain <INCIDENT_ID>` で絞り込みます。機械処理には各コマンドの `--json` を使えます。コマンドの上限と状態変更の有無は[コマンドリファレンス](commands-ja.md)を参照してください。

## Service が起動していない

| 症状 | まず確認 | 想定原因 | 安全な復旧 |
| --- | --- | --- | --- |
| `service: stopped` | `pueue-agent doctor` の `service.state` と `service.path` | user service が停止している | `pueue-agent start` を実行し、`pueue-agent status --compact` で `running` を確認する |
| `service: not_installed` | 対象 project と Pueue profile を確認して `pueue-agent doctor` | project の `enable` が完了していない | profile を確認後に `pueue-agent enable` を実行し、`pueue-agent doctor` を再実行する |
| `start` が失敗する | `pueue-agent doctor` の service と execution の error check | service manager、release binary、execution policy のいずれかが利用できない | `pueue-agent stop` で曖昧な起動状態を避け、信頼できる管理者が配布状態を復元した後に `pueue-agent start` と `pueue-agent doctor` で確認する |

`stop` は supervisor service を止めますが、実行中の Pueue task を取り消しません。対象ごとの停止境界は[運用ワークフロー](workflows-ja.md)を確認してください。

## Pueue profile または config が一致しない

| 症状 | まず確認 | 想定原因 | 安全な復旧 |
| --- | --- | --- | --- |
| `pueue: error` または `pueue.status` error | `pueue-agent doctor --pueue-config <PUEUE_CONFIG>` | Pueue が停止中、または別 profile を参照している | 運用管理者が登録時の profile で Pueue を復旧した後、同じ `--pueue-config` を付けて `pueue-agent enable`、`status --compact`、`doctor` を順に実行する |
| `project.config` が identity mismatch | `status --compact` の project と、`doctor` の `project.config` | 別 project root、`project_id`、`pueue_group` の組合せを参照している | 元の project root と登録時の profile に戻り、`pueue-agent status --compact` と `pueue-agent doctor` で一致を確認する。値を推測して変更しない |
| project group または callback が見つからない | `doctor` の `pueue.status` と `pueue.callback` | `enable` が未完了、または選択した profile が異なる | profile を確定してから `pueue-agent enable --pueue-config <PUEUE_CONFIG>` を再実行し、`pueue-agent doctor --pueue-config <PUEUE_CONFIG>` で確認する |

CLI ごとに別の profile を混在させないでください。profile の優先順位は[導入ガイド](getting-started-ja.md)にあります。

## Execution policy を読み込めない

| 症状 | まず確認 | 想定原因 | 安全な復旧 |
| --- | --- | --- | --- |
| `execution.policy` が error | `pueue-agent doctor` の policy code | service-owned policy が missing、unreadable、weak permissions、または unknown field と判定された | `pueue-agent stop` で新規 dispatch を止める。信頼できる管理者がインストール済み policy を正規の配布物から復元した後だけ、`pueue-agent start` と `pueue-agent doctor` を実行する |
| `execution.anchors` が error | `doctor` の anchor check と `pueue-agent version --json` | pinned executable、launcher、Pueue、または config の identity が変わった | 置換物を自動承認しない。管理者が信頼済み版を復元した後に `pueue-agent start`、`version --json`、`doctor` で再検証する |
| recent run に policy code と stage がある | `pueue-agent runs --limit 100 --json` | project policy または run-bound 検証で拒否された | code と stage を管理者へ渡す。原因が解消し `doctor` が通った後、必要性を確認して `pueue-agent wake --reason "policy recovery verified"` を使う |

`doctor` は policy の作成、修復、replacement の enrollment を行いません。これは診断の読み取り専用境界です。

## Project が paused または halted

| 症状 | まず確認 | 想定原因 | 安全な復旧 |
| --- | --- | --- | --- |
| `automation: paused` | `pueue-agent status --compact` の lifecycle | operator が automation を一時停止した | 停止理由と pending event を確認してから `pueue-agent resume`、続けて `pueue-agent status --compact` を実行する |
| `automation: halted` | `status` の halted reason、`events --limit 100`、`runs --limit 100` | guardrail または明示的な halt が発生した | 原因と budget を確認してから明示的に `pueue-agent resume` する。`resume` は pause と halt を解除するため、理由を確認せず実行しない |
| `automation: disabled` | `status` と `doctor` で登録と profile を確認 | project が disabled | 管理を再開する意思と profile を確認して `pueue-agent enable` を実行する |

`pause`、`halt`、`disable` 中も Pueue task の状態は別に確認します。automation の表示だけを根拠に task を再投入しないでください。

## Event が retry または dead-letter になった

| 症状 | まず確認 | 想定原因 | 安全な復旧 |
| --- | --- | --- | --- |
| event が `retry_wait` | `pueue-agent events --status retry-wait --limit 100` と関連 run | transient な起動・実行失敗後の bounded backoff | service を `pueue-agent status --compact` で確認し、`running` なら scheduler の再試行を待つ。重複する event や task を追加しない |
| event が `dead_letter` | `pueue-agent events --status dead-letter --limit 100 --json` と `pueue-agent runs --limit 100 --json` | retry 上限超過、policy block、または実行結果が不明な recovery | terminal 状態をその場で書き換えない。原因を解消し、元の副作用が発生していないと確認できた場合だけ、新しい判断として `pueue-agent wake --reason "dead-letter reviewed"` を使う |
| task/incident との関係が不明 | `pueue-agent inspect <TASK_ID>` または `pueue-agent explain <INCIDENT_ID>` | 複数 event が同じ run に束ねられた、または termination 記録がある | bounded な lineage を確認し、task の取消が必要なら確認済み ID に対してだけ `pueue-agent cancel --task-id <TASK_ID>` を使う |

`dead_letter` は自動再試行しない terminal 状態です。とくに restart 後に実行結果が不明な場合、安易な再投入は副作用を重複させます。

## Native gate で agent が起動しない

| 症状 | まず確認 | 想定原因 | 安全な復旧 |
| --- | --- | --- | --- |
| run が開始前に失敗し policy code がある | `pueue-agent runs --limit 100 --json` の `policy_code` と `failure_stage` | executable、root、argument、network、session、log の検証拒否 | `pueue-agent doctor` で policy と anchor を確認する。管理者が登録済み構成を復元し診断が通った後だけ、必要なら `pueue-agent wake --reason "native gate recovery verified"` を使う |
| `native_gate_failed` または `setsid_failed` | `runs` と `doctor` の bounded summary | helper readiness、process-group 作成、marker/ack 境界で失敗した | `pueue-agent stop` で新規 dispatch を止め、管理者の確認後に `pueue-agent start` と `doctor` を実行する。結果不明の run は再投入しない |
| `session_missing` または `session_not_owned` | `runs --json` の code と [設定テンプレート](../templates/config.toml) の context mode | 指定 session が存在しない、または project に属さない | 暗黙に fresh へ切り替えない。正しい project/context を管理者が確認した後に `pueue-agent doctor` を再実行する |

hidden の `internal-launch` は利用者向け復旧コマンドではありません。直接呼び出さないでください。

## Private temp admission または cleanup が失敗する

| 症状 | まず確認 | 想定原因 | 安全な復旧 |
| --- | --- | --- | --- |
| agent が private temp admission で `temp_unsafe` | `pueue-agent runs --limit 100 --json` と `doctor` の policy evidence | ownership、identity、mount boundary、entry/byte limit の検証失敗 | 検証を迂回しない。原因を管理者が解消した後、`pueue-agent doctor` で admission policy を再確認する |
| `unsupported_platform` | `doctor` と[対応環境](getting-started-ja.md#対応環境) | 必要な mount identity 機能を確認できない、または未対応 OS | 検証を迂回しない。対応済み Linux 環境へ移した後に `pueue-agent enable` と `pueue-agent doctor` で再確認する |
| terminal run 後も private temp が残っているように見える | `pueue-agent status --compact` で service、`pueue-agent runs --limit 100` で run の終端状態を別々に確認 | post-terminal cleanup が daemon 内で ownership を保持して再試行中、または別世代の directory を見ている可能性がある | service が `running` なら daemon の bounded retry に任せる。手動削除はせず、残り続ける場合は `pueue-agent stop` 後に管理者へ調査を依頼する |

private temp admission の policy evidence は `status`、`doctor`、`runs` で確認できます。一方、retained post-terminal cleanup は daemon の in-memory state で再試行され、現在の CLI に `cleanup_pending` のような専用診断 field はありません。上のコマンドは service と run の状態確認であり、retained cleanup の有無を証明しません。private temp の検証は安全側に停止し、弱い platform 判定へ fallback する設定はありません。

## Callback を取りこぼしたように見える

| 症状 | まず確認 | 想定原因 | 安全な復旧 |
| --- | --- | --- | --- |
| Pueue task は終端したが event が見えない | `status --compact`、`doctor` の callback、`events --limit 100` | callback 未登録、別 profile、service 停止、または callback 到着前 | `pueue-agent start` で service を動かし、通常の status reconciliation を待って `events --limit 100` を再確認する |
| `pueue.callback` が missing/different | `pueue-agent doctor --pueue-config <PUEUE_CONFIG>` | callback の未登録、または別の callback が設定されている | profile と既存 callback を確認してから `pueue-agent enable --pueue-config <PUEUE_CONFIG>` を実行し、`doctor` で一致を確認する |
| 同じ callback が複数回来たように見える | `events` と `inspect <TASK_ID>` の project-scoped lineage | callback 再送と reconciliation が同じ task を観測した | idempotent な記録に任せる。event を削除せず、`pueue-agent status --compact` で重複 dispatch がないことを確認する |

callback の補償経路は reconciliation です。利用者が `pueue-agent event` を手動実行して欠落を埋めないでください。

## Upgrade が失敗または rollback した

| 症状 | まず確認 | 想定原因 | 安全な復旧 |
| --- | --- | --- | --- |
| upgrade が install 前に失敗 | `pueue-agent version --json` と表示された next diagnostic | dirty/diverged source、active agent run、test/build failure、profile/policy error | 原因を解消して `pueue-agent doctor` で確認し、同じ `pueue-agent upgrade` を再実行する |
| human 出力が `rollback=succeeded`、または JSON が `"rollback":"succeeded"` | `version --json`、`status --compact`、`doctor` | install 後の restart または health check failure | 旧 binary と SQLite snapshot が復元されたことを確認し、原因を解消してから `pueue-agent upgrade` を再実行する |
| human 出力が `rollback=failed`、JSON が `"rollback":"failed"`、または service 状態が不明 | `version --json`、`status --compact`、`doctor --json` の bounded report | binary、database restore、service restart のいずれかが完了しなかった | `pueue-agent stop` で新規 dispatch を止め、report を信頼できる管理者へ渡す。管理者の復旧後に `pueue-agent start` と `doctor` で確認する |

upgrade の transaction 境界、snapshot、再実行条件は[運用ワークフロー](workflows-ja.md#supervisor-を更新rollbackする)を参照してください。

## 安全のため案内しない操作

| 症状 | まず確認 | 想定原因 | 安全な復旧 |
| --- | --- | --- | --- |
| 記録だけを直せば動きそうに見える | `status`、`doctor`、`events`、`runs` の bounded projection | SQLite の表示と実体を手作業で一致させようとしている | database を SQL client から更新・削除しない。supported CLI と daemon recovery を使う |
| permission error を回避したい | `doctor` の typed policy code | ownership、mode、anchor identity の異常 | permission を広げて検証を通さない。`pueue-agent stop` 後、信頼できる管理者が正規の配布状態を復元し、`start` と `doctor` で確認する |
| policy error をその場で消したい | `doctor` の `execution.policy` と `execution.anchors` | immutable policy または pinned identity の不一致 | policy file を直接編集・再生成・自動承認しない。管理者の復旧後に supported CLI で再検証する |
| 残った process をすぐ終了したい | `runs`、`status`、対象 task の `inspect` | 保存済み PID と現在の process identity が同じとは限らない | 未検証 PID へ signal を送らない。Pueue task は確認済み ID に対する `pueue-agent cancel --task-id <TASK_ID>`、service は `pueue-agent stop` を使う |

診断出力だけで安全な復旧を確定できない場合は、状態を変更せず、bounded な `doctor --json`、`events --json`、`runs --json` の結果を管理者へ渡してください。
