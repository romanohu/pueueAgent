# pueue-agent 停止・ライフサイクル設計

日付: 2026-08-11
ステータス: 設計承認済み

## 目的

「自律動作を止める」「supervisor service を止める」「実験 task を止める」「project の登録を解除する」を別の操作として明確にする。

通常の停止操作で学習 task を誤って kill しないことを最優先にする。

## コマンド体系

| 操作 | 対象 | Pueue task | agent run | project 状態 |
| --- | --- | --- | --- | --- |
| `pause` | project の自律 dispatch | 継続 | 継続 | `paused=true` |
| `resume` | project の自律 dispatch | 継続 | 継続 | pause / halt を解除 |
| `stop` | supervisor service | 継続 | daemon の shutdown policy に従う | 変更なし |
| `start` | supervisor service | 継続 | 新しい dispatch を再開 | 変更なし |
| `cancel --task-id ID` | 指定 task | 対象 task だけ終了要求 | 関連 event を記録 | project は継続 |
| `disable` | project の自動運用登録 | 継続 | 新規起動なし | `enabled=false` |
| `disable --remove` | project 登録と group 予約 | 継続 | 新規起動なし | 登録解除 |

`cancel` に task ID を必須とし、初期実装では `--all` を提供しない。複数 task を止める場合は task ID ごとに明示的に実行する。

## 各操作の意味

### `pause` / `resume`

`pause` は SQLite の project state を変更し、次を停止する。

- 新しい agent run の起動
- 定期 DeepCheck event の生成
- 自動 termination request の dispatch

既存の Pueue task と実行中 agent は中断しない。pending event と operator intervention は破棄せず保持する。

`resume` は pause を解除する。guardrail による halt の場合は、halt reason も同時に解除し、operator log に解除を記録する。

### `stop` / `start`

`stop` は user service だけを停止する。project の enabled / paused / halted state と Pueue task は変更しない。

daemon が SIGTERM を受けた場合は、既存の shutdown grace period の間だけ agent run を drain し、期限後に agent run を timed out として記録する。Pueue task は kill しない。再起動時には interrupted agent run と関連 event を既存 recovery 処理で回収する。

`start` は service を起動し、起動確認後に戻る。project が pause または halt 中なら service が起動しても agent は dispatch しない。

### `cancel`

`cancel --task-id ID` は次の順で動く。

1. 指定 task が現在の project group に属することを確認する。
2. task の最新 signature と状態を Pueue status から再検証する。
3. queued の task には Pueue の remove API、running の task には kill API を呼ぶ。
4. request、operator event、最終状態を SQLite に保存する。
5. task ID の再利用や stale status が検出された場合は kill せず停止する。

`cancel` は supervisor service を止めず、project の自律運用も止めない。以後の agent が task 完了・失敗を分析できるよう、通常の terminal event flow を使う。

### `disable`

`disable` は project の新規 event dispatch と新規 submission の所有を止めるが、既存 Pueue task は止めない。通常の disable は Pueue group の予約を維持する。

`--remove` は明示的な登録解除であり、Pueue status を取得できないときは group を解放しない。service は daemon 全体で共有されるため、project disable だけでは service を停止しない。

## status 表示

`status` と `status --compact` は、少なくとも次の状態を混同しない形式で表示する。

```text
service: running
automation: active
project: enabled=true paused=false halted=false
pueue: active=1 queued=0
agent_runs: active=0
```

`service: stopped` は supervisor が動いていないことだけを示し、Pueue task が停止したことを意味しない。`automation: paused`、`halted`、`disabled` も同様に task 状態とは分離して表示する。

## 失敗時の扱い

- `stop` / `start` が service manager で失敗した場合は、project state と Pueue task を変更せず非ゼロ終了する。
- `cancel` 前の Pueue status 取得に失敗した場合は kill しない。
- `cancel` 後に terminal state が確認できない場合は、termination failure として保存し、同じ task への無制限 retry は行わない。
- `resume` は halted reason を隠さず operator log に残す。
- disable / remove で未解決 task がある場合は、その task ID を bounded な結果に含める。

## テストと受け入れ条件

- pause が agent dispatch、periodic DeepCheck、automatic termination を止め、Pueue task を止めない。
- stop が supervisor service だけを止め、Pueue task と project state を変更しない。
- start が service の起動状態を確認して戻る。
- cancel が同じ project group の指定 task だけを対象にし、queued は remove、running は kill する。
- 別 project の task、stale task signature、terminal task は cancel しない。
- disable と disable --remove の違いが、group reservation と project registration に反映される。
- daemon の SIGTERM 後に agent event が recovery され、Pueue task が継続する。
- status text / JSON が service、automation、Pueue task、agent run を別々に表示する。
- README と日本語運用ドキュメントに、停止対象を選ぶ手順を表で記載する。
