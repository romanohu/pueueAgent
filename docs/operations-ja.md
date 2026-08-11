# 運用: 停止と再開

`pueue-agent` の automation、supervisor service、Pueue task、agent run、project 登録は別の対象です。`pueue-agent status` の `service:`、`automation:`、`project:`、`pueue:`、`agent_runs:` を別々に確認してください。automation の表示から Pueue task の状態を推測してはいけません。

| 目的 | コマンド | 影響する対象 | 影響しない対象 |
| --- | --- | --- | --- |
| 自律動作だけ止める | `pueue-agent pause` | 新しい agent 起動と自動 termination | 実行中 Pueue task、実行中 agent run、pending event |
| 自律動作を再開する | `pueue-agent resume` | pause/halt された automation の dispatch | 実行中 Pueue task の状態 |
| supervisor だけ止める | `pueue-agent stop` | user service と active agent の graceful shutdown | Pueue task、project 登録 |
| supervisor を起動する | `pueue-agent start` | user service | Pueue task、project の pause/halt 状態 |
| 実験 task を止める | `pueue-agent cancel --task-id <ID>` | 指定 project group の登録済み Pueue task 1件 | 別 task、supervisor service、project 登録 |
| project を無効化する | `pueue-agent disable` | project の automation | Pueue task、group の予約 |
| project 登録を解除する | `pueue-agent disable --remove` | project 登録と group の予約 | Pueue task |

## 手順

### 自律動作だけ止める

```bash
pueue-agent pause
pueue-agent status
```

`automation: paused` を確認します。pending event は保持され、実行中の Pueue task と agent run は継続します。再開は `pueue-agent resume` です。

### supervisor だけ止める

```bash
pueue-agent stop
pueue-agent status
```

`service: stopped` を確認します。これは scheduler service の停止であり、Pueue task は kill しません。Pueue task は kill しないが、active agent は drain 対象で、shutdown timeout 後に process tree を終了して timed_out と記録され得る。起動し直すときは `pueue-agent start` を使います。

### 実験を止める

```bash
pueue-agent status
pueue-agent cancel --task-id <ID>
pueue-agent status
```

`cancel` は現在の project group 内で stable identity を確認した指定 task 1件だけに kill を送ります。group 全体の停止、project の disable、service の stop の代用ではありません。

### project 登録を解除する

```bash
pueue-agent disable --remove
```

通常の `disable` は登録を残し group を予約します。`--remove` は明示的な登録解除です。いずれも Pueue task を kill しません。task も止める必要がある場合は、先に対象ごとに `pueue-agent cancel --task-id <ID>` を実行してください。

> 注意: `stop` と `disable` は Pueue task を kill しません。`pause` も実行中 task や current agent run を中断しません。停止対象を混同しないでください。
