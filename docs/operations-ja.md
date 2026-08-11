# 運用: 停止、再開、更新

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

`cancel` は現在の project group 内で stable identity を確認した指定 task 1件だけを対象にします。running task には `kill`、queued task には `remove` を送ります。group 全体の停止、project の disable、service の stop の代用ではありません。

### project 登録を解除する

```bash
pueue-agent disable --remove
```

通常の `disable` は登録を残し group を予約します。`--remove` は明示的な登録解除です。いずれも Pueue task を kill しません。task も止める必要がある場合は、先に対象ごとに `pueue-agent cancel --task-id <ID>` を実行してください。

> 注意: `stop` と `disable` は Pueue task を kill しません。`pause` も実行中 task や current agent run を中断しません。停止対象を混同しないでください。

## pueue-agent の更新

通常の更新は `pueue-agent upgrade` です。

```bash
pueue-agent upgrade
pueue-agent upgrade --json
pueue-agent upgrade --pueue-config ~/.config/pueue/experiments.yml
```

source は、現在の実行ファイルが `target/release/pueue-agent` の下にある場合、その project checkout を自動検出します。自動検出できない場合は `PUEUE_AGENT_SOURCE_ROOT` を設定し、特定の checkout を使う場合や fallback を明示したい場合は `--source <path>` を指定します。明示した `--source` が自動検出や環境変数より優先されます。Pueue の health check は supervisor と同じ profile を使い、設定の優先順位は `--pueue-config <path>`、`PUEUE_CONFIG`、既定の `~/.config/pueue/pueue.yml` です。

source checkout には `git`、Rust stable、Cargo が必要です。更新対象は clean な `main` branch で、`origin/main` を upstream とし、`origin/main` への fast-forward が可能な場合だけです。dirty worktree、branch の不一致、upstream の不一致、diverged checkout は更新前に拒否されます。

enabled project に active agent run がある場合、upgrade は source の fetch や binary の置換をせずに拒否します。agent run の完了または停止を確認し、他の operator が upgrade していないことを確認してから再試行してください。同時実行は upgrade lock でも調整されます。

upgrade は fetch、fast-forward、テスト、release build、binary の atomic install、service restart、health check を順に行います。revision がすでに current の場合は no-op として報告し、service を restart しません。fast-forward 後に失敗した revision は retry marker に残り、条件を直した再実行で同じ revision の処理を再試行できます。binary install の前に service を停止して SQLite の整合性境界を作り、停止後に `VACUUM INTO` で snapshot を取得します。この短い窓では operator による SQLite の直接書き込みを避けてください。更新後の restart または health check に失敗すると、SQLite snapshot と旧 binary を復元してから service を再起動し、health check を行う rollback を試みます。rollback の attempted/succeeded または failed は report で確認できます。失敗時は出力された診断コマンドを実行し、原因を直して `pueue-agent upgrade` を再実行してください。

upgrade は supervisor service と binary だけを扱います。Pueue daemon、group、実験 task を kill、stop、cancel することはなく、実験の処理は継続します。active agent run の coordination は更新を安全側に拒否するためのもので、実験 task を停止する手順ではありません。

source checkout の破損などで通常の upgrade を実行できない場合に限り、復旧手順として次を使います。

```bash
git pull --ff-only
./install.sh
```

これは通常の更新経路ではありません。手動復旧後は `pueue-agent version --json`、`pueue-agent status --json`、必要なら `pueue-agent doctor --json` で binary、service、state を確認します。
