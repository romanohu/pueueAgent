# 定期 DeepCheck 設計

日付: 2026-08-11
ステータス: 設計承認済み

## 目的

長時間の Pueue task が正常に `Running` のままでも、一定周期で coding agent を起動し、ログ・metric・artifact の状態を確認して、`STATE.md` と canonical な `state.json` に進捗を記録できるようにする。

異常検知と task 完了を待たずに観測できるようにするが、agent を常駐させず、既存の event scheduler に統合して token 消費を bounded にする。

## スコープ

- 実行中 task がある project 単位の定期 `DeepCheck` event
- `DeepCheck` event の冪等な生成と既存 scheduler への dispatch
- 定期 run の prompt と状態更新方針
- pause、halt、disable、agent run 中の安全な skip
- 設定と日本語ドキュメント

次は対象外とする。

- supervisor による ML metric の意味解釈
- metric の形式を統一する新しい収集 protocol
- task ごとの agent 起動
- Pueue に wake 用の dummy task を投入する方式

## 設定

既存の `[check]` にある `deep_check_interval_minutes` を wall-clock の周期として使う。

```toml
[check]
interval_minutes = 10
deep_check_interval_minutes = 30
```

- `0`: 定期 DeepCheck を無効化する。既存 project に突然 agent 起動を発生させない。
- `1` 以上: 実行中 task がある project で有効化する。
- `interval_minutes`: Pueue reconciliation の周期であり、agent 起動周期ではない。

## アーキテクチャ

daemon の `run_once` は、Pueue reconciliation、軽量な異常検知、termination 処理の後、既存 `Scheduler::tick` の前に project ごとの定期 event を評価する。

```text
Pueue status
  └─ reconciliation
       ├─ terminal/anomaly event
       ├─ periodic DeepCheck event
       └─ Scheduler::tick
            └─ project ごとに agent 最大1つ
```

定期 event の生成は専用の小さな scheduling unit に分離し、daemon、SQLite repository、config loader の責務を混ぜない。既存の `EventKind::DeepCheck`、event lease、project 単位の agent run 制約を再利用する。

## event 生成条件

次の条件をすべて満たすときだけ event を生成する。

1. `deep_check_interval_minutes > 0`。
2. project が enabled、paused でなく、halted でもない。
3. 今回の reconciliation snapshot に、その project の Pueue group に属する `Running` task が1つ以上ある。
4. project に `starting` または `running` の agent run がない。
5. 未処理の定期 `DeepCheck` event（`pending`、`claimed`、`retry_wait`）がない。
6. 前回の定期 DeepCheck から指定時間が経過している。

定期 event は project 単位で1つだけ作る。task が複数あっても task ごとには作らない。

初回は、現在の running task が指定間隔以上継続していれば次の tick で作成する。開始直後の task は、開始時刻から指定間隔が経過するまで待つ。Pueue の開始時刻が利用できない場合は、最初に観測した時刻を開始時刻として扱う。

同じ tick の再実行や supervisor の二重起動に備え、project、周期 bucket、event kind から安定した dedup key を作り、SQLite の idempotent insert を使う。既存 event の状態を新しい event で上書きしない。

## payload と prompt

event payload は bounded projection とし、command、環境変数、transcript、prompt 本文を保存しない。

```json
{
  "source": "periodic",
  "task_count": 2,
  "task_ids": [41, 42],
  "scheduled_at": 1720000000
}
```

既存 scheduler は `Dispatch mode: deep_check` を使う。agent は次の順に読む。

1. `.pueue-agent/instructions.md`
2. `.pueue-agent/state.json`
3. `.pueue-agent/STATE.md`
4. 必要なログ、metric、artifact

正常な場合は、観測時刻、対象 task、確認した metric、短い判断を `STATE.md` に記録し、canonical state の current facts と next action を必要な範囲で更新する。異常があれば、既存の crash / stalled と同じ制約のもとで調査する。定期 run が自動的に次の実験を作ることは要求せず、`instructions.md` と guardrail に従う。

異常 event と同じ tick に生成された場合は、既存 scheduler の優先順位に従い、異常 event を primary にしながら同じ project の event batch にまとめる。

## 停止・失敗時の扱い

- Pueue status 取得失敗時は DeepCheck event を作らず、既存 observation を変更しない。
- config または canonical state が不正な場合は既存 dispatch と同じく安全側で停止する。
- pause、halt、disable 中は event を作らない。pending の既存 event は保持する。
- agent run 中に周期を迎えた場合は event を作らない。次の tick で再評価する。
- agent の spawn failure は既存の `agent.max_retries` を使う。
- agent が起動後に失敗しても、同じ周期で無限 retry はしない。失敗を `agent_runs` に記録し、次の定期周期で再評価する。
- 定期 DeepCheck は `max_agent_runs` を消費するが、Pueue の experiment budget は消費しない。

## テストと受け入れ条件

- running task がない project では event が生成されない。
- running task が複数あっても project ごとに1 event だけ生成される。
- interval 前には生成されず、interval 到達後に1 event が生成される。
- daemon tick を繰り返しても同じ周期の event が重複しない。
- active agent、pause、halt、disable 中は生成されない。
- 異常 event と定期 event が同時に存在すると、1つの agent run に集約される。
- Pueue status failure 後に既存 observation と event が壊れない。
- template、README、運用ドキュメントに、無効化の既定値と token 消費の条件が記載される。
