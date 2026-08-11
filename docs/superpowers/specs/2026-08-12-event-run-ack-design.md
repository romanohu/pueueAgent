# Event Run Ack 設計

日付: 2026-08-12

ステータス: 設計承認待ち

## 目的

agent の spawn が成功したことを event の完了と誤認しない。event を一つの project に閉じた agent run に永続的に結び付け、process の終了とその結果の SQLite 更新まで成功したときだけ `completed` にする。agent の非ゼロ終了、timeout、daemon restart interruption はイベント単位で bounded exponential backoff の retry に戻し、attempt 上限を超えたものは通常の `failed` と区別できる dead-letter として SQLite に残す。

この slice の ack は goal の意味判定や実験結果の成功判定ではない。agent process がイベント処理を実行し、結果を永続化できたかという実行 ack だけを表す。

## スコープ

- `EventStatus` の二段階 ack 状態、SQLite schema 13 migration、project-scoped な event/run transaction
- launch gate と event の `in_flight` / `dispatched` の crash window の解消
- `AgentHandle::{poll,wait,timeout_now}` と daemon startup recovery の agent failure/timeout/restart retry
- `max_retries` の attempt semantics、bounded exponential backoff、dead-letter 判定
- grouped event をイベントごとの attempts で解決する処理
- 既存 `events`、`runs`、`status`、`doctor` での bounded なローカル可視化
- host-independent Rust tests と既存テスト期待値の更新

次はこの slice に含めない。

- Slack、webhook、その他の外部 delivery worker
- batch lineage、budget reservation、goal state machine、token accounting
- durable submission/proposal 経路の全面的な idempotency 再設計
- agent の出力を解析した goal 達成の意味判定

## 既存実装との差分

現状の `Scheduler::tick` は `AgentRunner::spawn` が `AgentHandle` を返した直後に `EventRepository::transition_many(..., EventStatus::Completed, ...)` を実行する。従って process が後で失敗しても event は completed のままになる。`src/agent.rs` の `AgentHandle::{poll,wait,timeout_now}` は `AgentRunRepository::finish` だけを呼び、`agent_run_events` の event を解決していない。`AgentRunRepository::recover_interrupted` は active run の linked event を `pending` に戻すだけで、retry limit/backoff/dead-letter を判定しない。

既存の `agent_run_events` は `(project_id, run_id)` と `(project_id, event_id)` の複合 foreign key を持ち、`AgentRunRepository::insert_with_events_and_reservation`、launch gate、lease、recovery が既に project 単位の境界を提供している。この slice は新しい配信基盤を作らず、それらの writer を一つの transaction にまとめる。

## event 状態と schema 契約

### 状態名

既存の値を維持し、次の三値を追加する。

| `EventStatus` | DB 値 | 意味 | 次の遷移 |
| --- | --- | --- | --- |
| `Pending` | `pending` | 未 claim | `Claimed` |
| `Claimed` | `claimed` | scheduler が lease を保持しているが run binding は未完了 | `InFlight`、`Pending`、`RetryWait`、`DeadLetter` |
| `InFlight` | `in_flight` | `agent_runs` と `agent_run_events` への binding が同一 transaction で commit 済み。process の launch ack はまだない | `Dispatched`、`RetryWait`、`DeadLetter` |
| `Dispatched` | `dispatched` | child spawn、PID 記録、launch gate marker/ack、run の gate release、event status 更新が commit 済み | `Completed`、`RetryWait`、`DeadLetter` |
| `RetryWait` | `retry_wait` | 次の attempt の時刻を待つ | `Claimed` |
| `Completed` | `completed` | process が正常終了し、run/intervention/event の durable 更新が同一 transaction で成功 | terminal |
| `Failed` | `failed` | config、guardrail、project 不在など、agent run の実行結果ではない通常の処理失敗 | terminal |
| `DeadLetter` | `dead_letter` | agent failure/timeout/restart interruption が retry 上限を超えた terminal 状態 | terminal |

`Failed` は既存の scheduler pre-dispatch error（不正 config、guardrail halt、project 不在）に残す。agent process が起動後に失敗して上限を超えた場合は必ず `DeadLetter` とし、通常の failed 件数だけを見ても dead-letter を見落とさないようにする。

schema 13 の `events` 制約は次の値を受け入れる。

```sql
status TEXT NOT NULL CHECK (status IN (
    'pending', 'claimed', 'in_flight', 'dispatched',
    'completed', 'retry_wait', 'failed', 'dead_letter'
)),
attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
CHECK (
    (status = 'claimed' AND lease_until IS NOT NULL)
    OR (status <> 'claimed' AND lease_until IS NULL)
)
```

`in_flight` と `dispatched` の run binding は SQLite の CHECK の subquery ではなく、repository transaction の project-scoped validation で強制する。両状態の row は同じ `project_id` の `agent_run_events` と、同じ project の `agent_runs` に結び付いていなければ書き込めない。`events_project_status_idx` は維持し、`events_project_status_not_before_idx`（project、status、not_before、event_id）を追加して retry/dead-letter の bounded query を安定させる。

既存 DB の migration は schema version を 12 から 13 に上げる。外部キーを持つ `agent_run_events` を壊さないため、`migrate_events_to_v13` は既存の `events` CREATE SQL の status CHECK 文字列だけを `PRAGMA writable_schema` の厳密な全体一致で置換し、置換件数を 1 件と検証してから `PRAGMA user_version = 13` を commit する。既存 row の kind、payload、attempts、lease、timestamps は変更しない。置換後に `PRAGMA integrity_check` と `sqlite_master` の status SQL を確認し、状態値以外の SQL を変更しない。新規 DB の version 0 CREATE 文にも同じ制約を記載する。

### attempts と retry policy

`attempts` は `EventRepository::claim_batch` が `pending` または `retry_wait` から `claimed` にした回数であり、現在の処理を含む 1-based の attempt number である。`agent.max_retries` は初回以外に許される retry 回数とする。

| `max_retries` | failure at attempt 1 | failure at attempt 2 | failure at attempt 3 |
| ---: | --- | --- | --- |
| 0 | `dead_letter` | 該当なし | 該当なし |
| 1 | `retry_wait`（次が最後） | `dead_letter` | 該当なし |
| 2 | `retry_wait` | `retry_wait` | `dead_letter` |

従って failure 後に retry へ戻す条件は `attempts <= max_retries`、dead-letter 条件は `attempts > max_retries` である。成功は attempts に関係なく completed にする。

retry delay は既存 `retry_backoff_seconds` の契約を共通 `src/retry.rs` に移し、base 60 秒、`60 * 2^(attempts - 1)`、exponent 6（最大 64 分）で cap する。`i64` overflow は saturating にし、`not_before = now + delay` も saturating にする。`RetryPolicy { max_retries: u32 }` は project config から tick ごとに作り、backoff の base/cap はこの slice の固定値で設定項目にはしない。

## 二段階 ack と transaction 境界

状態の実際の順序は次の通り。

```text
pending/retry_wait
  -- claim_batch (lease + attempts += 1) --> claimed
  -- insert_with_events_and_reservation transaction --> in_flight
  -- child spawn + gate ack + acknowledge_dispatch transaction --> dispatched
  -- process exit + finish_and_resolve_events transaction --> completed
                                                               \-> retry_wait/dead_letter
```

### run binding（第一段階）

`AgentRunRepository::insert_with_events_and_reservation` は既存の run INSERT、`agent_run_events` INSERT、intervention reservation の binding に加え、全 event が同じ project の `claimed` であることを確認して `claimed → in_flight` を同一 `TransactionBehavior::Immediate` transaction で行う。run は `starting`、launch gate は `pending` のまま commit する。event lease はこの時点で NULL にする。event だけが claimed のまま、または run だけが作られる中間状態を commit しない。

この transaction より前（claim 後、run INSERT 前）に daemon が落ちた場合は、startup recovery が run binding のない `claimed` row を同じ project の retry policy で解決する。これにより lease expiry を待たずに再実行できる。

### launch gate（第二段階）

`AgentRunner::spawn` の順序は次のように固定する。

1. `in_flight` binding transaction の commit。
2. child を launch gate shell の下で spawn。
3. `mark_running_and_apply_interventions` transaction で PID、run=`running`、intervention=`applied` を commit。
4. `mark_gate_release_requested` を commit。
5. stdin に release byte を送り、gate shell が child spawn 後に一時 marker を作って atomic rename し、`released\n` を返すまで待つ。
6. `AgentRunRepository::acknowledge_dispatch(project_id, run_id)` を一つの transaction で実行し、`launch_gate_state='released'` と linked events の `in_flight → dispatched` を同時に commit。
7. その後だけ `AgentHandle` を scheduler に返す。

既存 marker は「child spawn と gate shell の marker commit まで進んだ」証拠であり、process の正常終了や event completed の証拠ではない。marker の ack を受けた後に SQLite 更新が失敗した場合も event は completed にしない。

### process 終了（最終 ack）

`AgentHandle` は `project_id`、`run_id`、`retry_policy`、timeout deadline、child、PID、および最初に観測した terminal outcome を保持する。`poll`、`wait`、`timeout_now` は child の outcome を `AgentRunRepository::finish_and_resolve_events` に渡す。同関数は project と run の所有関係、linked event の project、event status（`in_flight`/`dispatched`）を検証し、以下を一つの immediate transaction で行う。

- `Completed`/exit code 0: run を `completed`、全 linked event を `completed`、`completed_at=now`、lease NULL。
- `Failed`/non-zero または `TimedOut`: run を終端化し、intervention reservation を release し、各 event の `attempts` を個別に policy 判定して `retry_wait` または `dead_letter` にする。
- dead-letter になった event の `last_error` には bounded な terminal reason と attempt number を保存する。外部通知は送らない。

process の outcome を観測した後にこの transaction が SQLite error で失敗したら、transaction 全体を rollback し、run/event は `in_flight`/`dispatched` のまま残す。`AgentHandle` は terminal outcome を保持して次の `poll` で同じ finalizer を再試行する。daemon は DB 更新成功を確認するまで handle を active list から削除しない。

`fail_before_gate_release`（log open、spawn、PID/start transaction、gate ack の失敗）は同じ event resolver を使うが、run は `failed`、event は policy に従い retry_wait/dead_letter にする。`UpgradeInProgress` だけは既存どおり `defer_claimed` で attempts を消費せず pending に戻す。

## grouped events

同じ project の複数 event を一つの prompt/run にまとめる既存 coalescing は維持する。run の process outcome は一つなので、正常終了なら全 linked event を completed、failure/timeout なら全 linked event を同じ reason で個別解決する。各 event の `attempts` を独立に比較するため、たとえば attempts が 1 と 3 の batch は同じ transaction で前者が `retry_wait`、後者が `dead_letter` になり得る。primary event の attempts だけで batch 全体を判定しない。

retry された event だけが次の claim に入り、dead-letter event は再び claim されない。後続の prompt は retry event だけの batch または別の新規 event との batch になる。既存の durable submission/proposal/reconciliation の idempotency を再利用し、この slice では agent 自体の side effect の dedup protocol を追加しない。従って retry は agent を再実行し得ることを operator output に明記する。

## daemon startup recovery

backoff/dead-letter 判定には project の `agent.max_retries` が必要なので、repository に config path を読ませない。`Daemon::run_once` が startup recovery の最初に `ProjectRepository::list_all()` で全 project の config を読み、`BTreeMap<ProjectId, RetryPolicy>` を構築して `AgentRunRepository::recover_interrupted(now, reason, &policies)` に渡す。disabled/paused project の run も対象になるため `list_active` ではなく全 project を使う。

config のどれかが missing/invalid の場合、daemon は recovery transaction を開始せず error を返し、`startup_recovery_pending` を true のままにする。次の tick で再試行し、設定が読めるまで run/event を部分的に mutate しない。この責務分離により repository は project-scoped policy を受け取るだけで、古い config を推測して dead-letter することがない。

recovery は project ごとの immediate transaction とする。各 project について次を同時に行う。

- `release_requested` run の marker を read-only に調べ、marker があれば gate state を released に補正する。
- `starting`/`running` の run を restart interruption reason 付き `failed` にする。
- active run に linked な `in_flight`/`dispatched` event を retry_wait/dead_letter に解決する。
- run binding 前の `claimed` event（`agent_run_events` がないもの）も同じように解決する。
- intervention reservation を既存 recovery 規則で applied または pending に戻す。

marker が確認できても process が正常終了したとは扱わない。startup recovery は常に interruption failure として retry policy を適用する。recovery transaction が失敗した project はその project の rows を変更せず error を返し、次回 tick で再試行する。

## local observability

外部 delivery worker は作らず、SQLite の `events.status='dead_letter'` が唯一の永続 dead-letter source of truth になる。専用の重複 `dead_letters` table は追加しない。

- `events`: 既存 `--status` filter が `in-flight`、`dispatched`、`dead-letter` を受け入れる（SQLite の DB 値はそれぞれ `in_flight`、`dispatched`、`dead_letter`）。human/JSON の bounded event summary に status、attempts、`not_before`、error、project-scoped な latest `run_id` を表示する。payload や transcript は表示しない。
- `runs`: 既存の event/run lineage に event status と run status を表示する。新しい top-level lineage source は追加しない。retry は複数 run として見える。
- `status`: human、compact、JSON の event counts に `in_flight`、`dispatched`、`retry_wait`、`dead_letter` を追加する。既存 `failed` count はそのまま別表示する。
- `doctor`: read-only check `events.dead_letter`（件数 0 なら ok、1 以上なら warning と `events --status dead-letter` の remediation）と `events.ack_consistency`（in_flight/dispatched に同じ project の run link がない場合 error）を追加する。doctor は repair/retry/外部通知を実行しない。

bounded summary は既存 `bounded_redacted_text` を通し、event payload、prompt、log contents、credential-like text は追加で返さない。schema JSON version は既存の 1 を維持し、追加 fields は既存の optional/enum-compatible projection として扱う。

## 受け入れ条件

- spawn 成功直後の event が `completed` ではなく `dispatched` であり、process の正常終了と final transaction 後だけ `completed` になる。
- process non-zero、timeout、daemon restart interruption が event 単位で retry_wait になり、`max_retries=0` の最初の failure は dead_letter になる。
- retry delay が attempts に対して bounded exponential であり、not_before 前には claim されない。
- 同じ project の grouped events が attempts の違いにより一部 retry、一部 dead-letter になれる。
- launch gate の spawn 前、marker 後、SQLite ack 前、process exit 後の crash window が recovery 可能である。
- startup recovery は全 project config を policy map に解決できない限り mutation を始めず、project transaction を跨いで別 project の event/run を参照しない。
- `events`、`runs`、`status`、`doctor` だけで dead-letter と ack 状態を bounded に確認できる。
- 外部 Slack/webhook、batch lineage、budget、goal state、token accounting の変更がない。
- `cargo fmt --all -- --check`、`cargo test --all-targets`、`git diff --check` が成功する。
