# Zero-adapter 継続 ML campaign 設計書

日付: 2026-08-17
ステータス: 会話上承認済み・文書レビュー待ち
対象: Linux 上の `pueue-agent`

## 0. 入力資料と優先順位

この設計は、利用テスト後に作成された次の2文書を問題分析と要件の入力にしている。

- `docs/report/continuous-ml-campaign-implementation-summary-ja.md`
- `docs/report/pueueAgent_continuous_ml_experiment_campaign_spec.md`

特に、service-owned hard policy、proposal と execution の分離、atomic reservation、
`unreconciled`、finite failure lineage、idle watchdog、isolated worktree、bounded durable state
を引き継ぐ。ただし report は現状分析と先行案であり、この文書や現在の source code より
強い規範ではない。implementation summary に記載された変更も、現在の branch に存在する
ことをこの設計だけで保証しない。

report と、その後の利用者との合意が衝突する場合は、次の合意を優先する。

- objective の必須入力は `.pueue-agent/STATE.md` とする。
- network は既定許可とする。ただし credential 継承は既定拒否のまま分離する。
- result manifest は初回から必須にせず、discovery 後の任意補助契約とする。
- code change は隔離 worktree、test、candidate commit を条件に自動提案・実行できる。
- goal 達成時は自動 retire/merge せず `goal_reached_pending_review` で止める。
- Pueue task の terminal result だけでなく、running 中の health と periodic observer を
  campaign lifecycle に含める。

## 1. 目的

任意の ML 学習リポジトリで、利用者がリポジトリ固有の controller や adapter を
用意しなくても、次の操作だけで目標指向の実験ループを開始できるようにする。

```bash
pueue-agent init
# .pueue-agent/STATE.md に目標と制約を書く
pueue-agent enable
pueue-agent submit -- python train.py
```

最初の `submit` は単発タスクではなく、managed campaign と baseline experiment を
作る。以後は `pueue-agent` が結果を観測し、エージェントに診断・次の仮説・実験案を
作らせ、安全性と予算を supervisor が検証して Pueue へ投入する。

利用者が初期入力として必須なのは、`STATE.md` の目標・制約と最初のコマンドだけで
ある。メトリクス形式、結果 manifest、学習フレームワーク別 adapter は開始条件に
しない。

## 2. 成功条件

この機能は、次をすべて満たしたときに成功とする。

- 一般的な ML リポジトリを、専用 glue code なしで campaign 化できる。
- baseline の完了後、エージェントがリポジトリ、ログ、成果物を調査し、次の実験を
  提案できる。
- supervisor だけが hard policy、予算、ID、冪等性、lineage、Pueue 投入を所有する。
- daemon の再起動や Pueue 応答の曖昧さがあっても、実験を重複投入しない。
- Pueue が `Running` でも、OOM、NaN、worker 消失、進捗停止などを検出・診断できる。
- 実行中 experiment ごとに観測セッションを引き継ぎ、既定約30分間隔で継続判断を
  行える。
- 目標達成の証拠を得たら自動 merge や自動 retire はせず、人のレビュー待ちで停止する。
- エージェントはコードを隔離 worktree 内で変更・検証・commit できるが、main へは
  merge できない。
- ネットワークは既定で利用可能だが、credential の継承は明示許可なしでは行わない。

## 3. 対象外

初期実装では次を扱わない。

- Optuna、Ray Tune 等に相当する汎用 HPO engine
- container、cluster、複数ホスト、GPU quota の統合管理
- 一つの project 内で同時に複数の active campaign を動かすこと
- Web UI、外部 dashboard、PR の自動作成・自動 merge
- 任意のログ形式を100%正確に理解する保証
- macOS における Linux と同等の agent/private-temp 実行対応
- agent が hard policy、目的、予算を変更する仕組み

## 4. 採用する方式

### Agent discovery + supervisor control

エージェントは未知のリポジトリを調査し、構造化された proposal を返す。supervisor は
proposal を検証し、予算を予約し、冪等に永続化してから Pueue へ実行を依頼する。

```text
STATE.md + initial command
          |
          v
campaign supervisor ---- SQLite (canonical state)
          |                         ^
          | bounded context         | observations/results
          v                         |
analysis / observer agent           |
          | structured proposal     |
          v                         |
validation + reservation ----------+
          |
          v
verified Pueue execution
```

この分離により、未知の ML コードを理解する柔軟性はエージェントに持たせつつ、重複投入、
予算超過、目的の書き換え、無制限 retry を supervisor 側で防ぐ。

### 採用しない方式

- エージェントから Pueue を直接操作する方式: restart safety、予算、冪等性を一貫して
  保証できない。
- 全リポジトリに厳密な metric/result protocol を先に要求する方式: adapter 不要という
  目的に反する。result manifest は discovery 後に任意で導入できる補助契約とする。

## 5. 信頼境界と source of truth

### `STATE.md`

人が campaign の目標、成功条件、禁止事項、制約を書く。最初の `submit` 時に内容を
検証し、bounded な正規化 snapshot と digest を SQLite に保存する。active campaign の
目的はこの snapshot で固定され、その後ファイルを編集しても現在の campaign には
反映しない。新しい目標は現在の campaign を retire した後、次の最初の `submit` で
snapshot 化する。

未記入の初期文面、実質的に空の目標、上限超過、制御文字を含む入力は、campaign や
Pueue task を作る前に actionable error として拒否する。`STATE.md` へ secret を書くことは
禁止し、CLI 文書と生成テンプレートにも明記する。

### Service policy

管理者が変更できる hard policy と既定値を所有する。

- network の許可・禁止。既定は許可。
- credential/environment inheritance の allowlist。既定は credential 不許可。
- 同時 experiment 数
- 24時間あたりの新規 experiment 数
- 1時間あたりの agent run 数
- 24時間あたりの code-change proposal 数
- 同一 spec の retry 回数
- failure fingerprint ごとの repair 回数
- observer の既定間隔と bounded input/output

network access と credential access は別の権限である。network が許可されていても、
ambient token、cloud credential、SSH agent、認証環境変数を自動継承しない。

### SQLite

campaign、proposal、experiment、budget reservation、observation、failure、lineage、
agent session mapping の canonical state を所有する。エージェントの会話履歴や
`state.json` は canonical state にしない。

既存 `state.json` schema v2 から agent-writable hard budget を除く。移行時に古い値を
service policy へ昇格させず、安全な service default を適用する。agent が出力に予算や
目的を含めても authority として扱わない。

## 6. Domain model

### Campaign

一つの registered project は最大一つの active campaign を持つ。
ここで live campaign は `retired` 以外の campaign を指す。`paused`、`budget_waiting`、
`goal_reached_pending_review`、`degraded`、`halted` も live であり、通常の `submit` で
別 campaign を重ねて作ることはできない。

主なフィールド:

- campaign ID、project ID
- objective snapshot、objective digest
- initial command
- state、state reason、created/updated time
- baseline experiment、best experiment
- current strategy level
- rolling budget counters と next eligible time

状態:

| 状態 | 意味 |
| --- | --- |
| `active` | 観測、分析、次の実験投入が可能 |
| `budget_waiting` | rolling limit の回復時刻まで待機 |
| `goal_reached_pending_review` | 達成証拠を提示し、人の判断待ち |
| `paused` | 人が一時停止。観測・新規投入を抑止 |
| `degraded` | 回復可能だが自律進行に必要な一部が失敗 |
| `halted` | 安全に継続できず、人の介入が必要 |
| `retired` | 終了済み。新しい目標で次 campaign を開始可能 |

通常の experiment failure や plateau だけでは campaign を `halted` にしない。

### Proposal

エージェントが返す最小の構造化出力:

- `kind`: experiment、repair、broader-search、recipe、code-change、data/evaluation
- hypothesis
- source experiment
- argv
- working directory
- expected evidence
- goal claim と、その根拠への参照（該当時のみ）

supervisor が proposal ID、canonical digest、parent lineage、retry index、budget slot、
execution identity を付与する。自由形式の説明は保存できるが、実行 authority にしない。

### Experiment

proposal から承認・予約された一回の検証単位。Pueue task ID、argv digest、source commit、
worktree、parent experiment、attempt、result state、health state、failure fingerprint、
evidence references を保持する。

Pueue の task state と experiment health state は独立させる。`Running` は process wrapper が
存在することを示すだけで、学習が正常とは限らない。

### Observation session

一つの running experiment に一つだけ対応する。experiment をまたいで再利用しない。

- agent session identity
- last observation time
- next observation time
- bounded summary と context digest
- last verdict
- consecutive stagnation count
- consecutive abnormal count

session が失われた場合は、SQLite の bounded history から新しい session を開始する。

## 7. 最初の `submit`

live campaign がない registered project で通常の `submit` を行うと、次の順で処理する。

1. project、policy、`STATE.md`、initial argv を side effect 前に検証する。
2. objective snapshot と initial-command digest を確定する。
3. global/project admission lock を取得する。
4. SQLite transaction で campaign、baseline proposal、baseline experiment、rolling budget
   reservation、submission intent を一度だけ作る。
5. transaction commit 後に verified Pueue add を行う。
6. task ID を得たら intent と experiment に対応付ける。
7. callback または reconciliation で実行状態を追跡する。

SQLite と Pueue に跨る単一 transaction は作れない。Pueue add の応答が曖昧な場合は
`unreconciled` とし、同じ command を自動再投入しない。Pueue 側で canonical digest と
一意に対応する task を確認できた場合だけ adopt する。複数候補または証拠不足なら
人の確認を要求する。

active campaign 中の通常 `submit` と `submit-batch` は、人・エージェントのどちらからも
拒否する。目標や制約への追加指示は既存の `steer` を使う。

## 8. 自律ループ

```text
experiment terminal or watchdog due
  -> bounded evidence collection
  -> analysis agent (resume when applicable)
  -> structured proposal / goal claim / no decision
  -> supervisor validation
  -> rolling budget reservation
  -> experiment intent commit
  -> verified Pueue add
  -> running health + periodic observation
```

### Evidence discovery

初期 experiment では manifest を要求しない。supervisor は bounded な範囲で次を収集する。

- Pueue state と exit status
- stdout/stderr の末尾と前回からの bounded delta
- metric、checkpoint、artifact らしいファイルの名前、更新時刻、bounded summary
- Git diff、commit、テスト結果
- process/worker の状態
- 過去の experiment、best evidence、failure fingerprint

エージェントはこれを使ってリポジトリを調査し、metric や artifact の位置を学ぶ。
発見後、candidate worktree に任意の bounded result manifest や heartbeat 出力を追加しても
よい。ただし manifest の有無だけで experiment の成功・失敗を決めない。

### Decision missing と idle watchdog

agent が正常終了しても有効な proposal、goal claim、明示的な wait decision のいずれも
返さなければ `decision_missing` とする。bounded retry 後も判断がなければ campaign を
`degraded` にして診断可能にする。

`active` なのに running experiment、pending proposal、budget wait、scheduled observation の
いずれもない状態を watchdog が検出し、idle event を作る。黙って停止しない。

### Plateau

改善が止まっても campaign は終了しない。strategy level を次の順で広げる。

1. local parameter tuning
2. broader parameter/model search
3. training recipe の変更
4. code change
5. data/evaluation の見直し

同じ仮説や同じ failure fingerprint を無限に繰り返さず、lineage を quarantine して別の
strategy へ移る。

## 9. Running task の health 判定

### 入力 signal

- OOM、traceback、panic、assertion、NaN/Inf 等の bounded log pattern
- log、metric、checkpoint、artifact の更新停止
- leader、worker、descendant process の状態
- wrapper は生存しているが worker が終了した状態
- discovery 済み heartbeat/result evidence
- 経過時間と、それまでの進捗傾向

一回の log 停止や一個の metric 欠損だけで異常確定しない。長い preprocessing、compile、
dataset preparation を誤停止しないため、複数 observation、過去傾向、強い failure signal を
組み合わせる。

### 二段階判定

1. supervisor の cheap check が `unhealthy_suspected` を作る。
2. diagnosis agent が bounded context を調べ、`healthy`、`uncertain`、
   `confirmed_unhealthy` を返す。

`healthy` は通常観測へ戻す。`uncertain` は backoff して再観測する。
`confirmed_unhealthy` は task identity と current state を再検証してから cancel する。
Pueue が terminal になったことを確認するまで replacement/repair experiment を投入しない。
cancel 結果が曖昧なら `termination_unknown` とし、自動 replacement を禁止する。

OOM、NaN、code error は failure fingerprint を作り、有限回の repair proposal を許可する。
同じ fingerprint が上限に達したらその lineage を quarantine し、別戦略へ進む。

## 10. Periodic observer

running experiment は、既定約30分間隔で observer agent に評価させる。間隔は service
policy が所有し、agent 自身は変更できない。

初回は新しい session を作り、同じ experiment の次回観測ではその session を resume する。
次の experiment は別 session を使う。観測間隔は前回観測の開始時刻ではなく完了時刻から
計算し、前回が実行中なら重複起動せず一回に coalesce する。

各観測に渡す context:

- Pueue state
- bounded log delta
- metric/checkpoint/artifact delta
- elapsed time と推定 progress
- process health
- 前回 verdict と bounded summary
- campaign objective と current best evidence

observer verdict:

| Verdict | 動作 |
| --- | --- |
| `continue` | 次回観測を予約して継続 |
| `investigate` | 短い backoff で追加診断 |
| `stop_and_repair` | identity 再検証、cancel、terminal 確認後に repair 提案 |
| `early_stop_no_improvement` | 複数観測または強い収束証拠を要求して停止 |
| `goal_evidence` | 証拠を検証し review 待ち候補を作る |

observer run も rolling agent budget に計上する。session memory は判断補助であり、
SQLite の state と evidence を上書きしない。

## 11. Goal 達成と人の判断

agent が goal 達成を主張するときは、baseline/current best との比較、experiment、metric、
artifact、test、candidate commit への bounded reference を必須にする。supervisor が参照先と
campaign lineage を検証した後、campaign を `goal_reached_pending_review` に移す。

この状態では新規 experiment と periodic observer を停止し、次を表示する。

- goal claim と根拠
- baseline と best experiment の比較
- 再現コマンドと主要 artifact
- candidate commit/worktree（存在する場合）
- 未解決の risk

人は `campaign retire` で終了するか、`campaign resume` で追加検証を続ける。自動的に
main へ merge、campaign retire、成果物公開は行わない。

## 12. Code change proposal

code change が必要な場合、supervisor は campaign/experiment 専用の隔離 worktree を作る。
エージェントはその中だけを変更し、指定された test と repository discovery で見つけた
関連 test を実行する。成功した candidate は commit し、その SHA を experiment に固定する。

次を禁止する。

- 元の working tree または main branch の直接変更
- main への自動 merge/rebase/push
- 未commit変更を別 experiment が再利用すること
- test failure を無視した candidate の通常 experiment 投入

失敗した worktree も診断に必要な bounded metadata を残し、cleanup は既存の安全な
descriptor-owned lifecycle に従う。

## 13. Rolling budget と予約

hard limit は累積 lifetime cap ではなく rolling window とする。安全側の初期既定値は次とし、
service operator だけが変更できる。

| Limit | 初期既定値 |
| --- | ---: |
| parallel experiments | 1 |
| new experiments / 24h | 24 |
| agent runs / hour | 6 |
| code-change proposals / 24h | 10 |
| same-spec retries | 2 |
| repairs / failure fingerprint | 2 |
| proposals accepted per decision cycle | 1 |

observer interval の初期既定値は30分とする。GPU 時間は portable に検証できる backend が
まだないため、初期 hard budget には含めない。GPU quota/accounting を導入するときは別の
承認済み設計と fail-closed な計測境界を必要とする。

実行可能性確認と budget reservation は、proposal の canonical digest と lineage を確定した
後、experiment intent 作成と同じ SQLite transaction で行う。予約済み slot は一度だけ
consume/release される。

limit 到達時は campaign を `budget_waiting` にし、最も早い回復時刻を保存する。時刻到来時に
daemon が自動で `active` に戻す。budget 到達を campaign の永久停止として扱わない。

## 14. Restart、冪等性、障害処理

### Idempotency key

proposal の canonical form、objective digest、source experiment、source commit、attempt、
strategy level から supervisor が digest を作る。同じ digest の active/terminal experiment が
あれば再作成しない。

### Restart recovery

daemon 起動時に SQLite から次を再構築する。

- active/budget-waiting/degraded campaign
- pending proposal と reservation
- Pueue task と未解決 submission intent
- running experiment と health state
- due observation と session mapping
- cancellation/termination confirmation 待ち

外部状態を観測してから一方向に state を進める。同じ callback、observation、proposal を
複数回受けても結果が変わらない repository API を使う。

### Error policy

- transient Pueue/read error: bounded retry と backoff
- Pueue add ambiguous: `unreconciled`、自動再投入禁止
- same-spec failure: finite retry
- repeated fingerprint: lineage quarantine
- invalid agent output: `decision_missing` と bounded retry
- observer session loss: SQLite historyから fresh session
- database/policy invariant failure: fail closed、campaign `degraded` または `halted`
- cancellation ambiguity: `termination_unknown`、replacement 禁止

error message と diagnostics は bounded projection のみを出し、prompt、credential、環境変数値、
raw transcript を表示しない。

terminal history は policy に従って archive できるが、idempotency、lineage、
failure fingerprint に必要な digest/tombstone は保持する。CLI の履歴 query は必ず
project/campaign scope、index、limit、cursor pagination を使い、campaign が長期間続いても
一回の query や agent context が全履歴を読み込まないようにする。

## 15. CLI

### 既存 command の変更

- `submit -- <command>`: live campaign がなければ campaign と baseline を自動作成する。
- `submit`, `submit-batch`: live campaign 中は拒否し、`steer` を案内する。
- `status`: campaign state、budget wait、running health、next observation、best experiment を表示。
- `doctor`: campaign liveness、unreconciled submission、observer overdue、termination unknown、
  policy/budget invariant を read-only で診断する。
- `steer`: active campaign の追加指示を event として記録するが、objective snapshot や hard
  policy を変更しない。

### 新規 command

```text
pueue-agent campaign status
pueue-agent campaign pause
pueue-agent campaign resume
pueue-agent campaign retire

pueue-agent proposal list
pueue-agent proposal inspect <proposal-id>

pueue-agent experiment list
pueue-agent experiment inspect <experiment-id>
```

`campaign resume` は goal review、pause、回復可能な degraded state からの再開に使う。
`campaign retire` は running/cancellation-unknown experiment がないことを再検証する。

## 16. Compatibility と migration

- schema migration は既存 project/event/run/submission を保持する。
- 既存の active campaign がない project では、valid な `STATE.md` と最初の `submit` から
  campaign を開始できる。
- v1 `state.json` の agent-writable budget は authority として移行しない。
- active campaign が存在する状態で upgrade しても、restart recovery が proposal、task、
  observer を重複作成しない。
- current campaign の objective snapshot と on-disk `STATE.md` が異なる場合、status で差分の
  存在だけを表示し、自動更新しない。
- Linux を正式対象とし、macOS では既知の private-temp transport 制約が解消するまで
  campaign execution 対応を主張しない。

## 17. 実装フェーズ

### Phase 1: Campaign safety core

- service-owned hard policy と state schema v2
- campaign/proposal/experiment/reservation schema
- 最初の submit による campaign/baseline 作成
- live campaign 中の direct submit 拒否
- proposal digest と atomic rolling reservation
- Pueue add intent、`unreconciled`、restart reconciliation
- campaign/proposal/experiment の最小 CLI と diagnostics

### Phase 2: Autonomous completion loop

- terminal experiment の bounded evidence collection
- analysis agent の structured proposal
- coordinator による検証・予約・投入
- `decision_missing`、idle watchdog、budget wake
- restart 後の session/proposal/task 再構築

### Phase 3: Running health と periodic observer

- Pueue state と health state の分離
- OOM/NaN/worker-loss/staleness signal
- suspicion → diagnosis → confirmed action
- experiment 単位の resumable observer session
- cancel/terminal confirmation と finite repair

### Phase 4: Evaluation と goal review

- baseline/current-best evidence 比較
- optional result manifest discovery
- plateau strategy escalation
- `goal_reached_pending_review` と review CLI

### Phase 5: Isolated code changes

- campaign worktree lifecycle
- agent edit/test/commit
- candidate SHA と experiment lineage
- main 非変更、不正 candidate の隔離

各 phase は単独で migration、restart、failure-injection、Linux real-Pueue acceptance を通してから
次へ進む。後続 phase のためだけの未使用 abstraction は先行導入しない。

## 18. 検証方針

### Unit / repository tests

- campaign state transition と invalid transition
- objective snapshot の immutability
- proposal canonicalization と duplicate suppression
- rolling budget の予約、解放、時刻回復
- same-spec retry と fingerprint quarantine
- observation session の一意性と coalescing
- goal claim evidence validation
- cancellation/termination unknown invariant

### Integration tests

- 最初の submit が campaign と baseline を一度だけ作る
- active campaign への direct submit/batch を拒否する
- baseline 完了から次 proposal/experiment を作る
- daemon restart の各境界で duplicate を作らない
- budget waiting が回復時刻後に自動再開する
- agent が proposal を返さない場合に silent idle にならない
- OOM、NaN、code failure から有限 repair を行う
- Pueue `Running` かつ worker dead を異常判定する
- 長い preprocessing を一回の stale signal で cancel しない
- cancel terminal 確認前に replacement を作らない
- observer が同一 experiment で session を resume し、次 experiment では新規 session を使う
- session 消失後に SQLite history から安全に再開する
- code change が隔離 worktree だけを変更し、test 後に candidate commit を固定する
- goal evidence で review 待ちになり、自動 retire/merge しない
- network 利用は既定で可能だが credential は継承されない
- agent 出力で objective/hard budget を変更できない

### Mandatory Linux real-Pueue acceptance

専用 adapter を持たない小さな ML fixture を使い、goal と initial command だけから次を実行する。

1. baseline を投入・完了する。
2. agent がログと artifact を発見し、第二 experiment を提案する。
3. restart を submission、running、observation、terminal の各境界で行う。
4. duplicate experiment がなく、idle watchdog と budget wake が動くことを確認する。
5. OOM、wrapper-only running、NaN、stagnation、healthy preprocessing を区別する。
6. observer context が同 experiment 内で継続し、experiment 間では分離されることを確認する。
7. repair、code change、candidate commit、goal review pause までを確認する。
8. network 到達性と credential 非継承を別々に検証する。

テストは fake Pueue だけで完了扱いにしない。SQLite transaction と外部 Pueue side effect の
間、cancel と terminal confirmation の間、agent result と proposal commit の間に failpoint を
置き、再起動後の一意性を確認する。

## 19. 運用上の可視性

`status` と `doctor` は最低限、次を bounded に表示する。

- campaign state/reason と objective digest
- running/pending/terminal experiment 数
- current best と baseline
- rolling budget の使用量と次の回復時刻
- latest observer verdict と next observation
- suspected/confirmed health issue
- unreconciled submission、termination unknown、decision missing
- quarantined lineage/fingerprint
- goal review evidence の参照

人が「停止中なのか、予算待ちなのか、実験中なのか、agent の判断待ちなのか」を一つの
画面で区別できることを liveness の受け入れ条件にする。

## 20. 設計上の不変条件

1. agent は Pueue、hard budget、objective、ID を直接変更しない。
2. SQLite commit と外部 side effect の間には durable intent を置く。
3. 曖昧な add/cancel は成功とみなさず、重複 add/replacement をしない。
4. running experiment ごとの observer は最大一つである。
5. task terminal を確認する前に replacement を開始しない。
6. goal claim は evidence reference なしでは campaign state を変えない。
7. code change は隔離 worktree と candidate commit に閉じ、main を変更しない。
8. network 許可は credential 継承を意味しない。
9. on-disk `STATE.md` の変更は active objective snapshot を暗黙更新しない。
10. ordinary failure、plateau、budget exhaustion は永久停止ではなく、有限 retry、strategy
    shift、rolling wait のいずれかへ進む。
11. 一つの campaign で同時に実行可能な analysis/decision cycle は一つだけである。
12. repair/replacement の source experiment は terminal proof を持たなければならない。
13. 長期履歴を agent context や CLI output へ無制限に展開しない。
