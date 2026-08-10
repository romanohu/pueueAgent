# P0〜P3 運用強化 設計書

## 目的

外部エージェントによる実験運用レポートを、`pueue-agent` 本体の改善仕様として記録する。現在の event 駆動・SQLite 永続化・fresh/resume agent 起動という基盤は維持し、利用者側の wrapper や長い `instructions.md` に委ねている安全運用を本体へ段階的に移す。

## 背景と観測された問題

外部利用では、Pueue task の完了を起点に fresh agent を起動し、`STATE.md` を介して次の実験へ進む基本ループは実用になった。一方、次の問題が確認された。

1. `status` が SQLite の writable 接続を使うため、読み取り確認に DB 書き込み権限が必要になる。
2. `max_experiments` が bootstrap/control task まで数える。
3. callback を発生させるための `/usr/bin/true` のような control task が Pueue 履歴へ混ざる。
4. batch 投入の lock、重複確認、厳格な task ID 解析、曖昧応答時の停止が利用者側 wrapper に分散している。
5. event、agent run、次に投入された task を確認するため、複数の CLI、raw Pueue JSON、SQLite、agent log を往復する必要がある。
6. task の variant、mode、seed、stage などを command 文字列から再解析している。
7. 自由記述の `STATE.md` に古い事実と現在の事実が共存し、fresh agent が矛盾した指示を読む可能性がある。
8. raw Pueue status や診断出力が大きく、環境変数や command の漏洩と prompt 消費が懸念される。

## 現行コードとの差分

- `Db::open_read_only` と `doctor` の read-only 経路は既に存在する。
- ただし通常の `status` はまだ writable な `resolve_project` を使っているため、P0 は未完了である。
- `status --json`、`events`、`inspect`、`explain`、`doctor` は既に存在する。
- detector の `wake` action は存在するが、operator が直接 event を作る `wake` CLI は存在しない。
- `max_experiments` は現状すべての accepted/started submission を数える。
- `runs`、`runs --follow`、submission metadata、batch submission、STATE 整合性検査は未実装である。

## 受け入れた互換性方針

- `pueue-agent submit` の既存呼び出しで `--kind` を省略した場合は `experiment` と扱う。
- `control` は `--kind control` と明示した submission だけに適用する。
- 既存の SQLite database は migration で読み取り可能なまま維持する。
- 既存の `status`、`events`、`inspect`、`explain`、`doctor` の text/JSON 出力を破壊しない。新しい field は versioned JSON として追加する。
- Pueue の外部操作と SQLite transaction を単一の atomic transaction と偽装しない。batch は durable state machine と lease で idempotency を実現する。

## 全体アーキテクチャ

```text
operator / agent
  ├─ submit [kind + metadata] ──> SQLite submission intent ──> pueue add
  ├─ wake [reason] ─────────────> SQLite operator event
  └─ submit-batch [request-id] ─> SQLite batch state machine ─> pueue add...

SQLite source of truth
  ├─ events ──> scheduler ──> agent runs
  ├─ submissions ──> task metadata / lineage / guardrails
  ├─ incidents / termination requests
  └─ batch requests / STATE diagnostics
```

各フェーズは個別の migration、CLI contract、回帰テストを持ち、P0 から順に main へ統合できるようにする。

## P0: read-only status と診断 redaction

### SQLite

読み取り系 CLI は `Db::open_read_only` を使う。read-only 接続では `journal_mode=WAL` の変更を行わず、既存の WAL を SQLite の通常の read-only 動作で読む。`immutable=1` は WAL を無視して古い snapshot を読む危険があるため採用しない。

`foreign_keys=ON`、busy timeout、read-only flag は維持する。読み取り系コードから migration、directory creation、WAL pragma が呼ばれないことをテストする。

### CLI と redaction

- `status` と `status --json` は read-only DB 接続を使う。
- `status --compact` は daemon/project state、active task 数、pending event 数、active agent 数、guardrail の要約だけを出す。
- Pueue task summary は project group と task ID を基本単位にし、command/env/payload は既定では要約・redaction する。
- credential らしい環境変数名は既定で値を `[REDACTED]` に置換する。
- 既存の bounded limit を維持し、prompt、transcript、payload 本文を診断出力へ流さない。

### 受け入れ条件

- `status` を DB ファイル・親ディレクトリが read-only の sandbox から実行できる。
- `status --json` の JSON schema と既存 field が維持される。
- `status --compact` が raw Pueue JSON を出力しない。
- credential らしい環境変数が診断 text/JSON に現れない。

## P1: submission kind と構造化 metadata

### Model と database

submission に次の分類を追加する。

- `experiment`: guardrail の `max_experiments` 対象。
- `control`: bootstrap、wake 用の互換 control task など。`max_experiments` 対象外。

submission に bounded な JSON metadata を保存する。metadata は object に限定し、サイズ、深さ、文字列長、キー数、許可される JSON value を検証する。最低限、次のような利用を想定する。

```json
{
  "variant": 2,
  "stage": "smoke",
  "mode": "learned",
  "seed": 11,
  "config_digest": "...",
  "role": "experiment"
}
```

`role` は submission kind と矛盾させない。CLI の `--kind` を省略した場合は `experiment`、`--kind control` では `control` を保存する。既存 submission は `experiment` として migration する。

### CLI

```bash
pueue-agent submit --kind experiment --metadata metadata.json -- python train.py
pueue-agent submit --kind control -- /usr/bin/true
```

metadata はファイル入力と bounded inline JSON のどちらか一方を提供し、Pueue command には混ぜず SQLite submission と diagnostics に保存する。`status --json`、`inspect`、`events` の summary では metadata 全文ではなく bounded な projection と digest を返す。

### 受け入れ条件

- 既存の `submit -- command` が `experiment` として動作する。
- control submission が `max_experiments` を消費しない。
- task ID が再利用されても metadata と submission の lineage が混ざらない。
- 不正、過大、深すぎる metadata を reject し、Pueue add を実行しない。

## P2: operator wake と runs/lineage observability

### wake

`pueue-agent wake --reason <TEXT>` は Pueue task を作らず、project-scoped の operator event を SQLite に保存する。pause 中は event を pending として保持し、resume 後に処理する。daemon の次の scheduler tick で agent を起動する既存 event flow を再利用する。

`wake` の reason は bounded text とし、prompt に渡る evidence として redaction/limit を通す。operator event は task completion と区別できる EventKind を持つ。

### runs と lineage

以下を追加する。

```bash
pueue-agent runs --json
pueue-agent runs --follow
pueue-agent status --compact
```

`runs` は event、agent run、intervention、agent が登録した submission の関係を project-scoped に表示する。`--follow` は SQLite を短い間隔で read-only polling し、agent process や Pueue を操作しない。JSON は bounded な summary とし、prompt/transcript/log 本文は出さない。

### 受け入れ条件

- `/usr/bin/true` を callback 起動用に投入せず、operator wake から agent を起動できる。
- `runs --json` で primary event、agent run、関連 submission を追跡できる。
- `runs --follow` が read-only で、停止可能で、同じ event を重複表示しない。
- 既存 diagnostics と schema version を維持する。

## CLI 出力デザイン

人間が terminal で見る text output は、Pueue の一覧性を参考にしつつ、`pueue-agent` 独自の責務が分かる表示にする。Pueue の task state と agent supervisor の event/run state を同じものとして表示しない。

### 共通ルール

- TTY では短い見出し、状態記号、揃えた列、末尾の要約を使う。
- パイプや redirect の場合は ANSI color と装飾を出さない。
- `NO_COLOR` を尊重する。
- JSON は装飾せず、既存の `schema_version` と field を維持する。
- 状態表示には `PA` または `pueue-agent` の見出しを付け、Pueue の raw status と混同しない。
- task ID には `task`、event ID には `event`、agent run ID には `run` の prefix を付ける。
- 表示する command、reason、metadata、environment は既定で bounded/redacted にする。

### status の例

```text
pueue-agent  project=vision-lab  healthy

  daemon       running
  pueue group  pueue-agent-vision-lab-a1b2
  experiments  running=1  queued=2  accepted=7/20
  agent runs   active=1  failed=0  limit=100
  events       pending=1  failed=0  incidents=open=0
  interventions pending=2  applied=8

  task 42  RUN  train.py --lr 0.001
  event 18  WAIT  task_finished  completion

summary: 1 active task, 1 pending event, 1 active agent
```

`status --compact` は上記のうち daemon、task、event、agent、guardrail の要約だけを一画面に収める。詳細な command や reason は出さない。

### events の例

```text
pueue-agent events  project=vision-lab  showing=3

EVENT    AGE   KIND          STATE  DISPATCH   DETAIL
event 18  12s   task_finished WAIT   completion task=42
event 17  2m    crash         DONE   crash      task=41
event 16  4m    auto_killed   DONE   crash      task=40

3 events shown; 0 more hidden
```

### submit / wake の例

```text
pueue-agent submit
  submission  sub-7c2d
  task        task=42
  kind        experiment
  group       pueue-agent-vision-lab-a1b2
  state       accepted
```

```text
pueue-agent wake
  event       event=19
  reason      start variant-2 analysis
  state       pending
```

script で安定した値を読む場合は `--json` を使う。人間向け text の列や装飾は互換性の対象にしないが、失敗時の exit code と JSON schema は安定させる。

### runs の例

```text
pueue-agent runs  project=vision-lab  showing=2

RUN       EVENT     MODE        STATE   STARTED  RESULT
run=31    event=18  completion  RUN     12s ago  waiting for agent
run=30    event=17  crash       DONE    2m ago   submitted task=42

lineage: event=18 -> run=31 -> pending submission
```

`runs --follow` は新しい event/run/submission の関係だけを差分表示する。Pueue の raw status を再描画するのではなく、supervisor の因果関係を表示する。

### CLI 出力の受け入れ条件

- TTY と pipe の両方で読める。
- Pueue の raw state と `pueue-agent` の event/run state が混同されない。
- 長い command、environment、prompt、log 本文が既定出力を膨らませない。
- JSON を選んだ場合は text header、ANSI、説明文を混ぜない。
- status、events、submit、wake、runs の各コマンドに同じ ID prefix、状態記号、redaction 規則を使う。

## P3: idempotent batch submission と STATE 整合性

### Batch submission

```bash
pueue-agent submit-batch \
  --request-id <uuid> \
  --manifest jobs.json
```

request ID を一意キーとする durable batch request を SQLite に保存する。manifest の各 job に stable job ID、kind、argv、metadata を持たせる。

状態は少なくとも `pending`、`dispatching`、`accepted`、`failed`、`partial`、`completed` を持つ。Pueue add の前後を記録し、同じ request ID の再実行は新しい task を作らず、現在の accepted task IDs と未処理 job を返す。途中失敗では、accepted/failed/pending を job 単位で JSON 出力する。

Pueue の group pause/resume を扱う場合も、SQLite state と external action の結果を別々に記録し、再起動時に再照合できるようにする。「pause → 検証 → resume」を DB transaction と表現しない。

### STATE 整合性

既存 `STATE.md` の自由記述は保持する。その上に、機械可読な bounded section または sidecar state を追加し、次の事実を canonical とする。

- `current_facts`
- `historical_facts`
- `next_action`
- `budgets`
- `active_lineage`

`doctor` または専用 state check は、古い `campaign stopped` と現在の active lineage のような矛盾を warning として表示する。agent prompt はまず canonical section を参照し、自由記述は補足情報として扱う。

### 受け入れ条件

- 同じ request ID の batch 再実行が重複 task を作らない。
- 部分成功時に accepted task IDs と未処理 job を機械可読に返す。
- Pueue/status/DB の不一致を再起動後に再照合できる。
- STATE の矛盾が warning として検出され、agent が停止すべき場合は安全側に倒れる。

## エラー処理と安全性

- 外部 Pueue 操作は SQLite の intent、lease、result と分離して永続化する。
- 曖昧な task ID、未知の group、metadata 不正、redaction 失敗は fail closed とする。
- 既存 guardrail、pause/halt、agent one-slot、retry 上限を新機能から迂回しない。
- `pueue-agent submit` を迂回する raw `pueue add` は、既存どおり監視対象の正式経路にしない。

## テスト方針

各フェーズで failing test を先に追加し、次に最小実装を行う。

- P0: read-only DB、status JSON/compact、環境変数 redaction、TTY/pipe output、既存 diagnostics regression
- P1: kind default、control guardrail exclusion、metadata validation、migration
- P2: wake event、runs lineage、follow polling、pause/resume、CLI 出力の共通 formatter
- P3: duplicate request、partial acceptance、restart recovery、STATE conflict warning
- 全フェーズ: `cargo fmt --check`、`cargo test --all-targets`、`cargo clippy --all-targets --all-features -- -D warnings`、Bats、ShellCheck、`git diff --check`

## 対象外

- 実行中 agent process へのリアルタイム prompt 書き換え
- Codex transcript 全文の SQLite 保存
- Pueue と SQLite をまたぐ完全な distributed transaction
- agent の判断を無視した自動 replacement submission
- 既存 project の raw YAML registry の自動移行
