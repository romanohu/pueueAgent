# 安全ポリシー・承認機能 設計書

日付: 2026-08-10
ステータス: レビュー待ち

## 目的

Pueue task の終了、replacement experiment の再投入など、外部状態を変更する操作について、
観測・提案・承認・実行を SQLite に永続化する。判断理由と承認履歴を残し、同じ操作の重複実行を防ぐ。

## 互換性

`[policy]` が存在しない既存 project は legacy mode とする。

- `notify`、`wake`、`kill` の既存挙動を維持する
- `wake` は agent 起動を止めない
- `kill` は既存どおり明示的な detector action として扱う

`[policy]` を追加した project だけ、新しい policy decision と proposal flow を適用する。

## 設定

```toml
[policy]
default = "suggest"          # observe | suggest | approve | execute
kill = "execute"
resubmit = "approve"
agent_write = "suggest"
```

各操作の値は `observe`、`suggest`、`approve`、`execute` のいずれかとする。
未指定の操作は `default` を使う。設定不備は fail closed で、proposal の自動実行を許可しない。

### 操作の意味

- `observe`: decision と監査 event だけを保存する
- `suggest`: proposal を pending で保存し、外部操作を行わない
- `approve`: operator の承認後に一度だけ実行する
- `execute`: 条件を満たせば supervisor が自動実行する

第一段階の操作種別は `kill`、`resubmit`、`agent_write` とする。`agent_write` は任意の agent command の
ファイル編集を強制的に止めるものではなく、agent の提案や検出された変更を監査対象にするための予約枠とする。

## SQLite モデル

### `proposals`

proposal ID、project、action、target fingerprint、bounded payload、status、created/expired time、policy decision を保存する。
同一 project・action・target fingerprint に対して active proposal を1つだけ許可する。

status は `pending`、`approved`、`rejected`、`expired`、`executed`、`failed` とする。

### `approvals`

proposal ID、decision (`approved` / `rejected`)、actor、reason、created time を保存する。
承認は append-only とし、現在の有効な決定は transaction 内で解決する。

### `policy_decisions`

proposal ID、policy level、outcome、reason、actor (`system` / operator)、created time を保存する。
`explain` が observation から外部操作までを復元できるようにする。

状態遷移と approval は compare-and-set と transaction で保護し、期限切れ・二重 approve・同時 execute を安全に処理する。

## CLI

```text
pueue-agent proposals [--status <STATUS>] [--json]
pueue-agent approve <proposal-id> [--reason <TEXT>]
pueue-agent reject <proposal-id> [--reason <TEXT>]
```

`approve` は proposal の内容と target fingerprint を再表示し、すでに実行済み・期限切れ・別 project の proposal は拒否する。
`reject` は外部操作を行わず、理由を保存する。実行結果は `explain` と `events` から追跡できる。

## 実行境界

- `kill` は既存の termination manager を経由し、直接 OS signal を送らない。
- `resubmit` は必ず `pueue-agent submit` の SQLite submission accounting を経由する。
- policy decision は agent prompt の判断に委ねず、supervisor が外部操作の直前に評価する。
- approval は target task signature、project、action、payload hash に結び付ける。
- approval が必要な操作は、再照合で target が変わった場合に無効化する。
- `wake` の agent 起動はこの Phase では制御しない。

## 検証

- policy level ごとの kill/resubmit 動作
- `[policy]` がない legacy project の回帰
- proposal の idempotency と重複 execute 防止
- approval の期限切れ、却下、二重承認、同時承認
- task signature 変更時の approval 無効化
- kill failure と resubmit failure の可視化
- SQLite transaction rollback と restart recovery
- 既存の termination、scheduler、reconciliation test の回帰がないこと

## 非目標

- 任意の agent process の filesystem write を完全に sandbox 化すること
- `wake` の自動起動を承認制にすること
- 通知送信や Web UI
