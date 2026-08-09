# 可視化・診断機能 設計書

日付: 2026-08-10
ステータス: レビュー待ち

## 目的

SQLite に蓄積された event、incident、Pueue task、termination request、agent run を、
CLI から追跡可能にする。通常運転の挙動は変えず、障害調査と自動化の判断理由を明確にする。

## 位置づけ

この機能は、次の拡張の Phase A とする。

1. Phase A: 可視化・診断
2. Phase B: 安全ポリシー・承認
3. Phase C: NVIDIA GPU / コスト-aware scheduling

現行の SQLite schema v5 と event/incident の project 境界を利用し、Phase A では既存テーブルを変更しない。

## CLI

### `status --json`

既存のテキスト出力を維持し、`--json` を追加する。JSON の最上位には `schema_version`、
project、daemon、Pueue、event、incident、termination、agent run、policy、resource の各セクションを持たせる。
Phase A では policy と resource は空または `not_configured` として表現し、後続 Phase で拡張できる形にする。

task command、incident payload、agent prompt、Codex transcript の全文は出力しない。既存の bounded payload、
task signature、ファイルパス、件数、状態だけを返す。

### `events`

プロジェクトの event を新しい順に表示する。

- `--kind <KIND>`: event kind で絞る
- `--status <STATUS>`: event status で絞る
- `--limit <N>`: 最大件数。既定値を設定し、上限を設ける
- `--json`: machine-readable output

各行または JSON item には event ID、kind、status、attempts、lease、created/completed time、bounded error summary を含める。

### `inspect <task-id>`

指定 project の Pueue task ID に対応する最新の task observation と、同じ stable signature に紐づく submission、
incident、termination request、event、agent run を時系列で表示する。task ID が再利用されている場合は lifecycle timestamp と
signature を使い、別 task の履歴を混ぜない。

### `explain <incident-id>`

incident の検知から現在の状態までを次の順に表示する。

```text
observation → incident transition → event → policy decision → approval → Pueue action
```

Phase A では policy decision と approval が存在しない場合も扱い、`not_configured` として説明する。

### `doctor`

読み取り専用で次を検査する。

- SQLite schema version、必要な table/index、foreign key、WAL、busy timeout
- project config の妥当性
- Pueue status と専用 group
- callback の登録状態
- service の状態と実行パス
- 期限切れ event lease、agent run、termination dispatch lease
- 後続 Phase の policy/resource provider が設定されている場合の到達性

第一段階では自動修復を行わない。各診断項目は `ok`、`warning`、`error` と短い修復方針を返す。

## 実装境界

- repository に read-only query API を追加する。
- CLI は既存の project root 解決と Pueue config 解決を利用する。
- JSON は serde で型付きにし、`schema_version` を固定する。
- 出力順は時刻だけでなく ID を tie-breaker にして決定的にする。
- query の limit、payload の文字数、エラー表示の長さを上限で制限する。
- Web UI、Slack/Discord 通知、全文ログ表示は対象外とする。

## 検証

- status の text/JSON が同じ状態を表すこと
- event の filter、limit、lease/failed 状態の表示
- task ID 再利用時に observation/signature が混ざらないこと
- incident の因果関係が決定的な順序で表示されること
- doctor が壊れた設定、Pueue error、期限切れ lease を安全に報告すること
- transcript や unbounded payload を出力しないこと
- 既存の全 Rust test、Bats、ShellCheck が通ること

## 非目標

- SQLite schema の大規模変更
- 通知送信や Web UI
- event 処理、agent 起動、termination の挙動変更
