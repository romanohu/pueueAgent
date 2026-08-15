# 利用者・開発者向けドキュメント再編 設計書

日付: 2026-08-15
ステータス: 承認済み

## 目的

`pueue-agent` の利用者・運用者と開発者が、実装コードや過去の設計履歴を
読み解かなくても、導入、日常操作、障害対応、内部構成、状態遷移、
セキュリティ境界を理解できる文書体系へ再編する。

README は短い入口にし、詳細を用途別ガイドへ分割する。コマンド名、設定キー、
状態名、対応プラットフォームを現行実装と一致させ、未実装機能や推測は記載しない。

## 対象読者

- 初めて `pueue-agent` を導入する利用者
- 実験投入、監視、介入、停止、復旧、更新を行う運用者
- daemon、scheduler、SQLite、Pueue、agent lifecycle を保守する開発者

利用者・運用者向け情報と開発者向け情報を同程度に扱う。文書は日本語を主言語とし、
CLI 名、設定キー、型名、環境変数、パスなどの識別子は実装上の表記を維持する。

## 検討した構成

### README 一本化

検索対象は一つになるが、すでに長い README がさらに肥大化する。利用者向けの
手順と内部設計が混在し、更新箇所も見つけにくいため採用しない。

### README を入口にした用途別ガイド

README から目的別の文書へ案内する。読者が必要な粒度へ直接移動でき、各文書の
責務も明確になる。既存の Markdown と GitHub の表示だけで利用できるため、
この構成を採用する。

### ドキュメントサイト

検索とナビゲーションは強化できるが、mdBook 等の生成・公開基盤が増える。
今回の目的には過剰なため採用しない。

## 文書構成

### `README.md`

README は「5分で全体像を掴む入口」とし、次だけを掲載する。

- ツールの目的と主要コンポーネント
- 対応環境と既知の制約
- 最短インストールとクイックスタート
- 日常的によく使うコマンド
- 用途別ガイドへの目次
- 導入前に知るべきセキュリティ上の注意

詳細な設定表、全コマンド、長い運用手順、内部状態遷移は用途別ガイドへ移す。

### `docs/getting-started-ja.md`

- Rust、Pueue、user service などの必要条件
- インストールと開発用ビルド
- Pueue profile と state directory の関係
- `init`、生成ファイルの編集、`enable`
- 最初の `submit` と `status`
- Linux の正式な検証範囲と macOS の既知制約

コマンドを順に実行すれば最初の監視対象を登録できる、再現可能な導入手順にする。

### `docs/commands-ja.md`

公開 CLI を次の分類で網羅する。

- セットアップ: `init`, `enable`, `disable`
- 投入: `submit`, `submit-batch`
- 状態確認: `status`, `events`, `runs`, `inspect`, `explain`, `doctor`
- 運用制御: `pause`, `resume`, `start`, `stop`, `cancel`
- 人による介入: `steer`, `steer list`, `wake`
- 保守: `version`, `upgrade`
- 内部・連携用: `event`, `daemon`

各コマンドは、構文、目的、読み取り専用か状態変更か、影響する対象、影響しない対象、
主なオプション、実行例、失敗時に次に確認するコマンドを記載する。hidden の
`internal-launch` は通常利用者向けコマンドとして掲載せず、architecture で
native helper の内部 entry point としてのみ説明する。

### `docs/workflows-ja.md`

コマンド単位ではなく、利用目的ごとの一連の手順を記載する。

- プロジェクト登録と最初の実験投入
- 単発投入と冪等な batch 投入
- 状態監視と periodic DeepCheck
- `steer` と `wake` による人の介入
- automation、service、Pueue task、project 登録の停止・再開
- 異常検知から `cancel` までの確認
- supervisor の安全な upgrade と rollback
- daemon 再起動後の復旧確認

既存 `docs/operations-ja.md` の有効な内容はここへ統合する。

### `docs/architecture-ja.md`

開発者が実装境界を追えるよう、コンポーネント表、Mermaid 図、状態表を併用する。

- Pueue profile、user service、daemon、SQLite、project 設定の所有関係
- `submit` から Pueue task、callback/reconciliation、SQLite event までの流れ
- scheduler の claim、policy preflight、agent run bind、native gate、dispatch
- agent 終了、DB 終端化、private temp cleanup の順序
- event、agent run、launch gate、intervention の主要状態遷移
- daemon 起動時の lease、marker、未完了 run の復旧
- descriptor-bound executable/config/root/temp と process-group cleanup の境界
- diagnostic projection と secret/redaction の境界

過去の `docs/superpowers/specs/` は設計履歴としてリンクできるが、利用者向けの
現行仕様の代替にはしない。

### `docs/troubleshooting-ja.md`

基本の診断順序を `status`、`doctor`、`events`、`runs` とし、症状別に次の表を使う。

| 症状 | まず確認 | 想定原因 | 安全な復旧 |
| --- | --- | --- | --- |

少なくとも service 停止、Pueue profile/config 不一致、policy 読み込み失敗、
project pause/halt、event retry/dead-letter、native gate failure、private temp admission、
upgrade rollback を扱う。SQLite の直接編集、policy の強制修復、未確認 PID への
signal 送信など、安全境界を迂回する手順は案内しない。

### `docs/operations-ja.md`

既存リンクを壊さないため削除しない。本文を新しい `workflows-ja.md` と
`troubleshooting-ja.md` への短い移転案内へ変更する。

## 図解する主要データフロー

### Submission と event 生成

```text
operator/agent
  -> pueue-agent submit
  -> SQLite submission intent
  -> verified Pueue add
  -> pueued task
  -> callback または status reconciliation
  -> task observation / event / incident
```

### Agent dispatch

```text
pending event
  -> scheduler claim
  -> execution policy と project policy の preflight
  -> agent run bind + intervention reservation
  -> native helper readiness
  -> release requested + private temp revalidation
  -> exec proof + exact ack
  -> dispatched
```

### Terminal persistence と cleanup

```text
target/process group terminal proof
  -> agent run と event の終端状態を transaction で永続化
  -> descriptor-owned private temp cleanup
  -> cleanup authority release
```

実際の文書では Mermaid を使い、図だけに情報を閉じ込めず、同じ内容を短い本文と
状態表でも説明する。

## プラットフォーム表示

対応状況は README、getting started、architecture で同じ表現を使う。

- Linux: Ubuntu GitHub Actions で debug check、release check、全ターゲットの
  serial test、shell syntax を検証済み。private temp の mount 境界確認には
  kernel 5.8 以降を要求する。
- macOS: launchd 経路は存在するが、private temp を `/dev/fd/11` の子パスとして
  利用できない既知制約があるため、Linux と同等の agent 実行対応を主張しない。
- その他: fail closed とし、対応済みとは記載しない。

## 保守上の source of truth

- コマンド名とオプション: `src/cli.rs`
- 設定キーと既定値: 設定型と `templates/config.toml`
- 状態名と復旧動作: model、repository、scheduler、daemon の実装
- Pueue 実行境界: `src/pueue.rs`, `src/pueue_process.rs`, `src/process.rs`
- agent lifecycle: `src/agent.rs`, `src/native_launcher.rs`, `src/environment.rs`
- 現行の security invariant: 実装と承認済み設計資料

文書の例には credential、prompt、transcript、環境変数値を含めない。診断例は
実際の bounded projection だけを示し、raw payload や secret の表示を約束しない。

## 検証

- README から5つの用途別ガイドへ移動できること
- `docs/operations-ja.md` の既存パスが残ること
- コマンド一覧が `src/cli.rs` の公開 CLI を漏れなく含むこと
- hidden `internal-launch` を一般利用者向けコマンドとして案内しないこと
- 文書内の相対リンク先が存在すること
- README と architecture の platform support 表現が一致すること
- コードブロックのコマンド名、オプション、設定キーが実装と一致すること
- `git diff --check` が成功すること
- `cargo check --all-targets` と `cargo check --release --all-targets` が成功すること
- `cargo test --all-targets -- --test-threads=1` が成功すること
- shell entrypoint の syntax check が成功すること

CLI とリンクの重要な同期条件は既存 integration test に文書契約として追加する。
Markdown site generator や新しい runtime dependency は追加しない。

## 対象外

- CLI、設定、SQLite schema、scheduler、agent lifecycle の挙動変更
- 英語版ドキュメントの複製
- mdBook 等のドキュメントサイト生成・公開基盤
- `docs/superpowers/` の過去設計・計画の書き換え
- macOS private temp transport の修正
