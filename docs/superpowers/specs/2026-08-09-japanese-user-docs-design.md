# 日本語利用者向けドキュメント 設計書

日付: 2026-08-09
ステータス: 承認済み

## 目的

Rust + SQLite 版 `pueue-agent` を日本語話者が導入・設定・運用できるように、
リポジトリの入口資料と生成テンプレートを日本語化する。

## 対象範囲

変更対象は次の利用者向けファイルに限定する。

- `README.md`
- `templates/config.toml`
- `templates/instructions.md`
- `templates/STATE.md`

`docs/superpowers/specs/`、`docs/superpowers/plans/`、
`.superpowers/sdd/` の設計・計画・レビュー記録は履歴資料のため変更しない。

## 方針

- README は日本語を主言語にし、利用者が最初に必要とする導入・Quick Start・設定・運用を先に説明する。
- `pueue-agent`、`pueue`、`codex`、サブコマンド、設定キー、環境変数、パス、正規表現、SQL、コード例は原文の表記を維持する。
- TOML の構造と値の意味は変更せず、コメントだけを日本語化する。
- エージェントに読ませる `instructions.md` と実験ノート `STATE.md` は、日本語で記入・運用できる内容にする。
- Codex の `fresh`、`resume`、`resume_latest` の違い、プロジェクト境界、暗黙の fresh fallback がないことを明記する。
- 異常検知と自動終了について、`notify` / `wake` / `kill` の違い、kill が Pueue 境界で行われること、再検証・重複防止・失敗の可視化を説明する。
- 旧 Bash/YAML 版の移行手順は現行実装に合わせて日本語化するが、旧設計書の内容は更新しない。
- 英語版の複製ファイルや日英併記は作らない。説明を一か所に保ち、更新漏れを避ける。

## README の構成

1. プロジェクト概要と設計上の要点
2. 動作の流れ
3. 必要条件とインストール
4. Quick Start
5. プロジェクトファイルと共有実験コンテキスト
6. TOML 設定リファレンス
7. Codex 会話コンテキストの明示的な継続
8. 異常検知と Pueue タスクの自動終了
9. サービスと状態ファイルの場所
10. Bash/YAML 版からの移行

技術用語は初出で短く説明し、実行可能なコマンドはコードブロックで示す。
README の構成に不要な新機能や設定項目は追加しない。

## テンプレートの責務

- `config.toml`: 設定キーを変更せず、日本語コメントで設定意図と安全な初期値を説明する。
- `instructions.md`: 起動された agent が守るべき読み取り、実験記録、再投入、ガードレール、Codex context の規則を日本語で示す。
- `STATE.md`: 人と agent が共有する実験ノートとして、目的・制約・履歴・現状・次の計画・ヘルスチェックを記録できるようにする。

## 検証

- Markdown のリンク先が存在することを確認する。
- README とテンプレートのコマンド例・設定キーが `src/` と `Cargo.toml` の現行 CLI に一致することを確認する。
- TOML テンプレートがコメント除去後も妥当な構造であることを確認する。
- 既存の Rust テスト、Bats、ShellCheck を実行し、文書変更による回帰がないことを確認する。
- `git diff --check` で空白エラーがないことを確認する。

## 非目標

- Rust 実装、CLI の挙動、SQLite スキーマ、設定キーの変更
- `docs/superpowers/` および `.superpowers/sdd/` の履歴資料の翻訳
- 新しい言語版 README の追加
