# 開発と検証

## 基本チェック

Linux 上で、stable Rust と C compiler、Git を用意し、リポジトリのルートから実行します。SQLite は Cargo の `bundled` feature でビルドします。

```bash
rustup toolchain install stable --profile minimal --component rustfmt --component clippy
cargo +stable fmt --all -- --check
cargo +stable check --locked --all-targets
cargo +stable clippy --locked --all-targets
cargo +stable test --locked --all-targets -- --test-threads=1
bash -n install.sh bin/pueue-agent
bash tests/shell/install_and_launcher.sh
```

`--locked` は、検証時に意図せず `Cargo.lock` を更新することを防ぎます。依存関係を更新する変更では、更新した lockfile もレビュー対象にしてください。Rust のテストは既存の検証手順に合わせて直列で実行します。

[CI workflow](../.github/workflows/ci.yml) は push、pull request、手動実行で、shell、format、Rust の各ジョブを実行します。format の失敗があっても、別ジョブの shell と Rust テストは実行できます。Clippy は通常の診断レベルで実行し、すべての warning を error にする設定は追加していません。Checkout action は検証済みの commit SHA に固定し、保存する Git 認証情報を無効にしています。

[既存の Stage 3 検証記録](report/2026-09-20-research-reliability-stage3-verification.md) では format が失敗しています。この既存差分を今回の機能修正へ混ぜないため、CI の format は当面、失敗内容を warning として表示する診断です。Linux 上で整形差分をレビューし、`cargo +stable fmt --all -- --check` が通ったら、その step の `continue-on-error` を外して必須チェックにしてください。Rust の check / Clippy / test と shell 回帰テストは通常どおり失敗を報告します。

## Rust がなくても実行できる shell 回帰テスト

```bash
bash tests/shell/install_and_launcher.sh
```

このテストは一時ディレクトリに installer と launcher のコピーを作り、PATH を限定して偽の Cargo を使います。実際の Rust ビルド、Pueue daemon、ネットワーク、通常のインストール先は使いません。

次の経路を検証します。

- 既定・絶対・相対 target directory と、空白を含むパス
- Cargo の target-dir 設定と、installer がリンクする出力先の一致
- 古い directory symlink の置換と、実ディレクトリへの上書き拒否
- build 失敗時の既存ファイルの保全と、成果物がない場合の失敗
- Cargo 不在時のエラーと、symlink 経由の installer 呼び出し
- launcher の引数、空文字列、終了ステータス、symlink chain、未ビルド時の説明

`install.sh` の出力先は `CARGO_TARGET_DIR`、未指定ならリポジトリ直下の `target` です。この出力先を Cargo に明示して、ユーザー共通の `build.target-dir` 設定によるリンク先の食い違いを防ぎます。相対 `CARGO_TARGET_DIR` は installer を呼んだカレントディレクトリを基準にします。インストール先は `PA_INSTALL_PREFIX`、未指定なら `$HOME/.local/bin` です。

## 実 Pueue を使う受け入れテスト

`tests/e2e/run.sh` は Linux の隔離した real Pueue を使う別の受け入れテストです。shell 回帰テストや Cargo テストの成功だけで、この受け入れテストの成功を代用しないでください。実行には `pueue`、`pueued` などの追加ツールが必要です。実行前に [E2E script](../tests/e2e/rust_supervisor.sh) の前提と隔離・cleanup を確認してください。通常の CI workflow はこの実サービスのテストを自動実行しません。
