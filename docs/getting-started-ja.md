# 導入ガイド

このガイドでは、リポジトリを取得してプロジェクトを初期化し、最初の実験を投入するまでを説明します。

## 対応環境

- Linux: Ubuntu GitHub Actions で debug check、release check、全ターゲットの serial test、shell syntax を検証済み。private temp の mount 境界確認には kernel 5.8 以降を要求する。
- macOS: launchd 経路は存在するが、private temp を `/dev/fd/11` の子パスとして利用できない既知制約があるため、Linux と同等の agent 実行対応を主張しない。
- その他: fail closed とし、対応済みとは記載しない。

## 必要条件

- Rust toolchain（`cargo`）
- 実行時に利用する Pueue（`pueue`）
- 実験を実行するプロジェクトディレクトリ

## インストール

リポジトリを取得し、インストールスクリプトを実行します。`PA_INSTALL_PREFIX` を指定しない場合、実行ファイルは `$HOME/.local/bin/pueue-agent` に配置されます。

```bash
git clone <repository-url>
cd pueueAgent
./install.sh
```

## Pueue profile を確認する

通常のコマンドは、次の優先順位で Pueue profile を選び、すべて同じ profile を使います。

1. コマンドラインの `--pueue-config <path>`
2. 環境変数 `PUEUE_CONFIG`
3. インストール済み user service 定義にある `--pueue-config <path>`
4. 既定値 `~/.config/pueue/pueue.yml`

指定するパスは絶対パスで、`.` や `..` を含めないでください。インストール済み service の profile と異なる profile を明示すると、設定の混在を防ぐためコマンドは失敗します。

## プロジェクトを初期化する

実験プロジェクトのルートへ移動して `init` を実行し、設定を確認します。`STATE.md` は人が記入する実験方針・制約の入口です。

```bash
cd /absolute/path/to/project
pueue-agent init
$EDITOR .pueue-agent/config.toml
$EDITOR .pueue-agent/STATE.md
```

## 生成ファイルを確認する

`init` はプロジェクト直下に `.pueue-agent/` を作成し、次のファイルとディレクトリを生成します。

- `.pueue-agent/config.toml`: project ID、Pueue group、agent、check、guardrails の設定
- `.pueue-agent/STATE.md`: 人が管理する実験方針、履歴、現在の状況、次の計画
- `.pueue-agent/state.json`: supervisor の機械的な状態（canonical state）
- `.pueue-agent/instructions.md`: agent に渡すプロジェクト指示のテンプレート
- `.pueue-agent/logs/`: プロジェクトログのディレクトリ

既存の `config.toml` があるプロジェクトでは、初期化は上書きせず失敗します。

## プロジェクトを有効化する

設定を保存したら user service をインストールして起動します。

```bash
pueue-agent enable
```

Linux では systemd user service、macOS では launchd 経路を使います。対応環境の制約は「対応環境」を確認してください。

## 最初の実験を投入する

監視対象の job は、raw `pueue add` ではなく必ず `pueue-agent submit` で投入してください。これにより project の設定、state、guardrails、agent supervisor の管理対象として登録されます。

```bash
pueue-agent submit -- python train.py --lr 0.001
```

`--` より後ろが実行するコマンドです。ここでは例として `train.py` を実行します。

## 状態を確認する

投入後は service、automation、project、Pueue、agent run の状態をまとめて確認できます。

```bash
pueue-agent status
```

表示された各項目を個別に確認し、automation の表示だけから Pueue task の状態を推測しないでください。

## 次に読むガイド

- [運用: 停止、再開、更新](operations-ja.md): pause、stop、cancel、disable、upgrade の手順
- 設定の詳細: `.pueue-agent/config.toml` と `templates/config.toml`
