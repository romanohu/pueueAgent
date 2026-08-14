# pueue-agent 更新経路設計

日付: 2026-08-11
ステータス: 設計承認済み

## 目的

ローカル checkout の `main` が更新されたとき、現在の手動手順である「git 更新、build、install、service 再起動、動作確認」を一つの安全なコマンドにまとめる。

更新対象は pueue-agent supervisor だけとし、Pueue が実行中の学習・評価 task は停止しない。

## コマンド

```bash
pueue-agent version
pueue-agent upgrade
pueue-agent upgrade --source /path/to/pueueAgent
```

`--source` は自動検出できない場合の明示指定である。更新対象 branch は `main` に固定し、別 branch や arbitrary ref を自動で checkout しない。

`version` は package version、build revision、source root、service 状態を human output と bounded JSON で表示する。package version はリリース単位、revision は通常の main 更新単位で変わる。

## source の検証

`upgrade` は次の順で source root を探す。

1. `--source` の指定
2. 実行中 binary の canonical path から `target/release/pueue-agent` の親をたどる自動検出
3. `PUEUE_AGENT_SOURCE_ROOT` 環境変数

見つかった path には対象 package の `Cargo.toml` と `.git` が必要である。source を特定できない場合は、更新操作を行わず `--source` の使用例を表示する。

## 更新手順

1. state directory の upgrade lock を取得する。
2. enabled project の active agent run を確認する。`starting` または `running` があれば、Pueue task と binary を変更せず終了する。
3. source checkout が `main`、`origin/main` tracking、clean worktree であることを検証する。
4. `git fetch origin main` を実行する。
5. `origin/main` に fast-forward できる場合だけ `main` を更新する。local change、branch divergence、fetch failure は停止条件とする。
6. `cargo test --all-targets` を実行する。
7. `cargo build --locked --release` を一時 target directory に対して実行する。
8. test と build が成功した場合だけ、現行 release binary を backup し、新 binary を install path に atomic に反映する。
9. systemd user service または launchd user agent を restart する。service 定義と callback の executable path は現在の install layout と一致させる。
10. service status、SQLite 接続、Pueue status の health check を実行する。更新前の
    immutable policy に固定された Pueue adapter は preflight 専用とする。binary
    replacement 後は policy を read-only で再読込し、新しい launcher identity に
    固定した adapter を構築して Pueue status を確認する。ambient PATH や未検証の
    executable/config path へはフォールバックしない。
11. 成功した場合は revision、service 状態、実行した検証を表示する。

既存の `install.sh` は build と symlink の低レベル経路として残す。`upgrade` はその処理を直接再実装せず、共有可能な install helper を使う。

## 安全性と rollback

- `git reset --hard`、強制 push、未確認の branch checkout は行わない。
- dirty worktree や branch divergence がある場合は、ユーザーの変更を上書きせず停止する。
- test / build failure では install path と service を変更しない。
- service restart または health check の失敗時は backup binary を戻して service を再起動する。
- rollback 後も service が起動しない場合は非ゼロ終了し、Pueue task を操作せず、`doctor` と `version` で調査できる情報を表示する。
- upgrade lock がある場合は二重更新を行わない。異常終了で残った lock は記録された PID の生存を検証してからのみ回収する。
- supervisor 再起動中に発生した Pueue の状態変化は、SQLite の永続 event と次回 reconciliation で回収する。

## service との関係

更新は supervisor の process だけを対象にする。Pueue daemon や Pueue group、実験 task は停止しない。

agent run が active の場合は既定で更新を拒否する。agent の作業を中断して event recovery に任せる強制更新モードは、初期実装のスコープに含めない。

## テストと受け入れ条件

- clean な main checkout が fast-forward 更新され、test、build、service restart、health check を通過する。
- dirty worktree、main 以外の branch、branch divergence が拒否される。
- fetch、test、build の各 failure で install binary が変わらない。
- service restart / health check failure で旧 binary に rollback される。
- active agent run 中は source、binary、service が変更されない。
- 更新対象がない場合は no-op になり、不要な service restart を行わない。
- `version` が package version と revision を区別して表示する。
- systemd と launchd の service control を fake 実装で検証する。
- README に通常の更新方法、失敗時の確認方法、source 検出失敗時の `--source` を記載する。
