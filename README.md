# pueue-agent

pueue で実行する長時間 ML 実験を、coding agent(Claude Code / Codex CLI /
Gemini CLI など headless 実行できる任意の CLI)が自律的に監視・修復・継続する
ツール。正常時はトークン消費ゼロ。

## 仕組み

```
人 or agent
  │  pueue-agent submit -- <実験コマンド>
  ▼
pueued ── 実験タスクをプロジェクト専用の pueue group で実行
  │
  │  トリガーは 2 系統
  ├─ (1) タスク終了時: pueue callback → callback.sh
  │        group からプロジェクトを逆引きし、結果に応じて wake へ
  │
  └─ (2) 定期: cron(interval_minutes ごと)→ sentinel.sh
           bash のみの機械チェック(トークン消費ゼロ)
           ├─ 正常(通常回)→ 何もせず終了(agent 起動なし)
           ├─ 正常(N 回に 1 回)→ deep_check として wake へ
           ├─ 失敗 / 停滞 / エラーパターン検知 → crash / stalled として wake へ
           └─ callback が取りこぼした完了タスク → task_finished として wake へ
  ▼
wake_agent.sh ── 唯一の agent 起動口
  │  起動前に bash 側でガードレールを判定(agent 任せにしない):
  │  連続失敗上限 / 通算実験数上限 / lock(多重起動防止)/ halted
  ▼  通過時のみ agent を起動
coding agent(headless・agent.command で自由に差し替え)
  │  読む: .pueue-agent/STATE.md, pueue log <id>, コード
  │  やる: 原因分析・実装修正・ハイパラ調整・次実験の設計
  │  書く: STATE.md 更新, コード編集
  ▼
pueue add -g <group> で(再)投入 ──→ pueued へ戻る(ループ)

通知: 各イベントで notify.sh が logs/notifications.log に追記
      (pueue-agent status / pueue-agent notifications -f で確認)
```

トリガーは 2 系統:

1. **cron → sentinel.sh**(定期): pueue の状態とログを機械的にチェック
2. **pueue callback → wake_agent.sh**(タスク終了時): 即座に agent を起動

wake_agent.sh が唯一の agent 起動口であり、ガードレール(停止条件・多重起動防止)はすべてここ(bash 側)に置く。agent が暴走しても起動口で確実に止められる。

## インストール

```
git clone <repo> && cd pueueAgent && ./install.sh
```

`~/.local/bin/pueue-agent`(`PA_INSTALL_PREFIX` で変更可)に、このリポジトリの
`bin/pueue-agent` への symlink を作成する。`jq` が無ければエラーで停止し、
`pueue` が無ければ警告のみ(サーバー側でのみ必須)。`~/.local/bin` が PATH に
無い場合は note を表示する。

なお、`pueue-agent enable` の初回実行時、pueue の callback 設定を書き換えた
場合は反映のために pueued の再起動が必要な旨の案内が表示されることがある。
その場合は表示された手順(実行中タスクが無いことを確認してから
`pueue shutdown && pueued -d` 等)に従うこと。

## 使い方

```
cd your-ml-repo
pueue-agent init          # .pueue-agent/ を生成(agent コマンド等を質問)
vi .pueue-agent/STATE.md  # 実験の目的・方針・制約を書く
pueue-agent enable        # cron + pueue callback を設定
pueue-agent submit -- python train.py --lr 0.01
```

## 日常操作

```
pueue-agent status         # 監視状況・未読通知
pueue-agent notifications -f
pueue-agent resume         # ガードレール停止からの再開
pueue-agent disable
```

## 設定

`.pueue-agent/config.yml` の主なキー:

| キー | 説明 |
| --- | --- |
| `agent.command` | agent 起動コマンド。`{prompt}` がプロンプトに置換される(例: `"claude -p {prompt} --permission-mode acceptEdits"`) |
| `agent.timeout_minutes` | agent 実行のタイムアウト(分) |
| `agent.max_retries` | agent 自体の起動失敗(API エラー等)のリトライ上限 |
| `pueue.group` | このプロジェクト専用の pueue group 名(`init` が設定) |
| `check.interval_minutes` | sentinel の起動間隔(`enable` が cron に反映) |
| `check.deep_check_every` | N 回に 1 回、正常でも agent を deep_check で起動 |
| `check.deep_check_interval_minutes` | 回数でなく時間で deep_check を行いたい場合に指定(0 なら無効) |
| `check.stall_minutes` | タスク出力がこの時間増えなかったら「停滞」と判定 |
| `check.extra_log_paths` | pueue のタスク出力に加えて監視する追加のログパス |
| `check.error_patterns` | ログ末尾に対する異常判定の正規表現のリスト |
| `guardrails.max_consecutive_failures` | 連続失敗がこの回数に達したら停止 |
| `guardrails.max_experiments` | 通算実験数の上限 |

## 停止条件(ガードレール)

wake_agent.sh は agent を起動する**前**に、必ず bash 側でこれらを判定する
(agent 任せにしない)。

- **連続失敗 3 回で停止**: crash/stalled による介入が、間に成功
  (task_finished での正常完了)を挟まず `guardrails.max_consecutive_failures`
  回連続したら、agent を起動せず停止状態にして通知する
- **通算実験数の上限を超えると停止**: 通算実験数が `guardrails.max_experiments`
  を超えると、同様に agent を起動せず停止・通知する(上限回数分の実験は処理される。
  例: 上限 20 なら 20 件目までは処理し、21 件目で停止)
- **agent リトライ 2 回超過で停止**: agent 自体の起動失敗(API エラー・
  タイムアウト等)は `agent.max_retries` までリトライし、超過したら通知して
  停止する
- **lock による多重起動防止**: ロックファイルで前回の agent がまだ作業中で
  ないか確認し、多重起動を防ぐ。`resume` を実行すると停止状態
  (`logs/halted`)が解除される
