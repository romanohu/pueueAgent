# pueue-agent

pueue で実行する長時間 ML 実験を、coding agent(Claude Code / Codex CLI /
Gemini CLI など headless 実行できる任意の CLI)が自律的に監視・修復・継続する
ツール。正常時はトークン消費ゼロ。

## 仕組み

```
┌─ GPU サーバー ──────────────────────────────────────────┐
│  pueued ── 実験タスクを実行                               │
│    │                                                    │
│    ├─ [タスク終了時] callback ──→ wake_agent.sh ─┐       │
│    │                                            │       │
│  cron (interval_minutes ごと)                    │       │
│    └─→ sentinel.sh (bash・トークン消費ゼロ)       │       │
│         ├─ 正常 & 通常回 → 何もせず終了            │       │
│         ├─ 正常 & N回目 → deep_check モードで ─────┤       │
│         └─ 異常検知 ──────────────────────────→ wake_agent.sh
│                                                 ▼       │
│                                     coding agent (headless)
│                                       読む: STATE.md, ログ, コード
│                                       やる: 分析・実装修正・ハイパラ調整
│                                       書く: STATE.md 更新, コード編集
│                                       └─ pueue add で(再)投入
│                                                         │
│  notify.sh ──→ Slack など(節目の通知)                    │
└─────────────────────────────────────────────────────────┘
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
- **通算実験数 20 で停止**: 通算実験数が `guardrails.max_experiments` に
  達したら、同様に agent を起動せず停止・通知する
- **agent リトライ 2 回超過で停止**: agent 自体の起動失敗(API エラー・
  タイムアウト等)は `agent.max_retries` までリトライし、超過したら通知して
  停止する
- **lock による多重起動防止**: ロックファイルで前回の agent がまだ作業中で
  ないか確認し、多重起動を防ぐ。`resume` を実行すると停止状態
  (`logs/halted`)が解除される
