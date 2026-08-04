# pueue-agent 設計書

日付: 2026-08-04
ステータス: 承認待ち

## 目的

pueue(タスク管理デーモン)で実行される長時間の ML 実験を、coding agent(Claude Code / Codex CLI / Gemini CLI などの headless 実行可能な CLI agent)が自律的に監視・修復・継続するシステム。

- 人 or agent が pueue に実験タスクを投入する
- 実行中は定期的に監視し、**正常時はトークン消費ゼロ**で済ませる
- 異常時は agent が起動し、原因分析・実装修正・ハイパラ調整を行って再投入する
- タスク完了時は agent が結果をまとめ、次の実験を自律的に設計・実装・投入する
- 節目で人に通知し、停止条件(ガードレール)で暴走を防ぐ

## 前提・スコープ

- 実行環境: リモート GPU サーバー(Linux 想定)。pueue 4.x と coding agent CLI が同一マシンにインストール済み
- 対象タスク: ML の学習・実験ジョブ(数時間〜数日)
- agent は「プロンプトを渡して 1 回 headless 実行できる CLI」に限定(API 直叩き・HTTP エンドポイントはスコープ外)
- 任意のプロジェクト/リポジトリに後付けで導入できる汎用ツールとする
- 実装言語: bash(+ jq)。GPU サーバーに Python 等の前提を置かない
- 開発環境は macOS だが、動作対象は Linux サーバー。両対応とする(cron は両者で利用可能)

## アーキテクチャ

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

## パッケージングと導入

中央インストール + プロジェクトごとの薄い設定。

```
~/project/pueueAgent/          ← ツール本体リポジトリ(一度だけ install)
  install.sh                   ← PATH への追加等
  bin/pueue-agent              ← CLI エントリポイント(サブコマンド dispatch)
  lib/
    sentinel.sh                ← 定期チェック本体
    wake_agent.sh              ← agent 起動口 + ガードレール
    notify.sh                  ← 通知
    common.sh                  ← 共通関数(config 読み込み, ログ等)
  templates/
    config.yml
    instructions.md
    STATE.md

各プロジェクト側(init で生成):
  your-ml-repo/
    .pueue-agent/
      config.yml               ← agent コマンド・チェック間隔・停止条件・通知先
      STATE.md                 ← agent の記憶(実験履歴・方針)
      instructions.md          ← agent への指示書(プロジェクト固有に編集可)
      logs/                    ← sentinel / wake の実行ログ, カウンタ, ロック

実験自体のログ・結果は扱わない(pueue が捕捉するタスク出力と、
ライブラリ固有の出力先(wandb 等)に任せる)。
```

### CLI サブコマンド

- `pueue-agent init` — `.pueue-agent/` を対話式で生成。git 管理の選択肢を提示:
  - すべてコミット(実験履歴を git で追う)
  - すべて `.gitignore` に追記(履歴に残さない)
  - 中間: config / instructions はコミット、STATE.md / logs は ignore
- `pueue-agent enable` — cron 登録 + pueue callback 設定 + プロジェクト専用 pueue group(例: `pa-<repo名>`)作成
- `pueue-agent disable` — cron / callback / group を綺麗に撤去
- `pueue-agent status` — 実行中タスク、agent の最終アクション、失敗カウンタ、停止状態を表示
- `pueue-agent resume` — ガードレール発動による停止状態を人が解除
- `pueue-agent submit <cmd>` — 専用 group へのタスク投入の薄いラッパー(人が最初の実験を投げる用)

### 複数プロジェクト並行

プロジェクトごとに pueue group と cron エントリを分離。pueue の callback はデーモン全体で 1 つの設定のため、callback スクリプトはタスクの group からプロジェクトを逆引きして該当プロジェクトの wake_agent.sh に dispatch する(group 名 → プロジェクトパスの対応表を `~/.config/pueue-agent/projects` に保持)。

## 設定ファイル(config.yml)

```yaml
agent:
  command: "claude -p {prompt} --permission-mode acceptEdits"
  # command: "codex exec {prompt}"
  timeout_minutes: 60         # agent 実行のタイムアウト
  max_retries: 2              # agent 自体の起動失敗(APIエラー等)のリトライ上限

check:
  interval_minutes: 10        # sentinel の起動間隔
  deep_check_every: 6         # N回に1回、正常でも agent を deep_check で起動
  # deep_check_interval_minutes: 60   # 回数ではなく時間で指定も可(どちらか一方)
  stall_minutes: 30           # タスク出力がこの時間増えなかったら「停滞」
  extra_log_paths: []         # pueue のタスク出力に加えて監視するパス(任意)
  error_patterns:             # ログ末尾に対する異常判定の正規表現(追加可)
    - "NaN"
    - "Traceback"
    - "CUDA (error|out of memory)"

guardrails:
  max_consecutive_failures: 3 # 連続失敗でこの数に達したら停止して人を待つ
  max_experiments: 20         # 通算実験数の上限

# 通知はターミナル上(logs/notifications.log + `pueue-agent status`)のため設定不要
```

## 動作フロー

### sentinel.sh(定期・トークンゼロ)

1. `pueue status --json` から対象 group のタスク状態を取得
2. 機械判定(監視対象は pueue が捕捉するタスク出力 + `extra_log_paths`):
   - タスクが failed / killed → `crash` モードで wake
   - タスク出力が `stall_minutes` 以上増えていない → `stalled` モードで wake
   - 出力末尾に `error_patterns` がマッチ → `crash` モードで wake
3. すべて正常の場合:
   - チェックカウンタ(`logs/check_count`)をインクリメント
   - `deep_check_every` 回目なら `deep_check` モードで wake、カウンタをリセット
   - それ以外は即終了(agent 起動なし)

### wake_agent.sh(唯一の agent 起動口)

引数: モード(`crash` / `stalled` / `deep_check` / `task_finished`)+ 文脈(タスク ID 等)。

起動**前**に bash 側でガードレールを判定:

1. ロックファイルで多重起動防止(前回の agent が作業中なら skip、ログに記録)
2. 停止状態(`logs/halted`)なら何もしない
3. 連続失敗カウンタが `max_consecutive_failures` 到達 → agent を起動せず停止状態にして通知
4. 通算実験数が `max_experiments` 到達 → 同様に停止・通知
5. 通過したら、モード別プロンプト + instructions.md + STATE.md への参照を組み立てて agent を起動
6. agent の exit code / 出力を `logs/` に記録。agent 自体の失敗は `agent.max_retries` までリトライ、超えたら通知して停止

### モード別の agent への指示(要点)

共通: まず STATE.md と instructions.md を読む。終了前に必ず STATE.md を更新する。git 管理下なら変更をコミットする(実験単位で「何を・なぜ変えたか」を追跡可能にする)。

- **crash / stalled**: ログ・スタックトレースを読んで原因分析 → コード/ハイパラを修正 → STATE.md の履歴を更新 → `pueue add -g <group>` で再投入(連続失敗カウンタの増加は wake_agent.sh が行う)
- **deep_check**: 「pueue 上はエラーなし。ログ・メトリクス・出力物を読み、実験が意味のある進行をしているか(loss の下がり方は妥当か、期待した挙動か)を判断せよ。問題なければ STATE.md にチェック結果を 1 行追記して終了。問題があれば crash 時と同様に介入」
- **task_finished**: 結果を分析・要約 → STATE.md の実験履歴に記録 → 通知(結果サマリ付き)→ 方針・制約の範囲で次の実験を設計・実装 → 投入(正常完了時の連続失敗カウンタのリセットは wake_agent.sh が行う)

### 失敗カウンタの運用

- 「連続失敗」= crash/stalled による介入が、間に成功(task_finished で正常完了)を挟まず連続した回数
- カウンタの増減は bash 側(wake_agent.sh)が行う。agent 任せにしない(ガードレールの信頼性のため)

## STATE.md(agent の記憶)

```markdown
# 実験キャンペーン: <目的を人が最初に書く>
## 方針・制約         ← 人が書く(探索範囲、やってはいけないこと)
## 実験履歴
| # | 変更内容 | 結果 (metric) | 判断 |
## 現在の状況          ← 実行中タスク、直近の agent の判断と理由
## 次の計画
## ヘルスチェック履歴   ← deep_check の結果 1 行ログ
```

- agent のセッション機構に依存しない = agent を差し替えても記憶が引き継がれる
- 人が読めば経緯がすべてわかる

## 通知(notify.sh)

通知はターミナル上で確認する方式。notify.sh はイベントをタイムスタンプ付きで
`.pueue-agent/logs/notifications.log` に追記し、`pueue-agent status` が未読分を
ハイライト表示する。リアルタイムに見たい場合は `pueue-agent notifications -f`
(tail -f 相当)を使う。外部サービス(Slack 等)への送信は行わない。

発火点:

1. 実験完了(結果サマリ付き)
2. 異常検知で agent が修正介入したとき(何をしたかの要約)
3. ガードレール発動で停止したとき(人の介入待ち)
4. agent 自体の失敗がリトライ上限を超えたとき

## エラー処理

- agent の API エラー・タイムアウト: `agent.max_retries` までリトライ → 超過で通知・停止
- sentinel / wake の実行ログは `.pueue-agent/logs/` に保存し、`pueue-agent status` で参照
- ロックファイルには PID を記録し、プロセス死亡時の stale lock は自動回収
- config.yml のパース失敗・必須項目欠落は明示的にエラー終了し通知

## テスト

- **フェイク実験スクリプト**で GPU 学習なしにエンドツーエンド検証: 数秒で「成功 / 失敗(exit 1)/ NaN 出力 / 停滞(sleep)」を再現するスクリプトを用意し、各モードの発火と agent 起動(モック agent = 呼び出し記録するだけのスクリプト)を確認
- bash 関数は **bats** で単体テスト(config パース、機械判定ロジック、カウンタ・ガードレール)
- agent 実機を使うテストは手動の受け入れ確認とする(トークン費用のため自動化しない)

## スコープ外(YAGNI)

- API 直叩き・HTTP エンドポイント型 agent のアダプタ
- Web ダッシュボード
- 複数サーバー間のオーケストレーション
- pueue 以外のジョブランナー対応
