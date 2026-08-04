# pueue-agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** pueue で走る ML 実験を coding agent が自律監視・修復・継続する汎用 CLI ツール `pueue-agent` を実装する。

**Architecture:** cron が起動する sentinel(bash・トークンゼロの機械チェック)と pueue の完了 callback が、唯一の agent 起動口 wake_agent に集約される。wake_agent は bash 側でガードレール(連続失敗・実験数上限・多重起動防止)を判定してから、設定された任意の CLI agent を headless 起動する。agent の記憶は `.pueue-agent/STATE.md` に外部化。

**Tech Stack:** bash (3.2 互換) + jq + awk。テストは bats-core。対象は Linux サーバーだが macOS でも動作。

**Spec:** `docs/superpowers/specs/2026-08-04-pueue-agent-design.md`

## Global Constraints

- bash 3.2 互換(macOS 標準 bash で動くこと): 連想配列・`declare -A`・`${var,,}` 禁止。shebang は `#!/usr/bin/env bash`
- 外部依存は `jq` のみ(+ POSIX 標準ツール)。Python 等を前提にしない
- すべての pueue 呼び出しは `${PA_PUEUE_BIN:-pueue}` を **クォート無しで** 展開して行う(テスト・e2e でモック/引数付きコマンドに差し替えるため)
- プロジェクト側に置くのは `.pueue-agent/`(config.yml / STATE.md / instructions.md / logs/)のみ。スクリプト本体は置かない
- 通知は外部サービスに送らない。`logs/notifications.log` への追記のみ
- 破壊的な pueue 操作(kill / remove / group remove)はユーザーの明示操作(disable 等)以外で行わない
- 全 `.sh` / `bin/pueue-agent` は shellcheck を通す(`shellcheck -x`)
- コミットは各タスクの最後に必ず行う

## File Structure

```
install.sh                    # PATH セットアップ(~/.local/bin に symlink)
bin/pueue-agent               # CLI エントリポイント(サブコマンド dispatch)
lib/common.sh                 # 共通: パス解決, config パーサ, ログ, pueue ラッパ
lib/notify.sh                 # pa_notify + notifications サブコマンド実装
lib/init.sh                   # init サブコマンド
lib/wake_agent.sh             # wake サブコマンド(ガードレール + agent 起動)
lib/sentinel.sh               # sentinel サブコマンド(定期チェック)
lib/callback.sh               # callback サブコマンド(pueue callback → dispatch)
lib/manage.sh                 # enable / disable / status / resume / submit
templates/config.yml
templates/STATE.md
templates/instructions.md
tests/*.bats                  # bats 単体テスト
tests/helpers/                # モック pueue / モック agent / テスト共通関数
tests/e2e/run.sh              # 実 pueued を使う E2E(隔離 config)
tests/e2e/fake_experiments/   # 成功/失敗/NaN/停滞 を数秒で再現するスクリプト
```

登録簿(全プロジェクト共有、ツールが管理): `~/.config/pueue-agent/projects` — 1 行 = `<group>\t<project_abs_path>`。

`.pueue-agent/logs/` 内の状態ファイル(すべて bash が管理):

| ファイル | 内容 |
|---|---|
| `runtime.log` | sentinel/wake の動作ログ |
| `notifications.log` | 通知(ISO8601 + イベント名 + メッセージ) |
| `notifications.seen` | status が既読にした際のバイトオフセット |
| `consec_failures` | 連続失敗カウンタ(数値 1 行) |
| `experiment_count` | 通算実験数(数値 1 行) |
| `check_count` | deep check 用カウンタ |
| `last_deep_check` | 最終 deep check の epoch 秒 |
| `halted` | 存在すれば停止状態(中身に理由) |
| `lock/` | mkdir ロック(中に pid ファイル) |
| `progress_<id>` | `<サイズ> <epoch>` 停滞検知用スナップショット |
| `handled_tasks` | crash 処理済みタスク id(1 行 1 id) |
| `agent_<epoch>.log` | agent 実行の stdout/stderr |

---

### Task 1: リポジトリ scaffold + common.sh の基礎 + テストハーネス

**Files:**
- Create: `bin/pueue-agent`, `lib/common.sh`, `tests/helpers/setup.bash`, `tests/test_common.bats`, `.gitignore`

**Interfaces:**
- Produces: `bin/pueue-agent <subcommand> [args...]` dispatcher(未知コマンドは usage を出し exit 1)
- Produces: `common.sh` の関数 — `pa_die MSG`(stderr に出し exit 1)、`pa_log MSG`(`$PA_DIR/logs/runtime.log` に `YYYY-MM-DDTHH:MM:SS msg` を追記)、`pa_find_project [path]`(引数 or `$PWD` から上方向に `.pueue-agent/` を探し、見つけたプロジェクトルート絶対パスを stdout へ。無ければ return 1)、`pa_set_project PATH`(`PA_PROJECT` と `PA_DIR=$PA_PROJECT/.pueue-agent` をセットし `logs/` を mkdir -p)
- Produces: 環境変数規約 `PA_PUEUE_BIN`(既定 `pueue`)

- [ ] **Step 1: 開発ツールをインストール**

```bash
brew install bats-core shellcheck
bats --version   # 期待: Bats 1.x
```

- [ ] **Step 2: 失敗するテストを書く**

`tests/helpers/setup.bash`:

```bash
# 共通テストセットアップ。各 .bats から load される。
REPO_ROOT="$(cd "$(dirname "$BATS_TEST_FILENAME")/.." && pwd)"
PA_BIN="$REPO_ROOT/bin/pueue-agent"

# 一時プロジェクトを作る: .pueue-agent/ 入りのディレクトリ (group は pa-proj に設定)
make_project() {
  local dir="$BATS_TEST_TMPDIR/proj"
  mkdir -p "$dir/.pueue-agent/logs"
  if [ -f "$REPO_ROOT/templates/config.yml" ]; then
    sed 's/^  group: .*/  group: "pa-proj"/' "$REPO_ROOT/templates/config.yml" \
      > "$dir/.pueue-agent/config.yml"
  fi
  echo "$dir"
}
```

`tests/test_common.bats`:

```bash
load helpers/setup

@test "unknown subcommand prints usage and exits 1" {
  run "$PA_BIN" no-such-command
  [ "$status" -eq 1 ]
  [[ "$output" == *"Usage:"* ]]
}

@test "no subcommand prints usage and exits 1" {
  run "$PA_BIN"
  [ "$status" -eq 1 ]
}

@test "pa_find_project finds .pueue-agent upward from cwd" {
  proj="$(make_project)"
  mkdir -p "$proj/src/deep"
  run bash -c "source '$REPO_ROOT/lib/common.sh' && cd '$proj/src/deep' && pa_find_project"
  [ "$status" -eq 0 ]
  [ "$output" = "$proj" ]
}

@test "pa_find_project fails when absent" {
  run bash -c "source '$REPO_ROOT/lib/common.sh' && cd '$BATS_TEST_TMPDIR' && pa_find_project"
  [ "$status" -eq 1 ]
}

@test "pa_log appends timestamped line" {
  proj="$(make_project)"
  bash -c "source '$REPO_ROOT/lib/common.sh' && pa_set_project '$proj' && pa_log 'hello world'"
  grep -q "hello world" "$proj/.pueue-agent/logs/runtime.log"
}
```

- [ ] **Step 3: テストが失敗することを確認**

Run: `bats tests/test_common.bats`
Expected: 全テスト FAIL(bin/pueue-agent が存在しない)

- [ ] **Step 4: 実装**

`lib/common.sh`:

```bash
#!/usr/bin/env bash
# pueue-agent 共通関数。他の lib/*.sh と bin/pueue-agent から source される。

pa_die() {
  echo "pueue-agent: error: $*" >&2
  exit 1
}

# プロジェクトルート探索: 引数 or $PWD から上へ .pueue-agent/ を探す
pa_find_project() {
  local dir="${1:-$PWD}"
  dir="$(cd "$dir" 2>/dev/null && pwd)" || return 1
  while [ "$dir" != "/" ]; do
    if [ -d "$dir/.pueue-agent" ]; then
      echo "$dir"
      return 0
    fi
    dir="$(dirname "$dir")"
  done
  return 1
}

pa_set_project() {
  PA_PROJECT="$(cd "$1" && pwd)" || pa_die "no such project dir: $1"
  PA_DIR="$PA_PROJECT/.pueue-agent"
  [ -d "$PA_DIR" ] || pa_die "not initialized: $PA_DIR missing (run: pueue-agent init)"
  mkdir -p "$PA_DIR/logs"
}

pa_log() {
  local ts
  ts="$(date '+%Y-%m-%dT%H:%M:%S')"
  echo "$ts $*" >> "$PA_DIR/logs/runtime.log"
}
```

`bin/pueue-agent`:

```bash
#!/usr/bin/env bash
set -u
# symlink 経由 (install.sh が ~/.local/bin に張る) でも本体位置を解決する
SOURCE="$0"
while [ -L "$SOURCE" ]; do
  DIR="$(cd "$(dirname "$SOURCE")" && pwd)"
  SOURCE="$(readlink "$SOURCE")"
  case "$SOURCE" in /*) : ;; *) SOURCE="$DIR/$SOURCE" ;; esac
done
PA_ROOT="$(cd "$(dirname "$SOURCE")/.." && pwd)"
# shellcheck source=lib/common.sh
source "$PA_ROOT/lib/common.sh"

usage() {
  cat >&2 <<'EOF'
Usage: pueue-agent <command> [args]

Commands:
  init            このリポジトリに .pueue-agent/ を生成
  enable          cron 登録 + pueue callback 設定 + group 作成
  disable         上記を撤去
  status          監視状況・通知・カウンタを表示
  resume          ガードレール停止状態を解除
  submit <cmd..>  専用 group にタスクを投入
  notifications   通知ログを表示 (-f で follow)
  sentinel [dir]  (内部) 定期チェック — cron から呼ばれる
  wake <mode> ..  (内部) agent 起動口
  callback <id> <group>  (内部) pueue callback から呼ばれる
EOF
  exit 1
}

[ $# -ge 1 ] || usage
cmd="$1"; shift
case "$cmd" in
  init)          source "$PA_ROOT/lib/init.sh";       pa_cmd_init "$@" ;;
  enable)        source "$PA_ROOT/lib/manage.sh";     pa_cmd_enable "$@" ;;
  disable)       source "$PA_ROOT/lib/manage.sh";     pa_cmd_disable "$@" ;;
  status)        source "$PA_ROOT/lib/manage.sh";     pa_cmd_status "$@" ;;
  resume)        source "$PA_ROOT/lib/manage.sh";     pa_cmd_resume "$@" ;;
  submit)        source "$PA_ROOT/lib/manage.sh";     pa_cmd_submit "$@" ;;
  notifications) source "$PA_ROOT/lib/notify.sh";     pa_cmd_notifications "$@" ;;
  sentinel)      source "$PA_ROOT/lib/sentinel.sh";   pa_cmd_sentinel "$@" ;;
  wake)          source "$PA_ROOT/lib/wake_agent.sh"; pa_cmd_wake "$@" ;;
  callback)      source "$PA_ROOT/lib/callback.sh";   pa_cmd_callback "$@" ;;
  *)             usage ;;
esac
```

`chmod +x bin/pueue-agent`。`.gitignore` に `*.bak` を追加。

注意: この時点で `lib/init.sh` 等はまだ無いが、dispatcher の case 分岐は source 失敗でエラーになるだけなので、未知コマンド/引数なしのテストは通る。

- [ ] **Step 5: テストが通ることを確認**

Run: `bats tests/test_common.bats`
Expected: 5 tests, 0 failures

- [ ] **Step 6: shellcheck**

Run: `shellcheck -x bin/pueue-agent lib/common.sh`
Expected: 指摘ゼロ

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat: scaffold pueue-agent CLI with common helpers and test harness"
```

---

### Task 2: config パーサ + config テンプレート

**Files:**
- Create: `templates/config.yml`
- Modify: `lib/common.sh`(末尾に追記)
- Test: `tests/test_config.bats`

**Interfaces:**
- Consumes: `pa_die`(Task 1)
- Produces: `pa_config KEY [DEFAULT]` — `$PA_DIR/config.yml` から `section.key` 形式のスカラ値を stdout へ。無ければ DEFAULT(それも無ければ空)。値の前後の引用符は除去
- Produces: `pa_config_list KEY` — リスト値を 1 行 1 要素で stdout へ
- Produces: `templates/config.yml` のスキーマ(下記)。**config.yml は「2 階層 + スカラのリスト」の YAML サブセットのみ対応**とコメントで明記

- [ ] **Step 1: config テンプレートを書く**

`templates/config.yml`:

```yaml
# pueue-agent 設定
# 注意: パーサは YAML サブセットのみ対応 —
#   セクション(トップレベルキー) / 2階層目の "key: value" / "- item" のリストのみ。
#   アンカー・複数行値・ネスト3階層以上は不可。

agent:
  # {prompt} が組み立てられたプロンプトに置換される。
  # 例: "codex exec {prompt}" / "gemini -p {prompt}"
  command: "claude -p {prompt} --permission-mode acceptEdits"
  timeout_minutes: 60
  max_retries: 2            # agent 自体の起動失敗(APIエラー等)のリトライ上限

pueue:
  group: ""                 # init が設定する (例: pa-myrepo)

check:
  interval_minutes: 10      # sentinel の起動間隔(enable が cron に反映)
  deep_check_every: 6       # N回に1回、正常でも agent を deep_check で起動
  deep_check_interval_minutes: 0   # 0以外なら回数でなく時間で deep_check
  stall_minutes: 30         # タスク出力がこの時間増えなかったら「停滞」
  extra_log_paths: []
  error_patterns:
    - "NaN"
    - "Traceback"
    - "CUDA (error|out of memory)"

guardrails:
  max_consecutive_failures: 3
  max_experiments: 20
```

- [ ] **Step 2: 失敗するテストを書く**

`tests/test_config.bats`:

```bash
load helpers/setup

setup() {
  proj="$(make_project)"
  export proj
}

cfg() {  # ヘルパ: pa_config をサブシェルで呼ぶ
  bash -c "source '$REPO_ROOT/lib/common.sh' && pa_set_project '$proj' && pa_config $1 ${2:-}"
}

@test "reads quoted scalar" {
  run cfg agent.command
  [ "$output" = 'claude -p {prompt} --permission-mode acceptEdits' ]
}

@test "reads numeric scalar" {
  run cfg check.interval_minutes
  [ "$output" = "10" ]
}

@test "similar key names do not collide" {
  # interval_minutes と deep_check_interval_minutes を混同しない
  run cfg check.deep_check_interval_minutes
  [ "$output" = "0" ]
}

@test "missing key returns default" {
  run cfg nosuch.key 42
  [ "$output" = "42" ]
}

@test "missing key without default returns empty, exit 0" {
  run cfg nosuch.key
  [ "$status" -eq 0 ]
  [ "$output" = "" ]
}

@test "reads list items" {
  run bash -c "source '$REPO_ROOT/lib/common.sh' && pa_set_project '$proj' && pa_config_list check.error_patterns"
  [ "${lines[0]}" = "NaN" ]
  [ "${lines[1]}" = "Traceback" ]
  [ "${lines[2]}" = "CUDA (error|out of memory)" ]
}

@test "empty inline list yields nothing" {
  run bash -c "source '$REPO_ROOT/lib/common.sh' && pa_set_project '$proj' && pa_config_list check.extra_log_paths"
  [ "$output" = "" ]
}
```

- [ ] **Step 3: テストが失敗することを確認**

Run: `bats tests/test_config.bats`
Expected: FAIL(pa_config 未定義)

- [ ] **Step 4: 実装**

`lib/common.sh` 末尾に追記:

```bash
# config.yml (YAMLサブセット) から section.key のスカラ値を読む
# usage: pa_config section.key [default]
pa_config() {
  local key="$1" default="${2-}"
  local section="${key%%.*}" name="${key#*.}"
  local file="$PA_DIR/config.yml" val
  [ -f "$file" ] || pa_die "config not found: $file"
  val=$(awk -v section="$section" -v name="$name" '
    /^[^ #]/ { in_section = ($0 == section ":") ; next }
    in_section {
      line = $0
      sub(/^  /, "", line)
      if (index(line, name ":") == 1) {
        sub(/^[^:]*:[ ]*/, "", line)
        sub(/[ ]*(#.*)?$/, "", line)      # 行末コメント除去
        gsub(/^"|"$/, "", line)           # 引用符除去
        print line
        exit
      }
    }
  ' "$file")
  if [ -n "$val" ]; then echo "$val"; else echo "$default"; fi
}

# リスト値を1行1要素で出力。 "key: []" は空。
pa_config_list() {
  local key="$1"
  local section="${key%%.*}" name="${key#*.}"
  local file="$PA_DIR/config.yml"
  [ -f "$file" ] || pa_die "config not found: $file"
  awk -v section="$section" -v name="$name" '
    /^[^ #]/ { in_section = ($0 == section ":"); in_list = 0; next }
    in_section {
      line = $0
      sub(/^  /, "", line)
      if (index(line, name ":") == 1) {
        rest = line
        sub(/^[^:]*:[ ]*/, "", rest)
        in_list = (rest == "" || rest == "[]") ? (rest == "") : 0
        next
      }
      if (in_list && line ~ /^  - /) {
        sub(/^  - /, "", line)
        gsub(/^"|"$/, "", line)
        print line
        next
      }
      if (line !~ /^  / ) in_list = 0
    }
  ' "$file"
}
```

注意(awk の引用符除去): `gsub(/^"|"$/, "", line)` は値の外側の `"` のみを想定。値自体に `"` を含めたい場合は考慮外(テンプレートのコメントに記載済みのサブセット制約)。

- [ ] **Step 5: テストが通ることを確認**

Run: `bats tests/test_config.bats tests/test_common.bats`
Expected: 全 pass

- [ ] **Step 6: shellcheck + Commit**

```bash
shellcheck -x lib/common.sh
git add -A && git commit -m "feat: config.yml subset parser and default template"
```

---

### Task 3: notify.sh(ターミナル通知)

**Files:**
- Create: `lib/notify.sh`
- Test: `tests/test_notify.bats`

**Interfaces:**
- Consumes: `pa_set_project`, `pa_log`(Task 1)
- Produces: `pa_notify EVENT MESSAGE` — `$PA_DIR/logs/notifications.log` に `ISO8601 [EVENT] MESSAGE` を追記(EVENT は `task_finished|intervention|halted|agent_error` のいずれか)
- Produces: `pa_cmd_notifications [-f] [project_dir]` — 通知ログを cat(-f で `tail -f`)
- Produces: `pa_unread_notifications` — `notifications.seen` のオフセット以降の行を出力。`pa_mark_notifications_seen` — 現在のファイルサイズを seen に記録

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_notify.bats`:

```bash
load helpers/setup

setup() { proj="$(make_project)"; export proj; }

in_proj() { bash -c "source '$REPO_ROOT/lib/common.sh' && source '$REPO_ROOT/lib/notify.sh' && pa_set_project '$proj' && $1"; }

@test "pa_notify appends timestamped event line" {
  in_proj "pa_notify halted 'stopped after 3 failures'"
  run cat "$proj/.pueue-agent/logs/notifications.log"
  [[ "$output" =~ \[halted\]\ stopped\ after\ 3\ failures ]]
}

@test "unread shows only new lines after mark seen" {
  in_proj "pa_notify task_finished 'exp1 done'"
  in_proj "pa_mark_notifications_seen"
  in_proj "pa_notify halted 'stop'"
  run in_proj "pa_unread_notifications"
  [[ "$output" == *"[halted] stop"* ]]
  [[ "$output" != *"exp1 done"* ]]
}

@test "notifications subcommand prints log" {
  in_proj "pa_notify task_finished 'exp1 done'"
  run bash -c "cd '$proj' && '$PA_BIN' notifications"
  [[ "$output" == *"exp1 done"* ]]
}

@test "unread with no log file is empty and exit 0" {
  run in_proj "pa_unread_notifications"
  [ "$status" -eq 0 ]
  [ "$output" = "" ]
}
```

- [ ] **Step 2: テストが失敗することを確認**

Run: `bats tests/test_notify.bats`
Expected: FAIL

- [ ] **Step 3: 実装**

`lib/notify.sh`:

```bash
#!/usr/bin/env bash
# ターミナル通知: logs/notifications.log への追記と閲覧。外部送信はしない。

pa_notify() {
  local event="$1"; shift
  local ts
  ts="$(date '+%Y-%m-%dT%H:%M:%S')"
  echo "$ts [$event] $*" >> "$PA_DIR/logs/notifications.log"
  # runtime.log にはイベント名のみ記録する。メッセージ本文まで書くと
  # status の「recent activity」表示が既読管理をすり抜けてしまう
  pa_log "notify [$event]"
}

pa_unread_notifications() {
  local log="$PA_DIR/logs/notifications.log" seen=0
  [ -f "$log" ] || return 0
  [ -f "$PA_DIR/logs/notifications.seen" ] && seen="$(cat "$PA_DIR/logs/notifications.seen")"
  tail -c "+$((seen + 1))" "$log"
}

pa_mark_notifications_seen() {
  local log="$PA_DIR/logs/notifications.log"
  [ -f "$log" ] || return 0
  wc -c < "$log" | tr -d ' ' > "$PA_DIR/logs/notifications.seen"
}

pa_cmd_notifications() {
  local follow=0
  if [ "${1-}" = "-f" ]; then follow=1; shift; fi
  local proj
  proj="$(pa_find_project "${1-}")" || pa_die "no .pueue-agent found (run inside a project)"
  pa_set_project "$proj"
  local log="$PA_DIR/logs/notifications.log"
  touch "$log"
  if [ "$follow" = 1 ]; then tail -f "$log"; else cat "$log"; fi
}
```

- [ ] **Step 4: テストが通ることを確認 + shellcheck + Commit**

```bash
bats tests/test_notify.bats && shellcheck -x lib/notify.sh
git add -A && git commit -m "feat: terminal notifications log"
```

---

### Task 4: テンプレート(STATE.md / instructions.md)+ init サブコマンド

**Files:**
- Create: `templates/STATE.md`, `templates/instructions.md`, `lib/init.sh`
- Test: `tests/test_init.bats`

**Interfaces:**
- Consumes: `pa_die`, `pa_find_project`(Task 1)
- Produces: `pa_cmd_init [--agent-cmd CMD] [--git-mode commit|ignore|mixed] [--group NAME] [dir]` — `.pueue-agent/` を生成。TTY かつフラグ不足なら対話式に質問。group 未指定時は `pa-<ディレクトリ名を [a-z0-9-] に正規化したもの>`
- Produces: git-mode の挙動 — `commit`: 何もしない(全部コミット対象)/ `ignore`: `.gitignore` に `.pueue-agent/` を追記 / `mixed`: `.gitignore` に `.pueue-agent/STATE.md` と `.pueue-agent/logs/` を追記。追記は重複チェック付き
- Produces: 生成された `config.yml` は `agent.command` と `pueue.group` が反映済み

- [ ] **Step 1: STATE.md テンプレートを書く**

`templates/STATE.md`:

```markdown
# 実験キャンペーン: (目的をここに書く)

## 方針・制約
<!-- 人が書く: 探索してよい範囲、やってはいけないこと、成功の定義 -->
- (例) lr, batch_size, model depth は変更可。データセットと評価指標は変更不可
- (例) 1 実験は最長 12 時間以内に収まる設定にする

## 実験履歴
| # | 変更内容 | 結果 (metric) | 判断 |
|---|---------|--------------|------|

## 現在の状況
(未開始)

## 次の計画
(未定)

## ヘルスチェック履歴
<!-- deep_check の結果を 1 行ずつ追記 -->
```

- [ ] **Step 2: instructions.md テンプレートを書く**

`templates/instructions.md`:

```markdown
# coding agent への指示書

あなたは pueue で実行される ML 実験を管理する自律 agent です。
起動されるたびに、以下を必ず守ってください。

## 毎回必ずやること
1. まず `.pueue-agent/STATE.md` を読み、経緯と方針・制約を把握する
2. 作業の最後に必ず STATE.md を更新する(実験履歴の行追加、現在の状況、次の計画)
3. このリポジトリが git 管理下なら、変更を「何を・なぜ変えたか」がわかるメッセージでコミットする
4. タスクの投入は必ず `pueue add -g {{GROUP}} -- <command>` で行う

## モード別の役割
起動時のプロンプトに mode が示されます。

- **crash / stalled**: タスクが失敗または停滞した。pueue のタスク出力
  (`pueue log <id>`)とスタックトレースを読んで原因を分析し、コードまたは
  ハイパーパラメータを修正して再投入する。原因と対処を STATE.md に記録する。
- **deep_check**: pueue 上はエラーなし。タスク出力・メトリクス・成果物を読み、
  実験が意味のある進行をしているか判断する(loss の下がり方は妥当か、
  期待した挙動か)。問題なければ STATE.md のヘルスチェック履歴に 1 行追記して
  終了する。問題があれば crash 時と同様に介入する。
- **task_finished**: タスクが完了した。結果を分析・要約して STATE.md の実験履歴に
  記録し、方針・制約の範囲で次に試すべき実験を設計・実装して投入する。
  実験を続ける価値がない(目的達成 or 頭打ち)と判断したら、投入せず
  STATE.md にその結論を書く。

## 禁止事項
- STATE.md の「方針・制約」を逸脱する実験(必要と思うなら STATE.md の
  「次の計画」に提案として書き、投入はしない)
- pueue の group 設定・他 group のタスクへの干渉
- `.pueue-agent/config.yml` の変更
```

- [ ] **Step 3: 失敗するテストを書く**

`tests/test_init.bats`:

```bash
load helpers/setup

setup() {
  target="$BATS_TEST_TMPDIR/myrepo"
  mkdir -p "$target"
  git -C "$target" init -q
}

@test "init creates .pueue-agent with all files" {
  run "$PA_BIN" init --agent-cmd 'echo {prompt}' --git-mode commit "$target"
  [ "$status" -eq 0 ]
  [ -f "$target/.pueue-agent/config.yml" ]
  [ -f "$target/.pueue-agent/STATE.md" ]
  [ -f "$target/.pueue-agent/instructions.md" ]
  [ -d "$target/.pueue-agent/logs" ]
}

@test "init writes agent command and derived group into config" {
  "$PA_BIN" init --agent-cmd 'codex exec {prompt}' --git-mode commit "$target"
  grep -q 'command: "codex exec {prompt}"' "$target/.pueue-agent/config.yml"
  grep -q 'group: "pa-myrepo"' "$target/.pueue-agent/config.yml"
}

@test "git-mode ignore appends to .gitignore" {
  "$PA_BIN" init --agent-cmd 'x {prompt}' --git-mode ignore "$target"
  grep -qx '.pueue-agent/' "$target/.gitignore"
}

@test "git-mode mixed ignores state and logs only" {
  "$PA_BIN" init --agent-cmd 'x {prompt}' --git-mode mixed "$target"
  grep -qx '.pueue-agent/STATE.md' "$target/.gitignore"
  grep -qx '.pueue-agent/logs/' "$target/.gitignore"
}

@test "init twice does not duplicate gitignore entries and refuses overwrite" {
  "$PA_BIN" init --agent-cmd 'x {prompt}' --git-mode ignore "$target"
  run "$PA_BIN" init --agent-cmd 'x {prompt}' --git-mode ignore "$target"
  [ "$status" -eq 1 ]
  [ "$(grep -cx '.pueue-agent/' "$target/.gitignore")" -eq 1 ]
}

@test "group name is sanitized" {
  weird="$BATS_TEST_TMPDIR/My_Weird Repo!"
  mkdir -p "$weird"
  "$PA_BIN" init --agent-cmd 'x {prompt}' --git-mode commit "$weird"
  grep -q 'group: "pa-my-weird-repo"' "$weird/.pueue-agent/config.yml"
}
```

- [ ] **Step 4: テストが失敗することを確認**

Run: `bats tests/test_init.bats`
Expected: FAIL

- [ ] **Step 5: 実装**

`lib/init.sh`:

```bash
#!/usr/bin/env bash
# pueue-agent init: プロジェクトに .pueue-agent/ を生成する

pa_gitignore_add() {  # 重複なしで .gitignore に1行追記
  local root="$1" entry="$2"
  grep -qx "$entry" "$root/.gitignore" 2>/dev/null && return 0
  echo "$entry" >> "$root/.gitignore"
}

pa_sanitize_group() {
  echo "$1" | tr '[:upper:]' '[:lower:]' | sed -e 's/[^a-z0-9]\{1,\}/-/g' -e 's/^-//' -e 's/-$//'
}

pa_cmd_init() {
  local agent_cmd="" git_mode="" group="" target=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --agent-cmd) agent_cmd="$2"; shift 2 ;;
      --git-mode)  git_mode="$2";  shift 2 ;;
      --group)     group="$2";     shift 2 ;;
      *)           target="$1";    shift ;;
    esac
  done
  target="${target:-$PWD}"
  target="$(cd "$target" && pwd)" || pa_die "no such dir: $target"
  [ -d "$target/.pueue-agent" ] && pa_die "already initialized: $target/.pueue-agent"

  # 対話フォールバック(TTY のみ)
  if [ -z "$agent_cmd" ]; then
    if [ -t 0 ]; then
      printf 'agent コマンド (例: claude -p {prompt} --permission-mode acceptEdits): ' >&2
      read -r agent_cmd
    else
      pa_die "--agent-cmd is required (non-interactive)"
    fi
  fi
  case "$agent_cmd" in
    *'{prompt}'*) : ;;
    *) pa_die "agent command must contain {prompt}" ;;
  esac
  if [ -z "$git_mode" ]; then
    if [ -t 0 ]; then
      printf 'git 管理 [commit/ignore/mixed] (default: mixed): ' >&2
      read -r git_mode
      git_mode="${git_mode:-mixed}"
    else
      pa_die "--git-mode is required (non-interactive)"
    fi
  fi
  case "$git_mode" in commit|ignore|mixed) : ;; *) pa_die "invalid --git-mode: $git_mode" ;; esac

  [ -n "$group" ] || group="pa-$(pa_sanitize_group "$(basename "$target")")"

  mkdir -p "$target/.pueue-agent/logs"
  # config: テンプレートに agent command と group を差し込む
  sed \
    -e "s|^  command: .*|  command: \"$(printf '%s' "$agent_cmd" | sed 's/[&|]/\\&/g')\"|" \
    -e "s|^  group: .*|  group: \"$group\"|" \
    "$PA_ROOT/templates/config.yml" > "$target/.pueue-agent/config.yml"
  cp "$PA_ROOT/templates/STATE.md" "$target/.pueue-agent/STATE.md"
  sed -e "s|{{GROUP}}|$group|g" "$PA_ROOT/templates/instructions.md" \
    > "$target/.pueue-agent/instructions.md"

  case "$git_mode" in
    ignore) pa_gitignore_add "$target" ".pueue-agent/" ;;
    mixed)  pa_gitignore_add "$target" ".pueue-agent/STATE.md"
            pa_gitignore_add "$target" ".pueue-agent/logs/" ;;
  esac

  echo "initialized $target/.pueue-agent (group: $group)"
  echo "next steps:"
  echo "  1. .pueue-agent/STATE.md に実験の目的・方針を書く"
  echo "  2. pueue-agent enable   # 監視を有効化"
  echo "  3. pueue-agent submit -- <実験コマンド>"
}
```

- [ ] **Step 6: テストが通ることを確認 + shellcheck + Commit**

```bash
bats tests/test_init.bats && shellcheck -x lib/init.sh
git add -A && git commit -m "feat: init subcommand with templates and git-mode options"
```

---

### Task 5: wake_agent.sh(ガードレール + agent 起動)

**Files:**
- Create: `lib/wake_agent.sh`, `tests/helpers/mock_agent.sh`
- Test: `tests/test_wake.bats`

**Interfaces:**
- Consumes: `pa_set_project`, `pa_config`, `pa_log`(Task 1-2)、`pa_notify`(Task 3)
- Produces: `pa_cmd_wake MODE PROJECT_DIR [TASK_ID] [RESULT]` — MODE は `crash|stalled|deep_check|task_finished`。RESULT は `Success` または `Failed:N`(callback/sentinel が渡す)
- Produces: ガードレール挙動(下記テストが仕様)。カウンタ増減はすべてここで行い、agent には任せない
- Produces: agent 実行 — `agent.command` の `{prompt}` を文字列 `"$PA_PROMPT"` に置換し、`PA_PROMPT` を export して `bash -c "cd '$PA_PROJECT' && <cmd>"` で実行。stdout/stderr は `logs/agent_<epoch>.log` へ。`timeout` コマンドがあれば `agent.timeout_minutes` 分でタイムアウト。非ゼロ終了は `agent.max_retries` 回までリトライ

- [ ] **Step 1: モック agent を書く**

`tests/helpers/mock_agent.sh`:

```bash
#!/usr/bin/env bash
# テスト用 agent: 呼び出し記録を残す。MOCK_AGENT_EXIT で終了コード制御。
echo "PROMPT:$1" >> "${MOCK_AGENT_LOG:?}"
exit "${MOCK_AGENT_EXIT:-0}"
```

`chmod +x tests/helpers/mock_agent.sh`

- [ ] **Step 2: 失敗するテストを書く**

`tests/test_wake.bats`:

```bash
load helpers/setup

setup() {
  proj="$(make_project)"
  export MOCK_AGENT_LOG="$BATS_TEST_TMPDIR/agent_calls.log"
  # config の agent.command をモックに差し替え
  sed -i.bak \
    "s|^  command: .*|  command: \"$REPO_ROOT/tests/helpers/mock_agent.sh {prompt}\"|" \
    "$proj/.pueue-agent/config.yml"
  logs="$proj/.pueue-agent/logs"
}

@test "crash mode launches agent with crash prompt and increments failures" {
  run "$PA_BIN" wake crash "$proj" 7 "Failed:1"
  [ "$status" -eq 0 ]
  grep -q "PROMPT:" "$MOCK_AGENT_LOG"
  grep -q "mode: crash" "$MOCK_AGENT_LOG"
  grep -q "task id: 7" "$MOCK_AGENT_LOG"
  [ "$(cat "$logs/consec_failures")" = "1" ]
}

@test "halts without launching agent when failures reach max" {
  echo 2 > "$logs/consec_failures"   # max は 3 (template default)
  run "$PA_BIN" wake crash "$proj" 8 "Failed:1"
  [ ! -f "$MOCK_AGENT_LOG" ]         # agent は起動されない
  [ -f "$logs/halted" ]
  grep -q "halted" "$logs/../logs/notifications.log"
}

@test "task_finished Success resets failures and increments experiment count" {
  echo 2 > "$logs/consec_failures"
  run "$PA_BIN" wake task_finished "$proj" 9 "Success"
  [ "$(cat "$logs/consec_failures")" = "0" ]
  [ "$(cat "$logs/experiment_count")" = "1" ]
  grep -q "mode: task_finished" "$MOCK_AGENT_LOG"
}

@test "halts when experiment count reaches max" {
  echo 20 > "$logs/experiment_count"  # max_experiments = 20 で 21 個目
  run "$PA_BIN" wake task_finished "$proj" 10 "Success"
  [ ! -f "$MOCK_AGENT_LOG" ]
  [ -f "$logs/halted" ]
}

@test "does nothing when halted" {
  echo "manual" > "$logs/halted"
  run "$PA_BIN" wake deep_check "$proj"
  [ "$status" -eq 0 ]
  [ ! -f "$MOCK_AGENT_LOG" ]
}

@test "lock prevents concurrent wake" {
  mkdir -p "$logs/lock"
  echo $$ > "$logs/lock/pid"         # 生きている PID = agent 稼働中とみなす
  run "$PA_BIN" wake deep_check "$proj"
  [ "$status" -eq 0 ]
  [ ! -f "$MOCK_AGENT_LOG" ]
}

@test "stale lock is reclaimed" {
  mkdir -p "$logs/lock"
  echo 99999999 > "$logs/lock/pid"   # 存在しない PID
  run "$PA_BIN" wake deep_check "$proj"
  grep -q "mode: deep_check" "$MOCK_AGENT_LOG"
}

@test "agent failure is retried then halts with notification" {
  export MOCK_AGENT_EXIT=1           # 常に失敗 (max_retries=2 → 計3回試行)
  run "$PA_BIN" wake deep_check "$proj"
  [ "$(grep -c PROMPT "$MOCK_AGENT_LOG")" -eq 3 ]
  [ -f "$logs/halted" ]
  grep -q "agent_error" "$logs/notifications.log"
}

@test "prompt contains instructions and state references" {
  run "$PA_BIN" wake deep_check "$proj"
  grep -q ".pueue-agent/instructions.md" "$MOCK_AGENT_LOG"
  grep -q ".pueue-agent/STATE.md" "$MOCK_AGENT_LOG"
}

@test "invalid mode fails" {
  run "$PA_BIN" wake bogus "$proj"
  [ "$status" -eq 1 ]
}
```

setup 内で `make_project` が config.yml をコピーするため、`templates/config.yml` が必要(Task 2 済み)。STATE.md / instructions.md も必要なので `make_project` を拡張する:

`tests/helpers/setup.bash` の `make_project` を以下に置き換え:

```bash
make_project() {
  local dir="$BATS_TEST_TMPDIR/proj"
  mkdir -p "$dir/.pueue-agent/logs"
  sed 's/^  group: .*/  group: "pa-proj"/' "$REPO_ROOT/templates/config.yml" \
    > "$dir/.pueue-agent/config.yml"
  cp "$REPO_ROOT/templates/STATE.md" "$dir/.pueue-agent/STATE.md"
  sed 's/{{GROUP}}/pa-proj/g' "$REPO_ROOT/templates/instructions.md" \
    > "$dir/.pueue-agent/instructions.md"
  echo "$dir"
}
```

- [ ] **Step 3: テストが失敗することを確認**

Run: `bats tests/test_wake.bats`
Expected: FAIL

- [ ] **Step 4: 実装**

`lib/wake_agent.sh`:

```bash
#!/usr/bin/env bash
# 唯一の agent 起動口。ガードレール判定 → プロンプト組み立て → agent 実行。
# usage: pa_cmd_wake MODE PROJECT_DIR [TASK_ID] [RESULT]

source "$PA_ROOT/lib/notify.sh"

pa_counter_get() { [ -f "$PA_DIR/logs/$1" ] && cat "$PA_DIR/logs/$1" || echo 0; }
pa_counter_set() { echo "$2" > "$PA_DIR/logs/$1"; }

pa_halt() {  # 理由を記録して停止状態にし、通知
  echo "$1" > "$PA_DIR/logs/halted"
  pa_notify halted "$1 — 'pueue-agent resume' で再開できます"
}

pa_acquire_lock() {
  local lock="$PA_DIR/logs/lock"
  if mkdir "$lock" 2>/dev/null; then
    echo $$ > "$lock/pid"
    return 0
  fi
  # stale lock 回収: 記録された PID が生きていなければ奪う
  local pid
  pid="$(cat "$lock/pid" 2>/dev/null || echo 0)"
  if [ "$pid" -gt 0 ] && kill -0 "$pid" 2>/dev/null; then
    return 1   # 稼働中
  fi
  echo $$ > "$lock/pid"
  return 0
}

pa_release_lock() { rm -rf "$PA_DIR/logs/lock"; }

pa_build_prompt() {  # $1=mode $2=task_id $3=result
  local mode="$1" task_id="${2-}" result="${3-}"
  local group
  group="$(pa_config pueue.group)"
  cat <<EOF
あなたは pueue-agent によって起動された自律実験管理 agent です。
mode: $mode
EOF
  [ -n "$task_id" ] && echo "task id: $task_id (result: ${result:-unknown})"
  cat <<EOF
pueue group: $group

まず .pueue-agent/instructions.md を読み、指示に従ってください。
次に .pueue-agent/STATE.md を読み、経緯を把握してから作業してください。
タスク出力は 'pueue log $task_id' で読めます。
作業を終える前に必ず STATE.md を更新してください。
EOF
}

pa_run_agent() {  # $1=prompt → 0/1
  local template cmd timeout_min retries attempt out
  template="$(pa_config agent.command)"
  timeout_min="$(pa_config agent.timeout_minutes 60)"
  retries="$(pa_config agent.max_retries 2)"
  cmd="${template//\{prompt\}/\"\$PA_PROMPT\"}"
  export PA_PROMPT="$1"
  attempt=0
  while [ "$attempt" -le "$retries" ]; do
    out="$PA_DIR/logs/agent_$(date +%s).log"
    pa_log "launching agent (attempt $((attempt + 1))): mode context in $out"
    if command -v timeout >/dev/null 2>&1; then
      timeout "$((timeout_min * 60))" bash -c "cd '$PA_PROJECT' && $cmd" \
        > "$out" 2>&1 && return 0
    else
      bash -c "cd '$PA_PROJECT' && $cmd" > "$out" 2>&1 && return 0
    fi
    pa_log "agent exited nonzero (attempt $((attempt + 1))), log: $out"
    attempt=$((attempt + 1))
    sleep 1
  done
  return 1
}

pa_cmd_wake() {
  local mode="${1-}" proj="${2-}" task_id="${3-}" result="${4-}"
  case "$mode" in
    crash|stalled|deep_check|task_finished) : ;;
    *) pa_die "invalid wake mode: ${mode:-<none>}" ;;
  esac
  [ -n "$proj" ] || pa_die "wake: project dir required"
  pa_set_project "$proj"

  # 1. 停止状態なら何もしない
  [ -f "$PA_DIR/logs/halted" ] && { pa_log "wake($mode) skipped: halted"; return 0; }

  # 2. 多重起動防止
  if ! pa_acquire_lock; then
    pa_log "wake($mode) skipped: another agent is running"
    return 0
  fi
  trap pa_release_lock EXIT

  # 3. カウンタ更新とガードレール(bash 側で完結。agent には任せない)
  local max_fail max_exp n
  max_fail="$(pa_config guardrails.max_consecutive_failures 3)"
  max_exp="$(pa_config guardrails.max_experiments 20)"
  case "$mode" in
    crash|stalled)
      n=$(( $(pa_counter_get consec_failures) + 1 ))
      pa_counter_set consec_failures "$n"
      if [ "$n" -ge "$max_fail" ]; then
        pa_halt "連続失敗が $n 回に達したため停止(人の介入待ち)"
        return 0
      fi
      ;;
    task_finished)
      n=$(( $(pa_counter_get experiment_count) + 1 ))
      pa_counter_set experiment_count "$n"
      case "$result" in Success*) pa_counter_set consec_failures 0 ;; esac
      if [ "$n" -gt "$max_exp" ]; then
        pa_halt "通算実験数が上限 ($max_exp) に達したため停止"
        return 0
      fi
      ;;
  esac

  # 4. agent 起動
  local prompt
  prompt="$(pa_build_prompt "$mode" "$task_id" "$result")"
  if pa_run_agent "$prompt"; then
    case "$mode" in
      crash|stalled)  pa_notify intervention "mode=$mode task=$task_id: agent が介入しました。詳細は STATE.md" ;;
      task_finished)  pa_notify task_finished "task=$task_id ($result) 完了処理済み。結果は STATE.md" ;;
      deep_check)     pa_log "deep_check completed" ;;
    esac
  else
    pa_halt "agent の起動が $(pa_config agent.max_retries 2) 回のリトライ後も失敗"
    pa_notify agent_error "agent 実行が失敗しました。logs/agent_*.log を確認してください"
  fi
}
```

- [ ] **Step 5: テストが通ることを確認**

Run: `bats tests/test_wake.bats`
Expected: 全 pass。注意: 「halts when experiment count reaches max」テストは実装の `-gt` 判定と整合すること(20 個目まで許可、21 個目で停止。テストは `echo 20` 済みで 21 個目の wake なので halt)

- [ ] **Step 6: 全テスト + shellcheck + Commit**

```bash
bats tests/ && shellcheck -x lib/wake_agent.sh
git add -A && git commit -m "feat: wake_agent with bash-side guardrails and agent launcher"
```

---

### Task 6: sentinel.sh(定期チェック)

**Files:**
- Create: `lib/sentinel.sh`, `tests/helpers/mock_pueue.sh`
- Test: `tests/test_sentinel.bats`

**Interfaces:**
- Consumes: `pa_set_project`, `pa_config`, `pa_config_list`, `pa_log`(Task 1-2)、`pa_cmd_wake`(Task 5、`"$PA_ROOT/bin/pueue-agent" wake ...` として子プロセス起動)
- Produces: `pa_cmd_sentinel [PROJECT_DIR]` — 機械チェックを行い、必要時のみ wake を呼ぶ。トークン消費(agent 起動)は異常時と deep check 時のみ
- Produces: タスクログ探索 `pa_task_log_file TASK_ID` — `$PA_TASK_LOG_DIR` が set ならそこ、無ければ `${XDG_DATA_HOME:-$HOME/.local/share}/pueue/task_logs` → `$HOME/Library/Application Support/pueue/task_logs` の順で `<id>.log` を探す

- [ ] **Step 1: モック pueue を書く**

`tests/helpers/mock_pueue.sh`:

```bash
#!/usr/bin/env bash
# テスト用 pueue。MOCK_PUEUE_STATUS_JSON のファイル内容を status --json で返す。
case "$1" in
  status) cat "${MOCK_PUEUE_STATUS_JSON:?}" ;;
  *) echo "mock_pueue: unhandled: $*" >&2; exit 1 ;;
esac
```

`chmod +x tests/helpers/mock_pueue.sh`

- [ ] **Step 2: 失敗するテストを書く**

status JSON フィクスチャは実測形状(pueue 4.0.4)に合わせる。`tests/test_sentinel.bats`:

```bash
load helpers/setup

setup() {
  proj="$(make_project)"
  logs="$proj/.pueue-agent/logs"
  export PA_PUEUE_BIN="$REPO_ROOT/tests/helpers/mock_pueue.sh"
  export MOCK_PUEUE_STATUS_JSON="$BATS_TEST_TMPDIR/status.json"
  export MOCK_AGENT_LOG="$BATS_TEST_TMPDIR/agent_calls.log"
  export PA_TASK_LOG_DIR="$BATS_TEST_TMPDIR/task_logs"
  mkdir -p "$PA_TASK_LOG_DIR"
  sed -i.bak \
    "s|^  command: .*|  command: \"$REPO_ROOT/tests/helpers/mock_agent.sh {prompt}\"|" \
    "$proj/.pueue-agent/config.yml"
}

# フィクスチャ生成ヘルパ (実測した pueue 4.0.4 の形状)
running_status() {  # $1=task_id
  cat > "$MOCK_PUEUE_STATUS_JSON" <<EOF
{"tasks":{"$1":{"id":$1,"group":"pa-proj","command":"python train.py",
 "status":{"Running":{"enqueued_at":"x","start":"x"}}}}}
EOF
}
failed_status() {   # $1=task_id
  cat > "$MOCK_PUEUE_STATUS_JSON" <<EOF
{"tasks":{"$1":{"id":$1,"group":"pa-proj","command":"python train.py",
 "status":{"Done":{"enqueued_at":"x","start":"x","end":"x","result":{"Failed":1}}}}}}
EOF
}
empty_status() { echo '{"tasks":{}}' > "$MOCK_PUEUE_STATUS_JSON"; }

@test "healthy running task: no agent launch, counter increments" {
  running_status 3
  echo "step 100 loss 0.5" > "$PA_TASK_LOG_DIR/3.log"
  run "$PA_BIN" sentinel "$proj"
  [ "$status" -eq 0 ]
  [ ! -f "$MOCK_AGENT_LOG" ]
  [ "$(cat "$logs/check_count")" = "1" ]
}

@test "no tasks at all: exits quietly without counting" {
  empty_status
  run "$PA_BIN" sentinel "$proj"
  [ "$status" -eq 0 ]
  [ ! -f "$MOCK_AGENT_LOG" ]
  [ ! -f "$logs/check_count" ]
}

@test "failed task triggers crash wake once and is deduped" {
  failed_status 4
  echo "Traceback ..." > "$PA_TASK_LOG_DIR/4.log"
  "$PA_BIN" sentinel "$proj"
  grep -q "mode: crash" "$MOCK_AGENT_LOG"
  # 2回目は handled 済みなので起動しない
  rm "$MOCK_AGENT_LOG"
  "$PA_BIN" sentinel "$proj"
  [ ! -f "$MOCK_AGENT_LOG" ]
}

@test "error pattern in running task output triggers crash wake" {
  running_status 5
  printf 'step 10\nloss: NaN\n' > "$PA_TASK_LOG_DIR/5.log"
  run "$PA_BIN" sentinel "$proj"
  grep -q "mode: crash" "$MOCK_AGENT_LOG"
}

@test "stalled output triggers stalled wake" {
  running_status 6
  echo "step 1" > "$PA_TASK_LOG_DIR/6.log"
  # 前回スナップショット: 同サイズ・31分前 (stall_minutes=30)
  size=$(wc -c < "$PA_TASK_LOG_DIR/6.log" | tr -d ' ')
  echo "$size $(( $(date +%s) - 1860 ))" > "$logs/progress_6"
  run "$PA_BIN" sentinel "$proj"
  grep -q "mode: stalled" "$MOCK_AGENT_LOG"
}

@test "growing output updates snapshot, no wake" {
  running_status 6
  echo "step 1 more output" > "$PA_TASK_LOG_DIR/6.log"
  echo "1 $(( $(date +%s) - 1860 ))" > "$logs/progress_6"   # サイズが変わっている
  run "$PA_BIN" sentinel "$proj"
  [ ! -f "$MOCK_AGENT_LOG" ]
  # スナップショットが更新されている
  size=$(wc -c < "$PA_TASK_LOG_DIR/6.log" | tr -d ' ')
  [[ "$(cat "$logs/progress_6")" == "$size "* ]]
}

@test "deep check fires on Nth healthy check and resets counter" {
  running_status 7
  echo ok > "$PA_TASK_LOG_DIR/7.log"
  echo 5 > "$logs/check_count"   # deep_check_every=6 → 今回が6回目
  run "$PA_BIN" sentinel "$proj"
  grep -q "mode: deep_check" "$MOCK_AGENT_LOG"
  [ "$(cat "$logs/check_count")" = "0" ]
}

@test "other groups' tasks are ignored" {
  cat > "$MOCK_PUEUE_STATUS_JSON" <<'EOF'
{"tasks":{"9":{"id":9,"group":"other","command":"x",
 "status":{"Done":{"result":{"Failed":1}}}}}}
EOF
  run "$PA_BIN" sentinel "$proj"
  [ ! -f "$MOCK_AGENT_LOG" ]
}
```

- [ ] **Step 3: テストが失敗することを確認**

Run: `bats tests/test_sentinel.bats`
Expected: FAIL

- [ ] **Step 4: 実装**

`lib/sentinel.sh`:

```bash
#!/usr/bin/env bash
# 定期チェック(cron から起動)。正常時は agent を起動せず終了 = トークンゼロ。

pa_task_log_file() {
  local id="$1" d
  for d in "${PA_TASK_LOG_DIR-}" \
           "${XDG_DATA_HOME:-$HOME/.local/share}/pueue/task_logs" \
           "$HOME/Library/Application Support/pueue/task_logs"; do
    [ -n "$d" ] && [ -f "$d/$id.log" ] && { echo "$d/$id.log"; return 0; }
  done
  return 1
}

pa_wake() {  # 子プロセスで wake を起動(sentinel 自身の状態を汚さない)
  "$PA_ROOT/bin/pueue-agent" wake "$@"
}

pa_check_task_output() {  # $1=task_id → 出力: "crash"|"stalled"|"" (正常)
  local id="$1" logfile size prev prev_size prev_ts now stall_sec pat
  logfile="$(pa_task_log_file "$id")" || return 0

  # エラーパターン(末尾16KB)
  while IFS= read -r pat; do
    [ -n "$pat" ] || continue
    if tail -c 16384 "$logfile" | grep -Eq "$pat"; then
      echo "crash"
      return 0
    fi
  done <<EOF
$(pa_config_list check.error_patterns)
EOF

  # 停滞検知
  now="$(date +%s)"
  stall_sec=$(( $(pa_config check.stall_minutes 30) * 60 ))
  size="$(wc -c < "$logfile" | tr -d ' ')"
  if [ -f "$PA_DIR/logs/progress_$id" ]; then
    prev="$(cat "$PA_DIR/logs/progress_$id")"
    prev_size="${prev%% *}"
    prev_ts="${prev##* }"
    if [ "$size" = "$prev_size" ]; then
      if [ $(( now - prev_ts )) -ge "$stall_sec" ]; then
        echo "stalled"
      fi
      return 0   # サイズ不変: スナップショットは更新しない
    fi
  fi
  echo "$size $now" > "$PA_DIR/logs/progress_$id"
}

pa_cmd_sentinel() {
  local proj
  proj="$(pa_find_project "${1-}")" || pa_die "no .pueue-agent found"
  pa_set_project "$proj"
  local group tasks_json
  group="$(pa_config pueue.group)"
  [ -n "$group" ] || pa_die "pueue.group not set in config"

  tasks_json="$(${PA_PUEUE_BIN:-pueue} status --json)" || pa_die "pueue status failed"

  # 1) failed / killed タスク(未処理のもの)→ crash wake
  local failed_ids id
  failed_ids="$(echo "$tasks_json" | jq -r --arg g "$group" '
    .tasks | to_entries[] | .value
    | select(.group == $g)
    | select((.status.Done.result? // empty) | type == "object" or . == "Killed")
    | .id')"
  for id in $failed_ids; do
    if ! grep -qx "$id" "$PA_DIR/logs/handled_tasks" 2>/dev/null; then
      echo "$id" >> "$PA_DIR/logs/handled_tasks"
      pa_log "sentinel: task $id failed -> wake crash"
      pa_wake crash "$PA_PROJECT" "$id" "Failed"
      return 0
    fi
  done

  # 2) 実行中タスクの出力チェック
  local running_ids verdict
  running_ids="$(echo "$tasks_json" | jq -r --arg g "$group" '
    .tasks | to_entries[] | .value
    | select(.group == $g) | select(.status.Running?) | .id')"
  for id in $running_ids; do
    verdict="$(pa_check_task_output "$id")"
    if [ -n "$verdict" ]; then
      local mode="$verdict"
      [ "$mode" = "crash" ] && pa_log "sentinel: error pattern in task $id output" \
                            || pa_log "sentinel: task $id output stalled"
      pa_wake "$mode" "$PA_PROJECT" "$id" ""
      return 0
    fi
  done

  # 3) 実行中タスクが無ければ何もしない(完了処理は callback の担当)
  [ -n "$running_ids" ] || { pa_log "sentinel: idle"; return 0; }

  # 4) 正常 → deep check 判定
  local every interval_min now last count
  every="$(pa_config check.deep_check_every 6)"
  interval_min="$(pa_config check.deep_check_interval_minutes 0)"
  now="$(date +%s)"
  if [ "$interval_min" -gt 0 ]; then
    last="$( [ -f "$PA_DIR/logs/last_deep_check" ] && cat "$PA_DIR/logs/last_deep_check" || echo 0 )"
    if [ $(( now - last )) -ge $(( interval_min * 60 )) ]; then
      echo "$now" > "$PA_DIR/logs/last_deep_check"
      pa_wake deep_check "$PA_PROJECT"
    fi
  else
    count=$(( $( [ -f "$PA_DIR/logs/check_count" ] && cat "$PA_DIR/logs/check_count" || echo 0 ) + 1 ))
    if [ "$count" -ge "$every" ]; then
      echo 0 > "$PA_DIR/logs/check_count"
      pa_wake deep_check "$PA_PROJECT"
    else
      echo "$count" > "$PA_DIR/logs/check_count"
    fi
  fi
}
```

実装ノート:
- `extra_log_paths` のチェック: `pa_check_task_output` はタスクログのみ対象。extra パスは `pa_cmd_sentinel` の 2) の後に同じパターン検査を追加で回す(サイズスナップショットは `progress_extra_<パスのmd5>` に保存…は不要。**YAGNI: extra_log_paths はエラーパターン検査のみ**とし、下記のループを 2) の直後に置く):

```bash
  local extra
  while IFS= read -r extra; do
    [ -n "$extra" ] && [ -f "$PA_PROJECT/$extra" ] || continue
    while IFS= read -r pat; do
      [ -n "$pat" ] || continue
      if tail -c 16384 "$PA_PROJECT/$extra" | grep -Eq "$pat"; then
        pa_log "sentinel: error pattern in extra log $extra"
        pa_wake crash "$PA_PROJECT" "" ""
        return 0
      fi
    done <<PATEOF
$(pa_config_list check.error_patterns)
PATEOF
  done <<EXTRAEOF
$(pa_config_list check.extra_log_paths)
EXTRAEOF
```

- Killed の status JSON は `{"Done":{"result":"Killed"}}`(文字列)。jq の select は「result がオブジェクト(Failed)または "Killed"」を拾う形にしてある

- [ ] **Step 5: テストが通ることを確認 + shellcheck + Commit**

```bash
bats tests/test_sentinel.bats && bats tests/ && shellcheck -x lib/sentinel.sh
git add -A && git commit -m "feat: sentinel periodic checks with stall/error detection and deep check"
```

---

### Task 7: callback dispatcher + プロジェクト登録簿

**Files:**
- Create: `lib/callback.sh`
- Test: `tests/test_callback.bats`

**Interfaces:**
- Consumes: `pa_die`(Task 1)、`pueue-agent wake`(Task 5)
- Produces: `pa_cmd_callback TASK_ID GROUP` — 登録簿 `${PA_REGISTRY:-~/.config/pueue-agent/projects}` から GROUP に対応するプロジェクトを引き、`pueue status --json` でタスクの result を取得して `wake task_finished`(Success)または `wake crash`(Failed/Killed)を起動。未登録 group は静かに exit 0
- Produces: 登録簿ヘルパ `pa_registry_file`(`$PA_REGISTRY` 優先)、`pa_registry_add GROUP PATH`、`pa_registry_remove GROUP`、`pa_registry_lookup GROUP`(Task 8 が消費)

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_callback.bats`:

```bash
load helpers/setup

setup() {
  proj="$(make_project)"
  export PA_REGISTRY="$BATS_TEST_TMPDIR/registry"
  export PA_PUEUE_BIN="$REPO_ROOT/tests/helpers/mock_pueue.sh"
  export MOCK_PUEUE_STATUS_JSON="$BATS_TEST_TMPDIR/status.json"
  export MOCK_AGENT_LOG="$BATS_TEST_TMPDIR/agent_calls.log"
  sed -i.bak \
    "s|^  command: .*|  command: \"$REPO_ROOT/tests/helpers/mock_agent.sh {prompt}\"|" \
    "$proj/.pueue-agent/config.yml"
  printf 'pa-proj\t%s\n' "$proj" > "$PA_REGISTRY"
}

@test "successful task dispatches task_finished wake" {
  cat > "$MOCK_PUEUE_STATUS_JSON" <<'EOF'
{"tasks":{"5":{"id":5,"group":"pa-proj",
 "status":{"Done":{"result":"Success"}}}}}
EOF
  run "$PA_BIN" callback 5 pa-proj
  [ "$status" -eq 0 ]
  grep -q "mode: task_finished" "$MOCK_AGENT_LOG"
  grep -q "result: Success" "$MOCK_AGENT_LOG"
}

@test "failed task dispatches crash wake and dedupes with sentinel" {
  cat > "$MOCK_PUEUE_STATUS_JSON" <<'EOF'
{"tasks":{"6":{"id":6,"group":"pa-proj",
 "status":{"Done":{"result":{"Failed":1}}}}}}
EOF
  run "$PA_BIN" callback 6 pa-proj
  grep -q "mode: crash" "$MOCK_AGENT_LOG"
  grep -qx "6" "$proj/.pueue-agent/logs/handled_tasks"
}

@test "unknown group exits 0 silently" {
  run "$PA_BIN" callback 1 some-other-group
  [ "$status" -eq 0 ]
  [ "$output" = "" ]
}

@test "registry add/lookup/remove roundtrip" {
  run bash -c "source '$REPO_ROOT/lib/common.sh' && PA_ROOT='$REPO_ROOT' source '$REPO_ROOT/lib/callback.sh' && \
    pa_registry_add g2 /tmp/x && pa_registry_lookup g2"
  [ "$output" = "/tmp/x" ]
  run bash -c "source '$REPO_ROOT/lib/common.sh' && PA_ROOT='$REPO_ROOT' source '$REPO_ROOT/lib/callback.sh' && \
    pa_registry_add g2 /tmp/x && pa_registry_remove g2 && pa_registry_lookup g2"
  [ "$status" -eq 1 ]
}
```

- [ ] **Step 2: テストが失敗することを確認**

Run: `bats tests/test_callback.bats`
Expected: FAIL

- [ ] **Step 3: 実装**

`lib/callback.sh`:

```bash
#!/usr/bin/env bash
# pueue の daemon callback から起動される。group からプロジェクトを逆引きし、
# タスク結果に応じて wake に dispatch する。

pa_registry_file() {
  echo "${PA_REGISTRY:-${XDG_CONFIG_HOME:-$HOME/.config}/pueue-agent/projects}"
}

pa_registry_add() {  # $1=group $2=path
  local f
  f="$(pa_registry_file)"
  mkdir -p "$(dirname "$f")"
  pa_registry_remove "$1"
  printf '%s\t%s\n' "$1" "$2" >> "$f"
}

pa_registry_remove() {  # $1=group
  local f tmp
  f="$(pa_registry_file)"
  [ -f "$f" ] || return 0
  tmp="$f.tmp.$$"
  awk -F'\t' -v g="$1" '$1 != g' "$f" > "$tmp" && mv "$tmp" "$f"
}

pa_registry_lookup() {  # $1=group → path or return 1
  local f path
  f="$(pa_registry_file)"
  [ -f "$f" ] || return 1
  path="$(awk -F'\t' -v g="$1" '$1 == g { print $2; exit }' "$f")"
  [ -n "$path" ] && echo "$path" || return 1
}

pa_cmd_callback() {
  local task_id="${1-}" group="${2-}" proj result
  [ -n "$task_id" ] && [ -n "$group" ] || pa_die "callback: task_id and group required"
  proj="$(pa_registry_lookup "$group")" || return 0   # 監視対象外の group

  # result を pueue 本体から取得(callback テンプレート変数に依存しない)
  result="$(${PA_PUEUE_BIN:-pueue} status --json | jq -r --arg id "$task_id" '
    .tasks[$id].status.Done.result
    | if type == "object" then "Failed:\(.Failed)" else . end' 2>/dev/null)"

  case "$result" in
    Success)
      "$PA_ROOT/bin/pueue-agent" wake task_finished "$proj" "$task_id" "Success" ;;
    Failed:*|Killed)
      # sentinel との二重処理防止: handled_tasks に記録してから crash wake
      mkdir -p "$proj/.pueue-agent/logs"
      grep -qx "$task_id" "$proj/.pueue-agent/logs/handled_tasks" 2>/dev/null \
        || echo "$task_id" >> "$proj/.pueue-agent/logs/handled_tasks"
      "$PA_ROOT/bin/pueue-agent" wake crash "$proj" "$task_id" "$result" ;;
    *)
      : ;;  # Done ではない or タスクなし: 何もしない
  esac
}
```

- [ ] **Step 4: テストが通ることを確認 + shellcheck + Commit**

```bash
bats tests/test_callback.bats && shellcheck -x lib/callback.sh
git add -A && git commit -m "feat: pueue callback dispatcher with project registry"
```

---

### Task 8: enable / disable / submit(cron・callback 設定・group 管理)

**Files:**
- Create: `lib/manage.sh`, `tests/helpers/mock_crontab.sh`
- Test: `tests/test_manage.bats`

**Interfaces:**
- Consumes: Task 1-2 の common、Task 7 の registry ヘルパ(`source "$PA_ROOT/lib/callback.sh"` で読み込む)
- Produces: `pa_cmd_enable [dir]` — ① `pueue group add <group>`(既存なら無視)② registry 登録 ③ crontab にエントリ追加(マーカー `# pueue-agent:<project>` 付き、`*/<interval> * * * *`)④ pueue.yml の `callback: null` をパッチ(既にツールの callback なら何もしない、別の値なら警告して手動設定を案内)⑤ callback を変更した場合「pueued の再起動が必要(実行中タスクが無いときに)」と表示
- Produces: `pa_cmd_disable [dir]` — cron エントリ除去 + registry 除去 + `pueue group remove`(タスクが残っていれば警告して group は残す)。callback 設定は触らない(他プロジェクトが使うため)
- Produces: `pa_cmd_submit [dir が cwd] -- CMD...` — `pueue add -g <group> -- CMD...`
- Produces: crontab 操作は `${PA_CRONTAB_BIN:-crontab}` 経由(テストで差し替え)。pueue.yml の場所は `${PA_PUEUE_CONFIG:-自動検出}`(`~/.config/pueue/pueue.yml` → `~/Library/Application Support/pueue/pueue.yml`)

- [ ] **Step 1: モック crontab を書く**

`tests/helpers/mock_crontab.sh`:

```bash
#!/usr/bin/env bash
# crontab モック: MOCK_CRONTAB_FILE を読み書きする
f="${MOCK_CRONTAB_FILE:?}"
case "${1-}" in
  -l) [ -f "$f" ] && cat "$f" || exit 1 ;;
  -)  cat > "$f" ;;
  *)  cat "$2" > "$f" ;;
esac
```

`chmod +x tests/helpers/mock_crontab.sh`

- [ ] **Step 2: 失敗するテストを書く**

`tests/test_manage.bats`:

```bash
load helpers/setup

setup() {
  proj="$(make_project)"
  export PA_REGISTRY="$BATS_TEST_TMPDIR/registry"
  export PA_CRONTAB_BIN="$REPO_ROOT/tests/helpers/mock_crontab.sh"
  export MOCK_CRONTAB_FILE="$BATS_TEST_TMPDIR/crontab"
  export PA_PUEUE_BIN="$REPO_ROOT/tests/helpers/mock_pueue.sh"
  export MOCK_PUEUE_STATUS_JSON="$BATS_TEST_TMPDIR/status.json"
  export PA_PUEUE_CONFIG="$BATS_TEST_TMPDIR/pueue.yml"
  echo '{"tasks":{}}' > "$MOCK_PUEUE_STATUS_JSON"
  cat > "$PA_PUEUE_CONFIG" <<'EOF'
daemon:
  callback: null
  callback_log_lines: 10
EOF
}

@test "enable registers cron, registry, and patches callback" {
  run "$PA_BIN" enable "$proj"
  [ "$status" -eq 0 ]
  grep -q "sentinel $proj" "$MOCK_CRONTAB_FILE"
  grep -q "pueue-agent:$proj" "$MOCK_CRONTAB_FILE"
  grep -q "\*/10 \* \* \* \*" "$MOCK_CRONTAB_FILE"
  grep -q $'^pa-proj\t' "$PA_REGISTRY"
  grep -q "pueue-agent' callback {{ id }} {{ group }}" "$PA_PUEUE_CONFIG"
  [[ "$output" == *"pueued"*  ]]   # 再起動の案内
}

@test "enable is idempotent (no duplicate cron lines)" {
  "$PA_BIN" enable "$proj"
  "$PA_BIN" enable "$proj"
  [ "$(grep -c "pueue-agent:$proj" "$MOCK_CRONTAB_FILE")" -eq 1 ]
}

@test "enable warns when callback is set to something else" {
  sed -i.bak 's|callback: null|callback: "notify-send hi"|' "$PA_PUEUE_CONFIG"
  run "$PA_BIN" enable "$proj"
  [ "$status" -eq 0 ]
  [[ "$output" == *"warning"* ]]
  grep -q 'callback: "notify-send hi"' "$PA_PUEUE_CONFIG"   # 上書きしない
}

@test "disable removes cron and registry" {
  "$PA_BIN" enable "$proj"
  run "$PA_BIN" disable "$proj"
  ! grep -q "pueue-agent:$proj" "$MOCK_CRONTAB_FILE"
  ! grep -q $'^pa-proj\t' "$PA_REGISTRY"
}

@test "disable keeps other projects' cron entries" {
  "$PA_BIN" enable "$proj"
  echo "* * * * * other-job # pueue-agent:/other/proj" >> "$MOCK_CRONTAB_FILE"
  "$PA_BIN" disable "$proj"
  grep -q "/other/proj" "$MOCK_CRONTAB_FILE"
}
```

submit のテスト: mock_pueue が `add` を記録するよう拡張:

`tests/helpers/mock_pueue.sh` を以下に置き換え:

```bash
#!/usr/bin/env bash
# テスト用 pueue。
case "$1" in
  status) cat "${MOCK_PUEUE_STATUS_JSON:?}" ;;
  add|group) echo "$*" >> "${MOCK_PUEUE_CALLS:-/dev/null}" ;;
  *) echo "mock_pueue: unhandled: $*" >&2; exit 1 ;;
esac
```

テスト追加:

```bash
@test "submit adds task to project group" {
  export MOCK_PUEUE_CALLS="$BATS_TEST_TMPDIR/pueue_calls"
  run bash -c "cd '$proj' && '$PA_BIN' submit -- python train.py --lr 0.1"
  [ "$status" -eq 0 ]
  grep -q -- "add -g pa-proj -- python train.py --lr 0.1" "$MOCK_PUEUE_CALLS"
}
```

- [ ] **Step 3: テストが失敗することを確認**

Run: `bats tests/test_manage.bats`
Expected: FAIL

- [ ] **Step 4: 実装**

`lib/manage.sh`(enable/disable/submit 部分。status/resume は Task 9 で追記):

```bash
#!/usr/bin/env bash
# enable / disable / status / resume / submit サブコマンド

source "$PA_ROOT/lib/callback.sh"   # registry ヘルパを利用
source "$PA_ROOT/lib/notify.sh"

pa_pueue_config_file() {
  if [ -n "${PA_PUEUE_CONFIG-}" ]; then echo "$PA_PUEUE_CONFIG"; return; fi
  local c
  for c in "${XDG_CONFIG_HOME:-$HOME/.config}/pueue/pueue.yml" \
           "$HOME/Library/Application Support/pueue/pueue.yml"; do
    [ -f "$c" ] && { echo "$c"; return; }
  done
  return 1
}

pa_cron_get() { ${PA_CRONTAB_BIN:-crontab} -l 2>/dev/null || true; }
pa_cron_set() { echo "$1" | ${PA_CRONTAB_BIN:-crontab} -; }

pa_cmd_enable() {
  local proj group interval marker current callback_cmd cfg
  proj="$(pa_find_project "${1-}")" || pa_die "no .pueue-agent found (run: pueue-agent init)"
  pa_set_project "$proj"
  group="$(pa_config pueue.group)"
  [ -n "$group" ] || pa_die "pueue.group not set in config"
  interval="$(pa_config check.interval_minutes 10)"

  # 1) pueue group(既存エラーは無視)
  ${PA_PUEUE_BIN:-pueue} group add "$group" >/dev/null 2>&1 || true

  # 2) registry
  pa_registry_add "$group" "$proj"

  # 3) cron(冪等)
  marker="# pueue-agent:$proj"
  current="$(pa_cron_get | grep -vF "$marker" || true)"
  pa_cron_set "$current
*/$interval * * * * '$PA_ROOT/bin/pueue-agent' sentinel '$proj' >> '$PA_DIR/logs/cron.log' 2>&1 $marker"

  # 4) pueue.yml の callback
  callback_cmd="'$PA_ROOT/bin/pueue-agent' callback {{ id }} {{ group }}"
  cfg="$(pa_pueue_config_file)" || pa_die "pueue config not found"
  if grep -q '^  callback: null$' "$cfg"; then
    sed -i.bak "s|^  callback: null$|  callback: \"$callback_cmd\"|" "$cfg"
    echo "pueue callback を設定しました。反映には pueued の再起動が必要です:"
    echo "  (実行中タスクが無いことを確認してから) pueue shutdown && pueued -d"
  elif grep -qF "pueue-agent" "$cfg"; then
    : # 既に設定済み
  else
    echo "warning: pueue.yml の callback が既に別の値です。手動で以下を設定してください:" >&2
    echo "  callback: \"$callback_cmd\"" >&2
  fi

  echo "enabled: group=$group, sentinel every $interval min"
}

pa_cmd_disable() {
  local proj group marker current
  proj="$(pa_find_project "${1-}")" || pa_die "no .pueue-agent found"
  pa_set_project "$proj"
  group="$(pa_config pueue.group)"

  marker="# pueue-agent:$proj"
  current="$(pa_cron_get | grep -vF "$marker" || true)"
  pa_cron_set "$current"
  pa_registry_remove "$group"
  if ! ${PA_PUEUE_BIN:-pueue} group remove "$group" >/dev/null 2>&1; then
    echo "warning: group '$group' にタスクが残っているため削除しませんでした" >&2
  fi
  echo "disabled: $proj"
}

pa_cmd_submit() {
  local proj group
  proj="$(pa_find_project)" || pa_die "no .pueue-agent found (cd into the project)"
  pa_set_project "$proj"
  group="$(pa_config pueue.group)"
  [ "${1-}" = "--" ] && shift
  [ $# -ge 1 ] || pa_die "usage: pueue-agent submit -- <command...>"
  ${PA_PUEUE_BIN:-pueue} add -g "$group" -- "$@"
}
```

実装ノート: `pa_cron_set` に渡す文字列の先頭に空行が入らないよう、`current` が空のときの整形に注意(`grep -v` の結果が空 → cron 行のみになるようにする。実装時に `printf '%s\n'` で整えて良い)。

- [ ] **Step 5: テストが通ることを確認 + shellcheck + Commit**

```bash
bats tests/test_manage.bats && bats tests/ && shellcheck -x lib/manage.sh
git add -A && git commit -m "feat: enable/disable/submit with cron, registry, and callback wiring"
```

---

### Task 9: status / resume サブコマンド

**Files:**
- Modify: `lib/manage.sh`(末尾に追記)
- Test: `tests/test_status.bats`

**Interfaces:**
- Consumes: Task 3 の `pa_unread_notifications` / `pa_mark_notifications_seen`、Task 5 のカウンタファイル群
- Produces: `pa_cmd_status [dir]` — 表示内容: ① group のタスク一覧(`pueue status -g <group>` の生出力)② 停止状態(halted の理由)③ カウンタ(連続失敗 / 通算実験数)④ 未読通知(表示後に既読化)⑤ runtime.log の末尾 5 行
- Produces: `pa_cmd_resume [dir]` — `logs/halted` を削除し `consec_failures` を 0 に。停止していなければその旨表示

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_status.bats`:

```bash
load helpers/setup

setup() {
  proj="$(make_project)"
  logs="$proj/.pueue-agent/logs"
  export PA_PUEUE_BIN="$REPO_ROOT/tests/helpers/mock_pueue.sh"
  export MOCK_PUEUE_STATUS_JSON="$BATS_TEST_TMPDIR/status.json"
  echo '{"tasks":{}}' > "$MOCK_PUEUE_STATUS_JSON"
}

@test "status shows counters and halted reason" {
  echo 2 > "$logs/consec_failures"
  echo 5 > "$logs/experiment_count"
  echo "連続失敗" > "$logs/halted"
  run "$PA_BIN" status "$proj"
  [ "$status" -eq 0 ]
  [[ "$output" == *"HALTED"* ]]
  [[ "$output" == *"連続失敗"* ]]
  [[ "$output" == *"consecutive failures: 2"* ]]
  [[ "$output" == *"experiments: 5"* ]]
}

@test "status shows unread notifications and marks them seen" {
  bash -c "source '$REPO_ROOT/lib/common.sh' && source '$REPO_ROOT/lib/notify.sh' && \
    pa_set_project '$proj' && pa_notify task_finished 'exp done'"
  run "$PA_BIN" status "$proj"
  [[ "$output" == *"exp done"* ]]
  run "$PA_BIN" status "$proj"
  [[ "$output" != *"exp done"* ]]
}

@test "resume clears halted state and failure counter" {
  echo reason > "$logs/halted"
  echo 3 > "$logs/consec_failures"
  run "$PA_BIN" resume "$proj"
  [ ! -f "$logs/halted" ]
  [ "$(cat "$logs/consec_failures")" = "0" ]
}

@test "resume when not halted says so" {
  run "$PA_BIN" resume "$proj"
  [[ "$output" == *"not halted"* ]]
}
```

mock_pueue に `status -g ...`(非 JSON)が来るため、mock_pueue の `status` 分岐はそのままで問題ない(`cat` するだけ)。

- [ ] **Step 2: テストが失敗することを確認**

Run: `bats tests/test_status.bats`
Expected: FAIL

- [ ] **Step 3: 実装**

`lib/manage.sh` 末尾に追記:

```bash
pa_cmd_status() {
  local proj group
  proj="$(pa_find_project "${1-}")" || pa_die "no .pueue-agent found"
  pa_set_project "$proj"
  group="$(pa_config pueue.group)"

  echo "=== pueue tasks (group: $group) ==="
  ${PA_PUEUE_BIN:-pueue} status -g "$group" 2>/dev/null || echo "(pueue not reachable)"
  echo

  if [ -f "$PA_DIR/logs/halted" ]; then
    echo "=== HALTED ==="
    cat "$PA_DIR/logs/halted"
    echo "(resume: pueue-agent resume)"
    echo
  fi

  echo "=== counters ==="
  echo "consecutive failures: $( [ -f "$PA_DIR/logs/consec_failures" ] && cat "$PA_DIR/logs/consec_failures" || echo 0 )"
  echo "experiments: $( [ -f "$PA_DIR/logs/experiment_count" ] && cat "$PA_DIR/logs/experiment_count" || echo 0 )"
  echo

  local unread
  unread="$(pa_unread_notifications)"
  if [ -n "$unread" ]; then
    echo "=== 未読通知 ==="
    echo "$unread"
    pa_mark_notifications_seen
    echo
  fi

  echo "=== recent activity ==="
  tail -5 "$PA_DIR/logs/runtime.log" 2>/dev/null || echo "(no activity)"
}

pa_cmd_resume() {
  local proj
  proj="$(pa_find_project "${1-}")" || pa_die "no .pueue-agent found"
  pa_set_project "$proj"
  if [ ! -f "$PA_DIR/logs/halted" ]; then
    echo "not halted"
    return 0
  fi
  rm -f "$PA_DIR/logs/halted"
  echo 0 > "$PA_DIR/logs/consec_failures"
  pa_log "resumed by user"
  echo "resumed. 監視を再開しました"
}
```

- [ ] **Step 4: テストが通ることを確認 + shellcheck + Commit**

```bash
bats tests/ && shellcheck -x lib/manage.sh
git add -A && git commit -m "feat: status and resume subcommands"
```

---

### Task 10: install.sh + README

**Files:**
- Create: `install.sh`, `README.md`
- Test: `tests/test_install.bats`

**Interfaces:**
- Produces: `install.sh` — `~/.local/bin/pueue-agent` → `<repo>/bin/pueue-agent` の symlink を作成(`PA_INSTALL_PREFIX` で先を変更可)。`jq` と `pueue` の存在チェックを行い、無ければエラー

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_install.bats`:

```bash
load helpers/setup

@test "install.sh creates symlink under prefix" {
  export PA_INSTALL_PREFIX="$BATS_TEST_TMPDIR/bin"
  run bash "$REPO_ROOT/install.sh"
  [ "$status" -eq 0 ]
  [ -L "$PA_INSTALL_PREFIX/pueue-agent" ]
  run "$PA_INSTALL_PREFIX/pueue-agent"
  [[ "$output" == *"Usage:"* ]]
}

@test "install.sh is idempotent" {
  export PA_INSTALL_PREFIX="$BATS_TEST_TMPDIR/bin"
  bash "$REPO_ROOT/install.sh"
  run bash "$REPO_ROOT/install.sh"
  [ "$status" -eq 0 ]
}
```

- [ ] **Step 2: テストが失敗することを確認**

Run: `bats tests/test_install.bats`
Expected: FAIL

- [ ] **Step 3: 実装**

`install.sh`:

```bash
#!/usr/bin/env bash
set -eu
REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
PREFIX="${PA_INSTALL_PREFIX:-$HOME/.local/bin}"

command -v jq >/dev/null 2>&1 || { echo "error: jq が必要です" >&2; exit 1; }
command -v pueue >/dev/null 2>&1 || echo "warning: pueue が見つかりません(サーバー側では必須)" >&2

mkdir -p "$PREFIX"
ln -sf "$REPO_ROOT/bin/pueue-agent" "$PREFIX/pueue-agent"
echo "installed: $PREFIX/pueue-agent"
case ":$PATH:" in
  *":$PREFIX:"*) : ;;
  *) echo "note: $PREFIX が PATH に含まれていません" ;;
esac
```

- [ ] **Step 4: README を書く**

`README.md` — 以下の構成で書く(内容は spec とテンプレートのコメントから要約):

```markdown
# pueue-agent

pueue で実行する長時間 ML 実験を、coding agent(Claude Code / Codex CLI /
Gemini CLI など headless 実行できる任意の CLI)が自律的に監視・修復・継続する
ツール。正常時はトークン消費ゼロ。

## 仕組み
(spec のアーキテクチャ図をそのまま転載)

## インストール
git clone <repo> && cd pueueAgent && ./install.sh

## 使い方
cd your-ml-repo
pueue-agent init          # .pueue-agent/ を生成(agent コマンド等を質問)
vi .pueue-agent/STATE.md  # 実験の目的・方針・制約を書く
pueue-agent enable        # cron + pueue callback を設定
pueue-agent submit -- python train.py --lr 0.01

## 日常操作
pueue-agent status         # 監視状況・未読通知
pueue-agent notifications -f
pueue-agent resume         # ガードレール停止からの再開
pueue-agent disable

## 設定
(.pueue-agent/config.yml の各キーの表)

## 停止条件(ガードレール)
(連続失敗 / 実験数上限 / agent リトライ上限 / 多重起動防止 の説明)
```

- [ ] **Step 5: テストが通ることを確認 + shellcheck + Commit**

```bash
bats tests/ && shellcheck -x install.sh
git add -A && git commit -m "feat: installer and README"
```

---

### Task 11: E2E テスト(実 pueued + フェイク実験 + モック agent)

**Files:**
- Create: `tests/e2e/run.sh`, `tests/e2e/fake_experiments/train_ok.sh`, `tests/e2e/fake_experiments/train_fail.sh`, `tests/e2e/fake_experiments/train_nan.sh`

**Interfaces:**
- Consumes: 全サブコマンド。実 pueue デーモンを**隔離 config**(一時ディレクトリ、独自 socket)で起動するため、ユーザーの pueued には触らない
- Produces: `tests/e2e/run.sh` — exit 0 = 合格。シナリオ: ① init ② 隔離 pueued 起動 ③ submit(成功実験)→ callback 相当を手動起動 → task_finished wake が発火しモック agent が呼ばれる ④ submit(失敗実験)→ sentinel が crash wake ⑤ NaN 実験 → sentinel が crash wake ⑥ 連続失敗で halt

- [ ] **Step 1: フェイク実験スクリプトを書く**

`tests/e2e/fake_experiments/train_ok.sh`:

```bash
#!/usr/bin/env bash
for i in 1 2 3; do echo "step $i loss 0.$((10 - i))"; sleep 1; done
echo "final accuracy 0.95"
```

`train_fail.sh`:

```bash
#!/usr/bin/env bash
echo "step 1 loss 0.9"
echo "Traceback (most recent call last):" >&2
echo "  ValueError: bad shape" >&2
exit 1
```

`train_nan.sh`:

```bash
#!/usr/bin/env bash
echo "step 1 loss 0.9"
echo "step 2 loss NaN"
sleep 300   # NaN を出したまま走り続ける(sentinel が検知して介入する状況)
```

全部 `chmod +x`。

- [ ] **Step 2: E2E スクリプトを書く**

`tests/e2e/run.sh`:

```bash
#!/usr/bin/env bash
# E2E: 隔離 pueued + フェイク実験 + モック agent で全経路を通す。
set -eu
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WORK="$(mktemp -d)"
trap 'pueue --config "$WORK/pueue.yml" shutdown >/dev/null 2>&1 || true; rm -rf "$WORK"' EXIT

fail() { echo "E2E FAIL: $*" >&2; exit 1; }

# --- 隔離 pueued ---
mkdir -p "$WORK/pueue"
cat > "$WORK/pueue.yml" <<EOF
shared:
  pueue_directory: "$WORK/pueue"
  use_unix_socket: true
  unix_socket_path: "$WORK/pueue.socket"
daemon:
  callback: null
EOF
pueued --config "$WORK/pueue.yml" -d
sleep 1
export PA_PUEUE_BIN="pueue --config $WORK/pueue.yml"
export PA_TASK_LOG_DIR="$WORK/pueue/task_logs"
export PA_REGISTRY="$WORK/registry"

# --- プロジェクト init(モック agent) ---
proj="$WORK/proj"
mkdir -p "$proj"
export MOCK_AGENT_LOG="$WORK/agent_calls.log"
"$REPO_ROOT/bin/pueue-agent" init \
  --agent-cmd "$REPO_ROOT/tests/helpers/mock_agent.sh {prompt}" \
  --git-mode commit --group pa-e2e "$proj"
printf 'pa-e2e\t%s\n' "$proj" > "$PA_REGISTRY"
$PA_PUEUE_BIN group add pa-e2e >/dev/null

# --- ① 成功実験 → callback → task_finished wake ---
$PA_PUEUE_BIN add -g pa-e2e -- "$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh" >/dev/null
for _ in $(seq 20); do
  st="$($PA_PUEUE_BIN status --json | jq -r '.tasks["0"].status | keys[0]')"
  [ "$st" = "Done" ] && break
  sleep 1
done
"$REPO_ROOT/bin/pueue-agent" callback 0 pa-e2e
grep -q "mode: task_finished" "$MOCK_AGENT_LOG" || fail "task_finished wake did not fire"
grep -q "result: Success" "$MOCK_AGENT_LOG" || fail "success result not passed"

# --- ② 失敗実験 → sentinel → crash wake ---
rm -f "$MOCK_AGENT_LOG"
$PA_PUEUE_BIN add -g pa-e2e -- "$REPO_ROOT/tests/e2e/fake_experiments/train_fail.sh" >/dev/null
sleep 3
"$REPO_ROOT/bin/pueue-agent" sentinel "$proj"
grep -q "mode: crash" "$MOCK_AGENT_LOG" || fail "crash wake did not fire for failed task"

# --- ③ NaN 実験(実行中) → sentinel → crash wake ---
rm -f "$MOCK_AGENT_LOG"
$PA_PUEUE_BIN add -g pa-e2e -- "$REPO_ROOT/tests/e2e/fake_experiments/train_nan.sh" >/dev/null
sleep 3
"$REPO_ROOT/bin/pueue-agent" sentinel "$proj"
grep -q "mode: crash" "$MOCK_AGENT_LOG" || fail "crash wake did not fire for NaN output"
$PA_PUEUE_BIN kill 2 >/dev/null 2>&1 || true

# --- ④ 連続失敗で halt (max=3, ①②③で crash 2回済み → もう1回) ---
"$REPO_ROOT/bin/pueue-agent" wake crash "$proj" 99 "Failed:1"
[ -f "$proj/.pueue-agent/logs/halted" ] || fail "did not halt after 3 consecutive failures"
rm -f "$MOCK_AGENT_LOG"
"$REPO_ROOT/bin/pueue-agent" wake deep_check "$proj"
[ ! -f "$MOCK_AGENT_LOG" ] || fail "halted state did not block wake"

# --- ⑤ resume で復帰 ---
"$REPO_ROOT/bin/pueue-agent" resume "$proj" | grep -q resumed || fail "resume failed"

echo "E2E PASS"
```

`chmod +x tests/e2e/run.sh`

- [ ] **Step 3: 実行して確認(初回は失敗を直しながら)**

Run: `tests/e2e/run.sh`
Expected: `E2E PASS`、exit 0

注意: pueue 4.0.4 の `--config` グローバルフラグ位置(`pueue --config <path> <subcommand>`)。socket path・task_logs の実パスは環境で異なりうるので、失敗したら `ls "$WORK/pueue"` で実構造を確認して `PA_TASK_LOG_DIR` を合わせる。

- [ ] **Step 4: 全テストスイートを通す**

```bash
bats tests/ && tests/e2e/run.sh && shellcheck -x bin/pueue-agent lib/*.sh install.sh
```

Expected: すべて成功

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "test: end-to-end test with isolated pueued and fake experiments"
```

---

## 手動受け入れ確認(実 agent、自動化しない)

E2E 合格後、実際の agent(例: Claude Code)で 1 度だけ確認する(トークン費用がかかるため自動化しない):

1. 実験用のダミーリポジトリを作り `pueue-agent init`(agent-cmd は実物)
2. STATE.md に簡単な目的を書く(例: 「fake_experiments/train_ok.sh 相当のスクリプトの出力精度を報告し、次の実験は投入せず終了せよ」)
3. `pueue-agent enable` → `pueue-agent submit -- ./train.sh`
4. 完了後、agent が STATE.md を更新し通知が記録されることを確認
