#!/usr/bin/env bash
# E2E: 隔離 pueued + フェイク実験 + モック agent で全経路を通す。
set -eu
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# macOS の unix domain socket path は長さ制限があるため、既定の mktemp -d
# (/var/folders/.../T/tmp.XXXXXX) ではなく短い /tmp 直下に隔離ディレクトリを作る。
WORK="$(mktemp -d /tmp/pae2e.XXXXXX)"
cleanup() {
  local pid
  pid="$(cat "$WORK/pueue/pueue.pid" 2>/dev/null || echo "")"
  pueue --config "$WORK/pueue.yml" shutdown >/dev/null 2>&1 || true
  # daemon の shutdown は非同期(pid ファイル削除・state 保存は shutdown コマンド
  # 復帰後も少し続く)。$WORK を即 rm -rf すると daemon 側で
  # "I/O error ... while removing pid file" のような無害だが紛らわしいエラーログが出るため、
  # プロセスが消えるまで最大 5 秒待ってから削除する。
  if [ -n "$pid" ]; then
    for _ in $(seq 50); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

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
# daemon は -d で自身をフォーク・デタッチするが、フォーク前の親プロセスの
# stdout/stderr をそのまま引き継ぐことがあり、後続のログ(WARN/ERROR 等)が
# このスクリプトの標準出力に紛れ込むことがあるため明示的に捨てる。
pueued --config "$WORK/pueue.yml" -d >/dev/null 2>&1
export PA_PUEUE_BIN="pueue --config $WORK/pueue.yml"
export PA_TASK_LOG_DIR="$WORK/pueue/task_logs"
export PA_REGISTRY="$WORK/registry"

# 固定 sleep ではなく、デーモンが実際に応答するまでポーリングする(高負荷機での
# 起動遅延に対して脆いため)。
up=""
for _ in $(seq 100); do
  # shellcheck disable=SC2086
  if $PA_PUEUE_BIN status --json >/dev/null 2>&1; then up=1; break; fi
  sleep 0.1
done
[ -n "$up" ] || fail "isolated pueued did not come up in time"

# --- プロジェクト init(モック agent) ---
proj="$WORK/proj"
mkdir -p "$proj"
export MOCK_AGENT_LOG="$WORK/agent_calls.log"
"$REPO_ROOT/bin/pueue-agent" init \
  --agent-cmd "$REPO_ROOT/tests/helpers/mock_agent.sh {prompt}" \
  --git-mode commit --group pa-e2e "$proj"
printf 'pa-e2e\t%s\n' "$proj" > "$PA_REGISTRY"
# shellcheck disable=SC2086  # PA_PUEUE_BIN は意図的に非クォート展開(コマンド名分割を許す)
$PA_PUEUE_BIN group add pa-e2e >/dev/null

# --- ① 成功実験 → callback → task_finished wake ---
# shellcheck disable=SC2086
$PA_PUEUE_BIN add -g pa-e2e -- "$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh" >/dev/null
for _ in $(seq 20); do
  # shellcheck disable=SC2086
  st="$($PA_PUEUE_BIN status --json | jq -r '.tasks["0"].status | keys[0]')"
  [ "$st" = "Done" ] && break
  sleep 1
done
[ "$st" = "Done" ] || fail "train_ok.sh did not finish in time"
"$REPO_ROOT/bin/pueue-agent" callback 0 pa-e2e
grep -q "mode: task_finished" "$MOCK_AGENT_LOG" || fail "task_finished wake did not fire"
grep -q "result: Success" "$MOCK_AGENT_LOG" || fail "success result not passed"

# --- ② 失敗実験 → sentinel → crash wake ---
rm -f "$MOCK_AGENT_LOG"
# shellcheck disable=SC2086
$PA_PUEUE_BIN add -g pa-e2e -- "$REPO_ROOT/tests/e2e/fake_experiments/train_fail.sh" >/dev/null
for _ in $(seq 20); do
  # shellcheck disable=SC2086
  st="$($PA_PUEUE_BIN status --json | jq -r '.tasks["1"].status | keys[0]')"
  [ "$st" = "Done" ] && break
  sleep 1
done
[ "$st" = "Done" ] || fail "train_fail.sh did not finish in time"
"$REPO_ROOT/bin/pueue-agent" sentinel "$proj"
grep -q "mode: crash" "$MOCK_AGENT_LOG" || fail "crash wake did not fire for failed task"

# --- ③ NaN 実験(実行中) → sentinel → crash wake ---
rm -f "$MOCK_AGENT_LOG"
# shellcheck disable=SC2086
$PA_PUEUE_BIN add -g pa-e2e -- "$REPO_ROOT/tests/e2e/fake_experiments/train_nan.sh" >/dev/null
# タスクが Running になり、かつ NaN 出力がログに現れるまでポーリングする(両方揃うまで
# sentinel を呼ぶと、まだ Queued だったり出力がまだ無かったりして正当に no-op してしまう)。
nan_log="$PA_TASK_LOG_DIR/2.log"
ready=""
for _ in $(seq 50); do
  # shellcheck disable=SC2086
  st="$($PA_PUEUE_BIN status --json | jq -r '.tasks["2"].status | keys[0]' 2>/dev/null || echo "")"
  if [ "$st" = "Running" ] && [ -f "$nan_log" ] && grep -q "NaN" "$nan_log"; then
    ready=1
    break
  fi
  sleep 0.2
done
[ -n "$ready" ] || fail "train_nan.sh did not reach Running with NaN output in time"
"$REPO_ROOT/bin/pueue-agent" sentinel "$proj"
grep -q "mode: crash" "$MOCK_AGENT_LOG" || fail "crash wake did not fire for NaN output"
# shellcheck disable=SC2086
$PA_PUEUE_BIN kill 2 >/dev/null 2>&1 || true

# --- ④ 連続失敗で halt (max=3, ①②③で crash 2回済み → もう1回) ---
# ①(task_finished/Success)は consec_failures を 0 にリセット、②で 1、③で 2。
# ここで手動 wake crash すると 3 に達して halt する。
# halt する呼び出しでは agent は起動されないはずなので、呼び出し前にログを空にしておき、
# 呼び出し後もログが作られていないことまで直接確認する(stale な③の内容で
# 見かけ上パスしてしまわないように)。
rm -f "$MOCK_AGENT_LOG"
"$REPO_ROOT/bin/pueue-agent" wake crash "$proj" 99 "Failed:1"
[ -f "$proj/.pueue-agent/logs/halted" ] || fail "did not halt after 3 consecutive failures"
[ ! -f "$MOCK_AGENT_LOG" ] || fail "agent was launched despite hitting the halt threshold"
# halted 中の wake は「イベント未消費」を意味する終了コード 3 を返す(set -eu 下では
# 素の呼び出しだと即座にスクリプトが終了してしまうため || で捕捉する)。
halted_wake_status=0
"$REPO_ROOT/bin/pueue-agent" wake deep_check "$proj" || halted_wake_status=$?
[ "$halted_wake_status" -eq 3 ] || fail "halted wake should return 3 (event not consumed), got $halted_wake_status"
[ ! -f "$MOCK_AGENT_LOG" ] || fail "halted state did not block wake"

# --- ⑤ resume で復帰 ---
"$REPO_ROOT/bin/pueue-agent" resume "$proj" | grep -q resumed || fail "resume failed"
[ ! -f "$proj/.pueue-agent/logs/halted" ] || fail "halted marker still present after resume"
[ "$(cat "$proj/.pueue-agent/logs/consec_failures" 2>/dev/null || echo "")" = "0" ] \
  || fail "consec_failures not reset after resume"
# 実際に監視が復帰していることまで証明する: resume 後に wake が agent を起動できるか。
rm -f "$MOCK_AGENT_LOG"
"$REPO_ROOT/bin/pueue-agent" wake deep_check "$proj"
grep -q "mode: deep_check" "$MOCK_AGENT_LOG" || fail "agent was not launched after resume"

echo "E2E PASS"
