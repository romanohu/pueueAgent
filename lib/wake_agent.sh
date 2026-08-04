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
