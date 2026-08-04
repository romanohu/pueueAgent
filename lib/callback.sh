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
  local task_id="${1-}" group="${2-}" proj result wake_status wake_mode
  [ -n "$task_id" ] && [ -n "$group" ] || pa_die "callback: task_id and group required"
  proj="$(pa_registry_lookup "$group")" || return 0   # 監視対象外の group
  pa_set_project "$proj"

  # result を pueue 本体から取得(callback テンプレート変数に依存しない)
  result="$(${PA_PUEUE_BIN:-pueue} status --json | jq -r --arg id "$task_id" '
    .tasks[$id].status.Done.result
    | if type == "object" then "Failed:\(.Failed)" else . end' 2>/dev/null)"

  case "$result" in
    Success)      wake_mode="task_finished" ;;
    Failed:*|Killed) wake_mode="crash" ;;
    *)            return 0 ;;  # Done ではない or タスクなし: 何もしない
  esac

  # wake を先に呼び、消費された(0)ときだけ handled_tasks に記録する。
  # lock busy / halted (3) で未消費のときに先回りで記録すると、その completion
  # を処理する者が誰もいなくなり永久に取りこぼす(sentinel との二重処理防止と
  # 「消費されたときだけ記録」の両立)。
  "$PA_ROOT/bin/pueue-agent" wake "$wake_mode" "$proj" "$task_id" "$result"
  wake_status=$?
  if [ "$wake_status" -eq 0 ]; then
    grep -qx "$task_id" "$PA_DIR/logs/handled_tasks" 2>/dev/null \
      || echo "$task_id" >> "$PA_DIR/logs/handled_tasks"
  else
    pa_log "callback: wake($wake_mode) for task $task_id not consumed (status=$wake_status), will retry next pass"
  fi
  return 0
}
