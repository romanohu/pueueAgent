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
