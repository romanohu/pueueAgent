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

  # shellcheck disable=SC2086
  tasks_json="$(${PA_PUEUE_BIN:-pueue} status --json)" || pa_die "pueue status failed"

  # 1) 完了(Done)タスクのうち未処理のもの(失敗/kill/成功いずれも)→ backstop 処理。
  #    callback が呼ばれなかった(pueued 未再起動)場合や、wake が lock busy / halted で
  #    未消費だった場合の取りこぼしをここで拾う。1 パスにつき最大1件、昇順 id で決定的に。
  local done_tasks id res wake_mode wake_status
  done_tasks="$(echo "$tasks_json" | jq -r --arg g "$group" '
    .tasks | to_entries[] | .value
    | select(.group == $g)
    | select(.status.Done?)
    | (.status.Done.result) as $r
    | select(($r | type == "object") or $r == "Killed" or $r == "Success")
    | [(.id | tostring), (if ($r | type == "object") then "Failed:\($r.Failed)" else $r end)]
    | @tsv
  ' | sort -t "$(printf '\t')" -k1,1n)"
  while IFS="$(printf '\t')" read -r id res; do
    [ -n "$id" ] || continue
    grep -qx "$id" "$PA_DIR/logs/handled_tasks" 2>/dev/null && continue
    if [ "$res" = "Success" ]; then
      wake_mode="task_finished"
      pa_log "sentinel: task $id done (Success, unhandled) -> wake task_finished"
    else
      wake_mode="crash"
      pa_log "sentinel: task $id failed -> wake crash"
    fi
    pa_wake "$wake_mode" "$PA_PROJECT" "$id" "$res"
    wake_status=$?
    if [ "$wake_status" -eq 0 ]; then
      echo "$id" >> "$PA_DIR/logs/handled_tasks"
    else
      pa_log "sentinel: wake($wake_mode) for task $id not consumed (status=$wake_status), will retry next pass"
    fi
    return 0
  done <<EOF
$done_tasks
EOF

  # 2) 実行中タスクの出力チェック
  local running_ids verdict
  running_ids="$(echo "$tasks_json" | jq -r --arg g "$group" '
    .tasks | to_entries[] | .value
    | select(.group == $g) | select(.status.Running?) | .id')"
  for id in $running_ids; do
    verdict="$(pa_check_task_output "$id")"
    if [ -n "$verdict" ]; then
      local mode="$verdict"
      # shellcheck disable=SC2015  # pa_log always succeeds; A && B || C is exhaustive here
      [ "$mode" = "crash" ] && pa_log "sentinel: error pattern in task $id output" \
                            || pa_log "sentinel: task $id output stalled"
      pa_wake "$mode" "$PA_PROJECT" "$id" ""
      return 0
    fi
  done

  # 2b) extra_log_paths のエラーパターン検査 (YAGNI: スナップショット無しの検査のみ)
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

  # 3) 実行中タスクが無ければ何もしない(完了タスクの処理は上の 1) と callback が担う)
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
