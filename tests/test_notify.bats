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
