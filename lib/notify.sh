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
