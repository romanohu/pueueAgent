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

# shellcheck disable=SC2086  # PA_CRONTAB_BIN は意図的に非クォート展開(コマンド名分割を許す)
pa_cron_get() { ${PA_CRONTAB_BIN:-crontab} -l 2>/dev/null || true; }
# shellcheck disable=SC2086  # 同上
pa_cron_set() { echo "$1" | ${PA_CRONTAB_BIN:-crontab} -; }

# 単一引用符で囲んで安全にシェル展開できるようにエスケープする ('\''  イディオム)
pa_shquote() {  # $1 → 中身だけをエスケープして返す(呼び出し側で '...' に包む)
  printf '%s' "$1" | sed "s/'/'\\\\''/g"
}

# クロンタブ(stdin)から、行末が "# pueue-agent:<proj>" マーカーと一致する行を除去する。
# プロジェクトパス同士が前方一致する場合(例: /a と /ab)に部分一致で誤除去しないよう、
# 部分文字列一致ではなく行末一致で判定する。
pa_cron_without_marker() {  # $1=marker
  awk -v m="$1" 'substr($0, length($0) - length(m) + 1) != m'
}

pa_cmd_enable() {
  local proj group interval marker current newline callback_cmd cfg
  proj="$(pa_find_project "${1-}")" || pa_die "no .pueue-agent found (run: pueue-agent init)"
  pa_set_project "$proj"
  group="$(pa_config pueue.group)"
  [ -n "$group" ] || pa_die "pueue.group not set in config"
  interval="$(pa_config check.interval_minutes 10)"

  # 1) pueue group(既存エラーは無視)
  # shellcheck disable=SC2086  # PA_PUEUE_BIN は意図的に非クォート展開
  ${PA_PUEUE_BIN:-pueue} group add "$group" >/dev/null 2>&1 || true

  # 2) registry
  pa_registry_add "$group" "$proj"

  # 3) cron(冪等)。既存クロンタブが空のとき先頭に空行が入らないよう
  #    printf で組み立ててから連結する。
  # cron は PATH=/usr/bin:/bin 相当の最小 PATH で実行されるため、pueue/jq/agent CLI
  # (cargo/homebrew/nvm 等で入る) が見つからず sentinel が pa_die したり agent 起動が
  # 失敗して halt することがある。enable 実行時点の $PATH をコマンドの前に埋め込んで
  # 継承させる。
  local path_quoted
  path_quoted="$(pa_shquote "$PATH")"
  marker="# pueue-agent:$proj"
  current="$(pa_cron_get | pa_cron_without_marker "$marker")"
  newline="*/$interval * * * * PATH='$path_quoted' '$PA_ROOT/bin/pueue-agent' sentinel '$proj' >> '$PA_DIR/logs/cron.log' 2>&1 $marker"
  if [ -n "$current" ]; then
    pa_cron_set "$(printf '%s\n%s' "$current" "$newline")"
  else
    pa_cron_set "$newline"
  fi

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
  current="$(pa_cron_get | pa_cron_without_marker "$marker")"
  pa_cron_set "$current"
  pa_registry_remove "$group"
  # shellcheck disable=SC2086  # PA_PUEUE_BIN は意図的に非クォート展開
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
  # shellcheck disable=SC2086  # PA_PUEUE_BIN は意図的に非クォート展開
  ${PA_PUEUE_BIN:-pueue} add -g "$group" -- "$@"
}

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
