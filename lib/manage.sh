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
  marker="# pueue-agent:$proj"
  current="$(pa_cron_get | grep -vF "$marker" || true)"
  newline="*/$interval * * * * '$PA_ROOT/bin/pueue-agent' sentinel $proj >> '$PA_DIR/logs/cron.log' 2>&1 $marker"
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
  current="$(pa_cron_get | grep -vF "$marker" || true)"
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
