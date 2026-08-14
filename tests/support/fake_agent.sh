#!/usr/bin/env bash
set -eu

: "${PUEUE_AGENT_TEST_AGENT_LOG:?PUEUE_AGENT_TEST_AGENT_LOG is required}"

state_file="${PUEUE_AGENT_TEST_AGENT_STATE:-${PUEUE_AGENT_TEST_AGENT_LOG}.state}"
calls=0
if [ -f "$state_file" ]; then
  calls="$(cat "$state_file")"
fi
calls=$((calls + 1))
printf '%s\n' "$calls" > "$state_file"

printf 'CALL %s\n' "$calls" >> "$PUEUE_AGENT_TEST_AGENT_LOG"
printf 'ARGC=%s\n' "$#" >> "$PUEUE_AGENT_TEST_AGENT_LOG"
index=1
for argument in "$@"; do
  printf 'ARGV[%s]=<%s>\n' "$index" "$argument" >> "$PUEUE_AGENT_TEST_AGENT_LOG"
  index=$((index + 1))
done
for environment_name in HOME PATH TMPDIR; do
  if [ -n "${!environment_name+x}" ]; then
    printf 'ENV_NAME=%s\n' "$environment_name" >> "$PUEUE_AGENT_TEST_AGENT_LOG"
  fi
done

fail_first="${PUEUE_AGENT_TEST_AGENT_FAIL_FIRST:-0}"
case "$fail_first" in
  ''|*[!0-9]*) exit 64 ;;
esac
if [ "$calls" -le "$fail_first" ]; then
  printf 'MODE=fail-first\n' >> "$PUEUE_AGENT_TEST_AGENT_LOG"
  exit 17
fi

mode="${PUEUE_AGENT_TEST_AGENT_MODE:-success}"
printf 'MODE=%s\n' "$mode" >> "$PUEUE_AGENT_TEST_AGENT_LOG"
case "$mode" in
  success)
    exit 0
    ;;
  fail|fail-before-marker)
    exit 17
    ;;
  sleep)
    sleep_seconds="${PUEUE_AGENT_TEST_AGENT_SLEEP_SECONDS:-0}"
    case "$sleep_seconds" in
      ''|*[!0-9]*) exit 64 ;;
    esac
    if [ "$sleep_seconds" -gt 2 ]; then
      exit 64
    fi
    sleep "$sleep_seconds"
    exit 0
    ;;
  exit)
    exit_code="${PUEUE_AGENT_TEST_AGENT_EXIT_CODE:-0}"
    case "$exit_code" in
      ''|*[!0-9]*) exit 64 ;;
    esac
    if [ "$exit_code" -gt 125 ]; then
      exit 64
    fi
    exit "$exit_code"
    ;;
  *)
    exit 64
    ;;
esac
