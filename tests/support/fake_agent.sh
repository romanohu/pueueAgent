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
printf '%s\n' "$*" >> "$PUEUE_AGENT_TEST_AGENT_LOG"

fail_first="${PUEUE_AGENT_TEST_AGENT_FAIL_FIRST:-0}"
if [ "$calls" -le "$fail_first" ]; then
  exit 17
fi
