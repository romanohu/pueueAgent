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

# Code-change editor fixture mode.  The output path is descriptor-backed and
# supplied by the supervisor, so no project pathname is inferred here.
if [ -n "${PUEUE_AGENT_EDITOR_OUTPUT:-}" ]; then
  printf 'EDITOR_MODE=%s\n' "${PUEUE_AGENT_EDITOR_MODE:-fresh}" >> "$PUEUE_AGENT_TEST_AGENT_LOG"
  if [ -n "${PUEUE_AGENT_EDITOR_SESSION_ID+x}" ]; then
    printf 'EDITOR_SESSION_ID=%s\n' "$PUEUE_AGENT_EDITOR_SESSION_ID" >> "$PUEUE_AGENT_TEST_AGENT_LOG"
  fi
  case "${PUEUE_AGENT_TEST_EDITOR_OUTPUT_MODE:-ready}" in
    malformed)
      printf '%s\n' '{malformed-editor' > "$PUEUE_AGENT_EDITOR_OUTPUT"
      exit 0
      ;;
    oversized)
      head -c 65537 /dev/zero | tr '\0' 'x' > "$PUEUE_AGENT_EDITOR_OUTPUT"
      exit 0
      ;;
    cannot_apply)
      printf '%s\n' '{"schema_version":1,"status":"cannot_apply","summary":"editor cannot apply the requested change","proposed_checks":[]}' > "$PUEUE_AGENT_EDITOR_OUTPUT"
      exit 0
      ;;
    fail)
      exit 17
      ;;
    *)
      printf '%s\n' '{"schema_version":1,"status":"ready","summary":"editor prepared candidate","proposed_checks":[{"source":"cargo","argv":["cargo","test","--all-targets","--","--test-threads=1"],"working_directory":"."}]}' > "$PUEUE_AGENT_EDITOR_OUTPUT"
      exit 0
      ;;
  esac
fi

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
