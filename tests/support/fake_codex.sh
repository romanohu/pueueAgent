#!/usr/bin/env bash
set -eu

: "${PUEUE_AGENT_TEST_CODEX_LOG:?PUEUE_AGENT_TEST_CODEX_LOG is required}"

{
  if [ -n "${CODEX_HOME+x}" ]; then
    printf 'ENV_NAME=CODEX_HOME\n'
  fi
  printf 'ARGC=%s\n' "$#"
  index=1
  for argument in "$@"; do
    if [ "$index" -le 5 ]; then
      printf 'ARG_%s=%s\n' "$index" "$argument"
    fi
    index=$((index + 1))
  done
} >> "$PUEUE_AGENT_TEST_CODEX_LOG"
