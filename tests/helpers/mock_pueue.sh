#!/usr/bin/env bash
# テスト用 pueue。
case "$1" in
  status) cat "${MOCK_PUEUE_STATUS_JSON:?}" ;;
  add) echo "$*" >> "${MOCK_PUEUE_CALLS:-/dev/null}" ;;
  group)
    echo "$*" >> "${MOCK_PUEUE_CALLS:-/dev/null}"
    [ -z "${MOCK_PUEUE_GROUP_FAIL:-}" ] || exit 1
    ;;
  *) echo "mock_pueue: unhandled: $*" >&2; exit 1 ;;
esac
