#!/usr/bin/env bash
# テスト用 pueue。MOCK_PUEUE_STATUS_JSON のファイル内容を status --json で返す。
case "$1" in
  status) cat "${MOCK_PUEUE_STATUS_JSON:?}" ;;
  *) echo "mock_pueue: unhandled: $*" >&2; exit 1 ;;
esac
