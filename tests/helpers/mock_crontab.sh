#!/usr/bin/env bash
# crontab モック: MOCK_CRONTAB_FILE を読み書きする
f="${MOCK_CRONTAB_FILE:?}"
case "${1-}" in
  -l) [ -f "$f" ] && cat "$f" || exit 1 ;;
  -)  cat > "$f" ;;
  *)  cat "$2" > "$f" ;;
esac
