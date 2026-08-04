#!/usr/bin/env bash
# テスト用 agent: 呼び出し記録を残す。MOCK_AGENT_EXIT で終了コード制御。
echo "PROMPT:$1" >> "${MOCK_AGENT_LOG:?}"
exit "${MOCK_AGENT_EXIT:-0}"
