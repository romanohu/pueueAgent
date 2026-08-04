load helpers/setup

setup() {
  proj="$(make_project)"
  export PA_REGISTRY="$BATS_TEST_TMPDIR/registry"
  export PA_PUEUE_BIN="$REPO_ROOT/tests/helpers/mock_pueue.sh"
  export MOCK_PUEUE_STATUS_JSON="$BATS_TEST_TMPDIR/status.json"
  export MOCK_AGENT_LOG="$BATS_TEST_TMPDIR/agent_calls.log"
  sed -i.bak \
    "s|^  command: .*|  command: \"$REPO_ROOT/tests/helpers/mock_agent.sh {prompt}\"|" \
    "$proj/.pueue-agent/config.yml"
  printf 'pa-proj\t%s\n' "$proj" > "$PA_REGISTRY"
}

@test "successful task dispatches task_finished wake" {
  cat > "$MOCK_PUEUE_STATUS_JSON" <<'EOF'
{"tasks":{"5":{"id":5,"group":"pa-proj",
 "status":{"Done":{"result":"Success"}}}}}
EOF
  run "$PA_BIN" callback 5 pa-proj
  [ "$status" -eq 0 ]
  grep -q "mode: task_finished" "$MOCK_AGENT_LOG"
  grep -q "result: Success" "$MOCK_AGENT_LOG"
}

@test "failed task dispatches crash wake and dedupes with sentinel" {
  cat > "$MOCK_PUEUE_STATUS_JSON" <<'EOF'
{"tasks":{"6":{"id":6,"group":"pa-proj",
 "status":{"Done":{"result":{"Failed":1}}}}}}
EOF
  run "$PA_BIN" callback 6 pa-proj
  grep -q "mode: crash" "$MOCK_AGENT_LOG"
  grep -qx "6" "$proj/.pueue-agent/logs/handled_tasks"
}

@test "unknown group exits 0 silently" {
  run "$PA_BIN" callback 1 some-other-group
  [ "$status" -eq 0 ]
  [ "$output" = "" ]
}

@test "registry add/lookup/remove roundtrip" {
  run bash -c "source '$REPO_ROOT/lib/common.sh' && PA_ROOT='$REPO_ROOT' source '$REPO_ROOT/lib/callback.sh' && \
    pa_registry_add g2 /tmp/x && pa_registry_lookup g2"
  [ "$output" = "/tmp/x" ]
  run bash -c "source '$REPO_ROOT/lib/common.sh' && PA_ROOT='$REPO_ROOT' source '$REPO_ROOT/lib/callback.sh' && \
    pa_registry_add g2 /tmp/x && pa_registry_remove g2 && pa_registry_lookup g2"
  [ "$status" -eq 1 ]
}
