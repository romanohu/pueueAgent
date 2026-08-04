load helpers/setup

setup() {
  proj="$(make_project)"
  export proj
}

cfg() {  # ヘルパ: pa_config をサブシェルで呼ぶ
  bash -c "source '$REPO_ROOT/lib/common.sh' && pa_set_project '$proj' && pa_config $1 ${2:-}"
}

@test "reads quoted scalar" {
  run cfg agent.command
  [ "$output" = 'claude -p {prompt} --permission-mode acceptEdits' ]
}

@test "reads numeric scalar" {
  run cfg check.interval_minutes
  [ "$output" = "10" ]
}

@test "similar key names do not collide" {
  # interval_minutes と deep_check_interval_minutes を混同しない
  run cfg check.deep_check_interval_minutes
  [ "$output" = "0" ]
}

@test "missing key returns default" {
  run cfg nosuch.key 42
  [ "$output" = "42" ]
}

@test "missing key without default returns empty, exit 0" {
  run cfg nosuch.key
  [ "$status" -eq 0 ]
  [ "$output" = "" ]
}

@test "reads list items" {
  run bash -c "source '$REPO_ROOT/lib/common.sh' && pa_set_project '$proj' && pa_config_list check.error_patterns"
  [ "${lines[0]}" = "NaN" ]
  [ "${lines[1]}" = "Traceback" ]
  [ "${lines[2]}" = "CUDA (error|out of memory)" ]
}

@test "empty inline list yields nothing" {
  run bash -c "source '$REPO_ROOT/lib/common.sh' && pa_set_project '$proj' && pa_config_list check.extra_log_paths"
  [ "$output" = "" ]
}
