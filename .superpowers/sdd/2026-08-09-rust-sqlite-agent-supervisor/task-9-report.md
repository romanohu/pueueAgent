# Task 9 Report: Supervisor loop, service integration, and callback installation

## Summary

Implemented the Rust supervisor daemon loop, user-service definition/rendering,
callback installation, and `enable`/`daemon` CLI integration.

Scope kept to Task 9:

- Added `src/daemon.rs` for bounded supervisor ticks.
- Added `src/service.rs` for service definitions, fakeable service control,
  callback registration, and enable orchestration.
- Added static systemd and launchd templates under `assets/`.
- Wired `daemon --pueue-config` and `enable --pueue-config`.
- Preserved the legacy Bash entrypoint (`bin/pueue-agent`) unchanged.
- Did not add raw OS signals for Pueue task control or shell wrappers.

## TDD

RED:

```text
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test daemon --test service
```

Result: failed as expected with unresolved imports for missing
`pueue_agent::daemon` and `pueue_agent::service`.

GREEN:

```text
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test daemon --test service
```

Result: 8 passed, 0 failed.

## Verification

All commands used the requested toolchain path:

```text
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin
```

Results:

- `cargo fmt --check`: passed.
- `cargo test --test daemon --test service`: passed, 8 tests.
- `cargo test --offline --all-targets --all-features`: passed, all listed Rust test targets.
- `cargo clippy --all-targets --all-features -- -D warnings`: passed.
- `cargo build --release`: passed; release binary built at `target/release/pueue-agent`.

## Notes

- Daemon status/Pueue integration errors propagate as errors and are not treated
  as idle state.
- Restart recovery is exercised through expired event-lease recovery.
- Callback registration is idempotent for the expected command and rejects
  conflicting existing callbacks.
- `enable` registers the project and callback before service installation; if
  service installation fails, the partial state remains recoverable.
- Service tests use fake managers and do not mutate host system services.

## Fix Round 1

Reviewer findings addressed:

- Wired the production `daemon` command to a cancellation token driven by
  platform shutdown signals: terminal interrupt on all platforms and SIGTERM on
  Unix/systemd. Tests keep injection through `Daemon::run(CancellationToken)` and
  `cancel_token_on_shutdown_signal`.
- Changed daemon shutdown to drain active child agents. The daemon keeps polling
  for prompt exits and then uses the same process-tree timeout cleanup path after
  a bounded grace period, so `agent_runs` rows do not remain `running`.
- Quoted and escaped systemd `ExecStart`, `Environment`, and
  `WorkingDirectory` values, including spaces, quotes, backslashes, and `%`
  specifiers. launchd continues to render boundary-safe `ProgramArguments`.
- Added shell-free Pueue group provisioning to the adapter/control path via
  `PueueApi::ensure_group`, implemented as `pueue group add <group>`, and called
  it before callback/service enable.
- Replaced global callback line search/replacement with daemon-scoped,
  indentation-aware Pueue YAML updates. The real file-backed registry now reads
  and updates `daemon.callback`, preserves nested non-daemon callback lines, and
  rejects configs that only contain non-daemon callback conflicts.

Focused tests added:

- `tests/integration/daemon.rs`
  - `daemon_shutdown_drains_child_agent_that_finishes_promptly`
  - `daemon_shutdown_bounds_long_running_child_agent_and_marks_it_terminal`
  - `injected_shutdown_signal_cancels_daemon_token`
- `tests/integration/service.rs`
  - `systemd_definition_quotes_paths_with_spaces_without_changing_launchd_argument_boundaries`
  - `systemd_definition_escapes_quotes_backslashes_and_percent_specifiers`
  - `enable_provisions_configured_pueue_group_before_callback_and_service_install`
  - `pueue_config_callback_registry_updates_daemon_scoped_callback_only`
  - `pueue_config_callback_registry_rejects_non_daemon_callback_conflicts`
- `tests/integration/pueue_adapter.rs`
  - `command_adapter_provisions_group_without_shell`

Verification results:

```text
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --offline --test service --test pueue_adapter
```

Result: passed, 20 tests.

```text
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --offline --test daemon
```

Result: passed, 6 tests.

```text
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --offline --all-targets --all-features
```

Result: passed, all Rust targets.

```text
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo fmt --all -- --check
```

Result: passed.

```text
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo clippy --offline --all-targets --all-features -- -D warnings
```

Result: passed.

Risks / follow-ups:

- The shutdown grace period defaults to 30 seconds. A still-running child after
  that point is marked `timed_out` through the existing process-tree termination
  path.
- `pueue group add` is now required during Rust `enable`; if Pueue changes
  duplicate-group exit behavior, the adapter may need to tolerate the specific
  already-exists response.
- The callback registry intentionally performs a narrow indentation-aware update
  instead of full YAML round-tripping, to preserve existing config formatting.
