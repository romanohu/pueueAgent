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
