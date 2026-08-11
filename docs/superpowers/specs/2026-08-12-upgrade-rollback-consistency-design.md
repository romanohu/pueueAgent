# Upgrade Rollback Consistency Design

## Goal

Make the upgrade consistency boundary cover the supervisor's SQLite writers, installed binary, and rollback state without changing the no-Pueue-task mutation policy.

## Design

- Keep the existing upgrade coordination lock for the entire `run()` operation.
- After the final active-agent rejection and before `VACUUM INTO` or binary installation, stop the supervisor through `ServiceControl::stop`.
- Treat launchd `bootout` output containing `Could not find service` as a successful idempotent stop. Preserve all other lifecycle failures.
- Keep rollback ordered as service stop, SQLite snapshot restore, binary restore, service restart, and health check. The SQLite snapshot and binary backup are removed only after a successful upgrade or after rollback handling completes.
- Do not invoke Pueue task mutation APIs. Document that the short quiesced consistency window is not a time for operator SQLite writes.

## Verification

Focused integration tests will assert stop-before-snapshot ordering, launchd stop behavior, and restoration of both SQLite and binary state in rollback. Documentation contract tests will cover the new rollback and quiesce guarantees. Final verification will run the requested service, upgrade, CLI, and full `cargo test --all-targets` suites plus `git diff --check`.
