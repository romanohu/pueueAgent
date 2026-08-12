# Task 4 report — Fix Round 2

## Result

Task 4 now fails closed for private-run generation cleanup. `PrivateRunTemp`
retains each run directory at its original `.pueue-agent/tmp/<run-id>` path.
`cleanup()` returns `PolicyViolationCode::TempUnsafe` on Unix and performs no
path mutation; `Drop` is a no-op. The retained directory is an intentional
bounded runtime/diagnostic artifact. A retry using the same run ID therefore
fails with the existing collision policy rather than risking another
generation.

## Race rationale

Under a same-UID parent-directory mutation threat, portable Linux/macOS APIs
do not provide an atomic conditional unlink or rename-by-inode operation. A
pathname can be replaced after identity observation and before the mutation;
descriptor-relative traversal plus a pathname rename cannot close that race.
The previous quarantine algorithm was removed completely. Task 6/native
process isolation is the deferred boundary for any future service-owned
cleanup mechanism.

Creation is fail-closed and retains any directory that was already created if
open, validation, metadata, or synchronization fails. Every successful
`mkdirat` immediately fsyncs its parent before the new directory is opened;
directory and parent synchronization errors are returned rather than ignored.

## Environment/auth coverage

Added explicit denials for `CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE`,
`AZURE_STORAGE_KEY`, and `AZURE_STORAGE_CONNECTION_STRING`, plus conservative
storage credential patterns. The direct Pueue environment regression verifies
trusted `PATH` and fixed baseline values only: proxy/certificate, auth, task,
and generated run/project variables are absent. Codex agent proxy/certificate
behavior remains scoped to the Codex agent; custom/task/Pueue parity remains
default-deny.

## Verification

- `cargo test --test codex_security -- --nocapture` — 23 passed
- `RUSTFLAGS='-D warnings' cargo check --all-targets` — passed
- `git diff --check` — passed
- `cargo fmt` unavailable in this host (`cargo` reports no `fmt` subcommand)

