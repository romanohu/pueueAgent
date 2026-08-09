# Task 4 Report: Safe Pueue adapter and submit command

## Delivered

- Added `PueueApi`, `CommandPueue`, typed `PueueTask`, and typed Pueue integration errors.
- `CommandPueue` invokes Pueue with `tokio::process::Command`; fixed configuration arguments, operations, `--escape`, `--`, and user arguments are passed as distinct argv entries without a shell.
- Hardened Pueue status parsing so task state details must be objects and optional timestamp fields must be strings when present; malformed task shapes return typed `InvalidStatusTask` errors.
- Added `submit::run` and `submit::run_with`. The submit path records a pending submission intent before `pueue add`, then attaches the returned task ID and a provisional task signature after success.
- The submit-time `task_signature` is now explicitly provisional: `provisional-submit:v1:group=<group>:task-id=<numeric-id>:intent=<submission-id>`. It is collision-resistant across repeated task IDs because it includes the unique submission intent ID, and it remains a placeholder until reconciliation replaces it with the authoritative enqueue/start/end signature.
- Wired the asynchronous `submit` CLI command to print the accepted task ID.
- Added fake-Pueue integration coverage for argument preservation with shell metacharacters and `--escape`, typed non-zero/invalid-JSON/invalid-task errors, pending-intent recovery after add failure, provisional signature uniqueness, and metadata attachment after a successful add.

## Scope

- No Bash is used by the Pueue production execution path.
- No reconciliation or termination behavior was added; authoritative signature replacement belongs to later reconciliation work.

## Changed files

- `Cargo.toml`
- `src/error.rs`
- `src/lib.rs`
- `src/main.rs`
- `src/pueue.rs`
- `src/submit.rs`
- `tests/integration/pueue_adapter.rs`
- `tests/support/fake_pueue.rs`
- `.superpowers/sdd/rust-sqlite-agent-supervisor/task-4-report.md`

## Verification

- Used the explicit temporary Cargo binary:
  `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/private/tmp/pueue-agent-cargo/bin:/usr/bin:/bin`.
- `cargo test --offline --test pueue_adapter` was first run after adding the review regression tests and failed on the missing `--escape`, lax status parsing, and old `group:id` task signature.
- `cargo test --offline --test pueue_adapter`: passed; 9 tests passed, 0 failed.
- `cargo fmt --all`: passed; exit code 0.
- `cargo test --offline --all-targets --all-features`: passed; 51 total tests passed, 0 failed.
- `cargo clippy --offline --all-targets --all-features -- -D warnings`: passed; finished `dev` profile.

`target/` contains local build artifacts from verification and was not staged.
