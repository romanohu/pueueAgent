# Task 4 Report: Safe Pueue adapter and submit command

## Delivered

- Added `PueueApi`, `CommandPueue`, typed `PueueTask`, and typed Pueue integration errors.
- `CommandPueue` invokes Pueue with `tokio::process::Command`; fixed configuration arguments, operations, and user arguments are passed as distinct argv entries without a shell.
- Added `submit::run` and `submit::run_with`. The submit path records a pending submission intent before `pueue add`, then attaches the returned task ID and task signature after success.
- Wired the asynchronous `submit` CLI command to print the accepted task ID.
- Added fake-Pueue integration coverage for argument preservation with shell metacharacters, typed non-zero and invalid-JSON errors, pending-intent recovery after add failure, and metadata attachment after a successful add.

## Scope

- No Bash is used by the Pueue production execution path.
- No reconciliation or termination behavior was added; those belong to later tasks.

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
  `/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin/cargo`.
- Set `RUSTUP_HOME=/private/tmp/pueue-agent-rustup`, `CARGO_HOME=/private/tmp/pueue-agent-cargo`, and `RUSTC=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin/rustc` for each Cargo command so verification did not depend on `cargo` being available on `PATH`.
- `cargo check --offline`: passed; finished `dev` profile.
- `cargo fmt --all --check`: passed; exit code 0.
- `cargo test --offline --all-targets --all-features`: passed; 48 total tests passed, 0 failed.
- `cargo clippy --offline --all-targets --all-features -- -D warnings`: passed; finished `dev` profile.

`target/` contains local build artifacts from verification and was not staged.
