# Research reliability stage1 final fix wave

Base: `600c1eca1623ab73996d891e64df7401c75eb919`

Fix commit: `fd30526fd6a73b64ec2d6297cca326f6fe2e7c38`
(`fix: close instruction backup durability gaps`)

## Scope

- Sync the `.pueue-agent` parent after creating or reusing
  `instructions.backups`, with a private `cfg(test)` parent-sync fault stage.
  The regression injects repeated parent-sync failures on creation and reuse,
  checks that the original instructions remain unchanged, and checks that no
  backup/rename publication occurs until the sync succeeds.
- Set the integration-test backup directory to `0700` and assert the backup
  symlink is rejected at the intended no-follow open boundary.
- Send bounded completion from the waiting-lock worker before joining it.
- Wait for a child-written `ready` marker emitted after the Python SIGTERM
  handler is installed, while retaining bounded polling.

No externally configurable production hooks, dependencies, SQLite/Pueue/session
behavior, remote checkout, or parent-owned verification document were changed.

## TDD evidence

RED, before the parent-sync implementation:

```text
$ cargo test --lib instructions_file::tests::backup_parent_directory_sync_failures_preserve_original_and_prevent_rename -- --test-threads=1
test instructions_file::tests::backup_parent_directory_sync_failures_preserve_original_and_prevent_rename ... FAILED
assertion failed: !backup_path.exists()
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 323 filtered out
```

The failure showed the old ordering created the backup file before the
faulted parent durability boundary.

GREEN after the fix:

```text
$ cargo test --lib instructions_file::tests::backup_parent_directory_sync_failures_preserve_original_and_prevent_rename -- --test-threads=1
test ...backup_parent_directory_sync_failures_preserve_original_and_prevent_rename ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 323 filtered out
```

## Focused verification

```text
$ cargo test --lib instructions -- --test-threads=1
test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 308 filtered out

$ cargo test --lib instructions -- --test-threads=4
test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 308 filtered out

$ cargo test --test instructions -- --test-threads=1
test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

$ bats tests/test_shell_entrypoints.bats
1..21
21 tests, 21 passed

$ git diff --check
# exit 0, no output

$ rustfmt --check --edition 2021 src/instructions_file.rs
# exit 0, no output

$ rustfmt --check --edition 2021 tests/integration/instructions.rs
# exit 0, no output

$ python3 -B -c 'from pathlib import Path; compile(Path("tests/support/signal_supervisor.py").read_text(), "tests/support/signal_supervisor.py", "exec")'
# exit 0, no output
```

The repository-wide `cargo fmt --check` remains nonzero because the baseline
contains 1,816 unrelated formatting diffs (first reported in `build.rs` and
many untouched production/test files). No unrelated formatting was applied;
the changed Rust files pass direct `rustfmt --check` above.

Linux/roko execution is parent-owned and was not run from this macOS worktree.
The parent has the full Linux Rust/Bats and E2E evidence. The parent-owned
`docs/report/2026-09-15-research-reliability-stage1-verification.md` remains
untouched and unstaged.
