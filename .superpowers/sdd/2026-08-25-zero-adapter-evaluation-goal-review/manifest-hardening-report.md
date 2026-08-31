# Phase 4 manifest file-type hardening report

## Status

DONE

## Base, branch, and worktree

- Base: `9fd71b70c93e55829539ec4722dd78ecec007eb0`
- Branch: `codex/phase4-evaluation-goal-review`
- Worktree: `/Users/suzuki_f/project/pueueAgent/.worktrees/phase4-evaluation-goal-review`
- Date: 2026-08-31

## Commit hashes

- `ea106db912c1db76f560b18e39e9c495468f3066` — `fix: harden result manifest file types`

## Files changed

- `src/result_manifest.rs`
- `tests/integration/reconciliation.rs`

## Implementation summary

- Unix manifest candidates now open with `O_NONBLOCK`, `O_NOFOLLOW`, and
  `O_CLOEXEC` through `OpenOptionsExt`, so final-component symlinks are not
  followed and special files cannot block the reconciliation read.
- The opened descriptor's metadata is checked and only regular files are read.
  FIFO, directory, and other opened non-regular descriptors return an invalid
  manifest classification. Kernels that reject sockets or other special files
  before returning a descriptor are classified with a no-follow metadata probe
  only after the safe open attempt; regular-file open failures remain
  `AppError::Io` and therefore retain the `result_io_error` retry path.
- Missing candidates still return `None` and fall through to the next candidate.
  Invalid candidates terminate discovery with `result_invalid`.
- The existing 16 KiB bound and JSON/schema/metric validation are unchanged.
- Added real Unix-gated reconciliation tests for symlink rejection and bounded
  FIFO handling. No general filesystem abstraction or unrelated portability
  layer was added.

## RED evidence

Tests were written before the production mutation and each was run against the
ordinary `File::open` implementation.

### Symlink regression

**Command:**

```text
cargo test --test reconciliation terminal_projection_rejects_symlink_manifest -- --test-threads=1
```

**Observed RED:**

```text
test terminal_projection_rejects_symlink_manifest ... FAILED
assertion `left == right` failed
left: None
right: Some("result_invalid")
test result: FAILED. 0 passed; 1 failed; 49 filtered out
error: test failed ... exit 101
```

This catches the production mutation that follows a symlink (ordinary
`File::open`) instead of rejecting the candidate before ingestion; the RED
implementation followed the valid target and produced no artifact defect.

### FIFO regression

**Command:**

```text
cargo test --test reconciliation terminal_projection_rejects_fifo_manifest_without_blocking -- --test-threads=1
```

**Observed RED:**

```text
test terminal_projection_rejects_fifo_manifest_without_blocking ... FAILED
panicked: FIFO manifest must not block reconciliation
test result: FAILED. 0 passed; 1 failed; 49 filtered out
error: test failed ... exit 101
```

The test runs reconciliation in a bounded blocking task. The cleanup writer is
started only after the 250 ms timeout, releases the unsafe reader, and is joined
before the assertion, so the RED run demonstrates the block without leaving a
hung process. This catches omission of `O_NONBLOCK` and/or the descriptor
regular-file check before `read_to_end`.

## Verification

- `cargo test --test reconciliation terminal_projection_rejects_symlink_manifest -- --test-threads=1` — PASS: 1 passed, 0 failed, 49 filtered out.
- `cargo test --test reconciliation terminal_projection_rejects_fifo_manifest_without_blocking -- --test-threads=1` — PASS: 1 passed, 0 failed, 49 filtered out.
- `cargo test --test reconciliation -- --test-threads=1` — PASS: 50 passed, 0 failed.
- `cargo check --all-targets` — PASS: finished with 0 errors.
- `git diff --check` — PASS: no output.
- `cargo fmt --all -- --check` — reports pre-existing formatting differences
  across unrelated repository files; no formatter rewrite was run.

## Self-review

- Security decisions for readable candidates are made from descriptor metadata,
  not a pre-open path metadata check. The no-follow probe exists only for Unix
  errors where the kernel refuses to produce a descriptor for a special file,
  and it never authorizes a read.
- `ELOOP` from `O_NOFOLLOW` is an invalid symlink candidate. FIFO, directories,
  and other descriptors are rejected before any read. Missing candidates retain
  the prior fall-through behavior.
- Metadata and read errors for intended regular files continue to map to
  `AppError::Io`, preserving durable `result_io_error` recovery behavior.
- Both new tests exercise the real reconciliation path and are clearly
  `#[cfg(unix)]` gated. Existing regular-file ingestion and I/O recovery remain
  green in the full reconciliation suite.
- Only the two implementation/test files are in the implementation commit; no
  merge, push, or roko access was performed.

## Concerns

- No focused implementation concerns remain after the required tests and
  all-target compile check. The repository-wide formatter check remains red due
  to unrelated baseline formatting differences, so unrelated files were left
  unchanged.
