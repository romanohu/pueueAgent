# Upgrade Revision Comparison Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make upgrade recognize a current binary when its embedded 12-character revision matches the prefix of the checkout's full HEAD, while preserving stale and unknown handling.

**Architecture:** Keep the existing `build.rs` short revision format. Add a small comparison predicate in `src/upgrade.rs` that rejects `unknown` and empty values, then accepts either exact equality or a safe short/full prefix relationship. Exercise the behavior through the existing real git fixture and assert that a current revision returns no-op without invoking cargo, install, or service restart.

**Tech Stack:** Rust, Tokio integration tests, local git fixture, Cargo.

## Global Constraints

- Do not modify service, periodic, cancel, or scheduler code.
- Preserve stale and `unknown` revisions as non-no-op.
- Run focused upgrade tests, `cargo test --all-targets`, and `git diff --check`.
- Commit the verified change and report the commit SHA.

---

### Task 1: Fix short/full revision no-op detection

**Files:**
- Modify: `src/upgrade.rs` near `UpgradeRunner::run` no-op decision
- Modify: `tests/integration/upgrade.rs` near existing no-op and stale/unknown tests

**Interfaces:**
- Consumes: `UpgradeOptions.installed_revision` and `CheckoutState.head`
- Produces: no-op behavior when either revision is a non-empty, non-`unknown` prefix of the other

- [x] **Step 1: Add the realistic failing regression test**

Use the existing `UpgradeFixture` and real git HEAD:

```rust
#[tokio::test]
async fn twelve_character_installed_revision_matches_full_checkout_head() {
    let fixture = UpgradeFixture::new();
    let full_head = git_stdout(fixture.source_root(), ["rev-parse", "HEAD"]);
    let installed_revision = full_head[..12].to_owned();

    let report = fixture
        .run_upgrade_with_installed_revision(&installed_revision)
        .await
        .unwrap();

    assert_eq!(report.old_revision, full_head);
    assert_eq!(report.new_revision, full_head);
    assert!(!report.tests.attempted);
    assert!(!report.build.attempted);
    assert!(!report.install.attempted);
    assert!(!report.restart.attempted);
    assert!(!report.health.attempted);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert!(fixture.service_calls().is_empty());
    assert!(fixture.commands.command_invocations.borrow().is_empty());
}
```

- [x] **Step 2: Run the regression test and verify it fails for the comparison bug**

Run:

```bash
cargo test --test upgrade twelve_character_installed_revision_matches_full_checkout_head -- --exact --nocapture
```

Expected: FAIL because the current implementation requires the installed revision to equal the full checkout HEAD and proceeds into the upgrade pipeline.

- [x] **Step 3: Implement the minimal safe comparison**

Add a private predicate that returns false for `unknown` or empty strings and otherwise accepts exact equality or either prefix direction. Use it in the existing no-op condition without changing fetch, merge, retry-marker, or lifecycle behavior.

- [x] **Step 4: Run focused upgrade tests and verify they pass**

Run:

```bash
cargo test --test upgrade -- --nocapture
```

Expected: all upgrade integration tests pass, including stale and `unknown` non-no-op coverage.

- [x] **Step 5: Run repository verification and inspect scope**

Run:

```bash
cargo test --all-targets
git diff --check
git diff --name-only HEAD
```

Expected: tests pass, diff check is clean, and only the plan plus upgrade implementation/test files are changed; no service, periodic, cancel, or scheduler files are modified.

- [x] **Step 6: Commit the verified change**

```bash
git add docs/superpowers/plans/2026-08-11-upgrade-revision-comparison.md src/upgrade.rs tests/integration/upgrade.rs
git commit -m "fix: recognize current short upgrade revision"
```
