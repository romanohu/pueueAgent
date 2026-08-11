# Upgrade Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `pueue-agent upgrade` で local `main` checkout の更新、テスト、release build、binary install、service restart、health check、rollback を安全に実行できるようにする。

**Architecture:** version/build metadata、source/git orchestration、binary installation、service lifecycle を分離する。upgrade は clean な main の fast-forward だけを許可し、既存の install layout を維持したまま新 binary を atomic に反映する。service restart は停止・ライフサイクル計画の `ServiceControl` API を利用する。

**Tech Stack:** Rust 2021、Clap、std::process::Command の argument vector、Tokio、既存 ServiceControl/ServiceManager、Git/Cargo、SQLite active-agent query、Rust integration tests。

## Global Constraints

- 更新対象は `origin/main` への fast-forward のみ。`git reset --hard` は使わない。
- dirty worktree、main 以外の branch、branch divergence、active agent run は更新前に拒否する。
- `cargo test --all-targets` と `cargo build --locked --release` 成功前に install path を変更しない。
- service failure / health failure では旧 binary に rollback し、Pueue task を操作しない。
- Pueue daemon、group、experiment task は upgrade の対象外である。
- upgrade は状態を bounded に表示し、秘密情報や全 command output を表示しない。
- 実装後の標準検証は `cargo test --all-targets` と `cargo fmt --check` で行う。

---

### Task 1: build metadata と `version` command を追加する

**Files:**
- Create: `build.rs`
- Create: `src/version.rs`
- Modify: `src/lib.rs`
- Modify: `src/cli.rs`
- Modify: `src/main.rs`
- Test: `tests/integration/cli_help.rs`

**Interfaces:**
- Add `Command::Version(VersionArgs)` and `VersionArgs { json: bool }`.
- Add `version::BuildInfo` with package version, git revision, source root, and optional service label.
- Add `version::render(BuildInfo, json) -> Result<String, AppError>`.

- [ ] **Step 1: Add failing version tests**

```rust
#[test]
fn version_reports_package_revision_and_json_mode() {
    let output = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["version", "--json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(value["package_version"].is_string());
    assert!(value["revision"].is_string());
    assert!(value["service"].is_string() || value["service"].is_null());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --test cli_help version_reports_package_revision_and_json_mode -- --exact`
Expected: FAIL because `version` is not a subcommand and build revision is not exposed.

- [ ] **Step 3: Implement build metadata and rendering**

In `build.rs`, run `git rev-parse --short=12 HEAD` when the source is a git checkout and set `PUEUE_AGENT_GIT_REVISION`; use `unknown` when git is unavailable. In `src/version.rs`, use `env!("CARGO_PKG_VERSION")` and `option_env!("PUEUE_AGENT_GIT_REVISION")`. Resolve the current executable and source root without printing full environment values. Add human output with lines `pueue-agent`, `revision`, `source`, and `service`, and bounded JSON with `schema_version`.

- [ ] **Step 4: Run version and help tests**

Run: `cargo test --test cli_help`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add build.rs src/version.rs src/lib.rs src/cli.rs src/main.rs tests/integration/cli_help.rs
git commit -m "feat: report pueue-agent build version"
```

### Task 2: source checkout validation と upgrade policy を追加する

**Files:**
- Create: `src/upgrade.rs`
- Modify: `src/lib.rs`
- Modify: `src/cli.rs`
- Test: `tests/integration/upgrade.rs`
- Modify: `Cargo.toml` to register the integration test target

**Interfaces:**
- Add `UpgradeArgs { source: Option<PathBuf>, json: bool }`.
- Add `UpgradeOptions` and `UpgradeReport`.
- Add `resolve_source_root(explicit: Option<&Path>, current_exe: &Path, env_source: Option<&Path>) -> Result<PathBuf, AppError>`.
- Add `validate_checkout(source: &Path, branch: &str, remote: &str) -> Result<CheckoutState, AppError>`.
- The test step defines `UpgradeFixture` with a temp git checkout, fake installed binary, fake command runner, and fake service recorder; later upgrade tasks reuse that fixture.

- [ ] **Step 1: Add failing source/policy tests**

```rust
#[test]
fn source_resolution_prefers_explicit_source_and_requires_cargo_manifest() {
    let fixture = UpgradeFixture::new();
    let source = resolve_source_root(
        Some(fixture.source_root()),
        Path::new("/unrelated/target/release/pueue-agent"),
        None,
    )
    .unwrap();
    assert_eq!(source, fixture.source_root());
}

#[test]
fn dirty_or_diverged_checkout_is_rejected_without_mutation() {
    let fixture = UpgradeFixture::dirty_main_checkout();
    let error = validate_checkout(fixture.source_root(), "main", "origin").unwrap_err();
    assert!(error.to_string().contains("clean") || error.to_string().contains("dirty"));
}
```

- [ ] **Step 2: Run focused tests to verify they fail**

Run: `cargo test --test upgrade source_resolution_ -- --nocapture`
Expected: FAIL because the upgrade module and source validation functions do not exist.

- [ ] **Step 3: Implement source resolution and git command adapter**

Resolve in this order: explicit `--source`, canonical executable path whose ancestors contain `target/release/pueue-agent`, then `PUEUE_AGENT_SOURCE_ROOT`. Require `.git` and `Cargo.toml` with package name `pueue-agent`. Execute git with `Command::new("git").current_dir(source).args(...)`; never pass a shell command string.

Validate clean worktree, current branch `main`, upstream `origin/main`, and ancestry. The update sequence uses `git fetch origin main` followed by `git merge --ff-only origin/main`; a divergent checkout returns an error before merge.

- [ ] **Step 4: Run source/policy tests**

Run: `cargo test --test upgrade source_resolution_ dirty_or_diverged_ -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/upgrade.rs src/lib.rs src/cli.rs tests/integration/upgrade.rs Cargo.toml
git commit -m "feat: validate upgrade source checkout"
```

### Task 3: test/build/install/rollback pipeline を追加する

**Files:**
- Modify: `src/upgrade.rs`
- Modify: `src/service.rs`
- Test: `tests/integration/upgrade.rs`

**Interfaces:**
- Add `UpgradeRunner` that accepts `UpgradeOptions`, `Db`, a `ServiceControl`, and an injected command runner for tests.
- `UpgradeReport` records old revision, new revision, test/build/install/restart/health outcomes, and rollback status.

- [ ] **Step 1: Add failing pipeline tests**

Add fake command and service implementations with these cases:

```rust
#[tokio::test]
async fn build_failure_leaves_installed_binary_and_service_unchanged() {
    let fixture = UpgradeFixture::build_failure();
    let report = fixture.run_upgrade().await.unwrap_err();
    assert!(report.to_string().contains("build"));
    assert_eq!(fixture.installed_binary(), fixture.old_binary_bytes());
    assert_eq!(fixture.service_calls(), Vec::<String>::new());
}

#[tokio::test]
async fn service_health_failure_restores_previous_binary() {
    let fixture = UpgradeFixture::service_failure();
    let error = fixture.run_upgrade().await.unwrap_err();
    assert!(error.to_string().contains("rollback"));
    assert_eq!(fixture.installed_binary(), fixture.old_binary_bytes());
    assert!(fixture.service_calls().contains(&"restart".to_owned()));
}
```

- [ ] **Step 2: Run pipeline tests to verify they fail**

Run: `cargo test --test upgrade build_failure_ service_health_failure_ -- --nocapture`
Expected: FAIL because the pipeline and rollback behavior are absent.

- [ ] **Step 3: Implement the guarded pipeline**

Before fetch, query `AgentRunRepository::find_active_by_project` for every enabled project and fail if any active run exists. Acquire an upgrade lock under the state directory containing the current PID; reject a live lock and reclaim only a lock whose PID is no longer alive.

After a successful fast-forward, run `cargo test --all-targets`. Build with `cargo build --locked --release --target-dir <temporary-target-dir>`. Copy the candidate binary to an install-side temporary path, copy the current binary to a state-directory backup, atomically rename the candidate into the existing release path, and retain the backup until health check succeeds.

Use the lifecycle `ServiceControl::restart` API. After restart, require `ServiceStatus::Running`, open the SQLite database, and run the configured Pueue status command. On any post-install failure, atomically restore the backup, restart the service, and return an error that includes whether rollback succeeded.

- [ ] **Step 4: Run upgrade pipeline tests**

Run: `cargo test --test upgrade`
Expected: PASS for successful update, no-op update, test/build failure, service failure, rollback, active-agent rejection, and lock contention.

- [ ] **Step 5: Commit**

```bash
git add src/upgrade.rs src/service.rs tests/integration/upgrade.rs
git commit -m "feat: add guarded upgrade and rollback pipeline"
```

### Task 4: CLI 接続と Japanese upgrade documentation を追加する

**Files:**
- Modify: `src/cli.rs`
- Modify: `src/main.rs`
- Modify: `README.md`
- Create: `docs/operations-ja.md` or extend the file created by the lifecycle plan
- Test: `tests/integration/cli_help.rs`

- [ ] **Step 1: Add failing CLI assertions**

Assert that `--help` includes `upgrade`, `upgrade --help` includes `--source` and `--json`, and `version --help` includes `--json`.

- [ ] **Step 2: Run help tests to verify they fail**

Run: `cargo test --test cli_help upgrade_ version_ -- --nocapture`
Expected: FAIL until the commands are connected.

- [ ] **Step 3: Connect command handlers and output**

Construct the database and service manager, invoke `UpgradeRunner`, and render a bounded human or JSON report. A no-op update must not restart the service. A failure must exit non-zero and print the next safe diagnostic command (`pueue-agent doctor` or `pueue-agent version`) without dumping git/cargo output.

- [ ] **Step 4: Write the Japanese update workflow**

Document the normal command `pueue-agent upgrade`, source auto-detection, `--source` fallback, prerequisites (`git`, Rust/Cargo), dirty-worktree refusal, active-agent refusal, rollback behavior, and the fact that Pueue experiments continue. Include the manual fallback `git pull --ff-only` plus `./install.sh` only as a recovery procedure, not the primary workflow.

- [ ] **Step 5: Run CLI/documentation tests**

Run: `cargo test --test cli_help --test upgrade` and `git diff --check`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/cli.rs src/main.rs README.md docs/operations-ja.md tests/integration/cli_help.rs
git commit -m "docs: document pueue-agent upgrade workflow"
```

## Final Verification

- Run `cargo fmt --all` and then `cargo fmt --check`.
- Run `cargo test --all-targets`.
- Run `git diff --check`.
- Run `pueue-agent version --json` from the built binary.
- Run the upgrade integration fixture against a local fake checkout; verify that a failed health check restores the previous binary and never sends a Pueue kill.
