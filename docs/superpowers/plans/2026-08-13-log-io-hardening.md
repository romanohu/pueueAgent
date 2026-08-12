# Log I/O Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound every configured log tail and make agent, task, and extra-log reads safe against symlinks, races, weak file types, and project-root escapes.

**Architecture:** Keep `LogSnapshot` as the detector’s value object, but make its byte-reading path descriptor-based: metadata, seek, and read all operate on one already-open descriptor. Add a Unix secure agent-log opener for the native launcher to consume, and a descriptor-relative project-root walker for Detector; configuration and the read boundary enforce the same 1 MiB cap.

**Tech Stack:** Rust 2021, `std::fs::File`, Unix `openat`/`O_NOFOLLOW`/`O_CLOEXEC` via `libc`, `serde`/TOML validation, existing Tokio/Rusqlite tests, `tempfile`, and host-independent Rust integration tests.

## Global Constraints

- `check.log_tail_bytes` must be 1..=1,048,576 bytes at both configuration load and log-read boundaries.
- Agent logs are owner-only regular files opened no-follow; weak permissions, symlinks, devices, and directories return `policy_blocked:agent_log_unsafe`.
- Task/extra paths are relative only; reject absolute paths, `..`, symlinks, devices, directories, `/tmp`, and outside-root paths.
- Task/extra logs are opened descriptor-relatively beneath a project-root directory descriptor; never canonicalize a path and open it later.
- Metadata and bytes for one snapshot come from the same descriptor.
- The native launch gate owns launch/marker/exec behavior. This plan provides its secure agent-log opening interface but does not implement the native gate.
- Do not change Codex arguments/session policy, Pueue policy, process-group policy, SQLite policy projections, or external notifications.
- Tests use generated temporary fixtures and no real Codex/Pueue, network, or host `/bin` layout.
- Every production change has a RED test, a minimal GREEN implementation, and a focused commit.
- Final checks are `cargo fmt --all -- --check`, `cargo test --all-targets`, `git diff --check`, and `bats tests/test_shell_entrypoints.bats`.

---

## File map and cross-plan interfaces

Files: create `src/project_logs.rs`; modify `src/logs.rs`, `src/config.rs`,
`src/detect.rs`, `src/daemon.rs`, `src/diagnostics.rs`, and `src/lib.rs`; test
the three integration files plus Unix unit tests. Consume `libc = "0.2"` from
the core plan; do not add a competing dependency declaration here.

The core plan owns the shared typed error. This plan consumes the exact
`crate::execution_policy::{PolicyViolation, LogUnsafeReason}` interface:

```rust
// Declared by the core plan; this plan does not redefine it.
pub struct PolicyViolation {
    pub code: PolicyViolationCode,             // LogUnsafe
    pub stage: PolicyViolationStage,           // NativeGate or PreBinding read boundary
    pub detail: PolicyViolationDetail,         // LogUnsafe(LogUnsafeReason)
}
```

No log safety test matches rendered strings. Tests match
`code == LogUnsafe` and `detail == LogUnsafe(reason)` directly; paths remain in
the caller's bounded project-scoped context and are not stored in the
violation.

Interfaces provided to the core native-gate plan:

```rust
pub struct AgentLogFile { file: std::fs::File, identity: LogFileIdentity, path: std::path::PathBuf }
pub struct GateMarkerIdentity { /* descriptor-derived device/inode/owner/mode */ }

pub fn open_agent_log(root: &ProjectRootLogReader, relative: &Path)
    -> Result<AgentLogFile, AppError>;
pub fn ensure_agent_log_dir(root: &ProjectRootLogReader) -> Result<(), AppError>;
pub fn create_gate_marker(root: &ProjectRootLogReader, relative: &Path)
    -> Result<GateMarkerIdentity, AppError>;
pub fn inspect_gate_marker(root: &ProjectRootLogReader, relative: &Path)
    -> Result<Option<GateMarkerIdentity>, AppError>;
pub fn inspect_existing_agent_log(root: &ProjectRootLogReader, relative: &Path)
    -> Result<LogFileIdentity, AppError>;
impl AgentLogFile {
    pub fn try_clone(&self) -> Result<std::fs::File, AppError>;
    pub fn into_file(self) -> std::fs::File;
    pub fn identity(&self) -> &LogFileIdentity;
    pub fn path(&self) -> &Path;
}
```

All functions walk every relative component from the pinned project-root
descriptor with `openat` and `O_NOFOLLOW`; neither resolves an intermediate
directory by ambient path. `ensure_agent_log_dir` creates only the fixed
`.pueue-agent/logs` components with `mkdirat` mode `0700` when absent and
rejects existing components that are not owner-controlled directories.
`open_agent_log` creates/appends the final component
with mode `0600`, then validates regular type, effective-UID ownership, and
`(mode & 0o077) == 0`. It never repairs weak files.
`create_gate_marker` uses `O_NOFOLLOW|O_CREAT|O_EXCL`, mode `0600`, writes the
bounded marker state, then `fsync`s the file and containing directory before
returning. `inspect_gate_marker` is read-only, accepts only an owner-only regular
marker with the exact bounded state, and distinguishes absence from unsafe or
indeterminate state without following an absolute path.
`inspect_existing_agent_log` uses `O_RDONLY|O_CLOEXEC|O_NOFOLLOW`, never
`O_CREAT`, reads no bytes, and maps missing paths to
`LogUnsafeReason::Missing`. The core gate consumes descriptors, never reopening
`path`. Unsafe type/ownership/path/content conditions return
`AppError::PolicyViolation`; operational open/write/fsync/close failures return
the existing bounded I/O/runtime errors so pre-marker retry classification is
preserved.

Interfaces provided to `Detector` and `LogSnapshot`:

```rust
pub const MAX_LOG_TAIL_BYTES: u32 = 1_048_576;
impl LogSnapshot {
    pub fn read_tail(path: &Path, tail_bytes: u32) -> Result<Self, AppError>;
    pub fn read_tail_from_file(file: &std::fs::File, tail_bytes: u32) -> Result<Self, AppError>;
}

pub struct ProjectRootLogReader { /* core VerifiedProjectRoot descriptor */ }
pub struct OpenedProjectLog { /* one descriptor + relative name */ }
impl ProjectRootLogReader {
    pub fn from_verified(root: VerifiedProjectRoot) -> Self;
    pub fn open(root: &Path) -> Result<Self, AppError>;
    pub fn open_relative(&self, relative: &Path) -> Result<OpenedProjectLog, AppError>;
}
impl OpenedProjectLog {
    pub fn file(&self) -> &std::fs::File;
    pub fn relative_path(&self) -> &Path;
    pub fn snapshot(&self, tail_bytes: u32) -> Result<LogSnapshot, AppError>;
}
```

`from_verified` is the production constructor and consumes the core plan's
`ProjectRootAnchor::verify_identity` result. `open(path)` is a fixture/helper
constructor that creates and immediately verifies the same anchor; it is not
used by production agent/recovery paths. `read_tail(path)` opens with
`O_RDONLY|O_CLOEXEC|O_NOFOLLOW` before metadata and
delegates to `read_tail_from_file`. The root reader pins device/inode/owner/mode
against canonical-root identity; `open_relative` rejects empty, `CurDir`,
absolute, prefix, and `ParentDir` components and uses no-follow walk/open.
`read_tail_from_file` does fstat, seek, and read on that same descriptor.

### Task 1: Enforce the bounded tail contract

**Files:**
- Modify: `src/config.rs:1-20,225-245`
- Modify: `src/logs.rs:1-65`
- Test: `tests/integration/config.rs` and `tests/integration/detection.rs`

**Interfaces:**
- Produces `config::MAX_LOG_TAIL_BYTES` (or re-exports `logs::MAX_LOG_TAIL_BYTES`) with value `1_048_576`.
- Produces `LogSnapshot::read_tail_from_file` and makes both read methods reject values outside `1..=MAX_LOG_TAIL_BYTES` with `AppError::Configuration { field: "check.log_tail_bytes" }`.

- [ ] **Step 1: Write the failing cap tests.** Add these exact assertions to the existing config/detection fixtures:

```rust
#[test]
fn log_tail_bytes_accepts_one_mib_but_rejects_zero_and_one_mib_plus_one() {
    assert_eq!(load_config(valid_config()
        .replace("log_tail_bytes = 16384", "log_tail_bytes = 1048576"))
        .unwrap().check.log_tail_bytes, 1_048_576);
    for value in ["0", "1048577"] {
        let error = load_config(valid_config()
            .replace("log_tail_bytes = 16384", &format!("log_tail_bytes = {value}")))
            .unwrap_err();
        assert!(error.to_string().contains("check.log_tail_bytes"));
    }
}

#[test]
fn log_snapshot_read_boundary_rejects_out_of_range_tail() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(temp.path(), b"bounded").unwrap();
    for value in [0, 1_048_577] {
        let error = pueue_agent::logs::LogSnapshot::read_tail(temp.path(), value).unwrap_err();
        assert!(error.to_string().contains("check.log_tail_bytes"));
    }
}

#[cfg(unix)]
#[test]
fn public_path_tail_read_rejects_a_symlink_before_metadata_or_read() {
    let temp = tempfile::tempdir().unwrap();
    let target = temp.path().join("target.log");
    let link = temp.path().join("link.log");
    std::fs::write(&target, b"target").unwrap();
    std::os::unix::fs::symlink(&target, &link).unwrap();
    assert!(matches!(pueue_agent::logs::LogSnapshot::read_tail(&link, 64),
        Err(pueue_agent::AppError::PolicyViolation { violation:
            pueue_agent::execution_policy::PolicyViolation {
                code: PolicyViolationCode::LogUnsafe,
                detail: PolicyViolationDetail::LogUnsafe(LogUnsafeReason::Symlink), ..
            }
        })));
}
```

- [ ] **Step 2: Run RED.** Run `cargo test --test config log_tail_bytes_accepts_one_mib_but_rejects_zero_and_one_mib_plus_one` and `cargo test --test detection log_snapshot_read_boundary_rejects_out_of_range_tail`. Expected failure: the current validator accepts 1,048,577 and `read_tail` silently clamps only to file length.

- [ ] **Step 3: Implement the minimal cap and same-descriptor reader.** Use one validator and call it at both public boundaries:

```rust
pub const MAX_LOG_TAIL_BYTES: u32 = 1_048_576;

fn bounded_tail(value: u32) -> Result<u64, AppError> {
    if !(1..=MAX_LOG_TAIL_BYTES).contains(&value) {
        return Err(AppError::Configuration { field: "check.log_tail_bytes" });
    }
    Ok(u64::from(value))
}
```

`read_tail_from_file` obtains metadata from `file`, seeks to `metadata.len().saturating_sub(tail)`, reads through `take`, and computes mtime/fingerprint from that metadata and byte buffer. `read_tail` opens once with `O_RDONLY|O_CLOEXEC|O_NOFOLLOW` on Unix and delegates; it must not call `metadata(path)` before opening. Wrap a typed `PolicyViolation` from a no-follow failure in the core `AppError::PolicyViolation` variant so callers can exact-match the stage/reason.

```rust
let file = open_read_no_follow(path)?; // no metadata(path) call precedes this
Self::read_tail_from_file(&file, tail_bytes)
```

- [ ] **Step 4: Run GREEN.** Run the two focused commands again; expected result: PASS. Then run `cargo test --test config --test detection`; expected result: existing detector evidence tests plus the new cap tests pass.

- [ ] **Step 5: Commit.**

```bash
git add src/config.rs src/logs.rs tests/integration/config.rs tests/integration/detection.rs
git commit -m "fix: bound configured log tails"
```

### Task 2: Add secure agent-log opening for the native gate

**Files:**
- Create: `src/project_logs.rs` (root pinning and reusable component walker)
- Modify: `src/logs.rs:1-65`
- Modify: `src/lib.rs`
- Test: Unix unit tests in `src/logs.rs`

**Interfaces:**
- Consumes `MAX_LOG_TAIL_BYTES`/`AppError` from Task 1.
- Produces `ProjectRootLogReader`, its private `openat` component walker,
  `LogFileIdentity`, `AgentLogFile`, `ensure_agent_log_dir`, `open_agent_log`,
  `create_gate_marker`, `inspect_gate_marker`, and
  `inspect_existing_agent_log` exactly as declared above; unsafe files map to
  `PolicyViolationCode::LogUnsafe` plus typed
  `PolicyViolationDetail::LogUnsafe(reason)`, never a rendered-string
  assertion or normal retryable I/O result.
- The core native-gate integration task later consumes this opener and changes `AgentRunner`; this task does not touch `src/agent.rs`.

- [ ] **Step 1: Write the failing secure-file tests.** Add Unix tests that create a temp path, then assert creation mode/regular type/current UID; create a pre-existing `0640` file and assert `agent_log_unsafe`; create a directory and a symlink to an outside regular file and assert `agent_log_unsafe`. Verify descriptor stability:

```rust
#[cfg(unix)]
#[test]
fn secure_agent_log_rejects_weak_directory_and_symlink_files() {
    let temp = tempfile::tempdir().unwrap();
    let reader = ProjectRootLogReader::open(temp.path()).unwrap();
    let weak = temp.path().join("weak.log");
    std::fs::write(&weak, b"x").unwrap();
    std::fs::set_permissions(&weak, std::os::unix::fs::PermissionsExt::from_mode(0o640)).unwrap();
    assert!(matches!(open_agent_log(&reader, Path::new("weak.log")),
        Err(AppError::PolicyViolation { violation: PolicyViolation {
            code: PolicyViolationCode::LogUnsafe,
            detail: PolicyViolationDetail::LogUnsafe(LogUnsafeReason::WeakPermissions), ..
        } })));
    let directory = temp.path().join("directory.log");
    std::fs::create_dir(&directory).unwrap();
    assert!(matches!(open_agent_log(&reader, Path::new("directory.log")),
        Err(AppError::PolicyViolation { violation: PolicyViolation {
            code: PolicyViolationCode::LogUnsafe,
            detail: PolicyViolationDetail::LogUnsafe(LogUnsafeReason::Directory), ..
        } })));
    let outside = temp.path().join("outside.log");
    std::fs::write(&outside, b"outside").unwrap();
    let link = temp.path().join("link.log");
    std::os::unix::fs::symlink(&outside, &link).unwrap();
    assert!(matches!(open_agent_log(&reader, Path::new("link.log")),
        Err(AppError::PolicyViolation { violation: PolicyViolation {
            code: PolicyViolationCode::LogUnsafe,
            detail: PolicyViolationDetail::LogUnsafe(LogUnsafeReason::Symlink), ..
        } })));
}
```

Also assert `mode & 0o077 == 0`, `metadata.uid() == geteuid()`, and `try_clone()` can be passed to `LogSnapshot::read_tail_from_file`. Start with a missing `.pueue-agent/logs`, call `ensure_agent_log_dir`, and assert both components are real owner-controlled directories; reject a symlink or group-writable existing component. Add marker tests proving exclusive mode-0600 creation, no-follow inspection, exact-state validation, and missing-versus-unsafe distinction. Add `inspect_existing_agent_log_does_not_create_or_read`: call it on a missing path and assert `LogUnsafeReason::Missing`, then assert the path still does not exist; call it on a sentinel file and assert only identity is returned (the sentinel contents are never loaded).

- [ ] **Step 2: Run RED.** Run `cargo test --lib logs::tests::secure_agent_log_rejects_weak_directory_and_symlink_files` and `cargo test --lib logs::tests::inspect_existing_agent_log_does_not_create_or_read`. Expected failure: `open_agent_log`, `inspect_existing_agent_log`, `AgentLogFile`, `PolicyViolation`, and `LogUnsafeReason` do not exist; current log access has no typed no-follow contract.

- [ ] **Step 3: Implement only the pinned root, secure directories/opener/marker, and inspector.** Consume `VerifiedProjectRoot` and the `libc` dependency supplied by the core plan; do not add or duplicate either. Production uses `ProjectRootLogReader::from_verified`; test/helper `open` creates a `ProjectRootAnchor`, verifies it, and delegates. Its private walker rejects empty/absolute/prefix/parent/curdir components and opens every intermediate directory with `openat(..., O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC)`. `ensure_agent_log_dir` uses descriptor-relative `mkdirat` only for the two fixed components and validates owner, directory type, and no group/other write bits on the opened descriptors. `open_agent_log` opens only the final component with `O_NOFOLLOW|O_CLOEXEC|O_CREAT|O_APPEND`, mode `0600`; `create_gate_marker` uses the same walker with `O_NOFOLLOW|O_CLOEXEC|O_CREAT|O_EXCL`, validates it, writes only the fixed marker state, and syncs the file and parent descriptor; `inspect_gate_marker` never creates or follows and validates exact contents from the opened descriptor. `inspect_existing_agent_log` uses `O_RDONLY|O_CLOEXEC|O_NOFOLLOW` without `O_CREAT`. Immediately fstat each returned descriptor and return exact typed log-unsafe details for non-regular, non-owner, weak-mode, symlink, directory, device, invalid marker contents, or missing files. On non-Unix return the existing typed unsupported-execution/native-gate violation. Do not modify `src/agent.rs`; the core gate task owns that call-site integration.

Minimal opener shape:

```rust
let file = root.open_final(
    relative,
    libc::O_RDWR | libc::O_APPEND | libc::O_CREAT | libc::O_NOFOLLOW | libc::O_CLOEXEC,
    0o600,
)?;
let identity = LogFileIdentity::from_open_descriptor(&file)
    .map_err(|_| agent_log_unsafe(path))?;
if !identity.is_regular_owner_only() { return Err(PolicyViolation {
    code: PolicyViolationCode::LogUnsafe,
    stage: PolicyViolationStage::NativeGate,
    detail: PolicyViolationDetail::LogUnsafe(LogUnsafeReason::WeakPermissions),
}.into()); }
Ok(AgentLogFile { file, identity, path: relative.to_owned() })
```

The non-mutating inspector uses the same `fstat` helper but omits `create` and
returns `LogUnsafeReason::Missing` on `ENOENT`; it never calls
`read_to_end`, `metadata(path)`, `set_permissions`, or `create_dir_all`.

- [ ] **Step 4: Run GREEN.** Run the two focused log tests; expected result: PASS, with exact code/detail matches and no missing-file creation. Do not run or modify the scheduler log-open test in this task because `AgentRunner` integration belongs to the core gate task. The core task must later run `cargo test --test scheduler log_open_failure_finishes_the_inserted_agent_run` after consuming this interface.

- [ ] **Step 5: Commit.**

```bash
git add src/project_logs.rs src/logs.rs src/lib.rs
git commit -m "feat: define secure agent log and marker I/O"
```

### Task 3: Replace Detector canonicalize-then-open with descriptor-relative reads

**Files:**
- Modify: `src/project_logs.rs`, `src/lib.rs`, `src/detect.rs:342-505`, `src/daemon.rs:212-216`
- Modify: `tests/integration/detection.rs` (move task fixtures beneath project root)
- Test: Unix unit tests in `src/project_logs.rs`

**Interfaces:**
- Consumes `LogSnapshot::read_tail_from_file` from Task 1.
- Produces `ProjectRootLogReader`/`OpenedProjectLog` exactly as declared in the cross-plan interface section.
- Changes `Detector::for_project`’s third argument to a project-relative task-log directory (the daemon passes `.pueue-agent/logs`); `inspect_task_at` opens one `ProjectRootLogReader` and uses it for task and extra logs.

- [ ] **Step 1: Write the failing traversal, type, and swap tests.** Add tests for `logs/train.log` success, absolute path rejection, `../outside.log` rejection, symlinked intermediate/final components, directory final component, and a stable descriptor after replacement:

```rust
#[cfg(unix)]
#[test]
fn descriptor_relative_snapshot_survives_path_symlink_swap() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("project");
    std::fs::create_dir_all(root.join("logs")).unwrap();
    let path = root.join("logs/train.log");
    std::fs::write(&path, b"original").unwrap();
    let reader = ProjectRootLogReader::open(&root).unwrap();
    let opened = reader.open_relative(Path::new("logs/train.log")).unwrap();
    let outside = temp.path().join("outside.log");
    std::fs::write(&outside, b"outside").unwrap();
    std::fs::rename(&path, root.join("logs/train.log.old")).unwrap();
    std::os::unix::fs::symlink(&outside, &path).unwrap();
    assert_eq!(opened.snapshot(1_048_576).unwrap().evidence, "original");
}
```

Add an integration test that swaps `logs/escape.log` to a symlink before each `Detector::inspect_task` call and asserts a bounded `log_path_unsafe`/`agent_log_unsafe` error, never outside-file evidence.

Traversal assertions must be typed, for example:

```rust
assert!(matches!(reader.open_relative(Path::new("logs/../outside.log")),
    Err(AppError::PolicyViolation { violation:
        PolicyViolation { code: PolicyViolationCode::LogUnsafe,
            detail: PolicyViolationDetail::LogUnsafe(LogUnsafeReason::ParentTraversal), .. }
    })));
assert!(matches!(reader.open_relative(Path::new("logs/escape.log")),
    Err(AppError::PolicyViolation { violation:
        PolicyViolation { code: PolicyViolationCode::LogUnsafe,
            detail: PolicyViolationDetail::LogUnsafe(LogUnsafeReason::Symlink), .. }
    })));
```

- [ ] **Step 2: Run RED.** Run `cargo test --lib project_logs::tests::descriptor_relative_snapshot_survives_path_symlink_swap` and `cargo test --test detection extra_logs_are_project_relative_and_reject_path_traversal_after_canonicalization`. Expected failure: module/interfaces are absent and current Detector canonicalizes then reopens the symlink target.

- [ ] **Step 3: Extend the Task 2 descriptor walker for read-only project logs.** Reuse the pinned root and private component walker; do not add a second root-opening implementation. Add `open_relative`, opening the final component with `O_RDONLY|O_NOFOLLOW|O_CLOEXEC`, fstat and require a regular file, and wrap every descriptor in `File` exactly once. `OpenedProjectLog::snapshot` delegates to `LogSnapshot::read_tail_from_file`. Delete `canonical_extra_log_path`; do not call `canonicalize` for a file that will later be opened.

Update `read_task_snapshot` to try the two relative names and ignore only `NotFound`; all unsafe type/symlink/traversal errors propagate. Update extra-log observations to retain the configured relative path for fingerprints/source display. Rework existing tests that put Pueue logs beside the project so task logs live at `project/.pueue-agent/logs`.

Minimal read path:

```rust
let root = ProjectRootLogReader::open(&self.project_root)?;
let opened = root.open_relative(&self.task_log_relative_dir.join(format!("{task_id}.log")))?;
let snapshot = opened.snapshot(tail_bytes)?; // fstat + seek + read on opened.file()
```

Root pinning shape:

```rust
let canonical = root.canonicalize()?;
let expected = LogFileIdentity::from_path_metadata(&std::fs::metadata(&canonical)?)?;
let descriptor = open_directory_no_follow(root)?;
let actual = LogFileIdentity::from_open_descriptor(&descriptor)?;
if actual != expected { return Err(root_changed_policy_violation(root)); }
```

- [ ] **Step 4: Run GREEN.** Run `cargo test --lib project_logs`, `cargo test --test detection`, and `cargo test --test daemon`; expected result: PASS, including source paths remaining relative and the symlink-swap test reading the already-open original descriptor.

- [ ] **Step 5: Commit.**

```bash
git add src/project_logs.rs src/lib.rs src/detect.rs src/daemon.rs tests/integration/detection.rs
git commit -m "fix: read project logs relative to an open root"
```

### Task 4: Add read-only diagnostics, regression fixtures, and final verification

**Files:**
- Modify: `src/diagnostics.rs:475-575` (doctor checks)
- Modify: `tests/integration/diagnostics.rs`
- Modify: `tests/test_shell_entrypoints.bats` only if the launcher output contract changes (otherwise do not touch it)

**Interfaces:**
- Consumes `MAX_LOG_TAIL_BYTES`, `inspect_existing_agent_log`, and
  `ProjectRootLogReader` safety errors; diagnostics never call the
  create-capable `open_agent_log`.
- Produces `diagnostics::inspect_known_agent_logs(&Db, &Project) -> Result<LogSafetySummary, AppError>`, which queries only bounded, project-scoped `agent_runs.log_path` values and calls `inspect_existing_agent_log` (never `open_agent_log`).
- Produces read-only doctor checks named `logs.cap` and `logs.agent_safety`; summaries contain only bounded project-relative path/code facts, never file contents, credentials, prompts, or raw OS paths outside the project.

- [ ] **Step 1: Write the failing diagnostic tests.** Add:

```rust
#[test]
fn doctor_reports_bounded_log_cap_and_agent_log_safety_without_reading_content() {
    let harness = DiagnosticsHarness::new();
    let log_dir = harness.project().root_path.join(".pueue-agent/logs");
    std::fs::create_dir_all(&log_dir).unwrap();
    let run_log = log_dir.join("agent-100-17.log");
    std::fs::write(&run_log, b"SECRET_PROMPT_MUST_NOT_APPEAR").unwrap();
    let event_id = EventRepository::new(&harness.db).insert_idempotent(&NewEvent::new(
        "project-a", EventKind::TaskFinished, "known-log", serde_json::json!({}), 100, 100,
    )).unwrap().event_id;
    AgentRunRepository::new(&harness.db).insert(&NewAgentRun::new(
        "project-a", event_id, None, AgentRunStatus::Completed, 100, &run_log,
    )).unwrap();
    let report = build_doctor_report(&harness.db, &harness.project(), &doctor_paths(&harness),
        doctor_external(), 100).unwrap();
    let rendered = render_doctor_report_value(&report, true).unwrap();
    let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(state_check(&value, "logs.cap")["status"], "ok");
    assert!(!rendered.contains("SECRET_PROMPT_MUST_NOT_APPEAR"));
}
```

On Unix add a weak-mode/symlink fixture at a DB-referenced `agent_runs.log_path` and assert `logs.agent_safety.status == "error"` with exact log-unsafe detail aggregation, without file contents. Add a config fixture with `log_tail_bytes = 1048577` and assert `logs.cap` reports an error without repairing the file. Also assert an unreferenced unsafe file in `.pueue-agent/logs` is ignored: doctor must not scan arbitrary directory entries.

- [ ] **Step 2: Run RED.** Run `cargo test --test diagnostics doctor_reports_bounded_log_cap_and_agent_log_safety_without_reading_content`. Expected failure: the new check names are absent and doctor currently has no log-cap/safety projection.

- [ ] **Step 3: Implement minimal read-only checks.** After project-config validation, add `logs.cap`: valid configured values report the exact maximum; invalid values report a bounded error. Implement `inspect_known_agent_logs` with a project-scoped query such as `SELECT log_path FROM agent_runs WHERE project_id = ?1 ORDER BY run_id DESC LIMIT 128`; construct one `ProjectRootLogReader`, lexically `strip_prefix(project.root_path)` from each stored absolute path, reject paths outside the root, and pass only the resulting relative path to `inspect_existing_agent_log(&reader, relative)`. Classify `Missing` as warning and typed unsafe results as error, and aggregate at most bounded relative path/code samples. Never scan `.pueue-agent/logs`, create directories, chmod files, call `open_agent_log`, follow symlinks, retry reads, or expose absolute paths/content.

Minimal projection shape:

```rust
checks.push(if (1..=MAX_LOG_TAIL_BYTES).contains(&project_config.check.log_tail_bytes) {
    doctor_ok("logs.cap", "log tail cap is 1048576 bytes", "none")
} else {
    doctor_error("logs.cap", "check.log_tail_bytes is outside 1..=1048576", "fix config.toml")
});
checks.push(log_safety_check(inspect_known_agent_logs(db, project)?)?);
```

- [ ] **Step 4: Run GREEN and the host-independent suite.** Run the focused diagnostic test, then:

```bash
cargo fmt --all -- --check
cargo test --all-targets
git diff --check
bats tests/test_shell_entrypoints.bats
```

Expected result: all commands exit 0; the Bats suite remains unchanged because no shell entrypoint is used or modified by this slice. If the full suite exposes an old test relying on an external task-log directory, move only that fixture under the project root and keep the production descriptor contract unchanged.

- [ ] **Step 5: Commit.**

```bash
git add src/diagnostics.rs tests/integration/diagnostics.rs
git commit -m "test: add bounded safe log diagnostics"
```

- [ ] **Step 6: Final diff review.** Run `git diff --check` and `git status --short`; verify the diff contains only the files in this plan, no credentials/runtime state, no native-gate implementation, no Codex/Pueue/process-policy changes, and no canonicalize-then-open path remains in `src/detect.rs`.
