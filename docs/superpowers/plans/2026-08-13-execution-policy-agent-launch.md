# Execution Policy and Secure Agent Launch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the supervisor the sole owner of executable identity, Codex security, environment, writable roots, native launch authorization, policy failure classification, and bounded local projections while preserving event-run acknowledgement and intervention semantics.

**Architecture:** Add a service-owned `execution_policy` boundary that resolves one immutable global policy at daemon startup and one immutable project policy before intervention reservation. Codex argv, sanitized environments, private temp, and the native descriptor-bound launcher consume those snapshots; active handles retain them through finalization. SQLite v14 stores only bounded execution projections, while status/events/runs/doctor expose paths, codes, and stages without secrets.

**Tech Stack:** Rust 2021, Tokio, rusqlite immediate transactions, Serde/TOML, explicit `libc = "0.2"` for Unix syscalls, tempfile, assert_cmd, and Bats. Linux/macOS agent execution is supported; non-Unix execution fails closed.

## Global Constraints

- Resolve bare `codex` and `pueue` once from trusted service `PATH` to canonical absolute regular executable anchors; never dynamically re-resolve.
- Every PATH component, anchor, custom executable, Pueue config, policy file, and launcher is outside all registered project roots and not group/other writable.
- `<service-state-dir>/execution-policy.toml` is service-owned, regular, no-follow, owner-only (`0600` Unix), atomically written with temporary file + `fsync` + rename + directory `fsync`; weak existing files fail closed.
- Missing policy creates secure Codex+Pueue defaults: network enabled, no custom agents, no added environment names.
- `enable` and daemon startup call the same host-independent `load_or_create_policy` path before resolving anchors or constructing Pueue/AgentRunner; a missing file is created in the fixture state directory, then loaded and validated in that same startup transaction.
- The immutable global snapshot owns `project_roots` and a pinned `PueueConfigAnchor`; companion Pueue work consumes those fields rather than inventing a second policy source.
- `ResolvedExecutionPolicy`, `ResolvedProjectExecutionPolicy`, and active-run policy/temp are immutable snapshots; full policy/environment stays memory-only.
- Codex is fixed to `workspace-write` and `approval_policy=never`; project args cannot override security, sandbox, cwd, add-dir, or network; safe model/reasoning args remain supported.
- Network is enabled by default; projects may only disable it. Unsupported installed-CLI capability blocks dispatch.
- Every child starts `env_clear`; authentication names are hard-denied from task/custom-agent environments. No credentials, environment values, prompt, transcript, or raw output is persisted.
- Each run gets owner-only `0700` `.pueue-agent/tmp/<run-id>`; global `/tmp`, add-dir, and second roots are excluded.
- Unix launch is shell-free; failed `setsid` is fatal. Cleanup sends TERM, waits boundedly, then KILLs the recorded process group through the shared marker-free process interface.
- Native gate verifies the executable descriptor/log while the child is blocked/suspended, atomically creates the mode-0600 marker before release, releases the child, confirms exec via close-on-exec status pipe only after release, then writes `released\n`.
- Linux uses the verified executable FD with `execveat`/`fexecve`. On macOS the native launcher first becomes session/process-group leader with mandatory `setsid`, then uses `posix_spawn(POSIX_SPAWN_START_SUSPENDED)` for a target that inherits the launcher's group. It verifies the anchor immediately before spawning, re-opens/re-verifies after successful spawn before marker authorization, kills on mismatch, then creates the marker and sends `SIGCONT`. Same-UID swap-and-restore between those macOS checks is outside the threat model; custom enrollment is an explicit operator trust grant.
- Policy violations are `policy_blocked:<code>`, direct `dead_letter`, and never retry; transient pre-release OS failures retain `RetryPolicy`; post-marker uncertainty always dead-letters.
- Preserve claim -> `insert_with_events_and_reservation` -> `in_flight` -> marker/release -> `acknowledge_dispatch` -> `dispatched` -> finalizer -> `completed`/`retry_wait`/`dead_letter`; marker is not completion evidence.
- Pueue command hardening and descriptor-relative task/extra-log reads are companion plans. This plan consumes the log plan's `open_agent_log` contract and does not implement the log plan's `log_tail_bytes` cap.
- Tests use generated fixtures, not real Codex/Pueue, network, or host `/bin` layout. Every task is failing test -> RED -> minimal implementation -> GREEN -> commit.

## Mandatory cross-plan execution order

The three companion plans deliberately share `config`, `service`, `main`,
`daemon`, and `diagnostics`. Implement them sequentially in this order; do not
run workers that edit those files concurrently:

1. Core Task 1.
2. Log Tasks 1-2 (they consume the core error types and `libc`).
3. Core Tasks 2-6.
4. Core Task 9, then Core Tasks 7-8 (the lifecycle can now persist v14 projections).
5. Pueue Tasks 1-4.
6. Log Task 3.
7. Core Task 10, Pueue Task 5, then Log Task 4.
8. Core Task 11 and Pueue Task 6 as one final regression/acceptance pass.

At every boundary, base the next task on the reviewed previous commit and run
that task's focused GREEN command. The final acceptance pass is not parallel.

## File map and public interfaces

- Create `src/execution_policy.rs`, `src/codex_command.rs`, `src/environment.rs`, `src/process.rs`, `src/native_launcher.rs`.
- `src/process.rs` owns the marker-free `ProcessGroupRequirement`, `VerifiedCommandSpec`, `VerifiedChild`, `spawn_verified_command`, and `terminate_process_group` contract; `src/native_launcher.rs` is the agent-specific marker/release layer built on it.
- Modify `src/config.rs`, `src/codex_session.rs`, `src/error.rs`, `src/retry.rs`, `src/agent.rs`, `src/scheduler.rs`, `src/daemon.rs`, `src/main.rs`, `src/service.rs`, `src/models.rs`, `src/db/migrations.rs`, `src/db/repositories.rs`, `src/status.rs`, `src/diagnostics.rs`, `src/runs.rs`, `src/lib.rs`.
- Test in `tests/integration/{execution_policy,codex_security,native_launcher,database,scheduler,daemon,service,diagnostics}.rs`, existing fixtures, and `tests/test_shell_entrypoints.bats`.

The exact cross-task contract is:

```rust
pub struct ExecutableIdentity { pub device: u64, pub inode: u64, pub owner: u32, pub mode: u32 }
pub struct ExecutableAnchor {
    pub canonical_path: PathBuf, pub identity: ExecutableIdentity,
    pub resolution_fingerprint: String,
}
pub struct PueueConfigAnchor {
    pub canonical_path: PathBuf, pub identity: ExecutableIdentity,
    pub resolution_fingerprint: String,
}
pub struct VerifiedPueueConfig { pub file: File, pub anchor: PueueConfigAnchor }
pub struct ProjectRootAnchor { pub canonical_path: PathBuf, pub identity: ExecutableIdentity }
pub struct VerifiedProjectRoot { pub directory: File, pub anchor: ProjectRootAnchor }
impl VerifiedProjectRoot { pub fn try_clone(&self) -> Result<Self, AppError>; }
impl ProjectRootAnchor {
    pub fn verify_identity(&self) -> Result<VerifiedProjectRoot, PolicyViolation>;
}
impl PueueConfigAnchor {
    pub fn verify_identity(&self, roots: &[PathBuf]) -> Result<VerifiedPueueConfig, AppError>;
}
impl ExecutableAnchor {
    pub fn resolve(program: &OsStr, trusted_path: &[PathBuf], roots: &[PathBuf]) -> Result<Self, PolicyViolation>;
    pub fn from_absolute(path: &Path, roots: &[PathBuf]) -> Result<Self, PolicyViolation>;
    pub fn verify_identity(&self) -> Result<VerifiedExecutable, PolicyViolation>;
}
pub enum NetworkMode { Enabled, Disabled }
pub enum AgentKind { BuiltInCodex, Custom }
pub struct ResolvedExecutionPolicy {
    pub codex_anchor: ExecutableAnchor, pub pueue_anchor: ExecutableAnchor,
    pub launcher_anchor: ExecutableAnchor, pub trusted_path: Vec<PathBuf>,
    pub project_roots: Vec<PathBuf>, pub pueue_config_anchor: PueueConfigAnchor,
    pub startup_environment: StartupEnvironment, pub codex_home: PathBuf,
    pub default_network: NetworkMode,
    pub custom_allowlist: BTreeMap<String, ExecutableAnchor>,
}
pub struct ResolvedProjectExecutionPolicy {
    pub project_id: String, pub root_anchor: ProjectRootAnchor,
    pub agent_anchor: ExecutableAnchor, pub agent_kind: AgentKind, pub network: NetworkMode,
    pub agent_environment_allow: BTreeSet<String>, pub task_environment_allow: BTreeSet<String>,
    pub codex_home: PathBuf, pub private_temp_relative_root: PathBuf,
}
pub enum PolicyViolationCode {
    PolicyMissing, PolicyUnreadable, PolicyWeakPermissions, PolicyUnknownField,
    TrustedPathUnsafe, AnchorMissing, AnchorReplaced, CustomAgentNotEnrolled,
    ProjectRootExecutable, UnsafeCodexArgument, NetworkOverride, EnvironmentName,
    SessionMissing, SessionNotOwned, RootChanged, LogUnsafe, TempUnsafe,
    SetSidFailed, NativeGateFailed, UnsupportedPlatform,
}
impl PolicyViolationCode {
    pub const fn as_str(&self) -> &'static str; // LogUnsafe => "agent_log_unsafe"
}
pub enum PolicyViolationStage { Startup, PreBinding, RunBoundPreMarker, NativeGate, PostMarker, Dispatched, Finalized }
pub enum PolicyViolationDetail { None, LogUnsafe(LogUnsafeReason) }
pub enum LogUnsafeReason { Missing, Symlink, Directory, Device, WeakPermissions, WrongOwner, InvalidContents, EmptyPath, CurDir, ParentTraversal, AbsolutePath, RootChanged }
pub struct PolicyViolation {
    pub code: PolicyViolationCode, pub stage: PolicyViolationStage,
    pub detail: PolicyViolationDetail,
}
impl PolicyViolation {
    pub fn new(code: PolicyViolationCode, stage: PolicyViolationStage) -> Self;
    pub fn with_detail(code: PolicyViolationCode, stage: PolicyViolationStage, detail: PolicyViolationDetail) -> Self;
}
// src/error.rs
// AppError::PolicyViolation { violation: PolicyViolation }
// impl From<PolicyViolation> for AppError
pub enum ProcessGroupRequirement { Required, NotRequired }
pub enum VerifiedChildIo {
    Capture,
    AgentLog { stdout: File, stderr: File, identity: LogFileIdentity },
}
pub struct VerifiedCommandSpec {
    pub launcher: ExecutableAnchor, pub executable: ExecutableAnchor,
    pub argv: Vec<OsString>, pub cwd: Option<PathBuf>,
    pub environment: SanitizedEnvironment, pub process_group: ProcessGroupRequirement,
    pub start_suspended: bool, pub project_root: Option<VerifiedProjectRoot>,
    pub pueue_config: Option<VerifiedPueueConfig>, pub child_io: VerifiedChildIo,
}
pub struct VerifiedChild {
    pub child: tokio::process::Child, pub pid: i64, pub process_group_id: Option<i64>,
    pub start_gate: StartGate, pub exec_status: ExecStatusReceiver, pub ack: AckReceiver,
}
pub struct StartGate { /* private release pipe; no marker state */ }
impl VerifiedChild {
    pub fn release(&mut self) -> Result<(), AppError>;
    pub async fn confirm_exec(&mut self) -> Result<(), AppError>;
    pub async fn wait_for_release_ack(&mut self) -> Result<(), AppError>;
    pub fn take_stdout(&mut self) -> Result<tokio::process::ChildStdout, AppError>;
    pub fn take_stderr(&mut self) -> Result<tokio::process::ChildStderr, AppError>;
}
pub fn spawn_verified_command(spec: VerifiedCommandSpec) -> Result<VerifiedChild, AppError>;
pub async fn terminate_process_group(child: &mut VerifiedChild);
pub fn load_or_create_policy(input: &PolicyLoadInput) -> Result<ResolvedExecutionPolicy, PolicyViolation>;
pub fn load_existing_policy(input: &PolicyLoadInput) -> Result<ResolvedExecutionPolicy, PolicyViolation>;
pub fn resolve_project_policy(g: &ResolvedExecutionPolicy, p: &Project, c: &ProjectConfig) -> Result<ResolvedProjectExecutionPolicy, PolicyViolation>;
```

`PolicyViolation` has no arbitrary string/path detail. Its optional typed detail
is used only for local control flow; display and SQLite reason are only
`policy_blocked:<code>`.

The hidden helper protocol is part of the public cross-plan contract. The only
helper argv element is its hidden subcommand. Fixed child descriptors are
control=3, release=4, exec-status=5, target=6, project-root=7, agent-log=8,
Pueue-config=9, and release-ack=10; unused descriptors are closed. Control uses
magic `PAEX`, version 1, an agent/Pueue mode tag, flags, and a big-endian u32
length followed by at most 1 MiB of length-prefixed binary fields. Enforce at
most 256 argv entries, 128 environment names, and 64 KiB per byte/string field.
Reject duplicate environment names, unknown mode/flags/fields, trailing bytes,
NUL, inconsistent optional descriptors, and expected device/inode/owner/mode
mismatches before target creation. `Capture` pipes target stdout/stderr through
the helper; `AgentLog` duplicates only the verified log descriptors. The ack FD
accepts only the fixed `released\n` record. Credentials can appear only in the
anonymous payload and sanitized target environment, never helper argv, SQLite,
or diagnostic text.

### Task 1: Service policy file, trusted PATH, and executable anchors

**Files:** Create `src/execution_policy.rs`; modify `src/lib.rs`, `src/error.rs`, `Cargo.toml` (`libc = "0.2"`); test `tests/integration/execution_policy.rs`.

**Interfaces:** `PolicyLoadInput { state_dir, project_roots, inherited_path, startup_environment, codex_home, pueue_config, launcher_path }`; `VerifiedExecutable { file: File, anchor: ExecutableAnchor }`; `ExecutableAnchor::verify_identity` opens no-follow and compares device/inode/owner/mode/fingerprint. `ProjectRootAnchor::verify_identity` does the equivalent directory check and returns an open descriptor. `PueueConfigAnchor` is resolved and pinned from the same input/global policy. `load_existing_policy` is strictly non-mutating; only `load_or_create_policy` may securely create a missing file.

- [ ] **Step 1: Write RED tests.** Include the daemon-startup interaction, using only fixture paths:

```rust
#[test] fn missing_policy_is_atomic_secure_default() { let h=PolicyHarness::new(); let p=load_or_create_policy(&h.input()).unwrap(); assert_eq!(p.default_network,NetworkMode::Enabled); assert!(p.custom_allowlist.is_empty()); assert_eq!(fs::metadata(h.policy()).unwrap().permissions().mode()&0o077,0); }
#[test] fn trusted_path_rejects_project_or_weak_component() { let h=PolicyHarness::new(); let mut i=h.input(); i.inherited_path=h.project_root.join("bin").into_os_string(); assert!(matches!(load_or_create_policy(&i),Err(PolicyViolation{code:PolicyViolationCode::TrustedPathUnsafe,..}))); }
#[test] fn replacement_fails_closed_without_reresolution() { let h=PolicyHarness::new(); let p=load_or_create_policy(&h.input()).unwrap(); let a=p.codex_anchor; fs::rename(&a.canonical_path,h.path("old")).unwrap(); fs::write(&a.canonical_path,"new").unwrap(); assert!(matches!(a.verify_identity(),Err(PolicyViolation{code:PolicyViolationCode::AnchorReplaced,..}))); }
#[test] fn daemon_startup_creates_missing_policy_before_anchor_resolution() { let h=PolicyHarness::new(); assert!(!h.policy().exists()); let p=load_or_create_policy(&h.input()).unwrap(); assert!(h.policy().is_file()); assert!(p.pueue_config_anchor.canonical_path.is_absolute()); }
#[test] fn existing_loader_never_creates_missing_policy() { let h=PolicyHarness::new(); assert!(matches!(load_existing_policy(&h.input()),Err(PolicyViolation{code:PolicyViolationCode::PolicyMissing,..}))); assert!(!h.policy().exists()); }
```

- [ ] **Step 2: Run RED:** `cargo test --test execution_policy missing_policy_is_atomic_secure_default`; expected: compile failure for missing module/types.
- [ ] **Step 3: Implement minimum:** add `libc = "0.2"` to `[dependencies]`; add the exact `AppError::PolicyViolation` variant/conversion and `PolicyViolation::{new,with_detail}` constructors; parse `version`, `trusted_path`, `[defaults] network`, `[executables] codex/pueue`, and `[projects.<id>] custom_agent/agent_environment_allow/task_environment_allow` with `deny_unknown_fields`; reject weak policy/dirs; canonicalize absolute trusted dirs outside `project_roots`; resolve built-ins, `ProjectRootAnchor`s, and `PueueConfigAnchor` once. Share validation between the non-mutating existing loader and the create-capable loader; atomically create missing policy with owner-only mode before resolving any anchor only in the latter.
- [ ] **Step 4: Run GREEN:** `cargo test --test execution_policy`; expected: PASS for schema, atomic creation, ownership/mode, PATH exclusion, custom-anchor validation, and replacement detection.
- [ ] **Step 5: Commit:** `git add src/lib.rs src/error.rs src/execution_policy.rs Cargo.toml tests/integration/execution_policy.rs && git commit -m "feat: add immutable execution policy anchors"`.

### Task 2: Project config narrowing and immutable project policy

**Files:** Modify `src/config.rs`, `src/execution_policy.rs`; test `tests/integration/config.rs`, `tests/integration/execution_policy.rs`.

**Interfaces:** Add `AgentExecutionConfig { network: NetworkMode }` and `AgentCodexConfig { model: Option<String>, reasoning_effort: Option<CodexReasoningEffort> }`; `resolve_project_policy` uses built-in Codex, exact enrolled custom agent, canonical root identity, and `network = global disabled || project disabled`; no project environment allowlist. Do not implement `check.log_tail_bytes` bounds here; the companion log plan owns that parser/cap contract.

- [ ] **Step 1: Write RED tests.**

```rust
#[test] fn network_defaults_enabled_and_only_narrows() { let c=load_config(valid_config()).unwrap(); assert_eq!(c.agent.execution.network,NetworkMode::Enabled); let d=load_config(valid_config().replace("[check]","[agent.execution]\nnetwork=\"disabled\"\n\n[agent.codex]\nreasoning_effort=\"high\"\n\n[check]")).unwrap(); assert_eq!(d.agent.execution.network,NetworkMode::Disabled); assert_eq!(d.agent.codex.reasoning_effort,Some(CodexReasoningEffort::High)); assert!(load_config(valid_config().replace("[check]","[agent.execution]\nnetwork=\"unsafe\"\n\n[check]")).is_err()); }
#[test] fn custom_agent_requires_service_enrollment_and_is_outside_root() { let h=PolicyHarness::new(); let g=load_or_create_policy(&h.input()).unwrap(); let c=config_for(&h.project("a"),"/opt/custom"); assert!(matches!(resolve_project_policy(&g,&h.project("a"),&c),Err(PolicyViolation{code:PolicyViolationCode::CustomAgentNotEnrolled,..}))); }
```

- [ ] **Step 2: Run RED:** `cargo test --test config network_defaults_enabled_and_only_narrows`; expected: unknown `[agent.execution]`/missing field.
- [ ] **Step 3: Implement minimum:** parse only `enabled|disabled`; parse optional Codex model and `low|medium|high|xhigh` reasoning effort only when `program="codex"`; custom `agent.program` is blocked unless service policy maps project ID to canonical executable; reject bare/relative/root paths. Leave `check.log_tail_bytes` validation to the companion log plan while preserving the existing parser behavior here.
- [ ] **Step 4: Run GREEN:** `cargo test --test config --test execution_policy`; expected: PASS with legacy config compatibility and narrowing-only resolution.
- [ ] **Step 5: Commit:** `git add src/config.rs src/execution_policy.rs tests/integration/config.rs tests/integration/execution_policy.rs && git commit -m "feat: resolve project execution policy"`.

### Task 3: Codex safe argv and owned explicit `resume_latest`

**Files:** Create `src/codex_command.rs`; modify `src/codex_session.rs`, `src/agent.rs`, `src/lib.rs`; test `tests/integration/codex_security.rs` and session unit tests.

**Interfaces:** `CodexCapabilities { workspace_write, approval_never, network_mode, project_config_isolation }`; `CodexArgvBuilder::new(policy, capabilities)` and `build(&AgentConfig, prompt, private_tmp) -> Result<Vec<OsString>, PolicyViolation>`; `resolve_latest_owned_session(codex_home, project_root) -> Result<String, PolicyViolation>`.

- [ ] **Step 1: Write RED tests.**

```rust
#[test] fn forbidden_codex_security_args_fail_but_structured_model_reasoning_survive() { let h=CodexHarness::new(); let a=h.builder().build(&config_with_codex("model-a",CodexReasoningEffort::High),"literal ; $(touch pwned)",&h.private_tmp).unwrap(); assert!(a.windows(2).any(|x|x==["--sandbox".into(),"workspace-write".into()])); assert!(a.windows(2).any(|x|x==["--ask-for-approval".into(),"never".into()])); assert!(a.iter().any(|x|x=="model_reasoning_effort=\"high\"")); for x in ["-c","--config=x=y","--profile=x","--dangerously-bypass-approvals-and-sandbox","--dangerously-bypass-hook-trust","--sandbox=danger-full-access","--add-dir=/tmp","-C","--cwd=/other","--network-access=enabled"] { assert!(matches!(h.builder().build(&config_with_args(["exec",x,"{prompt}"]),"p",&h.private_tmp),Err(PolicyViolation{code:PolicyViolationCode::UnsafeCodexArgument,..}))); } }
#[test] fn latest_chooses_verified_same_project_and_never_last_or_fresh() { let h=CodexHarness::new(); h.write_session("old",&h.root,10); h.write_session("new",&h.root,20); h.write_session("foreign",&h.other,30); let id=resolve_latest_owned_session(&h.home,&h.root).unwrap(); assert_eq!(id,h.id("new")); let a=h.builder().build(&config_resume_latest(),"p",&h.private_tmp).unwrap(); assert!(!a.iter().any(|x|x=="--last")); assert!(a.iter().any(|x|x==&id)); }
```

- [ ] **Step 2: Run RED:** `cargo test --test codex_security forbidden_codex_security_args_fail_but_model_reasoning_survive`; expected: missing builder/selector.
- [ ] **Step 3: Implement minimum:** treat existing `args=["exec","{prompt}"]` as a compatibility envelope: require exactly one `{prompt}`, permit only an optional leading `exec`, and never copy project args verbatim. Construct the installed CLI's final argv concretely as `codex --ask-for-approval never exec --ignore-user-config --ignore-rules --strict-config --sandbox workspace-write -C <canonical-root>`, followed by supervisor-generated `-c` values for `sandbox_workspace_write.network_access`, `sandbox_workspace_write.exclude_slash_tmp=true`, `sandbox_workspace_write.exclude_tmpdir_env_var=true`, `sandbox_workspace_write.writable_roots=[<private-run-tmp>]`, `projects={<canonical-root>={trust_level="untrusted"}}`, `allow_login_shell=false`, and `shell_environment_policy={inherit="all",ignore_default_excludes=false,experimental_use_profile=false,filters={<resolved-name>="include",...}}`. Auth names are never placed in `filters`. Convert structured model/reasoning fields to `--model` and `-c model_reasoning_effort="<enum>"`. The capability probe must verify every forced option/config key on the installed CLI. Reject extra `exec`, missing/duplicate prompt placeholders, `-c`/`--config`, profile, ignore/bypass, hook, sandbox, add-dir, cwd, approval/security, and network flags. Scan both session stores, verify metadata ID/cwd/root, sort modified time/ID/path, reject duplicate ownership, never use `--last`/fresh fallback, and use untrusted project status plus ignored user config/rules to disable project `.codex`/hooks/MCP as alternate policy channels.
- [ ] **Step 4: Run GREEN:** `cargo test --test codex_security && cargo test --lib codex_session`; expected: PASS.
- [ ] **Step 5: Commit:** `git add src/codex_command.rs src/codex_session.rs src/agent.rs src/lib.rs tests/integration/codex_security.rs && git commit -m "feat: build policy-locked Codex commands"`.

### Task 4: Default-deny environment and private run temp

**Files:** Create `src/environment.rs`; modify `src/execution_policy.rs`, `src/agent.rs`, `src/lib.rs`; test Codex integration/fixtures.

**Interfaces:** `StartupEnvironment::capture`; `SanitizedEnvironment::{for_codex_agent,for_custom_agent,for_codex_task,for_pueue,apply}`; `PrivateRunTemp::create(&VerifiedProjectRoot,run_id)`; names only in debug/diagnostics. `for_codex_agent` includes only installation-required Codex auth names, while `for_codex_task`, `for_custom_agent`, and `for_pueue` hard-deny all auth names. `for_pueue` consumes the pinned `ResolvedExecutionPolicy.pueue_anchor`, trusted PATH, and noncredential startup names only; Pueue command bounds remain in the companion Pueue plan.

- [ ] **Step 1: Write RED test.**

```rust
#[test] fn task_env_is_default_deny_and_auth_never_inherits() { let h=EnvironmentHarness::new(); let s=h.startup([("OPENAI_API_KEY","secret"),("DATASET_ROOT","/data")]); let p=h.policy_with_task_allow(["DATASET_ROOT","OPENAI_API_KEY"]); let e=SanitizedEnvironment::for_codex_task(&s,&p,41).unwrap(); assert_eq!(e.get("DATASET_ROOT"),Some("/data")); assert_eq!(e.get("OPENAI_API_KEY"),None); assert!(!format!("{e:?}").contains("secret")); }
#[test] fn private_temp_is_0700_and_removed() { let h=EnvironmentHarness::new(); let t=PrivateRunTemp::create(&h.root,41).unwrap(); assert_eq!(fs::metadata(t.path()).unwrap().permissions().mode()&0o777,0o700); let p=t.path().to_owned(); drop(t); assert!(!p.exists()); }
```

- [ ] **Step 2: Run RED:** `cargo test --test codex_security task_env_is_default_deny_and_auth_never_inherits`; expected: missing sanitizer/temp.
- [ ] **Step 3: Implement minimum:** `env_clear`; the built-in Codex agent gets HOME/CODEX_HOME/trusted PATH/C locale/TMPDIR/TMP/TEMP/non-secret IDs, explicitly admitted proxy/certificate names, and only the installation-required Codex auth names captured at startup. Its generated `shell_environment_policy.filters` contains only the smaller non-secret task baseline plus service-owned task names and never auth. Custom agents receive the non-secret baseline plus `agent_environment_allow`; `for_pueue` uses the non-secret default-deny baseline without Codex task names. Create `.pueue-agent/tmp/<run-id>` from a verified root with `mkdirat`/no-follow, validating fixed parents and owner-only `0700`; never use ambient `create_dir_all`. Cleanup walks only opened directory descriptors without following symlinks, applies explicit depth/entry bounds, unlinks known entries with `unlinkat`, and retains an over-limit tree for bounded diagnostics rather than recursively deleting an unresolved path. Debug/diagnostics expose names, never values.
- [ ] **Step 4: Run GREEN:** `cargo test --test codex_security task_env_is_default_deny_and_auth_never_inherits private_temp_is_0700_and_removed`; expected: PASS and no secret in fixture logs.
- [ ] **Step 5: Commit:** `git add src/environment.rs src/execution_policy.rs src/agent.rs src/lib.rs tests/integration/codex_security.rs tests/support/fake_agent.sh tests/support/fake_codex.sh && git commit -m "feat: enforce default-deny execution environments"`.

### Task 5: Policy violation classification and direct dead-letter

**Files:** Modify `src/error.rs`, `src/retry.rs`, `src/db/repositories.rs`, `src/scheduler.rs`, `src/agent.rs`; test database/scheduler integration.

**Interfaces:** `EventResolution::PolicyBlocked { code, stage }`; `AgentSpawnError.policy: Option<PolicyViolation>`; `EventRepository::dead_letter_claimed_without_run(project_id,event_ids,now,violation)`; policy-aware `fail_before_gate_release_with_policy`.

- [ ] **Step 1: Write RED test.**

```rust
#[test] fn policy_blocked_claim_dead_letters_without_retry_or_run() { let h=DatabaseHarness::claimed_events(2); let v=PolicyViolation::new(PolicyViolationCode::UnsafeCodexArgument,PolicyViolationStage::PreBinding); EventRepository::new(&h.db).dead_letter_claimed_without_run("project-a",&h.ids,200,&v).unwrap(); for e in h.events() { assert_eq!(e.status,EventStatus::DeadLetter); assert_eq!(e.last_error.as_deref(),Some("policy_blocked:unsafe_codex_argument")); } assert!(h.runs().is_empty()); }
```

- [ ] **Step 2: Run RED:** `cargo test --test database policy_blocked_claim_dead_letters_without_retry_or_run`; expected: missing resolution/method.
- [ ] **Step 3: Implement minimum:** direct method clears leases and writes only bounded `policy_blocked:<code>`; policy-bound runs dead-letter linked `in_flight` regardless attempts, reset Reserved+Applied before marker, retain Applied after marker; scheduler releases pre-binding reservation exactly once and preserves UpgradeInProgress defer semantics. `AppError::PolicyViolation` is classification data, not an automatic database transition: only the scheduler/agent event boundary maps it to direct dead-letter; Pueue/operator callers return the bounded error without mutating events.
- [ ] **Step 4: Run GREEN:** `cargo test --test database policy_blocked_claim_dead_letters_without_retry_or_run && cargo test --test scheduler`; expected: PASS with grouped attempts/intervention semantics unchanged.
- [ ] **Step 5: Commit:** `git add src/error.rs src/retry.rs src/db/repositories.rs src/scheduler.rs src/agent.rs tests/integration/database.rs tests/integration/scheduler.rs && git commit -m "feat: direct-dead-letter policy violations"`.

### Task 6: Native descriptor-bound gate and process groups

**Files:** Create `src/process.rs`, `src/native_launcher.rs`, `tests/support/native_process_fixture.rs`; modify `src/main.rs`, `src/cli.rs`, `src/agent.rs`, `src/execution_policy.rs`, `src/error.rs`, `src/lib.rs`; test `tests/integration/native_launcher.rs`, Bats.

**Interfaces:** The shared marker-free process interface is `spawn_verified_command(VerifiedCommandSpec) -> Result<VerifiedChild, AppError>`, where `VerifiedCommandSpec` carries the anchored trusted supervisor launcher, anchored target, argv, optional cwd, sanitized environment, process-group requirement, verified optional project/config descriptors, and `VerifiedChildIo`. `VerifiedChild` carries the Tokio child for the native launcher, PID/process-group ID, `StartGate`, `ExecStatusReceiver`, and `AckReceiver`; capture callers take stdout/stderr exactly once, while agent callers cannot. It exposes `release`, `confirm_exec`, and `wait_for_release_ack`, and is cleaned up by `terminate_process_group(&mut VerifiedChild)`. Add a Clap-hidden `Command::InternalLaunch` dispatched before normal commands; it accepts no configurable paths or FD numbers and uses only the fixed inherited protocol. The parent sends the versioned bounded launch payload over the control pipe, so target argv/prompt and environment values are not placed in the launcher's argv. The native agent gate builds on that interface and adds `NativeLaunchSpec { launcher, executable, argv, cwd, environment, project_log_reader, relative_log_path, relative_marker_path }`, `NativeLauncher::spawn -> NativeAgentChild { verified: VerifiedChild }`, and `NativeAgentChild::authorize_marker()`. `ensure_agent_log_dir`, `open_agent_log`, `create_gate_marker`, and `inspect_gate_marker` are consumed from the companion log plan and must provide component-wise no-follow, owner-only descriptor-relative contracts. This task does not implement the log tail cap.

- [ ] **Step 1: Write RED test.**

```rust
#[cfg(unix)] #[tokio::test] async fn gate_creates_marker_before_release_and_confirms_exec_after_release() { let h=NativeLauncherHarness::generated_rust_fixture(); let mut c=NativeLauncher::spawn(h.spec()).unwrap(); assert!(!h.started().exists()); c.authorize_marker().unwrap(); assert!(h.marker().exists()); c.verified.release().unwrap(); c.verified.confirm_exec().await.unwrap(); c.verified.wait_for_release_ack().await.unwrap(); assert!(h.started().exists()); }
#[cfg(unix)] #[test] fn internal_protocol_rejects_oversize_unknown_duplicate_and_fd_confusion() { for malformed in ProtocolHarness::malformed_cases() { assert!(matches!(malformed.run_helper(),Err(AppError::PolicyViolation{..}))); assert!(!malformed.target_started()); } }
#[cfg(unix)] #[tokio::test] async fn replaced_anchor_and_shell_metacharacters_fail_closed() { let h=NativeLauncherHarness::new(); let s=h.spec(); fs::rename(&s.executable.canonical_path,h.path("old")).unwrap(); fs::write(&s.executable.canonical_path,"#!/bin/sh\ntouch PWNED\n").unwrap(); assert!(matches!(NativeLauncher::spawn(s),Err(PolicyViolation{code:PolicyViolationCode::AnchorReplaced,..}))); assert!(!h.path("PWNED").exists()); }
#[cfg(target_os="macos")] #[tokio::test] async fn post_spawn_anchor_swap_is_killed_before_marker() { let h=NativeLauncherHarness::swap_after_posix_spawn(); let c=h.launch().unwrap(); assert!(c.child.try_wait().unwrap().is_some()); assert!(!h.marker().exists()); }
```

- [ ] **Step 2: Run RED:** `cargo test --test native_launcher gate_creates_marker_before_release_and_confirms_exec_after_release`; expected: missing native types.
- [ ] **Step 3: Implement minimum:** Add explicit `libc = "0.2"` and the hidden native-launch command. Implement the fixed FD and binary framing contract above with checked length arithmetic, one-time ownership of all FDs, close-on-exec everywhere except explicitly inherited target resources, and fail-closed parsing before target creation. `spawn_verified_command` directly starts only the verified supervisor launcher using Tokio `Command`, configures capture pipes or cloned agent-log descriptors, sends the bounded payload, and never exposes target argv/prompt in launcher argv. The launcher fstats and compares every expected descriptor identity, calls mandatory `setsid` before target creation when required, and closes unused mode-specific FDs. On Linux it forks a blocked target and uses `execveat`/`fexecve` on inherited target FD 6, never a path or PATH lookup. On macOS it verifies the anchor immediately before `posix_spawn(POSIX_SPAWN_START_SUSPENDED)`; the target inherits the launcher's process group, the launcher reopens/reverifies after spawn, and kills/reaps on mismatch before marker authorization. Same-UID swap-and-restore between macOS checks is outside the threat model; custom enrollment is an explicit trust grant. `NativeLauncher` verifies `ProjectRootAnchor`, constructs `ProjectRootLogReader` from that open root, calls companion `ensure_agent_log_dir` and `open_agent_log`, and passes clones via `VerifiedChildIo::AgentLog`; it uses `create_gate_marker` through the pinned root so marker creation is exclusive, owner-only, file- and directory-synced before `release`. It reads exec status and the exact ack only after release. Pipe closure before marker prevents target execution; marker creation/fork/exec/ack failures classify by stage. Tests compile a small generated Rust fixture executable into their temporary directory and never use a shell script or host executable. Non-Unix returns `UnsupportedPlatform`.
- [ ] **Step 4: Run GREEN:** `cargo test --test native_launcher && bats tests/test_shell_entrypoints.bats`; expected: PASS for no-shell, marker/release, revalidation, secure log, setsid and cleanup.
- [ ] **Step 5: Commit:** `git add Cargo.toml Cargo.lock src/process.rs src/native_launcher.rs src/main.rs src/cli.rs src/agent.rs src/execution_policy.rs src/error.rs src/lib.rs tests/integration/native_launcher.rs tests/support/native_process_fixture.rs tests/test_shell_entrypoints.bats && git commit -m "feat: add descriptor-bound native launch gate"`.

### Task 7: Agent lifecycle integration and preserved event ack

**Files:** Modify `src/agent.rs`, `src/scheduler.rs`, `src/daemon.rs`; test scheduler/daemon integration and fake agent.

**Interfaces:** `AgentRunner::new(config, Arc<ResolvedExecutionPolicy>)`; `AgentRunner::spawn(..., &ResolvedProjectExecutionPolicy, ...)`; `AgentHandle` retains policy/temp/process-group/execution projection; existing `poll`, `wait`, `timeout_now` retain `&mut self` and retry finalizer errors. Production log paths are always `.pueue-agent/logs/<bounded-run-name>` relative to the verified project root; test overrides must supply a fixture `ProjectRootAnchor` plus relative log name, never an unrestricted absolute directory.

- [ ] **Step 1: Write RED tests.**

```rust
#[tokio::test] async fn spawn_is_dispatched_but_not_completed_until_exit() { let h=SchedulerHarness::sleeping_agent(); let mut r=h.tick().await.unwrap(); assert_eq!(h.event().status,EventStatus::Dispatched); r.started.pop().unwrap().handle.wait(&h.db,200).await.unwrap(); assert_eq!(h.event().status,EventStatus::Completed); }
#[tokio::test] async fn unsafe_arg_dead_letters_without_retry() { let h=SchedulerHarness::unsafe_arg("--danger-full-access"); let _=h.tick().await; assert_eq!(h.event().status,EventStatus::DeadLetter); assert_eq!(h.event().attempts,1); }
```

- [ ] **Step 2: Run RED:** `cargo test --test scheduler spawn_is_dispatched_but_not_completed_until_exit`; expected: shell/immediate path does not consume policy/native state.
- [ ] **Step 3: Implement minimum:** remove the current `create_dir_all`/absolute `OpenOptions`/shell-gate path from `AgentRunner`. After binding, verify the retained `ProjectRootAnchor`, construct one `ProjectRootLogReader` from its open descriptor, derive only the bounded relative log/marker names, ensure/open them with the log-plan APIs, and pass the resulting descriptors into `NativeLaunchSpec`; neither the runner nor recovery reopens `log_path` or marker by ambient path. Resolve Codex/custom command, temp, and env; call `mark_running_and_apply_interventions`, `mark_gate_release_requested`, `authorize_marker` (durable marker exists), `release` (child may execute), `confirm_exec`, exact `wait_for_release_ack`, then `acknowledge_dispatch`. Retain terminal child outcome when finalizer fails and retry the same handle; cleanup process group TERM/KILL and temp only after terminal persistence.
- [ ] **Step 4: Run GREEN:** `cargo test --test scheduler --test daemon`; expected: PASS for two-stage ack, transient pre-marker retry, policy dead-letter, post-marker uncertainty, intervention retention, finalizer retry, shutdown handle retention.
- [ ] **Step 5: Commit:** `git add src/agent.rs src/scheduler.rs src/daemon.rs tests/integration/scheduler.rs tests/integration/daemon.rs tests/support/fake_agent.sh && git commit -m "feat: connect immutable policy to agent lifecycle"`.

### Task 8: Startup service wiring and project-before-intervention resolution

**Files:** Modify `src/service.rs`, `src/main.rs`, `src/daemon.rs`, `src/scheduler.rs`, systemd/launchd assets; test service/daemon/scheduler integration.

**Interfaces:** `ServicePaths` gains `codex_home`, `execution_policy`, `startup_environment`; `Daemon::new(..., Arc<ResolvedExecutionPolicy>, ...)`; scheduler resolves project policy before reservation; definitions include canonical HOME/CODEX_HOME/PATH/state/policy/config and no credentials. `CommandPueue` receives `ResolvedExecutionPolicy.pueue_anchor` and `pueue_config_anchor` through the companion plan; it must not resolve a bare executable or invent a config anchor. `enable` uses existing registered roots plus the candidate canonical root before any registration/side effect; daemon startup uses all registered roots. Other CLI/doctor/status paths use only `load_existing_policy` and remain non-mutating.

- [ ] **Step 1: Write RED test.**

```rust
#[test] fn definitions_pin_policy_and_codex_paths_without_secrets() { let p=service_paths_fixture(); for platform in [ServicePlatform::Systemd,ServicePlatform::Launchd] { let s=ServiceDefinition::for_platform(platform,&p).render(); assert!(s.contains(p.execution_policy.to_str().unwrap())); assert!(s.contains(p.codex_home.to_str().unwrap())); assert!(!s.contains("OPENAI_API_KEY")); } }
```

- [ ] **Step 2: Run RED:** `cargo test --test service definitions_pin_policy_and_codex_paths_without_secrets`; expected: missing path fields/rendering.
- [ ] **Step 3: Implement minimum:** `enable` canonicalizes the candidate, unions it with the registered-project query, atomically ensures/loads policy and validates all anchors before inserting the project, provisioning Pueue, or changing callbacks/services. Daemon loads global policy before Pueue/runner/Daemon and passes all registered roots; scheduler resolves project policy before intervention reservation. Read-only/operator commands call `load_existing_policy` and never create/repair the policy. Render only fixed paths/names, never env map or credential values. Pueue receives the pinned `PueueConfigAnchor` and `SanitizedEnvironment::for_pueue` snapshot.
- [ ] **Step 4: Run GREEN:** `cargo test --test service --test daemon --test scheduler`; expected: PASS and custom policy blocks release pending interventions correctly.
- [ ] **Step 5: Commit:** `git add src/service.rs src/main.rs src/daemon.rs src/scheduler.rs assets/systemd/pueue-agent.service assets/launchd/com.pueue-agent.plist tests/integration/service.rs tests/integration/daemon.rs && git commit -m "feat: load immutable policy at service startup"`.

### Task 9: Schema v14 execution projection

**Files:** Modify `src/db/migrations.rs`, `src/models.rs`, `src/db/repositories.rs`; test `tests/integration/database.rs`.

**Interfaces:** v14 adds nullable `execution_kind`, `executable_path`, `executable_identity`, `policy_code`, `failure_stage`; `ExecutionProjection { execution_kind, executable_path, executable_identity }`; `NewAgentRun::with_execution(ExecutionProjection)` writes it in the existing binding transaction while the legacy constructor supplies `None` for fixture/migration compatibility. `AgentRun`, the shared select/row parser, `RunLineage`, `RunSummary`, status/runs serializers, and all insert/finalizer queries carry the nullable fields explicitly. Pre-binding policy block creates no run.

- [ ] **Step 1: Write RED test.**

```rust
#[test] fn v14_adds_projection_and_preserves_v13_rows() { let h=DatabaseHarness::schema_v13_with_run(); let db=Db::open(&h.path).unwrap(); assert_eq!(db.user_version().unwrap(),14); for n in ["execution_kind","executable_path","executable_identity","policy_code","failure_stage"] { assert!(db.column_names("agent_runs").unwrap().contains(&n.to_owned())); } }
```

- [ ] **Step 2: Run RED:** `cargo test --test database v14_adds_projection_and_preserves_v13_rows`; expected: latest schema remains 13/columns absent.
- [ ] **Step 3: Implement minimum:** transactional ALTERs + `PRAGMA user_version=14`; update the central `AGENT_RUN_SELECT`, every positional row parser, insert column/value lists, lineage/summary queries, and finalizers. Preserve the old constructor as a projection-free compatibility wrapper and add `with_execution`; the same binding transaction writes bounded path/identity/kind, and policy finalization updates only code/stage. Add compile-time/round-trip tests for both constructors and each query projection; no env/argv/prompt/credential columns.
- [ ] **Step 4: Run GREEN:** `cargo test --test database`; expected: PASS for v13 migration compatibility, ack transactions, policy dead-letter, and non-secret projections.
- [ ] **Step 5: Commit:** `git add src/db/migrations.rs src/models.rs src/db/repositories.rs tests/integration/database.rs && git commit -m "feat: persist schema v14 execution projections"`.

### Task 10: Status/events/runs/doctor bounded projections

**Files:** Modify `src/status.rs`, `src/diagnostics.rs`, `src/runs.rs`, project-scoped repository queries; test diagnostics/database.

**Interfaces:** summaries add project/root, execution kind/path, policy code, failure stage; `EventRepository::policy_blocked_counts(project_id)` and bounded execution queries require project predicates; doctor checks `execution.policy`, `.anchors`, `.project_root`, `.policy_blocked`, `.log_contract`, `.ack_consistency`, read-only only.

- [ ] **Step 1: Write RED test.**

```rust
#[test] fn projections_show_code_stage_path_without_secrets() { let h=DiagnosticsHarness::policy_blocked(); for s in [h.events_json(),h.runs_json(),h.status_json(),h.doctor_json()] { assert!(s.contains("unsafe_codex_argument")); assert!(s.contains("pre_binding")); assert!(!s.contains("OPENAI_API_KEY")); assert!(!s.contains("prompt")); } }
```

- [ ] **Step 2: Run RED:** `cargo test --test diagnostics projections_show_code_stage_path_without_secrets`; expected: fields/checks absent.
- [ ] **Step 3: Implement minimum:** extend existing serializers with bounded fields; deterministic project-scoped joins; expose no payload/prompt/transcript/raw output; doctor checks policy ownership/schema/default, anchor replacement, root, policy counts, secure agent log, ack consistency without repair/retry/enroll.
- [ ] **Step 4: Run GREEN:** `cargo test --test diagnostics --test database`; expected: PASS, including foreign-project exclusion and no doctor mutation.
- [ ] **Step 5: Commit:** `git add src/status.rs src/diagnostics.rs src/runs.rs src/db/repositories.rs tests/integration/diagnostics.rs tests/integration/database.rs && git commit -m "feat: expose bounded execution diagnostics"`.

### Task 11: Fixtures, migration/recovery regressions, and final verification

**Files:** Modify fake fixtures, `tests/test_shell_entrypoints.bats`, and integration tests; only correct `src/daemon.rs`, `src/db/repositories.rs`, or `src/agent.rs` when a regression test proves it.

**Interfaces:** recovery uses persisted marker/gate/stage: it verifies the project root anchor, lexically strips that canonical root from the persisted `log_path`, requires the fixed `.pueue-agent/logs/<bounded-run-name>` grammar, derives the relative marker name, and calls `inspect_gate_marker` on the verified root. Absent plus pre-release state -> retry policy; a valid marker/release/dispatch uncertainty -> execution-unknown dead-letter; an unsafe/indeterminate marker -> conservative dead-letter; policy -> direct dead-letter. Recovery never calls `fs::metadata` on an ambient marker path. Shutdown retries the same retained handle. Grouped events/interventions retain existing release/applied rules.

- [ ] **Step 1: Write RED regressions.**

```rust
#[tokio::test] async fn recovery_retries_pre_marker_and_dead_letters_post_marker() { let h=RecoveryHarness::two_runs(); let r=h.recover().unwrap(); assert_eq!(r.requeued_events,1); assert_eq!(r.dead_lettered_events,1); assert_eq!(h.event("post").status,EventStatus::DeadLetter); assert_eq!(h.event("pre").status,EventStatus::RetryWait); assert_eq!(h.applied("post").status,InterventionStatus::Applied); }
```

- [ ] **Step 2: Run RED:** `cargo test --test daemon recovery_retries_pre_marker_and_dead_letters_post_marker`; expected: any stale recovery/projection/handle behavior fails.
- [ ] **Step 3: Implement fixture/integration corrections:** fake scripts record literal argv and selected names only, support fail-before-marker/sleep/exit; Bats proves metacharacters stay literal, native launcher is sole gate, unsafe options reject before fake Codex, auth is absent, caps hold; never alter companion Pueue/log scope.
- [ ] **Step 4: Run focused GREEN:**

```bash
cargo test --test execution_policy --test codex_security --test native_launcher --test database --test scheduler --test daemon --test service --test diagnostics
bats tests/test_shell_entrypoints.bats
```

Expected: all exit 0; no real Codex/Pueue/network, credentials, shell gate, or changed event/intervention ack semantics.
- [ ] **Step 5: Run acceptance GREEN:** `cargo fmt --all -- --check`; `cargo test --all-targets`; `git diff --check`; `bats tests/test_shell_entrypoints.bats`. Expected: all exit 0.
- [ ] **Step 6: Commit verified integration:** `git add src tests Cargo.toml assets/systemd/pueue-agent.service assets/launchd/com.pueue-agent.plist && git commit -m "test: verify secure execution policy integration"`.

## Self-review

Tasks 1-2 cover policy file/schema/ownership, trusted PATH, identities, root/weak-dir exclusion, immutable global/project resolution, pinned Pueue config, custom enrollment, and network narrowing; log-tail caps remain with the companion log plan. Tasks 3-4 cover concrete Codex CLI security/config isolation, safe args, owned `resume_latest`, default-deny/auth-hard-deny environments, private temp, and no credential persistence. Tasks 5-7 cover `PolicyViolation` code/stage, direct dead-letter/no retry, shared marker-free process interface, Linux FD-bound/macOS suspended launch, marker-before-release/status-after-release handshake, mandatory setsid, cleanup, and preserved two-stage ack/interventions. Task 8 covers service definitions/startup and Pueue policy consumption. Task 9 covers schema v14. Task 10 covers status/events/runs/doctor. Task 11 covers fixtures, recovery, regressions, and required final commands. Pueue command hardening and descriptor-relative non-agent logs are explicitly companion work.

Plan complete and saved to `docs/superpowers/plans/2026-08-13-execution-policy-agent-launch.md`. Two execution options:

1. Subagent-Driven (recommended) - dispatch a fresh subagent per task, reviewing between tasks.
2. Inline Execution - execute tasks in this session using executing-plans with checkpoints.
