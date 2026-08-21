//! Service-owned execution policy and immutable startup trust anchors.
//!
//! This module deliberately keeps policy failures bounded.  A policy error can
//! be rendered and persisted as `policy_blocked:<code>` without carrying a
//! path, environment value, command line, or other attacker-controlled text.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{OsStr, OsString},
    fmt,
    fs::{self, File, Metadata},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
    sync::{atomic::{AtomicU64, Ordering}, Arc},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};

use serde::Deserialize;

use crate::{config::ProjectConfig, models::Project, paths, AppError};

const POLICY_FILENAME: &str = "execution-policy.toml";
const POLICY_VERSION: u32 = 1;
const MAX_STATE_DIRECTORY_CREATIONS: usize = 4;
const DEFAULT_POLICY: &str = r#"version = 1

[defaults]
network = "enabled"

[campaign]
max_parallel_experiments = 1
max_new_experiments_per_24h = 24
max_agent_runs_per_hour = 6
max_code_change_proposals_per_24h = 10
max_same_spec_retries = 2
max_repairs_per_failure_fingerprint = 2
max_proposals_per_cycle = 1
observer_interval_minutes = 30
max_decision_attempts_per_cycle = 3
max_decision_wait_minutes = 1440

[executables]
codex = "codex"
pueue = "pueue"
"#;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
static POLICY_PUBLISH_HOOK: std::sync::Mutex<Option<fn(&Path)>> = std::sync::Mutex::new(None);
#[cfg(test)]
static POLICY_OPEN_HOOK: std::sync::Mutex<Option<fn(&Path)>> = std::sync::Mutex::new(None);
#[cfg(test)]
static POLICY_TEMPORARY_HOOK: std::sync::Mutex<Option<fn(&OsStr)>> = std::sync::Mutex::new(None);

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PolicyPublicationFailure {
    Write,
    Sync,
    Link,
}

#[cfg(test)]
static POLICY_PUBLICATION_FAILURE: std::sync::Mutex<Option<PolicyPublicationFailure>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
fn invoke_policy_publish_hook(state_dir: &Path) {
    if let Some(hook) = *POLICY_PUBLISH_HOOK.lock().unwrap() {
        hook(state_dir);
    }
}

#[cfg(test)]
fn invoke_policy_open_hook(state_dir: &Path) {
    if let Some(hook) = *POLICY_OPEN_HOOK.lock().unwrap() {
        hook(state_dir);
    }
}

#[cfg(test)]
fn invoke_policy_temporary_hook(temporary: &OsStr) {
    if let Some(hook) = *POLICY_TEMPORARY_HOOK.lock().unwrap() {
        hook(temporary);
    }
}

#[cfg(test)]
fn take_policy_publication_failure(stage: PolicyPublicationFailure) -> bool {
    let mut failure = POLICY_PUBLICATION_FAILURE.lock().unwrap();
    if *failure == Some(stage) {
        *failure = None;
        true
    } else {
        false
    }
}

/// Startup environment values are held in memory only.  The custom Debug and
/// Display implementations expose names/counts, never values.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct StartupEnvironment {
    // Keep the OS spelling of both names and values.  Environment names are
    // normally UTF-8, but a non-UTF-8 name must never be silently rewritten.
    values: BTreeMap<OsString, OsString>,
}

impl StartupEnvironment {
    pub fn capture() -> Self {
        Self {
            values: std::env::vars_os().collect(),
        }
    }

    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<OsString>,
        V: Into<OsString>,
    {
        Self {
            values: pairs
                .into_iter()
                .map(|(name, value)| (name.into(), value.into()))
                .collect(),
        }
    }

    pub fn names(&self) -> impl Iterator<Item = &OsStr> {
        self.values.keys().map(OsString::as_os_str)
    }

    pub fn get(&self, name: &str) -> Option<&OsStr> {
        self.values.get(OsStr::new(name)).map(OsString::as_os_str)
    }

    #[allow(dead_code)]
    pub(crate) fn values(&self) -> &BTreeMap<OsString, OsString> {
        &self.values
    }
}

impl fmt::Debug for StartupEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StartupEnvironment")
            .field("names", &self.values.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl fmt::Display for StartupEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "startup environment ({} names)", self.values.len())
    }
}

#[derive(Clone, Debug)]
pub struct PolicyLoadInput {
    pub state_dir: PathBuf,
    pub project_roots: Vec<PathBuf>,
    pub inherited_path: OsString,
    pub startup_environment: StartupEnvironment,
    pub codex_home: PathBuf,
    pub pueue_config: PathBuf,
    pub launcher_path: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutableIdentity {
    pub device: u64,
    pub inode: u64,
    pub owner: u32,
    pub mode: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutableAnchor {
    pub canonical_path: PathBuf,
    pub identity: ExecutableIdentity,
    pub resolution_fingerprint: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PueueConfigAnchor {
    pub canonical_path: PathBuf,
    pub identity: ExecutableIdentity,
    pub resolution_fingerprint: String,
}

pub struct VerifiedExecutable {
    pub file: File,
    pub anchor: ExecutableAnchor,
}

pub struct VerifiedPueueConfig {
    pub file: File,
    pub anchor: PueueConfigAnchor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectRootAnchor {
    pub canonical_path: PathBuf,
    pub identity: ExecutableIdentity,
    pub resolution_fingerprint: String,
}

pub struct VerifiedProjectRoot {
    pub directory: File,
    pub anchor: ProjectRootAnchor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkMode {
    Enabled,
    Disabled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CampaignLimits {
    pub max_parallel_experiments: u32,
    pub max_new_experiments_per_24h: u32,
    pub max_agent_runs_per_hour: u32,
    pub max_code_change_proposals_per_24h: u32,
    pub max_same_spec_retries: u32,
    pub max_repairs_per_failure_fingerprint: u32,
    pub max_proposals_per_cycle: u32,
    pub observer_interval_minutes: u32,
    pub max_decision_attempts_per_cycle: u32,
    pub max_decision_wait_minutes: u32,
}

impl Default for CampaignLimits {
    fn default() -> Self {
        Self {
            max_parallel_experiments: 1,
            max_new_experiments_per_24h: 24,
            max_agent_runs_per_hour: 6,
            max_code_change_proposals_per_24h: 10,
            max_same_spec_retries: 2,
            max_repairs_per_failure_fingerprint: 2,
            max_proposals_per_cycle: 1,
            observer_interval_minutes: 30,
            max_decision_attempts_per_cycle: 3,
            max_decision_wait_minutes: 1_440,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentKind {
    BuiltInCodex,
    Custom,
}

#[derive(Clone)]
pub struct ResolvedExecutionPolicy {
    pub codex_anchor: ExecutableAnchor,
    pub pueue_anchor: ExecutableAnchor,
    pub launcher_anchor: ExecutableAnchor,
    pub trusted_path: Vec<PathBuf>,
    pub project_roots: Vec<PathBuf>,
    project_root_anchors: Vec<ProjectRootAnchor>,
    pub pueue_config_anchor: PueueConfigAnchor,
    pub startup_environment: StartupEnvironment,
    pub codex_home: PathBuf,
    pub default_network: NetworkMode,
    pub campaign_limits: CampaignLimits,
    pub custom_allowlist: BTreeMap<String, ExecutableAnchor>,
    project_environment_allow: BTreeMap<String, (BTreeSet<String>, BTreeSet<String>)>,
    #[allow(dead_code)]
    trusted_path_descriptors: Vec<Arc<File>>,
}

impl fmt::Debug for ResolvedExecutionPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedExecutionPolicy")
            .field("codex_anchor", &self.codex_anchor)
            .field("pueue_anchor", &self.pueue_anchor)
            .field("launcher_anchor", &self.launcher_anchor)
            .field("trusted_path", &self.trusted_path)
            .field("project_roots", &self.project_roots)
            .field("pueue_config_anchor", &self.pueue_config_anchor)
            .field("startup_environment", &self.startup_environment)
            .field("codex_home", &self.codex_home)
            .field("default_network", &self.default_network)
            .field("campaign_limits", &self.campaign_limits)
            .field("custom_allowlist", &self.custom_allowlist)
            .finish()
    }
}

impl ResolvedExecutionPolicy {
    pub fn project_root_anchor(
        &self,
        root: &Path,
    ) -> Result<ProjectRootAnchor, PolicyViolation> {
        self.project_root_anchors
            .iter()
            .find(|anchor| anchor.canonical_path == root)
            .cloned()
            .ok_or_else(|| {
                PolicyViolation::new(
                    PolicyViolationCode::RootChanged,
                    PolicyViolationStage::PreBinding,
                )
            })
    }
}

#[derive(Clone, Debug)]
pub struct ResolvedProjectExecutionPolicy {
    pub project_id: String,
    pub root_anchor: ProjectRootAnchor,
    pub agent_anchor: ExecutableAnchor,
    pub agent_kind: AgentKind,
    pub network: NetworkMode,
    pub agent_environment_allow: BTreeSet<String>,
    pub task_environment_allow: BTreeSet<String>,
    pub codex_home: PathBuf,
    pub trusted_path: Vec<PathBuf>,
    pub private_temp_relative_root: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyViolationCode {
    PolicyMissing,
    PolicyUnreadable,
    PolicyWeakPermissions,
    PolicyUnknownField,
    TrustedPathUnsafe,
    AnchorMissing,
    AnchorReplaced,
    CustomAgentNotEnrolled,
    ProjectRootExecutable,
    UnsafeCodexArgument,
    NetworkOverride,
    EnvironmentName,
    SessionMissing,
    SessionNotOwned,
    RootChanged,
    LogUnsafe,
    TempUnsafe,
    SetSidFailed,
    NativeGateFailed,
    UnsupportedPlatform,
}

impl PolicyViolationCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PolicyMissing => "policy_missing",
            Self::PolicyUnreadable => "policy_unreadable",
            Self::PolicyWeakPermissions => "policy_weak_permissions",
            Self::PolicyUnknownField => "policy_unknown_field",
            Self::TrustedPathUnsafe => "trusted_path_unsafe",
            Self::AnchorMissing => "anchor_missing",
            Self::AnchorReplaced => "anchor_replaced",
            Self::CustomAgentNotEnrolled => "custom_agent_not_enrolled",
            Self::ProjectRootExecutable => "project_root_executable",
            Self::UnsafeCodexArgument => "unsafe_codex_argument",
            Self::NetworkOverride => "network_override",
            Self::EnvironmentName => "environment_name",
            Self::SessionMissing => "session_missing",
            Self::SessionNotOwned => "session_not_owned",
            Self::RootChanged => "root_changed",
            Self::LogUnsafe => "agent_log_unsafe",
            Self::TempUnsafe => "temp_unsafe",
            Self::SetSidFailed => "setsid_failed",
            Self::NativeGateFailed => "native_gate_failed",
            Self::UnsupportedPlatform => "unsupported_platform",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyViolationStage {
    Startup,
    PreBinding,
    RunBoundPreMarker,
    NativeGate,
    PostMarker,
    Dispatched,
    Finalized,
}

impl PolicyViolationStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::PreBinding => "pre_binding",
            Self::RunBoundPreMarker => "run_bound_pre_marker",
            Self::NativeGate => "native_gate",
            Self::PostMarker => "post_marker",
            Self::Dispatched => "dispatched",
            Self::Finalized => "finalized",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyViolationDetail {
    None,
    LogUnsafe(LogUnsafeReason),
    TempUnsafe(TempUnsafeReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TempUnsafeReason {
    DepthLimit,
    EntryLimit,
    ByteLimit,
    GenerationLimit,
    IdentityChanged,
    MountBoundary,
    InvalidEntry,
    IoFailure,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogUnsafeReason {
    Missing,
    Symlink,
    Directory,
    Device,
    WeakPermissions,
    WrongOwner,
    InvalidContents,
    EmptyPath,
    CurDir,
    ParentTraversal,
    AbsolutePath,
    RootChanged,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PolicyViolation {
    pub code: PolicyViolationCode,
    pub stage: PolicyViolationStage,
    pub detail: PolicyViolationDetail,
}

impl PolicyViolation {
    pub const fn new(code: PolicyViolationCode, stage: PolicyViolationStage) -> Self {
        Self {
            code,
            stage,
            detail: PolicyViolationDetail::None,
        }
    }

    pub const fn with_detail(
        code: PolicyViolationCode,
        stage: PolicyViolationStage,
        detail: PolicyViolationDetail,
    ) -> Self {
        Self {
            code,
            stage,
            detail,
        }
    }
}

impl fmt::Display for PolicyViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "policy_blocked:{}", self.code.as_str())
    }
}

impl std::error::Error for PolicyViolation {}

#[allow(dead_code)]
const fn unsupported_platform() -> PolicyViolation {
    PolicyViolation::new(
        PolicyViolationCode::UnsupportedPlatform,
        PolicyViolationStage::Startup,
    )
}

pub fn preflight_decision_runtime() -> Result<(), PolicyViolation> {
    #[cfg(target_os = "linux")]
    {
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::PreBinding,
        ))
    }
}

struct OpenedPath {
    canonical_path: PathBuf,
    file: File,
    resolution_fingerprint: String,
}

#[cfg(unix)]
fn open_path_nofollow(path: &Path) -> io::Result<OpenedPath> {
    use std::os::unix::ffi::OsStrExt;

    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "execution policy paths must be canonical absolute paths",
        ));
    }
    let root = std::ffi::CString::new("/").expect("literal contains no NUL");
    // SAFETY: root is a valid NUL-terminated path and the flags do not expose
    // a borrowed pointer after this call.
    let root_fd = unsafe {
        libc::open(
            root.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if root_fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: root_fd is freshly returned by open and is owned here.
    let mut directory = unsafe { File::from_raw_fd(root_fd) };
    let mut fingerprint = String::new();
    let components: Vec<_> = path.components().collect();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            if matches!(component, Component::RootDir) {
                continue;
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unsupported path component",
            ));
        };
        let bytes = name.as_bytes();
        if bytes.contains(&0) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "NUL path component"));
        }
        let name = std::ffi::CString::new(bytes).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "NUL path component")
        })?;
        let final_component = index + 1 == components.len();
        let flags = if final_component {
            libc::O_RDONLY
        } else {
            libc::O_RDONLY | libc::O_DIRECTORY
        } | libc::O_CLOEXEC
            | libc::O_NOFOLLOW;
        // SAFETY: directory is an owned directory descriptor and name points
        // to a NUL-terminated component for the duration of the call.
        let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fd is freshly returned by openat and is owned here.
        let opened = unsafe { File::from_raw_fd(fd) };
        let metadata = opened.metadata()?;
        if !secure_component_metadata(&metadata) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "weak or foreign path component",
            ));
        }
        let item = identity(&metadata);
        use std::fmt::Write as _;
        write!(
            &mut fingerprint,
            "{:?}:{:x}:{:x}:{:x}:{:x};",
            name, item.device, item.inode, item.owner, item.mode
        )
        .expect("writing to String cannot fail");
        if final_component {
            let canonical_path = fs::canonicalize(path)?;
            return Ok(OpenedPath {
                canonical_path,
                file: opened,
                resolution_fingerprint: fingerprint,
            });
        }
        directory = opened;
    }
    let canonical_path = fs::canonicalize(path)?;
    Ok(OpenedPath {
        canonical_path,
        file: directory,
        resolution_fingerprint: fingerprint,
    })
}

#[cfg(not(unix))]
fn open_path_nofollow(_path: &Path) -> io::Result<OpenedPath> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "execution policy requires Unix no-follow descriptors",
    ))
}

#[cfg(unix)]
fn openat_nofollow(directory: &File, name: &OsStr, flags: i32, mode: u32) -> io::Result<File> {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL path component"))?;
    // SAFETY: directory is an owned descriptor and name is NUL terminated.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            mode,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fd is freshly returned by openat and is owned here.
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(unix)]
fn open_child_path_nofollow(parent: &OpenedPath, name: &OsStr) -> io::Result<OpenedPath> {
    let file = openat_nofollow(&parent.file, name, libc::O_RDONLY, 0)?;
    let metadata = file.metadata()?;
    let canonical_path = fs::canonicalize(parent.canonical_path.join(name))?;
    let mut resolution_fingerprint = parent.resolution_fingerprint.clone();
    let item = identity(&metadata);
    use std::fmt::Write as _;
    write!(
        &mut resolution_fingerprint,
        "{:?}:{:x}:{:x}:{:x}:{:x};",
        name, item.device, item.inode, item.owner, item.mode
    )
    .expect("writing to String cannot fail");
    Ok(OpenedPath {
        canonical_path,
        file,
        resolution_fingerprint,
    })
}

#[cfg(unix)]
fn mkdirat(directory: &File, name: &OsStr, mode: u32) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL path component"))?;
    // SAFETY: directory is an owned descriptor and name is NUL terminated.
    let result = unsafe {
        libc::mkdirat(
            directory.as_raw_fd(),
            name.as_ptr(),
            mode as libc::mode_t,
        )
    };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn unlinkat(directory: &File, name: &OsStr) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL path component"))?;
    // SAFETY: directory is an owned descriptor and name is NUL terminated.
    let result = unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn linkat(directory: &File, source: &OsStr, destination: &OsStr) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let source = std::ffi::CString::new(source.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL path component"))?;
    let destination = std::ffi::CString::new(destination.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL path component"))?;
    // SAFETY: directory is an owned descriptor and both names are NUL terminated.
    let result = unsafe {
        libc::linkat(
            directory.as_raw_fd(),
            source.as_ptr(),
            directory.as_raw_fd(),
            destination.as_ptr(),
            0,
        )
    };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

impl ExecutableAnchor {
    pub fn resolve(
        program: &OsStr,
        trusted_path: &[PathBuf],
        roots: &[PathBuf],
    ) -> Result<Self, PolicyViolation> {
        #[cfg(not(unix))]
        {
            let _ = (program, trusted_path, roots);
            return Err(unsupported_platform());
        }
        #[cfg(unix)]
        {
        if program.is_empty() || program.to_string_lossy().contains('\0') {
            return Err(PolicyViolation::new(
                PolicyViolationCode::AnchorMissing,
                PolicyViolationStage::Startup,
            ));
        }
        let program_path = Path::new(program);
        if program_path.is_absolute() {
            return Self::from_absolute(program_path, roots);
        }
        if program_path.components().count() != 1
            || program_path
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::AnchorMissing,
                PolicyViolationStage::Startup,
            ));
        }

        for directory in trusted_path {
            let opened_directory = open_path_nofollow(directory).map_err(|_| {
                PolicyViolation::new(
                    PolicyViolationCode::TrustedPathUnsafe,
                    PolicyViolationStage::Startup,
                )
            })?;
            validate_opened_trusted_directory(&opened_directory, roots)?;
            match open_child_path_nofollow(&opened_directory, program_path.as_os_str()) {
                Ok(opened) => return Self::from_opened_executable(opened, roots),
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(_) => {
                    return Err(PolicyViolation::new(
                        PolicyViolationCode::AnchorMissing,
                        PolicyViolationStage::Startup,
                    ))
                }
            }
        }

        Err(PolicyViolation::new(
            PolicyViolationCode::AnchorMissing,
            PolicyViolationStage::Startup,
        ))
        }
    }

    pub fn from_absolute(path: &Path, roots: &[PathBuf]) -> Result<Self, PolicyViolation> {
        #[cfg(not(unix))]
        {
            let _ = (path, roots);
            return Err(unsupported_platform());
        }
        #[cfg(unix)]
        {
        let opened = open_path_nofollow(path).map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorMissing,
                PolicyViolationStage::Startup,
            )
        })?;
        Self::from_opened_executable(opened, roots)
        }
    }

    #[cfg(unix)]
    fn from_opened_executable(
        opened: OpenedPath,
        roots: &[PathBuf],
    ) -> Result<Self, PolicyViolation> {
        if inside_any_root(&opened.canonical_path, roots) {
            return Err(PolicyViolation::new(
                PolicyViolationCode::ProjectRootExecutable,
                PolicyViolationStage::Startup,
            ));
        }
        let metadata = opened.file.metadata().map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorMissing,
                PolicyViolationStage::Startup,
            )
        })?;
        if !metadata.is_file() {
            return Err(PolicyViolation::new(
                PolicyViolationCode::AnchorMissing,
                PolicyViolationStage::Startup,
            ));
        }
        if !secure_metadata(&metadata) || identity(&metadata).mode & 0o111 == 0 {
            return Err(PolicyViolation::new(
                PolicyViolationCode::TrustedPathUnsafe,
                PolicyViolationStage::Startup,
            ));
        }
        let identity = identity(&metadata);
        Ok(Self {
            canonical_path: opened.canonical_path,
            identity,
            resolution_fingerprint: opened.resolution_fingerprint,
        })
    }

    pub fn verify_identity(&self) -> Result<VerifiedExecutable, PolicyViolation> {
        #[cfg(not(unix))]
        {
            return Err(unsupported_platform());
        }
        #[cfg(unix)]
        {
        let opened = open_path_nofollow(&self.canonical_path).map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorReplaced,
                PolicyViolationStage::RunBoundPreMarker,
            )
        })?;
        let metadata = opened.file.metadata().map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorReplaced,
                PolicyViolationStage::RunBoundPreMarker,
            )
        })?;
        let current_identity = identity(&metadata);
        if opened.canonical_path != self.canonical_path
            || !metadata.is_file()
            || !secure_metadata(&metadata)
            || current_identity != self.identity
            || opened.resolution_fingerprint != self.resolution_fingerprint
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::AnchorReplaced,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        Ok(VerifiedExecutable {
            file: opened.file,
            anchor: self.clone(),
        })
        }
    }
}

impl ProjectRootAnchor {
    pub fn resolve(path: &Path) -> Result<Self, PolicyViolation> {
        #[cfg(not(unix))]
        {
            let _ = path;
            return Err(unsupported_platform());
        }
        #[cfg(unix)]
        {
        let opened = open_path_nofollow(path).map_err(|_| {
            PolicyViolation::new(PolicyViolationCode::RootChanged, PolicyViolationStage::Startup)
        })?;
        let metadata = opened.file.metadata().map_err(|_| {
            PolicyViolation::new(PolicyViolationCode::RootChanged, PolicyViolationStage::Startup)
        })?;
        if !metadata.is_dir() || !secure_metadata(&metadata) {
            return Err(PolicyViolation::new(
                PolicyViolationCode::RootChanged,
                PolicyViolationStage::Startup,
            ));
        }
        Ok(Self {
            canonical_path: opened.canonical_path,
            identity: identity(&metadata),
            resolution_fingerprint: opened.resolution_fingerprint,
        })
        }
    }

    pub fn verify_identity(&self) -> Result<VerifiedProjectRoot, PolicyViolation> {
        #[cfg(not(unix))]
        {
            return Err(unsupported_platform());
        }
        #[cfg(unix)]
        {
        let opened = open_path_nofollow(&self.canonical_path).map_err(|_| {
                PolicyViolation::new(
                    PolicyViolationCode::RootChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                )
            })?;
        let metadata = opened.file.metadata().map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::RootChanged,
                PolicyViolationStage::RunBoundPreMarker,
            )
        })?;
        if opened.canonical_path != self.canonical_path
            || !metadata.is_dir()
            || !secure_metadata(&metadata)
            || identity(&metadata) != self.identity
            || opened.resolution_fingerprint != self.resolution_fingerprint
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::RootChanged,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        Ok(VerifiedProjectRoot {
            directory: opened.file,
            anchor: self.clone(),
        })
        }
    }
}

impl VerifiedProjectRoot {
    pub fn try_clone(&self) -> Result<Self, AppError> {
        let directory = self.directory.try_clone().map_err(|source| AppError::Io {
            operation: "clone verified project root",
            source,
        })?;
        Ok(Self {
            directory,
            anchor: self.anchor.clone(),
        })
    }
}

impl PueueConfigAnchor {
    pub fn from_absolute(path: &Path, roots: &[PathBuf]) -> Result<Self, PolicyViolation> {
        #[cfg(not(unix))]
        {
            let _ = (path, roots);
            return Err(unsupported_platform());
        }
        #[cfg(unix)]
        {
        let opened = open_path_nofollow(path).map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorMissing,
                PolicyViolationStage::Startup,
            )
        })?;
        if opened.canonical_path != path {
            return Err(PolicyViolation::new(
                PolicyViolationCode::AnchorMissing,
                PolicyViolationStage::Startup,
            ));
        }
        if inside_any_root(&opened.canonical_path, roots) {
            return Err(PolicyViolation::new(
                PolicyViolationCode::TrustedPathUnsafe,
                PolicyViolationStage::Startup,
            ));
        }
        let metadata = opened.file.metadata().map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorMissing,
                PolicyViolationStage::Startup,
            )
        })?;
        if !metadata.is_file() || !secure_metadata(&metadata) {
            return Err(PolicyViolation::new(
                PolicyViolationCode::TrustedPathUnsafe,
                PolicyViolationStage::Startup,
            ));
        }
        let identity = identity(&metadata);
        Ok(Self {
            canonical_path: opened.canonical_path,
            identity,
            resolution_fingerprint: opened.resolution_fingerprint,
        })
        }
    }

    pub fn verify_identity(
        &self,
        roots: &[PathBuf],
    ) -> Result<VerifiedPueueConfig, AppError> {
        #[cfg(not(unix))]
        {
            let _ = roots;
            return Err(unsupported_platform().into());
        }
        #[cfg(unix)]
        {
        let opened = open_path_nofollow(&self.canonical_path).map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorReplaced,
                PolicyViolationStage::RunBoundPreMarker,
            )
        })?;
        if inside_any_root(&opened.canonical_path, roots) {
            return Err(PolicyViolation::new(
                PolicyViolationCode::TrustedPathUnsafe,
                PolicyViolationStage::RunBoundPreMarker,
            )
            .into());
        }
        let metadata = opened.file.metadata().map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorReplaced,
                PolicyViolationStage::RunBoundPreMarker,
            )
        })?;
        let current_identity = identity(&metadata);
        if opened.canonical_path != self.canonical_path
            || !metadata.is_file()
            || !secure_metadata(&metadata)
            || current_identity != self.identity
            || opened.resolution_fingerprint != self.resolution_fingerprint
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::AnchorReplaced,
                PolicyViolationStage::RunBoundPreMarker,
            )
            .into());
        }
        Ok(VerifiedPueueConfig {
            file: opened.file,
            anchor: self.clone(),
        })
        }
    }
}

/// Inspect a lexical Pueue configuration using the same no-follow, metadata,
/// and project-root checks as policy anchoring without creating policy state.
pub fn inspect_pueue_config_path(
    path: &Path,
    roots: &[PathBuf],
) -> Result<(), PolicyViolation> {
    PueueConfigAnchor::from_absolute(path, roots).map(|_| ())
}

pub fn load_or_create_policy(
    input: &PolicyLoadInput,
) -> Result<ResolvedExecutionPolicy, PolicyViolation> {
    #[cfg(not(unix))]
    {
        let _ = input;
        return Err(unsupported_platform());
    }
    #[cfg(unix)]
    {
        load_policy(input, true)
    }
}

pub fn load_existing_policy(
    input: &PolicyLoadInput,
) -> Result<ResolvedExecutionPolicy, PolicyViolation> {
    #[cfg(not(unix))]
    {
        let _ = input;
        return Err(unsupported_platform());
    }
    #[cfg(unix)]
    {
        load_policy(input, false)
    }
}

pub fn resolve_project_policy(
    global: &ResolvedExecutionPolicy,
    project: &Project,
    config: &ProjectConfig,
) -> Result<ResolvedProjectExecutionPolicy, PolicyViolation> {
    let configured_custom_path = if config.agent.program == "codex" {
        None
    } else {
        let configured_path = Path::new(&config.agent.program);
        if !configured_path.is_absolute()
            || configured_path
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::CustomAgentNotEnrolled,
                PolicyViolationStage::PreBinding,
            ));
        }
        if inside_any_root(configured_path, &global.project_roots)
            || configured_path.starts_with(&project.root_path)
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::ProjectRootExecutable,
                PolicyViolationStage::PreBinding,
            ));
        }
        Some(configured_path)
    };
    let root_anchor = global
        .project_root_anchors
        .iter()
        .find(|anchor| anchor.canonical_path == project.root_path)
        .cloned()
        .ok_or_else(|| {
            PolicyViolation::new(PolicyViolationCode::RootChanged, PolicyViolationStage::Startup)
        })?;
    let (agent_anchor, agent_kind) = match configured_custom_path {
        None => (global.codex_anchor.clone(), AgentKind::BuiltInCodex),
        Some(configured_path) => {
            let Some(anchor) = global.custom_allowlist.get(&project.project_id) else {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::CustomAgentNotEnrolled,
                    PolicyViolationStage::PreBinding,
                ));
            };
            if anchor.canonical_path != configured_path {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::CustomAgentNotEnrolled,
                    PolicyViolationStage::PreBinding,
                ));
            }
            (anchor.clone(), AgentKind::Custom)
        }
    };
    let (agent_environment_allow, task_environment_allow) = global
        .project_environment_allow
        .get(&project.project_id)
        .cloned()
        .unwrap_or_default();
    Ok(ResolvedProjectExecutionPolicy {
        project_id: project.project_id.clone(),
        root_anchor,
        agent_anchor,
        agent_kind,
        network: if matches!(global.default_network, NetworkMode::Disabled)
            || matches!(config.agent.execution.network, NetworkMode::Disabled)
        {
            NetworkMode::Disabled
        } else {
            NetworkMode::Enabled
        },
        agent_environment_allow,
        task_environment_allow,
        codex_home: global.codex_home.clone(),
        trusted_path: global.trusted_path.clone(),
        private_temp_relative_root: PathBuf::from(".pueue-agent/tmp"),
    })
}

pub(crate) fn resolve_decision_project_policy(
    global: &ResolvedExecutionPolicy,
    project: &ResolvedProjectExecutionPolicy,
) -> Result<ResolvedProjectExecutionPolicy, PolicyViolation> {
    preflight_decision_runtime()?;
    let root_anchor = global.project_root_anchor(&project.root_anchor.canonical_path)?;
    if root_anchor.identity != project.root_anchor.identity {
        return Err(PolicyViolation::new(
            PolicyViolationCode::RootChanged,
            PolicyViolationStage::PreBinding,
        ));
    }
    Ok(ResolvedProjectExecutionPolicy {
        project_id: project.project_id.clone(),
        root_anchor,
        agent_anchor: global.codex_anchor.clone(),
        agent_kind: AgentKind::BuiltInCodex,
        network: project.network,
        agent_environment_allow: BTreeSet::new(),
        task_environment_allow: BTreeSet::new(),
        codex_home: global.codex_home.clone(),
        trusted_path: global.trusted_path.clone(),
        private_temp_relative_root: PathBuf::from(".pueue-agent/tmp"),
    })
}

#[cfg(unix)]
fn load_policy(
    input: &PolicyLoadInput,
    create_missing: bool,
) -> Result<ResolvedExecutionPolicy, PolicyViolation> {
    let project_root_anchors = resolved_project_root_anchors(&input.project_roots)?;
    let project_roots = project_root_anchors
        .iter()
        .map(|anchor| anchor.canonical_path.clone())
        .collect::<Vec<_>>();
    let state_dir = open_service_directory(
        &input.state_dir,
        &project_roots,
        create_missing,
        &input.startup_environment,
    )?;
    let codex_home = validate_service_directory(&input.codex_home, &project_roots)?.canonical_path;
    let policy_name = OsStr::new(POLICY_FILENAME);
    let policy_file = match openat_nofollow(&state_dir.file, policy_name, libc::O_RDONLY, 0) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound && create_missing => {
            create_policy_file(&state_dir)?;
            openat_nofollow(&state_dir.file, policy_name, libc::O_RDONLY, 0).map_err(|_| {
                PolicyViolation::new(
                    PolicyViolationCode::PolicyUnreadable,
                    PolicyViolationStage::Startup,
                )
            })?
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(PolicyViolation::new(
                PolicyViolationCode::PolicyMissing,
                PolicyViolationStage::Startup,
            ));
        }
        Err(error) if error.raw_os_error() == Some(libc::ELOOP) => {
            return Err(PolicyViolation::new(
                PolicyViolationCode::PolicyWeakPermissions,
                PolicyViolationStage::Startup,
            ));
        }
        Err(_) => {
            return Err(PolicyViolation::new(
                PolicyViolationCode::PolicyUnreadable,
                PolicyViolationStage::Startup,
            ));
        }
    };
    #[cfg(test)]
    invoke_policy_open_hook(&state_dir.canonical_path);
    let policy_metadata = policy_file.metadata().map_err(|_| {
        PolicyViolation::new(
            PolicyViolationCode::PolicyUnreadable,
            PolicyViolationStage::Startup,
        )
    })?;
    if !policy_metadata.is_file() || !secure_metadata(&policy_metadata) || mode(&policy_metadata) & 0o077 != 0 {
        return Err(PolicyViolation::new(
            PolicyViolationCode::PolicyWeakPermissions,
            PolicyViolationStage::Startup,
        ));
    }
    let mut contents = String::new();
    (&policy_file)
        .take(1024 * 1024 + 1)
        .read_to_string(&mut contents)
        .map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::PolicyUnreadable,
                PolicyViolationStage::Startup,
            )
        })?;
    if contents.len() > 1024 * 1024 {
        return Err(PolicyViolation::new(
            PolicyViolationCode::PolicyUnreadable,
            PolicyViolationStage::Startup,
        ));
    }
    let raw: RawPolicy = toml::from_str(&contents).map_err(|_| {
        PolicyViolation::new(
            PolicyViolationCode::PolicyUnknownField,
            PolicyViolationStage::Startup,
        )
    })?;
    if raw.version != Some(POLICY_VERSION) {
        return Err(PolicyViolation::new(
            PolicyViolationCode::PolicyUnknownField,
            PolicyViolationStage::Startup,
        ));
    }

    let trusted_path_descriptors = parse_trusted_path(raw.trusted_path.as_deref(), &input.inherited_path)?;
    for directory in &trusted_path_descriptors {
        validate_opened_trusted_directory(directory, &project_roots)?;
    }
    let trusted_path: Vec<PathBuf> = trusted_path_descriptors
        .iter()
        .map(|directory| directory.canonical_path.clone())
        .collect();

    let codex_anchor = resolve_policy_executable(
        &raw.executables.codex,
        &trusted_path_descriptors,
        &trusted_path,
        &project_roots,
    )?;
    let pueue_anchor = resolve_policy_executable(
        &raw.executables.pueue,
        &trusted_path_descriptors,
        &trusted_path,
        &project_roots,
    )?;
    let launcher_anchor = ExecutableAnchor::from_absolute(&input.launcher_path, &project_roots)?;
    let pueue_config_anchor = PueueConfigAnchor::from_absolute(&input.pueue_config, &project_roots)?;
    let default_network = parse_network(raw.defaults.network.as_deref())?;
    let campaign_limits = parse_campaign_limits(raw.campaign)?;

    let mut custom_allowlist = BTreeMap::new();
    let mut project_environment_allow = BTreeMap::new();
    for (project_id, project) in raw.projects {
        let agent_environment_allow = parse_environment_names(project.agent_environment_allow)?;
        let task_environment_allow = parse_environment_names(project.task_environment_allow)?;
        if let Some(custom_agent) = project.custom_agent {
            let anchor = ExecutableAnchor::from_absolute(Path::new(&custom_agent), &project_roots)?;
            custom_allowlist.insert(project_id.clone(), anchor);
        }
        project_environment_allow.insert(
            project_id,
            (agent_environment_allow, task_environment_allow),
        );
    }

    Ok(ResolvedExecutionPolicy {
        codex_anchor,
        pueue_anchor,
        launcher_anchor,
        trusted_path,
        project_roots,
        project_root_anchors,
        pueue_config_anchor,
        startup_environment: input.startup_environment.clone(),
        codex_home,
        default_network,
        campaign_limits,
        custom_allowlist,
        project_environment_allow,
        trusted_path_descriptors: trusted_path_descriptors
            .into_iter()
            .map(|directory| Arc::new(directory.file))
            .collect(),
    })
}

#[cfg(unix)]
fn resolve_policy_executable(
    configured: &str,
    trusted_path_descriptors: &[OpenedPath],
    trusted_path: &[PathBuf],
    roots: &[PathBuf],
) -> Result<ExecutableAnchor, PolicyViolation> {
    if configured.is_empty() {
        return Err(PolicyViolation::new(
            PolicyViolationCode::AnchorMissing,
            PolicyViolationStage::Startup,
        ));
    }
    let configured_path = Path::new(configured);
    if configured_path.is_absolute() {
        return ExecutableAnchor::from_absolute(configured_path, roots);
    }
    if configured_path.components().count() != 1
        || configured_path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(PolicyViolation::new(
            PolicyViolationCode::AnchorMissing,
            PolicyViolationStage::Startup,
        ));
    }
    for (directory, _path) in trusted_path_descriptors.iter().zip(trusted_path) {
        match open_child_path_nofollow(directory, OsStr::new(configured)) {
            Ok(opened) => return ExecutableAnchor::from_opened_executable(opened, roots),
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::AnchorMissing,
                    PolicyViolationStage::Startup,
                ))
            }
        }
    }
    Err(PolicyViolation::new(
        PolicyViolationCode::AnchorMissing,
        PolicyViolationStage::Startup,
    ))
}

fn parse_trusted_path(
    configured: Option<&str>,
    inherited: &OsStr,
) -> Result<Vec<OpenedPath>, PolicyViolation> {
    let source = configured.map(OsString::from).unwrap_or_else(|| inherited.to_os_string());
    let paths: Vec<PathBuf> = std::env::split_paths(&source).collect();
    if paths.is_empty() || paths.iter().any(|path| path.as_os_str().is_empty()) {
        return Err(PolicyViolation::new(
            PolicyViolationCode::TrustedPathUnsafe,
            PolicyViolationStage::Startup,
        ));
    }
    paths
        .into_iter()
        .map(|path| {
            open_path_nofollow(&path)
                .map_err(|_| {
                    PolicyViolation::new(
                        PolicyViolationCode::TrustedPathUnsafe,
                        PolicyViolationStage::Startup,
                    )
                })
        })
        .collect()
}

fn parse_network(value: Option<&str>) -> Result<NetworkMode, PolicyViolation> {
    match value.unwrap_or("enabled") {
        "enabled" => Ok(NetworkMode::Enabled),
        "disabled" => Ok(NetworkMode::Disabled),
        _ => Err(PolicyViolation::new(
            PolicyViolationCode::PolicyUnknownField,
            PolicyViolationStage::Startup,
        )),
    }
}

fn parse_campaign_limits(raw: RawCampaignLimits) -> Result<CampaignLimits, PolicyViolation> {
    let defaults = CampaignLimits::default();
    let limits = CampaignLimits {
        max_parallel_experiments: raw
            .max_parallel_experiments
            .unwrap_or(defaults.max_parallel_experiments),
        max_new_experiments_per_24h: raw
            .max_new_experiments_per_24h
            .unwrap_or(defaults.max_new_experiments_per_24h),
        max_agent_runs_per_hour: raw
            .max_agent_runs_per_hour
            .unwrap_or(defaults.max_agent_runs_per_hour),
        max_code_change_proposals_per_24h: raw
            .max_code_change_proposals_per_24h
            .unwrap_or(defaults.max_code_change_proposals_per_24h),
        max_same_spec_retries: raw
            .max_same_spec_retries
            .unwrap_or(defaults.max_same_spec_retries),
        max_repairs_per_failure_fingerprint: raw
            .max_repairs_per_failure_fingerprint
            .unwrap_or(defaults.max_repairs_per_failure_fingerprint),
        max_proposals_per_cycle: raw
            .max_proposals_per_cycle
            .unwrap_or(defaults.max_proposals_per_cycle),
        observer_interval_minutes: raw
            .observer_interval_minutes
            .unwrap_or(defaults.observer_interval_minutes),
        max_decision_attempts_per_cycle: raw
            .max_decision_attempts_per_cycle
            .unwrap_or(defaults.max_decision_attempts_per_cycle),
        max_decision_wait_minutes: raw
            .max_decision_wait_minutes
            .unwrap_or(defaults.max_decision_wait_minutes),
    };
    if !(1..=64).contains(&limits.max_parallel_experiments)
        || !(1..=10_000).contains(&limits.max_new_experiments_per_24h)
        || !(1..=1_000).contains(&limits.max_agent_runs_per_hour)
        || limits.max_code_change_proposals_per_24h > 1_000
        || limits.max_same_spec_retries > 100
        || limits.max_repairs_per_failure_fingerprint > 100
        || !(1..=32).contains(&limits.max_proposals_per_cycle)
        || !(1..=1_440).contains(&limits.observer_interval_minutes)
        || !(1..=10).contains(&limits.max_decision_attempts_per_cycle)
        || !(1..=10_080).contains(&limits.max_decision_wait_minutes)
    {
        return Err(PolicyViolation::new(
            PolicyViolationCode::PolicyUnknownField,
            PolicyViolationStage::Startup,
        ));
    }
    Ok(limits)
}

fn parse_environment_names(
    names: Vec<String>,
) -> Result<BTreeSet<String>, PolicyViolation> {
    let mut parsed = BTreeSet::new();
    for name in names {
        let mut characters = name.bytes();
        let valid_first = matches!(characters.next(), Some(b'A'..=b'Z') | Some(b'_'));
        if !valid_first || characters.any(|byte| !matches!(byte, b'A'..=b'Z' | b'0'..=b'9' | b'_')) {
            return Err(PolicyViolation::new(
                PolicyViolationCode::EnvironmentName,
                PolicyViolationStage::Startup,
            ));
        }
        parsed.insert(name);
    }
    Ok(parsed)
}

fn resolved_project_root_anchors(
    roots: &[PathBuf],
) -> Result<Vec<ProjectRootAnchor>, PolicyViolation> {
    if roots.is_empty() {
        return Ok(Vec::new());
    }
    roots
        .iter()
        .map(|root| ProjectRootAnchor::resolve(root))
        .collect()
}

fn validate_service_directory(
    path: &Path,
    roots: &[PathBuf],
) -> Result<OpenedPath, PolicyViolation> {
    let opened = open_path_nofollow(path).map_err(|_| {
        PolicyViolation::new(
            PolicyViolationCode::PolicyUnreadable,
            PolicyViolationStage::Startup,
        )
    })?;
    validate_opened_service_directory(&opened, roots)?;
    Ok(opened)
}

#[cfg(unix)]
fn open_service_directory(
    path: &Path,
    roots: &[PathBuf],
    create_missing: bool,
    startup_environment: &StartupEnvironment,
) -> Result<OpenedPath, PolicyViolation> {
    match open_path_nofollow(path) {
        Ok(opened) => {
            validate_opened_service_directory(&opened, roots)?;
            Ok(opened)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound && create_missing => {
            create_service_directory(path, roots, startup_environment)
        }
        Err(_) => Err(PolicyViolation::new(
            PolicyViolationCode::PolicyUnreadable,
            PolicyViolationStage::Startup,
        )),
    }
}

#[cfg(not(unix))]
fn open_service_directory(
    _path: &Path,
    _roots: &[PathBuf],
    _create_missing: bool,
    _startup_environment: &StartupEnvironment,
) -> Result<OpenedPath, PolicyViolation> {
    Err(unsupported_platform())
}

#[cfg(unix)]
fn create_service_directory(
    path: &Path,
    roots: &[PathBuf],
    startup_environment: &StartupEnvironment,
) -> Result<OpenedPath, PolicyViolation> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(PolicyViolation::new(
            PolicyViolationCode::PolicyUnreadable,
            PolicyViolationStage::Startup,
        ));
    }
    let parent_path = path.parent().ok_or_else(|| {
        PolicyViolation::new(
            PolicyViolationCode::PolicyUnreadable,
            PolicyViolationStage::Startup,
        )
    })?;
    let name = path.file_name().ok_or_else(|| {
        PolicyViolation::new(
            PolicyViolationCode::PolicyUnreadable,
            PolicyViolationStage::Startup,
        )
    })?;
    match open_path_nofollow(parent_path) {
        Ok(parent) => {
            validate_opened_service_directory(&parent, roots)?;
            validate_configured_state_directory(path, roots, startup_environment)?;
            create_service_directory_component(&parent, name, roots)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            create_configured_service_directory_chain(
                path,
                roots,
                startup_environment,
            )
        }
        Err(_) => Err(PolicyViolation::new(
            PolicyViolationCode::PolicyUnreadable,
            PolicyViolationStage::Startup,
        )),
    }
}

#[cfg(unix)]
fn create_configured_service_directory_chain(
    path: &Path,
    roots: &[PathBuf],
    startup_environment: &StartupEnvironment,
) -> Result<OpenedPath, PolicyViolation> {
    validate_configured_state_directory(path, roots, startup_environment)?;

    let mut missing = Vec::new();
    let mut cursor = path;
    let ancestor = loop {
        match open_path_nofollow(cursor) {
            Ok(opened) => break opened,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let name = cursor.file_name().ok_or_else(policy_unreadable)?;
                missing.push(name.to_os_string());
                if missing.len() > MAX_STATE_DIRECTORY_CREATIONS {
                    return Err(policy_unreadable());
                }
                cursor = cursor.parent().ok_or_else(policy_unreadable)?;
            }
            Err(_) => return Err(policy_unreadable()),
        }
    };
    validate_opened_service_directory(&ancestor, roots)?;
    missing.reverse();

    let candidate_path = missing
        .iter()
        .fold(ancestor.canonical_path.clone(), |path, name| path.join(name));
    if inside_any_root(&candidate_path, roots) {
        return Err(PolicyViolation::new(
            PolicyViolationCode::TrustedPathUnsafe,
            PolicyViolationStage::Startup,
        ));
    }

    let mut opened = ancestor;
    for name in missing {
        opened = create_service_directory_component(&opened, &name, roots)?;
    }
    if opened.canonical_path != candidate_path {
        return Err(policy_unreadable());
    }
    validate_opened_service_directory(&opened, roots)?;
    Ok(opened)
}

#[cfg(unix)]
fn validate_configured_state_directory(
    path: &Path,
    roots: &[PathBuf],
    startup_environment: &StartupEnvironment,
) -> Result<(), PolicyViolation> {
    let home = startup_environment
        .get("HOME")
        .filter(|home| !home.is_empty())
        .map(Path::new)
        .filter(|home| home.is_absolute())
        .ok_or_else(policy_unreadable)?;
    validate_service_directory(home, roots)?;

    let explicit_state_dir = startup_environment
        .get("PUEUE_AGENT_STATE_DIR")
        .map(Path::new);
    let xdg_state_home = startup_environment.get("XDG_STATE_HOME").map(Path::new);
    let configured_database = paths::state_db_path_with_override(
        explicit_state_dir,
        xdg_state_home,
        Some(home),
    )
    .map_err(|_| policy_unreadable())?;
    let configured_state_dir = configured_database
        .parent()
        .ok_or_else(policy_unreadable)?;
    if path != configured_state_dir {
        return Err(policy_unreadable());
    }
    Ok(())
}

const fn policy_unreadable() -> PolicyViolation {
    PolicyViolation::new(
        PolicyViolationCode::PolicyUnreadable,
        PolicyViolationStage::Startup,
    )
}

#[cfg(unix)]
fn create_service_directory_component(
    parent: &OpenedPath,
    name: &OsStr,
    roots: &[PathBuf],
) -> Result<OpenedPath, PolicyViolation> {
    let candidate_path = parent.canonical_path.join(name);
    if inside_any_root(&candidate_path, roots) {
        return Err(PolicyViolation::new(
            PolicyViolationCode::TrustedPathUnsafe,
            PolicyViolationStage::Startup,
        ));
    }
    match mkdirat(&parent.file, name, 0o700) {
        Ok(()) => parent.file.sync_all().map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::PolicyUnreadable,
                PolicyViolationStage::Startup,
            )
        })?,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(_) => {
            return Err(PolicyViolation::new(
                PolicyViolationCode::PolicyUnreadable,
                PolicyViolationStage::Startup,
            ))
        }
    }
    let opened = open_child_path_nofollow(&parent, name).map_err(|_| {
        PolicyViolation::new(
            PolicyViolationCode::PolicyUnreadable,
            PolicyViolationStage::Startup,
        )
    })?;
    if opened.canonical_path != candidate_path {
        return Err(PolicyViolation::new(
            PolicyViolationCode::PolicyUnreadable,
            PolicyViolationStage::Startup,
        ));
    }
    validate_opened_service_directory(&opened, roots)?;
    Ok(opened)
}

fn validate_opened_service_directory(
    opened: &OpenedPath,
    roots: &[PathBuf],
) -> Result<(), PolicyViolation> {
    if inside_any_root(&opened.canonical_path, roots) {
        return Err(PolicyViolation::new(
            PolicyViolationCode::TrustedPathUnsafe,
            PolicyViolationStage::Startup,
        ));
    }
    let metadata = opened.file.metadata().map_err(|_| {
        PolicyViolation::new(
            PolicyViolationCode::PolicyUnreadable,
            PolicyViolationStage::Startup,
        )
    })?;
    if !metadata.is_dir() || !secure_metadata(&metadata) {
        return Err(PolicyViolation::new(
            PolicyViolationCode::PolicyWeakPermissions,
            PolicyViolationStage::Startup,
        ));
    }
    Ok(())
}

fn validate_opened_trusted_directory(
    opened: &OpenedPath,
    roots: &[PathBuf],
) -> Result<(), PolicyViolation> {
    if inside_any_root(&opened.canonical_path, roots) {
        return Err(PolicyViolation::new(
            PolicyViolationCode::TrustedPathUnsafe,
            PolicyViolationStage::Startup,
        ));
    }
    let metadata = opened.file.metadata().map_err(|_| {
        PolicyViolation::new(
            PolicyViolationCode::TrustedPathUnsafe,
            PolicyViolationStage::Startup,
        )
    })?;
    if !metadata.is_dir() || !secure_metadata(&metadata) {
        return Err(PolicyViolation::new(
            PolicyViolationCode::TrustedPathUnsafe,
            PolicyViolationStage::Startup,
        ));
    }
    Ok(())
}

fn inside_any_root(path: &Path, roots: &[PathBuf]) -> bool {
    roots.iter().any(|root| path == root || path.starts_with(root))
}

#[cfg(unix)]
fn create_policy_file(state_dir: &OpenedPath) -> Result<(), PolicyViolation> {
    for _ in 0..32 {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let temporary = OsString::from(format!(
            ".{POLICY_FILENAME}.{}.{}.tmp",
            std::process::id(),
            timestamp ^ u128::from(counter)
        ));
        let mut file = match openat_nofollow(
            &state_dir.file,
            &temporary,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o600,
        ) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(_) => {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::PolicyUnreadable,
                    PolicyViolationStage::Startup,
                ))
            }
        };
        #[cfg(test)]
        invoke_policy_temporary_hook(&temporary);
        #[cfg(test)]
        let force_write = take_policy_publication_failure(PolicyPublicationFailure::Write);
        #[cfg(not(test))]
        let force_write = false;
        #[cfg(test)]
        let force_sync = take_policy_publication_failure(PolicyPublicationFailure::Sync);
        #[cfg(not(test))]
        let force_sync = false;
        if force_write
            || file.write_all(DEFAULT_POLICY.as_bytes()).is_err()
            || force_sync
            || file.sync_all().is_err()
        {
            let _ = cleanup_policy_temporary(state_dir, &temporary);
            return Err(PolicyViolation::new(
                PolicyViolationCode::PolicyUnreadable,
                PolicyViolationStage::Startup,
            ));
        }
        #[cfg(test)]
        invoke_policy_publish_hook(&state_dir.canonical_path);
        #[cfg(test)]
        let force_link = take_policy_publication_failure(PolicyPublicationFailure::Link);
        #[cfg(not(test))]
        let force_link = false;
        let link_result = if force_link {
            Err(io::Error::new(io::ErrorKind::Other, "injected policy link failure"))
        } else {
            linkat(&state_dir.file, &temporary, OsStr::new(POLICY_FILENAME))
        };
        match link_result {
            Ok(()) => {
                unlinkat(&state_dir.file, &temporary).map_err(|_| {
                    PolicyViolation::new(
                        PolicyViolationCode::PolicyUnreadable,
                        PolicyViolationStage::Startup,
                    )
                })?;
                state_dir.file.sync_all().map_err(|_| {
                    PolicyViolation::new(
                        PolicyViolationCode::PolicyUnreadable,
                        PolicyViolationStage::Startup,
                    )
                })?;
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                cleanup_policy_temporary(state_dir, &temporary).map_err(|_| {
                    PolicyViolation::new(
                        PolicyViolationCode::PolicyUnreadable,
                        PolicyViolationStage::Startup,
                    )
                })?;
                return Ok(());
            }
            Err(_) => {
                let _ = cleanup_policy_temporary(state_dir, &temporary);
                return Err(PolicyViolation::new(
                    PolicyViolationCode::PolicyUnreadable,
                    PolicyViolationStage::Startup,
                ));
            }
        }
    }
    Err(PolicyViolation::new(
        PolicyViolationCode::PolicyUnreadable,
        PolicyViolationStage::Startup,
    ))
}

#[cfg(unix)]
fn cleanup_policy_temporary(state_dir: &OpenedPath, temporary: &OsStr) -> io::Result<()> {
    let unlink_result = unlinkat(&state_dir.file, temporary);
    let sync_result = state_dir.file.sync_all();
    unlink_result.and(sync_result)
}

fn secure_metadata(metadata: &Metadata) -> bool {
    owner(metadata) == current_uid() && mode(metadata) & 0o022 == 0
}

fn secure_component_metadata(metadata: &Metadata) -> bool {
    if mode(metadata) & 0o022 == 0 {
        return true;
    }
    // A root-owned sticky directory such as the host temporary directory does
    // not allow another uid to replace an entry owned by this service account.
    owner(metadata) == 0 && mode(metadata) & 0o1000 != 0
}

fn current_uid() -> u32 {
    #[cfg(unix)]
    {
        // SAFETY: geteuid has no preconditions and does not mutate memory.
        unsafe { libc::geteuid() as u32 }
    }
    #[cfg(not(unix))]
    {
        0
    }
}

fn identity(metadata: &Metadata) -> ExecutableIdentity {
    ExecutableIdentity {
        device: device(metadata),
        inode: inode(metadata),
        owner: owner(metadata),
        mode: mode(metadata),
    }
}

#[cfg(unix)]
fn device(metadata: &Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.dev()
}

#[cfg(not(unix))]
fn device(_metadata: &Metadata) -> u64 {
    0
}

#[cfg(unix)]
fn inode(metadata: &Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.ino()
}

#[cfg(not(unix))]
fn inode(_metadata: &Metadata) -> u64 {
    0
}

#[cfg(unix)]
fn owner(metadata: &Metadata) -> u32 {
    use std::os::unix::fs::MetadataExt;
    metadata.uid()
}

#[cfg(not(unix))]
fn owner(_metadata: &Metadata) -> u32 {
    0
}

#[cfg(unix)]
fn mode(metadata: &Metadata) -> u32 {
    use std::os::unix::fs::MetadataExt;
    metadata.mode() & 0o7777
}

#[cfg(not(unix))]
fn mode(metadata: &Metadata) -> u32 {
    if metadata.permissions().readonly() {
        0o444
    } else {
        0o666
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawPolicy {
    version: Option<u32>,
    trusted_path: Option<String>,
    defaults: RawDefaults,
    campaign: RawCampaignLimits,
    executables: RawExecutables,
    projects: BTreeMap<String, RawProjectPolicy>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawDefaults {
    network: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawCampaignLimits {
    max_parallel_experiments: Option<u32>,
    max_new_experiments_per_24h: Option<u32>,
    max_agent_runs_per_hour: Option<u32>,
    max_code_change_proposals_per_24h: Option<u32>,
    max_same_spec_retries: Option<u32>,
    max_repairs_per_failure_fingerprint: Option<u32>,
    max_proposals_per_cycle: Option<u32>,
    observer_interval_minutes: Option<u32>,
    max_decision_attempts_per_cycle: Option<u32>,
    max_decision_wait_minutes: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawExecutables {
    codex: String,
    pueue: String,
}

impl Default for RawExecutables {
    fn default() -> Self {
        Self {
            codex: "codex".to_owned(),
            pueue: "pueue".to_owned(),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawProjectPolicy {
    custom_agent: Option<String>,
    agent_environment_allow: Vec<String>,
    task_environment_allow: Vec<String>,
}

#[cfg(all(test, unix))]
mod fix_round_tests {
    use super::*;

    static POLICY_CWD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct CurrentDirGuard(PathBuf);

    impl Drop for CurrentDirGuard {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.0);
        }
    }

    fn install_policy_publish_hook(hook: Option<fn(&Path)>) {
        *POLICY_PUBLISH_HOOK.lock().unwrap() = hook;
    }

    fn install_policy_temporary_hook(hook: Option<fn(&OsStr)>) {
        *POLICY_TEMPORARY_HOOK.lock().unwrap() = hook;
    }

    fn install_policy_publication_failure(failure: Option<PolicyPublicationFailure>) {
        *POLICY_PUBLICATION_FAILURE.lock().unwrap() = failure;
    }

    fn write_cwd_sentinel(temporary: &OsStr) {
        fs::write(temporary, b"cwd sentinel").unwrap();
    }

    fn publish_winner(state_dir: &Path) {
        let path = state_dir.join(POLICY_FILENAME);
        fs::write(&path, DEFAULT_POLICY).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }
    }

    fn replace_policy_with_weak_file(state_dir: &Path) {
        let path = state_dir.join(POLICY_FILENAME);
        fs::rename(&path, state_dir.join("policy-old")).unwrap();
        fs::write(&path, DEFAULT_POLICY).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        }
    }

    fn startup_fixture() -> (tempfile::TempDir, PolicyLoadInput) {
        let temporary = tempfile::tempdir().unwrap();
        let base = fs::canonicalize(temporary.path()).unwrap();
        let state_dir = base.join("state");
        let project_root = base.join("project");
        let trusted_bin = base.join("trusted-bin");
        let codex_home = base.join("codex-home");
        for directory in [&state_dir, &project_root, &trusted_bin, &codex_home] {
            fs::create_dir(directory).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
            }
        }
        for name in ["codex", "pueue", "launcher"] {
            let path = trusted_bin.join(name);
            fs::write(&path, b"fixture").unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
            }
        }
        let pueue_config = base.join("pueue.yml");
        fs::write(&pueue_config, b"fixture: true\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&pueue_config, fs::Permissions::from_mode(0o600)).unwrap();
        }
        fs::write(state_dir.join(POLICY_FILENAME), DEFAULT_POLICY).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                state_dir.join(POLICY_FILENAME),
                fs::Permissions::from_mode(0o600),
            )
            .unwrap();
        }
        (
            temporary,
            PolicyLoadInput {
                state_dir: state_dir.clone(),
                project_roots: vec![project_root],
                inherited_path: trusted_bin.into_os_string(),
                startup_environment: StartupEnvironment::from_pairs([
                    ("HOME", base.as_os_str()),
                    ("PUEUE_AGENT_STATE_DIR", state_dir.as_os_str()),
                ]),
                codex_home,
                pueue_config,
                launcher_path: base.join("trusted-bin/launcher"),
            },
        )
    }

    #[test]
    fn concurrent_policy_creator_cannot_overwrite_the_winner() {
        let temporary = tempfile::tempdir().unwrap();
        let state_dir = fs::canonicalize(temporary.path()).unwrap();
        let opened = open_path_nofollow(&state_dir).unwrap();
        install_policy_publish_hook(Some(publish_winner));
        create_policy_file(&opened).unwrap();
        install_policy_publish_hook(None);
        assert_eq!(fs::read_to_string(state_dir.join(POLICY_FILENAME)).unwrap(), DEFAULT_POLICY);
    }

    #[test]
    fn publication_failures_unlink_only_the_descriptor_relative_temporary() {
        let _lock = POLICY_CWD_LOCK.lock().unwrap();
        let holder = tempfile::tempdir().unwrap();
        let state_dir = holder.path().join("state");
        let cwd = holder.path().join("cwd");
        fs::create_dir(&state_dir).unwrap();
        fs::create_dir(&cwd).unwrap();
        let opened = open_path_nofollow(&state_dir).unwrap();
        let _original_cwd = CurrentDirGuard(std::env::current_dir().unwrap());
        std::env::set_current_dir(&cwd).unwrap();
        install_policy_temporary_hook(Some(write_cwd_sentinel));

        for failure in [
            PolicyPublicationFailure::Write,
            PolicyPublicationFailure::Sync,
            PolicyPublicationFailure::Link,
        ] {
            install_policy_publication_failure(Some(failure));
            assert!(create_policy_file(&opened).is_err());
            assert_eq!(
                fs::read_dir(&state_dir).unwrap().count(),
                0,
                "state temporary leaked after {failure:?}"
            );
        }

        install_policy_temporary_hook(None);
        install_policy_publication_failure(None);
        assert_eq!(fs::read_dir(&cwd).unwrap().count(), 3);
    }

    #[test]
    fn policy_validation_is_bound_to_the_opened_descriptor() {
        let (_temporary, input) = startup_fixture();
        *POLICY_OPEN_HOOK.lock().unwrap() = Some(replace_policy_with_weak_file);
        let result = load_existing_policy(&input);
        *POLICY_OPEN_HOOK.lock().unwrap() = None;
        assert!(result.is_ok());
        assert_eq!(mode(&fs::metadata(input.state_dir.join(POLICY_FILENAME)).unwrap()), 0o644);
    }

    #[test]
    fn create_capable_load_securely_creates_a_missing_final_state_directory() {
        let (_temporary, input) = startup_fixture();
        fs::remove_file(input.state_dir.join(POLICY_FILENAME)).unwrap();
        fs::remove_dir(&input.state_dir).unwrap();

        let policy = load_or_create_policy(&input).unwrap();

        assert_eq!(policy.launcher_anchor.canonical_path, input.launcher_path);
        assert_eq!(mode(&fs::metadata(&input.state_dir).unwrap()), 0o700);
        assert_eq!(
            mode(&fs::metadata(input.state_dir.join(POLICY_FILENAME)).unwrap()),
            0o600
        );
    }

    #[test]
    fn create_capable_load_with_missing_home_does_not_create_a_final_leaf() {
        let (_temporary, mut input) = startup_fixture();
        fs::remove_file(input.state_dir.join(POLICY_FILENAME)).unwrap();
        fs::remove_dir(&input.state_dir).unwrap();
        input.startup_environment = StartupEnvironment::default();

        assert!(load_or_create_policy(&input).is_err());
        assert!(!input.state_dir.exists());
    }

    #[test]
    fn create_capable_load_does_not_create_state_inside_a_project_root() {
        let (_temporary, mut input) = startup_fixture();
        let rejected_state = input.project_roots[0].join("state");
        input.state_dir = rejected_state.clone();

        let error = load_or_create_policy(&input).unwrap_err();

        assert_eq!(error.code, PolicyViolationCode::TrustedPathUnsafe);
        assert!(!rejected_state.exists());
    }

    #[test]
    fn create_capable_load_does_not_create_state_below_an_unsafe_parent() {
        use std::os::unix::fs::PermissionsExt;

        let (_temporary, mut input) = startup_fixture();
        let unsafe_parent = input.state_dir.parent().unwrap().join("unsafe-parent");
        fs::create_dir(&unsafe_parent).unwrap();
        fs::set_permissions(&unsafe_parent, fs::Permissions::from_mode(0o777)).unwrap();
        let rejected_state = unsafe_parent.join("state");
        input.state_dir = rejected_state.clone();

        assert!(load_or_create_policy(&input).is_err());
        assert!(!rejected_state.exists());
    }

    #[test]
    fn create_capable_load_does_not_follow_a_state_directory_symlink() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let (_temporary, mut input) = startup_fixture();
        let base = input.state_dir.parent().unwrap().to_path_buf();
        let target = base.join("state-target");
        fs::create_dir(&target).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).unwrap();
        let linked_state = base.join("linked-state");
        symlink(&target, &linked_state).unwrap();
        input.state_dir = linked_state;

        assert!(load_or_create_policy(&input).is_err());
        assert!(!target.join(POLICY_FILENAME).exists());
    }

    #[test]
    fn create_capable_load_creates_an_absolute_xdg_state_home_and_app_chain() {
        let (_temporary, mut input) = startup_fixture();
        let base = input.state_dir.parent().unwrap().to_path_buf();
        let xdg_state_home = base.join("fresh-xdg-state-home");
        let state_dir = xdg_state_home.join("pueue-agent");
        input.state_dir = state_dir.clone();
        input.startup_environment = StartupEnvironment::from_pairs([
            ("HOME", base.as_os_str()),
            ("XDG_STATE_HOME", xdg_state_home.as_os_str()),
        ]);

        let policy = load_or_create_policy(&input).unwrap();

        assert_eq!(policy.launcher_anchor.canonical_path, input.launcher_path);
        assert_eq!(mode(&fs::metadata(&xdg_state_home).unwrap()), 0o700);
        assert_eq!(mode(&fs::metadata(&state_dir).unwrap()), 0o700);
        assert_eq!(
            mode(&fs::metadata(state_dir.join(POLICY_FILENAME)).unwrap()),
            0o600
        );
    }

    #[test]
    fn create_capable_load_rejects_an_unrecognized_missing_parent_chain() {
        let (_temporary, mut input) = startup_fixture();
        let missing_parent = input.state_dir.parent().unwrap().join("missing-parent");
        let rejected_state = missing_parent.join("state");
        input.state_dir = rejected_state.clone();

        assert!(load_or_create_policy(&input).is_err());
        assert!(!missing_parent.exists());
        assert!(!rejected_state.exists());
    }

    #[test]
    fn create_capable_load_with_missing_home_creates_no_xdg_state_chain() {
        let (_temporary, mut input) = startup_fixture();
        let base = input.state_dir.parent().unwrap();
        let xdg_state_home = base.join("xdg-without-home");
        let rejected_state = xdg_state_home.join("pueue-agent");
        input.state_dir = rejected_state.clone();
        input.startup_environment =
            StartupEnvironment::from_pairs([("XDG_STATE_HOME", xdg_state_home.as_os_str())]);

        assert!(load_or_create_policy(&input).is_err());
        assert!(!xdg_state_home.exists());
        assert!(!rejected_state.exists());
    }

    #[test]
    fn create_capable_load_rejects_relative_state_without_side_effects() {
        let (_temporary, mut input) = startup_fixture();
        let relative = PathBuf::from(format!(
            "relative-policy-state-{}/pueue-agent",
            std::process::id()
        ));
        input.state_dir = relative.clone();

        assert!(load_or_create_policy(&input).is_err());
        assert!(!relative.exists());
    }
}
