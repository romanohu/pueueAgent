//! Service-owned execution policy and immutable startup trust anchors.
//!
//! This module deliberately keeps policy failures bounded.  A policy error can
//! be rendered and persisted as `policy_blocked:<code>` without carrying a
//! path, environment value, command line, or other attacker-controlled text.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{OsStr, OsString},
    fmt,
    fs::{self, File, Metadata, OpenOptions},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Deserialize;

use crate::{config::ProjectConfig, models::Project, AppError};

const POLICY_FILENAME: &str = "execution-policy.toml";
const POLICY_VERSION: u32 = 1;
const DEFAULT_POLICY: &str = r#"version = 1

[defaults]
network = "enabled"

[executables]
codex = "codex"
pueue = "pueue"
"#;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Startup environment values are held in memory only.  The custom Debug and
/// Display implementations expose names/counts, never values.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct StartupEnvironment {
    values: BTreeMap<String, OsString>,
}

impl StartupEnvironment {
    pub fn capture() -> Self {
        Self {
            values: std::env::vars_os()
                .filter_map(|(name, value)| name.into_string().ok().map(|name| (name, value)))
                .collect(),
        }
    }

    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<OsString>,
    {
        Self {
            values: pairs
                .into_iter()
                .map(|(name, value)| (name.into(), value.into()))
                .collect(),
        }
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.values.keys().map(String::as_str)
    }

    pub fn get(&self, name: &str) -> Option<&OsStr> {
        self.values.get(name).map(OsString::as_os_str)
    }

    #[allow(dead_code)]
    pub(crate) fn values(&self) -> &BTreeMap<String, OsString> {
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
    pub pueue_config_anchor: PueueConfigAnchor,
    pub startup_environment: StartupEnvironment,
    pub codex_home: PathBuf,
    pub default_network: NetworkMode,
    pub custom_allowlist: BTreeMap<String, ExecutableAnchor>,
    project_environment_allow: BTreeMap<String, (BTreeSet<String>, BTreeSet<String>)>,
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
            .field("custom_allowlist", &self.custom_allowlist)
            .finish()
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

impl ExecutableAnchor {
    pub fn resolve(
        program: &OsStr,
        trusted_path: &[PathBuf],
        roots: &[PathBuf],
    ) -> Result<Self, PolicyViolation> {
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
            validate_trusted_directory(directory, roots)?;
            let candidate = directory.join(program_path);
            match fs::symlink_metadata(&candidate) {
                Ok(_) => return Self::from_absolute(&candidate, roots),
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

    pub fn from_absolute(path: &Path, roots: &[PathBuf]) -> Result<Self, PolicyViolation> {
        let canonical = canonical_path(path, PolicyViolationCode::AnchorMissing)?;
        if inside_any_root(&canonical, roots) {
            return Err(PolicyViolation::new(
                PolicyViolationCode::ProjectRootExecutable,
                PolicyViolationStage::Startup,
            ));
        }
        let file = open_nofollow(&canonical).map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorMissing,
                PolicyViolationStage::Startup,
            )
        })?;
        let metadata = file.metadata().map_err(|_| {
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
        let resolution_fingerprint = fingerprint(&canonical).map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorMissing,
                PolicyViolationStage::Startup,
            )
        })?;
        Ok(Self {
            canonical_path: canonical,
            identity,
            resolution_fingerprint,
        })
    }

    pub fn verify_identity(&self) -> Result<VerifiedExecutable, PolicyViolation> {
        let current = canonical_path(&self.canonical_path, PolicyViolationCode::AnchorReplaced)
            .map_err(|_| {
                PolicyViolation::new(
                    PolicyViolationCode::AnchorReplaced,
                    PolicyViolationStage::RunBoundPreMarker,
                )
            })?;
        let file = open_nofollow(&current).map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorReplaced,
                PolicyViolationStage::RunBoundPreMarker,
            )
        })?;
        let metadata = file.metadata().map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorReplaced,
                PolicyViolationStage::RunBoundPreMarker,
            )
        })?;
        let current_identity = identity(&metadata);
        let current_fingerprint = fingerprint(&current).map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorReplaced,
                PolicyViolationStage::RunBoundPreMarker,
            )
        })?;
        if current != self.canonical_path
            || !metadata.is_file()
            || !secure_metadata(&metadata)
            || current_identity != self.identity
            || current_fingerprint != self.resolution_fingerprint
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::AnchorReplaced,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        Ok(VerifiedExecutable {
            file,
            anchor: self.clone(),
        })
    }
}

impl ProjectRootAnchor {
    pub fn resolve(path: &Path) -> Result<Self, PolicyViolation> {
        let canonical = canonical_path(path, PolicyViolationCode::RootChanged)?;
        let file = open_nofollow(&canonical).map_err(|_| {
            PolicyViolation::new(PolicyViolationCode::RootChanged, PolicyViolationStage::Startup)
        })?;
        let metadata = file.metadata().map_err(|_| {
            PolicyViolation::new(PolicyViolationCode::RootChanged, PolicyViolationStage::Startup)
        })?;
        if !metadata.is_dir() || !secure_metadata(&metadata) {
            return Err(PolicyViolation::new(
                PolicyViolationCode::RootChanged,
                PolicyViolationStage::Startup,
            ));
        }
        Ok(Self {
            canonical_path: canonical,
            identity: identity(&metadata),
        })
    }

    pub fn verify_identity(&self) -> Result<VerifiedProjectRoot, PolicyViolation> {
        let current = canonical_path(&self.canonical_path, PolicyViolationCode::RootChanged)
            .map_err(|_| {
                PolicyViolation::new(
                    PolicyViolationCode::RootChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                )
            })?;
        let directory = open_nofollow(&current).map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::RootChanged,
                PolicyViolationStage::RunBoundPreMarker,
            )
        })?;
        let metadata = directory.metadata().map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::RootChanged,
                PolicyViolationStage::RunBoundPreMarker,
            )
        })?;
        if current != self.canonical_path
            || !metadata.is_dir()
            || !secure_metadata(&metadata)
            || identity(&metadata) != self.identity
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::RootChanged,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        Ok(VerifiedProjectRoot {
            directory,
            anchor: self.clone(),
        })
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
        let canonical = canonical_path(path, PolicyViolationCode::AnchorMissing)?;
        if inside_any_root(&canonical, roots) {
            return Err(PolicyViolation::new(
                PolicyViolationCode::TrustedPathUnsafe,
                PolicyViolationStage::Startup,
            ));
        }
        let file = open_nofollow(&canonical).map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorMissing,
                PolicyViolationStage::Startup,
            )
        })?;
        let metadata = file.metadata().map_err(|_| {
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
        let resolution_fingerprint = fingerprint(&canonical).map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorMissing,
                PolicyViolationStage::Startup,
            )
        })?;
        Ok(Self {
            canonical_path: canonical,
            identity,
            resolution_fingerprint,
        })
    }

    pub fn verify_identity(
        &self,
        roots: &[PathBuf],
    ) -> Result<VerifiedPueueConfig, AppError> {
        let current = canonical_path(&self.canonical_path, PolicyViolationCode::AnchorReplaced)?;
        if inside_any_root(&current, roots) {
            return Err(PolicyViolation::new(
                PolicyViolationCode::TrustedPathUnsafe,
                PolicyViolationStage::RunBoundPreMarker,
            )
            .into());
        }
        let file = open_nofollow(&current).map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorReplaced,
                PolicyViolationStage::RunBoundPreMarker,
            )
        })?;
        let metadata = file.metadata().map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorReplaced,
                PolicyViolationStage::RunBoundPreMarker,
            )
        })?;
        let current_identity = identity(&metadata);
        let current_fingerprint = fingerprint(&current).map_err(|_| {
            PolicyViolation::new(
                PolicyViolationCode::AnchorReplaced,
                PolicyViolationStage::RunBoundPreMarker,
            )
        })?;
        if current != self.canonical_path
            || !metadata.is_file()
            || !secure_metadata(&metadata)
            || current_identity != self.identity
            || current_fingerprint != self.resolution_fingerprint
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::AnchorReplaced,
                PolicyViolationStage::RunBoundPreMarker,
            )
            .into());
        }
        Ok(VerifiedPueueConfig {
            file,
            anchor: self.clone(),
        })
    }
}

pub fn load_or_create_policy(
    input: &PolicyLoadInput,
) -> Result<ResolvedExecutionPolicy, PolicyViolation> {
    load_policy(input, true)
}

pub fn load_existing_policy(
    input: &PolicyLoadInput,
) -> Result<ResolvedExecutionPolicy, PolicyViolation> {
    load_policy(input, false)
}

pub fn resolve_project_policy(
    global: &ResolvedExecutionPolicy,
    project: &Project,
    config: &ProjectConfig,
) -> Result<ResolvedProjectExecutionPolicy, PolicyViolation> {
    let root_anchor = ProjectRootAnchor::resolve(&project.root_path)?;
    let (agent_anchor, agent_kind) = if config.agent.program == "codex" {
        (global.codex_anchor.clone(), AgentKind::BuiltInCodex)
    } else {
        let Some(anchor) = global.custom_allowlist.get(&project.project_id) else {
            return Err(PolicyViolation::new(
                PolicyViolationCode::CustomAgentNotEnrolled,
                PolicyViolationStage::PreBinding,
            ));
        };
        (anchor.clone(), AgentKind::Custom)
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
        network: global.default_network,
        agent_environment_allow,
        task_environment_allow,
        codex_home: global.codex_home.clone(),
        private_temp_relative_root: PathBuf::from(".pueue-agent/tmp"),
    })
}

fn load_policy(
    input: &PolicyLoadInput,
    create_missing: bool,
) -> Result<ResolvedExecutionPolicy, PolicyViolation> {
    let project_roots = canonical_project_roots(&input.project_roots)?;
    let state_dir = validate_service_directory(&input.state_dir, &project_roots)?;
    let codex_home = validate_service_directory(&input.codex_home, &project_roots)?;
    let policy_path = state_dir.join(POLICY_FILENAME);

    match fs::symlink_metadata(&policy_path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink()
                || !metadata.is_file()
                || !secure_metadata(&metadata)
                || mode(&metadata) & 0o077 != 0
            {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::PolicyWeakPermissions,
                    PolicyViolationStage::Startup,
                ));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound && create_missing => {
            create_policy_file(&state_dir)?;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(PolicyViolation::new(
                PolicyViolationCode::PolicyMissing,
                PolicyViolationStage::Startup,
            ));
        }
        Err(_) => {
            return Err(PolicyViolation::new(
                PolicyViolationCode::PolicyUnreadable,
                PolicyViolationStage::Startup,
            ));
        }
    }

    let policy_file = open_nofollow(&policy_path).map_err(|_| {
        PolicyViolation::new(
            PolicyViolationCode::PolicyUnreadable,
            PolicyViolationStage::Startup,
        )
    })?;
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
    if raw.version.unwrap_or(POLICY_VERSION) != POLICY_VERSION {
        return Err(PolicyViolation::new(
            PolicyViolationCode::PolicyUnknownField,
            PolicyViolationStage::Startup,
        ));
    }

    let trusted_path = parse_trusted_path(raw.trusted_path.as_deref(), &input.inherited_path)?;
    for directory in &trusted_path {
        validate_trusted_directory(directory, &project_roots)?;
    }

    let codex_anchor = resolve_policy_executable(&raw.executables.codex, &trusted_path, &project_roots)?;
    let pueue_anchor = resolve_policy_executable(&raw.executables.pueue, &trusted_path, &project_roots)?;
    let launcher_anchor = ExecutableAnchor::from_absolute(&input.launcher_path, &project_roots)?;
    let pueue_config_anchor = PueueConfigAnchor::from_absolute(&input.pueue_config, &project_roots)?;
    let default_network = parse_network(raw.defaults.network.as_deref())?;

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
        pueue_config_anchor,
        startup_environment: input.startup_environment.clone(),
        codex_home,
        default_network,
        custom_allowlist,
        project_environment_allow,
    })
}

fn resolve_policy_executable(
    configured: &str,
    trusted_path: &[PathBuf],
    roots: &[PathBuf],
) -> Result<ExecutableAnchor, PolicyViolation> {
    if configured.is_empty() {
        return Err(PolicyViolation::new(
            PolicyViolationCode::AnchorMissing,
            PolicyViolationStage::Startup,
        ));
    }
    ExecutableAnchor::resolve(OsStr::new(configured), trusted_path, roots)
}

fn parse_trusted_path(
    configured: Option<&str>,
    inherited: &OsStr,
) -> Result<Vec<PathBuf>, PolicyViolation> {
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
        .map(|path| canonical_path(&path, PolicyViolationCode::TrustedPathUnsafe))
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

fn canonical_project_roots(roots: &[PathBuf]) -> Result<Vec<PathBuf>, PolicyViolation> {
    if roots.is_empty() {
        return Ok(Vec::new());
    }
    roots
        .iter()
        .map(|root| {
            let canonical = canonical_path(root, PolicyViolationCode::RootChanged)?;
            let file = open_nofollow(&canonical).map_err(|_| {
                PolicyViolation::new(PolicyViolationCode::RootChanged, PolicyViolationStage::Startup)
            })?;
            let metadata = file.metadata().map_err(|_| {
                PolicyViolation::new(PolicyViolationCode::RootChanged, PolicyViolationStage::Startup)
            })?;
            if !metadata.is_dir() || !secure_metadata(&metadata) {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::RootChanged,
                    PolicyViolationStage::Startup,
                ));
            }
            Ok(canonical)
        })
        .collect()
}

fn validate_service_directory(
    path: &Path,
    roots: &[PathBuf],
) -> Result<PathBuf, PolicyViolation> {
    let canonical = canonical_path(path, PolicyViolationCode::PolicyUnreadable)?;
    if inside_any_root(&canonical, roots) {
        return Err(PolicyViolation::new(
            PolicyViolationCode::TrustedPathUnsafe,
            PolicyViolationStage::Startup,
        ));
    }
    let file = open_nofollow(&canonical).map_err(|_| {
        PolicyViolation::new(
            PolicyViolationCode::PolicyUnreadable,
            PolicyViolationStage::Startup,
        )
    })?;
    let metadata = file.metadata().map_err(|_| {
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
    Ok(canonical)
}

fn validate_trusted_directory(
    path: &Path,
    roots: &[PathBuf],
) -> Result<(), PolicyViolation> {
    let canonical = canonical_path(path, PolicyViolationCode::TrustedPathUnsafe)?;
    if inside_any_root(&canonical, roots) {
        return Err(PolicyViolation::new(
            PolicyViolationCode::TrustedPathUnsafe,
            PolicyViolationStage::Startup,
        ));
    }
    let file = open_nofollow(&canonical).map_err(|_| {
        PolicyViolation::new(
            PolicyViolationCode::TrustedPathUnsafe,
            PolicyViolationStage::Startup,
        )
    })?;
    let metadata = file.metadata().map_err(|_| {
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

fn canonical_path(path: &Path, missing_code: PolicyViolationCode) -> Result<PathBuf, PolicyViolation> {
    if !path.is_absolute() {
        return Err(PolicyViolation::new(
            missing_code,
            PolicyViolationStage::Startup,
        ));
    }
    reject_symlink_components(path, missing_code)?;
    let canonical = fs::canonicalize(path).map_err(|_| {
        PolicyViolation::new(missing_code, PolicyViolationStage::Startup)
    })?;
    if !canonical.is_absolute() {
        return Err(PolicyViolation::new(missing_code, PolicyViolationStage::Startup));
    }
    reject_symlink_components(&canonical, missing_code)?;
    Ok(canonical)
}

fn reject_symlink_components(
    path: &Path,
    code: PolicyViolationCode,
) -> Result<(), PolicyViolation> {
    if !path.is_absolute() {
        return Err(PolicyViolation::new(code, PolicyViolationStage::Startup));
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(Path::new(std::path::MAIN_SEPARATOR_STR)),
            Component::CurDir => {}
            Component::ParentDir => current.push(".."),
            Component::Normal(name) => {
                current.push(name);
                let metadata = fs::symlink_metadata(&current).map_err(|_| {
                    PolicyViolation::new(code, PolicyViolationStage::Startup)
                })?;
                if metadata.file_type().is_symlink() {
                    return Err(PolicyViolation::new(code, PolicyViolationStage::Startup));
                }
            }
        }
    }
    Ok(())
}

fn inside_any_root(path: &Path, roots: &[PathBuf]) -> bool {
    roots.iter().any(|root| path == root || path.starts_with(root))
}

fn create_policy_file(state_dir: &Path) -> Result<(), PolicyViolation> {
    for _ in 0..32 {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let temporary = state_dir.join(format!(
            ".{POLICY_FILENAME}.{}.{}.tmp",
            std::process::id(),
            timestamp ^ u128::from(counter)
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        }
        let mut file = match options.open(&temporary) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(_) => {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::PolicyUnreadable,
                    PolicyViolationStage::Startup,
                ))
            }
        };
        if file.write_all(DEFAULT_POLICY.as_bytes()).is_err() || file.sync_all().is_err() {
            let _ = fs::remove_file(&temporary);
            return Err(PolicyViolation::new(
                PolicyViolationCode::PolicyUnreadable,
                PolicyViolationStage::Startup,
            ));
        }
        if fs::symlink_metadata(state_dir.join(POLICY_FILENAME)).is_ok() {
            let _ = fs::remove_file(&temporary);
            return Ok(());
        }
        drop(file);
        match fs::rename(&temporary, state_dir.join(POLICY_FILENAME)) {
            Ok(()) => {
                let directory = File::open(state_dir).map_err(|_| {
                    PolicyViolation::new(
                        PolicyViolationCode::PolicyUnreadable,
                        PolicyViolationStage::Startup,
                    )
                })?;
                directory.sync_all().map_err(|_| {
                    PolicyViolation::new(
                        PolicyViolationCode::PolicyUnreadable,
                        PolicyViolationStage::Startup,
                    )
                })?;
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&temporary);
                return Ok(());
            }
            Err(_) => {
                let _ = fs::remove_file(&temporary);
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

fn open_nofollow(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    options.open(path)
}

fn secure_metadata(metadata: &Metadata) -> bool {
    owner(metadata) == current_uid() && mode(metadata) & 0o022 == 0
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

fn fingerprint(path: &Path) -> io::Result<String> {
    let canonical = fs::canonicalize(path)?;
    let mut current = PathBuf::new();
    let mut result = String::new();
    for component in canonical.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(Path::new(std::path::MAIN_SEPARATOR_STR)),
            Component::CurDir => {}
            Component::ParentDir => current.push(".."),
            Component::Normal(name) => {
                current.push(name);
                let metadata = fs::symlink_metadata(&current)?;
                let item = identity(&metadata);
                use std::fmt::Write as _;
                write!(
                    &mut result,
                    "{:?}:{:x}:{:x}:{:x}:{:x};",
                    name, item.device, item.inode, item.owner, item.mode
                )
                .expect("writing to String cannot fail");
            }
        }
    }
    Ok(result)
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawPolicy {
    version: Option<u32>,
    trusted_path: Option<String>,
    defaults: RawDefaults,
    executables: RawExecutables,
    projects: BTreeMap<String, RawProjectPolicy>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawDefaults {
    network: Option<String>,
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
