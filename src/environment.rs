//! Explicit, default-deny child environments and private per-run temporary
//! directories.
//!
//! Environment values are kept as `OsString`s until the final process API
//! call.  Debug output intentionally contains names only.  Private run
//! directories are retained at their original unique paths: portable Unix
//! APIs do not provide an atomic conditional unlink/rename by inode, so a
//! cleanup pathname could otherwise mutate a replacement generation.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{OsStr, OsString},
    fmt,
    fs::File,
    io,
    path::{Path, PathBuf},
    time::Instant,
};

use serde::Serialize;

use crate::execution_policy::{
    ExecutableIdentity, PolicyViolation, PolicyViolationCode, PolicyViolationStage,
    PolicyViolationDetail, ResolvedExecutionPolicy, ResolvedProjectExecutionPolicy,
    TempUnsafeReason, VerifiedProjectRoot,
};
#[cfg(target_os = "linux")]
use std::{io::Write, sync::Mutex};
#[cfg(target_os = "linux")]
use crate::decision_protocol::MAX_DECISION_BYTES;

pub use crate::execution_policy::StartupEnvironment;

const PRIVATE_TEMP_ROOT: &str = ".pueue-agent";
const PRIVATE_TEMP_DIR: &str = "tmp";
const RESULTS_DIRECTORY: &str = "results";
const ARTIFACTS_DIRECTORY: &str = "artifacts";
const MAX_RUN_ID_BYTES: usize = 20;

/// The project-relative service directory owned by this supervisor.
pub(crate) fn private_service_root() -> &'static str {
    PRIVATE_TEMP_ROOT
}

/// Names and values injected onto agent tasks that manage one campaign
/// experiment. Values are identifiers and derived paths only; the result and
/// artifact directories are created by the task itself, never pre-created.
pub fn campaign_experiment_task_environment(
    project_root: &Path,
    campaign_id: &str,
    experiment_id: &str,
) -> Vec<(String, OsString)> {
    let service_root = project_root.join(PRIVATE_TEMP_ROOT);
    vec![
        (
            "PUEUE_AGENT_EXPERIMENT_ID".to_owned(),
            OsString::from(experiment_id),
        ),
        (
            "PUEUE_AGENT_CAMPAIGN_ID".to_owned(),
            OsString::from(campaign_id),
        ),
        (
            "PUEUE_AGENT_RESULT_PATH".to_owned(),
            service_root
                .join(RESULTS_DIRECTORY)
                .join(format!("{experiment_id}.json"))
                .into_os_string(),
        ),
        (
            "PUEUE_AGENT_ARTIFACT_DIR".to_owned(),
            service_root
                .join(ARTIFACTS_DIRECTORY)
                .join(experiment_id)
                .into_os_string(),
        ),
    ]
}

/// The sole private-temp descriptor inherited by native agent targets.
pub const PRIVATE_TEMP_TARGET_FD: i32 = 11;
const PRIVATE_TEMP_TARGET_PATH: &str = "/dev/fd/11";

pub(crate) fn private_temp_target_path() -> &'static Path {
    Path::new(PRIVATE_TEMP_TARGET_PATH)
}

pub const MAX_PRIVATE_TEMP_CLEANUP_DEPTH: usize = 32;
pub const MAX_PRIVATE_TEMP_CLEANUP_ENTRIES: usize = 4096;
pub const MAX_PRIVATE_TEMP_ALLOCATED_BYTES: u64 = 1024 * 1024 * 1024;
pub const MAX_PRIVATE_TEMP_GENERATIONS: usize = 4096;
pub const MAX_PRIVATE_TEMP_RUN_ID: i64 = i64::MAX - 1;
const MAX_DECISION_ARTIFACT_SCAN_ENTRIES: usize = 4096;

/// Metadata-only evidence discovered relative to a retained project-root
/// descriptor. File contents are deliberately outside this projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DecisionArtifactHint {
    pub path: String,
    pub size: u64,
    pub mtime: i64,
}

/// Discover bounded regular-file metadata without reopening the project by
/// pathname or following symlinks, special files, or mount changes.
pub fn collect_decision_artifact_hints(
    root_anchor: &crate::execution_policy::ProjectRootAnchor,
    max_hints: usize,
    max_depth: usize,
    max_field_bytes: usize,
) -> Result<Vec<DecisionArtifactHint>, crate::AppError> {
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root_anchor, max_hints, max_depth, max_field_bytes);
        return Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::RunBoundPreMarker,
        )
        .into());
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        if max_hints == 0 || max_depth == 0 || max_field_bytes == 0 {
            return Ok(Vec::new());
        }
        let verified = root_anchor.verify_identity()?;
        let root_mount = directory_mount_identity_at(
            &verified.directory,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let mut state = ArtifactHintScan {
            entries: 0,
            hints: Vec::with_capacity(max_hints.min(64)),
            max_hints,
            max_depth,
            max_field_bytes,
            root_owner: root_anchor.identity.owner,
            root_mount,
        };
        let result = scan_artifact_hints(&verified.directory, "", 0, &mut state);
        finish_artifact_scan_with_mount_reader(
            root_anchor,
            root_mount,
            result,
            |directory| {
                directory_mount_identity_at(
                    directory,
                    PolicyViolationStage::RunBoundPreMarker,
                )
            },
        )?;
        state.hints.sort_unstable_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.size.cmp(&right.size))
                .then_with(|| left.mtime.cmp(&right.mtime))
        });
        state.hints.truncate(max_hints);
        Ok(state.hints)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn finish_artifact_scan_with_mount_reader<F>(
    root_anchor: &crate::execution_policy::ProjectRootAnchor,
    captured_mount: MountIdentity,
    scan_result: Result<(), PolicyViolation>,
    mount_reader: F,
) -> Result<(), PolicyViolation>
where
    F: FnOnce(&File) -> Result<MountIdentity, PolicyViolation>,
{
    let verified = root_anchor.verify_identity()?;
    if mount_reader(&verified.directory)? != captured_mount {
        return Err(temp_violation(TempUnsafeReason::MountBoundary));
    }
    scan_result
}

const BASELINE_NAMES: &[&str] = &[
    "HOME",
    "PATH",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TMPDIR",
    "TMP",
    "TEMP",
    "PUEUE_AGENT_RUN_ID",
    "PUEUE_AGENT_PROJECT_ID",
];

const PROXY_AND_CERT_NAMES: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "NODE_EXTRA_CA_CERTS",
    "AWS_CA_BUNDLE",
];

const CODEX_AUTH_NAMES: &[&str] = &[
    "OPENAI_API_KEY",
    "CODEX_API_KEY",
    "CODEX_AUTH_TOKEN",
];

pub(crate) fn is_proxy_or_cert_name(name: &str) -> bool {
    PROXY_AND_CERT_NAMES.contains(&name)
}

/// Names used by the Codex shell environment filter.  This deliberately
/// contains a fixed non-secret baseline even when a project has no task
/// allowlist, so an empty filter can never accidentally mean "inherit all".
pub(crate) fn shell_baseline_names() -> &'static [&'static str] {
    BASELINE_NAMES
}

/// Conservative authentication-name denial.  The explicit names cover known
/// credentials; the suffix/prefix rules cover newly introduced credential
/// variables without relying on a single heuristic.
pub(crate) fn is_auth_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    matches!(
        upper.as_str(),
        "OPENAI_API_KEY"
            | "CODEX_API_KEY"
            | "CODEX_AUTH_TOKEN"
            | "CODEX_HOME"
            | "AWS_ACCESS_KEY_ID"
            | "AWS_SECRET_ACCESS_KEY"
            | "GITHUB_TOKEN"
            | "GH_TOKEN"
            | "NPM_TOKEN"
            | "AUTH_TOKEN"
            | "ACCESS_TOKEN"
            | "REFRESH_TOKEN"
            | "API_KEY"
            | "GOOGLE_APPLICATION_CREDENTIALS"
            | "DOCKER_AUTH_CONFIG"
            | "REGISTRY_AUTH_FILE"
            | "KUBECONFIG"
            | "AZURE_CLIENT_ID"
            | "AZURE_CLIENT_SECRET"
            | "AWS_SESSION_TOKEN"
            | "AWS_SECURITY_TOKEN"
            | "AWS_PROFILE"
            | "AWS_SHARED_CREDENTIALS_FILE"
            | "AWS_CONFIG_FILE"
            | "AWS_WEB_IDENTITY_TOKEN_FILE"
            | "AWS_ROLE_ARN"
            | "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"
            | "AWS_CONTAINER_CREDENTIALS_FULL_URI"
            | "GOOGLE_CLOUD_KEYFILE_JSON"
            | "GOOGLE_GHA_CREDS_PATH"
            | "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES"
            | "CLOUDSDK_AUTH_ACCESS_TOKEN"
            | "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"
            | "CLOUDSDK_CONFIG"
            | "BOTO_CONFIG"
            | "AZURE_TENANT_ID"
            | "AZURE_SUBSCRIPTION_ID"
            | "AZURE_FEDERATED_TOKEN_FILE"
            | "AZURE_USERNAME"
            | "AZURE_PASSWORD"
            | "AZURE_CONFIG_DIR"
            | "AZURE_AUTH_LOCATION"
            | "AZURE_STORAGE_KEY"
            | "AZURE_STORAGE_CONNECTION_STRING"
            | "DOCKER_CONFIG"
            | "CONTAINER_AUTH_FILE"
            | "BUILDAH_AUTH"
            | "HELM_KUBECONTEXT"
            | "HELM_CONFIG_HOME"
            | "HELM_DATA_HOME"
            | "HELM_CACHE_HOME"
            | "SSH_AUTH_SOCK"
            | "GIT_ASKPASS"
            | "GIT_SSH_COMMAND"
            | "GIT_CREDENTIAL_HELPER"
            | "GCM_INTERACTIVE"
    ) || upper.ends_with("_API_KEY")
        || upper.ends_with("_TOKEN")
        || upper.ends_with("_SECRET")
        || upper.ends_with("_PASSWORD")
        || upper.ends_with("_PRIVATE_KEY")
        || upper.ends_with("_CREDENTIAL")
        || upper.ends_with("_CREDENTIALS")
        || upper.starts_with("GIT_CONFIG_")
        || upper.starts_with("GCM_")
        || upper.contains("TOKEN")
        || upper.contains("SECRET")
        || upper.contains("PASSWORD")
        || upper.contains("PRIVATE_KEY")
        || upper.starts_with("AWS_SECRET_")
        || upper.starts_with("AZURE_CLIENT_SECRET")
        || upper.starts_with("AZURE_STORAGE_")
        || (upper.contains("STORAGE")
            && (upper.contains("KEY")
                || upper.contains("SECRET")
                || upper.contains("TOKEN")
                || upper.contains("CREDENTIAL")
                || upper.contains("PASSWORD")
                || upper.contains("CONNECTION")))
}

/// A captured, explicit child environment.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SanitizedEnvironment {
    values: BTreeMap<OsString, OsString>,
}

impl SanitizedEnvironment {
    pub(crate) fn entries(&self) -> impl Iterator<Item = (&OsStr, &OsStr)> {
        self.values
            .iter()
            .map(|(name, value)| (name.as_os_str(), value.as_os_str()))
    }

    pub fn get(&self, name: &str) -> Option<&OsStr> {
        self.values.get(OsStr::new(name)).map(OsString::as_os_str)
    }

    pub fn names(&self) -> impl Iterator<Item = &OsStr> {
        self.values.keys().map(OsString::as_os_str)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Replace the command's inherited environment and then add exactly the
    /// captured/generated values in this object.
    pub fn apply<C: EnvironmentCommand>(&self, command: &mut C) {
        command.environment_clear();
        for (name, value) in &self.values {
            command.environment_set(name, value);
        }
    }

    pub fn for_codex_agent(
        startup: &StartupEnvironment,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
    ) -> Result<Self, PolicyViolation> {
        let mut environment = Self::project_baseline(startup, policy, run_id, true)?;
        environment.insert_generated("CODEX_HOME", policy.codex_home.as_os_str());
        for name in CODEX_AUTH_NAMES {
            if let Some(value) = startup.get(name) {
                environment.insert_os(OsString::from(name), value.to_os_string());
            }
        }
        Ok(environment)
    }

    pub(crate) fn for_codex_decision(
        startup: &StartupEnvironment,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
    ) -> Result<Self, PolicyViolation> {
        let mut environment = Self::project_baseline(startup, policy, run_id, true)?;
        environment.insert_generated("CODEX_HOME", policy.codex_home.as_os_str());
        Ok(environment)
    }

    pub fn for_custom_agent(
        startup: &StartupEnvironment,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
    ) -> Result<Self, PolicyViolation> {
        let mut environment = Self::project_baseline(startup, policy, run_id, false)?;
        environment.copy_allowlist(startup, &policy.agent_environment_allow);
        Ok(environment)
    }

    pub fn for_codex_task(
        startup: &StartupEnvironment,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
    ) -> Result<Self, PolicyViolation> {
        let mut environment = Self::project_baseline(startup, policy, run_id, false)?;
        environment.copy_allowlist(startup, &policy.task_environment_allow);
        Ok(environment)
    }

    pub fn for_pueue(policy: &ResolvedExecutionPolicy) -> Result<Self, PolicyViolation> {
        let environment = Self::default_baseline(
            &policy.startup_environment,
            &policy.trusted_path,
            None,
            None,
            false,
        )?;
        // The caller launches the startup-pinned executable itself.  Keep the
        // anchor in this API to make it impossible to accidentally substitute
        // an ambient Pueue path while preparing the environment, but do not
        // expose the path as a child variable.
        let _ = &policy.pueue_anchor;
        Ok(environment)
    }

    fn project_baseline(
        startup: &StartupEnvironment,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
        include_proxy_cert: bool,
    ) -> Result<Self, PolicyViolation> {
        validate_run_id(run_id)?;
        Self::default_baseline(
            startup,
            &policy.trusted_path,
            Some(OsStr::new(PRIVATE_TEMP_TARGET_PATH)),
            Some((run_id, policy.project_id.as_str())),
            include_proxy_cert,
        )
    }

    fn default_baseline(
        startup: &StartupEnvironment,
        trusted_path: &[PathBuf],
        temp_path: Option<&OsStr>,
        ids: Option<(i64, &str)>,
        include_proxy_cert: bool,
    ) -> Result<Self, PolicyViolation> {
        let mut environment = Self::default();
        for name in ["HOME"] {
            environment.copy_startup_name(startup, name);
        }
        if trusted_path.is_empty() {
            if let Some(path) = startup.get("PATH") {
                environment.insert_os(OsString::from("PATH"), path.to_os_string());
            }
        } else {
            environment.insert_generated("PATH", join_trusted_path(trusted_path)?);
        }
        for name in ["LANG", "LC_ALL", "LC_CTYPE"] {
            environment.insert_generated(name, OsStr::new("C"));
        }
        if let Some(temp_path) = temp_path {
            for name in ["TMPDIR", "TMP", "TEMP"] {
                environment.insert_generated(name, temp_path);
            }
        } else {
            for name in ["TMPDIR", "TMP", "TEMP"] {
                environment.copy_startup_name(startup, name);
            }
        }
        if include_proxy_cert {
            for name in PROXY_AND_CERT_NAMES {
                environment.copy_startup_name(startup, name);
            }
        }
        if let Some((run_id, project_id)) = ids {
            environment.insert_generated("PUEUE_AGENT_RUN_ID", OsString::from(run_id.to_string()));
            environment.insert_generated("PUEUE_AGENT_PROJECT_ID", OsString::from(project_id));
        }
        Ok(environment)
    }

    fn copy_allowlist(&mut self, startup: &StartupEnvironment, allowlist: &BTreeSet<String>) {
        for name in allowlist {
            if !is_auth_name(name)
                && !is_proxy_or_cert_name(name)
            {
                if let Some(value) = startup.get(name) {
                    self.insert_os(OsString::from(name), value.to_os_string());
                }
            }
        }
    }

    fn copy_startup_name(&mut self, startup: &StartupEnvironment, name: &str) {
        if let Some(value) = startup.get(name) {
            self.insert_os(OsString::from(name), value.to_os_string());
        }
    }

    fn insert_generated(&mut self, name: &str, value: impl Into<OsString>) {
        self.values.insert(OsString::from(name), value.into());
    }

    pub(crate) fn apply_campaign_experiment_variables(
        &mut self,
        variables: &[(String, OsString)],
    ) {
        for (name, value) in variables {
            self.insert_generated(name, value.as_os_str());
        }
    }

    fn insert_os(&mut self, name: OsString, value: OsString) {
        self.values.insert(name, value);
    }
}

impl fmt::Debug for SanitizedEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SanitizedEnvironment")
            .field("names", &self.values.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl fmt::Display for SanitizedEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "sanitized environment ({} names)", self.values.len())
    }
}

/// The small command surface needed by `SanitizedEnvironment::apply`.
pub trait EnvironmentCommand {
    fn environment_clear(&mut self);
    fn environment_set(&mut self, name: &OsStr, value: &OsStr);
}

impl EnvironmentCommand for std::process::Command {
    fn environment_clear(&mut self) {
        self.env_clear();
    }

    fn environment_set(&mut self, name: &OsStr, value: &OsStr) {
        self.env(name, value);
    }
}

impl EnvironmentCommand for tokio::process::Command {
    fn environment_clear(&mut self) {
        self.env_clear();
    }

    fn environment_set(&mut self, name: &OsStr, value: &OsStr) {
        self.env(name, value);
    }
}

fn join_trusted_path(paths: &[PathBuf]) -> Result<OsString, PolicyViolation> {
    std::env::join_paths(paths).map_err(|_| environment_error())
}

fn validate_run_id(run_id: i64) -> Result<(), PolicyViolation> {
    let text = run_id.to_string();
    if run_id <= 0 || run_id > MAX_PRIVATE_TEMP_RUN_ID || text.len() > MAX_RUN_ID_BYTES {
        Err(PolicyViolation::new(
            PolicyViolationCode::TempUnsafe,
            PolicyViolationStage::RunBoundPreMarker,
        ))
    } else {
        Ok(())
    }
}

fn environment_error() -> PolicyViolation {
    PolicyViolation::new(
        PolicyViolationCode::EnvironmentName,
        PolicyViolationStage::PreBinding,
    )
}

fn temp_error() -> PolicyViolation {
    PolicyViolation::new(
        PolicyViolationCode::TempUnsafe,
        PolicyViolationStage::RunBoundPreMarker,
    )
}

pub struct ProjectAdmissionLock {
    directory: File,
}

/// Cross-process serialization for the durable run-ID floor and allocator.
/// The guard flocks a fresh descriptor opened relative to the retained
/// database-parent capability, so no replaceable lock-file leaf is trusted.
/// The descriptor is close-on-exec and never held across native launch.
pub struct RunIdAdmissionGuard {
    file: File,
}

impl RunIdAdmissionGuard {
    pub(crate) fn try_acquire(
        parent: &File,
    ) -> Result<Option<Self>, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = parent;
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::PreBinding,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            use std::os::fd::{AsRawFd, FromRawFd};
            let name = std::ffi::CString::new(".").expect("literal contains no NUL");
            let fd = unsafe {
                libc::openat(
                    parent.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY
                        | libc::O_DIRECTORY
                        | libc::O_CLOEXEC
                        | libc::O_NOFOLLOW,
                )
            };
            if fd < 0 {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::RootChanged,
                    PolicyViolationStage::PreBinding,
                ));
            }
            let file = unsafe { File::from_raw_fd(fd) };
            let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if result == 0 {
                return Ok(Some(Self { file }));
            }
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EWOULDBLOCK) {
                return Ok(None);
            }
            Err(PolicyViolation::new(PolicyViolationCode::TempUnsafe, PolicyViolationStage::PreBinding))
        }
    }
}

impl Drop for RunIdAdmissionGuard {
    fn drop(&mut self) {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            use std::os::fd::AsRawFd;
            unsafe {
                libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
            }
        }
    }
}

impl ProjectAdmissionLock {
    pub(crate) fn try_acquire(
        root: &VerifiedProjectRoot,
    ) -> Result<Option<Self>, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = root;
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::PreBinding,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            use std::os::fd::{AsRawFd, FromRawFd};
            let name = std::ffi::CString::new(".").expect("literal contains no NUL");
            let fd = unsafe {
                libc::openat(
                    root.directory.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY
                        | libc::O_DIRECTORY
                        | libc::O_CLOEXEC
                        | libc::O_NOFOLLOW,
                )
            };
            if fd < 0 {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::RootChanged,
                    PolicyViolationStage::PreBinding,
                ));
            }
            let directory = unsafe { File::from_raw_fd(fd) };
            let result = unsafe {
                libc::flock(directory.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB)
            };
            if result == 0 {
                return Ok(Some(Self { directory }));
            }
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
                return Ok(None);
            }
            Err(PolicyViolation::new(
                PolicyViolationCode::TempUnsafe,
                PolicyViolationStage::PreBinding,
            ))
        }
    }
}

impl Drop for ProjectAdmissionLock {
    fn drop(&mut self) {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            use std::os::fd::AsRawFd;
            unsafe {
                libc::flock(self.directory.as_raw_fd(), libc::LOCK_UN);
            }
        }
    }
}

/// An exclusive, owner-only per-run directory.
pub struct PrivateRunTemp {
    name: OsString,
    path: PathBuf,
    parent: File,
    directory: File,
    identity: (u64, u64),
    #[cfg(target_os = "linux")]
    decision_output_anchor: Mutex<Option<DecisionOutputAnchor>>,
}

/// An opaque, verified directory capability for the native target's private
/// temporary directory role. It has no raw-descriptor or path constructor.
/// This capability proves only the directory itself; descriptor-relative
/// descendant containment remains outside this launch ABI's scope.
pub(crate) struct VerifiedPrivateTemp {
    pub(crate) directory: File,
    pub(crate) identity: ExecutableIdentity,
    run_id: i64,
}

impl VerifiedPrivateTemp {
    pub(crate) fn target_path(&self) -> &'static Path {
        private_temp_target_path()
    }
}

impl fmt::Debug for VerifiedPrivateTemp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedPrivateTemp")
            .field("role", &"private-temp-target")
            .field("run_id", &self.run_id)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TempCleanupReport {
    pub entries_removed: usize,
    pub allocated_bytes_reclaimed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TempInventoryReport {
    pub generations: usize,
    pub retained_nonempty_generations: usize,
    pub retained_allocated_bytes: u64,
    pub max_generation_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct TempGenerationFloorReport {
    pub max_generation_id: Option<i64>,
}

impl TempInventoryReport {
    pub(crate) fn validate_durable_high_water(
        &self,
        durable_run_id_high_water: i64,
    ) -> Result<(), PolicyViolation> {
        validate_generation_high_water(self.max_generation_id, durable_run_id_high_water)
    }
}

impl TempGenerationFloorReport {
    pub(crate) fn validate_durable_high_water(
        &self,
        durable_run_id_high_water: i64,
    ) -> Result<(), PolicyViolation> {
        validate_generation_high_water(self.max_generation_id, durable_run_id_high_water)
    }
}

impl fmt::Debug for PrivateRunTemp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrivateRunTemp")
            .field("path", &self.path)
            .field("run_id", &self.name)
            .finish()
    }
}

impl PrivateRunTemp {
    /// Inspect top-level generation IDs for validation against the durable run
    /// ID high-water. Filesystem entries are never allocator authority and
    /// this observation never mutates global state.
    pub(crate) fn inspect_generation_floor(
        root: &VerifiedProjectRoot,
    ) -> Result<TempGenerationFloorReport, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = root;
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::PreBinding,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let root_mount = directory_mount_identity_at(
                &root.directory,
                PolicyViolationStage::PreBinding,
            )?;
            let service = match open_optional_directory_on_mount(
                &root.directory,
                OsStr::new(PRIVATE_TEMP_ROOT),
                root_mount,
                PolicyViolationStage::PreBinding,
            )? {
                Some(directory) => {
                    validate_private_temp_container_at(
                        &directory,
                        PolicyViolationStage::PreBinding,
                    )?;
                    directory
                }
                None => {
                    return Ok(TempGenerationFloorReport::default());
                }
            };
            let tmp = match open_optional_directory_on_mount(
                &service,
                OsStr::new(PRIVATE_TEMP_DIR),
                root_mount,
                PolicyViolationStage::PreBinding,
            )? {
                Some(directory) => {
                    validate_private_directory_at(&directory, PolicyViolationStage::PreBinding)?;
                    directory
                }
                None => {
                    return Ok(TempGenerationFloorReport::default());
                }
            };
            let entries = directory_entries(
                &tmp,
                MAX_PRIVATE_TEMP_GENERATIONS,
                TempUnsafeReason::GenerationLimit,
                PolicyViolationStage::PreBinding,
                None,
                None,
                #[cfg(all(test, unix))]
                None,
            )?;
            let mut report = TempGenerationFloorReport::default();
            for entry in entries {
                if entry.mount_identity != root_mount {
                    return Err(temp_violation_at(
                        TempUnsafeReason::MountBoundary,
                        PolicyViolationStage::PreBinding,
                    ));
                }
                let name = entry.name.to_str().ok_or_else(|| {
                    temp_violation_at(
                        TempUnsafeReason::InvalidEntry,
                        PolicyViolationStage::PreBinding,
                    )
                })?;
                let generation_id = name
                    .parse::<i64>()
                    .ok()
                    .filter(|value| *value > 0 && *value <= MAX_PRIVATE_TEMP_RUN_ID)
                    .filter(|value| name == value.to_string())
                    .ok_or_else(|| {
                        temp_violation_at(
                            TempUnsafeReason::InvalidEntry,
                            PolicyViolationStage::PreBinding,
                        )
                    })?;
                if !matches!(entry.kind, AuditedEntryKind::Directory) {
                    return Err(temp_violation_at(
                        TempUnsafeReason::InvalidEntry,
                        PolicyViolationStage::PreBinding,
                    ));
                }
                let generation = open_directory_on_mount(
                    &tmp,
                    &entry.name,
                    root_mount,
                    PolicyViolationStage::PreBinding,
                )?;
                validate_private_directory_at(&generation, PolicyViolationStage::PreBinding)?;
                if directory_identity_at(&generation, PolicyViolationStage::PreBinding)?
                    != entry.identity
                {
                    return Err(temp_violation_at(
                        TempUnsafeReason::IdentityChanged,
                        PolicyViolationStage::PreBinding,
                    ));
                }
                report.max_generation_id = Some(
                    report
                        .max_generation_id
                        .unwrap_or_default()
                        .max(generation_id),
                );
            }
            Ok(report)
        }
    }

    pub fn create(root: &VerifiedProjectRoot, run_id: i64) -> Result<Self, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = (root, run_id);
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            validate_run_id(run_id)?;
            let owner = root.directory.try_clone().map_err(|_| temp_error())?;
            let service =
                open_or_create_private_temp_container(&owner, OsStr::new(PRIVATE_TEMP_ROOT))?;
            let tmp = open_or_create_directory(&service, OsStr::new(PRIVATE_TEMP_DIR))?;
            let name = OsString::from(run_id.to_string());
            mkdirat_private(&tmp, &name)?;
            let directory = open_directory_nofollow(&tmp, &name).map_err(map_temp_io)?;
            validate_private_directory(&directory)?;
            directory.sync_all().map_err(|_| temp_error())?;
            tmp.sync_all().map_err(|_| temp_error())?;
            let identity = directory_identity(&directory)?;
            let path = root
                .anchor
                .canonical_path
                .join(PRIVATE_TEMP_ROOT)
                .join(PRIVATE_TEMP_DIR)
                .join(&name);
            Ok(Self {
                name,
                path,
                parent: tmp,
                directory,
                identity,
                #[cfg(target_os = "linux")]
                decision_output_anchor: Mutex::new(None),
            })
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn prepare_decision_schema(
        &self,
        schema: &[u8],
    ) -> Result<(), PolicyViolation> {
        self.prepare_named_output("decision-schema.json", "decision.json", schema)
    }

    pub(crate) fn prepare_health_diagnosis_schema(
        &self,
        schema: &[u8],
    ) -> Result<(), PolicyViolation> {
        self.prepare_named_output(
            "health-diagnosis-schema.json",
            "health-diagnosis.json",
            schema,
        )
    }

    fn prepare_named_output(
        &self,
        schema_name: &str,
        output_name: &str,
        schema: &[u8],
    ) -> Result<(), PolicyViolation> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (schema_name, output_name, schema);
            Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ))
        }
        #[cfg(target_os = "linux")]
        {
            self.revalidate_current()?;
            drop(create_private_decision_file(
                &self.directory,
                OsStr::new(schema_name),
                schema,
            )?);
            let output = create_private_decision_file(
                &self.directory,
                OsStr::new(output_name),
                &[],
            )?;
            *self
                .decision_output_anchor
                .lock()
                .map_err(|_| temp_error())? = Some(output);
            self.directory.sync_all().map_err(|_| temp_error())?;
            self.revalidate_current()
        }
    }

    pub(crate) fn read_decision_output(&self) -> Result<Vec<u8>, PolicyViolation> {
        self.read_named_output("decision.json")
    }

    pub(crate) fn read_health_diagnosis_output(&self) -> Result<Vec<u8>, PolicyViolation> {
        self.read_named_output("health-diagnosis.json")
    }

    fn read_named_output(&self, output_name: &str) -> Result<Vec<u8>, PolicyViolation> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = output_name;
            Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::Finalized,
            ))
        }
        #[cfg(target_os = "linux")]
        {
            self.revalidate_current()?;
            let anchor = self
                .decision_output_anchor
                .lock()
                .map_err(|_| temp_error())?;
            let anchor = anchor
                .as_ref()
                .ok_or_else(temp_error)?;
            let expected = anchor.identity;
            let name = OsStr::new(output_name);
            let parent_mount = directory_mount_identity_at(
                &self.directory,
                PolicyViolationStage::Finalized,
            )?;
            if entry_mount_identity_at(&self.directory, name, PolicyViolationStage::Finalized)?
                != parent_mount
            {
                return Err(temp_violation_at(
                    TempUnsafeReason::MountBoundary,
                    PolicyViolationStage::Finalized,
                ));
            }
            let before = validate_decision_output_file(&anchor.file, expected, parent_mount)?;
            let size = usize::try_from(before.size).map_err(|_| {
                temp_violation_at(TempUnsafeReason::ByteLimit, PolicyViolationStage::Finalized)
            })?;
            let bytes = read_decision_output_file(&anchor.file, size)?;
            let after = validate_decision_output_file(&anchor.file, expected, parent_mount)?;
            if before != after {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::Finalized,
                ));
            }
            let visible =
                artifact_entry_metadata_at(&self.directory, OsStr::new(output_name))?;
            if visible.identity != expected
                || visible.mount_identity != parent_mount
                || visible.owner != unsafe { libc::geteuid() as u32 }
                || visible.mode & 0o7777 != 0o600
                || visible.size != before.size
            {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::Finalized,
                ));
            }
            self.revalidate_current()?;
            Ok(bytes)
        }
    }

    /// Clone and revalidate the retained directory capability for target use.
    pub(crate) fn verified_target(&self) -> Result<VerifiedPrivateTemp, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ))
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let directory = self.directory.try_clone().map_err(|_| temp_error())?;
            let metadata = directory.metadata().map_err(|_| temp_error())?;
            let identity = verified_private_temp_identity(&metadata, self.identity)?;
            let run_id = self
                .name
                .to_str()
                .and_then(|name| name.parse::<i64>().ok())
                .ok_or_else(temp_error)?;
            Ok(VerifiedPrivateTemp {
                directory,
                identity,
                run_id,
            })
        }
    }

    pub fn cleanup_contents_before(
        &mut self,
        deadline: Option<Instant>,
    ) -> Result<TempCleanupReport, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = deadline;
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            check_cleanup_deadline(deadline)?;
            validate_private_directory(&self.directory)?;
            if directory_identity(&self.directory)? != self.identity {
                return Err(temp_violation(TempUnsafeReason::IdentityChanged));
            }
            let root_mount = directory_mount_identity_at(
                &self.directory,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            let mut state = AuditState::default();
            let entries = audit_directory(
                &self.directory,
                0,
                &mut state,
                deadline,
                PolicyViolationStage::RunBoundPreMarker,
                root_mount,
                #[cfg(all(test, unix))]
                None,
            )?;
            let mut report = TempCleanupReport {
                entries_removed: 0,
                allocated_bytes_reclaimed: 0,
            };
            remove_audited_entries(
                &self.directory,
                &entries,
                &mut report,
                deadline,
                root_mount,
            )?;
            finish_cleanup_before_success(
                &self.directory,
                deadline,
                #[cfg(all(test, unix))]
                None,
            )?;
            Ok(report)
        }
    }

    #[cfg(all(test, unix))]
    fn cleanup_contents_before_with_test_hook<F: FnOnce()>(
        &mut self,
        deadline: Option<Instant>,
        audit_hook: F,
    ) -> Result<TempCleanupReport, PolicyViolation> {
        validate_private_directory(&self.directory)?;
        if directory_identity(&self.directory)? != self.identity {
            return Err(temp_violation(TempUnsafeReason::IdentityChanged));
        }
        let root_mount = directory_mount_identity_at(
            &self.directory,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let mut state = AuditState::default();
        let entries = audit_directory(
            &self.directory,
            0,
            &mut state,
            deadline,
            PolicyViolationStage::RunBoundPreMarker,
            root_mount,
            None,
        )?;
        audit_hook();
        let mut report = TempCleanupReport {
            entries_removed: 0,
            allocated_bytes_reclaimed: 0,
        };
        let mut test_state = CleanupTestState::default();
        remove_audited_entries_impl(
            &self.directory,
            &entries,
            &mut report,
            deadline,
            root_mount,
            Some(&mut test_state),
            None,
        )?;
        finish_cleanup_before_success(
            &self.directory,
            deadline,
            Some(&mut test_state),
        )?;
        Ok(report)
    }

    #[cfg(all(test, unix))]
    fn cleanup_contents_before_with_test_state(
        &mut self,
        deadline: Option<Instant>,
        test_state: &mut CleanupTestState,
    ) -> Result<TempCleanupReport, PolicyViolation> {
        validate_private_directory(&self.directory)?;
        if directory_identity(&self.directory)? != self.identity {
            return Err(temp_violation(TempUnsafeReason::IdentityChanged));
        }
        let root_mount = directory_mount_identity_at(
            &self.directory,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let mut state = AuditState::default();
        let entries = audit_directory(
            &self.directory,
            0,
            &mut state,
            deadline,
            PolicyViolationStage::RunBoundPreMarker,
            root_mount,
            None,
        )?;
        let mut report = TempCleanupReport {
            entries_removed: 0,
            allocated_bytes_reclaimed: 0,
        };
        remove_audited_entries_impl(
            &self.directory,
            &entries,
            &mut report,
            deadline,
            root_mount,
            Some(&mut *test_state),
            None,
        )?;
        finish_cleanup_before_success(
            &self.directory,
            deadline,
            Some(&mut *test_state),
        )?;
        Ok(report)
    }

    #[cfg(all(test, unix))]
    fn cleanup_contents_before_with_mount_test_state(
        &mut self,
        deadline: Option<Instant>,
        mount_test_state: &mut MountBoundaryTestState,
    ) -> Result<TempCleanupReport, PolicyViolation> {
        validate_private_directory(&self.directory)?;
        if directory_identity(&self.directory)? != self.identity {
            return Err(temp_violation(TempUnsafeReason::IdentityChanged));
        }
        let root_mount = directory_mount_identity_at(
            &self.directory,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let mut state = AuditState::default();
        let entries = audit_directory(
            &self.directory,
            0,
            &mut state,
            deadline,
            PolicyViolationStage::RunBoundPreMarker,
            root_mount,
            Some(&mut *mount_test_state),
        )?;
        let mut report = TempCleanupReport {
            entries_removed: 0,
            allocated_bytes_reclaimed: 0,
        };
        let mut cleanup_test_state = CleanupTestState::default();
        remove_audited_entries_impl(
            &self.directory,
            &entries,
            &mut report,
            deadline,
            root_mount,
            Some(&mut cleanup_test_state),
            Some(&mut *mount_test_state),
        )?;
        finish_cleanup_before_success(
            &self.directory,
            deadline,
            Some(&mut cleanup_test_state),
        )?;
        Ok(report)
    }

    pub fn inspect_capacity(
        root: &VerifiedProjectRoot,
    ) -> Result<TempInventoryReport, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = root;
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::PreBinding,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let root_mount = directory_mount_identity_at(
                &root.directory,
                PolicyViolationStage::PreBinding,
            )?;
            let service = match open_optional_directory_on_mount(
                &root.directory,
                OsStr::new(PRIVATE_TEMP_ROOT),
                root_mount,
                PolicyViolationStage::PreBinding,
            )? {
                Some(directory) => {
                    validate_private_temp_container_at(
                        &directory,
                        PolicyViolationStage::PreBinding,
                    )?;
                    directory
                }
                None => {
                    return Ok(TempInventoryReport {
                        generations: 0,
                        retained_nonempty_generations: 0,
                        retained_allocated_bytes: 0,
                        max_generation_id: None,
                    });
                }
            };
            let tmp = match open_optional_directory_on_mount(
                &service,
                OsStr::new(PRIVATE_TEMP_DIR),
                root_mount,
                PolicyViolationStage::PreBinding,
            )? {
                Some(directory) => {
                    validate_private_directory_at(&directory, PolicyViolationStage::PreBinding)?;
                    directory
                }
                None => {
                    return Ok(TempInventoryReport {
                        generations: 0,
                        retained_nonempty_generations: 0,
                        retained_allocated_bytes: 0,
                        max_generation_id: None,
                    });
                }
            };
            let entries = directory_entries(
                &tmp,
                MAX_PRIVATE_TEMP_GENERATIONS,
                TempUnsafeReason::GenerationLimit,
                PolicyViolationStage::PreBinding,
                None,
                None,
                #[cfg(all(test, unix))]
                None,
            )?;
            let mut state = AuditState::default();
            let mut report = TempInventoryReport {
                generations: 0,
                retained_nonempty_generations: 0,
                retained_allocated_bytes: 0,
                max_generation_id: None,
            };
            for entry in entries {
                if entry.mount_identity != root_mount {
                    return Err(temp_violation_at(
                        TempUnsafeReason::MountBoundary,
                        PolicyViolationStage::PreBinding,
                    ));
                }
                let name = entry.name.to_str().ok_or_else(|| {
                    temp_violation_at(TempUnsafeReason::InvalidEntry, PolicyViolationStage::PreBinding)
                })?;
                let generation_id = name
                    .parse::<i64>()
                    .ok()
                    .filter(|value| *value > 0 && *value <= MAX_PRIVATE_TEMP_RUN_ID)
                    .filter(|value| name == value.to_string());
                if generation_id.is_none() {
                    return Err(temp_violation_at(
                        TempUnsafeReason::InvalidEntry,
                        PolicyViolationStage::PreBinding,
                    ));
                }
                if !matches!(entry.kind, AuditedEntryKind::Directory) {
                    return Err(temp_violation_at(
                        TempUnsafeReason::InvalidEntry,
                        PolicyViolationStage::PreBinding,
                    ));
                }
                let generation = open_directory_on_mount(
                    &tmp,
                    &entry.name,
                    root_mount,
                    PolicyViolationStage::PreBinding,
                )?;
                validate_private_directory_at(&generation, PolicyViolationStage::PreBinding)?;
                if directory_identity_at(&generation, PolicyViolationStage::PreBinding)?
                    != entry.identity
                {
                    return Err(temp_violation_at(
                        TempUnsafeReason::IdentityChanged,
                        PolicyViolationStage::PreBinding,
                    ));
                }
                let children = audit_directory(
                    &generation,
                    0,
                    &mut state,
                    None,
                    PolicyViolationStage::PreBinding,
                    root_mount,
                    #[cfg(all(test, unix))]
                    None,
                )
                .map_err(|error| stage_violation(error, PolicyViolationStage::PreBinding))?;
                report.generations += 1;
                report.max_generation_id = Some(
                    report
                        .max_generation_id
                        .unwrap_or_default()
                        .max(generation_id.expect("validated generation ID")),
                );
                let allocated = children
                    .iter()
                    .try_fold(0_u64, |total, child| {
                        total
                            .checked_add(
                                child.allocated_total(PolicyViolationStage::PreBinding)?,
                            )
                            .ok_or_else(|| {
                                temp_violation_at(
                                    TempUnsafeReason::ByteLimit,
                                    PolicyViolationStage::PreBinding,
                                )
                            })
                    })?;
                report.retained_allocated_bytes = report
                    .retained_allocated_bytes
                    .checked_add(allocated)
                    .ok_or_else(|| {
                        temp_violation_at(
                            TempUnsafeReason::ByteLimit,
                            PolicyViolationStage::PreBinding,
                        )
                    })?;
                if !children.is_empty() {
                    return Err(temp_violation_at(
                        TempUnsafeReason::InvalidEntry,
                        PolicyViolationStage::PreBinding,
                    ));
                }
            }
            Ok(report)
        }
    }

    /// Prove that the retained descriptor still names the exact owner-only
    /// generation visible under the fixed private-temp parent.
    pub fn revalidate_current(&self) -> Result<(), PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ))
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            validate_private_directory(&self.directory)?;
            if directory_identity(&self.directory)? != self.identity {
                return Err(temp_error());
            }
            let current = open_directory_nofollow(&self.parent, &self.name).map_err(map_temp_io)?;
            validate_private_directory(&current)?;
            if directory_identity(&current)? != self.identity {
                return Err(temp_error());
            }
            Ok(())
        }
    }

    /// Cleanup is intentionally unsupported.  Retaining the complete run
    /// directory is the only portable way to ensure a concurrent pathname
    /// replacement can never be mutated by this handle.  Callers receive a
    /// bounded policy error while the original generation remains available as
    /// a diagnostic/runtime artifact.
    pub fn cleanup(&mut self) -> Result<(), PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let _ = self;
            Err(temp_error())
        }
    }
}

impl Drop for PrivateRunTemp {
    fn drop(&mut self) {}
}

#[cfg(target_os = "linux")]
struct DecisionOutputAnchor {
    file: File,
    identity: (u64, u64),
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, PartialEq, Eq)]
struct DecisionFileSnapshot {
    identity: (u64, u64),
    owner: u32,
    mode: u32,
    links: u64,
    size: u64,
}

#[cfg(target_os = "linux")]
fn create_private_decision_file(
    directory: &File,
    name: &OsStr,
    contents: &[u8],
) -> Result<DecisionOutputAnchor, PolicyViolation> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::{ffi::OsStrExt, fs::MetadataExt};

    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| temp_violation(TempUnsafeReason::InvalidEntry))?;
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDWR
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW,
            0o600,
        )
    };
    if fd < 0 {
        return Err(temp_violation(TempUnsafeReason::InvalidEntry));
    }
    let mut file = unsafe { File::from_raw_fd(fd) };
    file.write_all(contents)
        .and_then(|()| file.sync_all())
        .map_err(|_| temp_error())?;
    let metadata = file.metadata().map_err(|_| temp_error())?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() as u32 }
        || metadata.mode() & 0o7777 != 0o600
        || metadata.nlink() != 1
    {
        return Err(temp_violation(TempUnsafeReason::InvalidEntry));
    }
    Ok(DecisionOutputAnchor {
        file,
        identity: (metadata.dev(), metadata.ino()),
    })
}

#[cfg(target_os = "linux")]
fn read_decision_output_file(file: &File, size: usize) -> Result<Vec<u8>, PolicyViolation> {
    use std::os::unix::fs::FileExt;

    let mut bytes = vec![0; size];
    let mut offset = 0;
    while offset < size {
        let read = file
            .read_at(&mut bytes[offset..], offset as u64)
            .map_err(|_| {
                temp_violation_at(TempUnsafeReason::IoFailure, PolicyViolationStage::Finalized)
            })?;
        if read == 0 {
            return Err(temp_violation_at(
                TempUnsafeReason::IdentityChanged,
                PolicyViolationStage::Finalized,
            ));
        }
        offset = offset.checked_add(read).ok_or_else(|| {
            temp_violation_at(TempUnsafeReason::ByteLimit, PolicyViolationStage::Finalized)
        })?;
    }
    Ok(bytes)
}

#[cfg(target_os = "linux")]
fn validate_decision_output_file(
    file: &File,
    expected: (u64, u64),
    expected_mount: MountIdentity,
) -> Result<DecisionFileSnapshot, PolicyViolation> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata().map_err(|_| {
        temp_violation_at(TempUnsafeReason::IoFailure, PolicyViolationStage::Finalized)
    })?;
    let snapshot = DecisionFileSnapshot {
        identity: (metadata.dev(), metadata.ino()),
        owner: metadata.uid(),
        mode: metadata.mode() & 0o7777,
        links: metadata.nlink(),
        size: metadata.size(),
    };
    if !metadata.is_file()
        || snapshot.identity != expected
        || snapshot.owner != unsafe { libc::geteuid() as u32 }
        || snapshot.mode != 0o600
        || snapshot.links != 1
        || snapshot.size > MAX_DECISION_BYTES as u64
        || directory_mount_identity_at(file, PolicyViolationStage::Finalized)? != expected_mount
    {
        return Err(temp_violation_at(
            TempUnsafeReason::InvalidEntry,
            PolicyViolationStage::Finalized,
        ));
    }
    Ok(snapshot)
}

#[cfg(unix)]
#[derive(Debug)]
struct AuditedEntry {
    name: OsString,
    identity: (u64, u64),
    mount_identity: MountIdentity,
    kind: AuditedEntryKind,
    allocated_bytes: u64,
    children: Vec<AuditedEntry>,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuditedEntryKind {
    Directory,
    Leaf,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MountIdentity([u64; 2]);

#[cfg(target_os = "linux")]
const LINUX_STATX_BASIC_STATS: u32 = 0x0000_07ff;
#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
const LINUX_STATX_MNT_ID: u32 = 0x0000_1000;

/// The stable 256-byte Linux kernel `struct statx` ABI subset used here.
/// Keeping this local avoids the libc wrapper/type availability boundary on
/// older glibc and on libc crate musl/uClibc configurations.
#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
#[repr(C, align(8))]
struct LinuxStatxBuffer {
    stx_mask: u32,
    before_mnt_id: [u8; 140],
    stx_mnt_id: u64,
    remaining: [u8; 104],
}

#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
const _: () = {
    assert!(std::mem::size_of::<LinuxStatxBuffer>() == 256);
    assert!(std::mem::align_of::<LinuxStatxBuffer>() == 8);
    assert!(std::mem::offset_of!(LinuxStatxBuffer, stx_mask) == 0);
    assert!(std::mem::offset_of!(LinuxStatxBuffer, stx_mnt_id) == 144);
};

#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LinuxSyscallFailure {
    Retry,
    Missing,
    MountBoundary,
    UnsupportedPlatform,
    IoFailure,
}

#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
fn classify_linux_openat2_errno(errno: libc::c_int) -> LinuxSyscallFailure {
    match errno {
        libc::EINTR => LinuxSyscallFailure::Retry,
        libc::ENOENT => LinuxSyscallFailure::Missing,
        libc::EXDEV | libc::ELOOP => LinuxSyscallFailure::MountBoundary,
        libc::ENOSYS | libc::EINVAL | libc::E2BIG => {
            LinuxSyscallFailure::UnsupportedPlatform
        }
        _ => LinuxSyscallFailure::IoFailure,
    }
}

#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
fn classify_linux_statx_errno(errno: libc::c_int) -> LinuxSyscallFailure {
    match errno {
        libc::EINTR => LinuxSyscallFailure::Retry,
        libc::ENOSYS => LinuxSyscallFailure::UnsupportedPlatform,
        _ => LinuxSyscallFailure::IoFailure,
    }
}

#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
fn parse_linux_statx_mount_identity(
    statx: &LinuxStatxBuffer,
) -> Result<MountIdentity, LinuxSyscallFailure> {
    if statx.stx_mask & LINUX_STATX_MNT_ID == 0 {
        return Err(LinuxSyscallFailure::UnsupportedPlatform);
    }
    Ok(MountIdentity([statx.stx_mnt_id, 0]))
}

#[cfg(unix)]
impl AuditedEntry {
    fn allocated_total(&self, stage: PolicyViolationStage) -> Result<u64, PolicyViolation> {
        self.children.iter().try_fold(self.allocated_bytes, |total, child| {
            total
                .checked_add(child.allocated_total(stage)?)
                .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))
        })
    }
}

#[cfg(unix)]
struct AuditState {
    entries: usize,
    allocated_bytes: u64,
    max_depth: usize,
    max_entries: usize,
    max_allocated_bytes: u64,
    #[cfg(test)]
    test_deadline_after_entries: Option<usize>,
}

#[cfg(unix)]
impl Default for AuditState {
    fn default() -> Self {
        Self {
            entries: 0,
            allocated_bytes: 0,
            max_depth: MAX_PRIVATE_TEMP_CLEANUP_DEPTH,
            max_entries: MAX_PRIVATE_TEMP_CLEANUP_ENTRIES,
            max_allocated_bytes: MAX_PRIVATE_TEMP_ALLOCATED_BYTES,
            #[cfg(test)]
            test_deadline_after_entries: None,
        }
    }
}

#[cfg(all(unix, test))]
impl AuditState {
    fn with_limits(max_depth: usize, max_entries: usize, max_allocated_bytes: u64) -> Self {
        Self {
            entries: 0,
            allocated_bytes: 0,
            max_depth,
            max_entries,
            max_allocated_bytes,
            test_deadline_after_entries: None,
        }
    }
}

#[cfg(all(unix, test))]
#[derive(Default)]
struct CleanupTestState {
    expire_after_recursion: bool,
    expire_before_sync: bool,
    deadline_crossed: bool,
    fail_after_first_unlink: bool,
    fail_sync: bool,
    sync_attempts: usize,
    sync_completed: bool,
}

#[cfg(all(unix, test))]
#[derive(Default)]
struct MountBoundaryTestState {
    mismatch_at_directory_open: bool,
    mismatch_before_unlink: bool,
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OptionalOpenStep {
    EntryMountPrecheck,
    SecureOpen,
    DescriptorMountRecheck,
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
#[derive(Default)]
struct OptionalOpenTestState {
    steps: Vec<OptionalOpenStep>,
}

#[cfg(all(unix, test))]
struct DirectoryEntriesTestState {
    fail_after_metadata: Option<usize>,
    close_counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(unix)]
fn check_audit_deadline(
    deadline: Option<Instant>,
    state: Option<&AuditState>,
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    #[cfg(not(test))]
    let _ = state;
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    #[cfg(test)]
    if state.is_some_and(|state| {
        state
            .test_deadline_after_entries
            .is_some_and(|limit| state.entries >= limit)
    }) {
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    Ok(())
}

#[cfg(unix)]
fn audit_directory(
    directory: &File,
    depth: usize,
    state: &mut AuditState,
    deadline: Option<Instant>,
    stage: PolicyViolationStage,
    root_mount: MountIdentity,
    #[cfg(all(test, unix))] mut mount_test_state: Option<&mut MountBoundaryTestState>,
) -> Result<Vec<AuditedEntry>, PolicyViolation> {
    check_audit_deadline(deadline, Some(state), stage)?;
    if depth > state.max_depth {
        return Err(temp_violation_at(TempUnsafeReason::DepthLimit, stage));
    }
    let mut entries = Vec::new();
    let remaining = state.max_entries.saturating_sub(state.entries);
    for listed in directory_entries(
        directory,
        remaining,
        TempUnsafeReason::EntryLimit,
        stage,
        deadline,
        Some(state),
        #[cfg(all(test, unix))]
        None,
    )? {
        check_audit_deadline(deadline, Some(state), stage)?;
        if listed.mount_identity != root_mount {
            return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
        }
        let children = if listed.kind == AuditedEntryKind::Directory {
            if depth == state.max_depth {
                return Err(temp_violation_at(TempUnsafeReason::DepthLimit, stage));
            }
            #[cfg(all(test, unix))]
            if mount_test_state.as_ref().is_some_and(|test_state| {
                test_state.mismatch_at_directory_open
            }) {
                if let Some(test_state) = mount_test_state.as_deref_mut() {
                    test_state.mismatch_at_directory_open = false;
                }
                return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
            }
            let child = open_directory_on_mount(directory, &listed.name, root_mount, stage)?;
            validate_private_directory_at(&child, stage)?;
            if directory_identity_at(&child, stage)? != listed.identity {
                return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
            }
            audit_directory(
                &child,
                depth + 1,
                state,
                deadline,
                stage,
                root_mount,
                #[cfg(all(test, unix))]
                mount_test_state.as_deref_mut(),
            )?
        } else {
            Vec::new()
        };
        entries.push(AuditedEntry {
            name: listed.name,
            identity: listed.identity,
            mount_identity: listed.mount_identity,
            kind: listed.kind,
            allocated_bytes: listed.allocated_bytes,
            children,
        });
    }
    Ok(entries)
}

#[cfg(unix)]
fn remove_audited_entries(
    directory: &File,
    entries: &[AuditedEntry],
    report: &mut TempCleanupReport,
    deadline: Option<Instant>,
    root_mount: MountIdentity,
) -> Result<(), PolicyViolation> {
    #[cfg(all(test, unix))]
    let mut test_state = CleanupTestState::default();
    remove_audited_entries_impl(
        directory,
        entries,
        report,
        deadline,
        root_mount,
        #[cfg(all(test, unix))]
        Some(&mut test_state),
        #[cfg(all(test, unix))]
        None,
    )
}

#[cfg(unix)]
fn remove_audited_entries_impl(
    directory: &File,
    entries: &[AuditedEntry],
    report: &mut TempCleanupReport,
    deadline: Option<Instant>,
    root_mount: MountIdentity,
    #[cfg(all(test, unix))] mut test_state: Option<&mut CleanupTestState>,
    #[cfg(all(test, unix))] mut mount_test_state: Option<&mut MountBoundaryTestState>,
) -> Result<(), PolicyViolation> {
    let mut modified = false;
    for entry in entries {
        if let Err(error) = remove_audited_entry(
            directory,
            entry,
            report,
            deadline,
            &mut modified,
            root_mount,
            #[cfg(all(test, unix))]
            test_state.as_deref_mut(),
            #[cfg(all(test, unix))]
            mount_test_state.as_deref_mut(),
        ) {
            if modified {
                if let Err(sync_error) = sync_directory(
                    directory,
                    #[cfg(all(test, unix))]
                    test_state.as_deref_mut(),
                ) {
                    return Err(sync_error);
                }
            }
            return Err(error);
        }
    }
    if modified {
        sync_directory(
            directory,
            #[cfg(all(test, unix))]
            test_state.as_deref_mut(),
        )?;
        check_cleanup_deadline_after_sync(
            deadline,
            #[cfg(all(test, unix))]
            test_state.as_deref_mut(),
        )?;
    }
    Ok(())
}

#[cfg(unix)]
fn finish_cleanup_before_success(
    directory: &File,
    deadline: Option<Instant>,
    #[cfg(all(test, unix))] mut test_state: Option<&mut CleanupTestState>,
) -> Result<(), PolicyViolation> {
    check_cleanup_deadline(deadline)?;
    sync_directory(
        directory,
        #[cfg(all(test, unix))]
        test_state.as_deref_mut(),
    )?;
    check_cleanup_deadline_after_sync(
        deadline,
        #[cfg(all(test, unix))]
        test_state.as_deref_mut(),
    )
}

#[cfg(unix)]
fn remove_audited_entry(
    directory: &File,
    entry: &AuditedEntry,
    report: &mut TempCleanupReport,
    deadline: Option<Instant>,
    modified: &mut bool,
    root_mount: MountIdentity,
    #[cfg(all(test, unix))] mut test_state: Option<&mut CleanupTestState>,
    #[cfg(all(test, unix))] mut mount_test_state: Option<&mut MountBoundaryTestState>,
) -> Result<(), PolicyViolation> {
    check_cleanup_deadline(deadline)?;
    let current = entry_metadata(directory, &entry.name)?;
    if entry.mount_identity != root_mount || current.mount_identity != root_mount {
        return Err(temp_violation(TempUnsafeReason::MountBoundary));
    }
    if current.identity != entry.identity
        || current.kind != entry.kind
        || current.mount_identity != entry.mount_identity
    {
        return Err(temp_violation(TempUnsafeReason::IdentityChanged));
    }
    if entry.kind == AuditedEntryKind::Directory {
        check_cleanup_deadline(deadline)?;
        let child = open_directory_on_mount(
            directory,
            &entry.name,
            root_mount,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        validate_private_directory(&child)?;
        if directory_identity(&child)? != entry.identity {
            return Err(temp_violation(TempUnsafeReason::IdentityChanged));
        }
        remove_audited_entries_impl(
            &child,
            &entry.children,
            report,
            deadline,
            root_mount,
            #[cfg(all(test, unix))]
            test_state.as_deref_mut(),
            #[cfg(all(test, unix))]
            mount_test_state.as_deref_mut(),
        )?;
        check_cleanup_deadline_after_recursion(
            deadline,
            #[cfg(all(test, unix))]
            test_state.as_deref_mut(),
        )?;
        check_cleanup_deadline(deadline)?;
        let current = entry_metadata(directory, &entry.name)?;
        if entry.mount_identity != root_mount || current.mount_identity != root_mount {
            return Err(temp_violation(TempUnsafeReason::MountBoundary));
        }
        if current.identity != entry.identity
            || current.kind != AuditedEntryKind::Directory
            || current.mount_identity != entry.mount_identity
        {
            return Err(temp_violation(TempUnsafeReason::IdentityChanged));
        }
        #[cfg(all(test, unix))]
        if mount_test_state.as_ref().is_some_and(|test_state| {
            test_state.mismatch_before_unlink
        }) {
            if let Some(test_state) = mount_test_state.as_deref_mut() {
                test_state.mismatch_before_unlink = false;
            }
            return Err(temp_violation(TempUnsafeReason::MountBoundary));
        }
        check_cleanup_deadline(deadline)?;
        unlinkat(directory, &entry.name, true)?;
    } else {
        #[cfg(all(test, unix))]
        if mount_test_state.as_ref().is_some_and(|test_state| {
            test_state.mismatch_before_unlink
        }) {
            if let Some(test_state) = mount_test_state.as_deref_mut() {
                test_state.mismatch_before_unlink = false;
            }
            return Err(temp_violation(TempUnsafeReason::MountBoundary));
        }
        check_cleanup_deadline(deadline)?;
        unlinkat(directory, &entry.name, false)?;
    }
    *modified = true;
    #[cfg(all(unix, test))]
    if let Some(test_state) = test_state.as_deref_mut() {
        if test_state.fail_after_first_unlink {
            test_state.fail_after_first_unlink = false;
            return Err(temp_violation(TempUnsafeReason::IoFailure));
        }
    }
    report.entries_removed = report
        .entries_removed
        .checked_add(1)
        .ok_or_else(|| temp_violation(TempUnsafeReason::EntryLimit))?;
    report.allocated_bytes_reclaimed = report
        .allocated_bytes_reclaimed
        .checked_add(entry.allocated_bytes)
        .ok_or_else(|| temp_violation(TempUnsafeReason::ByteLimit))?;
    Ok(())
}

#[cfg(unix)]
fn check_cleanup_deadline(deadline: Option<Instant>) -> Result<(), PolicyViolation> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        Err(temp_violation(TempUnsafeReason::IoFailure))
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn check_cleanup_deadline_after_recursion(
    deadline: Option<Instant>,
    #[cfg(all(test, unix))] test_state: Option<&mut CleanupTestState>,
) -> Result<(), PolicyViolation> {
    #[cfg(all(unix, test))]
    if let Some(test_state) = test_state {
        if test_state.expire_after_recursion {
            test_state.expire_after_recursion = false;
            return Err(temp_violation(TempUnsafeReason::IoFailure));
        }
    }
    check_cleanup_deadline(deadline)
}

#[cfg(unix)]
fn sync_directory(
    directory: &File,
    #[cfg(all(test, unix))] mut test_state: Option<&mut CleanupTestState>,
) -> Result<(), PolicyViolation> {
    #[cfg(all(unix, test))]
    if let Some(test_state) = test_state.as_deref_mut() {
        test_state.sync_attempts += 1;
        if test_state.expire_before_sync {
            test_state.expire_before_sync = false;
            test_state.deadline_crossed = true;
        }
        if test_state.fail_sync {
            return Err(temp_violation(TempUnsafeReason::IoFailure));
        }
    }
    let result = directory
        .sync_all()
        .map_err(|_| temp_violation(TempUnsafeReason::IoFailure));
    #[cfg(all(unix, test))]
    if result.is_ok() {
        if let Some(test_state) = test_state.as_deref_mut() {
            test_state.sync_completed = true;
        }
    }
    result
}

#[cfg(unix)]
fn check_cleanup_deadline_after_sync(
    deadline: Option<Instant>,
    #[cfg(all(test, unix))] test_state: Option<&mut CleanupTestState>,
) -> Result<(), PolicyViolation> {
    #[cfg(all(unix, test))]
    if let Some(test_state) = test_state {
        if test_state.deadline_crossed {
            test_state.deadline_crossed = false;
            return Err(temp_violation(TempUnsafeReason::IoFailure));
        }
    }
    check_cleanup_deadline(deadline)
}

#[cfg(unix)]
#[derive(Debug)]
struct ListedEntry {
    name: OsString,
    identity: (u64, u64),
    mount_identity: MountIdentity,
    kind: AuditedEntryKind,
    allocated_bytes: u64,
}

#[cfg(unix)]
struct OwnedDirectoryStream {
    stream: *mut libc::DIR,
    #[cfg(all(test, unix))]
    close_counter: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
}

#[cfg(unix)]
impl OwnedDirectoryStream {
    fn open(
        fd: libc::c_int,
        stage: PolicyViolationStage,
        #[cfg(all(test, unix))]
        close_counter: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    ) -> Result<Self, PolicyViolation> {
        let stream = unsafe { libc::fdopendir(fd) };
        if stream.is_null() {
            unsafe { libc::close(fd) };
            return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
        }
        Ok(Self {
            stream,
            #[cfg(all(test, unix))]
            close_counter,
        })
    }

    fn as_ptr(&self) -> *mut libc::DIR {
        self.stream
    }
}

#[cfg(unix)]
impl Drop for OwnedDirectoryStream {
    fn drop(&mut self) {
        if self.stream.is_null() {
            return;
        }
        unsafe { libc::closedir(self.stream) };
        self.stream = std::ptr::null_mut();
        #[cfg(all(test, unix))]
        if let Some(counter) = self.close_counter.as_ref() {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct ArtifactHintScan {
    entries: usize,
    hints: Vec<DecisionArtifactHint>,
    max_hints: usize,
    max_depth: usize,
    max_field_bytes: usize,
    root_owner: u32,
    root_mount: MountIdentity,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct ArtifactListedEntry {
    name: OsString,
    identity: (u64, u64),
    mount_identity: MountIdentity,
    owner: u32,
    mode: libc::mode_t,
    size: u64,
    mtime: i64,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct ArtifactEntryMetadata {
    identity: (u64, u64),
    mount_identity: MountIdentity,
    owner: u32,
    mode: libc::mode_t,
    size: u64,
    mtime: i64,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn scan_artifact_hints(
    directory: &File,
    prefix: &str,
    directory_depth: usize,
    state: &mut ArtifactHintScan,
) -> Result<(), PolicyViolation> {
    let mut entries = artifact_directory_entries(directory, state)?;
    entries.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    for entry in entries {
        if entry.mount_identity != state.root_mount {
            return Err(temp_violation(TempUnsafeReason::MountBoundary));
        }
        if entry.owner != state.root_owner {
            continue;
        }
        let Some(name) = entry.name.to_str() else {
            continue;
        };
        let entry_depth = directory_depth
            .checked_add(1)
            .ok_or_else(|| temp_violation(TempUnsafeReason::DepthLimit))?;
        if entry_depth > state.max_depth {
            continue;
        }
        let separator_bytes = usize::from(!prefix.is_empty());
        let path_bytes = prefix
            .len()
            .checked_add(separator_bytes)
            .and_then(|bytes| bytes.checked_add(name.len()))
            .ok_or_else(|| temp_violation(TempUnsafeReason::ByteLimit))?;
        if path_bytes > state.max_field_bytes {
            continue;
        }
        let is_directory = entry.mode & libc::S_IFMT == libc::S_IFDIR;
        let is_regular = entry.mode & libc::S_IFMT == libc::S_IFREG;
        if is_directory {
            if entry_depth == state.max_depth {
                continue;
            }
            let child = open_directory_on_mount(
                directory,
                &entry.name,
                state.root_mount,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            if directory_identity_at(&child, PolicyViolationStage::RunBoundPreMarker)?
                != entry.identity
            {
                return Err(temp_violation(TempUnsafeReason::IdentityChanged));
            }
            let path = if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}/{name}")
            };
            scan_artifact_hints(&child, &path, entry_depth, state)?;
        } else if is_regular && state.hints.len() < state.max_hints {
            let path = if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}/{name}")
            };
            state.hints.push(DecisionArtifactHint {
                path,
                size: entry.size,
                mtime: entry.mtime,
            });
        }
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn artifact_directory_entries(
    directory: &File,
    state: &mut ArtifactHintScan,
) -> Result<Vec<ArtifactListedEntry>, PolicyViolation> {
    use std::{ffi::CStr, os::unix::ffi::OsStrExt};

    let duplicate = duplicate_directory_fd(directory, PolicyViolationStage::RunBoundPreMarker)?;
    if unsafe { libc::lseek(duplicate, 0, libc::SEEK_SET) } < 0 {
        unsafe { libc::close(duplicate) };
        return Err(temp_violation(TempUnsafeReason::IoFailure));
    }
    let stream = OwnedDirectoryStream::open(
        duplicate,
        PolicyViolationStage::RunBoundPreMarker,
        #[cfg(all(test, unix))]
        None,
    )?;
    let remaining = MAX_DECISION_ARTIFACT_SCAN_ENTRIES.saturating_sub(state.entries);
    let mut entries = Vec::with_capacity(remaining.min(64));
    loop {
        clear_errno(PolicyViolationStage::RunBoundPreMarker)?;
        let entry = unsafe { libc::readdir(stream.as_ptr()) };
        if entry.is_null() {
            let errno = io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if errno != 0 {
                return Err(temp_violation(TempUnsafeReason::IoFailure));
            }
            break;
        }
        let dirent = unsafe { &*entry };
        let name_bytes = unsafe { CStr::from_ptr(dirent.d_name.as_ptr()) }.to_bytes();
        if name_bytes == b"." || name_bytes == b".." || name_bytes.is_empty() {
            continue;
        }
        state.entries = state
            .entries
            .checked_add(1)
            .ok_or_else(|| temp_violation(TempUnsafeReason::EntryLimit))?;
        if state.entries > MAX_DECISION_ARTIFACT_SCAN_ENTRIES {
            return Err(temp_violation(TempUnsafeReason::EntryLimit));
        }
        if name_bytes.len() > state.max_field_bytes {
            continue;
        }
        let name = OsStr::from_bytes(name_bytes);
        let metadata = artifact_entry_metadata_at(directory, name)?;
        entries.push(ArtifactListedEntry {
            name: name.to_os_string(),
            identity: metadata.identity,
            mount_identity: metadata.mount_identity,
            owner: metadata.owner,
            mode: metadata.mode,
            size: metadata.size,
            mtime: metadata.mtime,
        });
    }
    Ok(entries)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn artifact_entry_metadata_at(
    directory: &File,
    name: &OsStr,
) -> Result<ArtifactEntryMetadata, PolicyViolation> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};

    let mount_identity = entry_mount_identity_at(
        directory,
        name,
        PolicyViolationStage::RunBoundPreMarker,
    )?;
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| temp_violation(TempUnsafeReason::InvalidEntry))?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(temp_violation(TempUnsafeReason::IoFailure));
    }
    let stat = unsafe { stat.assume_init() };
    Ok(ArtifactEntryMetadata {
        identity: (stat.st_dev as u64, stat.st_ino as u64),
        mount_identity,
        owner: stat.st_uid,
        mode: stat.st_mode,
        size: u64::try_from(stat.st_size).unwrap_or(0),
        mtime: stat.st_mtime,
    })
}

#[cfg(unix)]
fn directory_entries(
    directory: &File,
    max_entries: usize,
    overflow_reason: TempUnsafeReason,
    stage: PolicyViolationStage,
    deadline: Option<Instant>,
    mut audit_state: Option<&mut AuditState>,
    #[cfg(all(test, unix))] test_state: Option<&mut DirectoryEntriesTestState>,
) -> Result<Vec<ListedEntry>, PolicyViolation> {
    use std::ffi::CStr;
    use std::os::unix::ffi::OsStrExt;
    check_audit_deadline(deadline, audit_state.as_ref().map(|state| &**state), stage)?;
    let duplicate = duplicate_directory_fd(directory, stage)?;
    if let Err(error) = check_audit_deadline(
        deadline,
        audit_state.as_ref().map(|state| &**state),
        stage,
    ) {
        unsafe { libc::close(duplicate) };
        return Err(error);
    }
    if unsafe { libc::lseek(duplicate, 0, libc::SEEK_SET) } < 0 {
        unsafe { libc::close(duplicate) };
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    #[cfg(all(test, unix))]
    let close_counter = test_state
        .as_ref()
        .map(|state| std::sync::Arc::clone(&state.close_counter));
    let stream = OwnedDirectoryStream::open(
        duplicate,
        stage,
        #[cfg(all(test, unix))]
        close_counter,
    )?;
    let mut result = Vec::with_capacity(max_entries.min(64));
    let mut count = 0_usize;
    loop {
        if let Err(error) = check_audit_deadline(
            deadline,
            audit_state.as_ref().map(|state| &**state),
            stage,
        ) {
            return Err(error);
        }
        clear_errno(stage)?;
        let entry = unsafe { libc::readdir(stream.as_ptr()) };
        if entry.is_null() {
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if errno != 0 {
                return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
            }
            break;
        }
        let dirent = unsafe { &*entry };
        let name_bytes = unsafe { CStr::from_ptr(dirent.d_name.as_ptr()) }.to_bytes();
        if name_bytes == b"." || name_bytes == b".." || name_bytes.is_empty() {
            continue;
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| temp_violation_at(overflow_reason, stage))?;
        if count > max_entries {
            return Err(temp_violation_at(overflow_reason, stage));
        }
        let name = OsStr::from_bytes(name_bytes).to_os_string();
        if let Err(error) = check_audit_deadline(
            deadline,
            audit_state.as_ref().map(|state| &**state),
            stage,
        ) {
            return Err(error);
        }
        let listed = entry_metadata_at(directory, &name, stage)?;
        if let Err(error) = check_audit_deadline(
            deadline,
            audit_state.as_ref().map(|state| &**state),
            stage,
        ) {
            return Err(error);
        }
        #[cfg(all(test, unix))]
        if test_state.as_ref().is_some_and(|state| {
            state
                .fail_after_metadata
                .is_some_and(|limit| count >= limit)
        }) {
            return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
        }
        if let Some(state) = audit_state.as_mut() {
            let state = &mut **state;
            state.entries = state
                .entries
                .checked_add(1)
                .ok_or_else(|| temp_violation_at(TempUnsafeReason::EntryLimit, stage))?;
            if state.entries > state.max_entries {
                return Err(temp_violation_at(TempUnsafeReason::EntryLimit, stage));
            }
            state.allocated_bytes = state
                .allocated_bytes
                .checked_add(listed.allocated_bytes)
                .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
            if state.allocated_bytes > state.max_allocated_bytes {
                return Err(temp_violation_at(TempUnsafeReason::ByteLimit, stage));
            }
        }
        result.push(ListedEntry {
            name,
            identity: listed.identity,
            mount_identity: listed.mount_identity,
            kind: listed.kind,
            allocated_bytes: listed.allocated_bytes,
        });
    }
    Ok(result)
}

#[cfg(unix)]
fn duplicate_directory_fd(
    directory: &File,
    stage: PolicyViolationStage,
) -> Result<libc::c_int, PolicyViolation> {
    use std::os::fd::AsRawFd;
    let fd = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
    if fd < 0 {
        Err(temp_violation_at(TempUnsafeReason::IoFailure, stage))
    } else {
        Ok(fd)
    }
}

#[cfg(unix)]
fn clear_errno(stage: PolicyViolationStage) -> Result<(), PolicyViolation> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let _ = stage;
    #[cfg(target_os = "linux")]
    unsafe {
        *libc::__errno_location() = 0;
        return Ok(())
    }
    #[cfg(target_os = "macos")]
    unsafe {
        *libc::__error() = 0;
        return Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            stage,
        ))
    }
}

#[cfg(target_os = "linux")]
fn directory_mount_identity_at(
    directory: &File,
    stage: PolicyViolationStage,
) -> Result<MountIdentity, PolicyViolation> {
    use std::os::fd::AsRawFd;
    linux_mount_identity_at(directory.as_raw_fd(), OsStr::new(""), libc::AT_EMPTY_PATH, stage)
}

#[cfg(target_os = "linux")]
fn entry_mount_identity_at(
    directory: &File,
    name: &OsStr,
    stage: PolicyViolationStage,
) -> Result<MountIdentity, PolicyViolation> {
    use std::os::fd::AsRawFd;
    linux_mount_identity_at(
        directory.as_raw_fd(),
        name,
        libc::AT_SYMLINK_NOFOLLOW,
        stage,
    )
}

#[cfg(target_os = "linux")]
fn linux_mount_identity_at(
    directory_fd: libc::c_int,
    name: &OsStr,
    flags: libc::c_int,
    stage: PolicyViolationStage,
) -> Result<MountIdentity, PolicyViolation> {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| temp_violation_at(TempUnsafeReason::InvalidEntry, stage))?;
    let mut statx = std::mem::MaybeUninit::<LinuxStatxBuffer>::zeroed();
    loop {
        let result = unsafe {
            libc::syscall(
                libc::SYS_statx,
                directory_fd,
                name.as_ptr(),
                flags,
                LINUX_STATX_BASIC_STATS | LINUX_STATX_MNT_ID,
                statx.as_mut_ptr(),
            )
        };
        if result == 0 {
            break;
        }
        let errno = io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO);
        match classify_linux_statx_errno(errno) {
            LinuxSyscallFailure::Retry => continue,
            LinuxSyscallFailure::UnsupportedPlatform => {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::UnsupportedPlatform,
                    stage,
                ));
            }
            LinuxSyscallFailure::Missing
            | LinuxSyscallFailure::MountBoundary
            | LinuxSyscallFailure::IoFailure => {
                return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
            }
        }
    }
    let statx = unsafe { statx.assume_init() };
    match parse_linux_statx_mount_identity(&statx) {
        Ok(identity) => Ok(identity),
        Err(LinuxSyscallFailure::UnsupportedPlatform) => Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            stage,
        )),
        Err(_) => Err(temp_violation_at(TempUnsafeReason::IoFailure, stage)),
    }
}

#[cfg(target_os = "macos")]
fn directory_mount_identity_at(
    directory: &File,
    stage: PolicyViolationStage,
) -> Result<MountIdentity, PolicyViolation> {
    use std::os::fd::AsRawFd;
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::zeroed();
    if unsafe { libc::fstatfs(directory.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    let stat = unsafe { stat.assume_init() };
    Ok(macos_mount_identity(&stat.f_fsid))
}

#[cfg(target_os = "macos")]
fn entry_mount_identity_at(
    directory: &File,
    name: &OsStr,
    stage: PolicyViolationStage,
) -> Result<MountIdentity, PolicyViolation> {
    match macos_entry_mount_identity_io(directory, name) {
        Ok(identity) => Ok(identity),
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::ENOSYS) | Some(libc::ENOTSUP)
            ) => Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                stage,
            )),
        Err(_) => Err(temp_violation_at(TempUnsafeReason::IoFailure, stage)),
    }
}

#[cfg(target_os = "macos")]
fn macos_entry_mount_identity_io(
    directory: &File,
    name: &OsStr,
) -> io::Result<MountIdentity> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    #[repr(C)]
    struct FsidAttributeBuffer {
        length: u32,
        fsid: [i32; 2],
    }
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL"))?;
    let mut attributes = libc::attrlist {
        bitmapcount: libc::ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: libc::ATTR_CMN_FSID,
        volattr: 0,
        dirattr: 0,
        fileattr: 0,
        forkattr: 0,
    };
    let mut buffer = FsidAttributeBuffer {
        length: 0,
        fsid: [0; 2],
    };
    loop {
        let result = unsafe {
            libc::getattrlistat(
                directory.as_raw_fd(),
                name.as_ptr(),
                &mut attributes as *mut _ as *mut libc::c_void,
                &mut buffer as *mut _ as *mut libc::c_void,
                std::mem::size_of::<FsidAttributeBuffer>(),
                libc::FSOPT_NOFOLLOW as libc::c_ulong,
            )
        };
        if result == 0 {
            break;
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return Err(error);
    }
    if buffer.length < std::mem::size_of::<FsidAttributeBuffer>() as u32 {
        return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
    }
    Ok(MountIdentity([
        buffer.fsid[0] as u32 as u64,
        buffer.fsid[1] as u32 as u64,
    ]))
}

#[cfg(target_os = "macos")]
fn macos_mount_identity(fsid: &libc::fsid_t) -> MountIdentity {
    let values = unsafe { std::mem::transmute_copy::<libc::fsid_t, [i32; 2]>(fsid) };
    MountIdentity([values[0] as u32 as u64, values[1] as u32 as u64])
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn directory_mount_identity_at(
    _directory: &File,
    stage: PolicyViolationStage,
) -> Result<MountIdentity, PolicyViolation> {
    Err(PolicyViolation::new(
        PolicyViolationCode::UnsupportedPlatform,
        stage,
    ))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn entry_mount_identity_at(
    _directory: &File,
    _name: &OsStr,
    stage: PolicyViolationStage,
) -> Result<MountIdentity, PolicyViolation> {
    Err(PolicyViolation::new(
        PolicyViolationCode::UnsupportedPlatform,
        stage,
    ))
}

#[cfg(unix)]
struct EntryMetadata {
    identity: (u64, u64),
    mount_identity: MountIdentity,
    kind: AuditedEntryKind,
    allocated_bytes: u64,
}

#[cfg(unix)]
fn entry_metadata(directory: &File, name: &OsStr) -> Result<EntryMetadata, PolicyViolation> {
    entry_metadata_at(
        directory,
        name,
        PolicyViolationStage::RunBoundPreMarker,
    )
}

#[cfg(unix)]
fn entry_metadata_at(
    directory: &File,
    name: &OsStr,
    stage: PolicyViolationStage,
) -> Result<EntryMetadata, PolicyViolation> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    let mount_identity = entry_mount_identity_at(directory, name, stage)?;
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| temp_violation_at(TempUnsafeReason::InvalidEntry, stage))?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstatat(directory.as_raw_fd(), name.as_ptr(), stat.as_mut_ptr(), libc::AT_SYMLINK_NOFOLLOW) } != 0 {
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    let stat = unsafe { stat.assume_init() };
    let kind = if (stat.st_mode & libc::S_IFMT) == libc::S_IFDIR {
        AuditedEntryKind::Directory
    } else {
        AuditedEntryKind::Leaf
    };
    let allocated_bytes = (stat.st_blocks as u64)
        .checked_mul(512)
        .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
    Ok(EntryMetadata {
        identity: (stat.st_dev as u64, stat.st_ino as u64),
        mount_identity,
        kind,
        allocated_bytes,
    })
}

#[cfg(unix)]
fn unlinkat(directory: &File, name: &OsStr, directory_entry: bool) -> Result<(), PolicyViolation> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| temp_violation(TempUnsafeReason::InvalidEntry))?;
    let flags = if directory_entry { libc::AT_REMOVEDIR } else { 0 };
    if unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), flags) } != 0 {
        return Err(temp_violation(TempUnsafeReason::IoFailure));
    }
    Ok(())
}

fn temp_violation(reason: TempUnsafeReason) -> PolicyViolation {
    temp_violation_at(reason, PolicyViolationStage::RunBoundPreMarker)
}

fn validate_generation_high_water(
    max_generation_id: Option<i64>,
    durable_run_id_high_water: i64,
) -> Result<(), PolicyViolation> {
    if !(0..=MAX_PRIVATE_TEMP_RUN_ID).contains(&durable_run_id_high_water)
        || max_generation_id.is_some_and(|generation_id| {
            generation_id <= 0
                || generation_id > MAX_PRIVATE_TEMP_RUN_ID
                || generation_id > durable_run_id_high_water
        })
    {
        return Err(temp_violation_at(
            TempUnsafeReason::InvalidEntry,
            PolicyViolationStage::PreBinding,
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn open_or_create_private_temp_container(
    parent: &File,
    name: &OsStr,
) -> Result<File, PolicyViolation> {
    match open_directory_nofollow(parent, name) {
        Ok(directory) => {
            validate_private_temp_container_at(
                &directory,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            Ok(directory)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            mkdirat_private(parent, name)?;
            let directory = open_directory_nofollow(parent, name).map_err(map_temp_io)?;
            validate_private_temp_container_at(
                &directory,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            Ok(directory)
        }
        Err(error) => Err(map_temp_io(error)),
    }
}

#[cfg(unix)]
fn open_or_create_directory(parent: &File, name: &OsStr) -> Result<File, PolicyViolation> {
    match open_directory_nofollow(parent, name) {
        Ok(directory) => {
            validate_private_directory(&directory)?;
            Ok(directory)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            mkdirat_private(parent, name)?;
            let directory = open_directory_nofollow(parent, name).map_err(map_temp_io)?;
            validate_private_directory(&directory)?;
            Ok(directory)
        }
        Err(error) => Err(map_temp_io(error)),
    }
}

#[cfg(unix)]
fn validate_private_temp_container_at(
    directory: &File,
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    let metadata = directory
        .metadata()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
    use std::os::unix::fs::MetadataExt;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() as u32 }
        || metadata.mode() & 0o022 != 0
    {
        return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_private_directory(directory: &File) -> Result<(), PolicyViolation> {
    validate_private_directory_at(directory, PolicyViolationStage::RunBoundPreMarker)
}

#[cfg(unix)]
fn validate_private_directory_at(
    directory: &File,
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    let metadata = directory
        .metadata()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
    use std::os::unix::fs::MetadataExt;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() as u32 }
        || metadata.mode() & 0o7777 != 0o700
    {
        return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
    }
    Ok(())
}

#[cfg(unix)]
fn verified_private_temp_identity(
    metadata: &std::fs::Metadata,
    expected: (u64, u64),
) -> Result<ExecutableIdentity, PolicyViolation> {
    use std::os::unix::fs::MetadataExt;

    let identity = ExecutableIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        owner: metadata.uid(),
        mode: metadata.mode() & 0o7777,
    };
    if !metadata.is_dir()
        || identity.owner != unsafe { libc::geteuid() as u32 }
        || identity.mode != 0o700
        || (identity.device, identity.inode) != expected
    {
        return Err(temp_error());
    }
    Ok(identity)
}

#[cfg(unix)]
fn directory_identity(directory: &File) -> Result<(u64, u64), PolicyViolation> {
    directory_identity_at(directory, PolicyViolationStage::RunBoundPreMarker)
}

#[cfg(unix)]
fn directory_identity_at(
    directory: &File,
    stage: PolicyViolationStage,
) -> Result<(u64, u64), PolicyViolation> {
    use std::os::unix::fs::MetadataExt;
    let metadata = directory
        .metadata()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
    Ok((metadata.dev(), metadata.ino()))
}

#[cfg(unix)]
fn mkdirat_private(parent: &File, name: &OsStr) -> Result<(), PolicyViolation> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    let name = std::ffi::CString::new(name.as_bytes()).map_err(|_| temp_error())?;
    let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
    if result == 0 {
        // Persist the newly visible directory before opening or validating it.
        // If this fails, leave the directory untouched for diagnostics.
        parent.sync_all().map_err(|_| temp_error())?;
        Ok(())
    } else {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::AlreadyExists {
            Err(temp_error())
        } else {
            Err(map_temp_io(error))
        }
    }
}

#[cfg(unix)]
fn open_directory_nofollow(parent: &File, name: &OsStr) -> io::Result<File> {
    use std::{os::fd::{AsRawFd, FromRawFd}, os::unix::ffi::OsStrExt};
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL"))?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY
                | libc::O_DIRECTORY
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_optional_directory_on_mount(
    parent: &File,
    name: &OsStr,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
) -> Result<Option<File>, PolicyViolation> {
    open_optional_directory_on_mount_impl(
        parent,
        name,
        expected_mount,
        stage,
        #[cfg(test)]
        None,
    )
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
fn open_optional_directory_on_mount_with_test_state(
    parent: &File,
    name: &OsStr,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
    test_state: &mut OptionalOpenTestState,
) -> Result<Option<File>, PolicyViolation> {
    open_optional_directory_on_mount_impl(
        parent,
        name,
        expected_mount,
        stage,
        Some(test_state),
    )
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct LinuxOpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

#[cfg(target_os = "linux")]
const LINUX_RESOLVE_NO_XDEV: u64 = 0x01;

#[cfg(target_os = "linux")]
fn linux_open_directory_no_xdev(
    parent: &File,
    name: &OsStr,
) -> Result<File, LinuxSyscallFailure> {
    use std::{os::fd::{AsRawFd, FromRawFd}, os::unix::ffi::OsStrExt};
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| LinuxSyscallFailure::IoFailure)?;
    let how = LinuxOpenHow {
        flags: (libc::O_RDONLY
            | libc::O_DIRECTORY
            | libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK) as u64,
        mode: 0,
        resolve: LINUX_RESOLVE_NO_XDEV,
    };
    loop {
        let fd = unsafe {
            libc::syscall(
                libc::SYS_openat2,
                parent.as_raw_fd(),
                name.as_ptr(),
                &how as *const LinuxOpenHow,
                std::mem::size_of::<LinuxOpenHow>(),
            )
        };
        if fd >= 0 {
            return Ok(unsafe { File::from_raw_fd(fd as libc::c_int) });
        }
        let errno = io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO);
        match classify_linux_openat2_errno(errno) {
            LinuxSyscallFailure::Retry => continue,
            failure => return Err(failure),
        }
    }
}

#[cfg(target_os = "linux")]
fn linux_syscall_violation(
    failure: LinuxSyscallFailure,
    stage: PolicyViolationStage,
) -> PolicyViolation {
    match failure {
        LinuxSyscallFailure::MountBoundary => {
            temp_violation_at(TempUnsafeReason::MountBoundary, stage)
        }
        LinuxSyscallFailure::UnsupportedPlatform => {
            PolicyViolation::new(PolicyViolationCode::UnsupportedPlatform, stage)
        }
        LinuxSyscallFailure::Retry
        | LinuxSyscallFailure::Missing
        | LinuxSyscallFailure::IoFailure => {
            temp_violation_at(TempUnsafeReason::IoFailure, stage)
        }
    }
}

#[cfg(target_os = "linux")]
fn open_optional_directory_on_mount_impl(
    parent: &File,
    name: &OsStr,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
    #[cfg(test)] mut test_state: Option<&mut OptionalOpenTestState>,
) -> Result<Option<File>, PolicyViolation> {
    #[cfg(test)]
    if let Some(state) = test_state.as_deref_mut() {
        state.steps.push(OptionalOpenStep::SecureOpen);
    }
    let directory = match linux_open_directory_no_xdev(parent, name) {
        Ok(directory) => directory,
        Err(LinuxSyscallFailure::Missing) => return Ok(None),
        Err(failure) => return Err(linux_syscall_violation(failure, stage)),
    };
    #[cfg(test)]
    if let Some(state) = test_state.as_deref_mut() {
        state.steps.push(OptionalOpenStep::DescriptorMountRecheck);
    }
    if directory_mount_identity_at(&directory, stage)? != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    Ok(Some(directory))
}

#[cfg(target_os = "linux")]
fn open_directory_on_mount(
    parent: &File,
    name: &OsStr,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
) -> Result<File, PolicyViolation> {
    let directory = linux_open_directory_no_xdev(parent, name)
        .map_err(|failure| linux_syscall_violation(failure, stage))?;
    if directory_mount_identity_at(&directory, stage)? != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    Ok(directory)
}

#[cfg(target_os = "macos")]
fn macos_mount_identity_violation(
    error: io::Error,
    stage: PolicyViolationStage,
) -> PolicyViolation {
    match error.raw_os_error() {
        Some(libc::ENOSYS) | Some(libc::ENOTSUP) => {
            PolicyViolation::new(PolicyViolationCode::UnsupportedPlatform, stage)
        }
        Some(libc::ELOOP) => temp_violation_at(TempUnsafeReason::MountBoundary, stage),
        _ => temp_violation_at(TempUnsafeReason::IoFailure, stage),
    }
}

#[cfg(target_os = "macos")]
fn open_optional_directory_on_mount_impl(
    parent: &File,
    name: &OsStr,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
    #[cfg(test)] mut test_state: Option<&mut OptionalOpenTestState>,
) -> Result<Option<File>, PolicyViolation> {
    #[cfg(test)]
    if let Some(state) = test_state.as_deref_mut() {
        state.steps.push(OptionalOpenStep::EntryMountPrecheck);
    }
    let entry_mount = match macos_entry_mount_identity_io(parent, name) {
        Ok(identity) => identity,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(macos_mount_identity_violation(error, stage)),
    };
    if entry_mount != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    #[cfg(test)]
    if let Some(state) = test_state.as_deref_mut() {
        state.steps.push(OptionalOpenStep::SecureOpen);
    }
    let directory = open_directory_nofollow(parent, name)
        .map_err(|error| macos_mount_identity_violation(error, stage))?;
    #[cfg(test)]
    if let Some(state) = test_state.as_deref_mut() {
        state.steps.push(OptionalOpenStep::DescriptorMountRecheck);
    }
    if directory_mount_identity_at(&directory, stage)? != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    Ok(Some(directory))
}

#[cfg(target_os = "macos")]
fn open_directory_on_mount(
    parent: &File,
    name: &OsStr,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
) -> Result<File, PolicyViolation> {
    let entry_mount = macos_entry_mount_identity_io(parent, name)
        .map_err(|error| macos_mount_identity_violation(error, stage))?;
    if entry_mount != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    let directory = open_directory_nofollow(parent, name)
        .map_err(|error| macos_mount_identity_violation(error, stage))?;
    if directory_mount_identity_at(&directory, stage)? != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    Ok(directory)
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn open_directory_on_mount(
    _parent: &File,
    _name: &OsStr,
    _expected_mount: MountIdentity,
    stage: PolicyViolationStage,
) -> Result<File, PolicyViolation> {
    Err(PolicyViolation::new(
        PolicyViolationCode::UnsupportedPlatform,
        stage,
    ))
}

#[cfg(unix)]
fn map_temp_io(error: io::Error) -> PolicyViolation {
    map_temp_io_at(error, PolicyViolationStage::RunBoundPreMarker)
}

fn map_temp_io_at(error: io::Error, stage: PolicyViolationStage) -> PolicyViolation {
    let _ = error;
    temp_violation_at(TempUnsafeReason::IoFailure, stage)
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        time::Duration,
    };

    fn test_temp(run_id: i64) -> (tempfile::TempDir, PrivateRunTemp) {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let temp = PrivateRunTemp::create(&root, run_id).unwrap();
        (holder, temp)
    }

    #[test]
    fn duplicate_directory_fd_sets_cloexec_atomically() {
        let (_holder, temp) = test_temp(701);
        let duplicate = duplicate_directory_fd(
            &temp.directory,
            PolicyViolationStage::RunBoundPreMarker,
        )
        .unwrap();
        let flags = unsafe { libc::fcntl(duplicate, libc::F_GETFD) };
        unsafe { libc::close(duplicate) };
        assert_ne!(flags & libc::FD_CLOEXEC, 0);
    }

    #[test]
    fn verified_target_rejects_private_directory_special_bits() {
        let (_holder, temp) = test_temp(702);
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o1700)).unwrap();

        let error = temp.verified_target().unwrap_err();

        assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
    }

    #[test]
    fn verified_private_temp_identity_uses_one_validated_metadata_snapshot() {
        use std::os::unix::fs::MetadataExt;

        let (_holder, temp) = test_temp(703);
        let metadata = temp.directory.metadata().unwrap();

        let identity = verified_private_temp_identity(&metadata, temp.identity).unwrap();

        assert_eq!(identity.device, metadata.dev());
        assert_eq!(identity.inode, metadata.ino());
        assert_eq!(identity.owner, metadata.uid());
        assert_eq!(identity.mode, metadata.mode() & 0o7777);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn retained_decision_output_anchor_prevents_inode_reuse_and_rejects_replacement() {
        use std::{
            io::Write as _,
            os::unix::fs::{MetadataExt, OpenOptionsExt},
        };

        let (_holder, temp) = test_temp(704);
        temp.prepare_decision_schema(b"{}").unwrap();
        let original_identity = temp
            .decision_output_anchor
            .lock()
            .unwrap()
            .as_ref()
            .map(|anchor| anchor.identity)
            .unwrap();
        let output = temp.path().join("decision.json");
        fs::remove_file(&output).unwrap();
        let mut replacement = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&output)
            .unwrap();
        replacement.write_all(b"replacement").unwrap();
        replacement.sync_all().unwrap();
        let metadata = replacement.metadata().unwrap();

        assert_ne!(original_identity, (metadata.dev(), metadata.ino()));
        let error = temp.read_decision_output().unwrap_err();
        assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
        assert!(matches!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(
                TempUnsafeReason::InvalidEntry | TempUnsafeReason::IdentityChanged
            )
        ));
    }

    #[test]
    fn inspect_capacity_rejects_maximum_sqlite_generation_id() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let retained = PrivateRunTemp::create(&root, MAX_PRIVATE_TEMP_RUN_ID).unwrap();
        let impossible = retained
            .path()
            .parent()
            .unwrap()
            .join(i64::MAX.to_string());
        fs::create_dir(&impossible).unwrap();
        fs::set_permissions(&impossible, fs::Permissions::from_mode(0o700)).unwrap();
        let error = PrivateRunTemp::inspect_capacity(&root).unwrap_err();
        assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::InvalidEntry)
        );
    }

    #[test]
    fn inspect_capacity_accepts_highest_allocatable_generation_id() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        PrivateRunTemp::create(&root, i64::MAX - 1).unwrap();
        let report = PrivateRunTemp::inspect_capacity(&root).unwrap();
        assert_eq!(report.max_generation_id, Some(i64::MAX - 1));
    }

    #[test]
    fn directory_entries_enforces_remaining_limit_before_collecting_more() {
        let (_holder, temp) = test_temp(702);
        fs::write(temp.path.join("one"), b"1").unwrap();
        fs::write(temp.path.join("two"), b"2").unwrap();
        let error = directory_entries(
            &temp.directory,
            1,
            TempUnsafeReason::EntryLimit,
            PolicyViolationStage::RunBoundPreMarker,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::EntryLimit));
        assert!(temp.path.join("one").exists());
        assert!(temp.path.join("two").exists());
    }

    #[test]
    fn audit_byte_limit_is_checked_before_any_removal() {
        let (_holder, temp) = test_temp(703);
        fs::write(temp.path.join("payload"), b"payload").unwrap();
        assert_eq!(AuditState::default().max_allocated_bytes, MAX_PRIVATE_TEMP_ALLOCATED_BYTES);
        let mut state = AuditState::with_limits(
            MAX_PRIVATE_TEMP_CLEANUP_DEPTH,
            MAX_PRIVATE_TEMP_CLEANUP_ENTRIES,
            0,
        );
        let error = audit_directory(
            &temp.directory,
            0,
            &mut state,
            None,
            PolicyViolationStage::RunBoundPreMarker,
            directory_mount_identity_at(
                &temp.directory,
                PolicyViolationStage::RunBoundPreMarker,
            )
            .unwrap(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::ByteLimit));
        assert!(temp.path.join("payload").exists());
    }

    #[test]
    fn audited_entry_allocation_uses_checked_addition() {
        let entry = AuditedEntry {
            name: OsString::from("parent"),
            identity: (1, 1),
            mount_identity: MountIdentity([1, 0]),
            kind: AuditedEntryKind::Directory,
            allocated_bytes: u64::MAX,
            children: vec![AuditedEntry {
                name: OsString::from("child"),
                identity: (1, 2),
                mount_identity: MountIdentity([1, 0]),
                kind: AuditedEntryKind::Leaf,
                allocated_bytes: 1,
                children: Vec::new(),
            }],
        };
        let error = entry
            .allocated_total(PolicyViolationStage::RunBoundPreMarker)
            .unwrap_err();
        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::ByteLimit));
    }

    #[test]
    fn test_hook_identity_change_aborts_and_retry_remains_authorized() {
        let (holder, mut temp) = test_temp(704);
        let original = temp.path.join("original");
        fs::write(&original, b"original").unwrap();
        let retired = holder.path().join("retired");
        let replacement = original.clone();
        let replacement_for_hook = replacement.clone();
        let error = temp
            .cleanup_contents_before_with_test_hook(None, move || {
                fs::rename(&original, &retired).unwrap();
                fs::write(&replacement_for_hook, b"replacement").unwrap();
            })
            .unwrap_err();
        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IdentityChanged));
        assert_eq!(fs::read(&replacement).unwrap(), b"replacement");
        temp.cleanup_contents_before(None).unwrap();
        let remaining: Vec<_> = fs::read_dir(temp.path()).unwrap().collect();
        assert!(remaining.is_empty(), "remaining entries: {remaining:?}");
    }

    #[test]
    fn mount_identity_mismatch_at_directory_open_preserves_tree_and_retry_authority() {
        let (_holder, mut temp) = test_temp(713);
        let nested = temp.path.join("nested");
        fs::create_dir(&nested).unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(nested.join("payload"), b"preserved").unwrap();
        let mut state = MountBoundaryTestState {
            mismatch_at_directory_open: true,
            ..MountBoundaryTestState::default()
        };

        let error = temp
            .cleanup_contents_before_with_mount_test_state(None, &mut state)
            .unwrap_err();

        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::MountBoundary)
        );
        assert_eq!(fs::read(nested.join("payload")).unwrap(), b"preserved");
        temp.cleanup_contents_before(None).unwrap();
        assert!(fs::read_dir(temp.path()).unwrap().next().is_none());
    }

    #[test]
    fn mount_identity_mismatch_before_unlink_deletes_nothing_and_remains_retryable() {
        let (_holder, mut temp) = test_temp(714);
        fs::write(temp.path.join("first"), b"first").unwrap();
        fs::write(temp.path.join("second"), b"second").unwrap();
        let mut state = MountBoundaryTestState {
            mismatch_before_unlink: true,
            ..MountBoundaryTestState::default()
        };

        let error = temp
            .cleanup_contents_before_with_mount_test_state(None, &mut state)
            .unwrap_err();

        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::MountBoundary)
        );
        assert_eq!(fs::read(temp.path.join("first")).unwrap(), b"first");
        assert_eq!(fs::read(temp.path.join("second")).unwrap(), b"second");
        temp.cleanup_contents_before(None).unwrap();
        assert!(fs::read_dir(temp.path()).unwrap().next().is_none());
    }

    #[test]
    fn optional_mount_open_checks_entry_before_descriptor_and_missing_is_empty() {
        let holder = tempfile::tempdir().unwrap();
        let parent = File::open(holder.path()).unwrap();
        let expected_mount = directory_mount_identity_at(
            &parent,
            PolicyViolationStage::PreBinding,
        )
        .unwrap();
        let mut missing_state = OptionalOpenTestState::default();

        let missing = open_optional_directory_on_mount_with_test_state(
            &parent,
            OsStr::new("missing"),
            expected_mount,
            PolicyViolationStage::PreBinding,
            &mut missing_state,
        )
        .unwrap();

        assert!(missing.is_none());
        #[cfg(target_os = "macos")]
        assert_eq!(
            missing_state.steps,
            vec![OptionalOpenStep::EntryMountPrecheck]
        );
        #[cfg(target_os = "linux")]
        assert_eq!(missing_state.steps, vec![OptionalOpenStep::SecureOpen]);

        let present = holder.path().join("present");
        fs::create_dir(&present).unwrap();
        fs::set_permissions(&present, fs::Permissions::from_mode(0o700)).unwrap();
        let mut present_state = OptionalOpenTestState::default();
        let opened = open_optional_directory_on_mount_with_test_state(
            &parent,
            OsStr::new("present"),
            expected_mount,
            PolicyViolationStage::PreBinding,
            &mut present_state,
        )
        .unwrap();

        assert!(opened.is_some());
        #[cfg(target_os = "macos")]
        assert_eq!(
            present_state.steps,
            vec![
                OptionalOpenStep::EntryMountPrecheck,
                OptionalOpenStep::SecureOpen,
                OptionalOpenStep::DescriptorMountRecheck,
            ]
        );
        #[cfg(target_os = "linux")]
        assert_eq!(
            present_state.steps,
            vec![
                OptionalOpenStep::SecureOpen,
                OptionalOpenStep::DescriptorMountRecheck,
            ]
        );
    }

    #[test]
    fn linux_statx_buffer_parser_requires_mount_id_and_reads_kernel_offsets() {
        let with_mount_id = LinuxStatxBuffer {
            stx_mask: LINUX_STATX_MNT_ID,
            before_mnt_id: [0; 140],
            stx_mnt_id: 73,
            remaining: [0; 104],
        };
        assert_eq!(
            parse_linux_statx_mount_identity(&with_mount_id),
            Ok(MountIdentity([73, 0]))
        );

        let without_mount_id = LinuxStatxBuffer {
            stx_mask: 0,
            before_mnt_id: [0; 140],
            stx_mnt_id: 91,
            remaining: [0; 104],
        };
        assert_eq!(
            parse_linux_statx_mount_identity(&without_mount_id),
            Err(LinuxSyscallFailure::UnsupportedPlatform)
        );
    }

    #[test]
    fn linux_openat2_errno_classifier_preserves_security_and_capability_meaning() {
        for errno in [libc::EXDEV, libc::ELOOP] {
            assert_eq!(
                classify_linux_openat2_errno(errno),
                LinuxSyscallFailure::MountBoundary
            );
        }
        for errno in [libc::ENOSYS, libc::EINVAL, libc::E2BIG] {
            assert_eq!(
                classify_linux_openat2_errno(errno),
                LinuxSyscallFailure::UnsupportedPlatform
            );
        }
        assert_eq!(
            classify_linux_openat2_errno(libc::ENOENT),
            LinuxSyscallFailure::Missing
        );
        assert_eq!(
            classify_linux_openat2_errno(libc::EINTR),
            LinuxSyscallFailure::Retry
        );
        assert_eq!(
            classify_linux_openat2_errno(libc::EACCES),
            LinuxSyscallFailure::IoFailure
        );
    }

    #[test]
    fn linux_statx_errno_classifier_retries_and_reports_capability() {
        assert_eq!(
            classify_linux_statx_errno(libc::EINTR),
            LinuxSyscallFailure::Retry
        );
        assert_eq!(
            classify_linux_statx_errno(libc::ENOSYS),
            LinuxSyscallFailure::UnsupportedPlatform
        );
        assert_eq!(
            classify_linux_statx_errno(libc::EACCES),
            LinuxSyscallFailure::IoFailure
        );
    }

    #[test]
    fn expired_deadline_performs_no_mutation() {
        let (_holder, mut temp) = test_temp(705);
        let file = temp.path.join("payload");
        fs::write(&file, b"payload").unwrap();
        let error = temp
            .cleanup_contents_before(Some(Instant::now() - Duration::from_secs(1)))
            .unwrap_err();
        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert_eq!(fs::read(&file).unwrap(), b"payload");
    }

    #[test]
    fn nested_audit_budget_bounds_retained_entries_before_recursing() {
        let (_holder, temp) = test_temp(706);
        let first = temp.path.join("first");
        let second = temp.path.join("second");
        fs::create_dir(&first).unwrap();
        fs::create_dir(&second).unwrap();
        fs::set_permissions(&first, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&second, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(first.join("nested"), b"one").unwrap();
        fs::write(second.join("nested"), b"two").unwrap();
        let mut state = AuditState::with_limits(32, 2, MAX_PRIVATE_TEMP_ALLOCATED_BYTES);

        let error = audit_directory(
            &temp.directory,
            0,
            &mut state,
            None,
            PolicyViolationStage::RunBoundPreMarker,
            directory_mount_identity_at(
                &temp.directory,
                PolicyViolationStage::RunBoundPreMarker,
            )
            .unwrap(),
            None,
        )
        .unwrap_err();

        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::EntryLimit));
        assert!(state.entries <= state.max_entries);
    }

    #[test]
    fn audit_deadline_is_checked_inside_readdir_and_fstat_loop() {
        let (_holder, temp) = test_temp(707);
        fs::write(temp.path.join("one"), b"one").unwrap();
        fs::write(temp.path.join("two"), b"two").unwrap();
        let mut state = AuditState::default();
        state.test_deadline_after_entries = Some(1);

        let error = audit_directory(
            &temp.directory,
            0,
            &mut state,
            Some(Instant::now() + Duration::from_secs(60)),
            PolicyViolationStage::RunBoundPreMarker,
            directory_mount_identity_at(
                &temp.directory,
                PolicyViolationStage::RunBoundPreMarker,
            )
            .unwrap(),
            None,
        )
        .unwrap_err();

        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert!(temp.path.join("one").exists());
        assert!(temp.path.join("two").exists());
    }

    #[test]
    fn deadline_after_recursive_cleanup_prevents_parent_unlink() {
        let (_holder, mut temp) = test_temp(708);
        let nested = temp.path.join("nested");
        fs::create_dir(&nested).unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(nested.join("payload"), b"payload").unwrap();
        let mut test_state = CleanupTestState {
            expire_after_recursion: true,
            ..CleanupTestState::default()
        };

        let error = temp
            .cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap_err();

        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert!(nested.exists());
        assert!(!nested.join("payload").exists());
    }

    #[test]
    fn modified_directory_is_synced_before_cleanup_error_returns() {
        let (_holder, mut temp) = test_temp(709);
        fs::write(temp.path.join("first"), b"first").unwrap();
        fs::write(temp.path.join("second"), b"second").unwrap();
        let mut test_state = CleanupTestState {
            fail_after_first_unlink: true,
            ..CleanupTestState::default()
        };

        let error = temp
            .cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap_err();

        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert!(test_state.sync_attempts > 0);
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);
    }

    #[test]
    fn cleanup_deadline_error_during_sync_remains_typed() {
        let (_holder, mut temp) = test_temp(710);
        fs::write(temp.path().join("first"), b"first").unwrap();
        fs::write(temp.path().join("second"), b"second").unwrap();
        let mut test_state = CleanupTestState {
            fail_after_first_unlink: true,
            expire_before_sync: true,
            ..CleanupTestState::default()
        };

        let error = temp
            .cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap_err();

        assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert_eq!(test_state.sync_attempts, 1);
    }

    #[test]
    fn deadline_crossing_after_single_unlink_still_syncs_before_error() {
        let (_holder, mut temp) = test_temp(712);
        let payload = temp.path.join("payload");
        fs::write(&payload, b"payload").unwrap();
        let mut test_state = CleanupTestState {
            expire_before_sync: true,
            ..CleanupTestState::default()
        };

        let error = temp
            .cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap_err();

        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert!(!payload.exists());
        assert_eq!(test_state.sync_attempts, 1);
        assert!(test_state.sync_completed);
        temp.cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap();
        assert_eq!(test_state.sync_attempts, 2);
    }

    #[test]
    fn directory_entries_closes_owned_dir_once_on_error_and_success() {
        use std::sync::{atomic::AtomicUsize, Arc};

        let (_holder, temp) = test_temp(713);
        fs::write(temp.path.join("payload"), b"payload").unwrap();
        let close_counter = Arc::new(AtomicUsize::new(0));
        let mut error_state = DirectoryEntriesTestState {
            fail_after_metadata: Some(1),
            close_counter: Arc::clone(&close_counter),
        };
        let error = directory_entries(
            &temp.directory,
            MAX_PRIVATE_TEMP_CLEANUP_ENTRIES,
            TempUnsafeReason::EntryLimit,
            PolicyViolationStage::RunBoundPreMarker,
            None,
            None,
            Some(&mut error_state),
        )
        .unwrap_err();
        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert_eq!(close_counter.load(std::sync::atomic::Ordering::SeqCst), 1);

        let success_counter = Arc::new(AtomicUsize::new(0));
        let mut success_state = DirectoryEntriesTestState {
            fail_after_metadata: None,
            close_counter: Arc::clone(&success_counter),
        };
        directory_entries(
            &temp.directory,
            MAX_PRIVATE_TEMP_CLEANUP_ENTRIES,
            TempUnsafeReason::EntryLimit,
            PolicyViolationStage::RunBoundPreMarker,
            None,
            None,
            Some(&mut success_state),
        )
        .unwrap();
        assert_eq!(success_counter.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn successful_cleanup_syncs_modified_parent_before_return() {
        let (_holder, mut temp) = test_temp(711);
        fs::write(temp.path.join("payload"), b"payload").unwrap();
        let mut test_state = CleanupTestState::default();

        temp.cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap();

        assert!(test_state.sync_attempts > 0);
    }

    #[test]
    fn empty_retry_must_sync_after_prior_final_unlink_sync_failure() {
        let (_holder, mut temp) = test_temp(714);
        let payload = temp.path.join("payload");
        fs::write(&payload, b"payload").unwrap();
        let mut test_state = CleanupTestState {
            fail_sync: true,
            ..CleanupTestState::default()
        };

        let first = temp
            .cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap_err();
        assert_eq!(first.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert!(!payload.exists());
        assert_eq!(test_state.sync_attempts, 1);

        let second = temp
            .cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap_err();
        assert_eq!(second.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert_eq!(test_state.sync_attempts, 2);

        test_state.fail_sync = false;
        temp.cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap();
        assert_eq!(test_state.sync_attempts, 3);
    }

    #[test]
    fn decision_artifact_root_mount_revalidation_precedes_success_and_saved_scan_error() {
        let holder = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(holder.path()).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root).unwrap();
        let captured = MountIdentity([41, 0]);
        let changed = MountIdentity([42, 0]);

        for scan_result in [
            Ok(()),
            Err(temp_violation(TempUnsafeReason::EntryLimit)),
        ] {
            let error = finish_artifact_scan_with_mount_reader(
                &anchor,
                captured,
                scan_result,
                |_| Ok(changed),
            )
            .unwrap_err();
            assert_eq!(
                error.detail,
                PolicyViolationDetail::TempUnsafe(TempUnsafeReason::MountBoundary)
            );
        }

        let error = finish_artifact_scan_with_mount_reader(
            &anchor,
            captured,
            Err(temp_violation(TempUnsafeReason::EntryLimit)),
            |_| Ok(captured),
        )
        .unwrap_err();
        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::EntryLimit)
        );
    }
}

fn temp_violation_at(reason: TempUnsafeReason, stage: PolicyViolationStage) -> PolicyViolation {
    PolicyViolation::with_detail(
        PolicyViolationCode::TempUnsafe,
        stage,
        PolicyViolationDetail::TempUnsafe(reason),
    )
}

fn stage_violation(mut violation: PolicyViolation, stage: PolicyViolationStage) -> PolicyViolation {
    violation.stage = stage;
    violation
}
