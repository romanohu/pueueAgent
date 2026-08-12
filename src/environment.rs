//! Explicit, default-deny child environments and private per-run temporary
//! directories.
//!
//! Environment values are kept as `OsString`s until the final process API
//! call.  Debug output intentionally contains names only.  Temporary
//! directory cleanup is descriptor-relative so a replaced pathname can never
//! cause a later generation (or a symlink target) to be removed.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{OsStr, OsString},
    fmt,
    fs::File,
    io,
    path::{Path, PathBuf},
};

use crate::execution_policy::{
    PolicyViolation, PolicyViolationCode, PolicyViolationStage,
    ResolvedExecutionPolicy, ResolvedProjectExecutionPolicy, VerifiedProjectRoot,
};

pub use crate::execution_policy::StartupEnvironment;

const PRIVATE_TEMP_ROOT: &str = ".pueue-agent";
const PRIVATE_TEMP_DIR: &str = "tmp";
const MAX_RUN_ID_BYTES: usize = 20;
#[cfg(unix)]
const MAX_CLEANUP_DEPTH: usize = 32;
#[cfg(unix)]
const MAX_CLEANUP_ENTRIES: usize = 4096;

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
            | "AWS_ACCESS_KEY_ID"
            | "AWS_SECRET_ACCESS_KEY"
            | "GITHUB_TOKEN"
            | "GH_TOKEN"
            | "NPM_TOKEN"
            | "AUTH_TOKEN"
            | "ACCESS_TOKEN"
            | "REFRESH_TOKEN"
            | "API_KEY"
    ) || upper.ends_with("_API_KEY")
        || upper.ends_with("_TOKEN")
        || upper.ends_with("_SECRET")
        || upper.ends_with("_PASSWORD")
        || upper.ends_with("_PRIVATE_KEY")
        || upper.ends_with("_CREDENTIAL")
        || upper.ends_with("_CREDENTIALS")
        || upper.contains("TOKEN")
        || upper.contains("SECRET")
        || upper.contains("PASSWORD")
        || upper.contains("PRIVATE_KEY")
        || upper.starts_with("AWS_SECRET_")
        || upper.starts_with("AZURE_CLIENT_SECRET")
}

/// A captured, explicit child environment.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SanitizedEnvironment {
    values: BTreeMap<OsString, OsString>,
}

impl SanitizedEnvironment {
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
        let mut environment = Self::project_baseline(startup, policy, run_id)?;
        environment.insert_generated("CODEX_HOME", policy.codex_home.as_os_str());
        for name in CODEX_AUTH_NAMES {
            if let Some(value) = startup.get(name) {
                environment.insert_os(OsString::from(name), value.to_os_string());
            }
        }
        Ok(environment)
    }

    pub fn for_custom_agent(
        startup: &StartupEnvironment,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
    ) -> Result<Self, PolicyViolation> {
        let mut environment = Self::project_baseline(startup, policy, run_id)?;
        environment.copy_allowlist(startup, &policy.agent_environment_allow);
        Ok(environment)
    }

    pub fn for_codex_task(
        startup: &StartupEnvironment,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
    ) -> Result<Self, PolicyViolation> {
        let mut environment = Self::project_baseline(startup, policy, run_id)?;
        environment.copy_allowlist(startup, &policy.task_environment_allow);
        Ok(environment)
    }

    pub fn for_pueue(policy: &ResolvedExecutionPolicy) -> Result<Self, PolicyViolation> {
        let environment = Self::default_baseline(
            &policy.startup_environment,
            &policy.trusted_path,
            None,
            None,
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
    ) -> Result<Self, PolicyViolation> {
        validate_run_id(run_id)?;
        let temp_path = policy
            .root_anchor
            .canonical_path
            .join(&policy.private_temp_relative_root)
            .join(run_id.to_string());
        Self::default_baseline(
            startup,
            &policy.trusted_path,
            Some(temp_path.as_os_str()),
            Some((run_id, policy.project_id.as_str())),
        )
    }

    fn default_baseline(
        startup: &StartupEnvironment,
        trusted_path: &[PathBuf],
        temp_path: Option<&OsStr>,
        ids: Option<(i64, &str)>,
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
        for name in PROXY_AND_CERT_NAMES {
            environment.copy_startup_name(startup, name);
        }
        if let Some((run_id, project_id)) = ids {
            environment.insert_generated("PUEUE_AGENT_RUN_ID", OsString::from(run_id.to_string()));
            environment.insert_generated("PUEUE_AGENT_PROJECT_ID", OsString::from(project_id));
        }
        Ok(environment)
    }

    fn copy_allowlist(&mut self, startup: &StartupEnvironment, allowlist: &BTreeSet<String>) {
        for name in allowlist {
            if !is_auth_name(name) {
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
    if run_id <= 0 || text.len() > MAX_RUN_ID_BYTES {
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

/// An exclusive, owner-only per-run directory.
pub struct PrivateRunTemp {
    directory: File,
    parent: File,
    name: OsString,
    path: PathBuf,
    identity: (u64, u64),
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
    pub fn create(root: &VerifiedProjectRoot, run_id: i64) -> Result<Self, PolicyViolation> {
        #[cfg(not(unix))]
        {
            let _ = (root, run_id);
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        #[cfg(unix)]
        {
            validate_run_id(run_id)?;
            let owner = root.directory.try_clone().map_err(|_| temp_error())?;
            let service = open_or_create_directory(&owner, OsStr::new(PRIVATE_TEMP_ROOT))?;
            let tmp = open_or_create_directory(&service, OsStr::new(PRIVATE_TEMP_DIR))?;
            let name = OsString::from(run_id.to_string());
            mkdirat_private(&tmp, &name)?;
            let directory = match open_directory_nofollow(&tmp, &name) {
                Ok(directory) => directory,
                Err(error) => {
                    let _ = unlinkat_raw(&tmp, &name, false);
                    return Err(map_temp_io(error));
                }
            };
            validate_private_directory(&directory)?;
            let metadata = directory.metadata().map_err(|_| temp_error())?;
            directory.sync_all().map_err(|_| temp_error())?;
            tmp.sync_all().map_err(|_| temp_error())?;
            let path = root
                .anchor
                .canonical_path
                .join(PRIVATE_TEMP_ROOT)
                .join(PRIVATE_TEMP_DIR)
                .join(&name);
            Ok(Self {
                directory,
                parent: tmp,
                name,
                path,
                identity: (device(&metadata), inode(&metadata)),
            })
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Best-effort explicit cleanup.  A failed or bounded cleanup intentionally
    /// leaves the tree in place for diagnostics.
    pub fn cleanup(&mut self) -> Result<(), PolicyViolation> {
        #[cfg(not(unix))]
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        #[cfg(unix)]
        {
            cleanup_private(self)
        }
    }
}

impl Drop for PrivateRunTemp {
    fn drop(&mut self) {
        let _ = self.cleanup();
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
            if let Err(error) = validate_private_directory(&directory) {
                let _ = unlinkat_raw(parent, name, true);
                return Err(error);
            }
            parent.sync_all().map_err(|_| temp_error())?;
            Ok(directory)
        }
        Err(error) => Err(map_temp_io(error)),
    }
}

#[cfg(unix)]
fn validate_private_directory(directory: &File) -> Result<(), PolicyViolation> {
    let metadata = directory.metadata().map_err(|_| temp_error())?;
    use std::os::unix::fs::MetadataExt;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() as u32 }
        || metadata.mode() & 0o777 != 0o700
    {
        return Err(temp_error());
    }
    Ok(())
}

#[cfg(unix)]
fn mkdirat_private(parent: &File, name: &OsStr) -> Result<(), PolicyViolation> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    let name = std::ffi::CString::new(name.as_bytes()).map_err(|_| temp_error())?;
    let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
    if result == 0 {
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
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

#[cfg(unix)]
fn unlinkat_raw(parent: &File, name: &OsStr, directory: bool) -> io::Result<()> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL"))?;
    let flags = if directory { libc::AT_REMOVEDIR } else { 0 };
    let result = unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), flags) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn cleanup_private(temp: &mut PrivateRunTemp) -> Result<(), PolicyViolation> {
    let mut state = CleanupState { entries: 0 };
    cleanup_directory(&temp.directory, 0, &mut state)?;
    if !same_generation(&temp.parent, &temp.name, temp.identity)? {
        return Err(temp_error());
    }
    unlinkat_raw(&temp.parent, &temp.name, true).map_err(map_temp_io)?;
    temp.parent.sync_all().map_err(|_| temp_error())?;
    Ok(())
}

#[cfg(unix)]
struct CleanupState {
    entries: usize,
}

#[cfg(unix)]
fn cleanup_directory(
    directory: &File,
    depth: usize,
    state: &mut CleanupState,
) -> Result<(), PolicyViolation> {
    if depth > MAX_CLEANUP_DEPTH {
        return Err(temp_error());
    }
    let names = read_directory_names(directory)?;
    for name in names {
        state.entries += 1;
        if state.entries > MAX_CLEANUP_ENTRIES {
            return Err(temp_error());
        }
        let item = statat_nofollow(directory, &name).map_err(map_temp_io)?;
        if item.is_dir {
            let child = open_directory_nofollow(directory, &name).map_err(map_temp_io)?;
            validate_private_child(&child)?;
            let child_identity = child_identity(&child)?;
            cleanup_directory(&child, depth + 1, state)?;
            if !same_generation(directory, &name, child_identity)? {
                return Err(temp_error());
            }
            unlinkat_raw(directory, &name, true).map_err(map_temp_io)?;
        } else {
            unlinkat_raw(directory, &name, false).map_err(map_temp_io)?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn validate_private_child(directory: &File) -> Result<(), PolicyViolation> {
    let metadata = directory.metadata().map_err(|_| temp_error())?;
    use std::os::unix::fs::MetadataExt;
    if metadata.uid() != unsafe { libc::geteuid() as u32 } || metadata.mode() & 0o022 != 0 {
        return Err(temp_error());
    }
    Ok(())
}

#[cfg(unix)]
fn child_identity(directory: &File) -> Result<(u64, u64), PolicyViolation> {
    let metadata = directory.metadata().map_err(|_| temp_error())?;
    Ok((device(&metadata), inode(&metadata)))
}

#[cfg(unix)]
fn same_generation(
    parent: &File,
    name: &OsStr,
    expected: (u64, u64),
) -> Result<bool, PolicyViolation> {
    let stat = statat_nofollow(parent, name).map_err(map_temp_io)?;
    Ok(stat.is_dir && (stat.device, stat.inode) == expected)
}

#[cfg(unix)]
struct StatAt {
    is_dir: bool,
    device: u64,
    inode: u64,
}

#[cfg(unix)]
fn statat_nofollow(parent: &File, name: &OsStr) -> io::Result<StatAt> {
    use std::{mem::MaybeUninit, os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL"))?;
    let mut stat = MaybeUninit::<libc::stat>::zeroed();
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    Ok(StatAt {
        is_dir: stat.st_mode & libc::S_IFMT == libc::S_IFDIR,
        device: stat.st_dev as u64,
        inode: stat.st_ino as u64,
    })
}

#[cfg(unix)]
fn read_directory_names(directory: &File) -> Result<Vec<OsString>, PolicyViolation> {
    use std::{ffi::CStr, os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    let duplicate = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(temp_error());
    }
    let stream = unsafe { libc::fdopendir(duplicate) };
    if stream.is_null() {
        unsafe { libc::close(duplicate) };
        return Err(temp_error());
    }
    let mut names = Vec::new();
    let mut read_error = None;
    unsafe { *last_errno() = 0 };
    loop {
        let entry = unsafe { libc::readdir(stream) };
        if entry.is_null() {
            let errno = unsafe { *last_errno() };
            if errno != 0 {
                read_error = Some(io::Error::from_raw_os_error(errno));
            }
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        let bytes = name.to_bytes();
        if bytes == b"." || bytes == b".." {
            continue;
        }
        if bytes.is_empty() || bytes.contains(&0) {
            read_error = Some(io::Error::new(io::ErrorKind::InvalidData, "invalid name"));
            break;
        }
        names.push(OsString::from(OsStr::from_bytes(bytes)));
        if names.len() > MAX_CLEANUP_ENTRIES {
            read_error = Some(io::Error::new(io::ErrorKind::Other, "entry bound"));
            break;
        }
    }
    unsafe { libc::closedir(stream) };
    read_error.map_or(Ok(names), |_| Err(temp_error()))
}

#[cfg(all(unix, target_os = "macos"))]
unsafe fn last_errno() -> *mut libc::c_int {
    libc::__error()
}

#[cfg(all(unix, not(target_os = "macos")))]
unsafe fn last_errno() -> *mut libc::c_int {
    libc::__errno_location()
}

#[cfg(unix)]
fn map_temp_io(error: io::Error) -> PolicyViolation {
    let _ = error;
    temp_error()
}

#[cfg(unix)]
fn device(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.dev()
}

#[cfg(unix)]
fn inode(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.ino()
}
