//! Descriptor-relative, no-follow access to project-scoped log files.
//!
//! The native launch gate uses this module for the agent log and its durable
//! marker.  Paths are accepted only as relative component lists and every
//! component is opened from the pinned project-root descriptor.

use std::{
    ffi::OsStr,
    fs::File,
    io,
    path::{Component, Path, PathBuf},
};

use crate::{
    execution_policy::{
        LogUnsafeReason, PolicyViolation, PolicyViolationCode, PolicyViolationDetail,
        PolicyViolationStage, ProjectRootAnchor, VerifiedProjectRoot,
    },
    logs::LogSnapshot,
    AppError,
};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

const AGENT_DIRECTORY_MODE: u32 = 0o700;
const AGENT_FILE_MODE: u32 = 0o600;
const GATE_MARKER_CONTENT: &[u8] = b"authorized\n";

pub struct ProjectRootLogReader {
    root: VerifiedProjectRoot,
}

#[derive(Debug)]
pub struct OpenedProjectLog {
    file: File,
    relative_path: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogFileIdentity {
    pub device: u64,
    pub inode: u64,
    pub owner: u32,
    pub mode: u32,
}

/// Marker identity intentionally has the same descriptor-derived shape as a
/// log identity.  Keeping the type distinct at the API boundary is useful to
/// callers even though the kernel fields we compare are identical.
pub type GateMarkerIdentity = LogFileIdentity;

#[derive(Debug)]
pub struct AgentLogFile {
    file: File,
    identity: LogFileIdentity,
    path: PathBuf,
}

impl ProjectRootLogReader {
    /// Production callers pass the descriptor verified by the execution
    /// policy.  No path is reopened by this constructor.
    pub fn from_verified(root: VerifiedProjectRoot) -> Self {
        Self { root }
    }

    /// Fixture/helper constructor.  It performs the same anchor resolution
    /// and immediate identity verification used by production callers.
    pub fn open(path: &Path) -> Result<Self, AppError> {
        #[cfg(not(unix))]
        {
            let _ = path;
            return Err(unsupported_platform(PolicyViolationStage::Startup));
        }
        #[cfg(unix)]
        {
            // The helper is intentionally not a path-security boundary: it
            // normalizes fixture paths (macOS commonly exposes /var as a
            // symlink), then delegates the actual proof to the same anchor
            // resolve/verify pair used by production.  Production consumes
            // `from_verified` and never canonicalizes an ambient path.
            let path = std::fs::canonicalize(path).map_err(|source| AppError::Io {
                operation: "canonicalize project root fixture",
                source,
            })?;
            let anchor = ProjectRootAnchor::resolve(&path).map_err(AppError::from)?;
            let verified = anchor.verify_identity().map_err(AppError::from)?;
            Ok(Self::from_verified(verified))
        }
    }

    /// Open a project-relative regular file for descriptor-based log reads.
    /// This is consumed by the detector task; agent logs use `open_agent_log`
    /// below for the stricter owner-only contract.
    pub fn open_relative(&self, relative: &Path) -> Result<OpenedProjectLog, AppError> {
        let (file, _) = self.open_final_with_parent(
            relative,
            libc::O_RDONLY | libc::O_NONBLOCK,
            0,
        )?;
        let identity = LogFileIdentity::from_open_descriptor(&file).map_err(|source| {
            AppError::Io {
                operation: "read project log metadata",
                source,
            }
        })?;
        if !identity.is_regular() {
            return Err(log_unsafe(identity.nonregular_reason()));
        }
        Ok(OpenedProjectLog {
            file,
            relative_path: relative.to_owned(),
        })
    }

    /// Internal descriptor-relative final-component open used by the native
    /// gate and by the detector.  The containing descriptor is retained for
    /// marker durability and is dropped by callers that do not need it.
    pub(crate) fn open_final(
        &self,
        relative: &Path,
        flags: i32,
        mode: u32,
    ) -> Result<File, AppError> {
        self.open_final_with_parent(relative, flags, mode)
            .map(|(file, _)| file)
    }

    fn open_final_with_parent(
        &self,
        relative: &Path,
        flags: i32,
        mode: u32,
    ) -> Result<(File, File), AppError> {
        #[cfg(not(unix))]
        {
            let _ = (relative, flags, mode);
            return Err(unsupported_platform(PolicyViolationStage::NativeGate));
        }
        #[cfg(unix)]
        {
            let components = relative_components(relative)?;
            let mut parent = self.root.directory.try_clone().map_err(|source| AppError::Io {
                operation: "clone project root descriptor",
                source,
            })?;
            for component in &components[..components.len() - 1] {
                let child = open_directory_at(&parent, component)
                    .map_err(|source| map_component_open_error(&parent, component, source))?;
                validate_directory(&child)?;
                parent = child;
            }
            let final_name = components
                .last()
                .expect("relative_components always returns one component");
            let file = openat_file(&parent, final_name, flags, mode).map_err(|source| {
                map_component_open_error(&parent, final_name, source)
            })?;
            let parent = parent.try_clone().map_err(|source| AppError::Io {
                operation: "clone log parent descriptor",
                source,
            })?;
            Ok((file, parent))
        }
    }
}

impl OpenedProjectLog {
    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn relative_path(&self) -> &Path {
        &self.relative_path
    }

    pub fn snapshot(&self, tail_bytes: u32) -> Result<LogSnapshot, AppError> {
        LogSnapshot::read_tail_from_file(&self.file, tail_bytes)
    }
}

impl LogFileIdentity {
    pub fn from_open_descriptor(file: &File) -> io::Result<Self> {
        #[cfg(not(unix))]
        {
            let _ = file;
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "descriptor log identity requires Unix",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = file.metadata()?;
            Ok(Self {
                device: metadata.dev(),
                inode: metadata.ino(),
                owner: metadata.uid(),
                mode: metadata.mode(),
            })
        }
    }

    pub fn is_regular(&self) -> bool {
        #[cfg(not(unix))]
        {
            false
        }
        #[cfg(unix)]
        {
            self.mode & libc::S_IFMT as u32 == libc::S_IFREG as u32
        }
    }

    pub fn is_regular_owner_only(&self) -> bool {
        #[cfg(not(unix))]
        {
            false
        }
        #[cfg(unix)]
        {
            self.is_regular()
                && self.owner == unsafe { libc::geteuid() as u32 }
                && self.mode & 0o077 == 0
        }
    }

    fn nonregular_reason(self) -> LogUnsafeReason {
        #[cfg(unix)]
        {
            if self.mode & libc::S_IFMT as u32 == libc::S_IFDIR as u32 {
                return LogUnsafeReason::Directory;
            }
        }
        LogUnsafeReason::Device
    }
}

impl AgentLogFile {
    pub fn try_clone(&self) -> Result<File, AppError> {
        self.file.try_clone().map_err(|source| AppError::Io {
            operation: "clone agent log handle",
            source,
        })
    }

    pub fn into_file(self) -> File {
        self.file
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn identity(&self) -> &LogFileIdentity {
        &self.identity
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub fn ensure_agent_log_dir(root: &ProjectRootLogReader) -> Result<(), AppError> {
    #[cfg(not(unix))]
    {
        let _ = root;
        return Err(unsupported_platform(PolicyViolationStage::NativeGate));
    }
    #[cfg(unix)]
    {
        let root_directory = root.root.directory.try_clone().map_err(|source| AppError::Io {
            operation: "clone project root descriptor",
            source,
        })?;
        let first = ensure_directory_at(&root_directory, OsStr::new(".pueue-agent"))?;
        let second = ensure_directory_at(&first, OsStr::new("logs"))?;
        validate_directory(&second)
    }
}

pub fn open_agent_log(
    root: &ProjectRootLogReader,
    relative: &Path,
) -> Result<AgentLogFile, AppError> {
    #[cfg(not(unix))]
    {
        let _ = (root, relative);
        return Err(unsupported_platform(PolicyViolationStage::NativeGate));
    }
    #[cfg(unix)]
    {
        let file = root.open_final(
            relative,
            libc::O_RDWR | libc::O_APPEND | libc::O_CREAT,
            AGENT_FILE_MODE,
        )?;
        let identity = LogFileIdentity::from_open_descriptor(&file).map_err(|source| AppError::Io {
            operation: "read agent log metadata",
            source,
        })?;
        validate_owner_only_file(identity)?;
        Ok(AgentLogFile {
            file,
            identity,
            path: relative.to_owned(),
        })
    }
}

pub fn create_gate_marker(
    root: &ProjectRootLogReader,
    relative: &Path,
) -> Result<GateMarkerIdentity, AppError> {
    #[cfg(not(unix))]
    {
        let _ = (root, relative);
        return Err(unsupported_platform(PolicyViolationStage::NativeGate));
    }
    #[cfg(unix)]
    {
        let (file, parent) = root.open_final_with_parent(
            relative,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            AGENT_FILE_MODE,
        )
        .map_err(|error| classify_marker_create_error(root, relative, error))?;
        let identity = LogFileIdentity::from_open_descriptor(&file).map_err(|source| AppError::Io {
            operation: "read gate marker metadata",
            source,
        })?;
        validate_owner_only_file(identity)?;
        use std::io::Write;
        (&file).write_all(GATE_MARKER_CONTENT).map_err(|source| AppError::Io {
            operation: "write gate marker",
            source,
        })?;
        file.sync_all().map_err(|source| AppError::Io {
            operation: "sync gate marker",
            source,
        })?;
        parent.sync_all().map_err(|source| AppError::Io {
            operation: "sync gate marker directory",
            source,
        })?;
        Ok(identity)
    }
}

pub fn inspect_gate_marker(
    root: &ProjectRootLogReader,
    relative: &Path,
) -> Result<Option<GateMarkerIdentity>, AppError> {
    #[cfg(not(unix))]
    {
        let _ = (root, relative);
        return Err(unsupported_platform(PolicyViolationStage::NativeGate));
    }
    #[cfg(unix)]
    {
        let file = match root.open_final(relative, libc::O_RDONLY | libc::O_NONBLOCK, 0) {
            Ok(file) => file,
            Err(AppError::Io { source, .. })
                if source.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
            Err(error) => return Err(error),
        };
        let identity = LogFileIdentity::from_open_descriptor(&file).map_err(|source| AppError::Io {
            operation: "read gate marker metadata",
            source,
        })?;
        validate_owner_only_file(identity)?;
        use std::io::Read;
        let mut contents = Vec::new();
        (&file)
            .take((GATE_MARKER_CONTENT.len() + 1) as u64)
            .read_to_end(&mut contents)
            .map_err(|source| AppError::Io {
                operation: "read gate marker",
                source,
            })?;
        if contents != GATE_MARKER_CONTENT {
            return Err(log_unsafe(LogUnsafeReason::InvalidContents));
        }
        Ok(Some(identity))
    }
}

pub fn inspect_existing_agent_log(
    root: &ProjectRootLogReader,
    relative: &Path,
) -> Result<LogFileIdentity, AppError> {
    #[cfg(not(unix))]
    {
        let _ = (root, relative);
        return Err(unsupported_platform(PolicyViolationStage::NativeGate));
    }
    #[cfg(unix)]
    {
        let file = match root.open_final(relative, libc::O_RDONLY | libc::O_NONBLOCK, 0) {
            Ok(file) => file,
            Err(AppError::Io { source, .. })
                if source.raw_os_error() == Some(libc::ENOENT) => {
                    return Err(log_unsafe(LogUnsafeReason::Missing));
                }
            Err(error) => return Err(error),
        };
        let identity = LogFileIdentity::from_open_descriptor(&file).map_err(|source| AppError::Io {
            operation: "read agent log metadata",
            source,
        })?;
        validate_owner_only_file(identity)?;
        Ok(identity)
    }
}

fn validate_owner_only_file(identity: LogFileIdentity) -> Result<(), AppError> {
    if !identity.is_regular() {
        return Err(log_unsafe(identity.nonregular_reason()));
    }
    #[cfg(unix)]
    {
        if identity.owner != unsafe { libc::geteuid() as u32 } {
            return Err(log_unsafe(LogUnsafeReason::WrongOwner));
        }
        if identity.mode & 0o077 != 0 {
            return Err(log_unsafe(LogUnsafeReason::WeakPermissions));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_directory_at(parent: &File, name: &OsStr) -> Result<File, AppError> {
    use std::os::unix::ffi::OsStrExt;
    for _ in 0..4 {
        match open_directory_at(parent, name) {
            Ok(directory) => {
                validate_directory(&directory)?;
                return Ok(directory);
            }
            Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
                let name = std::ffi::CString::new(name.as_bytes()).map_err(|_| AppError::Io {
                    operation: "create agent log directory",
                    source: io::Error::new(io::ErrorKind::InvalidInput, "NUL path component"),
                })?;
                // SAFETY: parent is an owned directory descriptor and name is
                // NUL terminated for the duration of the call.
                let result = unsafe {
                    libc::mkdirat(
                        parent.as_raw_fd(),
                        name.as_ptr(),
                        AGENT_DIRECTORY_MODE as libc::mode_t,
                    )
                };
                if result == 0 {
                    continue;
                }
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::EEXIST) {
                    continue;
                }
                return Err(AppError::Io {
                    operation: "create agent log directory",
                    source: error,
                });
            }
            Err(source) => return Err(map_component_open_error(parent, name, source)),
        }
    }
    Err(AppError::Io {
        operation: "create agent log directory",
        source: io::Error::new(io::ErrorKind::AlreadyExists, "directory changed during creation"),
    })
}

#[cfg(unix)]
fn open_directory_at(parent: &File, name: &OsStr) -> io::Result<File> {
    openat_file(parent, name, libc::O_RDONLY | libc::O_DIRECTORY, 0)
}

#[cfg(unix)]
fn openat_file(parent: &File, name: &OsStr, flags: i32, mode: u32) -> io::Result<File> {
    use std::os::unix::{ffi::OsStrExt, io::FromRawFd};
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL path component"))?;
    // SAFETY: parent is an owned descriptor; name remains alive for the call.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            mode,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fd is freshly returned by openat and ownership is transferred
    // exactly once to this File.
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(unix)]
fn validate_directory(directory: &File) -> Result<(), AppError> {
    let identity = LogFileIdentity::from_open_descriptor(directory).map_err(|source| AppError::Io {
        operation: "read agent log directory metadata",
        source,
    })?;
    if !identity.is_directory() {
        return Err(log_unsafe(identity.nonregular_reason()));
    }
    if identity.owner != unsafe { libc::geteuid() as u32 } {
        return Err(log_unsafe(LogUnsafeReason::WrongOwner));
    }
    if identity.mode & 0o022 != 0 {
        return Err(log_unsafe(LogUnsafeReason::WeakPermissions));
    }
    Ok(())
}

#[cfg(unix)]
impl LogFileIdentity {
    fn is_directory(&self) -> bool {
        self.mode & libc::S_IFMT as u32 == libc::S_IFDIR as u32
    }
}

fn relative_components(relative: &Path) -> Result<Vec<&OsStr>, AppError> {
    if relative.as_os_str().is_empty() {
        return Err(log_unsafe(LogUnsafeReason::EmptyPath));
    }
    let mut components = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(name) => components.push(name),
            Component::CurDir => return Err(log_unsafe(LogUnsafeReason::CurDir)),
            Component::ParentDir => return Err(log_unsafe(LogUnsafeReason::ParentTraversal)),
            Component::RootDir | Component::Prefix(_) => {
                return Err(log_unsafe(LogUnsafeReason::AbsolutePath));
            }
        }
    }
    if components.is_empty() {
        return Err(log_unsafe(LogUnsafeReason::EmptyPath));
    }
    Ok(components)
}

fn map_walk_error(source: io::Error) -> AppError {
    match source.raw_os_error() {
        #[cfg(unix)]
        Some(libc::ELOOP) => log_unsafe(LogUnsafeReason::Symlink),
        #[cfg(unix)]
        Some(libc::ENOTDIR) | Some(libc::EISDIR) => log_unsafe(LogUnsafeReason::Directory),
        _ => AppError::Io {
            operation: "open project log path",
            source,
        },
    }
}

#[cfg(unix)]
fn map_component_open_error(parent: &File, name: &OsStr, source: io::Error) -> AppError {
    if matches!(
        source.raw_os_error(),
        Some(libc::ELOOP) | Some(libc::ENOTDIR) | Some(libc::EISDIR)
    )
        && is_symlink_at(parent, name).unwrap_or(false)
    {
        return log_unsafe(LogUnsafeReason::Symlink);
    }
    map_walk_error(source)
}

#[cfg(unix)]
fn is_symlink_at(parent: &File, name: &OsStr) -> io::Result<bool> {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL path component"))?;
    // SAFETY: stat_buf is initialized by fstatat on success; parent is an
    // owned descriptor and name remains alive for the call.
    let mut stat_buf: libc::stat = unsafe { std::mem::zeroed() };
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            &mut stat_buf,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(stat_buf.st_mode & libc::S_IFMT as libc::mode_t == libc::S_IFLNK as libc::mode_t)
}

#[cfg(unix)]
fn classify_marker_create_error(
    root: &ProjectRootLogReader,
    relative: &Path,
    error: AppError,
) -> AppError {
    let AppError::Io { source, .. } = &error else {
        return error;
    };
    if source.raw_os_error() != Some(libc::EEXIST) {
        return error;
    }
    // O_EXCL intentionally reports EEXIST for both a regular existing marker
    // and a symlink.  A no-follow probe lets us preserve the typed symlink
    // distinction without ever opening the marker's target.
    match root.open_final(relative, libc::O_RDONLY, 0) {
        Err(AppError::PolicyViolation { violation })
            if violation.detail
                == PolicyViolationDetail::LogUnsafe(LogUnsafeReason::Symlink) =>
        {
            log_unsafe(LogUnsafeReason::Symlink)
        }
        _ => error,
    }
}

fn log_unsafe(reason: LogUnsafeReason) -> AppError {
    PolicyViolation::with_detail(
        PolicyViolationCode::LogUnsafe,
        PolicyViolationStage::NativeGate,
        PolicyViolationDetail::LogUnsafe(reason),
    )
    .into()
}

#[cfg(not(unix))]
fn unsupported_platform(stage: PolicyViolationStage) -> AppError {
    PolicyViolation::new(PolicyViolationCode::UnsupportedPlatform, stage).into()
}
