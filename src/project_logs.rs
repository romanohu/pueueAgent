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
        PolicyViolationStage, VerifiedProjectRoot,
    },
    logs::LogSnapshot,
    AppError,
};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

#[cfg(test)]
use crate::execution_policy::ProjectRootAnchor;

#[cfg(test)]
use std::cell::Cell;

const AGENT_DIRECTORY_MODE: u32 = 0o700;
const AGENT_FILE_MODE: u32 = 0o600;
const GATE_MARKER_CONTENT: &[u8] = b"authorized\n";

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MarkerIoFailure {
    Write,
    FileSync,
    BeforePublish,
    DirectorySync,
}

#[derive(Clone, Copy)]
enum MarkerIoStage {
    Write,
    FileSync,
    BeforePublish,
    DirectorySync,
}

#[cfg(test)]
thread_local! {
    static TEST_MARKER_FAILURE: Cell<Option<MarkerIoFailure>> = const { Cell::new(None) };
}

#[cfg(test)]
pub(crate) fn set_test_marker_failure(failure: Option<MarkerIoFailure>) {
    TEST_MARKER_FAILURE.with(|slot| slot.set(failure));
}

#[cfg(test)]
fn take_test_marker_failure(stage: MarkerIoStage) -> Option<io::Error> {
    let expected = match stage {
        MarkerIoStage::Write => MarkerIoFailure::Write,
        MarkerIoStage::FileSync => MarkerIoFailure::FileSync,
        MarkerIoStage::BeforePublish => MarkerIoFailure::BeforePublish,
        MarkerIoStage::DirectorySync => MarkerIoFailure::DirectorySync,
    };
    TEST_MARKER_FAILURE.with(|slot| {
        if slot.get() == Some(expected) {
            slot.set(None);
            Some(io::Error::new(io::ErrorKind::Other, "injected marker I/O failure"))
        } else {
            None
        }
    })
}

#[cfg(not(test))]
fn take_test_marker_failure(_stage: MarkerIoStage) -> Option<io::Error> {
    None
}

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

    /// Test-only fixture constructor. Production callers must pass the
    /// descriptor verified by execution policy through `from_verified`.
    #[cfg(test)]
    pub(crate) fn open_for_tests(path: &Path) -> Result<Self, AppError> {
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
        #[cfg(not(unix))]
        {
            let _ = (self, relative);
            return Err(unsupported_platform(PolicyViolationStage::PreBinding));
        }
        #[cfg(unix)]
        {
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
            let (parent, final_name) = self.open_parent_for(relative)?;
            let file = openat_file(&parent, &final_name, flags, mode).map_err(|source| {
                map_component_open_error(&parent, &final_name, source)
            })?;
            Ok((file, parent))
        }
    }

    #[cfg(unix)]
    fn open_parent_for(&self, relative: &Path) -> Result<(File, std::ffi::OsString), AppError> {
        let components = relative_components(relative)?;
        let final_name = components
            .last()
            .expect("relative_components always returns one component")
            .to_os_string();
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
        Ok((parent, final_name))
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
        let (parent, final_name) = root.open_parent_for(relative)?;
        let (file, private_name) = create_private_marker(&parent)?;
        let identity = match LogFileIdentity::from_open_descriptor(&file) {
            Ok(identity) => identity,
            Err(source) => {
                let primary = AppError::Io {
                    operation: "read gate marker metadata",
                    source,
                };
                cleanup_private_marker(&parent, &private_name);
                return Err(primary);
            }
        };
        if let Err(primary) = validate_owner_only_file(identity) {
            cleanup_private_marker(&parent, &private_name);
            return Err(primary);
        }
        use std::io::Write;
        if let Err(source) = take_test_marker_failure(MarkerIoStage::Write)
            .map_or_else(
                || (&file).write_all(GATE_MARKER_CONTENT),
                Err,
            )
        {
            let primary = AppError::Io {
                operation: "write gate marker",
                source,
            };
            cleanup_private_marker(&parent, &private_name);
            return Err(primary);
        }
        if let Err(source) = take_test_marker_failure(MarkerIoStage::FileSync)
            .map_or_else(|| file.sync_all(), Err)
        {
            let primary = AppError::Io {
                operation: "sync gate marker",
                source,
            };
            cleanup_private_marker(&parent, &private_name);
            return Err(primary);
        }
        if let Some(source) = take_test_marker_failure(MarkerIoStage::BeforePublish) {
            let primary = AppError::Io {
                operation: "publish gate marker",
                source,
            };
            cleanup_private_marker(&parent, &private_name);
            return Err(primary);
        }
        if let Err(source) = link_private_marker(&parent, &private_name, &final_name) {
            let primary = classify_marker_create_error(
                root,
                relative,
                AppError::Io {
                    operation: "publish gate marker",
                    source,
                },
            );
            cleanup_private_marker(&parent, &private_name);
            return Err(primary);
        }
        let unlink_error = unlink_private_marker(&parent, &private_name).err();
        let sync_error = take_test_marker_failure(MarkerIoStage::DirectorySync)
            .map_or_else(|| parent.sync_all().err(), Some);
        if let Some(source) = unlink_error {
            return Err(AppError::Io {
                operation: "remove private gate marker",
                source,
            });
        }
        if let Some(source) = sync_error {
            // Publication has already completed. Keep the valid final marker
            // so recovery can conservatively observe execution uncertainty.
            return Err(AppError::Io {
                operation: "sync gate marker directory",
                source,
            });
        }
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
static PRIVATE_MARKER_COUNTER: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(unix)]
fn create_private_marker(parent: &File) -> Result<(File, std::ffi::OsString), AppError> {
    for _ in 0..32 {
        let name = private_marker_name();
        match openat_file(
            parent,
            &name,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            AGENT_FILE_MODE,
        ) {
            Ok(file) => return Ok((file, name)),
            Err(source) if source.raw_os_error() == Some(libc::EEXIST) => continue,
            Err(source) => {
                return Err(AppError::Io {
                    operation: "create private gate marker",
                    source,
                });
            }
        }
    }
    Err(AppError::Io {
        operation: "create private gate marker",
        source: io::Error::new(
            io::ErrorKind::AlreadyExists,
            "private gate marker name collision",
        ),
    })
}

#[cfg(unix)]
fn private_marker_name() -> std::ffi::OsString {
    use std::os::unix::ffi::OsStringExt;
    let counter = PRIVATE_MARKER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // arc4random is available on both Linux and macOS and provides an
    // unpredictable per-process suffix without adding a dependency.
    let random_a = unsafe { libc::arc4random() };
    let random_b = unsafe { libc::arc4random() };
    let pid = unsafe { libc::getpid() };
    OsStringExt::from_vec(
        format!(
            ".pueue-agent-marker-{pid}-{counter:016x}-{random_a:08x}{random_b:08x}"
        )
        .into_bytes(),
    )
}

#[cfg(unix)]
fn link_private_marker(parent: &File, private_name: &OsStr, final_name: &OsStr) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let private_name = std::ffi::CString::new(private_name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL path component"))?;
    let final_name = std::ffi::CString::new(final_name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL path component"))?;
    // SAFETY: parent is an owned directory descriptor and both names remain
    // alive for this call. linkat publishes the fully-synced private inode
    // without replacing an existing final name.
    let result = unsafe {
        libc::linkat(
            parent.as_raw_fd(),
            private_name.as_ptr(),
            parent.as_raw_fd(),
            final_name.as_ptr(),
            0,
        )
    };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn unlink_private_marker(parent: &File, name: &OsStr) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL path component"))?;
    // SAFETY: parent is an owned directory descriptor and name remains alive
    // for this call. This helper is used only for private temp names.
    let result = unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn cleanup_private_marker(parent: &File, name: &OsStr) {
    let _ = unlink_private_marker(parent, name);
    let _ = parent.sync_all();
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
    let identity = identity_at(parent, name)?;
    Ok(identity.mode & libc::S_IFMT as u32 == libc::S_IFLNK as u32)
}

#[cfg(unix)]
fn identity_at(parent: &File, name: &OsStr) -> io::Result<LogFileIdentity> {
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
    Ok(LogFileIdentity {
        device: stat_buf.st_dev as u64,
        inode: stat_buf.st_ino as u64,
        owner: stat_buf.st_uid as u32,
        mode: stat_buf.st_mode as u32,
    })
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
    // and a symlink. A no-follow fstatat probe preserves the typed symlink
    // distinction without opening a FIFO/device or following any target.
    match root.open_parent_for(relative) {
        Ok((parent, name)) => match is_symlink_at(&parent, &name) {
            Ok(true) => log_unsafe(LogUnsafeReason::Symlink),
            Ok(false) | Err(_) => error,
        },
        Err(_) => error,
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
