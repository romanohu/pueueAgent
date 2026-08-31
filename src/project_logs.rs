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

pub(crate) enum ResultManifestOpen {
    Missing,
    Invalid,
    File(File),
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

/// Startup recovery distinguishes an absent marker from a marker final-entry
/// that cannot be trusted after its secure parent has been pinned. Callers
/// must treat `Indeterminate` conservatively; it never asserts that a marker
/// was valid or that execution began.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupGateMarkerInspection {
    Absent,
    Valid,
    Indeterminate,
}

#[derive(Debug)]
pub struct AgentLogFile {
    file: File,
    identity: LogFileIdentity,
    path: PathBuf,
}

/// Private marker-publication authority bound to the exact directory and log
/// generation opened during native-agent spawn. It exposes no raw descriptor
/// and can publish only its prevalidated marker name.
#[cfg(unix)]
pub(crate) struct AgentGateDirectory {
    parent: File,
    parent_identity: LogFileIdentity,
    log_name: std::ffi::OsString,
    marker_name: std::ffi::OsString,
}

impl ProjectRootLogReader {
    /// Production callers pass the descriptor verified by the execution
    /// policy.  No path is reopened by this constructor.
    pub fn from_verified(root: VerifiedProjectRoot) -> Self {
        Self { root }
    }

    /// Revalidate that the canonical project-root path still resolves to the
    /// identity pinned by this reader. The returned descriptor is used only
    /// for the proof; all project log access remains relative to `self.root`.
    pub fn revalidate_root_path_identity(&self) -> Result<(), AppError> {
        self.root
            .anchor
            .verify_identity()
            .map(|_| ())
            .map_err(AppError::from)
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

    /// Open a result-manifest candidate below the pinned project root.
    /// Missing entries and unsafe final entries are classified for discovery;
    /// parent symlinks remain hard policy failures.
    pub(crate) fn open_result_manifest(
        &self,
        relative: &Path,
    ) -> Result<ResultManifestOpen, AppError> {
        #[cfg(not(unix))]
        {
            let _ = (self, relative);
            return Err(unsupported_platform(PolicyViolationStage::PreBinding));
        }
        #[cfg(unix)]
        {
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
                let child = match open_directory_at(&parent, component) {
                    Ok(child) => child,
                    Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
                        return Ok(ResultManifestOpen::Missing);
                    }
                    Err(source) => {
                        return Err(map_result_parent_open_error(&parent, component, source));
                    }
                };
                validate_directory(&child)?;
                parent = child;
            }

            let file = match openat_file(
                &parent,
                &final_name,
                libc::O_RDONLY | libc::O_NONBLOCK,
                0,
            ) {
                Ok(file) => file,
                Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
                    return Ok(ResultManifestOpen::Missing);
                }
                Err(source) => {
                    if result_final_is_nonregular(&parent, &final_name) {
                        return Ok(ResultManifestOpen::Invalid);
                    }
                    return Err(AppError::Io {
                        operation: "open result manifest",
                        source,
                    });
                }
            };
            let identity = LogFileIdentity::from_open_descriptor(&file).map_err(|source| {
                AppError::Io {
                    operation: "read result manifest metadata",
                    source,
                }
            })?;
            if !identity.is_regular() {
                return Ok(ResultManifestOpen::Invalid);
            }
            Ok(ResultManifestOpen::File(file))
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

pub fn ensure_agent_log_dir(root: &ProjectRootLogReader) -> Result<LogFileIdentity, AppError> {
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
        validate_directory(&second)?;
        LogFileIdentity::from_open_descriptor(&second).map_err(|source| AppError::Io {
            operation: "read agent log directory metadata",
            source,
        })
    }
}

/// Inspect the fixed agent-log directory without creating or repairing any
/// component. This is used immediately before marker publication to prove
/// that the directory opened during spawn is still the active generation.
pub fn inspect_agent_log_dir(root: &ProjectRootLogReader) -> Result<LogFileIdentity, AppError> {
    #[cfg(not(unix))]
    {
        let _ = root;
        return Err(unsupported_platform(PolicyViolationStage::NativeGate));
    }
    #[cfg(unix)]
    {
        let first = open_directory_at(&root.root.directory, OsStr::new(".pueue-agent"))
            .map_err(|source| {
                map_required_component_open_error(
                    &root.root.directory,
                    OsStr::new(".pueue-agent"),
                    source,
                )
            })?;
        validate_directory(&first)?;
        let second = open_directory_at(&first, OsStr::new("logs"))
            .map_err(|source| {
                map_required_component_open_error(&first, OsStr::new("logs"), source)
            })?;
        validate_directory(&second)?;
        LogFileIdentity::from_open_descriptor(&second).map_err(|source| AppError::Io {
            operation: "read agent log directory metadata",
            source,
        })
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

/// Open the agent log and retain its checked parent generation for the native
/// launch gate. Log and marker must be distinct final names in the same fixed
/// secure directory.
#[cfg(unix)]
pub(crate) fn open_agent_log_gate(
    root: &ProjectRootLogReader,
    log_relative: &Path,
    marker_relative: &Path,
) -> Result<(AgentLogFile, AgentGateDirectory), AppError> {
    let (log_parent_components, log_name) = split_relative_parent(log_relative)?;
    let (marker_parent_components, marker_name) = split_relative_parent(marker_relative)?;
    if log_parent_components != marker_parent_components
        || log_parent_components.as_slice()
            != [OsStr::new(".pueue-agent"), OsStr::new("logs")]
        || log_name == marker_name
    {
        return Err(log_unsafe(LogUnsafeReason::RootChanged));
    }
    let (file, parent) = root.open_final_with_parent(
        log_relative,
        libc::O_RDWR | libc::O_APPEND | libc::O_CREAT,
        AGENT_FILE_MODE,
    )?;
    validate_directory(&parent)?;
    let parent_identity = LogFileIdentity::from_open_descriptor(&parent).map_err(|source| {
        AppError::Io {
            operation: "read agent gate directory metadata",
            source,
        }
    })?;
    let identity = LogFileIdentity::from_open_descriptor(&file).map_err(|source| AppError::Io {
        operation: "read agent log metadata",
        source,
    })?;
    validate_owner_only_file(identity)?;
    Ok((
        AgentLogFile {
            file,
            identity,
            path: log_relative.to_owned(),
        },
        AgentGateDirectory {
            parent,
            parent_identity,
            log_name,
            marker_name,
        },
    ))
}

#[cfg(unix)]
impl AgentGateDirectory {
    pub(crate) fn revalidate_current(
        &self,
        root: &ProjectRootLogReader,
        expected_log: LogFileIdentity,
    ) -> Result<(), AppError> {
        let current_parent = inspect_agent_log_dir(root)?;
        if current_parent != self.parent_identity {
            return Err(log_unsafe(LogUnsafeReason::RootChanged));
        }
        self.revalidate_retained(expected_log)
    }

    pub(crate) fn publish_marker(&self) -> Result<GateMarkerIdentity, AppError> {
        create_gate_marker_in_parent(&self.parent, &self.marker_name)
    }

    pub(crate) fn revalidate_published(
        &self,
        root: &ProjectRootLogReader,
        expected_log: LogFileIdentity,
        expected_marker: GateMarkerIdentity,
    ) -> Result<(), AppError> {
        self.revalidate_current(root, expected_log)?;
        let marker = inspect_gate_marker_in_parent(&self.parent, &self.marker_name)?
            .ok_or_else(|| log_unsafe(LogUnsafeReason::Missing))?;
        if marker != expected_marker {
            return Err(log_unsafe(LogUnsafeReason::RootChanged));
        }
        Ok(())
    }

    pub(crate) fn inspect_marker(&self) -> Result<Option<GateMarkerIdentity>, AppError> {
        inspect_gate_marker_in_parent(&self.parent, &self.marker_name)
    }

    fn revalidate_retained(&self, expected_log: LogFileIdentity) -> Result<(), AppError> {
        let parent = LogFileIdentity::from_open_descriptor(&self.parent).map_err(|source| {
            AppError::Io {
                operation: "read agent gate directory metadata",
                source,
            }
        })?;
        if parent != self.parent_identity {
            return Err(log_unsafe(LogUnsafeReason::RootChanged));
        }
        let log = inspect_file_identity_in_parent(&self.parent, &self.log_name)?
            .ok_or_else(|| log_unsafe(LogUnsafeReason::Missing))?;
        validate_owner_only_file(log)?;
        if log != expected_log {
            return Err(log_unsafe(LogUnsafeReason::RootChanged));
        }
        Ok(())
    }
}

#[cfg(unix)]
fn split_relative_parent(
    relative: &Path,
) -> Result<(Vec<std::ffi::OsString>, std::ffi::OsString), AppError> {
    let components = relative_components(relative)?;
    let name = components
        .last()
        .expect("validated relative path has a final name")
        .to_os_string();
    let parent = components[..components.len() - 1]
        .iter()
        .map(|component| component.to_os_string())
        .collect();
    Ok((parent, name))
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
        create_gate_marker_in_parent(&parent, &final_name).map_err(|error| {
            classify_marker_create_error(root, relative, error)
        })
    }
}

#[cfg(unix)]
fn create_gate_marker_in_parent(
    parent: &File,
    final_name: &OsStr,
) -> Result<GateMarkerIdentity, AppError> {
    let (file, private_name) = create_private_marker(parent)?;
    let identity = match LogFileIdentity::from_open_descriptor(&file) {
        Ok(identity) => identity,
        Err(source) => {
            let primary = AppError::Io {
                operation: "read gate marker metadata",
                source,
            };
            cleanup_private_marker(parent, &private_name);
            return Err(primary);
        }
    };
    if let Err(primary) = validate_owner_only_file(identity) {
        cleanup_private_marker(parent, &private_name);
        return Err(primary);
    }
    use std::io::Write;
    if let Err(source) = take_test_marker_failure(MarkerIoStage::Write)
        .map_or_else(|| (&file).write_all(GATE_MARKER_CONTENT), Err)
    {
        let primary = AppError::Io {
            operation: "write gate marker",
            source,
        };
        cleanup_private_marker(parent, &private_name);
        return Err(primary);
    }
    if let Err(source) = take_test_marker_failure(MarkerIoStage::FileSync)
        .map_or_else(|| file.sync_all(), Err)
    {
        let primary = AppError::Io {
            operation: "sync gate marker",
            source,
        };
        cleanup_private_marker(parent, &private_name);
        return Err(primary);
    }
    if let Some(source) = take_test_marker_failure(MarkerIoStage::BeforePublish) {
        let primary = AppError::Io {
            operation: "publish gate marker",
            source,
        };
        cleanup_private_marker(parent, &private_name);
        return Err(primary);
    }
    if let Err(source) = link_private_marker(parent, &private_name, final_name) {
        let primary = AppError::Io {
            operation: "publish gate marker",
            source,
        };
        cleanup_private_marker(parent, &private_name);
        return Err(primary);
    }
    let unlink_error = unlink_private_marker(parent, &private_name).err();
    let sync_error = take_test_marker_failure(MarkerIoStage::DirectorySync)
        .map_or_else(|| parent.sync_all().err(), Some);
    if let Some(source) = unlink_error {
        return Err(AppError::Io {
            operation: "remove private gate marker",
            source,
        });
    }
    if let Some(source) = sync_error {
        return Err(AppError::Io {
            operation: "sync gate marker directory",
            source,
        });
    }
    Ok(identity)
}

#[cfg(unix)]
fn inspect_file_identity_in_parent(
    parent: &File,
    name: &OsStr,
) -> Result<Option<LogFileIdentity>, AppError> {
    let file = match openat_file(parent, name, libc::O_RDONLY | libc::O_NONBLOCK, 0) {
        Ok(file) => file,
        Err(source) if source.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
        Err(source) => return Err(map_component_open_error(parent, name, source)),
    };
    LogFileIdentity::from_open_descriptor(&file)
        .map(Some)
        .map_err(|source| AppError::Io {
            operation: "read bound project file metadata",
            source,
        })
}

#[cfg(unix)]
fn inspect_gate_marker_in_parent(
    parent: &File,
    name: &OsStr,
) -> Result<Option<GateMarkerIdentity>, AppError> {
    let file = match openat_file(parent, name, libc::O_RDONLY | libc::O_NONBLOCK, 0) {
        Ok(file) => file,
        Err(source) if source.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
        Err(source) => return Err(map_component_open_error(parent, name, source)),
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

/// Inspect a startup-recovery marker through the fixed agent-log directory.
/// The log parent is opened and identity-checked before and after inspecting
/// only the final marker entry. Parent/root failures are returned to preserve
/// fail-closed, no-mutation recovery; final-entry failures are explicitly
/// represented as indeterminate evidence for conservative recovery handling.
pub fn inspect_startup_gate_marker(
    root: &ProjectRootLogReader,
    relative: &Path,
) -> Result<StartupGateMarkerInspection, AppError> {
    #[cfg(not(unix))]
    {
        let _ = (root, relative);
        return Err(unsupported_platform(PolicyViolationStage::Startup));
    }
    #[cfg(unix)]
    {
        let (parent_components, _) = split_relative_parent(relative)?;
        if parent_components.as_slice() != [OsStr::new(".pueue-agent"), OsStr::new("logs")] {
            return Err(log_unsafe(LogUnsafeReason::RootChanged));
        }

        root.revalidate_root_path_identity()?;
        let pinned_parent = inspect_agent_log_dir(root)?;
        let (parent, name) = root.open_parent_for(relative)?;
        validate_directory(&parent)?;
        let opened_parent = LogFileIdentity::from_open_descriptor(&parent).map_err(|source| {
            AppError::Io {
                operation: "read startup marker parent metadata",
                source,
            }
        })?;
        if opened_parent != pinned_parent {
            return Err(log_unsafe(LogUnsafeReason::RootChanged));
        }

        let final_entry = inspect_gate_marker_in_parent(&parent, &name);
        let current_parent = inspect_agent_log_dir(root)?;
        if current_parent != pinned_parent || current_parent != opened_parent {
            return Err(log_unsafe(LogUnsafeReason::RootChanged));
        }
        root.revalidate_root_path_identity()?;
        match final_entry {
            Ok(Some(_)) => Ok(StartupGateMarkerInspection::Valid),
            Ok(None) => Ok(StartupGateMarkerInspection::Absent),
            Err(_) => Ok(StartupGateMarkerInspection::Indeterminate),
        }
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
    // UUID v4 uses the operating system RNG on every supported Unix target,
    // unlike the BSD-specific arc4random API.
    let random = uuid::Uuid::new_v4().as_u128();
    let pid = unsafe { libc::getpid() };
    OsStringExt::from_vec(
        format!(".pueue-agent-marker-{pid}-{counter:016x}-{random:032x}").into_bytes(),
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
fn map_result_parent_open_error(parent: &File, name: &OsStr, source: io::Error) -> AppError {
    if matches!(
        source.raw_os_error(),
        Some(libc::ELOOP) | Some(libc::ENOTDIR) | Some(libc::EISDIR)
    ) && is_symlink_at(parent, name).unwrap_or(false)
    {
        return log_unsafe(LogUnsafeReason::Symlink);
    }
    AppError::Io {
        operation: "open result manifest",
        source,
    }
}

#[cfg(unix)]
fn result_final_is_nonregular(parent: &File, name: &OsStr) -> bool {
    identity_at(parent, name)
        .map(|identity| !identity.is_regular())
        .unwrap_or(false)
}

#[cfg(unix)]
fn map_required_component_open_error(
    parent: &File,
    name: &OsStr,
    source: io::Error,
) -> AppError {
    if source.raw_os_error() == Some(libc::ENOENT) {
        return log_unsafe(LogUnsafeReason::Missing);
    }
    map_component_open_error(parent, name, source)
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
