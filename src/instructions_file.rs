//! Private descriptor-relative instruction file access and publication.
//!
//! This module intentionally has no public mutation hooks.  It owns all file
//! descriptors, metadata identities, and the checked rename used by the
//! explicit instruction update command.

use std::{
    ffi::OsStr,
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{execution_policy::ProjectRootAnchor, AppError};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::io::AsRawFd;

use super::instructions::classification::MAX_BYTES;

const STATE_DIRECTORY: &str = ".pueue-agent";
const CONFIG_FILE: &str = "config.toml";
const INSTRUCTIONS_FILE: &str = "instructions.md";
const LOCK_FILE: &str = ".instructions.lock";
const BACKUP_DIRECTORY: &str = "instructions.backups";
const FILE_MODE: u32 = 0o600;
const DIRECTORY_MODE: u32 = 0o700;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(crate) struct Inspection {
    pub(crate) canonical_root: PathBuf,
    pub(crate) bytes: Vec<u8>,
}

pub(crate) enum PublishResult {
    Updated(PathBuf),
    Current(Vec<u8>),
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn inspect(project_root: &Path) -> Result<Inspection, AppError> {
    let project = open_project(project_root)?;
    let (file, identity) = open_instructions(&project.state_dir)?;
    verify_named_identity(&project.instructions_path, identity)?;
    let bytes = read_bounded(&file, "read project instructions")?;
    Ok(Inspection {
        canonical_root: project.canonical_root,
        bytes,
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn inspect(_project_root: &Path) -> Result<Inspection, AppError> {
    Err(unsupported_platform())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn publish(
    project_root: &Path,
    expected_before: &[u8],
    candidate: &[u8],
    before_sha256: &str,
) -> Result<PublishResult, AppError> {
    if expected_before.len() > MAX_BYTES || candidate.len() > MAX_BYTES {
        return Err(conflict(
            "instruction input or candidate exceeds 65536 bytes",
        ));
    }
    if std::str::from_utf8(expected_before).is_err() || std::str::from_utf8(candidate).is_err() {
        return Err(conflict(
            "instruction input or candidate is not valid UTF-8",
        ));
    }

    let project = open_project(project_root)?;
    let lock = acquire_lock(&project.state_dir, &project.lock_path)?;
    project.revalidate()?;

    let (original, original_identity) = open_instructions(&project.state_dir)?;
    verify_named_identity(&project.instructions_path, original_identity)?;
    let actual = read_bounded(&original, "read project instructions before apply")?;
    if actual != expected_before {
        if crate::instructions::is_current_distribution(&actual) {
            return Ok(PublishResult::Current(actual));
        }
        return Err(conflict("instruction file changed after preview"));
    }

    let backup_path = backup_original(&project, expected_before, before_sha256)?;
    // The original is checked again after the backup is complete.  A
    // non-cooperating editor can still race a rename; this final check makes
    // all detectable changes fail closed.
    project.revalidate()?;
    let (rechecked, rechecked_identity) = open_instructions(&project.state_dir)?;
    if rechecked_identity != original_identity
        || read_bounded(&rechecked, "recheck project instructions")? != expected_before
    {
        return Err(conflict("instruction file changed during backup"));
    }

    let state_path = project.canonical_root.join(STATE_DIRECTORY);
    let temporary = create_candidate(&project.state_dir, &state_path, candidate)?;
    let temporary_name = temporary.name.clone();
    let temporary_identity = temporary.identity;
    let mut published = false;
    let publish_result = (|| {
        project.revalidate()?;
        let (before_rename, before_rename_identity) = open_instructions(&project.state_dir)?;
        if before_rename_identity != original_identity
            || read_bounded(
                &before_rename,
                "recheck project instructions before publish",
            )? != expected_before
        {
            return Err(conflict("instruction file changed before publish"));
        }
        verify_temporary(
            &project.state_dir,
            &temporary_name,
            temporary_identity,
            candidate,
        )?;
        maybe_swap_candidate_for_tests(&project.state_dir, &temporary_name);
        maybe_fail(FaultStage::Rename)?;
        rename_at(
            &project.state_dir,
            &temporary_name,
            OsStr::new(INSTRUCTIONS_FILE),
        )
        .map_err(|source| map_publish_error("publish instructions", source))?;
        published = true;

        let (after_rename, after_rename_identity) = open_instructions(&project.state_dir)
            .map_err(|error| uncertain_publication(error, &backup_path))?;
        let after_rename_bytes = read_bounded(&after_rename, "verify published instructions")
            .map_err(|error| uncertain_publication(error, &backup_path))?;
        if after_rename_identity != temporary_identity || after_rename_bytes != candidate {
            return Err(uncertain_publication(
                conflict("published instructions could not be verified"),
                &backup_path,
            ));
        }
        maybe_fail(FaultStage::PublicationDirectorySync)
            .map_err(|error| uncertain_publication(error, &backup_path))?;
        project.state_dir.sync_all().map_err(|source| {
            uncertain_publication(
                io_error("sync published instructions", source),
                &backup_path,
            )
        })?;
        Ok(())
    })();
    drop(temporary.file);
    if !published {
        cleanup_owned_entry(
            &project.state_dir,
            &state_path.join(&temporary_name),
            &temporary_name,
            temporary_identity,
        );
    }
    drop(lock);
    publish_result.map(|()| PublishResult::Updated(backup_path))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn publish(
    _project_root: &Path,
    _expected_before: &[u8],
    _candidate: &[u8],
    _before_sha256: &str,
) -> Result<PublishResult, AppError> {
    Err(unsupported_platform())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct OpenProject {
    canonical_root: PathBuf,
    anchor: ProjectRootAnchor,
    root: crate::execution_policy::VerifiedProjectRoot,
    state_dir: File,
    state_identity: Identity,
    instructions_path: PathBuf,
    lock_path: PathBuf,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl OpenProject {
    fn revalidate(&self) -> Result<(), AppError> {
        let verified = self.anchor.verify_identity().map_err(map_policy_error)?;
        let metadata = verified
            .directory
            .metadata()
            .map_err(|source| io_error("inspect project root before publish", source))?;
        if !metadata.is_dir() || identity_from_metadata(&metadata) != self.root_identity() {
            return Err(unsafe_path("project root changed"));
        }
        let state = open_at(
            &verified.directory,
            OsStr::new(STATE_DIRECTORY),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NONBLOCK,
            0,
        )
        .map_err(|source| map_component_error("open project state directory", source))?;
        let state_identity = validate_directory(&state, "project state directory")?;
        if !same_directory_identity(state_identity, self.state_identity) {
            return Err(unsafe_path("project state directory changed"));
        }
        validate_config(&state)?;
        verify_directory_named_identity(
            &self.canonical_root.join(STATE_DIRECTORY),
            state_identity,
        )?;
        Ok(())
    }

    fn root_identity(&self) -> Identity {
        identity_from_file(&self.root.directory).expect("verified root descriptor remains readable")
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_project(project_root: &Path) -> Result<OpenProject, AppError> {
    let canonical_root = project_root
        .canonicalize()
        .map_err(|source| io_error("resolve project path for instruction update", source))?;
    if !canonical_root.is_absolute() {
        return Err(unsafe_path("project path is not absolute"));
    }
    let anchor = ProjectRootAnchor::resolve(&canonical_root).map_err(map_policy_error)?;
    let root = anchor.verify_identity().map_err(map_policy_error)?;
    let _root_identity = identity_from_file(&root.directory)?;
    let state_dir = open_at(
        &root.directory,
        OsStr::new(STATE_DIRECTORY),
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NONBLOCK,
        0,
    )
    .map_err(|source| map_component_error("open project state directory", source))?;
    let state_identity = validate_directory(&state_dir, "project state directory")?;
    verify_directory_named_identity(&canonical_root.join(STATE_DIRECTORY), state_identity)?;
    validate_config(&state_dir)?;
    let instructions_path = canonical_root.join(STATE_DIRECTORY).join(INSTRUCTIONS_FILE);
    let lock_path = canonical_root.join(STATE_DIRECTORY).join(LOCK_FILE);
    Ok(OpenProject {
        canonical_root,
        anchor,
        root,
        state_dir,
        state_identity,
        instructions_path,
        lock_path,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn validate_config(state_dir: &File) -> Result<(), AppError> {
    let config = open_at(
        state_dir,
        OsStr::new(CONFIG_FILE),
        libc::O_RDONLY | libc::O_NONBLOCK,
        0,
    )
    .map_err(|source| {
        if source.raw_os_error() == Some(libc::ENOENT) {
            conflict("project is not initialized")
        } else {
            map_component_error("open project configuration", source)
        }
    })?;
    validate_regular(&config, "project configuration", false).map(|_| ())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_instructions(state_dir: &File) -> Result<(File, Identity), AppError> {
    let file = open_at(
        state_dir,
        OsStr::new(INSTRUCTIONS_FILE),
        libc::O_RDONLY | libc::O_NONBLOCK,
        0,
    )
    .map_err(|source| {
        if source.raw_os_error() == Some(libc::ENOENT) {
            conflict("instruction file is missing")
        } else {
            map_component_error("open project instructions", source)
        }
    })?;
    let identity = validate_regular(&file, "project instructions", true)?;
    Ok((file, identity))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn validate_regular(file: &File, label: &'static str, bounded: bool) -> Result<Identity, AppError> {
    let identity = identity_from_file(file)?;
    if !identity.is_regular() {
        return Err(unsafe_path(label));
    }
    if identity.owner != effective_uid() || identity.nlink != 1 || identity.mode & 0o022 != 0 {
        return Err(unsafe_path(label));
    }
    if bounded
        && file
            .metadata()
            .map_err(|source| io_error("inspect project instructions", source))?
            .len()
            > MAX_BYTES as u64
    {
        return Err(conflict("instruction file exceeds 65536 bytes"));
    }
    Ok(identity)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn validate_directory(file: &File, label: &'static str) -> Result<Identity, AppError> {
    let identity = identity_from_file(file)?;
    if !identity.is_directory() || identity.owner != effective_uid() || identity.mode & 0o022 != 0 {
        return Err(unsafe_path(label));
    }
    Ok(identity)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn read_bounded(file: &File, operation: &'static str) -> Result<Vec<u8>, AppError> {
    let mut bytes = Vec::new();
    file.take((MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|source| io_error(operation, source))?;
    if bytes.len() > MAX_BYTES {
        return Err(conflict("instruction file exceeds 65536 bytes"));
    }
    Ok(bytes)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn acquire_lock(state_dir: &File, lock_path: &Path) -> Result<File, AppError> {
    let mut lock = None;
    let mut created = false;
    let mut last_error = None;
    for _ in 0..8 {
        match open_at(
            state_dir,
            OsStr::new(LOCK_FILE),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_NONBLOCK,
            FILE_MODE,
        ) {
            Ok(opened) => {
                lock = Some(opened);
                created = true;
                break;
            }
            Err(source) if source.raw_os_error() == Some(libc::EEXIST) => match open_at(
                state_dir,
                OsStr::new(LOCK_FILE),
                libc::O_RDWR | libc::O_NONBLOCK,
                0,
            ) {
                Ok(opened) => {
                    lock = Some(opened);
                    break;
                }
                Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
                    last_error = Some(source);
                    std::thread::yield_now();
                    continue;
                }
                Err(source) => {
                    last_error = Some(source);
                    break;
                }
            },
            Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
                last_error = Some(source);
                std::thread::yield_now();
                continue;
            }
            Err(source) => {
                last_error = Some(source);
                break;
            }
        }
    }
    let lock = lock.ok_or_else(|| {
        map_component_error(
            "open instruction update lock",
            last_error.unwrap_or_else(|| io::Error::other("lock open retries exhausted")),
        )
    })?;
    if created {
        set_mode(&lock, FILE_MODE)
            .map_err(|source| io_error("secure instruction update lock", source))?;
    }
    let identity = validate_regular(&lock, "instruction update lock", false)?;
    verify_named_identity(lock_path, identity)?;
    if identity.mode & 0o777 != FILE_MODE {
        return Err(unsafe_path("instruction update lock permissions"));
    }
    maybe_fail(FaultStage::LockAcquire)?;
    notify_before_flock_for_tests();
    let result = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) };
    if result != 0 {
        return Err(io_error(
            "lock instruction update",
            io::Error::last_os_error(),
        ));
    }
    verify_named_identity(lock_path, identity)?;
    Ok(lock)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct TemporaryFile {
    file: File,
    name: std::ffi::OsString,
    identity: Identity,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn backup_original(
    project: &OpenProject,
    original: &[u8],
    before_sha256: &str,
) -> Result<PathBuf, AppError> {
    let backup_directory = open_or_create_backup_directory(&project.state_dir)?;
    let backup_name = format!("{before_sha256}.md");
    let backup_path = project
        .canonical_root
        .join(STATE_DIRECTORY)
        .join(BACKUP_DIRECTORY)
        .join(&backup_name);
    match open_at(
        &backup_directory,
        OsStr::new(&backup_name),
        libc::O_RDONLY | libc::O_NONBLOCK,
        0,
    ) {
        Ok(existing) => {
            let identity = validate_regular(&existing, "instruction backup", true)?;
            if identity.mode & 0o777 != FILE_MODE {
                return Err(unsafe_path("instruction backup permissions"));
            }
            verify_named_identity(&backup_path, identity)?;
            if read_bounded(&existing, "read existing instruction backup")? != original {
                return Err(conflict(
                    "existing instruction backup does not match original",
                ));
            }
            maybe_sync(&existing, FaultStage::BackupFileSync)
                .map_err(|source| io_error("sync existing instruction backup", source))?;
            maybe_sync(&backup_directory, FaultStage::BackupDirectorySync)
                .map_err(|source| io_error("sync existing instruction backup directory", source))?;
            Ok(backup_path)
        }
        Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
            let backup = open_at(
                &backup_directory,
                OsStr::new(&backup_name),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NONBLOCK,
                FILE_MODE,
            )
            .map_err(|source| map_component_error("create instruction backup", source))?;
            if let Err(error) = validate_regular(&backup, "instruction backup", false) {
                cleanup_created_entry(
                    &backup_directory,
                    &backup_path,
                    OsStr::new(&backup_name),
                    &backup,
                );
                return Err(error);
            }
            if let Err(source) = set_mode(&backup, FILE_MODE) {
                cleanup_created_entry(
                    &backup_directory,
                    &backup_path,
                    OsStr::new(&backup_name),
                    &backup,
                );
                return Err(io_error("secure instruction backup", source));
            }
            let identity = match validate_regular(&backup, "instruction backup", false) {
                Ok(identity) => identity,
                Err(error) => {
                    cleanup_created_entry(
                        &backup_directory,
                        &backup_path,
                        OsStr::new(&backup_name),
                        &backup,
                    );
                    return Err(error);
                }
            };
            if identity.nlink != 1 {
                cleanup_created_entry(
                    &backup_directory,
                    &backup_path,
                    OsStr::new(&backup_name),
                    &backup,
                );
                return Err(unsafe_path("instruction backup link count"));
            }
            if let Err(error) = verify_named_identity(&backup_path, identity) {
                cleanup_created_entry(
                    &backup_directory,
                    &backup_path,
                    OsStr::new(&backup_name),
                    &backup,
                );
                return Err(error);
            }
            if let Err(error) = write_and_sync_backup(&backup, original) {
                cleanup_created_entry(
                    &backup_directory,
                    &backup_path,
                    OsStr::new(&backup_name),
                    &backup,
                );
                return Err(error);
            }
            if let Err(source) = maybe_sync(&backup_directory, FaultStage::BackupDirectorySync) {
                // The file itself is complete.  Keep it so a caller can
                // recover even when directory durability is uncertain.
                return Err(io_error("sync instruction backup directory", source));
            }
            Ok(backup_path)
        }
        Err(source) => Err(map_component_error("open instruction backup", source)),
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_or_create_backup_directory(state_dir: &File) -> Result<File, AppError> {
    match open_at(
        state_dir,
        OsStr::new(BACKUP_DIRECTORY),
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NONBLOCK,
        0,
    ) {
        Ok(directory) => {
            let identity = validate_directory(&directory, "instruction backup directory")?;
            if identity.mode & 0o777 != DIRECTORY_MODE {
                return Err(unsafe_path("instruction backup directory permissions"));
            }
            Ok(directory)
        }
        Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {
            mkdir_at(state_dir, OsStr::new(BACKUP_DIRECTORY), DIRECTORY_MODE).map_err(
                |source| map_component_error("create instruction backup directory", source),
            )?;
            let directory = open_at(
                state_dir,
                OsStr::new(BACKUP_DIRECTORY),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NONBLOCK,
                0,
            )
            .map_err(|source| map_component_error("open instruction backup directory", source))?;
            set_mode(&directory, DIRECTORY_MODE)
                .map_err(|source| io_error("secure instruction backup directory", source))?;
            let identity = validate_directory(&directory, "instruction backup directory")?;
            state_dir
                .sync_all()
                .map_err(|source| io_error("sync instruction backup directory", source))?;
            if identity.mode & 0o777 != DIRECTORY_MODE {
                return Err(unsafe_path("instruction backup directory permissions"));
            }
            Ok(directory)
        }
        Err(source) => Err(map_component_error(
            "open instruction backup directory",
            source,
        )),
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn write_and_sync_backup(file: &File, original: &[u8]) -> Result<(), AppError> {
    maybe_fail(FaultStage::BackupWrite)?;
    let mut writer = file;
    writer
        .write_all(original)
        .map_err(|source| io_error("write instruction backup", source))?;
    maybe_fail(FaultStage::BackupFileSync)?;
    file.sync_all()
        .map_err(|source| io_error("sync instruction backup", source))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn create_candidate(
    state_dir: &File,
    state_path: &Path,
    candidate: &[u8],
) -> Result<TemporaryFile, AppError> {
    let pid = std::process::id();
    for _ in 0..16 {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = std::ffi::OsString::from(format!(".instructions.{pid}.{counter}.tmp"));
        let file = match open_at(
            state_dir,
            &name,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NONBLOCK,
            FILE_MODE,
        ) {
            Ok(file) => file,
            Err(source) if source.raw_os_error() == Some(libc::EEXIST) => continue,
            Err(source) => return Err(map_component_error("create instruction candidate", source)),
        };
        if let Err(source) = set_mode(&file, FILE_MODE) {
            cleanup_created_entry(state_dir, &state_path.join(&name), &name, &file);
            return Err(io_error("secure instruction candidate", source));
        }
        let identity = match validate_regular(&file, "instruction candidate", false) {
            Ok(identity) => identity,
            Err(error) => {
                cleanup_created_entry(state_dir, &state_path.join(&name), &name, &file);
                return Err(error);
            }
        };
        if let Err(error) = write_and_sync_candidate(&file, candidate) {
            cleanup_created_entry(state_dir, &state_path.join(&name), &name, &file);
            return Err(error);
        }
        return Ok(TemporaryFile {
            file,
            name,
            identity,
        });
    }
    Err(io_error(
        "create instruction candidate",
        io::Error::new(io::ErrorKind::AlreadyExists, "temporary name collision"),
    ))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn write_and_sync_candidate(file: &File, candidate: &[u8]) -> Result<(), AppError> {
    maybe_fail(FaultStage::CandidateWrite)?;
    let mut writer = file;
    writer
        .write_all(candidate)
        .map_err(|source| io_error("write instruction candidate", source))?;
    maybe_fail(FaultStage::CandidateFileSync)?;
    file.sync_all()
        .map_err(|source| io_error("sync instruction candidate", source))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn verify_temporary(
    state_dir: &File,
    name: &OsStr,
    expected_identity: Identity,
    candidate: &[u8],
) -> Result<(), AppError> {
    let file = open_at(state_dir, name, libc::O_RDONLY | libc::O_NONBLOCK, 0)
        .map_err(|source| map_component_error("verify instruction candidate", source))?;
    let identity = validate_regular(&file, "instruction candidate", true)?;
    if identity != expected_identity
        || read_bounded(&file, "verify instruction candidate")? != candidate
    {
        return Err(unsafe_path("instruction candidate changed"));
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn verify_named_identity(path: &Path, expected: Identity) -> Result<(), AppError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            conflict("project path changed during instruction update")
        } else {
            io_error("inspect instruction path identity", source)
        }
    })?;
    if metadata.file_type().is_symlink() || identity_from_metadata(&metadata) != expected {
        return Err(unsafe_path("instruction path identity changed"));
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn cleanup_created_entry(parent: &File, path: &Path, name: &OsStr, file: &File) {
    if let Ok(identity) = identity_from_file(file) {
        cleanup_owned_entry(parent, path, name, identity);
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn cleanup_owned_entry(parent: &File, path: &Path, name: &OsStr, expected: Identity) {
    if verify_named_identity(path, expected).is_ok() {
        let _ = unlink_at(parent, name);
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn verify_directory_named_identity(path: &Path, expected: Identity) -> Result<(), AppError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            conflict("project path changed during instruction update")
        } else {
            io_error("inspect instruction directory identity", source)
        }
    })?;
    if metadata.file_type().is_symlink()
        || !same_directory_identity(identity_from_metadata(&metadata), expected)
    {
        return Err(unsafe_path("instruction directory identity changed"));
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn rename_at(parent: &File, source: &OsStr, destination: &OsStr) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let source = std::ffi::CString::new(source.as_bytes())?;
    let destination = std::ffi::CString::new(destination.as_bytes())?;
    let result = unsafe {
        libc::renameat(
            parent.as_raw_fd(),
            source.as_ptr(),
            parent.as_raw_fd(),
            destination.as_ptr(),
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn mkdir_at(parent: &File, name: &OsStr, mode: u32) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(name.as_bytes())?;
    let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), mode as libc::mode_t) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn unlink_at(parent: &File, name: &OsStr) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(name.as_bytes())?;
    let result = unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_at(parent: &File, name: &OsStr, flags: i32, mode: u32) -> io::Result<File> {
    use std::os::unix::{ffi::OsStrExt, io::FromRawFd};
    let name = std::ffi::CString::new(name.as_bytes())?;
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
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Identity {
    device: u64,
    inode: u64,
    owner: u32,
    mode: u32,
    nlink: u64,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl Identity {
    fn is_regular(self) -> bool {
        self.mode & libc::S_IFMT as u32 == libc::S_IFREG as u32
    }

    fn is_directory(self) -> bool {
        self.mode & libc::S_IFMT as u32 == libc::S_IFDIR as u32
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn same_directory_identity(left: Identity, right: Identity) -> bool {
    left.device == right.device
        && left.inode == right.inode
        && left.owner == right.owner
        && left.mode == right.mode
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn identity_from_file(file: &File) -> Result<Identity, AppError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = file
        .metadata()
        .map_err(|source| io_error("inspect instruction file identity", source))?;
    Ok(Identity {
        device: metadata.dev(),
        inode: metadata.ino(),
        owner: metadata.uid(),
        mode: metadata.mode(),
        nlink: metadata.nlink(),
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn identity_from_metadata(metadata: &fs::Metadata) -> Identity {
    use std::os::unix::fs::MetadataExt;
    Identity {
        device: metadata.dev(),
        inode: metadata.ino(),
        owner: metadata.uid(),
        mode: metadata.mode(),
        nlink: metadata.nlink(),
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn effective_uid() -> u32 {
    unsafe { libc::geteuid() as u32 }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn set_mode(file: &File, mode: u32) -> io::Result<()> {
    let result = unsafe { libc::fchmod(file.as_raw_fd(), mode as libc::mode_t) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn maybe_sync(file: &File, stage: FaultStage) -> io::Result<()> {
    maybe_fail_io(stage)?;
    file.sync_all()
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn io_error(operation: &'static str, source: io::Error) -> AppError {
    AppError::Io { operation, source }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn map_component_error(operation: &'static str, source: io::Error) -> AppError {
    if matches!(
        source.raw_os_error(),
        Some(errno) if matches!(errno, libc::ELOOP | libc::ENOTDIR | libc::EISDIR)
    ) {
        unsafe_path(operation)
    } else {
        io_error(operation, source)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn map_publish_error(operation: &'static str, source: io::Error) -> AppError {
    if source.raw_os_error() == Some(libc::ELOOP) {
        unsafe_path(operation)
    } else {
        io_error(operation, source)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn uncertain_publication(error: AppError, backup: &Path) -> AppError {
    AppError::Message {
        message: format!(
            "instructions: publication durability uncertain; backup retained at {} ({})",
            backup.display(),
            error
        ),
    }
}

fn conflict(message: &'static str) -> AppError {
    AppError::Message {
        message: format!("instructions: conflict: {message}"),
    }
}

fn unsafe_path(message: &'static str) -> AppError {
    AppError::Message {
        message: format!("instructions: unsafe path: {message}"),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn unsupported_platform() -> AppError {
    AppError::Message {
        message: "instructions: unsupported platform for protected publication".to_owned(),
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn map_policy_error(error: crate::execution_policy::PolicyViolation) -> AppError {
    AppError::Message {
        message: format!("instructions: unsafe path: {}", error),
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FaultStage {
    LockAcquire,
    BackupWrite,
    BackupFileSync,
    BackupDirectorySync,
    CandidateWrite,
    CandidateFileSync,
    Rename,
    PublicationDirectorySync,
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
fn take_fault(stage: FaultStage) -> bool {
    TEST_FAULT.with(|slot| {
        let mut value = slot.borrow_mut();
        if *value == Some(stage) {
            *value = None;
            true
        } else {
            false
        }
    })
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
fn maybe_fail_io(stage: FaultStage) -> io::Result<()> {
    if take_fault(stage) {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "injected instruction I/O failure",
        ))
    } else {
        Ok(())
    }
}

#[cfg(all(not(test), any(target_os = "linux", target_os = "macos")))]
fn maybe_fail_io(_stage: FaultStage) -> io::Result<()> {
    Ok(())
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
fn maybe_fail(stage: FaultStage) -> Result<(), AppError> {
    maybe_fail_io(stage).map_err(|source| io_error("instruction update fault point", source))
}

#[cfg(all(not(test), any(target_os = "linux", target_os = "macos")))]
fn maybe_fail(_stage: FaultStage) -> Result<(), AppError> {
    Ok(())
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
fn maybe_fail_io_for_tests(stage: FaultStage) -> io::Result<()> {
    maybe_fail_io(stage)
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
static TEST_SWAP_CANDIDATE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
fn maybe_swap_candidate_for_tests(state_dir: &File, name: &OsStr) {
    if !TEST_SWAP_CANDIDATE.swap(false, Ordering::SeqCst) {
        return;
    }
    unlink_at(state_dir, name).expect("remove owned candidate for deterministic swap");
    let mut replacement = open_at(
        state_dir,
        name,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NONBLOCK,
        FILE_MODE,
    )
    .expect("create replacement candidate for deterministic swap");
    replacement
        .write_all(b"replacement candidate")
        .expect("write replacement candidate");
    replacement.sync_all().expect("sync replacement candidate");
}

#[cfg(all(not(test), any(target_os = "linux", target_os = "macos")))]
fn maybe_swap_candidate_for_tests(_state_dir: &File, _name: &OsStr) {}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
static TEST_LOCK_BEFORE_FLOCK: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>> =
    std::sync::Mutex::new(None);

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
fn notify_before_flock_for_tests() {
    if let Some(sender) = TEST_LOCK_BEFORE_FLOCK.lock().unwrap().take() {
        let _ = sender.send(());
    }
}

#[cfg(all(not(test), any(target_os = "linux", target_os = "macos")))]
fn notify_before_flock_for_tests() {}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
thread_local! {
    static TEST_FAULT: std::cell::RefCell<Option<FaultStage>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instructions;
    use std::fs;
    use tempfile::tempdir;

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn project() -> (tempfile::TempDir, PathBuf, Vec<u8>) {
        let temporary = tempdir().unwrap();
        let root = temporary.path().join("project");
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join(STATE_DIRECTORY)).unwrap();
        fs::write(root.join(STATE_DIRECTORY).join(CONFIG_FILE), b"[project]\n").unwrap();
        let original = include_bytes!("../templates/legacy/instructions-v0.md").to_vec();
        fs::write(
            root.join(STATE_DIRECTORY).join(INSTRUCTIONS_FILE),
            &original,
        )
        .unwrap();
        (temporary, root, original)
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn private_fault_point_is_consumed_once() {
        let _ = project();
        TEST_FAULT.with(|slot| *slot.borrow_mut() = Some(FaultStage::CandidateWrite));
        assert!(maybe_fail_io_for_tests(FaultStage::CandidateWrite).is_err());
        assert!(maybe_fail_io_for_tests(FaultStage::CandidateWrite).is_ok());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn backup_and_candidate_faults_preserve_the_original() {
        for stage in [
            FaultStage::LockAcquire,
            FaultStage::BackupWrite,
            FaultStage::BackupFileSync,
            FaultStage::BackupDirectorySync,
            FaultStage::CandidateWrite,
            FaultStage::CandidateFileSync,
            FaultStage::Rename,
        ] {
            let (_temporary, root, original) = project();
            let preview = instructions::update(&root, None).unwrap();
            let token = preview.preview_token.as_deref().unwrap();
            TEST_FAULT.with(|slot| *slot.borrow_mut() = Some(stage));
            let result = instructions::update(&root, Some(token));
            assert!(result.is_err(), "fault stage {stage:?} unexpectedly passed");
            assert_eq!(
                fs::read(root.join(STATE_DIRECTORY).join(INSTRUCTIONS_FILE)).unwrap(),
                original
            );
            TEST_FAULT.with(|slot| *slot.borrow_mut() = None);
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn reusable_backup_is_synced_before_retry_after_directory_sync_fault() {
        let (_temporary, root, original) = project();
        let preview = instructions::update(&root, None).unwrap();
        let token = preview.preview_token.as_deref().unwrap().to_owned();

        TEST_FAULT.with(|slot| *slot.borrow_mut() = Some(FaultStage::BackupDirectorySync));
        assert!(instructions::update(&root, Some(&token)).is_err());
        let backup_dir = root.join(STATE_DIRECTORY).join(BACKUP_DIRECTORY);
        assert!(backup_dir.is_dir());
        assert_eq!(
            fs::read(root.join(STATE_DIRECTORY).join(INSTRUCTIONS_FILE)).unwrap(),
            original
        );

        TEST_FAULT.with(|slot| *slot.borrow_mut() = Some(FaultStage::BackupDirectorySync));
        assert!(instructions::update(&root, Some(&token)).is_err());
        assert_eq!(
            fs::read(root.join(STATE_DIRECTORY).join(INSTRUCTIONS_FILE)).unwrap(),
            original
        );

        assert!(instructions::update(&root, Some(&token)).is_ok());
        assert_eq!(
            fs::read(root.join(STATE_DIRECTORY).join(INSTRUCTIONS_FILE)).unwrap(),
            include_bytes!("../templates/instructions.md")
        );
        TEST_FAULT.with(|slot| *slot.borrow_mut() = None);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn rename_fault_cleans_only_the_owned_candidate() {
        let (_temporary, root, original) = project();
        let preview = instructions::update(&root, None).unwrap();
        let token = preview.preview_token.as_deref().unwrap();
        TEST_FAULT.with(|slot| *slot.borrow_mut() = Some(FaultStage::Rename));
        let result = instructions::update(&root, Some(token));
        assert!(result.is_err());
        assert_eq!(
            fs::read(root.join(STATE_DIRECTORY).join(INSTRUCTIONS_FILE)).unwrap(),
            original
        );
        let temporary_count = fs::read_dir(root.join(STATE_DIRECTORY))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".instructions.")
                    && entry.file_name().to_string_lossy().ends_with(".tmp")
            })
            .count();
        assert_eq!(temporary_count, 0);
        TEST_FAULT.with(|slot| *slot.borrow_mut() = None);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn candidate_cleanup_retains_a_replacement_at_the_named_path() {
        let (_temporary, root, original) = project();
        let preview = instructions::update(&root, None).unwrap();
        let token = preview.preview_token.as_deref().unwrap();
        TEST_SWAP_CANDIDATE.store(true, std::sync::atomic::Ordering::SeqCst);
        TEST_FAULT.with(|slot| *slot.borrow_mut() = Some(FaultStage::Rename));

        let result = instructions::update(&root, Some(token));
        assert!(result.is_err());
        assert_eq!(
            fs::read(root.join(STATE_DIRECTORY).join(INSTRUCTIONS_FILE)).unwrap(),
            original
        );
        let replacements = fs::read_dir(root.join(STATE_DIRECTORY))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".instructions.")
                    && entry.file_name().to_string_lossy().ends_with(".tmp")
            })
            .collect::<Vec<_>>();
        assert_eq!(replacements.len(), 1);
        assert_eq!(
            fs::read(replacements[0].path()).unwrap(),
            b"replacement candidate"
        );
        TEST_SWAP_CANDIDATE.store(false, std::sync::atomic::Ordering::SeqCst);
        TEST_FAULT.with(|slot| *slot.borrow_mut() = None);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn waiting_lock_replacement_is_rejected_after_flock() {
        use std::os::unix::fs::PermissionsExt;
        use std::sync::mpsc;
        use std::time::Duration;

        let (_temporary, root, original) = project();
        let preview = instructions::update(&root, None).unwrap();
        let token = preview.preview_token.as_deref().unwrap().to_owned();
        let state_path = root.join(STATE_DIRECTORY);
        let lock_path = state_path.join(LOCK_FILE);
        fs::write(&lock_path, b"").unwrap();
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(FILE_MODE)).unwrap();
        let holder = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        assert_eq!(unsafe { libc::flock(holder.as_raw_fd(), libc::LOCK_EX) }, 0);

        let (sender, receiver) = mpsc::channel();
        *TEST_LOCK_BEFORE_FLOCK.lock().unwrap() = Some(sender);
        let worker_root = root.clone();
        let worker = std::thread::spawn(move || instructions::update(&worker_root, Some(&token)));
        receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("waiting updater did not reach flock");

        let old_lock_path = state_path.join(".instructions.lock.old");
        fs::rename(&lock_path, &old_lock_path).unwrap();
        fs::write(&lock_path, b"").unwrap();
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(FILE_MODE)).unwrap();
        drop(holder);

        let result = worker.join().unwrap();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("instructions: unsafe"));
        assert_eq!(
            fs::read(state_path.join(INSTRUCTIONS_FILE)).unwrap(),
            original
        );
        let _ = fs::remove_file(old_lock_path);
        let _ = fs::remove_file(lock_path);
        *TEST_LOCK_BEFORE_FLOCK.lock().unwrap() = None;
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn publication_sync_fault_reports_uncertainty_and_retains_backup() {
        let (_temporary, root, original) = project();
        let preview = instructions::update(&root, None).unwrap();
        let token = preview.preview_token.as_deref().unwrap();
        TEST_FAULT.with(|slot| *slot.borrow_mut() = Some(FaultStage::PublicationDirectorySync));
        let result = instructions::update(&root, Some(token));
        let error = result.unwrap_err().to_string();
        assert!(error.contains("publication durability uncertain"));
        let updated = fs::read(root.join(STATE_DIRECTORY).join(INSTRUCTIONS_FILE)).unwrap();
        assert_eq!(updated, include_bytes!("../templates/instructions.md"));
        let backup_dir = root.join(STATE_DIRECTORY).join(BACKUP_DIRECTORY);
        assert!(fs::read_dir(backup_dir).unwrap().next().is_some());
        assert_ne!(updated, original);
        TEST_FAULT.with(|slot| *slot.borrow_mut() = None);
    }
}
