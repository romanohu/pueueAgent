use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    ffi::OsString,
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

#[cfg(any(
    target_os = "linux",
    target_os = "macos"
))]
use std::os::unix::{
    ffi::{OsStrExt, OsStringExt},
    io::{AsRawFd, FromRawFd},
};

#[cfg(all(
    test,
    any(
        target_os = "linux",
        target_os = "macos"
    )
))]
use std::sync::Mutex;

use serde::Deserialize;
use uuid::Uuid;

use crate::{
    execution_policy::{PolicyViolation, PolicyViolationCode, PolicyViolationStage},
    AppError,
};

const SESSION_STORES: [&str; 2] = ["sessions", "archived_sessions"];
const MAX_SESSION_STORE_DEPTH: usize = 32;
const MAX_SESSION_STORE_ENTRIES: usize = 4096;
const MAX_SESSION_METADATA_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionTraversalHookPoint {
    Store,
    NestedDirectory,
}

#[cfg(all(
    test,
    any(
        target_os = "linux",
        target_os = "macos"
    )
))]
#[derive(Debug)]
struct SessionTraversalHookState {
    point: SessionTraversalHookPoint,
    target: PathBuf,
    replacement: PathBuf,
}

#[cfg(all(
    test,
    any(
        target_os = "linux",
        target_os = "macos"
    )
))]
static SESSION_TRAVERSAL_HOOK: Mutex<Option<SessionTraversalHookState>> = Mutex::new(None);
#[cfg(all(
    test,
    any(
        target_os = "linux",
        target_os = "macos"
    )
))]
static SESSION_TRAVERSAL_TEST_LOCK: Mutex<()> = Mutex::new(());

#[cfg(all(
    test,
    any(
        target_os = "linux",
        target_os = "macos"
    )
))]
fn invoke_session_traversal_hook(point: SessionTraversalHookPoint, path: &Path) {
    let mut hook = SESSION_TRAVERSAL_HOOK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(state) = hook.as_ref() else {
        return;
    };
    if state.point != point || state.target != path {
        return;
    }
    let state = hook.take().unwrap();
    fs::remove_dir(&state.target).unwrap();
    #[cfg(any(
        target_os = "linux",
        target_os = "macos"
    ))]
    std::os::unix::fs::symlink(&state.replacement, &state.target).unwrap();
}

#[cfg(not(test))]
#[allow(dead_code)]
fn invoke_session_traversal_hook(_point: SessionTraversalHookPoint, _path: &Path) {}

#[derive(Debug)]
struct LatestSessionCandidate {
    id: String,
    path: PathBuf,
    modified_nanos: u128,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Debug)]
struct OpenedSessionMetadata {
    file: File,
}

#[derive(Debug, Deserialize)]
struct SessionMetadata {
    #[serde(rename = "type")]
    kind: String,
    payload: SessionMetadataPayload,
}

#[derive(Debug, Deserialize)]
struct SessionMetadataPayload {
    id: String,
    cwd: PathBuf,
}

pub fn normalize_session_id(value: &str) -> Result<String, AppError> {
    let value = value.trim();
    let parsed = Uuid::parse_str(value).map_err(|_| AppError::Configuration {
        field: "agent.context.session_id",
    })?;
    let normalized = parsed.hyphenated().to_string();
    if value != normalized {
        return Err(AppError::Configuration {
            field: "agent.context.session_id",
        });
    }
    Ok(normalized)
}

pub fn home_from_environment() -> Result<PathBuf, AppError> {
    if let Some(codex_home) = env::var_os("CODEX_HOME") {
        if codex_home.is_empty() {
            return Err(AppError::Configuration {
                field: "CODEX_HOME",
            });
        }
        return Ok(PathBuf::from(codex_home));
    }

    let home = env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .ok_or(AppError::Configuration { field: "HOME" })?;
    Ok(PathBuf::from(home).join(".codex"))
}

pub fn verify_project_ownership(
    codex_home: &Path,
    project_root: &Path,
    session_id: &str,
) -> Result<String, AppError> {
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos"
    )))]
    {
        let _ = (codex_home, project_root, session_id);
        return Err(AppError::from(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::PreBinding,
        )));
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "macos"
    ))]
    verify_project_ownership_unix(codex_home, project_root, session_id)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn verify_project_ownership_unix(
    codex_home: &Path,
    project_root: &Path,
    session_id: &str,
) -> Result<String, AppError> {
    let session_id = normalize_session_id(session_id)?;
    let metadata = locate_metadata(codex_home, &session_id)?;
    let metadata = read_metadata_from_file(metadata.file, &session_id)?;

    if metadata.kind != "session_meta" {
        return Err(metadata_error(
            &session_id,
            "metadata is malformed (first record is not session_meta)",
        ));
    }
    let metadata_id = normalize_metadata_id(&metadata.payload.id, &session_id)?;
    if metadata_id != session_id {
        return Err(metadata_error(
            &session_id,
            "metadata session ID does not match the requested session",
        ));
    }
    if !metadata.payload.cwd.is_absolute() {
        return Err(metadata_error(
            &session_id,
            "metadata is malformed (cwd is not absolute)",
        ));
    }

    let canonical_project_root = canonical_project_root(project_root).map_err(|source| {
        AppError::Io {
            operation: "canonicalize project root for Codex resume",
            source,
        }
    })?;
    let canonical_session_cwd = fs::canonicalize(&metadata.payload.cwd)
        .map_err(|_| metadata_error(&session_id, "metadata cwd cannot be canonicalized"))?;
    if !canonical_session_cwd.starts_with(&canonical_project_root) {
        return Err(metadata_error(
            &session_id,
            "metadata cwd is outside project root",
        ));
    }

    Ok(session_id)
}

/// Resolve the most recently modified valid session owned by `project_root`.
///
/// This is intentionally an explicit, bounded supervisor operation.  It does
/// not use Codex's ambient `--last` state and never falls back to a fresh
/// session.  Candidate metadata is treated as untrusted: malformed, foreign,
/// and symlinked entries are skipped; duplicate valid ownership is rejected.
pub fn resolve_latest_owned_session(
    codex_home: &Path,
    project_root: &Path,
) -> Result<String, PolicyViolation> {
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos"
    )))]
    {
        let _ = (codex_home, project_root);
        return Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::PreBinding,
        ));
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "macos"
    ))]
    resolve_latest_owned_session_unix(codex_home, project_root)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn resolve_latest_owned_session_unix(
    codex_home: &Path,
    project_root: &Path,
) -> Result<String, PolicyViolation> {
    let canonical_project_root = canonical_project_root(project_root).map_err(|_| {
        PolicyViolation::new(
            PolicyViolationCode::RootChanged,
            PolicyViolationStage::PreBinding,
        )
    })?;
    let mut remaining_entries = MAX_SESSION_STORE_ENTRIES;
    let mut candidates = BTreeMap::<String, LatestSessionCandidate>::new();
    let mut seen_ids = BTreeSet::<String>::new();

    let (codex_home, codex_home_path) =
        open_session_home(codex_home).map_err(|_| session_not_owned())?;
    for store_name in SESSION_STORES {
        let store_path = codex_home_path.join(store_name);
        invoke_session_traversal_hook(SessionTraversalHookPoint::Store, &store_path);
        let store = match open_directory_child_nofollow(&codex_home, std::ffi::OsStr::new(store_name)) {
            Ok(store) => store,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(session_not_owned()),
        };
        let mut visit = |path: &Path,
                         _entry: &std::ffi::OsStr,
                         opened: Option<(File, fs::Metadata)>| {
            let Some((file, file_metadata)) = opened else {
                return Ok(());
            };
            if !file_metadata.is_file() {
                return Ok(());
            }
            let Some(filename_id) = filename_session_id(path) else {
                return Ok(());
            };
            let Ok(modified) = file_metadata.modified() else {
                return Ok(());
            };
            let Ok(modified_nanos) = modified.duration_since(UNIX_EPOCH) else {
                return Ok(());
            };
            let Ok(metadata) = read_metadata_from_file(file, &filename_id) else {
                return Ok(());
            };
            if metadata.kind != "session_meta"
                || normalize_metadata_id(&metadata.payload.id, &filename_id)
                    .ok()
                    .as_deref()
                    != Some(filename_id.as_str())
                || !metadata.payload.cwd.is_absolute()
            {
                return Ok(());
            }
            if !seen_ids.insert(filename_id.clone()) {
                return Err(());
            }
            let Ok(canonical_cwd) = fs::canonicalize(&metadata.payload.cwd) else {
                return Ok(());
            };
            if !canonical_cwd.starts_with(&canonical_project_root) {
                return Ok(());
            }
            let candidate = LatestSessionCandidate {
                id: filename_id.clone(),
                path: path.to_path_buf(),
                modified_nanos: modified_nanos.as_nanos(),
            };
            if candidates.insert(filename_id, candidate).is_some() {
                return Err(());
            }
            Ok(())
        };
        walk_session_directory(
            &store,
            &store_path,
            0,
            &mut remaining_entries,
            &mut visit,
        )
        .map_err(|_| session_not_owned())?;
    }

    candidates
        .into_values()
        .max_by(|left, right| {
            left.modified_nanos
                .cmp(&right.modified_nanos)
                .then_with(|| left.id.cmp(&right.id))
                .then_with(|| left.path.cmp(&right.path))
        })
        .map(|candidate| candidate.id)
        .ok_or_else(session_missing)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionWalkError {
    Io,
    Limit,
    Callback,
}

fn walk_session_directory<F>(
    directory: &File,
    display_directory: &Path,
    depth: usize,
    remaining_entries: &mut usize,
    callback: &mut F,
) -> Result<(), SessionWalkError>
where
    F: FnMut(&Path, &std::ffi::OsStr, Option<(File, fs::Metadata)>) -> Result<(), ()>,
{
    let mut entries = read_directory_entries(directory).map_err(|_| SessionWalkError::Io)?;
    while let Some(entry) = entries
        .next_entry()
        .map_err(|_| SessionWalkError::Io)?
    {
        if *remaining_entries == 0 {
            return Err(SessionWalkError::Limit);
        }
        *remaining_entries -= 1;

        let path = display_directory.join(&entry);
        let opened = match open_child_nofollow(directory, &entry) {
            Ok(file) => {
                let metadata = file.metadata().map_err(|_| SessionWalkError::Io)?;
                Some((file, metadata))
            }
            Err(_) => None,
        };
        if opened
            .as_ref()
            .is_some_and(|(_, metadata)| metadata.is_dir())
        {
            if depth == MAX_SESSION_STORE_DEPTH {
                return Err(SessionWalkError::Limit);
            }
            invoke_session_traversal_hook(SessionTraversalHookPoint::NestedDirectory, &path);
            let (file, _) = opened.expect("directory entry was just checked");
            walk_session_directory(&file, &path, depth + 1, remaining_entries, callback)?;
        } else {
            callback(&path, &entry, opened).map_err(|_| SessionWalkError::Callback)?;
        }
    }
    Ok(())
}

fn filename_session_id(path: &Path) -> Option<String> {
    let filename = path.file_name()?.to_str()?;
    let stem = filename.strip_suffix(".jsonl")?;
    stem.match_indices('-')
        .filter_map(|(index, _)| stem.get(index + 1..))
        .find_map(|candidate| normalize_session_id(candidate).ok())
}

fn session_missing() -> PolicyViolation {
    PolicyViolation::new(
        PolicyViolationCode::SessionMissing,
        PolicyViolationStage::PreBinding,
    )
}

fn session_not_owned() -> PolicyViolation {
    PolicyViolation::new(
        PolicyViolationCode::SessionNotOwned,
        PolicyViolationStage::PreBinding,
    )
}

fn locate_metadata(codex_home: &Path, session_id: &str) -> Result<OpenedSessionMetadata, AppError> {
    locate_metadata_with_limit(
        codex_home,
        session_id,
        MAX_SESSION_STORE_ENTRIES,
    )
}

fn locate_metadata_with_limit(
    codex_home: &Path,
    session_id: &str,
    max_entries: usize,
) -> Result<OpenedSessionMetadata, AppError> {
    let suffix = format!("-{session_id}.jsonl");
    let mut matched_path = None;
    let mut callback_failure = None;
    let mut remaining_entries = max_entries;

    let (codex_home, codex_home_path) = open_session_home(codex_home)
        .map_err(|_| metadata_error(session_id, "session store is unreadable"))?;
    for store_name in SESSION_STORES {
        let store_path = codex_home_path.join(store_name);
        invoke_session_traversal_hook(SessionTraversalHookPoint::Store, &store_path);
        let store = match open_directory_child_nofollow(&codex_home, std::ffi::OsStr::new(store_name)) {
            Ok(store) => store,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) if error.kind() == std::io::ErrorKind::NotADirectory => {
                return Err(metadata_error(
                    session_id,
                    "session store is not a directory",
                ));
            }
            Err(_) => return Err(metadata_error(session_id, "session store is unreadable")),
        };
        let mut visit = |_path: &Path,
                         entry: &std::ffi::OsStr,
                         opened: Option<(File, fs::Metadata)>| {
            if !entry.to_str().is_some_and(|name| name.ends_with(&suffix)) {
                return Ok(());
            }
            let Some((file, metadata)) = opened else {
                callback_failure = Some("metadata path is not a regular file");
                return Err(());
            };
            if !metadata.is_file() {
                callback_failure = Some("metadata path is not a regular file");
                return Err(());
            }
            if matched_path.replace(OpenedSessionMetadata { file }).is_some() {
                callback_failure = Some("metadata is ambiguous across local session stores");
                return Err(());
            }
            Ok(())
        };
        let result = walk_session_directory(
            &store,
            &store_path,
            0,
            &mut remaining_entries,
            &mut visit,
        );
        if let Err(error) = result {
            return match error {
                SessionWalkError::Limit => Err(metadata_error(
                    session_id,
                    "metadata discovery exceeded the traversal limit",
                )),
                SessionWalkError::Callback => Err(metadata_error(
                    session_id,
                    callback_failure.unwrap_or("session store is unreadable"),
                )),
                SessionWalkError::Io => Err(metadata_error(
                    session_id,
                    "session store is unreadable",
                )),
            };
        }
    }

    matched_path.ok_or_else(|| metadata_error(session_id, "metadata was not found in CODEX_HOME"))
}

fn read_metadata_from_file(
    file: File,
    session_id: &str,
) -> Result<SessionMetadata, AppError> {
    let reader = BufReader::new(file);
    let mut limited = reader.take((MAX_SESSION_METADATA_BYTES + 1) as u64);
    let mut first_record = Vec::new();
    limited
        .read_until(b'\n', &mut first_record)
        .map_err(|_| metadata_error(session_id, "metadata is unreadable"))?;

    if first_record.is_empty() || first_record.len() > MAX_SESSION_METADATA_BYTES {
        return Err(metadata_error(
            session_id,
            "metadata is empty or exceeds the size limit",
        ));
    }

    serde_json::from_slice(&first_record)
        .map_err(|_| metadata_error(session_id, "metadata is malformed"))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_session_home(codex_home: &Path) -> std::io::Result<(File, PathBuf)> {
    let absolute = if codex_home.is_absolute() {
        codex_home.to_path_buf()
    } else {
        env::current_dir()?.join(codex_home)
    };
    let canonical = fs::canonicalize(&absolute)?;
    let directory = open_directory_path_nofollow(&canonical)?;
    Ok((directory, canonical))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_directory_path_nofollow(path: &Path) -> std::io::Result<File> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::CurDir | std::path::Component::ParentDir))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "session path must be canonical and absolute",
        ));
    }
    let root = std::ffi::CString::new("/").unwrap();
    // SAFETY: root is a static NUL-terminated path; the returned descriptor
    // is immediately owned by File.
    let root_fd = unsafe {
        libc::open(
            root.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if root_fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: root_fd is freshly returned and transferred to File.
    let mut directory = unsafe { File::from_raw_fd(root_fd) };
    for component in path.components() {
        let std::path::Component::Normal(name) = component else {
            continue;
        };
        directory = open_directory_child_nofollow(&directory, name)?;
    }
    Ok(directory)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_directory_child_nofollow(
    parent: &File,
    name: &std::ffi::OsStr,
) -> std::io::Result<File> {
    open_child_with_flags(parent, name, libc::O_DIRECTORY)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_child_nofollow(parent: &File, name: &std::ffi::OsStr) -> std::io::Result<File> {
    open_child_with_flags(parent, name, 0)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_child_with_flags(
    parent: &File,
    name: &std::ffi::OsStr,
    extra_flags: i32,
) -> std::io::Result<File> {
    let bytes = name.as_bytes();
    if bytes.is_empty()
        || bytes == b"."
        || bytes == b".."
        || bytes.len() > 255
        || bytes.contains(&0)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid session directory entry",
        ));
    }
    let name = std::ffi::CString::new(bytes).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid session directory entry")
    })?;
    // SAFETY: parent is an owned directory descriptor and name is a bounded
    // NUL-terminated component valid for this call.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY
                | extra_flags
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK,
            0,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fd is freshly returned by openat and transferred to File.
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct SessionDirectoryEntries {
    directory: *mut libc::DIR,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl SessionDirectoryEntries {
    fn next_entry(&mut self) -> std::io::Result<Option<OsString>> {
        loop {
            set_errno_zero();
            // SAFETY: directory is a live DIR owned by this value.
            let entry = unsafe { libc::readdir(self.directory) };
            if entry.is_null() {
                let errno = last_errno();
                self.close();
                if errno != 0 {
                    return Err(std::io::Error::from_raw_os_error(errno));
                }
                return Ok(None);
            }
            // SAFETY: d_name is a NUL-terminated field in this dirent.
            let bytes = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
            if bytes == b"." || bytes == b".." {
                continue;
            }
            if bytes.is_empty() || bytes.len() > 255 || std::str::from_utf8(bytes).is_err() {
                self.close();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid session directory entry",
                ));
            }
            return Ok(Some(OsString::from_vec(bytes.to_vec())));
        }
    }

    fn close(&mut self) {
        if !self.directory.is_null() {
            // SAFETY: directory is closed at most once by this value.
            unsafe { libc::closedir(self.directory) };
            self.directory = std::ptr::null_mut();
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl Drop for SessionDirectoryEntries {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn read_directory_entries(parent: &File) -> std::io::Result<SessionDirectoryEntries> {
    // Keep the enumeration descriptor close-on-exec as well as the parent
    // descriptor.  fdopendir takes ownership of this duplicate.
    let duplicate = unsafe { libc::fcntl(parent.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: duplicate is an owned directory descriptor transferred to
    // fdopendir; DirectoryEntries closes it exactly once.
    let directory = unsafe { libc::fdopendir(duplicate) };
    if directory.is_null() {
        let error = std::io::Error::last_os_error();
        // SAFETY: fdopendir did not take ownership on failure.
        unsafe { libc::close(duplicate) };
        return Err(error);
    }
    Ok(SessionDirectoryEntries { directory })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn set_errno_zero() {
    #[cfg(target_os = "linux")]
    unsafe {
        *libc::__errno_location() = 0;
    }
    #[cfg(target_os = "macos")]
    unsafe {
        *libc::__error() = 0;
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn last_errno() -> i32 {
    #[cfg(target_os = "linux")]
    unsafe {
        return *libc::__errno_location();
    }
    #[cfg(target_os = "macos")]
    unsafe {
        return *libc::__error();
    }
    #[allow(unreachable_code)]
    0
}

fn canonical_project_root(project_root: &Path) -> std::io::Result<PathBuf> {
    let canonical = fs::canonicalize(project_root)?;
    let metadata = fs::symlink_metadata(&canonical)?;
    if !metadata.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotADirectory,
            "canonical project root is not a directory",
        ));
    }
    Ok(canonical)
}

fn normalize_metadata_id(value: &str, session_id: &str) -> Result<String, AppError> {
    let parsed = Uuid::parse_str(value)
        .map_err(|_| metadata_error(session_id, "metadata is malformed (invalid session ID)"))?;
    Ok(parsed.hyphenated().to_string())
}

fn metadata_error(session_id: &str, reason: &'static str) -> AppError {
    AppError::CodexSessionMetadata {
        session_id: session_id.to_owned(),
        reason,
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    const SESSION_ID: &str = "019f9f30-5f31-7a40-8e28-bd95e1f6c537";
    const OTHER_SESSION_ID: &str = "019f9f30-a553-7e21-b108-16a5c341f728";

    #[test]
    fn finds_session_metadata_at_the_maximum_discovery_depth() {
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root).unwrap();
        let mut metadata_dir = temp.path().join("codex-home/sessions");
        for depth in 0..MAX_SESSION_STORE_DEPTH {
            metadata_dir = metadata_dir.join(format!("level-{depth}"));
        }
        write_metadata(&metadata_dir, SESSION_ID, SESSION_ID, &project_root);

        let verified =
            verify_project_ownership(&temp.path().join("codex-home"), &project_root, SESSION_ID)
                .unwrap();

        assert_eq!(verified, SESSION_ID);
    }

    #[test]
    fn rejects_metadata_discovery_beyond_the_depth_limit() {
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root).unwrap();
        let mut metadata_dir = temp.path().join("codex-home/sessions");
        for depth in 0..=MAX_SESSION_STORE_DEPTH {
            metadata_dir = metadata_dir.join(format!("level-{depth}"));
        }
        write_metadata(&metadata_dir, SESSION_ID, SESSION_ID, &project_root);

        let error =
            verify_project_ownership(&temp.path().join("codex-home"), &project_root, SESSION_ID)
                .unwrap_err();

        assert!(matches!(
            error,
            AppError::CodexSessionMetadata {
                session_id,
                reason: "metadata discovery exceeded the traversal limit",
            } if session_id == SESSION_ID
        ));
    }

    #[test]
    fn shares_metadata_discovery_entry_limit_across_session_stores() {
        let temp = TempDir::new().unwrap();
        for (store_name, entry_names) in [
            ("sessions", &["one"][..]),
            ("archived_sessions", &["two", "three"][..]),
        ] {
            let store = temp.path().join("codex-home").join(store_name);
            fs::create_dir_all(&store).unwrap();
            for name in entry_names {
                fs::write(store.join(name), "not metadata").unwrap();
            }
        }

        let error = locate_metadata_with_limit(
            &temp.path().join("codex-home"),
            SESSION_ID,
            2,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            AppError::CodexSessionMetadata {
                session_id,
                reason: "metadata discovery exceeded the traversal limit",
            } if session_id == SESSION_ID
        ));
    }

    #[test]
    fn stops_discovery_when_the_second_metadata_candidate_is_found() {
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root).unwrap();
        write_metadata(
            &temp.path().join("codex-home/sessions"),
            SESSION_ID,
            SESSION_ID,
            &project_root,
        );
        write_metadata(
            &temp.path().join("codex-home/archived_sessions"),
            SESSION_ID,
            SESSION_ID,
            &project_root,
        );
        let error = locate_metadata_with_limit(
            &temp.path().join("codex-home"),
            SESSION_ID,
            MAX_SESSION_STORE_ENTRIES,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            AppError::CodexSessionMetadata {
                session_id,
                reason: "metadata is ambiguous across local session stores",
            } if session_id == SESSION_ID
        ));
    }

    #[test]
    fn rejects_metadata_with_a_different_payload_id() {
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root).unwrap();
        write_metadata(
            &temp.path().join("codex-home/sessions"),
            SESSION_ID,
            OTHER_SESSION_ID,
            &project_root,
        );

        let error =
            verify_project_ownership(&temp.path().join("codex-home"), &project_root, SESSION_ID)
                .unwrap_err();

        assert!(matches!(
            error,
            AppError::CodexSessionMetadata {
                session_id,
                reason: "metadata session ID does not match the requested session",
            } if session_id == SESSION_ID
        ));
    }

    #[test]
    fn rejects_a_first_metadata_record_over_the_size_limit() {
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root).unwrap();
        let metadata_dir = temp.path().join("codex-home/sessions");
        fs::create_dir_all(&metadata_dir).unwrap();
        fs::write(
            metadata_dir.join(format!("rollout-test-{SESSION_ID}.jsonl")),
            vec![b' '; MAX_SESSION_METADATA_BYTES + 1],
        )
        .unwrap();

        let error =
            verify_project_ownership(&temp.path().join("codex-home"), &project_root, SESSION_ID)
                .unwrap_err();

        assert!(matches!(
            error,
            AppError::CodexSessionMetadata {
                session_id,
                reason: "metadata is empty or exceeds the size limit",
            } if session_id == SESSION_ID
        ));
    }

    #[test]
    fn metadata_reader_does_not_follow_a_symlink() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let outside = temp.path().join("outside.jsonl");
        let codex_home = temp.path().join("codex-home");
        let candidate = codex_home
            .join("sessions")
            .join(format!("rollout-test-{SESSION_ID}.jsonl"));
        fs::write(
            &outside,
            format!(
                "{}\n",
                json!({
                    "type": "session_meta",
                    "payload": {"id": SESSION_ID, "cwd": temp.path()},
                })
            ),
        )
        .unwrap();
        fs::create_dir_all(candidate.parent().unwrap()).unwrap();
        symlink(&outside, &candidate).unwrap();

        let error = verify_project_ownership(&codex_home, &temp.path(), SESSION_ID).unwrap_err();
        assert!(matches!(
            error,
            AppError::CodexSessionMetadata {
                reason: "metadata path is not a regular file",
                ..
            }
        ));
    }

    #[test]
    fn latest_rejects_store_replacement_before_directory_open() {
        let _serial = SESSION_TRAVERSAL_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        let codex_home = temp.path().join("codex-home");
        let replacement = temp.path().join("replacement-sessions");
        let store = codex_home.join("sessions");
        fs::create_dir_all(&project_root).unwrap();
        fs::create_dir_all(&store).unwrap();
        fs::create_dir_all(codex_home.join("archived_sessions")).unwrap();
        write_metadata(&replacement, SESSION_ID, SESSION_ID, &project_root);
        let _hook = install_session_traversal_replacement(
            SessionTraversalHookPoint::Store,
            store,
            replacement,
        );

        let error = resolve_latest_owned_session(&codex_home, &project_root).unwrap_err();
        assert_eq!(error.code, PolicyViolationCode::SessionNotOwned);
    }

    #[test]
    fn explicit_rejects_store_replacement_before_directory_open() {
        let _serial = SESSION_TRAVERSAL_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        let codex_home = temp.path().join("codex-home");
        let replacement = temp.path().join("replacement-sessions");
        let store = codex_home.join("sessions");
        fs::create_dir_all(&project_root).unwrap();
        fs::create_dir_all(&store).unwrap();
        fs::create_dir_all(codex_home.join("archived_sessions")).unwrap();
        write_metadata(&replacement, SESSION_ID, SESSION_ID, &project_root);
        let _hook = install_session_traversal_replacement(
            SessionTraversalHookPoint::Store,
            store,
            replacement,
        );

        assert!(verify_project_ownership(&codex_home, &project_root, SESSION_ID).is_err());
    }

    #[test]
    fn latest_rejects_nested_directory_replacement_before_openat() {
        let _serial = SESSION_TRAVERSAL_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        let codex_home = temp.path().join("codex-home");
        let nested = codex_home.join("sessions/nested");
        let replacement = temp.path().join("replacement-nested");
        fs::create_dir_all(&project_root).unwrap();
        fs::create_dir_all(&nested).unwrap();
        fs::create_dir_all(codex_home.join("archived_sessions")).unwrap();
        write_metadata(&replacement, SESSION_ID, SESSION_ID, &project_root);
        let _hook = install_session_traversal_replacement(
            SessionTraversalHookPoint::NestedDirectory,
            nested,
            replacement,
        );

        let error = resolve_latest_owned_session(&codex_home, &project_root).unwrap_err();
        assert_eq!(error.code, PolicyViolationCode::SessionMissing);
    }

    #[test]
    fn explicit_rejects_nested_directory_replacement_before_openat() {
        let _serial = SESSION_TRAVERSAL_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        let codex_home = temp.path().join("codex-home");
        let nested = codex_home.join("sessions/nested");
        let replacement = temp.path().join("replacement-nested");
        fs::create_dir_all(&project_root).unwrap();
        fs::create_dir_all(&nested).unwrap();
        fs::create_dir_all(codex_home.join("archived_sessions")).unwrap();
        write_metadata(&replacement, SESSION_ID, SESSION_ID, &project_root);
        let _hook = install_session_traversal_replacement(
            SessionTraversalHookPoint::NestedDirectory,
            nested,
            replacement,
        );

        assert!(verify_project_ownership(&codex_home, &project_root, SESSION_ID).is_err());
    }

    struct SessionTraversalHookGuard;

    impl Drop for SessionTraversalHookGuard {
        fn drop(&mut self) {
            SESSION_TRAVERSAL_HOOK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
        }
    }

    fn install_session_traversal_replacement(
        point: SessionTraversalHookPoint,
        target: PathBuf,
        replacement: PathBuf,
    ) -> SessionTraversalHookGuard {
        *SESSION_TRAVERSAL_HOOK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(SessionTraversalHookState {
            point,
            target: fs::canonicalize(target).unwrap(),
            replacement,
        });
        SessionTraversalHookGuard
    }

    fn write_metadata(directory: &Path, filename_id: &str, payload_id: &str, cwd: &Path) {
        fs::create_dir_all(directory).unwrap();
        fs::write(
            directory.join(format!("rollout-test-{filename_id}.jsonl")),
            format!(
                "{}\n",
                json!({
                    "timestamp": "2026-08-09T00:00:00Z",
                    "type": "session_meta",
                    "payload": {
                        "id": payload_id,
                        "cwd": cwd,
                    }
                })
            ),
        )
        .unwrap();
    }
}
