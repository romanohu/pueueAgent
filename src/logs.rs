use std::{
    fs,
    io,
    path::Path,
    time::UNIX_EPOCH,
};

#[cfg(unix)]
use std::os::unix::{
    ffi::OsStrExt,
    fs::FileExt,
    io::FromRawFd,
};

use crate::{
    config::bounded_log_tail,
    execution_policy::{
        LogUnsafeReason, PolicyViolation, PolicyViolationCode, PolicyViolationDetail,
        PolicyViolationStage,
    },
    AppError,
};

pub use crate::config::MAX_LOG_TAIL_BYTES;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSnapshot {
    pub byte_size: u64,
    pub modified_at_nanos: Option<u128>,
    pub fingerprint: String,
    pub evidence: String,
}

impl LogSnapshot {
    pub fn read_tail(path: &Path, tail_bytes: u32) -> Result<Self, AppError> {
        bounded_log_tail(i64::from(tail_bytes))?;
        let file = open_read_no_follow(path)?;
        Self::read_tail_from_file(&file, tail_bytes)
    }

    pub fn read_tail_from_file(file: &fs::File, tail_bytes: u32) -> Result<Self, AppError> {
        let tail_len = u64::from(bounded_log_tail(i64::from(tail_bytes))?);
        let metadata = file.metadata().map_err(|source| AppError::Io {
            operation: "read log metadata",
            source,
        })?;
        let byte_size = metadata.len();
        let tail_len = tail_len.min(byte_size);
        let bytes = read_tail_bytes(file, byte_size.saturating_sub(tail_len), tail_len)?;
        let modified_at_nanos = metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos());
        let fingerprint = format!(
            "log:v1:size={byte_size}:mtime={}:tail={:016x}",
            modified_at_nanos
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_owned()),
            fnv1a64(&bytes)
        );

        Ok(Self {
            byte_size,
            modified_at_nanos,
            fingerprint,
            evidence: String::from_utf8_lossy(&bytes).into_owned(),
        })
    }
}

#[cfg(unix)]
fn read_tail_bytes(file: &fs::File, offset: u64, tail_len: u64) -> Result<Vec<u8>, AppError> {
    let length = usize::try_from(tail_len).map_err(|_| AppError::Io {
        operation: "allocate log tail",
        source: io::Error::new(io::ErrorKind::InvalidInput, "log tail is too large"),
    })?;
    let mut bytes = vec![0_u8; length];
    let mut read = 0;
    while read < length {
        match file.read_at(&mut bytes[read..], offset + read as u64) {
            Ok(0) => break,
            Ok(count) => read += count,
            Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(AppError::Io {
                    operation: "read log tail",
                    source,
                });
            }
        }
    }
    bytes.truncate(read);
    Ok(bytes)
}

#[cfg(not(unix))]
fn read_tail_bytes(_file: &fs::File, _offset: u64, _tail_len: u64) -> Result<Vec<u8>, AppError> {
    Err(AppError::PolicyViolation {
        violation: PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::Startup,
        ),
    })
}

fn log_unsafe(reason: LogUnsafeReason) -> AppError {
    PolicyViolation::with_detail(
        PolicyViolationCode::LogUnsafe,
        PolicyViolationStage::NativeGate,
        PolicyViolationDetail::LogUnsafe(reason),
    )
    .into()
}

#[cfg(unix)]
fn open_read_no_follow(path: &Path) -> Result<fs::File, AppError> {
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        AppError::Io {
            operation: "open log file",
            source: io::Error::new(io::ErrorKind::InvalidInput, "log path contains NUL"),
        }
    })?;
    // O_NONBLOCK prevents a FIFO from making the read boundary wait forever;
    // its descriptor is rejected as a non-regular log immediately below.
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        let source = io::Error::last_os_error();
        if source.raw_os_error() == Some(libc::ELOOP) {
            return Err(log_unsafe(LogUnsafeReason::Symlink));
        }
        return Err(AppError::Io {
            operation: "open log file",
            source,
        });
    }
    // SAFETY: fd is freshly returned by open and is owned by this File.
    let file = unsafe { fs::File::from_raw_fd(fd) };
    let metadata = file.metadata().map_err(|source| AppError::Io {
        operation: "read log metadata",
        source,
    })?;
    if metadata.is_dir() {
        return Err(log_unsafe(LogUnsafeReason::Directory));
    }
    if !metadata.is_file() {
        return Err(log_unsafe(LogUnsafeReason::Device));
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_read_no_follow(_path: &Path) -> Result<fs::File, AppError> {
    Err(AppError::PolicyViolation {
        violation: PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::Startup,
        ),
    })
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(all(test, unix))]
mod tests {
    use super::LogSnapshot;
    use crate::{
        execution_policy::{
            LogUnsafeReason, PolicyViolation, PolicyViolationCode, PolicyViolationDetail,
        },
        project_logs::{
            create_gate_marker, ensure_agent_log_dir, inspect_existing_agent_log,
            inspect_gate_marker, open_agent_log, MarkerIoFailure, ProjectRootLogReader,
            set_test_marker_failure,
        },
        AppError,
    };
    use std::{
        fs,
        os::unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, PermissionsExt},
        },
        path::{Path, PathBuf},
        process::Command,
        sync::mpsc,
        time::{Duration, Instant},
    };

    fn assert_log_unsafe(error: AppError, reason: LogUnsafeReason) {
        assert!(matches!(
            &error,
            AppError::PolicyViolation {
                violation: PolicyViolation {
                    code: PolicyViolationCode::LogUnsafe,
                    detail: PolicyViolationDetail::LogUnsafe(actual),
                    ..
                }
            } if *actual == reason
        ), "unexpected error: {error:?}");
    }

    #[test]
    fn secure_agent_log_creation_is_owner_only_regular_and_offset_neutral() {
        let temp = tempfile::tempdir().unwrap();
        let reader = ProjectRootLogReader::open_for_tests(temp.path()).unwrap();
        let relative = Path::new("agent.log");

        let opened = open_agent_log(&reader, relative).unwrap();
        assert_eq!(opened.path(), relative);
        let metadata = opened.file().metadata().unwrap();
        assert!(metadata.is_file());
        assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
        assert_eq!(metadata.mode() & 0o077, 0);
        assert_eq!(metadata.mode() & 0o777, 0o600);

        fs::write(temp.path().join(relative), b"0123456789abcdef").unwrap();
        let clone = opened.try_clone().unwrap();
        let snapshot = LogSnapshot::read_tail_from_file(&clone, 4).unwrap();
        assert_eq!(snapshot.evidence, "cdef");

        fs::rename(temp.path().join(relative), temp.path().join("moved.log")).unwrap();
        fs::write(temp.path().join(relative), b"replacement").unwrap();
        let stable = LogSnapshot::read_tail_from_file(&opened.file(), 16).unwrap();
        assert_eq!(stable.evidence, "0123456789abcdef");
    }

    #[test]
    fn secure_agent_log_rejects_weak_directory_and_symlink_files() {
        let temp = tempfile::tempdir().unwrap();
        let reader = ProjectRootLogReader::open_for_tests(temp.path()).unwrap();
        let weak = temp.path().join("weak.log");
        fs::write(&weak, b"x").unwrap();
        fs::set_permissions(&weak, fs::Permissions::from_mode(0o640)).unwrap();
        assert_log_unsafe(
            open_agent_log(&reader, Path::new("weak.log")).unwrap_err(),
            LogUnsafeReason::WeakPermissions,
        );

        let directory = temp.path().join("directory.log");
        fs::create_dir(&directory).unwrap();
        assert_log_unsafe(
            open_agent_log(&reader, Path::new("directory.log")).unwrap_err(),
            LogUnsafeReason::Directory,
        );

        let outside = temp.path().join("outside.log");
        fs::write(&outside, b"outside").unwrap();
        let link = temp.path().join("link.log");
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        assert_log_unsafe(
            open_agent_log(&reader, Path::new("link.log")).unwrap_err(),
            LogUnsafeReason::Symlink,
        );
    }

    #[test]
    fn secure_agent_log_directory_is_fixed_owner_only_and_no_follow() {
        let temp = tempfile::tempdir().unwrap();
        let reader = ProjectRootLogReader::open_for_tests(temp.path()).unwrap();
        ensure_agent_log_dir(&reader).unwrap();
        for relative in [Path::new(".pueue-agent"), Path::new(".pueue-agent/logs")] {
            let metadata = fs::symlink_metadata(temp.path().join(relative)).unwrap();
            assert!(metadata.is_dir());
            assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
            assert_eq!(metadata.mode() & 0o077, 0);
            assert_eq!(metadata.mode() & 0o777, 0o700);
        }

        let weak_root = tempfile::tempdir().unwrap();
        fs::create_dir(weak_root.path().join(".pueue-agent")).unwrap();
        fs::set_permissions(
            weak_root.path().join(".pueue-agent"),
            fs::Permissions::from_mode(0o770),
        )
        .unwrap();
        let weak_reader = ProjectRootLogReader::open_for_tests(weak_root.path()).unwrap();
        assert_log_unsafe(ensure_agent_log_dir(&weak_reader).unwrap_err(), LogUnsafeReason::WeakPermissions);

        let link_root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), link_root.path().join(".pueue-agent")).unwrap();
        let link_reader = ProjectRootLogReader::open_for_tests(link_root.path()).unwrap();
        assert_log_unsafe(ensure_agent_log_dir(&link_reader).unwrap_err(), LogUnsafeReason::Symlink);
    }

    #[test]
    fn gate_marker_is_exclusive_durable_exact_and_no_follow() {
        let temp = tempfile::tempdir().unwrap();
        let reader = ProjectRootLogReader::open_for_tests(temp.path()).unwrap();
        ensure_agent_log_dir(&reader).unwrap();
        let relative = Path::new(".pueue-agent/logs/agent.log.gate-started");

        let created = create_gate_marker(&reader, relative).unwrap();
        let marker = temp.path().join(relative);
        let metadata = fs::symlink_metadata(&marker).unwrap();
        assert!(metadata.is_file());
        assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
        assert_eq!(metadata.mode() & 0o077, 0);
        assert_eq!(metadata.mode() & 0o777, 0o600);
        assert_eq!(inspect_gate_marker(&reader, relative).unwrap(), Some(created));
        assert!(create_gate_marker(&reader, relative).is_err());
        assert_eq!(inspect_gate_marker(&reader, relative).unwrap(), Some(created));

        fs::write(&marker, b"authorized\nextra").unwrap();
        assert_log_unsafe(
            inspect_gate_marker(&reader, relative).unwrap_err(),
            LogUnsafeReason::InvalidContents,
        );

        fs::remove_file(&marker).unwrap();
        let outside = temp.path().join("outside-marker");
        fs::write(&outside, b"authorized\n").unwrap();
        std::os::unix::fs::symlink(&outside, &marker).unwrap();
        assert_log_unsafe(
            inspect_gate_marker(&reader, relative).unwrap_err(),
            LogUnsafeReason::Symlink,
        );
    }

    #[test]
    fn marker_inspection_distinguishes_missing_and_unsafe_states() {
        let temp = tempfile::tempdir().unwrap();
        let reader = ProjectRootLogReader::open_for_tests(temp.path()).unwrap();
        ensure_agent_log_dir(&reader).unwrap();
        let relative = Path::new(".pueue-agent/logs/missing.gate-started");
        assert_eq!(inspect_gate_marker(&reader, relative).unwrap(), None);
        assert!(!temp.path().join(relative).exists());

        let invalid = Path::new(".pueue-agent/logs/invalid.gate-started");
        fs::write(temp.path().join(invalid), b"authorized\n").unwrap();
        fs::set_permissions(temp.path().join(invalid), fs::Permissions::from_mode(0o640)).unwrap();
        assert_log_unsafe(inspect_gate_marker(&reader, invalid).unwrap_err(), LogUnsafeReason::WeakPermissions);
    }

    #[test]
    fn inspect_existing_agent_log_does_not_create_or_read() {
        let temp = tempfile::tempdir().unwrap();
        let reader = ProjectRootLogReader::open_for_tests(temp.path()).unwrap();
        let missing = Path::new("missing.log");
        assert_log_unsafe(
            inspect_existing_agent_log(&reader, missing).unwrap_err(),
            LogUnsafeReason::Missing,
        );
        assert!(!temp.path().join(missing).exists());

        let sentinel = Path::new("sentinel.log");
        fs::write(temp.path().join(sentinel), b"must never be read\n").unwrap();
        fs::set_permissions(temp.path().join(sentinel), fs::Permissions::from_mode(0o600)).unwrap();
        let identity = inspect_existing_agent_log(&reader, sentinel).unwrap();
        let metadata = fs::metadata(temp.path().join(sentinel)).unwrap();
        assert_eq!(identity.device, metadata.dev());
        assert_eq!(identity.inode, metadata.ino());
        assert_eq!(identity.owner, metadata.uid());
        assert_eq!(identity.mode, metadata.mode());
    }

    #[test]
    fn relative_log_paths_reject_ambiguous_components() {
        let temp = tempfile::tempdir().unwrap();
        let reader = ProjectRootLogReader::open_for_tests(temp.path()).unwrap();
        for (path, reason) in [
            (PathBuf::new(), LogUnsafeReason::EmptyPath),
            (PathBuf::from("."), LogUnsafeReason::CurDir),
            (PathBuf::from(".."), LogUnsafeReason::ParentTraversal),
            (PathBuf::from("/tmp/log"), LogUnsafeReason::AbsolutePath),
        ] {
            assert_log_unsafe(open_agent_log(&reader, &path).unwrap_err(), reason);
        }
    }

    #[test]
    fn test_only_project_root_constructor_compiles() {
        let temp = tempfile::tempdir().unwrap();
        let _reader = ProjectRootLogReader::open_for_tests(temp.path()).unwrap();
    }

    #[test]
    fn marker_io_failures_cleanup_created_marker_and_allow_retry() {
        for failure in [MarkerIoFailure::Write, MarkerIoFailure::FileSync] {
            let temp = tempfile::tempdir().unwrap();
            let reader = ProjectRootLogReader::open_for_tests(temp.path()).unwrap();
            let relative = Path::new("marker");
            set_test_marker_failure(Some(failure));
            let error = create_gate_marker(&reader, relative).unwrap_err();
            assert!(matches!(error, AppError::Io { .. }));
            assert!(!temp.path().join(relative).exists());
            assert!(create_gate_marker(&reader, relative).is_ok());
        }

        let temp = tempfile::tempdir().unwrap();
        let reader = ProjectRootLogReader::open_for_tests(temp.path()).unwrap();
        let relative = Path::new("directory-sync-marker");
        set_test_marker_failure(Some(MarkerIoFailure::DirectorySync));
        let error = create_gate_marker(&reader, relative).unwrap_err();
        assert!(matches!(error, AppError::Io { .. }));
        let marker = temp.path().join(relative);
        assert_eq!(fs::read(&marker).unwrap(), b"authorized\n");
        assert!(inspect_gate_marker(&reader, relative).unwrap().is_some());
        assert!(create_gate_marker(&reader, relative).is_err());

        let temp = tempfile::tempdir().unwrap();
        let reader = ProjectRootLogReader::open_for_tests(temp.path()).unwrap();
        let relative = Path::new("before-publish-marker");
        set_test_marker_failure(Some(MarkerIoFailure::BeforePublish));
        let error = create_gate_marker(&reader, relative).unwrap_err();
        assert!(matches!(error, AppError::Io { .. }));
        assert!(!temp.path().join(relative).exists());
        assert!(create_gate_marker(&reader, relative).is_ok());

        set_test_marker_failure(None);
    }

    #[test]
    fn marker_fifo_existing_probe_completes_without_blocking() {
        const ROOT_ENV: &str = "PUEUE_AGENT_MARKER_FIFO_ROOT";
        if let Ok(root) = std::env::var(ROOT_ENV) {
            let reader = ProjectRootLogReader::open_for_tests(Path::new(&root)).unwrap();
            let error = create_gate_marker(&reader, Path::new("marker.fifo")).unwrap_err();
            assert!(matches!(error, AppError::Io { .. }));
            return;
        }

        let temp = tempfile::tempdir().unwrap();
        let fifo = temp.path().join("marker.fifo");
        let fifo_c = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
        // SAFETY: fifo_c is a valid NUL-terminated path and mode is bounded.
        let result = unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) };
        assert_eq!(result, 0, "mkfifo failed: {:?}", std::io::Error::last_os_error());

        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("logs::tests::marker_fifo_existing_probe_completes_without_blocking")
            .arg("--nocapture")
            .env(ROOT_ENV, temp.path())
            .spawn()
            .unwrap();
        let pid = child.id();
        let (sender, receiver) = mpsc::channel();
        let waiter = std::thread::spawn(move || sender.send(child.wait()).unwrap());
        let deadline = Instant::now() + Duration::from_secs(2);
        match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(Ok(status)) => assert!(status.success(), "FIFO probe child failed: {status}"),
            Ok(Err(error)) => panic!("FIFO probe child wait failed: {error}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // The pre-fix classifier blocks opening a FIFO here. Kill the
                // child and join its waiter so the test never leaks a thread.
                // SAFETY: pid came from the child process just spawned above.
                let kill_result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
                assert_eq!(kill_result, 0, "failed to kill blocked FIFO probe");
                let _ = receiver.recv_timeout(Duration::from_secs(1));
                waiter.join().unwrap();
                panic!("existing FIFO marker probe exceeded bounded timeout");
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                waiter.join().unwrap();
                panic!("FIFO probe waiter disconnected");
            }
        }
        waiter.join().unwrap();
    }
}
