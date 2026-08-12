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
