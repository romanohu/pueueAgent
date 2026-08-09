use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    path::Path,
    time::UNIX_EPOCH,
};

use crate::AppError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSnapshot {
    pub byte_size: u64,
    pub modified_at_nanos: Option<u128>,
    pub fingerprint: String,
    pub evidence: String,
}

impl LogSnapshot {
    pub fn read_tail(path: &Path, tail_bytes: u32) -> Result<Self, AppError> {
        let metadata = fs::metadata(path).map_err(|source| AppError::Io {
            operation: "read log metadata",
            source,
        })?;
        let byte_size = metadata.len();
        let tail_len = u64::from(tail_bytes).min(byte_size);
        let mut file = fs::File::open(path).map_err(|source| AppError::Io {
            operation: "open log file",
            source,
        })?;
        file.seek(SeekFrom::Start(byte_size - tail_len))
            .map_err(|source| AppError::Io {
                operation: "seek log tail",
                source,
            })?;
        let mut bytes = Vec::with_capacity(usize::try_from(tail_len).unwrap_or(usize::MAX));
        file.take(tail_len)
            .read_to_end(&mut bytes)
            .map_err(|source| AppError::Io {
                operation: "read log tail",
                source,
            })?;
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

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
