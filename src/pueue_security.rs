use std::path::Path;

use crate::{execution_policy::ResolvedExecutionPolicy, AppError};

pub const MAX_PUEUE_OUTPUT_BYTES: usize = 64 * 1024;

/// Verify the Pueue configuration capability pinned at startup.
///
/// `enable` writes the daemon callback, so this preflight must happen before
/// registration or any external mutation. The anchor identity verification
/// checks the pinned canonical path, fingerprint, owner, permissions, and
/// that the config remains outside project roots. It deliberately projects
/// any failed check to a bounded configuration error.
pub fn validate_pinned_config(
    policy: &ResolvedExecutionPolicy,
    configured_path: &Path,
) -> Result<(), AppError> {
    if configured_path != policy.pueue_config_anchor.canonical_path.as_path() {
        return Err(AppError::Configuration {
            field: "pueue_config",
        });
    }
    policy
        .pueue_config_anchor
        .verify_identity(&policy.project_roots)
        .map(|_| ())
        .map_err(|_| AppError::Configuration {
            field: "pueue_config",
        })
}

pub fn validate_group(value: &str) -> Result<(), AppError> {
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 128
        || !bytes[0].is_ascii_alphanumeric()
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(byte))
    {
        return Err(AppError::Validation {
            field: "pueue_group",
            message: "has invalid characters or length",
        });
    }

    Ok(())
}
