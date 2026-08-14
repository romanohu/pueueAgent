use crate::AppError;

pub const MAX_PUEUE_OUTPUT_BYTES: usize = 64 * 1024;

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
