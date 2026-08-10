use std::collections::HashSet;

use crate::{
    models::{BatchJob, BatchJobStatus, BatchStatus, NewBatchRequest},
    AppError,
};

pub const MAX_BATCH_REQUEST_ID_BYTES: usize = 128;
pub const MAX_BATCH_PROJECT_ID_BYTES: usize = 128;
pub const MAX_BATCH_JOB_ID_BYTES: usize = 128;
pub const MAX_BATCH_MANIFEST_HASH_BYTES: usize = 128;
pub const MAX_BATCH_ERROR_BYTES: usize = 2_048;
pub const MAX_BATCH_ARGV_JSON_BYTES: usize = 64 * 1024;
pub const MAX_BATCH_METADATA_JSON_BYTES: usize = 16 * 1024;
pub const MAX_BATCH_JOBS: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchJobResult {
    Accepted {
        pueue_task_id: i64,
        submission_id: String,
    },
    Failed {
        error: String,
    },
}

pub(crate) fn validate_new_batch(request: &NewBatchRequest) -> Result<(), AppError> {
    validate_bounded_nonempty(
        "request_id",
        &request.request_id,
        MAX_BATCH_REQUEST_ID_BYTES,
    )?;
    validate_bounded_nonempty(
        "project_id",
        &request.project_id,
        MAX_BATCH_PROJECT_ID_BYTES,
    )?;
    validate_bounded_nonempty(
        "manifest_hash",
        &request.manifest_hash,
        MAX_BATCH_MANIFEST_HASH_BYTES,
    )?;
    if request.jobs.is_empty() || request.jobs.len() > MAX_BATCH_JOBS {
        return Err(AppError::Validation {
            field: "jobs",
            message: "must contain between 1 and 128 jobs",
        });
    }

    let mut job_ids = HashSet::with_capacity(request.jobs.len());
    let mut ordinals = HashSet::with_capacity(request.jobs.len());
    for job in &request.jobs {
        validate_bounded_nonempty("job_id", &job.job_id, MAX_BATCH_JOB_ID_BYTES)?;
        if job.ordinal < 0 {
            return Err(AppError::Validation {
                field: "ordinal",
                message: "must be non-negative",
            });
        }
        if !job_ids.insert(&job.job_id) {
            return Err(AppError::Validation {
                field: "job_id",
                message: "must be unique within a batch",
            });
        }
        if !ordinals.insert(job.ordinal) {
            return Err(AppError::Validation {
                field: "ordinal",
                message: "must be unique within a batch",
            });
        }
        if job.argv.is_empty() {
            return Err(AppError::Validation {
                field: "argv",
                message: "must not be empty",
            });
        }
        let argv_json =
            serde_json::to_vec(&job.argv).map_err(|source| AppError::Serialization {
                operation: "serialize batch job arguments",
                source,
            })?;
        if argv_json.len() > MAX_BATCH_ARGV_JSON_BYTES {
            return Err(AppError::Validation {
                field: "argv",
                message: "serialized arguments exceed the batch limit",
            });
        }
        if !job.metadata.is_object() {
            return Err(AppError::Validation {
                field: "metadata",
                message: "must be a JSON object",
            });
        }
        let metadata_json =
            serde_json::to_vec(&job.metadata).map_err(|source| AppError::Serialization {
                operation: "serialize batch job metadata",
                source,
            })?;
        if metadata_json.len() > MAX_BATCH_METADATA_JSON_BYTES {
            return Err(AppError::Validation {
                field: "metadata",
                message: "serialized metadata exceeds the batch limit",
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_error(error: &str) -> Result<(), AppError> {
    validate_bounded_nonempty("error", error, MAX_BATCH_ERROR_BYTES)
}

pub(crate) fn validate_accepted_result(
    pueue_task_id: i64,
    submission_id: &str,
) -> Result<(), AppError> {
    if pueue_task_id < 0 {
        return Err(AppError::Validation {
            field: "pueue_task_id",
            message: "must be non-negative",
        });
    }
    validate_bounded_nonempty("submission_id", submission_id, MAX_BATCH_JOB_ID_BYTES)
}

pub(crate) fn derive_request_status(jobs: &[BatchJob]) -> BatchStatus {
    if jobs
        .iter()
        .all(|job| job.status == BatchJobStatus::Accepted)
    {
        return BatchStatus::Completed;
    }
    if jobs.iter().any(|job| job.status == BatchJobStatus::Failed) {
        if jobs
            .iter()
            .any(|job| job.status == BatchJobStatus::Accepted)
        {
            BatchStatus::Partial
        } else {
            BatchStatus::Failed
        }
    } else if jobs
        .iter()
        .any(|job| job.status == BatchJobStatus::Accepted)
    {
        BatchStatus::Accepted
    } else if jobs
        .iter()
        .any(|job| job.status == BatchJobStatus::Dispatching)
    {
        BatchStatus::Dispatching
    } else {
        BatchStatus::Pending
    }
}

fn validate_bounded_nonempty(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), AppError> {
    if value.is_empty() || value.len() > max_bytes {
        return Err(AppError::Validation {
            field,
            message: "must be non-empty and within the size limit",
        });
    }
    Ok(())
}
