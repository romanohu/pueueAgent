use std::{
    collections::HashSet,
    ffi::OsString,
    path::{Component, Path},
};

use serde::{de, de::DeserializeSeed, Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    environment::{ResearchDirectoryRecord, ResearchFileRecord},
    models::ProposalKind,
    output::permits_lossless_evidence_text,
    proposals::{self, ProposalInput},
    pueue::validate_add_argv,
    process::MAX_FIELD_SIZE,
    research_protocol::{
        CheckpointRequest, MAX_RESEARCH_ANSWER_BYTES, MAX_RESEARCH_ARGV,
        MAX_RESEARCH_EVIDENCE_REF_BYTES,
    },
    AppError,
};

const SUPPORTED_INTERPRETERS: &[&str] = &["python", "python3"];
const MAX_CHECKPOINT_PATH_BYTES: usize = 4 * 1024;
pub(crate) const CHECKPOINT_SUPPORT_VERSION: u8 = 1;
pub(crate) const MAX_CHECKPOINT_SOURCE_BYTES: usize = 32 * 1024;
pub(crate) const MAX_CHECKPOINT_CANDIDATES: usize = 4;
const MAX_CHECKPOINT_SUPPORT_REASON_BYTES: usize = 1024;
const MAX_CHECKPOINT_CONTEXT_BYTES: usize = if crate::research_evidence::MAX_RESEARCH_CONTEXT_BYTES
    < crate::research_evidence::MAX_RESEARCH_NATIVE_EVIDENCE_BYTES
{
    crate::research_evidence::MAX_RESEARCH_CONTEXT_BYTES
} else {
    crate::research_evidence::MAX_RESEARCH_NATIVE_EVIDENCE_BYTES
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CheckpointLoaderRole {
    Entrypoint,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointLoaderEvidenceV1 {
    pub(crate) reference: String,
    pub(crate) role: CheckpointLoaderRole,
    pub(crate) argv_index: usize,
    pub(crate) argv_token: String,
    pub(crate) root_relative_path: String,
    pub(crate) length: u64,
    pub(crate) sha256: String,
    pub(crate) file: ResearchFileRecord,
    pub(crate) content: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointCandidateEvidenceV1 {
    pub(crate) reference: String,
    pub(crate) source_experiment_id: String,
    pub(crate) argv_path: String,
    pub(crate) root_relative_path: String,
    pub(crate) length: u64,
    pub(crate) sha256: String,
    pub(crate) file: ResearchFileRecord,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CheckpointSupportEvidenceV1 {
    Available {
        support_version: u8,
        source_experiment_id: String,
        source_proposal_id: String,
        source_submission_id: String,
        normalized_working_directory: String,
        working_directory_record: ResearchDirectoryRecord,
        loader_support: Vec<CheckpointLoaderEvidenceV1>,
        checkpoint_candidates: Vec<CheckpointCandidateEvidenceV1>,
        candidates_complete: bool,
        candidates_omitted_at_least: usize,
        candidate_limit: usize,
    },
    Unavailable {
        support_version: u8,
        reason: String,
        loader_support: Vec<CheckpointLoaderEvidenceV1>,
        checkpoint_candidates: Vec<CheckpointCandidateEvidenceV1>,
        candidates_complete: bool,
        candidates_omitted_at_least: usize,
        candidate_limit: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SelectedCheckpointSupport<'a> {
    pub(crate) loader: &'a CheckpointLoaderEvidenceV1,
    pub(crate) candidate: &'a CheckpointCandidateEvidenceV1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointSourceLayout {
    pub(crate) normalized_working_directory: String,
    pub(crate) entrypoint_index: usize,
    pub(crate) entrypoint_token: String,
    pub(crate) entrypoint_root_relative_path: String,
}

/// The only command delta admitted for a checkpoint successor.
///
/// `index` is the inserted option index for a pair delta and the complete
/// `--flag=path` token index for an equals delta.  Keeping the exact index and
/// form makes restart validation deterministic rather than rediscovering a
/// potentially different matching token.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CheckpointArgvDeltaForm {
    Pair,
    Equals,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointArgvDelta {
    form: CheckpointArgvDeltaForm,
    index: usize,
    flag: String,
}

impl CheckpointArgvDelta {
    pub(crate) const fn form(&self) -> CheckpointArgvDeltaForm {
        self.form
    }

    pub(crate) const fn index(&self) -> usize {
        self.index
    }

    pub(crate) fn flag(&self) -> &str {
        &self.flag
    }
}

pub(crate) const PREPARED_CHECKPOINT_VERSION: u8 = 1;
pub(crate) const MAX_PREPARED_CHECKPOINT_BYTES: usize = MAX_RESEARCH_ANSWER_BYTES;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CheckpointSourceRuntimeV1 {
    OriginalProjectRoot,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointSuccessorIds {
    pub(crate) proposal_id: String,
    pub(crate) experiment_id: String,
    pub(crate) submission_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedCheckpoint {
    pub(crate) schema_version: u8,

    pub(crate) project_id: String,
    pub(crate) campaign_id: String,
    pub(crate) review_id: String,
    pub(crate) review_attempt: i64,
    pub(crate) review_session_generation: i64,
    pub(crate) review_agent_run_id: i64,
    pub(crate) review_event_id: i64,

    pub(crate) source_experiment_id: String,
    pub(crate) source_proposal_id: String,
    pub(crate) source_submission_id: String,
    pub(crate) source_task_id: i64,
    pub(crate) source_managed_task_signature: String,
    pub(crate) source_raw_task_signature: String,

    pub(crate) context_digest: String,
    pub(crate) response_digest: String,
    pub(crate) campaign_objective_digest: String,
    pub(crate) source_proposal_canonical_digest: String,
    pub(crate) learning_spec_digest: String,

    pub(crate) source_runtime: CheckpointSourceRuntimeV1,
    pub(crate) source_root_canonical_path: String,
    pub(crate) source_root_resolution_fingerprint: String,
    pub(crate) source_root_record: ResearchDirectoryRecord,
    pub(crate) source_working_directory_record: ResearchDirectoryRecord,

    pub(crate) support_version: u8,
    pub(crate) loader: CheckpointLoaderEvidenceV1,
    pub(crate) source_checkpoint: CheckpointCandidateEvidenceV1,
    pub(crate) retained_checkpoint: ResearchFileRecord,

    pub(crate) source_argv: Vec<String>,
    pub(crate) source_working_directory: String,
    pub(crate) request: CheckpointRequest,
    pub(crate) delta: CheckpointArgvDelta,
    pub(crate) retained_argv: Vec<String>,

    pub(crate) successor_ids: CheckpointSuccessorIds,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedCheckpointCommand {
    source_argv: Vec<String>,
    request_argv: Vec<String>,
    source_working_directory: String,
    request_working_directory: String,
    checkpoint_path: String,
    delta: CheckpointArgvDelta,
}

impl ValidatedCheckpointCommand {
    pub(crate) fn source_argv(&self) -> &[String] {
        &self.source_argv
    }

    pub(crate) fn request_argv(&self) -> &[String] {
        &self.request_argv
    }

    pub(crate) fn source_working_directory(&self) -> &str {
        &self.source_working_directory
    }

    pub(crate) fn request_working_directory(&self) -> &str {
        &self.request_working_directory
    }

    pub(crate) fn checkpoint_path(&self) -> &str {
        &self.checkpoint_path
    }

    pub(crate) fn delta(&self) -> &CheckpointArgvDelta {
        &self.delta
    }

    /// Revalidate the exact source/request pair and replace only the retained
    /// checkpoint path value.  The retained path is an already-verified
    /// supervisor path; this helper only guarantees whole-token substitution.
    pub(crate) fn reapply_retained_path(
        &self,
        retained_path: &str,
    ) -> Result<Vec<String>, AppError> {
        let request = CheckpointRequest {
            path: self.checkpoint_path.clone(),
            argv: self.request_argv.clone(),
            working_directory: self.request_working_directory.clone(),
            support_evidence_refs: Vec::new(),
        };
        let revalidated = validate_checkpoint_command(
            &self.source_argv,
            &self.source_working_directory,
            &request,
        )?;
        if revalidated.delta != self.delta {
            return Err(checkpoint_validation_error(
                "checkpoint argv delta changed during revalidation",
            ));
        }
        validate_retained_path(retained_path)?;

        let mut argv = revalidated.request_argv;
        match revalidated.delta.form {
            CheckpointArgvDeltaForm::Pair => {
                let path_index =
                    revalidated.delta.index.checked_add(1).ok_or_else(|| {
                        checkpoint_validation_error("checkpoint argv index overflow")
                    })?;
                if argv.get(revalidated.delta.index).map(String::as_str)
                    != Some(revalidated.delta.flag.as_str())
                    || argv.get(path_index).map(String::as_str)
                        != Some(revalidated.checkpoint_path.as_str())
                {
                    return Err(checkpoint_validation_error(
                        "checkpoint argv delta no longer identifies the original path",
                    ));
                }
                argv[path_index] = retained_path.to_owned();
            }
            CheckpointArgvDeltaForm::Equals => {
                let expected =
                    format!("{}={}", revalidated.delta.flag, revalidated.checkpoint_path);
                if argv.get(revalidated.delta.index).map(String::as_str) != Some(expected.as_str())
                {
                    return Err(checkpoint_validation_error(
                        "checkpoint argv delta no longer identifies the original path",
                    ));
                }
                argv[revalidated.delta.index] =
                    format!("{}={retained_path}", revalidated.delta.flag);
            }
        }
        validate_command_argv(&argv)?;
        Ok(argv)
    }
}

/// Validate the authoritative source command and the requested successor
/// command.  The request may differ only by one checkpoint-loading option and
/// its exact path value; source learning arguments retain byte/order identity.
pub(crate) fn validate_checkpoint_command(
    source_argv: &[String],
    source_working_directory: &str,
    request: &CheckpointRequest,
) -> Result<ValidatedCheckpointCommand, AppError> {
    validate_checkpoint_path(&request.path)?;

    let source = validate_command_proposal(source_argv, source_working_directory)?;
    let requested = validate_command_proposal(&request.argv, &request.working_directory)?;
    if source.working_directory() != requested.working_directory() {
        return Err(checkpoint_validation_error(
            "source and requested working directories must match",
        ));
    }

    validate_trainer_command_shape(source_argv)?;
    validate_trainer_command_shape(&request.argv)?;
    validate_command_argv(source_argv)?;
    validate_command_argv(&request.argv)?;
    let delta = validate_checkpoint_argv_delta(source_argv, &request.argv, &request.path)?;

    Ok(ValidatedCheckpointCommand {
        source_argv: source.argv().to_owned(),
        request_argv: requested.argv().to_owned(),
        source_working_directory: source.working_directory().to_owned(),
        request_working_directory: requested.working_directory().to_owned(),
        checkpoint_path: request.path.clone(),
        delta,
    })
}

pub(crate) fn checkpoint_source_layout(
    source_argv: &[String],
    source_working_directory: &str,
) -> Result<CheckpointSourceLayout, AppError> {
    let source = validate_command_proposal(source_argv, source_working_directory)?;
    validate_trainer_command_shape(source_argv)?;

    let entrypoint_index = if SUPPORTED_INTERPRETERS.contains(&source_argv[0].as_str()) {
        1
    } else {
        0
    };
    let entrypoint_token = source_argv
        .get(entrypoint_index)
        .ok_or_else(|| checkpoint_validation_error("trainer entrypoint is missing"))?
        .clone();
    let lookup_token = entrypoint_token
        .strip_prefix("./")
        .unwrap_or(entrypoint_token.as_str());
    let cwd_components = normal_relative_components(source.working_directory())
        .ok_or_else(|| checkpoint_validation_error("working directory is not normalized"))?;
    let entrypoint_components = normal_relative_components(lookup_token)
        .ok_or_else(|| checkpoint_validation_error("trainer entrypoint is not normalized"))?;
    let mut root_components = cwd_components;
    root_components.extend(entrypoint_components);
    let entrypoint_root_relative_path = root_components.join("/");
    if entrypoint_root_relative_path.is_empty()
        || entrypoint_root_relative_path.len() > MAX_CHECKPOINT_PATH_BYTES
    {
        return Err(checkpoint_validation_error(
            "trainer entrypoint path is out of bounds",
        ));
    }

    Ok(CheckpointSourceLayout {
        normalized_working_directory: source.working_directory().to_owned(),
        entrypoint_index,
        entrypoint_token,
        entrypoint_root_relative_path,
    })
}

pub(crate) fn checkpoint_candidate_argv_path(
    normalized_working_directory: &str,
    candidate_root_relative_path: &str,
) -> Option<String> {
    let cwd = normal_relative_components(normalized_working_directory)?;
    let candidate = normal_relative_components(candidate_root_relative_path)?;
    if candidate.len() <= cwd.len() || !candidate.starts_with(&cwd) {
        return None;
    }
    let argv_path = candidate[cwd.len()..].join("/");
    if argv_path.is_empty() || argv_path.len() > MAX_CHECKPOINT_PATH_BYTES {
        return None;
    }
    Some(argv_path)
}

pub(crate) fn serialize_prepared_checkpoint(
    checkpoint: &PreparedCheckpoint,
) -> Result<String, AppError> {
    validate_prepared_checkpoint(checkpoint)?;
    let encoded = serde_json::to_string(checkpoint).map_err(|source| AppError::Serialization {
        operation: "serialize prepared checkpoint",
        source,
    })?;
    if encoded.is_empty() || encoded.len() > MAX_PREPARED_CHECKPOINT_BYTES {
        return Err(prepared_checkpoint_error(
            "serialized prepared checkpoint exceeds the durable bound",
        ));
    }
    Ok(encoded)
}

pub(crate) fn parse_prepared_checkpoint(
    checkpoint_json: &str,
) -> Result<PreparedCheckpoint, AppError> {
    if checkpoint_json.is_empty() || checkpoint_json.len() > MAX_PREPARED_CHECKPOINT_BYTES {
        return Err(prepared_checkpoint_error(
            "prepared checkpoint exceeds the durable bound",
        ));
    }
    let value = parse_strict_json_value(checkpoint_json.as_bytes())?;
    let checkpoint =
        serde_json::from_value::<PreparedCheckpoint>(value).map_err(|source| {
            AppError::Serialization {
                operation: "parse prepared checkpoint schema",
                source,
            }
        })?;
    validate_prepared_checkpoint(&checkpoint)?;
    Ok(checkpoint)
}

pub(crate) fn checkpoint_learning_spec_digest(
    source_argv: &[String],
    normalized_working_directory: &str,
) -> Result<String, AppError> {
    let proposal = validate_command_proposal(source_argv, normalized_working_directory)?;
    if proposal.working_directory() != normalized_working_directory {
        return Err(prepared_checkpoint_error(
            "learning working directory is not normalized",
        ));
    }
    validate_argv_vector("learning_spec.argv", source_argv)?;
    if normal_relative_components(normalized_working_directory).is_none() {
        return Err(prepared_checkpoint_error(
            "learning working directory is invalid",
        ));
    }
    #[derive(Serialize)]
    struct LearningSpec<'a> {
        domain: &'static str,
        schema_version: u8,
        argv: &'a [String],
        working_directory: &'a str,
    }
    let canonical = LearningSpec {
        domain: "pueue-research-checkpoint-learning",
        schema_version: PREPARED_CHECKPOINT_VERSION,
        argv: source_argv,
        working_directory: normalized_working_directory,
    };
    let bytes = serde_json::to_vec(&canonical).map_err(|source| AppError::Serialization {
        operation: "serialize checkpoint learning specification",
        source,
    })?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

pub(crate) fn checkpoint_successor_ids(
    review_id: &str,
    review_attempt: i64,
) -> Result<CheckpointSuccessorIds, AppError> {
    if !valid_bounded_identity(review_id) {
        return Err(prepared_checkpoint_error(
            "successor review identity is invalid",
        ));
    }
    if review_attempt <= 0 {
        return Err(prepared_checkpoint_error(
            "successor review attempt must be positive",
        ));
    }
    Ok(CheckpointSuccessorIds {
        proposal_id: checkpoint_successor_id("proposal", review_id, review_attempt)?,
        experiment_id: checkpoint_successor_id("experiment", review_id, review_attempt)?,
        submission_id: checkpoint_successor_id("submission", review_id, review_attempt)?,
    })
}

fn checkpoint_successor_id(
    kind: &'static str,
    review_id: &str,
    review_attempt: i64,
) -> Result<String, AppError> {
    #[derive(Serialize)]
    struct Successor<'a> {
        domain: &'static str,
        schema_version: u8,
        resource_kind: &'static str,
        review_id: &'a str,
        review_attempt: i64,
    }
    let canonical = Successor {
        domain: "pueue-research-checkpoint-successor",
        schema_version: PREPARED_CHECKPOINT_VERSION,
        resource_kind: kind,
        review_id,
        review_attempt,
    };
    let bytes = serde_json::to_vec(&canonical).map_err(|source| AppError::Serialization {
        operation: "serialize checkpoint successor identity",
        source,
    })?;
    let id = format!(
        "research-checkpoint-{kind}:{:x}",
        Sha256::digest(bytes)
    );
    if valid_bounded_identity(&id) {
        Ok(id)
    } else {
        Err(prepared_checkpoint_error(
            "derived checkpoint successor identity is invalid",
        ))
    }
}

fn validate_prepared_checkpoint(checkpoint: &PreparedCheckpoint) -> Result<(), AppError> {
    if checkpoint.schema_version != PREPARED_CHECKPOINT_VERSION {
        return Err(prepared_checkpoint_error(
            "prepared checkpoint schema version is unsupported",
        ));
    }
    for identity in [
        &checkpoint.project_id,
        &checkpoint.campaign_id,
        &checkpoint.review_id,
        &checkpoint.source_experiment_id,
        &checkpoint.source_proposal_id,
        &checkpoint.source_submission_id,
    ] {
        if !valid_bounded_identity(identity) {
            return Err(prepared_checkpoint_error(
                "prepared checkpoint identity is invalid",
            ));
        }
    }
    if checkpoint.review_attempt <= 0
        || checkpoint.review_session_generation < 0
        || checkpoint.review_agent_run_id <= 0
        || checkpoint.review_event_id <= 0
        || checkpoint.source_task_id < 0
    {
        return Err(prepared_checkpoint_error(
            "prepared checkpoint numeric identity is invalid",
        ));
    }
    for (field, value) in [
        (
            "source_managed_task_signature",
            checkpoint.source_managed_task_signature.as_str(),
        ),
        (
            "source_raw_task_signature",
            checkpoint.source_raw_task_signature.as_str(),
        ),
        (
            "source_root_canonical_path",
            checkpoint.source_root_canonical_path.as_str(),
        ),
        (
            "source_root_resolution_fingerprint",
            checkpoint.source_root_resolution_fingerprint.as_str(),
        ),
    ] {
        validate_bounded_field(field, value, MAX_FIELD_SIZE, true)?;
    }
    if !Path::new(&checkpoint.source_root_canonical_path).is_absolute() {
        return Err(prepared_checkpoint_error(
            "source root canonical path must be absolute",
        ));
    }
    for digest in [
        &checkpoint.context_digest,
        &checkpoint.response_digest,
        &checkpoint.campaign_objective_digest,
        &checkpoint.source_proposal_canonical_digest,
        &checkpoint.learning_spec_digest,
    ] {
        if !is_lower_hex_digest(digest) {
            return Err(prepared_checkpoint_error(
                "prepared checkpoint digest is invalid",
            ));
        }
    }
    if checkpoint.source_runtime != CheckpointSourceRuntimeV1::OriginalProjectRoot
        || checkpoint.support_version != CHECKPOINT_SUPPORT_VERSION
    {
        return Err(prepared_checkpoint_error(
            "prepared checkpoint runtime or support version is unsupported",
        ));
    }
    validate_directory_record(&checkpoint.source_root_record)?;
    validate_directory_record(&checkpoint.source_working_directory_record)?;
    if checkpoint.source_working_directory_record.mount_identity
        != checkpoint.source_root_record.mount_identity
    {
        return Err(prepared_checkpoint_error(
            "source working directory is on a different mount",
        ));
    }
    if normal_relative_components(&checkpoint.source_working_directory).is_none() {
        return Err(prepared_checkpoint_error(
            "source working directory is not normalized",
        ));
    }
    if checkpoint.source_working_directory == "."
        && checkpoint.source_working_directory_record != checkpoint.source_root_record
    {
        return Err(prepared_checkpoint_error(
            "root working directory record does not match the source root",
        ));
    }
    validate_argv_vector("source_argv", &checkpoint.source_argv)?;
    validate_argv_vector("request.argv", &checkpoint.request.argv)?;
    validate_argv_vector("retained_argv", &checkpoint.retained_argv)?;
    if checkpoint.request.support_evidence_refs.len() != 2 {
        return Err(prepared_checkpoint_error(
            "checkpoint request must contain exactly two evidence references",
        ));
    }
    if checkpoint.request.support_evidence_refs[0]
        == checkpoint.request.support_evidence_refs[1]
    {
        return Err(prepared_checkpoint_error(
            "checkpoint request evidence references must be unique",
        ));
    }
    for reference in &checkpoint.request.support_evidence_refs {
        validate_bounded_field(
            "request.support_evidence_refs",
            reference,
            MAX_RESEARCH_EVIDENCE_REF_BYTES,
            true,
        )?;
    }

    let command = validate_checkpoint_command(
        &checkpoint.source_argv,
        &checkpoint.source_working_directory,
        &checkpoint.request,
    )?;
    if command.delta() != &checkpoint.delta {
        return Err(prepared_checkpoint_error(
            "checkpoint argv delta does not match the request",
        ));
    }
    validate_retained_argv(&command, &checkpoint.retained_argv)?;

    validate_loader_evidence(
        &checkpoint.loader,
        &checkpoint.source_working_directory,
    )?;
    let source_layout = checkpoint_source_layout(
        &checkpoint.source_argv,
        &checkpoint.source_working_directory,
    )?;
    if checkpoint.loader.argv_index != source_layout.entrypoint_index
        || checkpoint.loader.argv_token != source_layout.entrypoint_token
        || checkpoint.loader.root_relative_path != source_layout.entrypoint_root_relative_path
    {
        return Err(prepared_checkpoint_error(
            "checkpoint loader does not match the source command layout",
        ));
    }
    validate_research_file_record(&checkpoint.loader.file, MAX_CHECKPOINT_SOURCE_BYTES as u64)?;
    if checkpoint.loader.file.root != checkpoint.source_root_record
        || checkpoint.loader.reference
            != format!("loader-source:{}", checkpoint.loader.sha256)
    {
        return Err(prepared_checkpoint_error(
            "checkpoint loader does not match the source root or digest",
        ));
    }
    if loader_is_direct_child_of_cwd(
        &checkpoint.loader,
        &checkpoint.source_working_directory,
    ) && checkpoint.loader.file.parent != checkpoint.source_working_directory_record
    {
        return Err(prepared_checkpoint_error(
            "checkpoint loader parent does not match the source working directory",
        ));
    }

    let artifact_prefix = [
        ".pueue-agent".to_owned(),
        "artifacts".to_owned(),
        checkpoint.source_experiment_id.clone(),
    ];
    validate_candidate_evidence(
        &checkpoint.source_checkpoint,
        &checkpoint.source_experiment_id,
        &checkpoint.source_working_directory,
        &artifact_prefix,
    )?;
    validate_research_file_record(
        &checkpoint.source_checkpoint.file,
        crate::environment::MAX_PRIVATE_TEMP_ALLOCATED_BYTES,
    )?;
    if checkpoint.source_checkpoint.file.root != checkpoint.source_root_record
        || checkpoint.source_checkpoint.argv_path != checkpoint.request.path
    {
        return Err(prepared_checkpoint_error(
            "selected checkpoint does not match the request or source root",
        ));
    }
    validate_checkpoint_candidate_reference(
        &checkpoint.source_checkpoint.reference,
        &checkpoint.source_experiment_id,
        &checkpoint.source_checkpoint.sha256,
    )?;
    if !checkpoint
        .request
        .support_evidence_refs
        .iter()
        .any(|reference| reference == &checkpoint.loader.reference)
        || !checkpoint
            .request
            .support_evidence_refs
            .iter()
            .any(|reference| reference == &checkpoint.source_checkpoint.reference)
    {
        return Err(prepared_checkpoint_error(
            "checkpoint request references do not select the loader and candidate",
        ));
    }
    if checkpoint.source_checkpoint.length != checkpoint.retained_checkpoint.logical_bytes
        || checkpoint.source_checkpoint.sha256 != checkpoint.retained_checkpoint.sha256
    {
        return Err(prepared_checkpoint_error(
            "retained checkpoint does not match the selected source checkpoint",
        ));
    }
    validate_research_file_record(
        &checkpoint.retained_checkpoint,
        crate::environment::MAX_PRIVATE_TEMP_ALLOCATED_BYTES,
    )?;
    let expected_retained_path = format!(
        "research-checkpoints/{}/{}/checkpoint",
        checkpoint.campaign_id, checkpoint.review_id
    );
    if checkpoint.retained_checkpoint.relative_path != expected_retained_path {
        return Err(prepared_checkpoint_error(
            "retained checkpoint path is invalid",
        ));
    }

    let learning_spec_digest = checkpoint_learning_spec_digest(
        &checkpoint.source_argv,
        &checkpoint.source_working_directory,
    )?;
    if checkpoint.learning_spec_digest != learning_spec_digest {
        return Err(prepared_checkpoint_error(
            "learning specification digest does not match the source command",
        ));
    }
    let successor_ids = checkpoint_successor_ids(&checkpoint.review_id, checkpoint.review_attempt)?;
    if checkpoint.successor_ids != successor_ids {
        return Err(prepared_checkpoint_error(
            "successor identities do not match the review",
        ));
    }
    Ok(())
}

fn validate_argv_vector(field: &'static str, argv: &[String]) -> Result<(), AppError> {
    if argv.is_empty() || argv.len() > MAX_RESEARCH_ARGV {
        return Err(prepared_checkpoint_error("argv cardinality is out of bounds"));
    }
    for value in argv {
        validate_bounded_field(field, value, MAX_FIELD_SIZE, false)?;
    }
    Ok(())
}

fn validate_bounded_field(
    _field: &'static str,
    value: &str,
    maximum_bytes: usize,
    require_nonempty: bool,
) -> Result<(), AppError> {
    if (require_nonempty && value.is_empty())
        || value.len() > maximum_bytes
        || value.chars().any(char::is_control)
    {
        return Err(prepared_checkpoint_error(
            "prepared checkpoint field is empty, oversized, or contains control text",
        ));
    }
    Ok(())
}

fn validate_directory_record(record: &ResearchDirectoryRecord) -> Result<(), AppError> {
    if record.mode & 0o022 != 0 {
        return Err(prepared_checkpoint_error(
            "prepared directory record has unsafe permissions",
        ));
    }
    Ok(())
}

fn validate_research_file_record(
    record: &ResearchFileRecord,
    maximum_bytes: u64,
) -> Result<(), AppError> {
    if normal_relative_components(&record.relative_path).is_none()
        || record.mode & 0o022 != 0
        || record.logical_bytes > maximum_bytes
        || record.allocated_bytes > crate::environment::MAX_PRIVATE_TEMP_ALLOCATED_BYTES
        || !is_lower_hex_digest(&record.sha256)
    {
        return Err(prepared_checkpoint_error(
            "prepared research file record is invalid",
        ));
    }
    validate_directory_record(&record.root)?;
    validate_directory_record(&record.parent)?;
    if record.root.mount_identity != record.parent.mount_identity
        || record.mount_identity != record.root.mount_identity
    {
        return Err(prepared_checkpoint_error(
            "prepared research file crosses a mount boundary",
        ));
    }
    Ok(())
}

fn validate_checkpoint_candidate_reference(
    reference: &str,
    source_experiment_id: &str,
    sha256: &str,
) -> Result<(), AppError> {
    let prefix = "checkpoint:";
    let body = reference
        .strip_prefix(prefix)
        .ok_or_else(|| prepared_checkpoint_error("checkpoint candidate reference is invalid"))?;
    let (body, digest) = body
        .rsplit_once(':')
        .ok_or_else(|| prepared_checkpoint_error("checkpoint candidate reference is invalid"))?;
    if digest != sha256 || !is_lower_hex_digest(digest) {
        return Err(prepared_checkpoint_error(
            "checkpoint candidate reference digest is invalid",
        ));
    }
    let (source_id, ordinal_text) = body
        .rsplit_once(':')
        .ok_or_else(|| prepared_checkpoint_error("checkpoint candidate reference is invalid"))?;
    let ordinal = ordinal_text
        .parse::<usize>()
        .map_err(|_| prepared_checkpoint_error("checkpoint candidate ordinal is invalid"))?;
    if source_id != source_experiment_id
        || ordinal >= MAX_CHECKPOINT_CANDIDATES
        || ordinal_text != ordinal.to_string()
    {
        return Err(prepared_checkpoint_error(
            "checkpoint candidate reference identity is invalid",
        ));
    }
    Ok(())
}

fn validate_retained_argv(
    command: &ValidatedCheckpointCommand,
    retained_argv: &[String],
) -> Result<(), AppError> {
    if retained_argv.len() != command.request_argv().len() {
        return Err(prepared_checkpoint_error(
            "retained argv does not preserve the request shape",
        ));
    }
    let path_index = match command.delta().form {
        CheckpointArgvDeltaForm::Pair => command
            .delta()
            .index
            .checked_add(1)
            .ok_or_else(|| prepared_checkpoint_error("retained argv index overflow"))?,
        CheckpointArgvDeltaForm::Equals => command.delta().index,
    };
    for (index, (expected, actual)) in command
        .request_argv()
        .iter()
        .zip(retained_argv)
        .enumerate()
    {
        if index == path_index {
            continue;
        }
        if expected != actual {
            return Err(prepared_checkpoint_error(
                "retained argv changes the learning command",
            ));
        }
    }
    match command.delta().form {
        CheckpointArgvDeltaForm::Pair => {
            if retained_argv[command.delta().index] != command.delta().flag {
                return Err(prepared_checkpoint_error(
                    "retained argv loses the checkpoint flag",
                ));
            }
            validate_retained_path(&retained_argv[path_index])?;
            if retained_argv[path_index] == command.checkpoint_path() {
                return Err(prepared_checkpoint_error(
                    "retained argv did not replace the source checkpoint path",
                ));
            }
        }
        CheckpointArgvDeltaForm::Equals => {
            let prefix = format!("{}=", command.delta().flag);
            let Some(path) = retained_argv[path_index].strip_prefix(&prefix) else {
                return Err(prepared_checkpoint_error(
                    "retained argv loses the checkpoint flag",
                ));
            };
            validate_retained_path(path)?;
            if path == command.checkpoint_path() {
                return Err(prepared_checkpoint_error(
                    "retained argv did not replace the source checkpoint path",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn checkpoint_support_from_persisted_context(
    context_json: &str,
    expected_context_digest: &str,
) -> Result<CheckpointSupportEvidenceV1, AppError> {
    if context_json.is_empty() || context_json.len() > MAX_CHECKPOINT_CONTEXT_BYTES {
        return Err(checkpoint_evidence_error(
            "persisted context exceeds the native bound",
        ));
    }
    if !is_lower_hex_digest(expected_context_digest)
        || format!("{:x}", Sha256::digest(context_json.as_bytes())) != expected_context_digest
    {
        return Err(checkpoint_evidence_error(
            "persisted context digest does not match",
        ));
    }

    let context = parse_strict_json_value(context_json.as_bytes())?;
    if context.get("schema_version").and_then(Value::as_u64)
        != Some(crate::research_evidence::RESEARCH_CONTEXT_SCHEMA_VERSION as u64)
    {
        return Err(checkpoint_evidence_error(
            "persisted context schema version is unsupported",
        ));
    }
    let operations = context
        .get("operations")
        .and_then(Value::as_object)
        .ok_or_else(|| checkpoint_evidence_error("persisted context operations are missing"))?;
    let support_value = operations
        .get("checkpoint_support")
        .cloned()
        .ok_or_else(|| checkpoint_evidence_error("checkpoint support packet is missing"))?;
    let support = serde_json::from_value::<CheckpointSupportEvidenceV1>(support_value)
        .map_err(|_| checkpoint_evidence_error("checkpoint support packet is malformed"))?;
    validate_checkpoint_support(&context, &support)?;
    Ok(support)
}

pub(crate) fn select_checkpoint_support<'a>(
    support: &'a CheckpointSupportEvidenceV1,
    request: &CheckpointRequest,
) -> Result<SelectedCheckpointSupport<'a>, AppError> {
    let (loader_support, checkpoint_candidates) = match support {
        CheckpointSupportEvidenceV1::Available {
            loader_support,
            checkpoint_candidates,
            ..
        } if loader_support.len() == 1 => (loader_support, checkpoint_candidates),
        CheckpointSupportEvidenceV1::Available { .. }
        | CheckpointSupportEvidenceV1::Unavailable { .. } => {
            return Err(checkpoint_evidence_error(
                "checkpoint support is not selectable",
            ));
        }
    };
    if request.support_evidence_refs.len() != 2 {
        return Err(checkpoint_evidence_error(
            "checkpoint selection requires one loader and one candidate reference",
        ));
    }

    let mut references = HashSet::new();
    let mut selected_loader = None;
    let mut selected_candidate = None;
    for reference in &request.support_evidence_refs {
        if !references.insert(reference.as_str()) {
            return Err(checkpoint_evidence_error(
                "checkpoint support references must be unique",
            ));
        }
        let loader_matches = loader_support
            .iter()
            .filter(|loader| loader.reference == *reference)
            .collect::<Vec<_>>();
        let candidate_matches = checkpoint_candidates
            .iter()
            .filter(|candidate| candidate.reference == *reference)
            .collect::<Vec<_>>();
        match (loader_matches.as_slice(), candidate_matches.as_slice()) {
            ([loader], []) if selected_loader.is_none() => selected_loader = Some(*loader),
            ([], [candidate]) if selected_candidate.is_none() => {
                selected_candidate = Some(*candidate)
            }
            _ => {
                return Err(checkpoint_evidence_error(
                    "checkpoint support reference is unknown or ambiguous",
                ));
            }
        }
    }

    let loader = selected_loader.ok_or_else(|| {
        checkpoint_evidence_error("checkpoint selection is missing the loader reference")
    })?;
    let candidate = selected_candidate.ok_or_else(|| {
        checkpoint_evidence_error("checkpoint selection is missing the candidate reference")
    })?;
    if request.path != candidate.argv_path {
        return Err(checkpoint_evidence_error(
            "checkpoint request path does not match selected evidence",
        ));
    }
    Ok(SelectedCheckpointSupport { loader, candidate })
}

fn validate_checkpoint_support(
    context: &Value,
    support: &CheckpointSupportEvidenceV1,
) -> Result<(), AppError> {
    match support {
        CheckpointSupportEvidenceV1::Unavailable {
            support_version,
            reason,
            loader_support,
            checkpoint_candidates,
            candidates_complete,
            candidate_limit,
            ..
        } => {
            if *support_version != CHECKPOINT_SUPPORT_VERSION
                || reason.is_empty()
                || reason.len() > MAX_CHECKPOINT_SUPPORT_REASON_BYTES
                || reason.chars().any(char::is_control)
                || !loader_support.is_empty()
                || !checkpoint_candidates.is_empty()
                || *candidates_complete
                || *candidate_limit != MAX_CHECKPOINT_CANDIDATES
            {
                return Err(checkpoint_evidence_error(
                    "unavailable checkpoint support is not empty and bounded",
                ));
            }
            Ok(())
        }
        CheckpointSupportEvidenceV1::Available {
            support_version,
            source_experiment_id,
            source_proposal_id,
            source_submission_id,
            normalized_working_directory,
            working_directory_record,
            loader_support,
            checkpoint_candidates,
            candidates_complete,
            candidates_omitted_at_least,
            candidate_limit,
        } => {
            if *support_version != CHECKPOINT_SUPPORT_VERSION
                || !valid_bounded_identity(source_experiment_id)
                || !valid_bounded_identity(source_proposal_id)
                || !valid_bounded_identity(source_submission_id)
                || normal_relative_components(normalized_working_directory).is_none()
                || loader_support.len() != 1
                || checkpoint_candidates.is_empty()
                || checkpoint_candidates.len() > MAX_CHECKPOINT_CANDIDATES
                || *candidate_limit != MAX_CHECKPOINT_CANDIDATES
                || (*candidates_complete && *candidates_omitted_at_least != 0)
                || (!*candidates_complete && *candidates_omitted_at_least == 0)
            {
                return Err(checkpoint_evidence_error(
                    "checkpoint support cardinality or bounds are invalid",
                ));
            }
            validate_context_bindings(
                context,
                source_experiment_id,
                source_proposal_id,
                source_submission_id,
            )?;

            let loader = &loader_support[0];
            validate_loader_evidence(loader, normalized_working_directory)?;
            let common_root = &loader.file.root;
            if normalized_working_directory == "." && working_directory_record != common_root {
                return Err(checkpoint_evidence_error(
                    "root working directory identity does not match file records",
                ));
            }
            if loader_is_direct_child_of_cwd(loader, normalized_working_directory)
                && loader.file.parent != *working_directory_record
            {
                return Err(checkpoint_evidence_error(
                    "direct loader parent does not match working directory",
                ));
            }

            let mut references = HashSet::new();
            if !references.insert(loader.reference.as_str()) {
                return Err(checkpoint_evidence_error(
                    "checkpoint evidence references must be unique",
                ));
            }
            let expected_loader_reference = format!("loader-source:{}", loader.sha256);
            if loader.reference != expected_loader_reference {
                return Err(checkpoint_evidence_error(
                    "loader evidence reference is not deterministic",
                ));
            }

            let artifact_prefix = [
                ".pueue-agent".to_owned(),
                "artifacts".to_owned(),
                source_experiment_id.clone(),
            ];
            let mut candidate_paths = Vec::with_capacity(checkpoint_candidates.len());
            let mut candidate_logical_bytes = 0_u64;
            for candidate in checkpoint_candidates {
                validate_candidate_evidence(
                    candidate,
                    source_experiment_id,
                    normalized_working_directory,
                    &artifact_prefix,
                )?;
                if candidate.file.root != *common_root {
                    return Err(checkpoint_evidence_error(
                        "checkpoint file roots do not share the source root",
                    ));
                }
                if candidate.file.logical_bytes
                    > crate::environment::MAX_PRIVATE_TEMP_ALLOCATED_BYTES
                    || candidate.file.allocated_bytes
                        > crate::environment::MAX_PRIVATE_TEMP_ALLOCATED_BYTES
                {
                    return Err(checkpoint_evidence_error(
                        "checkpoint candidate exceeds the fixed byte bound",
                    ));
                }
                candidate_logical_bytes = candidate_logical_bytes
                    .checked_add(candidate.file.logical_bytes)
                    .ok_or_else(|| {
                        checkpoint_evidence_error(
                            "checkpoint candidate aggregate exceeds the fixed byte bound",
                        )
                    })?;
                if candidate_logical_bytes > crate::environment::MAX_PRIVATE_TEMP_ALLOCATED_BYTES {
                    return Err(checkpoint_evidence_error(
                        "checkpoint candidate aggregate exceeds the fixed byte bound",
                    ));
                }
                if !references.insert(candidate.reference.as_str()) {
                    return Err(checkpoint_evidence_error(
                        "checkpoint evidence references must be unique",
                    ));
                }
                candidate_paths.push(candidate.root_relative_path.as_str());
            }
            candidate_paths.sort_unstable();
            candidate_paths.dedup();
            if candidate_paths.len() != checkpoint_candidates.len() {
                return Err(checkpoint_evidence_error(
                    "checkpoint candidate paths must be unique",
                ));
            }
            for (ordinal, root_relative_path) in candidate_paths.iter().enumerate() {
                let candidate = checkpoint_candidates
                    .iter()
                    .find(|candidate| candidate.root_relative_path == *root_relative_path)
                    .expect("candidate path was validated");
                let expected_reference = format!(
                    "checkpoint:{source_experiment_id}:{ordinal}:{}",
                    candidate.sha256
                );
                if candidate.reference != expected_reference {
                    return Err(checkpoint_evidence_error(
                        "checkpoint evidence reference is not deterministic",
                    ));
                }
            }
            Ok(())
        }
    }
}

fn validate_loader_evidence(
    loader: &CheckpointLoaderEvidenceV1,
    normalized_working_directory: &str,
) -> Result<(), AppError> {
    if loader.role != CheckpointLoaderRole::Entrypoint
        || loader.argv_index > 1
        || loader.argv_token.is_empty()
        || loader.argv_token.len() > MAX_CHECKPOINT_PATH_BYTES
        || has_shell_payload(&loader.argv_token)
        || normal_relative_components(&loader.root_relative_path).is_none()
        || loader.file.relative_path != loader.root_relative_path
        || !is_lower_hex_digest(&loader.sha256)
        || loader.file.sha256 != loader.sha256
        || loader.length != loader.file.logical_bytes
        || usize::try_from(loader.length).ok() != Some(loader.content.len())
        || loader.content.len() > MAX_CHECKPOINT_SOURCE_BYTES
        || format!("{:x}", Sha256::digest(loader.content.as_bytes())) != loader.sha256
        || !permits_lossless_evidence_text(&loader.content)
    {
        return Err(checkpoint_evidence_error(
            "loader evidence does not match the complete source record",
        ));
    }
    let lookup_token = loader
        .argv_token
        .strip_prefix("./")
        .unwrap_or(loader.argv_token.as_str());
    if (loader.argv_index == 0 && !is_direct_project_entrypoint(&loader.argv_token))
        || (loader.argv_index == 1 && !is_interpreter_entrypoint(&loader.argv_token))
    {
        return Err(checkpoint_evidence_error(
            "loader argv token is not an accepted entrypoint",
        ));
    }
    let cwd = normal_relative_components(normalized_working_directory)
        .ok_or_else(|| checkpoint_evidence_error("loader working directory is invalid"))?;
    let token = normal_relative_components(lookup_token)
        .ok_or_else(|| checkpoint_evidence_error("loader argv token is not normalized"))?;
    let mut expected_path = cwd;
    expected_path.extend(token);
    if expected_path.join("/") != loader.root_relative_path {
        return Err(checkpoint_evidence_error(
            "loader path is not the command entrypoint",
        ));
    }
    Ok(())
}

fn loader_is_direct_child_of_cwd(
    loader: &CheckpointLoaderEvidenceV1,
    normalized_working_directory: &str,
) -> bool {
    let Some(cwd) = normal_relative_components(normalized_working_directory) else {
        return false;
    };
    let Some(loader_path) = normal_relative_components(&loader.root_relative_path) else {
        return false;
    };
    loader_path.len() == cwd.len() + 1 && loader_path.starts_with(&cwd)
}

fn validate_candidate_evidence(
    candidate: &CheckpointCandidateEvidenceV1,
    source_experiment_id: &str,
    normalized_working_directory: &str,
    artifact_prefix: &[String; 3],
) -> Result<(), AppError> {
    let candidate_components = normal_relative_components(&candidate.root_relative_path)
        .ok_or_else(|| checkpoint_evidence_error("candidate path is not normalized"))?;
    if candidate.source_experiment_id != source_experiment_id
        || candidate.file.relative_path != candidate.root_relative_path
        || !candidate_components.starts_with(artifact_prefix)
        || candidate_components.len() <= artifact_prefix.len()
        || checkpoint_candidate_argv_path(
            normalized_working_directory,
            &candidate.root_relative_path,
        ) != Some(candidate.argv_path.clone())
        || !is_lower_hex_digest(&candidate.sha256)
        || candidate.file.sha256 != candidate.sha256
        || candidate.length != candidate.file.logical_bytes
    {
        return Err(checkpoint_evidence_error(
            "checkpoint candidate record or path is invalid",
        ));
    }
    Ok(())
}

fn validate_context_bindings(
    context: &Value,
    source_experiment_id: &str,
    source_proposal_id: &str,
    source_submission_id: &str,
) -> Result<(), AppError> {
    let Some(facts_value) = context.get("facts") else {
        return Ok(());
    };
    let Some(facts) = facts_value.as_object() else {
        return Err(checkpoint_evidence_error(
            "persisted context facts are malformed",
        ));
    };
    check_optional_context_id(facts.get("review"), "experiment_id", source_experiment_id)?;
    check_optional_context_id(facts.get("target"), "experiment_id", source_experiment_id)?;
    check_optional_context_id(facts.get("target"), "proposal_id", source_proposal_id)?;
    check_optional_context_id(facts.get("target"), "submission_id", source_submission_id)?;
    Ok(())
}

fn check_optional_context_id(
    value: Option<&Value>,
    field: &'static str,
    expected: &str,
) -> Result<(), AppError> {
    let Some(value) = value else {
        return Ok(());
    };
    let Some(object) = value.as_object() else {
        return Err(checkpoint_evidence_error(
            "persisted context binding is malformed",
        ));
    };
    let Some(actual) = object.get(field) else {
        return Ok(());
    };
    if actual.as_str() != Some(expected) {
        return Err(checkpoint_evidence_error(
            "persisted context binding does not match support evidence",
        ));
    }
    Ok(())
}

fn valid_bounded_identity(value: &str) -> bool {
    crate::environment::validate_research_id(value).is_ok()
}

fn is_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn parse_strict_json_value(bytes: &[u8]) -> Result<Value, AppError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = StrictJsonValue
        .deserialize(&mut deserializer)
        .map_err(|_| checkpoint_evidence_error("persisted context is not strict JSON"))?;
    deserializer
        .end()
        .map_err(|_| checkpoint_evidence_error("persisted context has trailing data"))?;
    Ok(value)
}

struct StrictJsonValue;

impl<'de> de::DeserializeSeed<'de> for StrictJsonValue {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictJsonValueVisitor)
    }
}

struct StrictJsonValueVisitor;

impl<'de> de::Visitor<'de> for StrictJsonValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a strict JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Number(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| de::Error::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Null)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(StrictJsonValue)? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut object = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if object.contains_key(&key) {
                return Err(de::Error::custom("duplicate JSON object key"));
            }
            let value = map.next_value_seed(StrictJsonValue)?;
            object.insert(key, value);
        }
        Ok(Value::Object(object))
    }
}

fn normal_relative_components(value: &str) -> Option<Vec<String>> {
    if value == "." {
        return Some(Vec::new());
    }
    if value.is_empty()
        || value.len() > MAX_CHECKPOINT_PATH_BYTES
        || value.chars().any(char::is_control)
    {
        return None;
    }
    let path = Path::new(value);
    if path.is_absolute() {
        return None;
    }
    let components = path
        .components()
        .map(|component| match component {
            Component::Normal(component) => component.to_str().map(ToOwned::to_owned),
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => None,
        })
        .collect::<Option<Vec<_>>>()?;
    if components.is_empty() || components.join("/") != value {
        return None;
    }
    Some(components)
}

/// Validate only the exact argv delta.  This is useful when the surrounding
/// proposal/cwd policy has already been checked by the caller.
pub(crate) fn validate_checkpoint_argv_delta(
    source_argv: &[String],
    request_argv: &[String],
    checkpoint_path: &str,
) -> Result<CheckpointArgvDelta, AppError> {
    validate_checkpoint_path(checkpoint_path)?;
    if source_argv.is_empty() || request_argv.is_empty() {
        return Err(checkpoint_validation_error("argv must be non-empty"));
    }

    let mut candidates = Vec::new();
    let mut standalone_path_count = 0usize;
    let mut equals_path_count = 0usize;
    for (index, token) in request_argv.iter().enumerate() {
        if token == checkpoint_path {
            standalone_path_count = standalone_path_count.saturating_add(1);
            if index > 0 && is_checkpoint_flag(&request_argv[index - 1]) {
                candidates.push(CheckpointArgvDelta {
                    form: CheckpointArgvDeltaForm::Pair,
                    index: index - 1,
                    flag: request_argv[index - 1].clone(),
                });
            }
        }
        if let Some((flag, value)) = token.split_once('=') {
            if value == checkpoint_path && is_checkpoint_flag(flag) {
                equals_path_count = equals_path_count.saturating_add(1);
                candidates.push(CheckpointArgvDelta {
                    form: CheckpointArgvDeltaForm::Equals,
                    index,
                    flag: flag.to_owned(),
                });
            }
        }
    }

    if candidates.len() != 1 {
        return Err(checkpoint_validation_error(
            "argv must contain exactly one checkpoint delta",
        ));
    }
    let delta = candidates.remove(0);
    match delta.form {
        CheckpointArgvDeltaForm::Pair if standalone_path_count != 1 || equals_path_count != 0 => {
            return Err(checkpoint_validation_error(
                "checkpoint path must occur exactly once as a pair value",
            ));
        }
        CheckpointArgvDeltaForm::Equals if standalone_path_count != 0 || equals_path_count != 1 => {
            return Err(checkpoint_validation_error(
                "checkpoint path must occur exactly once as an equals value",
            ));
        }
        _ => {}
    }

    if source_argv
        .iter()
        .any(|token| token == delta.flag() || token.starts_with(&format!("{}=", delta.flag())))
    {
        return Err(checkpoint_validation_error(
            "checkpoint flag already exists in source argv",
        ));
    }

    let mut stripped = request_argv.to_vec();
    match delta.form {
        CheckpointArgvDeltaForm::Pair => {
            let end = delta
                .index
                .checked_add(2)
                .ok_or_else(|| checkpoint_validation_error("checkpoint argv index overflow"))?;
            if end > stripped.len() {
                return Err(checkpoint_validation_error(
                    "checkpoint argv delta index is invalid",
                ));
            }
            stripped.drain(delta.index..end);
        }
        CheckpointArgvDeltaForm::Equals => {
            if delta.index >= stripped.len() {
                return Err(checkpoint_validation_error(
                    "checkpoint argv delta index is invalid",
                ));
            }
            stripped.remove(delta.index);
        }
    }
    if stripped != source_argv {
        return Err(checkpoint_validation_error(
            "request argv changes source tokens outside the checkpoint delta",
        ));
    }
    Ok(delta)
}

fn validate_command_proposal(
    argv: &[String],
    working_directory: &str,
) -> Result<proposals::ValidatedProposal, AppError> {
    proposals::validate(
        ProposalInput {
            kind: ProposalKind::Experiment,
            hypothesis: String::new(),
            source_experiment_id: None,
            argv: argv.to_vec(),
            working_directory: working_directory.to_owned(),
            expected_evidence: Vec::new(),
        },
        "research-checkpoint",
    )
}

fn validate_command_argv(argv: &[String]) -> Result<(), AppError> {
    let mut add_args = Vec::with_capacity(argv.len() + 1);
    add_args.push(OsString::from("--"));
    add_args.extend(argv.iter().map(OsString::from));
    validate_add_argv(&add_args)
}

fn validate_trainer_command_shape(argv: &[String]) -> Result<(), AppError> {
    if argv.is_empty() {
        return Err(checkpoint_validation_error(
            "trainer argv must be non-empty",
        ));
    }
    if argv.iter().any(|token| has_shell_payload(token)) {
        return Err(checkpoint_validation_error(
            "shell payloads are unsupported for checkpoint continuation",
        ));
    }

    let program = argv[0].as_str();
    if SUPPORTED_INTERPRETERS.contains(&program) {
        if argv.len() < 2 || !is_interpreter_entrypoint(&argv[1]) {
            return Err(checkpoint_validation_error(
                "interpreter command must have a direct relative trainer entrypoint",
            ));
        }
        return Ok(());
    }
    if is_shell_or_wrapper(program)
        || Path::new(program)
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(is_shell_or_wrapper)
        || !is_direct_project_entrypoint(program)
    {
        return Err(checkpoint_validation_error(
            "trainer command must use a direct project-relative entrypoint",
        ));
    }
    Ok(())
}

fn validate_checkpoint_path(path: &str) -> Result<(), AppError> {
    if path.is_empty()
        || path.len() > MAX_CHECKPOINT_PATH_BYTES
        || path.chars().any(char::is_control)
        || has_shell_payload(path)
    {
        return Err(checkpoint_validation_error(
            "checkpoint path must be a bounded relative path",
        ));
    }
    let path = Path::new(path);
    if path.is_absolute() {
        return Err(checkpoint_validation_error(
            "checkpoint path must be relative",
        ));
    }
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(component) => {
                components.push(component.to_string_lossy().into_owned())
            }
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(checkpoint_validation_error(
                    "checkpoint path must use normal relative components",
                ));
            }
        }
    }
    if components.is_empty() || components.join("/") != path.to_string_lossy() {
        return Err(checkpoint_validation_error(
            "checkpoint path must use normal relative components",
        ));
    }
    Ok(())
}

fn validate_retained_path(path: &str) -> Result<(), AppError> {
    if path.is_empty()
        || path.len() > MAX_CHECKPOINT_PATH_BYTES
        || path.chars().any(char::is_control)
        || has_shell_payload(path)
        || !Path::new(path).is_absolute()
    {
        return Err(checkpoint_validation_error(
            "retained checkpoint path is invalid",
        ));
    }
    let mut normal_components = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::RootDir if normal_components.is_empty() => {}
            Component::Normal(component) => {
                normal_components.push(component.to_string_lossy().into_owned())
            }
            Component::CurDir
            | Component::ParentDir
            | Component::Prefix(_)
            | Component::RootDir => {
                return Err(checkpoint_validation_error(
                    "retained checkpoint path must use absolute normal components",
                ));
            }
        }
    }
    if normal_components.is_empty() {
        return Err(checkpoint_validation_error(
            "retained checkpoint path must name a file",
        ));
    }
    if path != format!("/{}", normal_components.join("/")) {
        return Err(checkpoint_validation_error(
            "retained checkpoint path must use absolute normal components",
        ));
    }
    Ok(())
}

fn is_checkpoint_flag(value: &str) -> bool {
    value.starts_with("--")
        && !value.starts_with("---")
        && value.len() > 2
        && !value.contains('=')
        && !has_shell_payload(value)
        && value[2..]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn is_interpreter_entrypoint(value: &str) -> bool {
    if value.is_empty() || value.starts_with('-') || has_shell_payload(value) {
        return false;
    }
    let path = Path::new(value);
    if path.is_absolute() {
        return false;
    }
    let mut has_normal = false;
    if !path.components().all(|component| {
        if matches!(component, Component::Normal(_)) {
            has_normal = true;
            true
        } else {
            matches!(component, Component::CurDir)
        }
    }) {
        return false;
    }
    has_normal
}

fn is_direct_project_entrypoint(value: &str) -> bool {
    if !value.starts_with("./") && !value.contains('/') {
        return false;
    }
    if !is_interpreter_entrypoint(value) {
        return false;
    }
    let mut component_index = 0;
    for component in Path::new(value).components() {
        if matches!(component, Component::CurDir) && component_index != 0 {
            return false;
        }
        component_index += 1;
    }
    true
}

fn is_shell_or_wrapper(value: &str) -> bool {
    matches!(
        value,
        "sh" | "bash"
            | "dash"
            | "zsh"
            | "fish"
            | "csh"
            | "ksh"
            | "tcsh"
            | "cmd"
            | "cmd.exe"
            | "powershell"
            | "pwsh"
            | "env"
            | "sudo"
            | "doas"
            | "nice"
            | "timeout"
            | "xargs"
            | "exec"
            | "time"
            | "poetry"
            | "uv"
            | "pipenv"
            | "conda"
            | "docker"
            | "podman"
            | "singularity"
            | "srun"
            | "mpirun"
            | "mpiexec"
            | "torchrun"
    )
}

fn has_shell_payload(value: &str) -> bool {
    value.bytes().any(|byte| {
        matches!(
            byte,
            b'\\' | b'\'' | b'"' | b'`' | b'$' | b';' | b'|' | b'&' | b'<' | b'>'
        )
    })
}

fn checkpoint_validation_error(message: &'static str) -> AppError {
    AppError::Validation {
        field: "checkpoint.argv",
        message,
    }
}

fn checkpoint_evidence_error(message: &'static str) -> AppError {
    AppError::Validation {
        field: "checkpoint_support",
        message,
    }
}

fn prepared_checkpoint_error(message: &'static str) -> AppError {
    AppError::Validation {
        field: "prepared_checkpoint",
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(path: &str, argv: &[&str], working_directory: &str) -> CheckpointRequest {
        CheckpointRequest {
            path: path.to_owned(),
            argv: argv.iter().map(|value| (*value).to_owned()).collect(),
            working_directory: working_directory.to_owned(),
            support_evidence_refs: vec!["loader:entrypoint".to_owned()],
        }
    }

    #[test]
    fn accepts_pair_delta_and_replaces_only_exact_path_value() {
        let source = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "--lr".to_owned(),
            "0.001".to_owned(),
        ];
        let request = request(
            "checkpoints/step-20.json",
            &[
                "python",
                "train.py",
                "--resume",
                "checkpoints/step-20.json",
                "--lr",
                "0.001",
            ],
            ".",
        );

        let command = validate_checkpoint_command(&source, ".", &request).unwrap();
        assert_eq!(command.delta().form(), CheckpointArgvDeltaForm::Pair);
        assert_eq!(command.delta().index(), 2);
        assert_eq!(
            command
                .reapply_retained_path("/private/checkpoint")
                .unwrap(),
            vec![
                "python",
                "train.py",
                "--resume",
                "/private/checkpoint",
                "--lr",
                "0.001",
            ]
        );
    }

    #[test]
    fn accepts_equals_delta_and_preserves_learning_arguments() {
        let source = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "--lr".to_owned(),
            "0.001".to_owned(),
            "--steps".to_owned(),
            "20".to_owned(),
        ];
        let request = request(
            "checkpoints/step-20.json",
            &[
                "python",
                "train.py",
                "--lr",
                "0.001",
                "--steps",
                "20",
                "--resume=checkpoints/step-20.json",
            ],
            ".",
        );

        let command = validate_checkpoint_command(&source, ".", &request).unwrap();
        assert_eq!(command.delta().form(), CheckpointArgvDeltaForm::Equals);
        assert_eq!(command.delta().index(), 6);
        assert_eq!(
            command
                .reapply_retained_path("/private/checkpoint")
                .unwrap(),
            vec![
                "python",
                "train.py",
                "--lr",
                "0.001",
                "--steps",
                "20",
                "--resume=/private/checkpoint",
            ]
        );
        assert_eq!(command.source_argv(), source.as_slice());
    }

    #[test]
    fn rejects_changed_learning_specification_or_order() {
        let source = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "--lr".to_owned(),
            "0.001".to_owned(),
            "--steps".to_owned(),
            "20".to_owned(),
        ];
        for argv in [
            vec![
                "python",
                "train.py",
                "--lr",
                "0.01",
                "--steps",
                "20",
                "--resume",
                "checkpoint.json",
            ],
            vec![
                "python",
                "train.py",
                "--steps",
                "20",
                "--lr",
                "0.001",
                "--resume",
                "checkpoint.json",
            ],
            vec![
                "python",
                "other.py",
                "--lr",
                "0.001",
                "--steps",
                "20",
                "--resume",
                "checkpoint.json",
            ],
        ] {
            let request = request("checkpoint.json", &argv, ".");
            assert!(validate_checkpoint_command(&source, ".", &request).is_err());
        }
    }

    #[test]
    fn rejects_ambiguous_or_existing_checkpoint_delta() {
        let source = vec!["python".to_owned(), "train.py".to_owned()];
        for argv in [
            vec![
                "python",
                "train.py",
                "--resume",
                "checkpoint.json",
                "--resume",
                "checkpoint.json",
            ],
            vec![
                "python",
                "train.py",
                "--resume=checkpoint.json",
                "--resume",
                "checkpoint.json",
            ],
        ] {
            let request = request("checkpoint.json", &argv, ".");
            assert!(validate_checkpoint_command(&source, ".", &request).is_err());
        }

        let source_with_flag = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "--resume".to_owned(),
            "old.json".to_owned(),
        ];
        let request = request(
            "checkpoint.json",
            &[
                "python",
                "train.py",
                "--resume",
                "old.json",
                "--resume",
                "checkpoint.json",
            ],
            ".",
        );
        assert!(validate_checkpoint_command(&source_with_flag, ".", &request).is_err());
    }

    #[test]
    fn rejects_substrings_shell_payloads_and_non_long_flags() {
        let source = vec!["python".to_owned(), "train.py".to_owned()];
        for (path, argv) in [
            (
                "checkpoint.json",
                vec!["python", "train.py", "--resume", "checkpoint.json.bak"],
            ),
            (
                "checkpoint.json",
                vec!["python", "train.py", "-r", "checkpoint.json"],
            ),
            (
                "checkpoint.json",
                vec!["python", "train.py", "--resume/path", "checkpoint.json"],
            ),
            (
                "checkpoint.json",
                vec!["python", "train.py", "--resume=prefix-checkpoint.json"],
            ),
            (
                "checkpoint.json",
                vec![
                    "python",
                    "train.py; rm -rf /",
                    "--resume",
                    "checkpoint.json",
                ],
            ),
        ] {
            let request = request(path, &argv, ".");
            assert!(validate_checkpoint_command(&source, ".", &request).is_err());
        }
    }

    #[test]
    fn checkpoint_delta_roundtrips_and_rejects_unknown_fields() {
        let delta = CheckpointArgvDelta {
            form: CheckpointArgvDeltaForm::Pair,
            index: 2,
            flag: "--resume".to_owned(),
        };
        let encoded = serde_json::to_vec(&delta).unwrap();
        assert_eq!(
            serde_json::from_slice::<CheckpointArgvDelta>(&encoded).unwrap(),
            delta
        );

        let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        value["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<CheckpointArgvDelta>(value).is_err());
    }

    #[test]
    fn shell_guard_isolated_from_valid_delta_when_original_token_is_retained() {
        let source = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "literal;token".to_owned(),
        ];
        let request_argv = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "literal;token".to_owned(),
            "--resume".to_owned(),
            "checkpoint.json".to_owned(),
        ];
        assert!(validate_checkpoint_argv_delta(&source, &request_argv, "checkpoint.json").is_ok());
        assert!(validate_checkpoint_command(
            &source,
            ".",
            &request(
                "checkpoint.json",
                &[
                    "python",
                    "train.py",
                    "literal;token",
                    "--resume",
                    "checkpoint.json"
                ],
                "."
            )
        )
        .is_err());
    }

    #[test]
    fn rejects_dot_only_interpreter_and_direct_trainer_entrypoints() {
        for (source, request_argv) in [
            (
                vec!["python".to_owned(), ".".to_owned()],
                vec!["python", ".", "--resume", "checkpoint.json"],
            ),
            (
                vec!["./".to_owned()],
                vec!["./", "--resume", "checkpoint.json"],
            ),
        ] {
            let request = request("checkpoint.json", &request_argv, ".");
            assert!(validate_checkpoint_command(&source, ".", &request).is_err());
        }
    }

    #[test]
    fn source_layout_and_candidate_projection_preserve_lookup_rules() {
        let interpreter =
            checkpoint_source_layout(&["python".to_owned(), "train.py".to_owned()], ".").unwrap();
        assert_eq!(interpreter.entrypoint_index, 1);
        assert_eq!(interpreter.entrypoint_token, "train.py");
        assert_eq!(interpreter.entrypoint_root_relative_path, "train.py");

        let direct = checkpoint_source_layout(&["./train.py".to_owned()], ".").unwrap();
        assert_eq!(direct.entrypoint_index, 0);
        assert_eq!(direct.entrypoint_token, "./train.py");
        assert_eq!(direct.entrypoint_root_relative_path, "train.py");

        for argv in [
            vec!["train.py".to_owned()],
            vec![".".to_owned()],
            vec!["./".to_owned()],
        ] {
            assert!(checkpoint_source_layout(&argv, ".").is_err());
        }

        let root = checkpoint_source_layout(
            &vec!["python".to_owned(), "./trainer/train.py".to_owned()],
            "runs/exp",
        )
        .unwrap();
        assert_eq!(root.normalized_working_directory, "runs/exp");
        assert_eq!(root.entrypoint_index, 1);
        assert_eq!(root.entrypoint_token, "./trainer/train.py");
        assert_eq!(
            root.entrypoint_root_relative_path,
            "runs/exp/trainer/train.py"
        );

        assert_eq!(
            checkpoint_candidate_argv_path(
                "runs/exp",
                "runs/exp/.pueue-agent/artifacts/e/checkpoint.json"
            ),
            Some(".pueue-agent/artifacts/e/checkpoint.json".to_owned())
        );
        assert_eq!(
            checkpoint_candidate_argv_path("runs/exp", "runs/other/checkpoint.json"),
            None
        );
        assert_eq!(checkpoint_candidate_argv_path("runs/exp", "runs/exp"), None);
        assert_eq!(
            checkpoint_candidate_argv_path("runs/exp", "runs/exp/../other"),
            None
        );
    }

    fn sample_directory() -> ResearchDirectoryRecord {
        ResearchDirectoryRecord {
            device: 1,
            inode: 2,
            owner: 3,
            mode: 0o700,
            mount_identity: [4, 5],
        }
    }

    fn sample_file(relative_path: &str, bytes: &[u8]) -> ResearchFileRecord {
        let directory = sample_directory();
        let mount_identity = directory.mount_identity;
        ResearchFileRecord {
            relative_path: relative_path.to_owned(),
            root: directory.clone(),
            parent: directory,
            device: 6,
            inode: 7,
            owner: 3,
            mode: 0o600,
            mount_identity,
            logical_bytes: bytes.len() as u64,
            allocated_bytes: 512,
            sha256: format!("{:x}", Sha256::digest(bytes)),
        }
    }

    fn sample_support() -> CheckpointSupportEvidenceV1 {
        let source = "x = 1\n";
        let source_sha256 = format!("{:x}", Sha256::digest(source.as_bytes()));
        let candidate_path = ".pueue-agent/artifacts/exp/checkpoint.json";
        let candidate_bytes = b"checkpoint";
        let candidate_sha256 = format!("{:x}", Sha256::digest(candidate_bytes));
        CheckpointSupportEvidenceV1::Available {
            support_version: CHECKPOINT_SUPPORT_VERSION,
            source_experiment_id: "exp".to_owned(),
            source_proposal_id: "proposal".to_owned(),
            source_submission_id: "submission".to_owned(),
            normalized_working_directory: ".".to_owned(),
            working_directory_record: sample_directory(),
            loader_support: vec![CheckpointLoaderEvidenceV1 {
                reference: format!("loader-source:{source_sha256}"),
                role: CheckpointLoaderRole::Entrypoint,
                argv_index: 1,
                argv_token: "train.py".to_owned(),
                root_relative_path: "train.py".to_owned(),
                length: source.len() as u64,
                sha256: source_sha256,
                file: sample_file("train.py", source.as_bytes()),
                content: source.to_owned(),
            }],
            checkpoint_candidates: vec![CheckpointCandidateEvidenceV1 {
                reference: format!("checkpoint:exp:0:{candidate_sha256}"),
                source_experiment_id: "exp".to_owned(),
                argv_path: candidate_path.to_owned(),
                root_relative_path: candidate_path.to_owned(),
                length: candidate_bytes.len() as u64,
                sha256: candidate_sha256,
                file: sample_file(candidate_path, candidate_bytes),
            }],
            candidates_complete: true,
            candidates_omitted_at_least: 0,
            candidate_limit: MAX_CHECKPOINT_CANDIDATES,
        }
    }

    fn context_for_support(support: CheckpointSupportEvidenceV1) -> (String, String) {
        let context = serde_json::json!({
            "schema_version": crate::research_evidence::RESEARCH_CONTEXT_SCHEMA_VERSION,
            "facts": {
                "review": {"experiment_id": "exp"},
                "target": {
                    "experiment_id": "exp",
                    "proposal_id": "proposal",
                    "submission_id": "submission"
                }
            },
            "operations": {"checkpoint_support": support}
        });
        let context_json = serde_json::to_string(&context).unwrap();
        let digest = format!("{:x}", Sha256::digest(context_json.as_bytes()));
        (context_json, digest)
    }

    fn sample_support_with_two_candidates() -> CheckpointSupportEvidenceV1 {
        let CheckpointSupportEvidenceV1::Available {
            support_version,
            source_experiment_id,
            source_proposal_id,
            source_submission_id,
            normalized_working_directory,
            working_directory_record,
            loader_support,
            mut checkpoint_candidates,
            candidates_complete,
            candidates_omitted_at_least,
            candidate_limit,
        } = sample_support()
        else {
            unreachable!();
        };
        let bytes = b"checkpoint";
        let sha256 = format!("{:x}", Sha256::digest(bytes));
        let second_path = ".pueue-agent/artifacts/exp/other.json";
        checkpoint_candidates.push(CheckpointCandidateEvidenceV1 {
            reference: format!("checkpoint:exp:1:{sha256}"),
            source_experiment_id: source_experiment_id.clone(),
            argv_path: second_path.to_owned(),
            root_relative_path: second_path.to_owned(),
            length: bytes.len() as u64,
            sha256,
            file: sample_file(second_path, bytes),
        });
        CheckpointSupportEvidenceV1::Available {
            support_version,
            source_experiment_id,
            source_proposal_id,
            source_submission_id,
            normalized_working_directory,
            working_directory_record,
            loader_support,
            checkpoint_candidates,
            candidates_complete,
            candidates_omitted_at_least,
            candidate_limit,
        }
    }

    fn parse_context_value(
        value: serde_json::Value,
    ) -> Result<CheckpointSupportEvidenceV1, AppError> {
        let context = serde_json::to_string(&value).unwrap();
        let digest = format!("{:x}", Sha256::digest(context.as_bytes()));
        checkpoint_support_from_persisted_context(&context, &digest)
    }

    fn sample_request(path: &str, refs: &[String]) -> CheckpointRequest {
        CheckpointRequest {
            path: path.to_owned(),
            argv: vec!["python".to_owned(), "train.py".to_owned()],
            working_directory: ".".to_owned(),
            support_evidence_refs: refs.to_owned(),
        }
    }

    #[test]
    fn support_selector_requires_one_loader_and_one_candidate_in_either_order() {
        let support = sample_support();
        let (loader_ref, candidate_ref, candidate_path) = match &support {
            CheckpointSupportEvidenceV1::Available {
                loader_support,
                checkpoint_candidates,
                ..
            } => (
                loader_support[0].reference.clone(),
                checkpoint_candidates[0].reference.clone(),
                checkpoint_candidates[0].argv_path.clone(),
            ),
            CheckpointSupportEvidenceV1::Unavailable { .. } => unreachable!(),
        };

        for refs in [
            vec![loader_ref.clone(), candidate_ref.clone()],
            vec![candidate_ref.clone(), loader_ref.clone()],
        ] {
            let selected =
                select_checkpoint_support(&support, &sample_request(&candidate_path, &refs))
                    .unwrap();
            assert_eq!(selected.loader.reference, loader_ref);
            assert_eq!(selected.candidate.reference, candidate_ref);
        }

        let two_candidates = sample_support_with_two_candidates();
        let second_ref = match &two_candidates {
            CheckpointSupportEvidenceV1::Available {
                checkpoint_candidates,
                ..
            } => checkpoint_candidates[1].reference.clone(),
            CheckpointSupportEvidenceV1::Unavailable { .. } => unreachable!(),
        };
        for refs in [
            vec![candidate_ref.clone()],
            vec![loader_ref.clone()],
            vec![loader_ref.clone(), loader_ref.clone()],
            vec![loader_ref.clone(), "foreign".to_owned()],
            vec![loader_ref.clone(), candidate_ref.clone(), second_ref],
        ] {
            assert!(select_checkpoint_support(
                &two_candidates,
                &sample_request(&candidate_path, &refs)
            )
            .is_err());
        }
        assert!(select_checkpoint_support(
            &support,
            &sample_request("other/checkpoint.json", &[loader_ref, candidate_ref])
        )
        .is_err());

        let unavailable = CheckpointSupportEvidenceV1::Unavailable {
            support_version: CHECKPOINT_SUPPORT_VERSION,
            reason: "unsupported".to_owned(),
            loader_support: Vec::new(),
            checkpoint_candidates: Vec::new(),
            candidates_complete: false,
            candidates_omitted_at_least: 0,
            candidate_limit: MAX_CHECKPOINT_CANDIDATES,
        };
        assert!(select_checkpoint_support(
            &unavailable,
            &sample_request(
                &candidate_path,
                &["loader".to_owned(), "candidate".to_owned()]
            )
        )
        .is_err());
    }

    #[test]
    fn parser_requires_common_root_and_provable_cwd_parent_identity() {
        let (context, _) = context_for_support(sample_support());
        let mut value: serde_json::Value = serde_json::from_str(&context).unwrap();
        value["operations"]["checkpoint_support"]["working_directory_record"]["inode"] =
            serde_json::json!(999);
        assert!(parse_context_value(value).is_err());

        let (context, _) = context_for_support(sample_support());
        let mut value: serde_json::Value = serde_json::from_str(&context).unwrap();
        value["operations"]["checkpoint_support"]["checkpoint_candidates"][0]["file"]["root"]
            ["inode"] = serde_json::json!(999);
        assert!(parse_context_value(value).is_err());

        let (context, _) = context_for_support(sample_support());
        let mut value: serde_json::Value = serde_json::from_str(&context).unwrap();
        value["operations"]["checkpoint_support"]["loader_support"][0]["file"]["parent"]["inode"] =
            serde_json::json!(999);
        assert!(parse_context_value(value).is_err());

        let (context, _) = context_for_support(sample_support());
        let mut value: serde_json::Value = serde_json::from_str(&context).unwrap();
        value["operations"]["checkpoint_support"]["loader_support"][0]["argv_token"] =
            serde_json::json!("trainer/train.py");
        value["operations"]["checkpoint_support"]["loader_support"][0]["root_relative_path"] =
            serde_json::json!("trainer/train.py");
        value["operations"]["checkpoint_support"]["loader_support"][0]["file"]["relative_path"] =
            serde_json::json!("trainer/train.py");
        value["operations"]["checkpoint_support"]["loader_support"][0]["file"]["parent"]["inode"] =
            serde_json::json!(999);
        assert!(parse_context_value(value).is_ok());
    }

    #[test]
    fn parser_rejects_present_non_object_facts() {
        for facts in [serde_json::json!("malformed"), serde_json::json!([])] {
            let (context, _) = context_for_support(sample_support());
            let mut value: serde_json::Value = serde_json::from_str(&context).unwrap();
            value["facts"] = facts;
            assert!(parse_context_value(value).is_err());
        }
    }

    #[test]
    fn parser_enforces_fixed_candidate_byte_budgets_and_exact_boundary() {
        let limit = crate::environment::MAX_PRIVATE_TEMP_ALLOCATED_BYTES;

        let (context, _) = context_for_support(sample_support());
        let mut value: serde_json::Value = serde_json::from_str(&context).unwrap();
        value["operations"]["checkpoint_support"]["checkpoint_candidates"][0]["length"] =
            serde_json::json!(limit + 1);
        value["operations"]["checkpoint_support"]["checkpoint_candidates"][0]["file"]
            ["logical_bytes"] = serde_json::json!(limit + 1);
        assert!(parse_context_value(value).is_err());

        let (context, _) = context_for_support(sample_support());
        let mut value: serde_json::Value = serde_json::from_str(&context).unwrap();
        value["operations"]["checkpoint_support"]["checkpoint_candidates"][0]["file"]
            ["allocated_bytes"] = serde_json::json!(limit + 1);
        assert!(parse_context_value(value).is_err());

        let (context, _) = context_for_support(sample_support_with_two_candidates());
        let mut value: serde_json::Value = serde_json::from_str(&context).unwrap();
        for candidate in value["operations"]["checkpoint_support"]["checkpoint_candidates"]
            .as_array_mut()
            .unwrap()
        {
            candidate["length"] = serde_json::json!(limit / 2 + 1);
            candidate["file"]["logical_bytes"] = serde_json::json!(limit / 2 + 1);
        }
        assert!(parse_context_value(value).is_err());

        let (context, _) = context_for_support(sample_support());
        let mut value: serde_json::Value = serde_json::from_str(&context).unwrap();
        value["operations"]["checkpoint_support"]["checkpoint_candidates"][0]["length"] =
            serde_json::json!(limit);
        value["operations"]["checkpoint_support"]["checkpoint_candidates"][0]["file"]
            ["logical_bytes"] = serde_json::json!(limit);
        value["operations"]["checkpoint_support"]["checkpoint_candidates"][0]["file"]
            ["allocated_bytes"] = serde_json::json!(limit);
        assert!(parse_context_value(value).is_ok());
    }

    #[test]
    fn strict_support_parser_accepts_complete_source_and_rejects_unknown_fields() {
        let (context, digest) = context_for_support(sample_support());
        let parsed = checkpoint_support_from_persisted_context(&context, &digest).unwrap();
        assert!(matches!(
            parsed,
            CheckpointSupportEvidenceV1::Available { .. }
        ));

        let mut value: serde_json::Value = serde_json::from_str(&context).unwrap();
        value["operations"]["checkpoint_support"]["unexpected"] = serde_json::json!(true);
        let context = serde_json::to_string(&value).unwrap();
        let digest = format!("{:x}", Sha256::digest(context.as_bytes()));
        assert!(checkpoint_support_from_persisted_context(&context, &digest).is_err());
    }

    #[test]
    fn strict_support_parser_rejects_duplicate_digest_tamper_and_bad_membership() {
        let (context, _digest) = context_for_support(sample_support());
        assert!(checkpoint_support_from_persisted_context(&context, &"a".repeat(64)).is_err());

        let duplicate = format!(
            "{{\"schema_version\":1,\"operations\":{{\"checkpoint_support\":{},\"checkpoint_support\":{}}}}}",
            serde_json::to_string(&sample_support()).unwrap(),
            serde_json::to_string(&sample_support()).unwrap()
        );
        let duplicate_digest = format!("{:x}", Sha256::digest(duplicate.as_bytes()));
        assert!(checkpoint_support_from_persisted_context(&duplicate, &duplicate_digest).is_err());

        let mut value: serde_json::Value = serde_json::from_str(&context).unwrap();
        value["facts"]["target"]["proposal_id"] = serde_json::json!("foreign");
        let tampered = serde_json::to_string(&value).unwrap();
        let tampered_digest = format!("{:x}", Sha256::digest(tampered.as_bytes()));
        assert!(checkpoint_support_from_persisted_context(&tampered, &tampered_digest).is_err());

        let mut value: serde_json::Value = serde_json::from_str(&context).unwrap();
        value["operations"]["checkpoint_support"]["loader_support"][0]["content"] =
            serde_json::json!("x = 2\n");
        let tampered = serde_json::to_string(&value).unwrap();
        let tampered_digest = format!("{:x}", Sha256::digest(tampered.as_bytes()));
        assert!(checkpoint_support_from_persisted_context(&tampered, &tampered_digest).is_err());

        let mut value: serde_json::Value = serde_json::from_str(&context).unwrap();
        value["operations"]["checkpoint_support"]["checkpoint_candidates"][0]
            ["root_relative_path"] = serde_json::json!("foreign/checkpoint.json");
        let tampered = serde_json::to_string(&value).unwrap();
        let tampered_digest = format!("{:x}", Sha256::digest(tampered.as_bytes()));
        assert!(checkpoint_support_from_persisted_context(&tampered, &tampered_digest).is_err());
    }

    #[test]
    fn strict_support_parser_accepts_unique_refs_for_identical_candidate_bytes() {
        let CheckpointSupportEvidenceV1::Available {
            support_version,
            source_experiment_id,
            source_proposal_id,
            source_submission_id,
            normalized_working_directory,
            working_directory_record,
            loader_support,
            mut checkpoint_candidates,
            candidates_complete,
            candidates_omitted_at_least,
            candidate_limit,
        } = sample_support()
        else {
            unreachable!();
        };
        let bytes = b"checkpoint";
        let sha256 = format!("{:x}", Sha256::digest(bytes));
        let second_path = ".pueue-agent/artifacts/exp/other.json";
        checkpoint_candidates[0].reference = format!("checkpoint:exp:0:{sha256}");
        checkpoint_candidates.push(CheckpointCandidateEvidenceV1 {
            reference: format!("checkpoint:exp:1:{sha256}"),
            source_experiment_id: source_experiment_id.clone(),
            argv_path: second_path.to_owned(),
            root_relative_path: second_path.to_owned(),
            length: bytes.len() as u64,
            sha256,
            file: sample_file(second_path, bytes),
        });
        let support = CheckpointSupportEvidenceV1::Available {
            support_version,
            source_experiment_id,
            source_proposal_id,
            source_submission_id,
            normalized_working_directory,
            working_directory_record,
            loader_support,
            checkpoint_candidates,
            candidates_complete,
            candidates_omitted_at_least,
            candidate_limit,
        };
        let (context, digest) = context_for_support(support);
        assert!(checkpoint_support_from_persisted_context(&context, &digest).is_ok());
    }

    #[test]
    fn unavailable_support_is_explicitly_empty_and_missing_packet_has_no_authority() {
        let support = CheckpointSupportEvidenceV1::Unavailable {
            support_version: CHECKPOINT_SUPPORT_VERSION,
            reason: "no representable checkpoint".to_owned(),
            loader_support: Vec::new(),
            checkpoint_candidates: Vec::new(),
            candidates_complete: false,
            candidates_omitted_at_least: 0,
            candidate_limit: MAX_CHECKPOINT_CANDIDATES,
        };
        let (context, digest) = context_for_support(support);
        assert!(matches!(
            checkpoint_support_from_persisted_context(&context, &digest).unwrap(),
            CheckpointSupportEvidenceV1::Unavailable { .. }
        ));

        let legacy = serde_json::json!({
            "schema_version": 1,
            "operations": {}
        });
        let legacy = serde_json::to_string(&legacy).unwrap();
        let legacy_digest = format!("{:x}", Sha256::digest(legacy.as_bytes()));
        assert!(checkpoint_support_from_persisted_context(&legacy, &legacy_digest).is_err());
    }

    #[test]
    fn rejects_changed_or_non_normalized_working_directory() {
        let source = vec!["python".to_owned(), "train.py".to_owned()];
        let changed = request(
            "checkpoint.json",
            &["python", "train.py", "--resume", "checkpoint.json"],
            "nested",
        );
        assert!(validate_checkpoint_command(&source, ".", &changed).is_err());

        for working_directory in ["../other", "/tmp", "nested/./other", ""] {
            let request = request(
                "checkpoint.json",
                &["python", "train.py", "--resume", "checkpoint.json"],
                working_directory,
            );
            assert!(validate_checkpoint_command(&source, ".", &request).is_err());
        }
    }

    #[test]
    fn supports_only_direct_entrypoint_or_small_interpreter_shape() {
        let direct = vec![
            "./train.py".to_owned(),
            "--steps".to_owned(),
            "20".to_owned(),
        ];
        let direct_request = request(
            "checkpoint.json",
            &["./train.py", "--steps", "20", "--resume", "checkpoint.json"],
            ".",
        );
        assert!(validate_checkpoint_command(&direct, ".", &direct_request).is_ok());

        let nested_direct = vec!["trainer/train.py".to_owned()];
        let nested_request = request(
            "checkpoint.json",
            &["trainer/train.py", "--resume=checkpoint.json"],
            ".",
        );
        assert!(validate_checkpoint_command(&nested_direct, ".", &nested_request).is_ok());

        let interpreter = vec!["python3".to_owned(), "train.py".to_owned()];
        let interpreter_request = request(
            "checkpoint.json",
            &["python3", "train.py", "--resume=checkpoint.json"],
            ".",
        );
        assert!(validate_checkpoint_command(&interpreter, ".", &interpreter_request).is_ok());

        for source_argv in [
            vec!["sh".to_owned(), "-c".to_owned(), "train".to_owned()],
            vec!["./sh".to_owned(), "-c".to_owned(), "train".to_owned()],
            vec!["env".to_owned(), "python".to_owned(), "train.py".to_owned()],
            vec!["python".to_owned(), "-m".to_owned(), "trainer".to_owned()],
            vec!["python".to_owned(), "-u".to_owned(), "train.py".to_owned()],
            vec!["/usr/bin/python".to_owned(), "train.py".to_owned()],
            vec!["train.py".to_owned()],
            vec!["../train.py".to_owned()],
        ] {
            let request = request(
                "checkpoint.json",
                &source_argv
                    .iter()
                    .map(String::as_str)
                    .chain(["--resume", "checkpoint.json"])
                    .collect::<Vec<_>>(),
                ".",
            );
            assert!(validate_checkpoint_command(&source_argv, ".", &request).is_err());
        }
    }

    fn prepared_file(
        relative_path: &str,
        bytes: &[u8],
        root: &ResearchDirectoryRecord,
        parent: &ResearchDirectoryRecord,
    ) -> ResearchFileRecord {
        ResearchFileRecord {
            relative_path: relative_path.to_owned(),
            root: root.clone(),
            parent: parent.clone(),
            device: 6,
            inode: 7,
            owner: 3,
            mode: 0o600,
            mount_identity: root.mount_identity,
            logical_bytes: bytes.len() as u64,
            allocated_bytes: 512,
            sha256: format!("{:x}", Sha256::digest(bytes)),
        }
    }

    fn prepared_fixture(
        working_directory: &str,
        form: CheckpointArgvDeltaForm,
        ordinal: usize,
    ) -> PreparedCheckpoint {
        let root = sample_directory();
        let cwd = if working_directory == "." {
            root.clone()
        } else {
            ResearchDirectoryRecord {
                device: 1,
                inode: 10,
                owner: 3,
                mode: 0o700,
                mount_identity: [4, 5],
            }
        };
        let source = b"print('trainer')\n";
        let candidate = b"checkpoint-bytes";
        let source_path = if working_directory == "." {
            "train.py".to_owned()
        } else {
            format!("{working_directory}/train.py")
        };
        let candidate_root_path = ".pueue-agent/artifacts/experiment/step.json";
        let candidate_argv_path = if working_directory == "." {
            candidate_root_path.to_owned()
        } else {
            "artifacts/experiment/step.json".to_owned()
        };
        let loader_sha256 = format!("{:x}", Sha256::digest(source));
        let candidate_sha256 = format!("{:x}", Sha256::digest(candidate));
        let loader = CheckpointLoaderEvidenceV1 {
            reference: format!("loader-source:{loader_sha256}"),
            role: CheckpointLoaderRole::Entrypoint,
            argv_index: 1,
            argv_token: "train.py".to_owned(),
            root_relative_path: source_path.clone(),
            length: source.len() as u64,
            sha256: loader_sha256,
            file: prepared_file(&source_path, source, &root, &cwd),
            content: String::from_utf8(source.to_vec()).unwrap(),
        };
        let source_checkpoint = CheckpointCandidateEvidenceV1 {
            reference: format!("checkpoint:experiment:{ordinal}:{candidate_sha256}"),
            source_experiment_id: "experiment".to_owned(),
            argv_path: candidate_argv_path.clone(),
            root_relative_path: candidate_root_path.to_owned(),
            length: candidate.len() as u64,
            sha256: candidate_sha256.clone(),
            file: prepared_file(candidate_root_path, candidate, &root, &root),
        };
        let request_path = candidate_argv_path;
        let (request_argv, retained_argv) = match form {
            CheckpointArgvDeltaForm::Pair => (
                vec![
                    "python".to_owned(),
                    "train.py".to_owned(),
                    "--resume".to_owned(),
                    request_path.clone(),
                    "--lr".to_owned(),
                    "0.001".to_owned(),
                ],
                vec![
                    "python".to_owned(),
                    "train.py".to_owned(),
                    "--resume".to_owned(),
                    "/private/state/research-checkpoints/campaign/review/checkpoint"
                        .to_owned(),
                    "--lr".to_owned(),
                    "0.001".to_owned(),
                ],
            ),
            CheckpointArgvDeltaForm::Equals => (
                vec![
                    "python".to_owned(),
                    "train.py".to_owned(),
                    "--lr".to_owned(),
                    "0.001".to_owned(),
                    format!("--resume={request_path}"),
                ],
                vec![
                    "python".to_owned(),
                    "train.py".to_owned(),
                    "--lr".to_owned(),
                    "0.001".to_owned(),
                    "--resume=/private/state/research-checkpoints/campaign/review/checkpoint"
                        .to_owned(),
                ],
            ),
        };
        let request = CheckpointRequest {
            path: request_path,
            argv: request_argv.clone(),
            working_directory: working_directory.to_owned(),
            support_evidence_refs: vec![
                loader.reference.clone(),
                source_checkpoint.reference.clone(),
            ],
        };
        let source_argv = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "--lr".to_owned(),
            "0.001".to_owned(),
        ];
        let delta = validate_checkpoint_argv_delta(
            &source_argv,
            &request_argv,
            &request.path,
        )
        .unwrap();
        let learning_spec_digest =
            checkpoint_learning_spec_digest(&source_argv, working_directory).unwrap();
        let retained_root = ResearchDirectoryRecord {
            device: 11,
            inode: 12,
            owner: 3,
            mode: 0o700,
            mount_identity: [14, 15],
        };
        let retained_parent = ResearchDirectoryRecord {
            device: 11,
            inode: 13,
            owner: 3,
            mode: 0o700,
            mount_identity: retained_root.mount_identity,
        };
        PreparedCheckpoint {
            schema_version: PREPARED_CHECKPOINT_VERSION,
            project_id: "project".to_owned(),
            campaign_id: "campaign".to_owned(),
            review_id: "review".to_owned(),
            review_attempt: 1,
            review_session_generation: 0,
            review_agent_run_id: 7,
            review_event_id: 8,
            source_experiment_id: "experiment".to_owned(),
            source_proposal_id: "proposal".to_owned(),
            source_submission_id: "submission".to_owned(),
            source_task_id: 0,
            source_managed_task_signature: "managed-task".to_owned(),
            source_raw_task_signature: "raw-task".to_owned(),
            context_digest: "a".repeat(64),
            response_digest: "b".repeat(64),
            campaign_objective_digest: "c".repeat(64),
            source_proposal_canonical_digest: "d".repeat(64),
            learning_spec_digest,
            source_runtime: CheckpointSourceRuntimeV1::OriginalProjectRoot,
            source_root_canonical_path: "/private/project".to_owned(),
            source_root_resolution_fingerprint: "root-fingerprint".to_owned(),
            source_root_record: root,
            source_working_directory_record: cwd,
            support_version: CHECKPOINT_SUPPORT_VERSION,
            loader,
            source_checkpoint,
            retained_checkpoint: ResearchFileRecord {
                relative_path: "research-checkpoints/campaign/review/checkpoint".to_owned(),
                root: retained_root,
                parent: retained_parent,
                device: 16,
                inode: 17,
                owner: 3,
                mode: 0o600,
                mount_identity: [14, 15],
                logical_bytes: candidate.len() as u64,
                allocated_bytes: 512,
                sha256: candidate_sha256,
            },
            source_argv,
            source_working_directory: working_directory.to_owned(),
            request,
            delta,
            retained_argv,
            successor_ids: checkpoint_successor_ids("review", 1).unwrap(),
        }
    }

    #[test]
    fn prepared_checkpoint_roundtrips_root_nested_and_both_delta_forms() {
        for (working_directory, form) in [
            (".", CheckpointArgvDeltaForm::Pair),
            (".pueue-agent", CheckpointArgvDeltaForm::Equals),
        ] {
            let checkpoint = prepared_fixture(working_directory, form, 0);
            let encoded = serialize_prepared_checkpoint(&checkpoint).unwrap();
            assert!(encoded.len() <= MAX_PREPARED_CHECKPOINT_BYTES);
            assert_eq!(parse_prepared_checkpoint(&encoded).unwrap(), checkpoint);
        }
    }

    #[test]
    fn prepared_checkpoint_preserves_selected_candidate_ordinal_two() {
        let checkpoint = prepared_fixture(".", CheckpointArgvDeltaForm::Pair, 2);
        let encoded = serialize_prepared_checkpoint(&checkpoint).unwrap();
        let parsed = parse_prepared_checkpoint(&encoded).unwrap();
        assert_eq!(parsed.source_checkpoint.reference, checkpoint.source_checkpoint.reference);
        assert!(parsed.source_checkpoint.reference.starts_with("checkpoint:experiment:2:"));
    }

    #[test]
    fn prepared_codec_rejects_cross_mount_records_and_loader_not_bound_to_source_layout() {
        let checkpoint = prepared_fixture(".", CheckpointArgvDeltaForm::Pair, 0);

        let mut source_mount = checkpoint.clone();
        source_mount.source_checkpoint.file.mount_identity = [99, 100];
        assert!(serialize_prepared_checkpoint(&source_mount).is_err());

        let mut cwd_mount = checkpoint.clone();
        cwd_mount.source_working_directory_record.mount_identity = [99, 100];
        assert!(serialize_prepared_checkpoint(&cwd_mount).is_err());

        let mut loader = checkpoint;
        loader.loader.argv_token = "other.py".to_owned();
        loader.loader.root_relative_path = "other.py".to_owned();
        loader.loader.file.relative_path = "other.py".to_owned();
        assert!(serialize_prepared_checkpoint(&loader).is_err());
    }

    #[test]
    fn prepared_codec_rejects_noncanonical_ordinals_and_retained_paths() {
        let checkpoint = prepared_fixture(".", CheckpointArgvDeltaForm::Pair, 2);
        for ordinal in ["02", "+2"] {
            let mut invalid = checkpoint.clone();
            let previous_reference = invalid.source_checkpoint.reference.clone();
            invalid.source_checkpoint.reference = format!(
                "checkpoint:experiment:{ordinal}:{}",
                invalid.source_checkpoint.sha256
            );
            for reference in &mut invalid.request.support_evidence_refs {
                if *reference == previous_reference {
                    *reference = invalid.source_checkpoint.reference.clone();
                }
            }
            assert!(serialize_prepared_checkpoint(&invalid).is_err());
        }
        for path in [
            "/private//state/research-checkpoints/campaign/review/checkpoint",
            "/private/./state/research-checkpoints/campaign/review/checkpoint",
            "/private/state/research-checkpoints/campaign/review/checkpoint/",
        ] {
            let mut invalid = checkpoint.clone();
            invalid.retained_argv[3] = path.to_owned();
            assert!(serialize_prepared_checkpoint(&invalid).is_err());
        }
    }

    #[test]
    fn prepared_checkpoint_allows_zero_task_and_generation_but_rejects_negative_or_nonpositive_ids() {
        let checkpoint = prepared_fixture(".", CheckpointArgvDeltaForm::Pair, 0);
        assert!(serialize_prepared_checkpoint(&checkpoint).is_ok());
        for mutation in [
            |value: &mut PreparedCheckpoint| value.source_task_id = -1,
            |value: &mut PreparedCheckpoint| value.review_session_generation = -1,
            |value: &mut PreparedCheckpoint| value.review_attempt = 0,
            |value: &mut PreparedCheckpoint| value.review_agent_run_id = 0,
            |value: &mut PreparedCheckpoint| value.review_event_id = 0,
        ] {
            let mut invalid = checkpoint.clone();
            mutation(&mut invalid);
            assert!(serialize_prepared_checkpoint(&invalid).is_err());
        }
    }

    #[test]
    fn prepared_successor_ids_are_stable_and_domain_separated() {
        let first = checkpoint_successor_ids("review", 1).unwrap();
        assert_eq!(first, checkpoint_successor_ids("review", 1).unwrap());
        assert_ne!(first.proposal_id, first.experiment_id);
        assert_ne!(first.proposal_id, first.submission_id);
        assert_ne!(first.experiment_id, first.submission_id);
        assert_ne!(first, checkpoint_successor_ids("other-review", 1).unwrap());
        assert_ne!(first, checkpoint_successor_ids("review", 2).unwrap());
        for id in [first.proposal_id, first.experiment_id, first.submission_id] {
            assert!(valid_bounded_identity(&id));
        }
    }

    #[test]
    fn learning_digest_ignores_retained_path_but_changes_learning_spec() {
        let first = prepared_fixture(".", CheckpointArgvDeltaForm::Pair, 0);
        let mut second = first.clone();
        second.retained_argv[3] = "/private/other/checkpoint".to_owned();
        assert_eq!(
            checkpoint_learning_spec_digest(&first.source_argv, &first.source_working_directory)
                .unwrap(),
            checkpoint_learning_spec_digest(&second.source_argv, &second.source_working_directory)
                .unwrap()
        );
        second.source_argv[3] = "0.002".to_owned();
        assert_ne!(
            first.learning_spec_digest,
            checkpoint_learning_spec_digest(&second.source_argv, &second.source_working_directory)
                .unwrap()
        );
        assert_ne!(
            first.learning_spec_digest,
            checkpoint_learning_spec_digest(&first.source_argv, "nested").unwrap()
        );
    }

    #[test]
    fn prepared_codec_rejects_duplicate_unknown_version_and_raw_size() {
        for working_directory in [".", ".pueue-agent"] {
            let checkpoint = prepared_fixture(working_directory, CheckpointArgvDeltaForm::Pair, 0);
            let encoded = serialize_prepared_checkpoint(&checkpoint).unwrap();
            let duplicate = encoded.replacen(
                "\"schema_version\":1,",
                "\"schema_version\":1,\"schema_version\":1,",
                1,
            );
            assert!(parse_prepared_checkpoint(&duplicate).is_err());

            let mut unknown: Value = serde_json::from_str(&encoded).unwrap();
            unknown["unexpected"] = Value::Bool(true);
            assert!(parse_prepared_checkpoint(&serde_json::to_string(&unknown).unwrap()).is_err());
            let mut nested_unknown: Value = serde_json::from_str(&encoded).unwrap();
            nested_unknown["loader"]["unexpected"] = Value::Bool(true);
            assert!(
                parse_prepared_checkpoint(&serde_json::to_string(&nested_unknown).unwrap())
                    .is_err()
            );
            let nested_duplicate = encoded.replacen(
                "\"source_root_record\":{\"device\":1,",
                "\"source_root_record\":{\"device\":1,\"device\":1,",
                1,
            );
            assert!(parse_prepared_checkpoint(&nested_duplicate).is_err());

            let mut version: Value = serde_json::from_str(&encoded).unwrap();
            version["schema_version"] = Value::from(2);
            assert!(parse_prepared_checkpoint(&serde_json::to_string(&version).unwrap()).is_err());

            let mut oversized = checkpoint.clone();
            oversized.retained_argv[0] = "x".repeat(MAX_PREPARED_CHECKPOINT_BYTES);
            let oversized = serde_json::to_string(&oversized).unwrap();
            assert!(oversized.len() > MAX_PREPARED_CHECKPOINT_BYTES);
            assert!(parse_prepared_checkpoint(&oversized).is_err());
        }
    }

    #[test]
    fn prepared_codec_rejects_mutated_authority_fields_and_retained_path_syntax() {
        let checkpoint = prepared_fixture(".", CheckpointArgvDeltaForm::Pair, 0);
        let mut cases = Vec::new();
        let mut loader_digest = checkpoint.clone();
        loader_digest.loader.sha256 = "e".repeat(64);
        cases.push(loader_digest);
        let mut loader_record = checkpoint.clone();
        loader_record.loader.file.logical_bytes += 1;
        cases.push(loader_record);
        let mut candidate_record = checkpoint.clone();
        candidate_record.source_checkpoint.file.root.inode += 1;
        cases.push(candidate_record);
        let mut retained_digest = checkpoint.clone();
        retained_digest.retained_checkpoint.sha256 = "f".repeat(64);
        cases.push(retained_digest);
        let mut source_id = checkpoint.clone();
        source_id.source_experiment_id = "foreign".to_owned();
        cases.push(source_id);
        let mut delta = checkpoint.clone();
        delta.delta.index += 1;
        cases.push(delta);
        let mut argv = checkpoint.clone();
        argv.request.argv[0] = "python3".to_owned();
        cases.push(argv);
        let mut path = checkpoint.clone();
        path.request.path = "foreign/checkpoint.json".to_owned();
        cases.push(path);
        let mut refs = checkpoint.clone();
        refs.request.support_evidence_refs[1] = refs.request.support_evidence_refs[0].clone();
        cases.push(refs);
        let mut retained_path = checkpoint.clone();
        retained_path.retained_argv[3] = "../checkpoint".to_owned();
        cases.push(retained_path);
        for invalid in cases {
            assert!(serialize_prepared_checkpoint(&invalid).is_err());
        }
    }
}
