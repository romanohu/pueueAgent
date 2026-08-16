use std::path::{Component, Path};

use serde::Serialize;
use sha2::Digest;

use crate::{
    models::ProposalKind,
    process::{MAX_ARGV, MAX_FIELD_SIZE},
    AppError,
};

const MAX_HYPOTHESIS_BYTES: usize = 4 * 1024;
const MAX_EXPECTED_EVIDENCE_ITEMS: usize = 16;
const MAX_EXPECTED_EVIDENCE_BYTES: usize = 512;
const MAX_SOURCE_EXPERIMENT_ID_BYTES: usize = 128;
const PUEUE_ADD_FIXED_ARGV_ITEMS: usize = 9;

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposalInput {
    pub kind: ProposalKind,
    pub hypothesis: String,
    pub source_experiment_id: Option<String>,
    pub argv: Vec<String>,
    pub working_directory: String,
    pub expected_evidence: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ValidatedProposal {
    kind: ProposalKind,
    hypothesis: String,
    source_experiment_id: Option<String>,
    argv: Vec<String>,
    working_directory: String,
    expected_evidence: Vec<String>,
    canonical_digest: String,
}

impl ValidatedProposal {
    pub const fn kind(&self) -> ProposalKind { self.kind }

    pub fn hypothesis(&self) -> &str { &self.hypothesis }

    pub fn source_experiment_id(&self) -> Option<&str> { self.source_experiment_id.as_deref() }

    pub fn argv(&self) -> &[String] { &self.argv }

    pub fn working_directory(&self) -> &str { &self.working_directory }

    pub fn expected_evidence(&self) -> &[String] { &self.expected_evidence }

    pub fn canonical_digest(&self) -> &str { &self.canonical_digest }
}

pub fn validate(
    input: ProposalInput,
    objective_digest: &str,
) -> Result<ValidatedProposal, AppError> {
    validate_proposal_shape(&input)?;
    let working_directory = normalize_working_directory(&input.working_directory)?;
    let canonical = serde_json::to_vec(&CanonicalProposal {
        schema_version: 1,
        objective_digest,
        kind: input.kind.as_str(),
        hypothesis: &input.hypothesis,
        source_experiment_id: input.source_experiment_id.as_deref(),
        argv: &input.argv,
        working_directory: &working_directory,
        expected_evidence: &input.expected_evidence,
    })
    .map_err(|source| AppError::Serialization {
        operation: "serialize canonical campaign proposal",
        source,
    })?;

    Ok(ValidatedProposal {
        kind: input.kind,
        hypothesis: input.hypothesis,
        source_experiment_id: input.source_experiment_id,
        argv: input.argv,
        working_directory,
        expected_evidence: input.expected_evidence,
        canonical_digest: format!("{:x}", sha2::Sha256::digest(canonical)),
    })
}

pub fn validate_initial_baseline(
    input: ProposalInput,
    objective_digest: &str,
) -> Result<ValidatedProposal, AppError> {
    if input.kind != ProposalKind::Experiment {
        return Err(validation_error("baseline.kind", "must be an experiment proposal"));
    }
    if input.source_experiment_id.is_some() {
        return Err(validation_error(
            "baseline.source_experiment_id",
            "must be absent",
        ));
    }
    validate(input, objective_digest)
}

#[derive(Serialize)]
struct CanonicalProposal<'a> {
    schema_version: u8,
    objective_digest: &'a str,
    kind: &'a str,
    hypothesis: &'a str,
    source_experiment_id: Option<&'a str>,
    argv: &'a [String],
    working_directory: &'a str,
    expected_evidence: &'a [String],
}

fn validate_proposal_shape(input: &ProposalInput) -> Result<(), AppError> {
    validate_text(
        "hypothesis",
        &input.hypothesis,
        MAX_HYPOTHESIS_BYTES,
        "must be at most 4096 bytes without control characters",
    )?;

    if let Some(source_experiment_id) = &input.source_experiment_id {
        validate_identifier("source_experiment_id", source_experiment_id)?;
    }
    if matches!(
        input.kind,
        ProposalKind::Repair | ProposalKind::CodeChange | ProposalKind::DataEvaluation
    ) && input.source_experiment_id.is_none()
    {
        return Err(validation_error(
            "source_experiment_id",
            "is required for this proposal kind",
        ));
    }

    if input.argv.is_empty() {
        return Err(validation_error("argv", "must not be empty"));
    }
    if input.argv.len() > MAX_ARGV - PUEUE_ADD_FIXED_ARGV_ITEMS {
        return Err(validation_error(
            "argv",
            "contains too many items for the native control frame",
        ));
    }
    for argument in &input.argv {
        validate_text(
            "argv",
            argument,
            MAX_FIELD_SIZE,
            "items must fit the native field limit without control characters",
        )?;
    }

    if input.expected_evidence.len() > MAX_EXPECTED_EVIDENCE_ITEMS {
        return Err(validation_error(
            "expected_evidence",
            "contains too many items",
        ));
    }
    for evidence in &input.expected_evidence {
        validate_text(
            "expected_evidence",
            evidence,
            MAX_EXPECTED_EVIDENCE_BYTES,
            "items must be at most 512 bytes without control characters",
        )?;
    }

    Ok(())
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), AppError> {
    if value.is_empty()
        || value.len() > MAX_SOURCE_EXPERIMENT_ID_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(validation_error(
            field,
            "must be 1 to 128 bytes without control characters",
        ));
    }
    Ok(())
}

fn validate_text(
    field: &'static str,
    value: &str,
    maximum_bytes: usize,
    message: &'static str,
) -> Result<(), AppError> {
    if value.len() > maximum_bytes || value.chars().any(char::is_control) {
        return Err(validation_error(field, message));
    }
    Ok(())
}

fn normalize_working_directory(value: &str) -> Result<String, AppError> {
    if value.chars().any(char::is_control) || Path::new(value).is_absolute() {
        return Err(validation_error(
            "working_directory",
            "must be . or a relative path of normal components",
        ));
    }
    if value == "." {
        return Ok(".".to_owned());
    }

    let mut components = Vec::new();
    for component in Path::new(value).components() {
        match component {
            Component::Normal(component) => components.push(component.to_string_lossy().into_owned()),
            Component::CurDir | Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(validation_error(
                    "working_directory",
                    "must be . or a relative path of normal components",
                ));
            }
        }
    }
    if components.is_empty() {
        return Err(validation_error(
            "working_directory",
            "must be . or a relative path of normal components",
        ));
    }

    let normalized = components.join("/");
    if value != normalized {
        return Err(validation_error(
            "working_directory",
            "must be . or a relative path of normal components",
        ));
    }

    Ok(normalized)
}

fn validation_error(field: &'static str, message: &'static str) -> AppError {
    AppError::Validation { field, message }
}

#[cfg(test)]
mod tests {
    use crate::{
        models::ProposalKind,
        proposals::{self, ProposalInput},
    };

    fn input(kind: ProposalKind) -> ProposalInput {
        ProposalInput {
            kind,
            hypothesis: "Reduce learning rate after the baseline".to_owned(),
            source_experiment_id: Some("exp-baseline".to_owned()),
            argv: vec![
                "python".to_owned(),
                "train.py".to_owned(),
                "--lr".to_owned(),
                "0.001".to_owned(),
            ],
            working_directory: ".".to_owned(),
            expected_evidence: vec!["validation loss".to_owned()],
        }
    }

    #[test]
    fn validates_initial_baseline_proposal() {
        let mut baseline = input(ProposalKind::Experiment);
        baseline.source_experiment_id = None;

        let proposal = proposals::validate_initial_baseline(baseline, "objective-digest").unwrap();

        assert_eq!(proposal.kind(), ProposalKind::Experiment);
        assert_eq!(proposal.source_experiment_id(), None);
        assert_eq!(proposal.working_directory(), ".");
    }

    #[test]
    fn validates_repair_proposal_with_source_experiment() {
        let proposal = proposals::validate(input(ProposalKind::Repair), "objective-digest").unwrap();

        assert_eq!(proposal.kind(), ProposalKind::Repair);
        assert_eq!(proposal.source_experiment_id(), Some("exp-baseline"));
    }

    #[test]
    fn validates_code_change_proposal_with_source_experiment() {
        let proposal = proposals::validate(input(ProposalKind::CodeChange), "objective-digest").unwrap();

        assert_eq!(proposal.kind(), ProposalKind::CodeChange);
        assert_eq!(proposal.argv(), ["python", "train.py", "--lr", "0.001"]);
    }

    #[test]
    fn rejects_unknown_input_fields() {
        let json = r#"{
            "kind":"experiment",
            "hypothesis":"test",
            "source_experiment_id":null,
            "argv":["python","train.py"],
            "working_directory":".",
            "expected_evidence":[],
            "unexpected":"value"
        }"#;

        assert!(serde_json::from_str::<ProposalInput>(json).is_err());
    }

    #[test]
    fn rejects_empty_argv() {
        let mut proposal = input(ProposalKind::Experiment);
        proposal.argv.clear();

        assert!(proposals::validate(proposal, "objective-digest").is_err());
    }

    #[test]
    fn rejects_absolute_or_parent_traversing_working_directory() {
        for working_directory in ["/tmp", "../other", "nested/../../other", "nested/./other"] {
            let mut proposal = input(ProposalKind::Experiment);
            proposal.working_directory = working_directory.to_owned();

            assert!(proposals::validate(proposal, "objective-digest").is_err());
        }
    }

    #[test]
    fn rejects_control_characters() {
        let mut proposal = input(ProposalKind::Experiment);
        proposal.hypothesis.push('\n');

        assert!(proposals::validate(proposal, "objective-digest").is_err());
    }

    #[test]
    fn rejects_too_many_or_too_large_evidence_items() {
        let mut too_many = input(ProposalKind::Experiment);
        too_many.expected_evidence = (0..17).map(|index| format!("evidence-{index}")).collect();
        assert!(proposals::validate(too_many, "objective-digest").is_err());

        let mut too_large = input(ProposalKind::Experiment);
        too_large.expected_evidence = vec!["x".repeat(513)];
        assert!(proposals::validate(too_large, "objective-digest").is_err());
    }

    #[test]
    fn rejects_hypothesis_over_four_kib() {
        let mut proposal = input(ProposalKind::Experiment);
        proposal.hypothesis = "x".repeat(4_097);

        assert!(proposals::validate(proposal, "objective-digest").is_err());
    }

    #[test]
    fn rejects_argv_item_over_native_field_limit() {
        let mut proposal = input(ProposalKind::Experiment);
        proposal.argv = vec!["x".repeat(crate::process::MAX_FIELD_SIZE + 1)];

        assert!(proposals::validate(proposal, "objective-digest").is_err());
    }

    #[test]
    fn repair_code_change_and_data_evaluation_require_source_experiment() {
        for kind in [
            ProposalKind::Repair,
            ProposalKind::CodeChange,
            ProposalKind::DataEvaluation,
        ] {
            let mut proposal = input(kind);
            proposal.source_experiment_id = None;

            assert!(proposals::validate(proposal, "objective-digest").is_err());
        }
    }

    #[test]
    fn initial_baseline_rejects_source_experiment() {
        assert!(proposals::validate_initial_baseline(
            input(ProposalKind::Experiment),
            "objective-digest"
        )
        .is_err());
    }

    #[test]
    fn semantically_identical_proposals_have_one_digest() {
        let input = input(ProposalKind::Experiment);
        assert_eq!(
            proposals::validate(input.clone(), "objective-digest")
                .unwrap()
                .canonical_digest(),
            proposals::validate(input, "objective-digest")
                .unwrap()
                .canonical_digest(),
        );
    }

    #[test]
    fn objective_digest_changes_canonical_identity() {
        let input = input(ProposalKind::Experiment);
        let first = proposals::validate(input.clone(), "objective-digest-one").unwrap();
        let second = proposals::validate(input, "objective-digest-two").unwrap();

        assert_ne!(first.canonical_digest(), second.canonical_digest());
    }
}
