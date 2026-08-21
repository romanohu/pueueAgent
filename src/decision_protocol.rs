use serde::Serialize;
use sha2::Digest;

use crate::{
    AppError,
    execution_policy::CampaignLimits,
    models::ProposalKind,
    proposals::{self, ProposalInput, ValidatedProposal},
};

pub const MAX_DECISION_BYTES: usize = 128 * 1024;
pub const MAX_WAIT_REASON_BYTES: usize = 4 * 1024;
pub const MAX_WAIT_EVIDENCE_ITEMS: usize = 16;
pub const MAX_WAIT_EVIDENCE_BYTES: usize = 512;

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionInput {
    Proposal {
        schema_version: u8,
        proposal: ProposalInput,
    },
    Wait {
        schema_version: u8,
        reason: String,
        requested_wait_minutes: u32,
        expected_evidence: Vec<String>,
    },
}

#[derive(Debug)]
pub enum ValidatedDecision {
    Proposal(ValidatedProposal),
    Wait(ValidatedWait),
}

impl ValidatedDecision {
    pub fn canonical_digest(&self) -> &str {
        match self {
            Self::Proposal(proposal) => proposal.canonical_digest(),
            Self::Wait(wait) => wait.canonical_digest(),
        }
    }
}

#[derive(Debug)]
pub struct ValidatedWait {
    objective_digest: String,
    reason: String,
    pub requested_wait_minutes: u32,
    expected_evidence: Vec<String>,
    canonical_digest: String,
}

impl ValidatedWait {
    pub fn objective_digest(&self) -> &str {
        &self.objective_digest
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn expected_evidence(&self) -> &[String] {
        &self.expected_evidence
    }

    pub fn canonical_digest(&self) -> &str {
        &self.canonical_digest
    }
}

pub fn parse_and_validate_decision(
    bytes: &[u8],
    objective_digest: &str,
    limits: CampaignLimits,
) -> Result<ValidatedDecision, AppError> {
    if bytes.is_empty() || bytes.len() > MAX_DECISION_BYTES {
        return Err(validation_error("decision", "must be 1 to 131072 bytes"));
    }

    let input = serde_json::from_slice(bytes).map_err(|source| AppError::Serialization {
        operation: "parse campaign decision",
        source,
    })?;

    match input {
        DecisionInput::Proposal {
            schema_version,
            proposal,
        } => {
            validate_schema_version(schema_version)?;
            let proposal = proposals::validate(proposal, objective_digest)?;
            if proposal.kind() == ProposalKind::CodeChange {
                return Err(validation_error(
                    "proposal.kind",
                    "code_change decisions are not permitted",
                ));
            }
            Ok(ValidatedDecision::Proposal(proposal))
        }
        DecisionInput::Wait {
            schema_version,
            reason,
            requested_wait_minutes,
            expected_evidence,
        } => {
            validate_schema_version(schema_version)?;
            validate_wait_text("reason", &reason, MAX_WAIT_REASON_BYTES)?;
            if expected_evidence.len() > MAX_WAIT_EVIDENCE_ITEMS {
                return Err(validation_error(
                    "expected_evidence",
                    "contains too many items",
                ));
            }
            for evidence in &expected_evidence {
                validate_wait_text("expected_evidence", evidence, MAX_WAIT_EVIDENCE_BYTES)?;
            }
            if !(1..=limits.max_decision_wait_minutes).contains(&requested_wait_minutes) {
                return Err(validation_error(
                    "requested_wait_minutes",
                    "must be within the service-owned decision wait limit",
                ));
            }

            let canonical = serde_json::to_vec(&CanonicalWait {
                schema_version,
                objective_digest,
                decision: "wait",
                reason: &reason,
                requested_wait_minutes,
                expected_evidence: &expected_evidence,
            })
            .map_err(|source| AppError::Serialization {
                operation: "serialize canonical campaign wait decision",
                source,
            })?;

            Ok(ValidatedDecision::Wait(ValidatedWait {
                objective_digest: objective_digest.to_owned(),
                reason,
                requested_wait_minutes,
                expected_evidence,
                canonical_digest: format!("{:x}", sha2::Sha256::digest(canonical)),
            }))
        }
    }
}

#[derive(Serialize)]
struct CanonicalWait<'a> {
    schema_version: u8,
    objective_digest: &'a str,
    decision: &'static str,
    reason: &'a str,
    requested_wait_minutes: u32,
    expected_evidence: &'a [String],
}

fn validate_schema_version(schema_version: u8) -> Result<(), AppError> {
    if schema_version != 1 {
        return Err(validation_error("schema_version", "must be 1"));
    }
    Ok(())
}

fn validate_wait_text(
    field: &'static str,
    value: &str,
    maximum_bytes: usize,
) -> Result<(), AppError> {
    if value.len() > maximum_bytes || value.chars().any(char::is_control) {
        return Err(validation_error(
            field,
            "must fit the decision text limit without control characters",
        ));
    }
    Ok(())
}

fn validation_error(field: &'static str, message: &'static str) -> AppError {
    AppError::Validation { field, message }
}

#[cfg(test)]
mod tests {
    use crate::{
        decision_protocol::{ValidatedDecision, ValidatedWait, parse_and_validate_decision},
        execution_policy::CampaignLimits,
    };

    #[test]
    fn decision_protocol_accepts_one_proposal_or_finite_wait() {
        let limits = CampaignLimits::default();
        let proposal = br#"{"schema_version":1,"decision":"proposal","proposal":{"kind":"experiment","hypothesis":"lower lr","source_experiment_id":"exp-1","argv":["python","train.py","--lr","0.001"],"working_directory":".","expected_evidence":["validation loss"]}}"#;
        assert!(matches!(
            parse_and_validate_decision(proposal, "objective-digest", limits).unwrap(),
            ValidatedDecision::Proposal(_)
        ));

        let wait = br#"{"schema_version":1,"decision":"wait","reason":"artifact pending","requested_wait_minutes":30,"expected_evidence":["checkpoint"]}"#;
        assert!(matches!(
            parse_and_validate_decision(wait, "objective-digest", limits).unwrap(),
            ValidatedDecision::Wait(ValidatedWait {
                requested_wait_minutes: 30,
                ..
            })
        ));
    }

    #[test]
    fn decision_protocol_rejects_code_change_unknown_fields_and_unbounded_wait() {
        let limits = CampaignLimits::default();
        let rejected: [&[u8]; 4] = [
            br#"{"schema_version":1,"decision":"proposal","proposal":{"kind":"code_change","hypothesis":"edit source","source_experiment_id":"exp-1","argv":["python","train.py"],"working_directory":".","expected_evidence":[]}}"#,
            br#"{"schema_version":1,"decision":"wait","reason":"later","requested_wait_minutes":30,"expected_evidence":[],"extra":true}"#,
            br#"{"schema_version":1,"decision":"wait","reason":"later","requested_wait_minutes":0,"expected_evidence":[]}"#,
            br#"{"schema_version":1,"decision":"wait","reason":"later","requested_wait_minutes":10081,"expected_evidence":[]}"#,
        ];
        for document in rejected {
            assert!(parse_and_validate_decision(document, "objective-digest", limits).is_err());
        }
    }
}
