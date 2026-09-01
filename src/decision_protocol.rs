use serde::Serialize;
use sha2::Digest;

use crate::{
    execution_policy::CampaignLimits,
    proposals::{self, ProposalInput, ValidatedProposal},
    AppError,
};

pub const MAX_DECISION_BYTES: usize = 128 * 1024;
pub const MAX_WAIT_REASON_BYTES: usize = 4 * 1024;
pub const MAX_WAIT_EVIDENCE_ITEMS: usize = 16;
pub const MAX_WAIT_EVIDENCE_BYTES: usize = 512;
pub const MAX_EVIDENCE_REF_BYTES: usize = 512;
pub const MAX_REVIEW_NOTE_BYTES: usize = 1024;

#[derive(Debug)]
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

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DecisionEnvelope {
    schema_version: u8,
    decision: DecisionKind,
    proposal: Option<ProposalInput>,
    reason: Option<String>,
    requested_wait_minutes: Option<u32>,
    expected_evidence: Option<Vec<String>>,
    evidence_ref: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum DecisionKind {
    Proposal,
    Wait,
    GoalReached,
}

#[derive(Debug)]
pub enum ValidatedDecision {
    Proposal(ValidatedProposal),
    Wait(ValidatedWait),
    GoalReached(ValidatedGoalReached),
}

impl ValidatedDecision {
    pub fn canonical_digest(&self) -> &str {
        match self {
            Self::Proposal(proposal) => proposal.canonical_digest(),
            Self::Wait(wait) => wait.canonical_digest(),
            Self::GoalReached(goal) => goal.canonical_digest(),
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

#[derive(Debug)]
pub struct ValidatedGoalReached {
    objective_digest: String,
    evidence_ref: String,
    canonical_digest: String,
}

impl ValidatedGoalReached {
    pub fn objective_digest(&self) -> &str {
        &self.objective_digest
    }

    pub fn evidence_ref(&self) -> &str {
        &self.evidence_ref
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

    let envelope: DecisionEnvelope =
        serde_json::from_slice(bytes).map_err(|source| AppError::Serialization {
            operation: "parse campaign decision",
            source,
        })?;
    let input = match envelope.decision {
        DecisionKind::Proposal => {
            if envelope.reason.is_some()
                || envelope.requested_wait_minutes.is_some()
                || envelope.expected_evidence.is_some()
                || envelope.evidence_ref.is_some()
            {
                return Err(validation_error(
                    "decision",
                    "wait fields must be null or absent for a proposal decision",
                ));
            }
            DecisionInput::Proposal {
                schema_version: envelope.schema_version,
                proposal: envelope.proposal.ok_or_else(|| {
                    validation_error("proposal", "must be present for a proposal decision")
                })?,
            }
        }
        DecisionKind::Wait => {
            if envelope.proposal.is_some() || envelope.evidence_ref.is_some() {
                return Err(validation_error(
                    "proposal",
                    "must be null or absent for a wait decision",
                ));
            }
            DecisionInput::Wait {
                schema_version: envelope.schema_version,
                reason: envelope.reason.ok_or_else(|| {
                    validation_error("reason", "must be present for a wait decision")
                })?,
                requested_wait_minutes: envelope.requested_wait_minutes.ok_or_else(|| {
                    validation_error(
                        "requested_wait_minutes",
                        "must be present for a wait decision",
                    )
                })?,
                expected_evidence: envelope.expected_evidence.ok_or_else(|| {
                    validation_error("expected_evidence", "must be present for a wait decision")
                })?,
            }
        }
        DecisionKind::GoalReached => {
            if envelope.proposal.is_some()
                || envelope.reason.is_some()
                || envelope.requested_wait_minutes.is_some()
                || envelope.expected_evidence.is_some()
            {
                return Err(validation_error(
                    "decision",
                    "proposal and wait fields must be null or absent for a goal_reached decision",
                ));
            }
            let evidence_ref = envelope.evidence_ref.ok_or_else(|| {
                validation_error(
                    "evidence_ref",
                    "must be present for a goal_reached decision",
                )
            })?;
            return {
                validate_schema_version(envelope.schema_version)?;
                validate_wait_text("evidence_ref", &evidence_ref, MAX_EVIDENCE_REF_BYTES)?;
                if evidence_ref.trim().is_empty() {
                    return Err(validation_error(
                        "evidence_ref",
                        "must be non-empty without control characters",
                    ));
                }
                let canonical = serde_json::to_vec(&CanonicalGoalReached {
                    schema_version: envelope.schema_version,
                    objective_digest,
                    decision: "goal_reached",
                    evidence_ref: &evidence_ref,
                })
                .map_err(|source| AppError::Serialization {
                    operation: "serialize canonical campaign goal_reached decision",
                    source,
                })?;
                Ok(ValidatedDecision::GoalReached(ValidatedGoalReached {
                    objective_digest: objective_digest.to_owned(),
                    evidence_ref,
                    canonical_digest: format!("{:x}", sha2::Sha256::digest(canonical)),
                }))
            };
        }
    };

    match input {
        DecisionInput::Proposal {
            schema_version,
            proposal,
        } => {
            validate_schema_version(schema_version)?;
            let proposal = proposals::validate(proposal, objective_digest)?;
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

#[derive(Serialize)]
struct CanonicalGoalReached<'a> {
    schema_version: u8,
    objective_digest: &'a str,
    decision: &'static str,
    evidence_ref: &'a str,
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
        models::ProposalKind,
    };

    #[test]
    fn decision_protocol_accepts_bounded_code_change() {
        let bytes = br#"{"schema_version":1,"decision":"proposal","proposal":{"kind":"code_change","hypothesis":"reduce allocator pressure","source_experiment_id":"exp-1","argv":["python","train.py"],"working_directory":".","expected_evidence":["lower peak memory"]}}"#;
        let decision =
            parse_and_validate_decision(bytes, "objective", CampaignLimits::default()).unwrap();
        let ValidatedDecision::Proposal(proposal) = decision else {
            panic!("expected proposal");
        };
        assert_eq!(proposal.kind(), ProposalKind::CodeChange);
    }

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

        let strict_wait = br#"{"schema_version":1,"decision":"wait","proposal":null,"reason":"artifact pending","requested_wait_minutes":30,"expected_evidence":["checkpoint"]}"#;
        assert!(matches!(
            parse_and_validate_decision(strict_wait, "objective-digest", limits).unwrap(),
            ValidatedDecision::Wait(_)
        ));
    }

    #[test]
    fn decision_protocol_rejects_code_change_unknown_fields_and_unbounded_wait() {
        let limits = CampaignLimits::default();
        let rejected: [&[u8]; 5] = [
            br#"{"schema_version":1,"decision":"wait","reason":"later","requested_wait_minutes":30,"expected_evidence":[],"extra":true}"#,
            br#"{"schema_version":1,"decision":"wait","reason":"later","requested_wait_minutes":0,"expected_evidence":[]}"#,
            br#"{"schema_version":1,"decision":"wait","reason":"later","requested_wait_minutes":10081,"expected_evidence":[]}"#,
            br#"{"schema_version":1,"decision":"proposal","proposal":{"kind":"experiment","hypothesis":"lower lr","source_experiment_id":null,"argv":["python","train.py"],"working_directory":".","expected_evidence":[]},"reason":"inactive","requested_wait_minutes":null,"expected_evidence":null}"#,
            br#"{"schema_version":1,"decision":"wait","proposal":{"kind":"experiment","hypothesis":"lower lr","source_experiment_id":null,"argv":["python","train.py"],"working_directory":".","expected_evidence":[]},"reason":"later","requested_wait_minutes":30,"expected_evidence":[]}"#,
        ];
        for document in rejected {
            assert!(parse_and_validate_decision(document, "objective-digest", limits).is_err());
        }
    }
}
