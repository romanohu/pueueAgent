use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    db::{
        CampaignRepository, Db, DecisionReservation, ExperimentRepository, InterventionRepository,
        ProjectRepository, ProposalRepository,
    },
    environment::{collect_decision_artifact_hints, DecisionArtifactHint},
    execution_policy::ProjectRootAnchor,
    interventions::Intervention,
    models::{CampaignState, Experiment, ExperimentStatus, Proposal, ProposalStatus},
    output::bounded_redacted_text,
    AppError,
};

pub const DECISION_CONTEXT_SCHEMA_VERSION: u8 = 1;
pub const DECISION_CONTEXT_SCHEMA_VERSION_V2: u8 = 2;
pub const MAX_DECISION_CONTEXT_BYTES: usize = 128 * 1024;
pub const MAX_ARTIFACT_HINTS: usize = 64;
pub const MAX_ARTIFACT_HINT_DEPTH: usize = 4;
pub const MAX_ARTIFACT_HINT_FIELD_BYTES: usize = 4 * 1024;

const MAX_RECENT_OUTCOMES: usize = 32;
const MAX_RECENT_RESEARCH_ADVICE: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionContextBundle {
    pub json: String,
    pub digest: String,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredDecisionContext {
    schema_version: u8,
    objective: StoredObjective,
    source_experiment: StoredSourceExperiment,
    terminal_observation: StoredTerminalObservation,
    recent_outcomes: StoredRecentOutcomes,
    budgets: StoredBudgets,
    intervention: StoredIntervention,
    artifact_hints: Vec<StoredArtifactHint>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredDecisionContextV2 {
    schema_version: u8,
    objective: StoredObjective,
    source_experiment: StoredSourceExperiment,
    terminal_observation: StoredTerminalObservation,
    recent_outcomes: StoredRecentOutcomes,
    budgets: StoredBudgets,
    intervention: StoredIntervention,
    artifact_hints: Vec<StoredArtifactHint>,
    research: StoredResearchSupplement,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredResearchSupplement {
    review_id: String,
    reason: String,
    next_direction: Option<String>,
    recent_advice: Vec<StoredResearchAdvice>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredResearchAdvice {
    evidence_ref: String,
    review_id: String,
    attempt: i64,
    state: String,
    notes: String,
}

#[derive(Deserialize)]
struct StoredSchemaVersion {
    schema_version: u8,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredObjective { text: String, digest: String }

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSourceExperiment {
    experiment_id: String,
    proposal_id: String,
    proposal_kind: crate::models::ProposalKind,
    status: crate::models::ExperimentStatus,
    attempt: i64,
    command_digest: String,
    failure_code: Option<String>,
    failure_fingerprint: Option<String>,
    created_at: i64,
    updated_at: i64,
    finished_at: Option<i64>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredTerminalObservation {
    task_id: Option<i64>,
    task_signature: Option<String>,
    state: String,
    enqueued_at: Option<i64>,
    started_at: Option<i64>,
    ended_at: Option<i64>,
    exit_code: Option<i32>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRecentOutcomes {
    proposals: Vec<StoredRecentProposal>,
    experiments: Vec<StoredRecentExperiment>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRecentProposal {
    proposal_id: String,
    kind: crate::models::ProposalKind,
    status: crate::models::ProposalStatus,
    source_experiment_id: Option<String>,
    canonical_digest: String,
    created_at: i64,
    updated_at: i64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRecentExperiment {
    experiment_id: String,
    proposal_id: String,
    status: crate::models::ExperimentStatus,
    attempt: i64,
    failure_code: Option<String>,
    failure_fingerprint: Option<String>,
    created_at: i64,
    updated_at: i64,
    finished_at: Option<i64>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredBudgets {
    campaign_state: CampaignState,
    next_eligible_at: Option<i64>,
    rolling_usage: std::collections::BTreeMap<String, i64>,
    experiment_counts: std::collections::BTreeMap<String, i64>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredIntervention { pending: Vec<StoredPendingIntervention> }

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredPendingIntervention {
    insertion_sequence: i64,
    message: String,
    created_at: i64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredArtifactHint { path: String, size: u64, mtime: i64 }

pub(crate) fn validate_stored_decision_context(
    json: &str,
    expected_objective_digest: &str,
    expected_source_experiment_id: &str,
) -> Result<String, AppError> {
    if json.is_empty() || json.len() > MAX_DECISION_CONTEXT_BYTES {
        return Err(validation_error(
            "decision_context",
            "must fit the serialized context limit",
        ));
    }
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|source| AppError::Serialization {
            operation: "parse stored decision context",
            source,
        })?;
    if contains_control_character(&value) {
        return Err(validation_error(
            "decision_context",
            "must not contain control characters",
        ));
    }
    let schema_version: StoredSchemaVersion =
        serde_json::from_value(value.clone()).map_err(|source| AppError::Serialization {
            operation: "validate stored decision context schema version",
            source,
        })?;
    let objective_digest = match schema_version.schema_version {
        DECISION_CONTEXT_SCHEMA_VERSION => {
            let context: StoredDecisionContext =
                serde_json::from_value(value).map_err(|source| AppError::Serialization {
                    operation: "validate stored decision context schema",
                    source,
                })?;
            if context.objective.digest != expected_objective_digest {
                return Err(validation_error(
                    "decision_context.objective.digest",
                    "does not match the immutable campaign objective digest",
                ));
            }
            if context.source_experiment.experiment_id != expected_source_experiment_id {
                return Err(validation_error(
                    "decision_context.source_experiment.experiment_id",
                    "does not match the reserved source experiment",
                ));
            }
            context.objective.digest
        }
        DECISION_CONTEXT_SCHEMA_VERSION_V2 => {
            let context: StoredDecisionContextV2 =
                serde_json::from_value(value).map_err(|source| AppError::Serialization {
                    operation: "validate stored decision context schema",
                    source,
                })?;
            validate_research_supplement(&context.research)?;
            if context.objective.digest != expected_objective_digest {
                return Err(validation_error(
                    "decision_context.objective.digest",
                    "does not match the immutable campaign objective digest",
                ));
            }
            if context.source_experiment.experiment_id != expected_source_experiment_id {
                return Err(validation_error(
                    "decision_context.source_experiment.experiment_id",
                    "does not match the reserved source experiment",
                ));
            }
            context.objective.digest
        }
        _ => {
            return Err(validation_error(
                "decision_context.schema_version",
                "does not match the supported schema version",
            ));
        }
    };
    Ok(objective_digest)
}

pub(crate) fn decision_context_schema_version(json: &str) -> Result<u8, AppError> {
    if json.is_empty() || json.len() > MAX_DECISION_CONTEXT_BYTES {
        return Err(validation_error(
            "decision_context",
            "must fit the serialized context limit",
        ));
    }
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|source| AppError::Serialization {
            operation: "parse stored decision context",
            source,
        })?;
    if contains_control_character(&value) {
        return Err(validation_error(
            "decision_context",
            "must not contain control characters",
        ));
    }
    let version: StoredSchemaVersion =
        serde_json::from_value(value).map_err(|source| AppError::Serialization {
            operation: "validate stored decision context schema version",
            source,
        })?;
    if !matches!(
        version.schema_version,
        DECISION_CONTEXT_SCHEMA_VERSION | DECISION_CONTEXT_SCHEMA_VERSION_V2
    ) {
        return Err(validation_error(
            "decision_context.schema_version",
            "does not match the supported schema version",
        ));
    }
    Ok(version.schema_version)
}

fn validate_research_supplement(
    research: &StoredResearchSupplement,
) -> Result<(), AppError> {
    validate_context_text(
        "decision_context.research.review_id",
        &research.review_id,
        crate::research_protocol::MAX_RESEARCH_ID_BYTES,
        true,
    )?;
    validate_context_text(
        "decision_context.research.reason",
        &research.reason,
        crate::research_protocol::MAX_RESEARCH_REASON_BYTES,
        false,
    )?;
    if let Some(next_direction) = research.next_direction.as_deref() {
        validate_context_text(
            "decision_context.research.next_direction",
            next_direction,
            crate::research_protocol::MAX_RESEARCH_NEXT_DIRECTION_BYTES,
            false,
        )?;
    }
    if research.recent_advice.len() > MAX_RECENT_RESEARCH_ADVICE {
        return Err(validation_error(
            "decision_context.research.recent_advice",
            "contains too many research notes",
        ));
    }
    for advice in &research.recent_advice {
        validate_context_text(
            "decision_context.research.recent_advice.review_id",
            &advice.review_id,
            crate::research_protocol::MAX_RESEARCH_ID_BYTES,
            true,
        )?;
        validate_context_text(
            "decision_context.research.recent_advice.evidence_ref",
            &advice.evidence_ref,
            crate::research_protocol::MAX_RESEARCH_EVIDENCE_REF_BYTES,
            true,
        )?;
        if advice.evidence_ref != format!("research:{}:note", advice.review_id) {
            return Err(validation_error(
                "decision_context.research.recent_advice.evidence_ref",
                "must match the canonical saved-advice reference for its review",
            ));
        }
        if advice.attempt < 0 {
            return Err(validation_error(
                "decision_context.research.recent_advice.attempt",
                "must not be negative",
            ));
        }
        if !matches!(
            advice.state.as_str(),
            "pending" | "running" | "ready" | "retry_wait" | "blocked" | "completed"
        ) {
            return Err(validation_error(
                "decision_context.research.recent_advice.state",
                "is not a supported research review state",
            ));
        }
        validate_context_text(
            "decision_context.research.recent_advice.notes",
            &advice.notes,
            crate::research_protocol::MAX_RESEARCH_NOTES_BYTES,
            false,
        )?;
    }
    Ok(())
}

fn validate_context_text(
    field: &'static str,
    value: &str,
    maximum_bytes: usize,
    require_nonempty: bool,
) -> Result<(), AppError> {
    if (require_nonempty && value.is_empty())
        || value.len() > maximum_bytes
        || value.chars().any(char::is_control)
    {
        return Err(validation_error(
            field,
            "is empty, oversized, or contains control characters",
        ));
    }
    Ok(())
}

fn contains_control_character(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(value) => value.chars().any(invalid_context_character),
        serde_json::Value::Array(values) => values.iter().any(contains_control_character),
        serde_json::Value::Object(values) => values.iter().any(|(key, value)| {
            key.chars().any(invalid_context_character) || contains_control_character(value)
        }),
        _ => false,
    }
}

fn invalid_context_character(character: char) -> bool {
    character.is_control() && !matches!(character, '\n' | '\r' | '\t')
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionPueueTaskProjection {
    pub task_id: i64,
    pub task_signature: String,
    pub group: String,
    pub state: String,
    pub enqueued_at: Option<i64>,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub exit_code: Option<i32>,
}

pub struct DecisionEvidenceRequest<'a> {
    pub reservation: &'a DecisionReservation,
    pub root_anchor: &'a ProjectRootAnchor,
    pub pueue_tasks: &'a [DecisionPueueTaskProjection],
    pub observed_at: i64,
}

pub struct DecisionEvidenceBuilder<'db> {
    db: &'db Db,
}

impl<'db> DecisionEvidenceBuilder<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn build(
        &self,
        request: &DecisionEvidenceRequest<'_>,
    ) -> Result<DecisionContextBundle, AppError> {
        let campaign = CampaignRepository::new(self.db)
            .find_by_id(&request.reservation.campaign_id)?
            .ok_or_else(|| validation_error("campaign_id", "does not identify a campaign"))?;
        let project = ProjectRepository::new(self.db)
            .find_by_id(&campaign.project_id)?
            .ok_or_else(|| validation_error("project_id", "does not identify a project"))?;
        if request.root_anchor.canonical_path != project.root_path {
            return Err(validation_error(
                "root_anchor",
                "must identify the persisted project root",
            ));
        }

        let (source, command_digest) = ExperimentRepository::new(self.db)
            .inspect_for_campaign(
                &request.reservation.campaign_id,
                &request.reservation.source_experiment_id,
            )?
            .ok_or_else(|| {
                validation_error(
                    "source_experiment_id",
                    "does not identify a campaign experiment",
                )
            })?;
        if !is_terminal(source.status) {
            return Err(validation_error(
                "source_experiment_id",
                "must identify a terminal experiment",
            ));
        }
        let source_proposal = ProposalRepository::new(self.db)
            .find_for_campaign(&campaign.campaign_id, &source.proposal_id)?
            .ok_or_else(|| {
                validation_error("proposal_id", "does not identify the source proposal")
            })?;
        let terminal_observation = terminal_observation(&project.pueue_group, &source, request)?;

        let mut proposals = ProposalRepository::new(self.db)
            .list_for_campaign(&campaign.campaign_id, MAX_RECENT_OUTCOMES * 3)?;
        proposals.retain(|proposal| {
            matches!(
                proposal.status,
                ProposalStatus::Accepted | ProposalStatus::Rejected
            )
        });
        proposals.sort_unstable_by(compare_proposals);
        proposals.truncate(MAX_RECENT_OUTCOMES);
        let experiments = ExperimentRepository::new(self.db)
            .list_terminal_for_campaign(&campaign.campaign_id, MAX_RECENT_OUTCOMES)?;
        let status = CampaignRepository::new(self.db)
            .status_projection_for_project(&campaign.project_id, request.observed_at)?
            .ok_or_else(|| validation_error("campaign", "has no status projection"))?;
        if status.campaign_id != campaign.campaign_id {
            return Err(validation_error(
                "campaign",
                "status projection does not match the reserved campaign",
            ));
        }
        let mut interventions = InterventionRepository::new(self.db).list(
            &campaign.project_id,
            crate::models::InterventionStatus::Pending,
            crate::interventions::MAX_INTERVENTIONS_PER_RUN,
        )?;
        interventions.sort_unstable_by(compare_interventions);

        let artifact_hints = collect_decision_artifact_hints(
            request.root_anchor,
            MAX_ARTIFACT_HINTS,
            MAX_ARTIFACT_HINT_DEPTH,
            MAX_ARTIFACT_HINT_FIELD_BYTES,
        )?;
        let context = DecisionContext {
            schema_version: DECISION_CONTEXT_SCHEMA_VERSION,
            objective: ObjectiveSection {
                text: &campaign.objective_text,
                digest: &campaign.objective_digest,
            },
            source_experiment: SourceExperimentSection {
                experiment_id: &source.experiment_id,
                proposal_id: &source.proposal_id,
                proposal_kind: source_proposal.kind.as_str(),
                status: source.status.as_str(),
                attempt: source.attempt,
                command_digest: &command_digest,
                failure_code: source.failure_code.as_deref(),
                failure_fingerprint: source.failure_fingerprint.as_deref(),
                created_at: source.created_at,
                updated_at: source.updated_at,
                finished_at: source.finished_at,
            },
            terminal_observation,
            recent_outcomes: RecentOutcomesSection {
                proposals: proposals.iter().map(RecentProposal::from).collect(),
                experiments: experiments.iter().map(RecentExperiment::from).collect(),
            },
            budgets: BudgetSection {
                campaign_state: status.state,
                next_eligible_at: status.next_eligible_at,
                rolling_usage: status.rolling_usage,
                experiment_counts: status.experiment_counts,
            },
            intervention: InterventionSection {
                pending: interventions
                    .iter()
                    .map(PendingIntervention::from)
                    .collect(),
            },
            artifact_hints,
        };
        let bytes = serde_json::to_vec(&context).map_err(|source| AppError::Serialization {
            operation: "serialize decision context",
            source,
        })?;
        if bytes.len() > MAX_DECISION_CONTEXT_BYTES {
            return Err(validation_error(
                "decision_context",
                "exceeds the serialized context limit",
            ));
        }
        let digest = format!("{:x}", Sha256::digest(&bytes));
        let json = String::from_utf8(bytes).expect("JSON serialization always emits UTF-8");
        Ok(DecisionContextBundle { json, digest })
    }
}

#[derive(Serialize)]
struct DecisionContext<'a> {
    schema_version: u8,
    objective: ObjectiveSection<'a>,
    source_experiment: SourceExperimentSection<'a>,
    terminal_observation: TerminalObservationSection<'a>,
    recent_outcomes: RecentOutcomesSection<'a>,
    budgets: BudgetSection,
    intervention: InterventionSection,
    artifact_hints: Vec<DecisionArtifactHint>,
}

#[derive(Serialize)]
struct ObjectiveSection<'a> {
    text: &'a str,
    digest: &'a str,
}

#[derive(Serialize)]
struct SourceExperimentSection<'a> {
    experiment_id: &'a str,
    proposal_id: &'a str,
    proposal_kind: &'a str,
    status: &'a str,
    attempt: i64,
    command_digest: &'a str,
    failure_code: Option<&'a str>,
    failure_fingerprint: Option<&'a str>,
    created_at: i64,
    updated_at: i64,
    finished_at: Option<i64>,
}

#[derive(Serialize)]
struct TerminalObservationSection<'a> {
    task_id: Option<i64>,
    task_signature: Option<&'a str>,
    state: &'a str,
    enqueued_at: Option<i64>,
    started_at: Option<i64>,
    ended_at: Option<i64>,
    exit_code: Option<i32>,
}

#[derive(Serialize)]
struct RecentOutcomesSection<'a> {
    proposals: Vec<RecentProposal<'a>>,
    experiments: Vec<RecentExperiment<'a>>,
}

#[derive(Serialize)]
struct RecentProposal<'a> {
    proposal_id: &'a str,
    kind: &'a str,
    status: &'a str,
    source_experiment_id: Option<&'a str>,
    canonical_digest: &'a str,
    created_at: i64,
    updated_at: i64,
}

impl<'a> From<&'a Proposal> for RecentProposal<'a> {
    fn from(proposal: &'a Proposal) -> Self {
        Self {
            proposal_id: &proposal.proposal_id,
            kind: proposal.kind.as_str(),
            status: proposal.status.as_str(),
            source_experiment_id: proposal.source_experiment_id.as_deref(),
            canonical_digest: &proposal.canonical_digest,
            created_at: proposal.created_at,
            updated_at: proposal.updated_at,
        }
    }
}

#[derive(Serialize)]
struct RecentExperiment<'a> {
    experiment_id: &'a str,
    proposal_id: &'a str,
    status: &'a str,
    attempt: i64,
    failure_code: Option<&'a str>,
    failure_fingerprint: Option<&'a str>,
    created_at: i64,
    updated_at: i64,
    finished_at: Option<i64>,
}

impl<'a> From<&'a Experiment> for RecentExperiment<'a> {
    fn from(experiment: &'a Experiment) -> Self {
        Self {
            experiment_id: &experiment.experiment_id,
            proposal_id: &experiment.proposal_id,
            status: experiment.status.as_str(),
            attempt: experiment.attempt,
            failure_code: experiment.failure_code.as_deref(),
            failure_fingerprint: experiment.failure_fingerprint.as_deref(),
            created_at: experiment.created_at,
            updated_at: experiment.updated_at,
            finished_at: experiment.finished_at,
        }
    }
}

#[derive(Serialize)]
struct BudgetSection {
    campaign_state: CampaignState,
    next_eligible_at: Option<i64>,
    rolling_usage: std::collections::BTreeMap<String, i64>,
    experiment_counts: std::collections::BTreeMap<String, i64>,
}

#[derive(Serialize)]
struct InterventionSection {
    pending: Vec<PendingIntervention>,
}

#[derive(Serialize)]
struct PendingIntervention {
    insertion_sequence: i64,
    message: String,
    created_at: i64,
}

impl From<&Intervention> for PendingIntervention {
    fn from(intervention: &Intervention) -> Self {
        Self {
            insertion_sequence: intervention.insertion_sequence,
            message: bounded_redacted_text(&intervention.message),
            created_at: intervention.created_at,
        }
    }
}

fn terminal_observation<'a>(
    project_group: &str,
    source: &Experiment,
    request: &'a DecisionEvidenceRequest<'a>,
) -> Result<TerminalObservationSection<'a>, AppError> {
    let Some(task_id) = source.pueue_task_id else {
        return Ok(TerminalObservationSection {
            task_id: None,
            task_signature: None,
            state: source.status.as_str(),
            enqueued_at: None,
            started_at: None,
            ended_at: source.finished_at,
            exit_code: None,
        });
    };
    let projection = request
        .pueue_tasks
        .iter()
        .find(|task| {
            task.task_id == task_id
                && task.group == project_group
                && source.task_signature.as_deref() == Some(task.task_signature.as_str())
        })
        .ok_or_else(|| {
            validation_error(
                "pueue_tasks",
                "does not contain the source experiment task identity",
            )
        })?;
    let state = canonical_task_state(&projection.state)
        .ok_or_else(|| validation_error("pueue_tasks", "contains an invalid terminal state"))?;
    Ok(TerminalObservationSection {
        task_id: Some(projection.task_id),
        task_signature: Some(&projection.task_signature),
        state,
        enqueued_at: projection.enqueued_at,
        started_at: projection.started_at,
        ended_at: projection.ended_at,
        exit_code: projection.exit_code,
    })
}

fn canonical_task_state(state: &str) -> Option<&'static str> {
    if state.eq_ignore_ascii_case("done") {
        Some("done")
    } else if state.eq_ignore_ascii_case("failed") {
        Some("failed")
    } else if state.eq_ignore_ascii_case("killed") {
        Some("killed")
    } else if state.eq_ignore_ascii_case("finished") {
        Some("finished")
    } else if state.eq_ignore_ascii_case("success") {
        Some("success")
    } else {
        None
    }
}

fn compare_proposals(left: &Proposal, right: &Proposal) -> std::cmp::Ordering {
    right
        .created_at
        .cmp(&left.created_at)
        .then_with(|| right.proposal_id.cmp(&left.proposal_id))
}

fn compare_interventions(left: &Intervention, right: &Intervention) -> std::cmp::Ordering {
    left.insertion_sequence
        .cmp(&right.insertion_sequence)
        .then_with(|| left.intervention_id.cmp(&right.intervention_id))
}

fn is_terminal(status: ExperimentStatus) -> bool {
    matches!(
        status,
        ExperimentStatus::Succeeded | ExperimentStatus::Failed | ExperimentStatus::Cancelled
    )
}

fn validation_error(field: &'static str, message: &'static str) -> AppError {
    AppError::Validation { field, message }
}

#[cfg(test)]
mod tests {
    use super::validate_stored_decision_context;
    use serde_json::{json, Value};

    fn valid_v2_context() -> Value {
        json!({
            "schema_version": 2,
            "objective": {"text": "Reach the objective", "digest": "objective-digest"},
            "source_experiment": {
                "experiment_id": "experiment-1", "proposal_id": "proposal-1",
                "proposal_kind": "experiment", "status": "succeeded", "attempt": 1,
                "command_digest": "command-digest", "failure_code": null,
                "failure_fingerprint": null, "created_at": 100, "updated_at": 103,
                "finished_at": 103
            },
            "terminal_observation": {
                "task_id": 41, "task_signature": "pueue-task:v1:terminal",
                "state": "done", "enqueued_at": 100, "started_at": 101,
                "ended_at": 103, "exit_code": 0
            },
            "recent_outcomes": {"proposals": [], "experiments": []},
            "budgets": {"campaign_state": "active", "next_eligible_at": null,
                "rolling_usage": {}, "experiment_counts": {}},
            "intervention": {"pending": []}, "artifact_hints": [],
            "research": {
                "review_id": "review-1",
                "reason": "continue from the confirmed terminal result",
                "next_direction": "try the lower learning rate",
                "recent_advice": [{
                    "evidence_ref": "research:review-1:note",
                    "review_id": "review-1",
                    "attempt": 0,
                    "state": "completed",
                    "notes": "the loss improved"
                }]
            }
        })
    }

    #[test]
    fn strict_v2_context_accepts_bounded_research_handoff() {
        let context = valid_v2_context();
        let json = serde_json::to_string(&context).unwrap();

        assert_eq!(
            validate_stored_decision_context(&json, "objective-digest", "experiment-1")
                .unwrap(),
            "objective-digest"
        );
    }

    #[test]
    fn strict_v2_context_rejects_untrusted_research_fields_and_bounds() {
        let mut cases = Vec::new();

        let mut mismatched_ref = valid_v2_context();
        mismatched_ref["research"]["recent_advice"][0]["evidence_ref"] =
            json!("research:other-review:note");
        cases.push(("mismatched evidence reference", mismatched_ref));

        let mut malformed_ref = valid_v2_context();
        malformed_ref["research"]["recent_advice"][0]["evidence_ref"] = json!("unrelated");
        cases.push(("malformed evidence reference", malformed_ref));

        let mut omitted_ref = valid_v2_context();
        omitted_ref["research"]["recent_advice"][0]
            .as_object_mut()
            .unwrap()
            .remove("evidence_ref");
        cases.push(("omitted evidence reference", omitted_ref));

        let mut unknown_top_level = valid_v2_context();
        unknown_top_level["unknown"] = json!(true);
        cases.push(("unknown top-level field", unknown_top_level));

        let mut unknown_research = valid_v2_context();
        unknown_research["research"]["unknown"] = json!(true);
        cases.push(("unknown research field", unknown_research));

        let mut unknown_advice = valid_v2_context();
        unknown_advice["research"]["recent_advice"][0]["unknown"] = json!(true);
        cases.push(("unknown advice field", unknown_advice));

        let mut oversized_review_id = valid_v2_context();
        oversized_review_id["research"]["review_id"] =
            json!("r".repeat(crate::research_protocol::MAX_RESEARCH_ID_BYTES + 1));
        cases.push(("oversized research review id", oversized_review_id));

        let mut oversized_advice_review_id = valid_v2_context();
        let advice_review_id = "a".repeat(crate::research_protocol::MAX_RESEARCH_ID_BYTES + 1);
        oversized_advice_review_id["research"]["recent_advice"][0]["review_id"] =
            json!(&advice_review_id);
        oversized_advice_review_id["research"]["recent_advice"][0]["evidence_ref"] =
            json!(format!("research:{advice_review_id}:note"));
        cases.push((
            "oversized advice review id",
            oversized_advice_review_id,
        ));

        let mut oversized_reason = valid_v2_context();
        oversized_reason["research"]["reason"] =
            json!("r".repeat(crate::research_protocol::MAX_RESEARCH_REASON_BYTES + 1));
        cases.push(("oversized research reason", oversized_reason));

        let mut oversized_direction = valid_v2_context();
        oversized_direction["research"]["next_direction"] =
            json!("d".repeat(crate::research_protocol::MAX_RESEARCH_NEXT_DIRECTION_BYTES + 1));
        cases.push(("oversized next direction", oversized_direction));

        let mut oversized_notes = valid_v2_context();
        oversized_notes["research"]["recent_advice"][0]["notes"] =
            json!("n".repeat(crate::research_protocol::MAX_RESEARCH_NOTES_BYTES + 1));
        cases.push(("oversized advice notes", oversized_notes));

        let mut too_many_notes = valid_v2_context();
        let advice = too_many_notes["research"]["recent_advice"][0].clone();
        too_many_notes["research"]["recent_advice"] = Value::Array(
            std::iter::repeat(advice)
                .take(super::MAX_RECENT_RESEARCH_ADVICE + 1)
                .collect(),
        );
        cases.push(("too many advice notes", too_many_notes));

        for (label, context) in cases {
            let json = serde_json::to_string(&context).unwrap();
            assert!(
                validate_stored_decision_context(&json, "objective-digest", "experiment-1")
                    .is_err(),
                "{label} must be rejected"
            );
        }
    }
}
