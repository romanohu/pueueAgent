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
pub const MAX_DECISION_CONTEXT_BYTES: usize = 128 * 1024;
pub const MAX_ARTIFACT_HINTS: usize = 64;
pub const MAX_ARTIFACT_HINT_DEPTH: usize = 4;
pub const MAX_ARTIFACT_HINT_FIELD_BYTES: usize = 4 * 1024;

const MAX_RECENT_OUTCOMES: usize = 32;

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
    let context: StoredDecisionContext =
        serde_json::from_value(value).map_err(|source| AppError::Serialization {
            operation: "validate stored decision context schema",
            source,
        })?;
    if context.schema_version != DECISION_CONTEXT_SCHEMA_VERSION {
        return Err(validation_error(
            "decision_context.schema_version",
            "does not match the supported schema version",
        ));
    }
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
    Ok(context.objective.digest)
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
