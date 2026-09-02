use std::collections::BTreeMap;

use rusqlite::{
    params,
    types::Type,
    Connection, OptionalExtension, Row, Transaction, TransactionBehavior,
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::{
    code_change,
    execution_policy::CampaignLimits,
    models::{
        BudgetDimension, BudgetReservation, BudgetReservationStatus, Campaign, CampaignState,
        CodeChangeState, EventKind, Experiment, ExperimentStatus, ExperimentTerminalOutcome,
        NewCodeChangeRun, NewEvent, ObjectiveMetric, Proposal, ProposalKind, ProposalStatus,
        Submission, SubmissionKind, SubmissionStatus,
    },
    output::bounded_redacted_text,
    proposals::ValidatedProposal,
    state::ObjectiveSnapshot,
    AppError,
};

use super::{code_changes::validate_sha, database_error, Db};

const ROLLING_WINDOW_SECONDS: i64 = 24 * 60 * 60;
const AGENT_RUN_WINDOW_SECONDS: i64 = 60 * 60;
const MAX_FAILURE_FIELD_BYTES: usize = 128;
const MAX_STATUS_TASK_IDS: i64 = 100;

const CAMPAIGN_SELECT: &str = "SELECT campaign_id, project_id, objective_text, objective_digest,
        initial_argv_json, state, state_reason, baseline_experiment_id, next_eligible_at,
        created_at, updated_at, base_revision_sha
    FROM campaigns";
const PROPOSAL_SELECT: &str = "SELECT proposal_id, campaign_id, kind, status, hypothesis,
        source_experiment_id, argv_json, working_directory, expected_evidence_json,
        canonical_digest, reject_reason, created_at, updated_at
    FROM proposals";
const EXPERIMENT_SELECT: &str = "SELECT experiment_id, campaign_id, proposal_id, submission_id,
        parent_experiment_id, attempt, status, pueue_task_id, task_signature, failure_code,
        failure_fingerprint, created_at, updated_at, finished_at,
        code_change_run_id, code_revision_sha
    FROM experiments";
const SUBMISSION_SELECT: &str = "SELECT submission_id, project_id, argv_json, created_at,
        pueue_task_id, task_signature, status, kind, metadata_json, origin_agent_run_id
    FROM submissions";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedSubmissionIntent {
    pub campaign: Campaign,
    pub proposal: Proposal,
    pub experiment: Experiment,
    pub submission: Submission,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentDecisionReservation {
    Reserved(BudgetReservation),
    BudgetWaiting { next_eligible_at: i64 },
    Deferred { state: CampaignState },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalAcceptance {
    Accepted(ManagedSubmissionIntent),
    PendingCodeChange,
    BudgetWaiting { next_eligible_at: i64 },
}

impl ProposalAcceptance {
    pub fn accepted(self) -> Option<ManagedSubmissionIntent> {
        match self {
            Self::Accepted(intent) => Some(intent),
            Self::PendingCodeChange | Self::BudgetWaiting { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CampaignStatusProjection {
    pub campaign_id: String,
    pub state: CampaignState,
    pub state_reason: Option<String>,
    pub objective_digest: String,
    pub next_eligible_at: Option<i64>,
    pub experiment_counts: BTreeMap<String, i64>,
    pub rolling_usage: BTreeMap<String, i64>,
    pub unreconciled_count: i64,
    pub current_best_experiment_id: Option<String>,
    pub plateau_count: i64,
    pub primary_metric_name: Option<String>,
    pub primary_metric_value: Option<f64>,
    pub has_objective: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CampaignDoctorProjection {
    pub live_campaign_count: i64,
    pub objective_digest: Option<String>,
    pub baseline_linkage_errors: i64,
    pub orphan_reservations: i64,
    pub task_identity_disagreements: i64,
    pub submission_boundary_count: i64,
    pub budget_wake_errors: i64,
}

pub struct StartCampaignRequest<'a> {
    pub campaign_id: &'a str,
    pub project_id: &'a str,
    pub objective: &'a ObjectiveSnapshot,
    pub initial_argv: &'a [String],
    pub baseline: &'a ValidatedProposal,
    pub submission_id: &'a str,
    pub experiment_id: &'a str,
    pub proposal_id: &'a str,
    pub metadata: &'a Value,
    pub origin_agent_run_id: Option<i64>,
    pub objective_metric: Option<&'a ObjectiveMetric>,
    pub now: i64,
}

pub struct CampaignRepository<'db> {
    db: &'db Db,
}

pub struct ProposalRepository<'db> {
    db: &'db Db,
}

pub struct ExperimentRepository<'db> {
    db: &'db Db,
}

impl<'db> CampaignRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn start_with_baseline(
        &self,
        request: StartCampaignRequest<'_>,
        limits: &CampaignLimits,
    ) -> Result<ManagedSubmissionIntent, AppError> {
        self.start_with_baseline_at_revision(request, limits, None)
    }

    pub fn start_with_baseline_at_revision(
        &self,
        request: StartCampaignRequest<'_>,
        limits: &CampaignLimits,
        base_revision_sha: Option<&str>,
    ) -> Result<ManagedSubmissionIntent, AppError> {
        if let Some(base_revision_sha) = base_revision_sha {
            validate_sha("base_revision_sha", base_revision_sha)?;
        }
        if let Some(objective_metric) = request.objective_metric {
            objective_metric.validate()?;
        }
        let initial_argv_json = serialize_strings(
            request.initial_argv,
            "serialize campaign initial arguments",
        )?;
        let proposal_argv_json =
            serialize_strings(request.baseline.argv(), "serialize baseline arguments")?;
        let evidence_json = serialize_strings(
            request.baseline.expected_evidence(),
            "serialize baseline expected evidence",
        )?;
        let metadata_json = serialize_submission_metadata(
            Some(request.metadata),
            request.campaign_id,
            request.proposal_id,
            request.experiment_id,
        )?;
        let objective_metric_json = match request.objective_metric {
            Some(metric) => Some(
                serde_json::to_string(metric).map_err(|source| AppError::Serialization {
                    operation: "serialize campaign objective metric",
                    source,
                })?,
            ),
            None => None,
        };
        let window_ends_at = rolling_window_end(request.now)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin campaign baseline reservation"))?;

        validate_project_available(&transaction, request.project_id)?;
        if request.baseline.objective_digest() != request.objective.digest {
            return Err(validation_error(
                "baseline.objective_digest",
                "must match the campaign objective digest",
            ));
        }
        if request.baseline.kind() != ProposalKind::Experiment {
            return Err(validation_error(
                "baseline.kind",
                "must be an experiment proposal",
            ));
        }
        if request.baseline.source_experiment_id().is_some() {
            return Err(validation_error(
                "baseline.source_experiment_id",
                "must be absent",
            ));
        }
        if request.baseline.argv() != request.initial_argv {
            return Err(validation_error(
                "baseline.argv",
                "must match the campaign initial arguments",
            ));
        }
        if limits.max_parallel_experiments == 0
            || limits.max_new_experiments_per_24h == 0
            || limits.max_proposals_per_cycle == 0
        {
            return Err(validation_error(
                "campaign_limits",
                "do not permit a baseline experiment",
            ));
        }
        if transaction
            .query_row(
                "SELECT 1 FROM campaigns WHERE project_id = ?1 AND state <> 'retired' LIMIT 1",
                [request.project_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(database_error("check live campaign before baseline start"))?
            .is_some()
        {
            return Err(validation_error(
                "campaign",
                "the project already has a live campaign",
            ));
        }
        validate_submission_origin_agent_run(
            &transaction,
            request.project_id,
            request.origin_agent_run_id,
        )?;

        transaction
            .execute(
                "INSERT INTO campaigns (
                    campaign_id, project_id, objective_text, objective_digest, initial_argv_json,
                    state, state_reason, baseline_experiment_id, next_eligible_at,
                    objective_metric_json, base_revision_sha, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, NULL, ?7, ?8, ?9, ?9)",
                params![
                    request.campaign_id,
                    request.project_id,
                    request.objective.text,
                    request.objective.digest,
                    initial_argv_json,
                    CampaignState::Active,
                    objective_metric_json,
                    base_revision_sha,
                    request.now,
                ],
            )
            .map_err(database_error("insert campaign baseline"))?;
        insert_proposal(
            &transaction,
            request.proposal_id,
            request.campaign_id,
            request.baseline,
            ProposalStatus::Accepted,
            &proposal_argv_json,
            &evidence_json,
            request.now,
        )?;
        insert_submission(
            &transaction,
            request.submission_id,
            request.project_id,
            &proposal_argv_json,
            &metadata_json,
            request.origin_agent_run_id,
            request.now,
        )?;
        insert_experiment(
            &transaction,
            request.experiment_id,
            request.campaign_id,
            request.proposal_id,
            request.submission_id,
            None,
            0,
            None,
            None,
            None,
            None,
            request.now,
        )?;
        insert_experiment_reservation(
            &transaction,
            request.campaign_id,
            request.experiment_id,
            request.now,
            window_ends_at,
        )?;
        let updated = transaction
            .execute(
                "UPDATE campaigns
                 SET baseline_experiment_id = ?1, updated_at = ?2
                 WHERE campaign_id = ?3 AND baseline_experiment_id IS NULL",
                params![request.experiment_id, request.now, request.campaign_id],
            )
            .map_err(database_error("bind campaign baseline experiment"))?;
        if updated != 1 {
            return Err(validation_error(
                "campaign",
                "baseline experiment binding changed concurrently",
            ));
        }

        let intent = read_intent_by_experiment(&transaction, request.experiment_id)?;
        transaction
            .commit()
            .map_err(database_error("commit campaign baseline reservation"))?;
        Ok(intent)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn accept_proposal(
        &self,
        campaign_id: &str,
        proposal_id: &str,
        experiment_id: &str,
        submission_id: &str,
        proposal: &ValidatedProposal,
        limits: &CampaignLimits,
        now: i64,
    ) -> Result<ProposalAcceptance, AppError> {
        if proposal.kind() == ProposalKind::CodeChange {
            return Err(validation_error(
                "proposal.kind",
                "code-change proposals require an admission outcome",
            ));
        }
        self.accept_proposal_inner(
            campaign_id,
            proposal_id,
            experiment_id,
            submission_id,
            proposal,
            limits,
            now,
            None,
            None,
        )
    }

    pub fn reject_orphan_code_change(
        &self,
        campaign_id: &str,
        proposal_id: &str,
        reason: &str,
        now: i64,
    ) -> Result<Proposal, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin orphan code-change normalization"))?;
        let campaign = read_campaign(&transaction, campaign_id)?;
        let proposal = read_proposal(&transaction, proposal_id)?;
        if proposal.campaign_id != campaign_id
            || proposal.kind != ProposalKind::CodeChange
            || proposal.status != ProposalStatus::Pending
        {
            return Err(validation_error(
                "proposal",
                "must be a pending code-change proposal in the campaign",
            ));
        }
        let run_exists: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM code_change_runs
                 WHERE campaign_id = ?1 AND proposal_id = ?2",
                params![campaign_id, proposal_id],
                |row| row.get(0),
            )
            .map_err(database_error("check orphan code-change run"))?;
        if run_exists != 0 {
            return Err(validation_error(
                "code_change.run",
                "cannot normalize a proposal that has a durable run",
            ));
        }
        reject_code_change_in_transaction(
            &transaction,
            campaign_id,
            proposal_id,
            &campaign.project_id,
            reason,
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit orphan code-change normalization"))?;
        let mut rejected = proposal;
        rejected.status = ProposalStatus::Rejected;
        rejected.reject_reason = Some(bounded_redacted_text(reason));
        rejected.updated_at = now;
        Ok(rejected)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn accept_code_change_proposal(
        &self,
        campaign_id: &str,
        proposal_id: &str,
        experiment_id: &str,
        submission_id: &str,
        proposal: &ValidatedProposal,
        limits: &CampaignLimits,
        now: i64,
        run: Option<&NewCodeChangeRun>,
        rejection_reason: Option<&str>,
    ) -> Result<ProposalAcceptance, AppError> {
        if proposal.kind() != ProposalKind::CodeChange {
            return Err(validation_error(
                "proposal.kind",
                "must be a code-change proposal",
            ));
        }
        if run.is_none() == rejection_reason.is_none() {
            return Err(validation_error(
                "code_change",
                "must provide exactly one durable outcome",
            ));
        }
        self.accept_proposal_inner(
            campaign_id,
            proposal_id,
            experiment_id,
            submission_id,
            proposal,
            limits,
            now,
            run,
            rejection_reason,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn accept_proposal_inner(
        &self,
        campaign_id: &str,
        proposal_id: &str,
        experiment_id: &str,
        submission_id: &str,
        proposal: &ValidatedProposal,
        limits: &CampaignLimits,
        now: i64,
        code_change_run: Option<&NewCodeChangeRun>,
        code_change_rejection_reason: Option<&str>,
    ) -> Result<ProposalAcceptance, AppError> {
        let argv_json = serialize_strings(
            proposal.argv(),
            "serialize campaign proposal arguments",
        )?;
        let evidence_json = serialize_strings(
            proposal.expected_evidence(),
            "serialize campaign proposal expected evidence",
        )?;
        let metadata_json =
            serialize_submission_metadata(None, campaign_id, proposal_id, experiment_id)?;
        let window_ends_at = rolling_window_end(now)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin campaign proposal acceptance"))?;

        let campaign = read_campaign(&transaction, campaign_id)?;
        if campaign.state == CampaignState::BudgetWaiting {
            let next_eligible_at = campaign.next_eligible_at.ok_or_else(|| {
                validation_error(
                    "campaign.next_eligible_at",
                    "budget-waiting campaign must have a finite wake time",
                )
            })?;
            return Ok(ProposalAcceptance::BudgetWaiting { next_eligible_at });
        }
        if campaign.state != CampaignState::Active {
            return Err(validation_error(
                "campaign",
                "must be active to accept a proposal",
            ));
        }
        validate_project_available(&transaction, &campaign.project_id)?;
        if proposal.objective_digest() != campaign.objective_digest {
            return Err(validation_error(
                "proposal.objective_digest",
                "must match the persisted campaign objective digest",
            ));
        }

        if let Some(existing) = find_proposal_by_digest(
            &transaction,
            campaign_id,
            proposal.canonical_digest(),
        )? {
            return match existing.status {
                ProposalStatus::Accepted => {
                    read_intent_by_proposal(&transaction, &existing.proposal_id)
                        .map(ProposalAcceptance::Accepted)
                }
                ProposalStatus::Pending if existing.kind == ProposalKind::CodeChange => {
                    Ok(ProposalAcceptance::PendingCodeChange)
                }
                ProposalStatus::Pending | ProposalStatus::Rejected => Err(validation_error(
                    "proposal.canonical_digest",
                    "matches a proposal that has no accepted experiment",
                )),
            };
        }

        let source_experiment_id = proposal.source_experiment_id().ok_or_else(|| {
            validation_error(
                "source_experiment_id",
                "is required outside baseline campaign creation",
            )
        })?;
        let source = read_experiment(&transaction, source_experiment_id)?;
        if source.campaign_id != campaign_id {
            return Err(validation_error(
                "source_experiment_id",
                "must belong to the same campaign",
            ));
        }
        if !is_terminal(source.status) {
            return Err(validation_error(
                "source_experiment_id",
                "must reference a terminal experiment",
            ));
        }

        let cycle_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM proposals
                 WHERE campaign_id = ?1 AND source_experiment_id = ?2 AND status = 'accepted'",
                params![campaign_id, source_experiment_id],
                |row| row.get(0),
            )
            .map_err(database_error("count campaign decision-cycle proposals"))?;
        if cycle_count >= i64::from(limits.max_proposals_per_cycle) {
            return Err(validation_error(
                "campaign_limits.max_proposals_per_cycle",
                "the source decision cycle has no proposal slots",
            ));
        }

        if proposal.kind() == ProposalKind::CodeChange {
            if limits.max_code_change_proposals_per_24h == 0 {
                return Err(validation_error(
                    "campaign_limits.max_code_change_proposals_per_24h",
                    "does not permit code-change proposals",
                ));
            }
            let code_change_count = count_live_reservations(
                &transaction,
                campaign_id,
                BudgetDimension::CodeChange,
                now,
            )?;
            if code_change_count >= i64::from(limits.max_code_change_proposals_per_24h) {
                return enter_proposal_budget_wait(
                    transaction,
                    campaign_id,
                    BudgetDimension::CodeChange,
                    "code_change_budget_exhausted",
                    now,
                );
            }
            insert_proposal(
                &transaction,
                proposal_id,
                campaign_id,
                proposal,
                ProposalStatus::Pending,
                &argv_json,
                &evidence_json,
                now,
            )?;
            insert_code_change_reservation(
                &transaction,
                campaign_id,
                proposal_id,
                now,
                window_ends_at,
            )?;
            if let Some(run) = code_change_run {
                validate_atomic_code_change_run(run, campaign_id, proposal_id)?;
                insert_code_change_run_in_transaction(&transaction, run, &campaign.project_id)?;
            } else if let Some(reason) = code_change_rejection_reason {
                reject_code_change_in_transaction(
                    &transaction,
                    campaign_id,
                    proposal_id,
                    &campaign.project_id,
                    reason,
                    now,
                )?;
            }
            transaction
                .commit()
                .map_err(database_error("commit pending code-change proposal"))?;
            return Ok(ProposalAcceptance::PendingCodeChange);
        }

        let parallel_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM experiments
                 WHERE campaign_id = ?1
                   AND status IN ('reserved','submitting','accepted','unreconciled')",
                [campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error("count parallel campaign experiments"))?;
        if parallel_count >= i64::from(limits.max_parallel_experiments) {
            return Err(validation_error(
                "campaign_limits.max_parallel_experiments",
                "the parallel experiment budget is exhausted",
            ));
        }
        let rolling_count = count_live_reservations(
            &transaction,
            campaign_id,
            BudgetDimension::Experiment,
            now,
        )?;
        if rolling_count >= i64::from(limits.max_new_experiments_per_24h) {
            return enter_proposal_budget_wait(
                transaction,
                campaign_id,
                BudgetDimension::Experiment,
                "experiment_budget_exhausted",
                now,
            );
        }
        let same_spec_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*)
                 FROM experiments AS experiment
                 JOIN proposals AS candidate ON candidate.proposal_id = experiment.proposal_id
                 WHERE experiment.campaign_id = ?1
                   AND candidate.argv_json = ?2
                   AND candidate.working_directory = ?3",
                params![campaign_id, argv_json, proposal.working_directory()],
                |row| row.get(0),
            )
            .map_err(database_error("count same-spec campaign experiments"))?;
        if same_spec_count > i64::from(limits.max_same_spec_retries) {
            return Err(validation_error(
                "campaign_limits.max_same_spec_retries",
                "the same experiment specification exhausted its retries",
            ));
        }
        if proposal.kind() == ProposalKind::Repair {
            let fingerprint = source
                .failure_fingerprint
                .as_deref()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    validation_error(
                        "source_experiment_id",
                        "repair proposals require a trusted source failure fingerprint",
                    )
                })?;
            let repair_count: i64 = transaction
                .query_row(
                    "SELECT COUNT(*)
                     FROM proposals AS repair
                     JOIN experiments AS repair_source
                       ON repair_source.experiment_id = repair.source_experiment_id
                     WHERE repair.campaign_id = ?1
                       AND repair.kind = 'repair'
                       AND repair.status <> 'rejected'
                       AND repair_source.failure_fingerprint = ?2",
                    params![campaign_id, fingerprint],
                    |row| row.get(0),
                )
                .map_err(database_error("count repairs for failure fingerprint"))?;
            if repair_count >= i64::from(limits.max_repairs_per_failure_fingerprint) {
                return Err(validation_error(
                    "campaign_limits.max_repairs_per_failure_fingerprint",
                    "the source failure fingerprint exhausted its repairs",
                ));
            }
        }

        insert_proposal(
            &transaction,
            proposal_id,
            campaign_id,
            proposal,
            ProposalStatus::Accepted,
            &argv_json,
            &evidence_json,
            now,
        )?;
        insert_submission(
            &transaction,
            submission_id,
            &campaign.project_id,
            &argv_json,
            &metadata_json,
            None,
            now,
        )?;
        insert_experiment(
            &transaction,
            experiment_id,
            campaign_id,
            proposal_id,
            submission_id,
            Some(source_experiment_id),
            same_spec_count,
            None,
            None,
            None,
            None,
            now,
        )?;
        insert_experiment_reservation(
            &transaction,
            campaign_id,
            experiment_id,
            now,
            window_ends_at,
        )?;
        let intent = read_intent_by_experiment(&transaction, experiment_id)?;
        transaction
            .commit()
            .map_err(database_error("commit campaign proposal acceptance"))?;
        Ok(ProposalAcceptance::Accepted(intent))
    }

    /// Accept a same-spec resume proposal for a terminal source experiment and
    /// reserve the successor experiment with checkpoint lineage metadata
    /// (`resume_of_experiment_id`, `checkpoint_note`).  Returns `Ok(None)`
    /// when the coordinator budget path cannot admit the successor now so the
    /// caller can fall back to a bounded escalation instead of forcing the
    /// reservation through.
    #[allow(clippy::too_many_arguments)]
    pub fn accept_resume_proposal(
        &self,
        campaign_id: &str,
        proposal_id: &str,
        experiment_id: &str,
        submission_id: &str,
        proposal: &ValidatedProposal,
        checkpoint_note: &str,
        limits: &CampaignLimits,
        now: i64,
    ) -> Result<Option<ManagedSubmissionIntent>, AppError> {
        let argv_json = serialize_strings(
            proposal.argv(),
            "serialize campaign resume arguments",
        )?;
        let evidence_json = serialize_strings(
            proposal.expected_evidence(),
            "serialize campaign resume expected evidence",
        )?;
        let metadata_json =
            serialize_submission_metadata(None, campaign_id, proposal_id, experiment_id)?;
        let window_ends_at = rolling_window_end(now)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin campaign resume proposal acceptance"))?;

        let campaign = read_campaign(&transaction, campaign_id)?;
        if campaign.state != CampaignState::Active {
            transaction
                .commit()
                .map_err(database_error("commit blocked campaign resume proposal"))?;
            return Ok(None);
        }
        validate_project_available(&transaction, &campaign.project_id)?;
        if proposal.objective_digest() != campaign.objective_digest {
            return Err(validation_error(
                "proposal.objective_digest",
                "must match the persisted campaign objective digest",
            ));
        }
        if find_proposal_by_digest(&transaction, campaign_id, proposal.canonical_digest())?
            .is_some()
        {
            return Err(validation_error(
                "proposal.canonical_digest",
                "already exists in this campaign",
            ));
        }
        let source_experiment_id = proposal.source_experiment_id().ok_or_else(|| {
            validation_error(
                "source_experiment_id",
                "is required for a resume proposal",
            )
        })?;
        let source = read_experiment(&transaction, source_experiment_id)?;
        if source.campaign_id != campaign_id {
            return Err(validation_error(
                "source_experiment_id",
                "must belong to the same campaign",
            ));
        }
        if !is_terminal(source.status) {
            return Err(validation_error(
                "source_experiment_id",
                "must reference a terminal experiment",
            ));
        }

        let resume_count: i64 =
            count_live_repair_descendants(&transaction, source_experiment_id)
                .map_err(database_error("count campaign resume successors"))?;
        if resume_count >= i64::from(limits.max_live_repairs) {
            transaction
                .commit()
                .map_err(database_error("commit blocked campaign resume proposal"))?;
            return Ok(None);
        }
        let parallel_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM experiments
                 WHERE campaign_id = ?1
                   AND status IN ('reserved','submitting','accepted','unreconciled')",
                [campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error("count parallel campaign experiments"))?;
        if parallel_count >= i64::from(limits.max_parallel_experiments) {
            transaction
                .commit()
                .map_err(database_error("commit blocked campaign resume proposal"))?;
            return Ok(None);
        }
        let rolling_count = count_live_reservations(
            &transaction,
            campaign_id,
            BudgetDimension::Experiment,
            now,
        )?;
        if rolling_count >= i64::from(limits.max_new_experiments_per_24h) {
            transaction
                .commit()
                .map_err(database_error("commit blocked campaign resume proposal"))?;
            return Ok(None);
        }
        let same_spec_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*)
                 FROM experiments AS experiment
                 JOIN proposals AS candidate ON candidate.proposal_id = experiment.proposal_id
                 WHERE experiment.campaign_id = ?1
                   AND candidate.argv_json = ?2
                   AND candidate.working_directory = ?3",
                params![campaign_id, argv_json, proposal.working_directory()],
                |row| row.get(0),
            )
            .map_err(database_error("count same-spec campaign experiments"))?;
        if same_spec_count > i64::from(limits.max_same_spec_retries) {
            transaction
                .commit()
                .map_err(database_error("commit blocked campaign resume proposal"))?;
            return Ok(None);
        }

        insert_proposal(
            &transaction,
            proposal_id,
            campaign_id,
            proposal,
            ProposalStatus::Accepted,
            &argv_json,
            &evidence_json,
            now,
        )?;
        insert_submission(
            &transaction,
            submission_id,
            &campaign.project_id,
            &argv_json,
            &metadata_json,
            None,
            now,
        )?;
        insert_experiment(
            &transaction,
            experiment_id,
            campaign_id,
            proposal_id,
            submission_id,
            Some(source_experiment_id),
            same_spec_count,
            Some(source_experiment_id),
            Some(checkpoint_note),
            None,
            None,
            now,
        )?;
        insert_experiment_reservation(
            &transaction,
            campaign_id,
            experiment_id,
            now,
            window_ends_at,
        )?;
        let intent = read_intent_by_experiment(&transaction, experiment_id)?;
        transaction
            .commit()
            .map_err(database_error("commit campaign resume proposal acceptance"))?;
        Ok(Some(intent))
    }

    pub fn find_by_id(&self, campaign_id: &str) -> Result<Option<Campaign>, AppError> {
        let connection = self.db.connect()?;
        find_campaign(&connection, campaign_id)
    }

    pub fn find_live_by_project(&self, project_id: &str) -> Result<Option<Campaign>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!(
                    "{CAMPAIGN_SELECT} WHERE project_id = ?1 AND state <> 'retired' LIMIT 1"
                ),
                [project_id],
                campaign_from_row,
            )
            .optional()
            .map_err(database_error("find live campaign by project"))
    }

    pub fn find_latest_by_project(&self, project_id: &str) -> Result<Option<Campaign>, AppError> {
        let connection = self.db.connect()?;
        find_latest_campaign_by_project(&connection, project_id)
    }

    pub fn recover_submission_boundaries(&self, now: i64) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin campaign submission boundary recovery"))?;
        let inconsistent: i64 = transaction
            .query_row(
                "SELECT COUNT(*)
                 FROM experiments
                 JOIN submissions USING (submission_id)
                 WHERE experiments.status = 'submitting'
                   AND (submissions.status <> 'pending'
                        OR experiments.pueue_task_id IS NOT NULL
                        OR experiments.task_signature IS NOT NULL
                        OR submissions.pueue_task_id IS NOT NULL
                        OR submissions.task_signature IS NOT NULL)",
                [],
                |row| row.get(0),
            )
            .map_err(database_error(
                "validate stale campaign submission boundaries",
            ))?;
        if inconsistent != 0 {
            return Err(validation_error(
                "campaign.submission",
                "submitting campaign identity is inconsistent",
            ));
        }
        let recovered_submissions = transaction
            .execute(
                "UPDATE submissions
                 SET status = 'unreconciled'
                 WHERE status = 'pending' AND submission_id IN (
                     SELECT submission_id FROM experiments WHERE status = 'submitting'
                 )",
                [],
            )
            .map_err(database_error(
                "quarantine stale campaign submission intents",
            ))?;
        let recovered = transaction
            .execute(
                "UPDATE experiments
                 SET status = 'unreconciled', failure_code = 'pueue_add_interrupted',
                     updated_at = ?1
                 WHERE status = 'submitting'",
                [now],
            )
            .map_err(database_error(
                "quarantine stale campaign experiment submissions",
            ))?;
        if recovered_submissions != recovered {
            return Err(validation_error(
                "campaign.submission",
                "submitting campaign rows changed inconsistently during recovery",
            ));
        }
        transaction
            .commit()
            .map_err(database_error("commit campaign submission boundary recovery"))?;
        Ok(recovered)
    }

    pub fn list_reserved_submission_intents(
        &self,
        limit: usize,
    ) -> Result<Vec<ManagedSubmissionIntent>, AppError> {
        validate_inspection_limit(limit)?;
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT experiment.experiment_id
                 FROM experiments AS experiment
                 JOIN campaigns AS campaign USING (campaign_id)
                 JOIN projects AS project USING (project_id)
                 WHERE experiment.status = 'reserved'
                   AND campaign.state = 'active'
                   AND project.enabled = 1
                   AND project.paused = 0
                   AND project.halted_reason IS NULL
                 ORDER BY experiment.experiment_id
                 LIMIT ?1",
            )
            .map_err(database_error(
                "prepare reserved campaign submission intent list",
            ))?;
        let experiment_ids = statement
            .query_map([limit as i64], |row| row.get::<_, String>(0))
            .map_err(database_error(
                "query reserved campaign submission intent list",
            ))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error(
                "read reserved campaign submission intent list",
            ))?;
        drop(statement);
        experiment_ids
            .iter()
            .map(|experiment_id| read_intent_by_experiment(&connection, experiment_id))
            .collect()
    }

    pub fn reserve_agent_decision(
        &self,
        campaign_id: &str,
        decision_key: &str,
        limits: &CampaignLimits,
        now: i64,
    ) -> Result<AgentDecisionReservation, AppError> {
        self.reserve_agent_run(campaign_id, decision_key, limits, now)
    }

    /// Reserve one ordinary agent-run budget slot for a durable owner.  The
    /// subject key is intentionally caller-supplied so dedicated code-change
    /// editors consume the same finite hourly budget as decisions without
    /// changing decision reservation semantics.
    pub fn reserve_agent_run(
        &self,
        campaign_id: &str,
        decision_key: &str,
        limits: &CampaignLimits,
        now: i64,
    ) -> Result<AgentDecisionReservation, AppError> {
        if decision_key.is_empty()
            || decision_key.len() > 256
            || decision_key.chars().any(char::is_control)
        {
            return Err(validation_error(
                "decision_key",
                "must be non-empty, bounded, and contain no control characters",
            ));
        }
        let window_ends_at = now.checked_add(AGENT_RUN_WINDOW_SECONDS).ok_or_else(|| {
            validation_error(
                "now",
                "cannot represent the end of the agent-run rolling window",
            )
        })?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin campaign agent decision reservation"))?;
        let campaign = read_campaign(&transaction, campaign_id)?;

        if campaign.state == CampaignState::BudgetWaiting {
            let next_eligible_at = campaign.next_eligible_at.ok_or_else(|| {
                validation_error(
                    "campaign.next_eligible_at",
                    "budget-waiting campaign must have a finite wake time",
                )
            })?;
            transaction
                .commit()
                .map_err(database_error("commit waiting campaign agent decision"))?;
            return Ok(AgentDecisionReservation::BudgetWaiting { next_eligible_at });
        }
        if campaign.state != CampaignState::Active {
            let state = campaign.state;
            transaction
                .commit()
                .map_err(database_error("commit deferred campaign agent decision"))?;
            return Ok(AgentDecisionReservation::Deferred { state });
        }

        if let Some(existing) = find_agent_decision_reservation(
            &transaction,
            campaign_id,
            decision_key,
        )? {
            transaction
                .commit()
                .map_err(database_error("commit existing campaign agent decision"))?;
            return Ok(AgentDecisionReservation::Reserved(existing));
        }

        let live_count = count_live_reservations(
            &transaction,
            campaign_id,
            BudgetDimension::AgentRun,
            now,
        )?;
        if live_count >= i64::from(limits.max_agent_runs_per_hour) {
            let next_eligible_at = earliest_live_reservation_expiry(
                &transaction,
                campaign_id,
                BudgetDimension::AgentRun,
                now,
            )?
            .ok_or_else(|| {
                validation_error(
                    "campaign.agent_run_budget",
                    "exhausted rolling budget has no finite reservation expiry",
                )
            })?;
            let updated = transaction
                .execute(
                    "UPDATE campaigns
                     SET state = 'budget_waiting', state_reason = 'agent_run_budget_exhausted',
                         next_eligible_at = ?1, updated_at = ?2
                     WHERE campaign_id = ?3 AND state = 'active'",
                    params![next_eligible_at, now, campaign_id],
                )
                .map_err(database_error("wait for campaign agent-run budget"))?;
            if updated != 1 {
                return Err(validation_error(
                    "campaign",
                    "state changed while reserving an agent decision",
                ));
            }
            transaction
                .commit()
                .map_err(database_error("commit campaign agent-run budget wait"))?;
            return Ok(AgentDecisionReservation::BudgetWaiting { next_eligible_at });
        }

        let reservation_id = format!(
            "agent-run:{}:{:x}",
            campaign_id,
            Sha256::digest(decision_key.as_bytes())
        );
        transaction
            .execute(
                "INSERT INTO budget_reservations (
                    reservation_id, campaign_id, experiment_id, dimension, subject_key, status,
                    window_started_at, window_ends_at, created_at, updated_at
                 ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, ?6, ?6)",
                params![
                    reservation_id,
                    campaign_id,
                    BudgetDimension::AgentRun,
                    decision_key,
                    BudgetReservationStatus::Consumed,
                    now,
                    window_ends_at,
                ],
            )
            .map_err(database_error("insert campaign agent decision reservation"))?;
        let reservation = read_budget_reservation(&transaction, &reservation_id)?;
        transaction
            .commit()
            .map_err(database_error("commit campaign agent decision reservation"))?;
        Ok(AgentDecisionReservation::Reserved(reservation))
    }

    pub fn wake_eligible_campaigns(&self, now: i64) -> Result<Vec<String>, AppError> {
        self.wake_eligible_campaigns_with_limits(&CampaignLimits::default(), now)
    }

    pub fn wake_eligible_campaigns_with_limits(
        &self,
        limits: &CampaignLimits,
        now: i64,
    ) -> Result<Vec<String>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin campaign rolling-budget wake"))?;
        let campaign_ids = {
            let mut statement = transaction
                .prepare(
                    "SELECT campaign_id FROM campaigns
                     WHERE state = 'budget_waiting' AND next_eligible_at <= ?1
                     ORDER BY next_eligible_at, campaign_id
                     LIMIT 100",
                )
                .map_err(database_error("prepare eligible campaign wake list"))?;
            let rows = statement
                .query_map([now], |row| row.get::<_, String>(0))
                .map_err(database_error("query eligible campaign wake list"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read eligible campaign wake list"))?;
            rows
        };
        let dimensions = [
            (
                BudgetDimension::Experiment,
                limits.max_new_experiments_per_24h,
            ),
            (BudgetDimension::AgentRun, limits.max_agent_runs_per_hour),
            (
                BudgetDimension::CodeChange,
                limits.max_code_change_proposals_per_24h,
            ),
        ];
        let mut woken = Vec::new();
        for campaign_id in campaign_ids {
            let mut next_expiry = None;
            for (dimension, limit) in dimensions {
                if limit == 0 {
                    continue;
                }
                let used = count_live_reservations(&transaction, &campaign_id, dimension, now)?;
                if used >= i64::from(limit) {
                    let expiry = earliest_live_reservation_expiry(
                        &transaction,
                        &campaign_id,
                        dimension,
                        now,
                    )?
                    .ok_or_else(|| {
                        validation_error(
                            "campaign.next_eligible_at",
                            "exhausted rolling budget has no finite wake time",
                        )
                    })?;
                    next_expiry = Some(next_expiry.map_or(expiry, |current: i64| {
                        current.min(expiry)
                    }));
                }
            }
            if let Some(next_eligible_at) = next_expiry {
                transaction
                    .execute(
                        "UPDATE campaigns
                         SET next_eligible_at = ?1, updated_at = ?2
                         WHERE campaign_id = ?3 AND state = 'budget_waiting'",
                        params![next_eligible_at, now, campaign_id],
                    )
                    .map_err(database_error("advance campaign rolling-budget wake"))?;
            } else {
                transaction
                    .execute(
                        "UPDATE campaigns
                         SET state = 'active', state_reason = 'rolling_budget_available',
                             next_eligible_at = NULL, updated_at = ?1
                         WHERE campaign_id = ?2 AND state = 'budget_waiting'",
                        params![now, campaign_id],
                    )
                    .map_err(database_error("wake campaign rolling budget"))?;
                woken.push(campaign_id);
            }
        }
        transaction
            .commit()
            .map_err(database_error("commit campaign rolling-budget wake"))?;
        Ok(woken)
    }

    pub fn status_projection_for_project(
        &self,
        project_id: &str,
        now: i64,
    ) -> Result<Option<CampaignStatusProjection>, AppError> {
        let connection = self.db.connect()?;
        let campaign = connection
            .query_row(
                "SELECT campaign_id, state, state_reason, objective_digest, next_eligible_at, current_best_experiment_id, plateau_count, objective_metric_json
                 FROM campaigns
                 WHERE project_id = ?1
                 ORDER BY (state = 'retired') ASC, created_at DESC, campaign_id DESC
                 LIMIT 1",
                [project_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, CampaignState>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, Option<String>>(7)?,
                    ))
                },
            )
            .optional()
            .map_err(database_error("read bounded campaign status projection"))?;
        let Some((campaign_id, state, state_reason, objective_digest, next_eligible_at, current_best_experiment_id, plateau_count, objective_metric_json)) = campaign
        else {
            return Ok(None);
        };
        let has_objective = objective_metric_json.is_some();
        let mut current_best_experiment_id = current_best_experiment_id;
        let (primary_metric_name, primary_metric_value) = match &current_best_experiment_id.clone() {
            Some(best_id) => {
                let row = connection
                    .query_row(
                        "SELECT em.primary_metric_name, em.primary_metric_value FROM experiment_metrics em JOIN experiments e ON e.experiment_id = em.experiment_id WHERE em.experiment_id = ?1 AND e.campaign_id = ?2",
                        rusqlite::params![best_id, campaign_id],
                        |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, Option<f64>>(1)?)),
                    )
                    .optional()
                    .map_err(database_error("read campaign best metric for status"))?;
                match row {
                    Some((name, value)) => (name, value),
                    None => {
                        // malformed cross-campaign pointer – do not expose unvalidated best ID
                        current_best_experiment_id = None;
                        (None, None)
                    }
                }
            }
            None => (None, None),
        };
        let experiment_counts = grouped_campaign_counts(
            &connection,
            "SELECT status, COUNT(*) FROM experiments
             WHERE campaign_id = ?1 GROUP BY status",
            &campaign_id,
            "count campaign status experiment states",
        )?;
        let mut statement = connection
            .prepare(
                "SELECT dimension, COUNT(*) FROM budget_reservations
                 WHERE campaign_id = ?1 AND status IN ('reserved','consumed')
                   AND window_ends_at > ?2
                 GROUP BY dimension",
            )
            .map_err(database_error("prepare campaign rolling usage projection"))?;
        let rolling_usage = statement
            .query_map(params![campaign_id, now], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(database_error("query campaign rolling usage projection"))?
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map_err(database_error("read campaign rolling usage projection"))?;
        let unreconciled_count = experiment_counts.get("unreconciled").copied().unwrap_or(0);
        Ok(Some(CampaignStatusProjection {
            campaign_id,
            state,
            state_reason,
            objective_digest,
            next_eligible_at,
            experiment_counts,
            rolling_usage,
            unreconciled_count,
            current_best_experiment_id,
            plateau_count,
            primary_metric_name,
            primary_metric_value,
            has_objective,
        }))
    }

    pub fn doctor_projection_for_project(
        &self,
        project_id: &str,
    ) -> Result<CampaignDoctorProjection, AppError> {
        let connection = self.db.connect()?;
        let live_campaign_count = connection
            .query_row(
                "SELECT COUNT(*) FROM campaigns
                 WHERE project_id = ?1 AND state <> 'retired'",
                [project_id],
                |row| row.get(0),
            )
            .map_err(database_error("count live campaigns for doctor"))?;
        let objective_digest = connection
            .query_row(
                "SELECT objective_digest FROM campaigns
                 WHERE project_id = ?1 AND state <> 'retired'
                 ORDER BY created_at DESC, campaign_id DESC LIMIT 1",
                [project_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(database_error("read live campaign objective digest for doctor"))?;
        let baseline_linkage_errors = connection
            .query_row(
                "SELECT COUNT(*) FROM campaigns AS campaign
                 WHERE campaign.project_id = ?1 AND campaign.state <> 'retired'
                   AND (campaign.baseline_experiment_id IS NULL OR NOT EXISTS (
                       SELECT 1 FROM experiments AS experiment
                       JOIN proposals AS proposal
                         ON proposal.proposal_id = experiment.proposal_id
                        AND proposal.campaign_id = experiment.campaign_id
                       WHERE experiment.experiment_id = campaign.baseline_experiment_id
                         AND experiment.campaign_id = campaign.campaign_id
                         AND proposal.source_experiment_id IS NULL
                   ))",
                [project_id],
                |row| row.get(0),
            )
            .map_err(database_error("check campaign baseline linkage"))?;
        let orphan_reservations = connection
            .query_row(
                "SELECT COUNT(*)
                 FROM budget_reservations AS reservation
                 JOIN campaigns AS campaign ON campaign.campaign_id = reservation.campaign_id
                 WHERE campaign.project_id = ?1 AND (
                     (reservation.dimension = 'experiment' AND NOT EXISTS (
                         SELECT 1 FROM experiments AS experiment
                         WHERE experiment.experiment_id = reservation.experiment_id
                           AND experiment.campaign_id = reservation.campaign_id
                     ))
                     OR (reservation.dimension = 'code_change' AND NOT EXISTS (
                         SELECT 1 FROM proposals AS proposal
                         WHERE proposal.proposal_id = reservation.subject_key
                           AND proposal.campaign_id = reservation.campaign_id
                     ))
                 )",
                [project_id],
                |row| row.get(0),
            )
            .map_err(database_error("check orphan campaign reservations"))?;
        let task_identity_disagreements = connection
            .query_row(
                "SELECT COUNT(*)
                 FROM experiments AS experiment
                 JOIN campaigns AS campaign ON campaign.campaign_id = experiment.campaign_id
                 JOIN submissions AS submission
                   ON submission.submission_id = experiment.submission_id
                 WHERE campaign.project_id = ?1
                   AND (experiment.pueue_task_id IS NOT submission.pueue_task_id
                        OR experiment.task_signature IS NOT submission.task_signature)",
                [project_id],
                |row| row.get(0),
            )
            .map_err(database_error("check campaign task identity agreement"))?;
        let submission_boundary_count = connection
            .query_row(
                "SELECT COUNT(*)
                 FROM experiments AS experiment
                 JOIN campaigns AS campaign ON campaign.campaign_id = experiment.campaign_id
                 WHERE campaign.project_id = ?1
                   AND experiment.status IN ('submitting','unreconciled')",
                [project_id],
                |row| row.get(0),
            )
            .map_err(database_error("count campaign submission boundaries"))?;
        let budget_wake_errors = connection
            .query_row(
                "SELECT COUNT(*) FROM campaigns
                 WHERE project_id = ?1 AND state = 'budget_waiting'
                   AND next_eligible_at IS NULL",
                [project_id],
                |row| row.get(0),
            )
            .map_err(database_error("check campaign budget wake times"))?;
        Ok(CampaignDoctorProjection {
            live_campaign_count,
            objective_digest,
            baseline_linkage_errors,
            orphan_reservations,
            task_identity_disagreements,
            submission_boundary_count,
            budget_wake_errors,
        })
    }

    pub fn status_for_project(
        &self,
        project_id: &str,
    ) -> Result<
        (
            Campaign,
            i64,
            BTreeMap<String, i64>,
            BTreeMap<String, i64>,
            Vec<i64>,
        ),
        AppError,
    > {
        let connection = self.db.connect()?;
        let campaign = find_latest_campaign_by_project(&connection, project_id)?
            .ok_or_else(|| validation_error("campaign", "the project has no campaign"))?;
        let proposal_count = connection
            .query_row(
                "SELECT COUNT(*) FROM proposals WHERE campaign_id = ?1",
                [&campaign.campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error("count campaign proposals for status"))?;
        let experiment_counts = grouped_campaign_counts(
            &connection,
            "SELECT status, COUNT(*) FROM experiments WHERE campaign_id = ?1 GROUP BY status",
            &campaign.campaign_id,
            "count campaign experiments for status",
        )?;
        let budget_usage = grouped_campaign_counts(
            &connection,
            "SELECT status, COUNT(*) FROM budget_reservations WHERE campaign_id = ?1 GROUP BY status",
            &campaign.campaign_id,
            "count campaign budget reservations for status",
        )?;
        let mut statement = connection
            .prepare(
                "SELECT pueue_task_id FROM experiments
                 WHERE campaign_id = ?1 AND pueue_task_id IS NOT NULL
                 ORDER BY pueue_task_id DESC, experiment_id DESC LIMIT ?2",
            )
            .map_err(database_error("prepare campaign task IDs for status"))?;
        let task_ids = statement
            .query_map(
                params![campaign.campaign_id, MAX_STATUS_TASK_IDS],
                |row| row.get(0),
            )
            .map_err(database_error("query campaign task IDs for status"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read campaign task IDs for status"))?;
        Ok((
            campaign,
            proposal_count,
            experiment_counts,
            budget_usage,
            task_ids,
        ))
    }

    pub fn pause(&self, project_id: &str, now: i64) -> Result<Campaign, AppError> {
        let _admission = super::repositories::acquire_project_lifecycle_admission(
            self.db,
            project_id,
            "acquire campaign pause admission lock",
        )?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin campaign pause transition"))?;
        let campaign = read_latest_campaign_by_project(&transaction, project_id)?;
        if campaign.state == CampaignState::Paused {
            return Ok(campaign);
        }
        if !matches!(
            campaign.state,
            CampaignState::Active | CampaignState::BudgetWaiting | CampaignState::Degraded
        ) {
            return Err(validation_error(
                "campaign",
                "state does not permit an operator pause",
            ));
        }
        update_campaign_state(
            &transaction,
            &campaign.campaign_id,
            CampaignState::Paused,
            Some("operator_paused"),
            now,
            "pause campaign",
        )?;
        let stored = read_campaign(&transaction, &campaign.campaign_id)?;
        transaction
            .commit()
            .map_err(database_error("commit campaign pause transition"))?;
        Ok(stored)
    }

    pub fn review_accept(
        &self,
        project_id: &str,
        note: Option<&str>,
        now: i64,
    ) -> Result<Campaign, AppError> {
        if let Some(note) = note {
            validate_review_note(note)?;
        }
        let _admission = super::repositories::acquire_project_lifecycle_admission(
            self.db,
            project_id,
            "acquire campaign review accept admission lock",
        )?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin campaign review accept transition"))?;
        let campaign = read_latest_campaign_by_project(&transaction, project_id)?;
        if campaign.state == CampaignState::Retired
            && campaign.state_reason.as_deref() == Some("goal_accepted")
        {
            transaction
                .commit()
                .map_err(database_error("commit idempotent campaign review accept"))?;
            return Ok(campaign);
        }
        if campaign.state != CampaignState::GoalReachedPendingReview {
            return Err(validation_error(
                "campaign",
                "only a goal-reached pending-review campaign can be accepted",
            ));
        }
        let (project_id_val, pueue_group): (String, String) = transaction
            .query_row(
                "SELECT project_id, pueue_group FROM projects WHERE project_id = ?1",
                [project_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(database_error("read project for campaign review accept"))?
            .ok_or_else(|| {
                validation_error("project_id", "does not identify a registered project")
            })?;
        let nonterminal_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM experiments
                  WHERE campaign_id = ?1
                    AND (status NOT IN ('succeeded','failed','cancelled')
                         OR failure_code = 'termination_unknown')",
                [&campaign.campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error("check campaign experiments before review accept"))?;
        if nonterminal_count != 0 {
            return Err(validation_error(
                "campaign",
                "cannot accept while experiments are nonterminal or termination is unknown",
            ));
        }
        let reserved_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM budget_reservations
                  WHERE campaign_id = ?1 AND status = 'reserved'",
                [&campaign.campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error(
                "check campaign reservations before review accept",
            ))?;
        if reserved_count != 0 {
            return Err(validation_error(
                "campaign",
                "cannot accept while budget reservations remain reserved",
            ));
        }
        // Retire with goal_accepted reason atomically with operator log.
        update_campaign_state(
            &transaction,
            &campaign.campaign_id,
            CampaignState::Retired,
            Some("goal_accepted"),
            now,
            "accept campaign goal review",
        )?;
        let mut details = serde_json::json!({
            "campaign_id": campaign.campaign_id,
            "review": "accept",
            "reason": "goal_accepted",
        });
        if let Some(note) = note {
            details["note"] = serde_json::Value::String(crate::output::bounded_redacted_text(note));
        }
        insert_review_operator_log(
            &transaction,
            &project_id_val,
            &pueue_group,
            "halt",
            &details,
            now,
        )?;
        let stored = read_campaign(&transaction, &campaign.campaign_id)?;
        transaction
            .commit()
            .map_err(database_error("commit campaign review accept transition"))?;
        Ok(stored)
    }

    pub fn review_reject(
        &self,
        project_id: &str,
        note: Option<&str>,
        now: i64,
    ) -> Result<Campaign, AppError> {
        if let Some(note) = note {
            validate_review_note(note)?;
        }
        let _admission = super::repositories::acquire_project_lifecycle_admission(
            self.db,
            project_id,
            "acquire campaign review reject admission lock",
        )?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin campaign review reject transition"))?;
        let campaign = read_latest_campaign_by_project(&transaction, project_id)?;
        if campaign.state == CampaignState::Active
            && campaign.state_reason.as_deref() == Some("goal_claim_rejected")
        {
            transaction
                .commit()
                .map_err(database_error("commit idempotent campaign review reject"))?;
            return Ok(campaign);
        }
        if campaign.state != CampaignState::GoalReachedPendingReview {
            return Err(validation_error(
                "campaign",
                "only a goal-reached pending-review campaign can be rejected",
            ));
        }
        validate_project_available(&transaction, project_id)?;
        let unsafe_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM experiments
                  WHERE campaign_id = ?1
                    AND (status = 'unreconciled' OR failure_code = 'termination_unknown')",
                [&campaign.campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error(
                "check campaign reconciliation before review reject",
            ))?;
        if unsafe_count != 0 {
            return Err(validation_error(
                "campaign",
                "cannot reject while reconciliation or termination state is unknown",
            ));
        }
        let (project_id_val, pueue_group): (String, String) = transaction
            .query_row(
                "SELECT project_id, pueue_group FROM projects WHERE project_id = ?1",
                [project_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(database_error("read project for campaign review reject"))?
            .ok_or_else(|| {
                validation_error("project_id", "does not identify a registered project")
            })?;
        // Identify the unique current completed goal_reached claimant by joining
        // decision_cycles to the exact campaign_decision event with matching
        // dedup/campaign/source lineage and completed status. Fail closed unless
        // exactly one such claimant exists.
        let (cycle_id, source_experiment_id, event_id, payload_json) = {
            let mut statement = transaction
                .prepare(
                    "SELECT dc.cycle_id, dc.source_experiment_id, ev.event_id, ev.payload_json
                      FROM decision_cycles dc
                      JOIN events ev
                        ON ev.project_id = ?1
                       AND ev.dedup_key = 'campaign-decision:v1:' || dc.cycle_id
                       AND ev.campaign_id = dc.campaign_id
                       AND ev.experiment_id = dc.source_experiment_id
                       AND ev.kind = 'campaign_decision'
                      WHERE dc.campaign_id = ?2
                        AND dc.state = 'completed'
                        AND dc.last_decision_kind = 'goal_reached'
                        AND ev.status = 'completed'",
                )
                .map_err(database_error(
                    "find claiming goal claimants for review reject",
                ))?;
            let rows = statement
                .query_map(params![project_id, campaign.campaign_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(database_error(
                    "query claiming goal claimants for review reject",
                ))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(database_error(
                    "read claiming goal claimants for review reject",
                ))?;
            if rows.len() != 1 {
                return Err(validation_error(
                    "campaign_decision_event",
                    "must have exactly one completed goal_reached claimant with a matching completed campaign_decision event",
                ));
            }
            rows.into_iter().next().unwrap()
        };
        // Validate payload lineage exactly matches cycle and source.
        let payload: serde_json::Value =
            serde_json::from_str(&payload_json).map_err(|source| AppError::Serialization {
                operation: "parse claiming decision event payload",
                source,
            })?;
        if payload.get("source").and_then(|value| value.as_str()) != Some("terminal_experiment")
            || payload.get("cycle_id").and_then(|value| value.as_str()) != Some(cycle_id.as_str())
            || payload
                .get("source_experiment_id")
                .and_then(|value| value.as_str())
                != Some(source_experiment_id.as_str())
        {
            return Err(validation_error(
                "campaign_decision_event",
                "payload lineage does not match the claiming goal cycle",
            ));
        }
        // Atomically reactivate campaign and dead-letter that exact event.
        update_campaign_state(
            &transaction,
            &campaign.campaign_id,
            CampaignState::Active,
            Some("goal_claim_rejected"),
            now,
            "reject campaign goal review",
        )?;
        // Preserve completed_at: do not clear it.
        let updated = transaction
            .execute(
                "UPDATE events SET status = 'dead_letter', last_error = 'goal_claim_rejected' WHERE event_id = ?1 AND status = 'completed'",
                [event_id],
            )
            .map_err(database_error("dead-letter claiming decision event"))?;
        if updated != 1 {
            return Err(validation_error(
                "campaign_decision_event",
                "status changed during review rejection",
            ));
        }
        let mut details = serde_json::json!({
            "campaign_id": campaign.campaign_id,
            "review": "reject",
            "reason": "goal_claim_rejected",
        });
        if let Some(note) = note {
            details["note"] = serde_json::Value::String(crate::output::bounded_redacted_text(note));
        }
        insert_review_operator_log(
            &transaction,
            &project_id_val,
            &pueue_group,
            "resume",
            &details,
            now,
        )?;
        let stored = read_campaign(&transaction, &campaign.campaign_id)?;
        transaction
            .commit()
            .map_err(database_error("commit campaign review reject transition"))?;
        Ok(stored)
    }

    pub fn resume(&self, project_id: &str, now: i64) -> Result<Campaign, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin campaign resume transition"))?;
        let campaign = read_latest_campaign_by_project(&transaction, project_id)?;
        if !matches!(
            campaign.state,
            CampaignState::Active
                | CampaignState::Paused
                | CampaignState::BudgetWaiting
                | CampaignState::Degraded
        ) {
            return Err(validation_error(
                "campaign",
                "state does not permit an operator resume",
            ));
        }
        validate_project_available(&transaction, project_id)?;
        let unsafe_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM experiments
                 WHERE campaign_id = ?1
                   AND (status = 'unreconciled' OR failure_code = 'termination_unknown')",
                [&campaign.campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error("check campaign reconciliation before resume"))?;
        if unsafe_count != 0 {
            return Err(validation_error(
                "campaign",
                "cannot resume while reconciliation or termination state is unknown",
            ));
        }
        let has_degraded_decision_cycle: bool = transaction
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM decision_cycles
                    WHERE campaign_id = ?1 AND state = 'degraded'
                 )",
                [&campaign.campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error(
                "check exhausted decision cycles before campaign resume",
            ))?;
        if has_degraded_decision_cycle {
            update_campaign_state(
                &transaction,
                &campaign.campaign_id,
                CampaignState::Degraded,
                Some("decision_attempts_exhausted"),
                now,
                "preserve exhausted decision degradation during campaign resume",
            )?;
            let stored = read_campaign(&transaction, &campaign.campaign_id)?;
            transaction.commit().map_err(database_error(
                "commit degraded campaign resume transition",
            ))?;
            return Ok(stored);
        }
        if campaign.state == CampaignState::Active {
            return Ok(campaign);
        }
        update_campaign_state(
            &transaction,
            &campaign.campaign_id,
            CampaignState::Active,
            Some("operator_resumed"),
            now,
            "resume campaign",
        )?;
        let stored = read_campaign(&transaction, &campaign.campaign_id)?;
        transaction
            .commit()
            .map_err(database_error("commit campaign resume transition"))?;
        Ok(stored)
    }

    pub fn retire(&self, project_id: &str, now: i64) -> Result<Campaign, AppError> {
        let _admission = super::repositories::acquire_project_lifecycle_admission(
            self.db,
            project_id,
            "acquire campaign retire admission lock",
        )?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin campaign retire transition"))?;
        let campaign = read_latest_campaign_by_project(&transaction, project_id)?;
        if campaign.state == CampaignState::Retired {
            return Ok(campaign);
        }
        if !matches!(
            campaign.state,
            CampaignState::Active | CampaignState::Paused
        ) {
            return Err(validation_error(
                "campaign",
                "state does not permit operator retirement",
            ));
        }
        let nonterminal_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM experiments
                 WHERE campaign_id = ?1
                   AND (status NOT IN ('succeeded','failed','cancelled')
                        OR failure_code = 'termination_unknown')",
                [&campaign.campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error("check campaign experiments before retire"))?;
        if nonterminal_count != 0 {
            return Err(validation_error(
                "campaign",
                "cannot retire while experiments are nonterminal or termination is unknown",
            ));
        }
        let reserved_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM budget_reservations
                 WHERE campaign_id = ?1 AND status = 'reserved'",
                [&campaign.campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error("check campaign reservations before retire"))?;
        if reserved_count != 0 {
            return Err(validation_error(
                "campaign",
                "cannot retire while budget reservations remain reserved",
            ));
        }
        update_campaign_state(
            &transaction,
            &campaign.campaign_id,
            CampaignState::Retired,
            Some("operator_retired"),
            now,
            "retire campaign",
        )?;
        let stored = read_campaign(&transaction, &campaign.campaign_id)?;
        transaction
            .commit()
            .map_err(database_error("commit campaign retire transition"))?;
        Ok(stored)
    }
}

impl<'db> ProposalRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn find_by_id(&self, proposal_id: &str) -> Result<Option<Proposal>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{PROPOSAL_SELECT} WHERE proposal_id = ?1"),
                [proposal_id],
                proposal_from_row,
            )
            .optional()
            .map_err(database_error("find campaign proposal by ID"))
    }

    pub fn list_for_campaign(
        &self,
        campaign_id: &str,
        limit: usize,
    ) -> Result<Vec<Proposal>, AppError> {
        validate_inspection_limit(limit)?;
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{PROPOSAL_SELECT} WHERE campaign_id = ?1
                 ORDER BY created_at DESC, proposal_id DESC LIMIT ?2"
            ))
            .map_err(database_error("prepare scoped campaign proposal list"))?;
        let proposals = statement
            .query_map(params![campaign_id, limit as i64], proposal_from_row)
            .map_err(database_error("query scoped campaign proposal list"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read scoped campaign proposal list"))?;
        Ok(proposals)
    }

    pub fn find_for_campaign(
        &self,
        campaign_id: &str,
        proposal_id: &str,
    ) -> Result<Option<Proposal>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{PROPOSAL_SELECT} WHERE campaign_id = ?1 AND proposal_id = ?2"),
                params![campaign_id, proposal_id],
                proposal_from_row,
            )
            .optional()
            .map_err(database_error("find scoped campaign proposal by ID"))
    }

    pub fn find_by_digest(
        &self,
        campaign_id: &str,
        canonical_digest: &str,
    ) -> Result<Option<Proposal>, AppError> {
        let connection = self.db.connect()?;
        find_proposal_by_digest(&connection, campaign_id, canonical_digest)
    }

}

impl<'db> ExperimentRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn find_by_id(&self, experiment_id: &str) -> Result<Option<Experiment>, AppError> {
        let connection = self.db.connect()?;
        find_experiment(&connection, experiment_id)
    }

    pub fn list_for_campaign(
        &self,
        campaign_id: &str,
        limit: usize,
    ) -> Result<Vec<Experiment>, AppError> {
        validate_inspection_limit(limit)?;
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{EXPERIMENT_SELECT} WHERE campaign_id = ?1
                 ORDER BY created_at DESC, experiment_id DESC LIMIT ?2"
            ))
            .map_err(database_error("prepare scoped campaign experiment list"))?;
        let experiments = statement
            .query_map(params![campaign_id, limit as i64], experiment_from_row)
            .map_err(database_error("query scoped campaign experiment list"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read scoped campaign experiment list"))?;
        Ok(experiments)
    }

    pub fn list_terminal_for_campaign(
        &self,
        campaign_id: &str,
        limit: usize,
    ) -> Result<Vec<Experiment>, AppError> {
        validate_inspection_limit(limit)?;
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{EXPERIMENT_SELECT} WHERE campaign_id = ?1
                   AND status IN ('succeeded','failed','cancelled')
                 ORDER BY finished_at DESC, experiment_id DESC LIMIT ?2"
            ))
            .map_err(database_error(
                "prepare terminal campaign experiment list",
            ))?;
        let experiments = statement
            .query_map(params![campaign_id, limit as i64], experiment_from_row)
            .map_err(database_error("query terminal campaign experiment list"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read terminal campaign experiment list"))?;
        Ok(experiments)
    }

    pub fn inspect_for_campaign(
        &self,
        campaign_id: &str,
        experiment_id: &str,
    ) -> Result<Option<(Experiment, String)>, AppError> {
        let connection = self.db.connect()?;
        let experiment = connection
            .query_row(
                &format!("{EXPERIMENT_SELECT} WHERE campaign_id = ?1 AND experiment_id = ?2"),
                params![campaign_id, experiment_id],
                experiment_from_row,
            )
            .optional()
            .map_err(database_error("find scoped campaign experiment by ID"))?;
        let Some(experiment) = experiment else {
            return Ok(None);
        };
        let argv_json: String = connection
            .query_row(
                "SELECT argv_json FROM proposals
                 WHERE campaign_id = ?1 AND proposal_id = ?2",
                params![campaign_id, experiment.proposal_id],
                |row| row.get(0),
            )
            .map_err(database_error("read scoped experiment argv digest source"))?;
        Ok(Some((
            experiment,
            format!("{:x}", Sha256::digest(argv_json.as_bytes())),
        )))
    }

    pub fn find_by_submission_id(
        &self,
        submission_id: &str,
    ) -> Result<Option<Experiment>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{EXPERIMENT_SELECT} WHERE submission_id = ?1"),
                [submission_id],
                experiment_from_row,
            )
            .optional()
            .map_err(database_error("find experiment by submission ID"))
    }

    pub fn mark_submitting(&self, experiment_id: &str, now: i64) -> Result<Experiment, AppError> {
        self.begin_submitting_or_defer(experiment_id, now)?
            .ok_or_else(|| {
                validation_error(
                    "campaign",
                    "campaign and project authority must permit reserved submission",
                )
            })
    }

    pub(crate) fn begin_submitting_or_defer(
        &self,
        experiment_id: &str,
        now: i64,
    ) -> Result<Option<Experiment>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin experiment submitting transition"))?;
        let experiment = read_experiment(&transaction, experiment_id)?;
        if experiment.status != ExperimentStatus::Reserved {
            return Err(validation_error(
                "experiment",
                "only a reserved experiment can begin submission",
            ));
        }
        let campaign = read_campaign(&transaction, &experiment.campaign_id)?;
        let submission = read_submission(&transaction, &experiment.submission_id)?;
        if submission.status != SubmissionStatus::Pending
            || submission.pueue_task_id.is_some()
            || submission.task_signature.is_some()
        {
            return Err(validation_error(
                "submission",
                "reserved experiment submission identity is inconsistent",
            ));
        }
        let project_available = project_is_available(&transaction, &campaign.project_id)?;
        if campaign.state == CampaignState::BudgetWaiting && campaign.next_eligible_at.is_none() {
            return Err(validation_error(
                "campaign.next_eligible_at",
                "budget-waiting campaign must have a finite wake time",
            ));
        }
        if campaign.state != CampaignState::Active || !project_available {
            transaction
                .commit()
                .map_err(database_error("commit deferred experiment submission"))?;
            return Ok(None);
        }
        transaction
            .execute(
                "UPDATE experiments SET status = ?1, updated_at = ?2 WHERE experiment_id = ?3",
                params![ExperimentStatus::Submitting, now, experiment_id],
            )
            .map_err(database_error("mark experiment submitting"))?;
        let stored = read_experiment(&transaction, experiment_id)?;
        transaction
            .commit()
            .map_err(database_error("commit experiment submitting transition"))?;
        Ok(Some(stored))
    }

    pub fn mark_accepted(
        &self,
        experiment_id: &str,
        task_id: i64,
        task_signature: &str,
        now: i64,
    ) -> Result<Experiment, AppError> {
        validate_task_identity(task_id, task_signature)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin experiment acceptance transition"))?;
        let experiment = read_experiment(&transaction, experiment_id)?;
        let submission = read_submission(&transaction, &experiment.submission_id)?;
        if experiment.status == ExperimentStatus::Accepted {
            if experiment.pueue_task_id == Some(task_id)
                && experiment.task_signature.as_deref() == Some(task_signature)
                && submission.status == SubmissionStatus::Accepted
                && submission.pueue_task_id == Some(task_id)
                && submission.task_signature.as_deref() == Some(task_signature)
            {
                return Ok(experiment);
            }
            return Err(validation_error(
                "pueue_task_id",
                "conflicts with the accepted experiment identity",
            ));
        }
        if experiment.status != ExperimentStatus::Submitting {
            return Err(validation_error(
                "experiment",
                "only a submitting experiment can be accepted",
            ));
        }
        if submission.status != SubmissionStatus::Pending
            || submission.pueue_task_id.is_some()
            || submission.task_signature.is_some()
        {
            return Err(validation_error(
                "submission",
                "submitting experiment submission identity is inconsistent",
            ));
        }

        transaction
            .execute(
                "UPDATE submissions
                 SET pueue_task_id = ?1, task_signature = ?2, status = ?3
                 WHERE submission_id = ?4",
                params![
                    task_id,
                    task_signature,
                    SubmissionStatus::Accepted,
                    submission.submission_id,
                ],
            )
            .map_err(database_error("bind accepted campaign submission"))?;
        transaction
            .execute(
                "UPDATE experiments
                 SET status = ?1, pueue_task_id = ?2, task_signature = ?3, updated_at = ?4
                 WHERE experiment_id = ?5",
                params![
                    ExperimentStatus::Accepted,
                    task_id,
                    task_signature,
                    now,
                    experiment_id,
                ],
            )
            .map_err(database_error("mark experiment accepted"))?;
        let stored = read_experiment(&transaction, experiment_id)?;
        transaction
            .commit()
            .map_err(database_error("commit experiment acceptance transition"))?;
        Ok(stored)
    }

    pub fn mark_unreconciled(
        &self,
        experiment_id: &str,
        reason_code: &'static str,
        now: i64,
    ) -> Result<Experiment, AppError> {
        validate_failure_field("reason_code", reason_code)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin experiment unreconciled transition"))?;
        let experiment = read_experiment(&transaction, experiment_id)?;
        let submission = read_submission(&transaction, &experiment.submission_id)?;
        if experiment.status == ExperimentStatus::Unreconciled {
            if experiment.failure_code.as_deref() == Some(reason_code)
                && submission.status == SubmissionStatus::Unreconciled
                && experiment.pueue_task_id.is_none()
                && experiment.task_signature.is_none()
                && submission.pueue_task_id.is_none()
                && submission.task_signature.is_none()
            {
                return Ok(experiment);
            }
            return Err(validation_error(
                "reason_code",
                "conflicts with the unreconciled experiment state",
            ));
        }
        if experiment.status != ExperimentStatus::Submitting {
            return Err(validation_error(
                "experiment",
                "only a submitting experiment can become unreconciled",
            ));
        }
        if submission.status != SubmissionStatus::Pending
            || submission.pueue_task_id.is_some()
            || submission.task_signature.is_some()
        {
            return Err(validation_error(
                "submission",
                "submitting experiment submission identity is inconsistent",
            ));
        }

        transaction
            .execute(
                "UPDATE submissions SET status = ?1 WHERE submission_id = ?2",
                params![SubmissionStatus::Unreconciled, submission.submission_id],
            )
            .map_err(database_error("mark campaign submission unreconciled"))?;
        transaction
            .execute(
                "UPDATE experiments
                 SET status = ?1, failure_code = ?2, updated_at = ?3
                 WHERE experiment_id = ?4",
                params![
                    ExperimentStatus::Unreconciled,
                    reason_code,
                    now,
                    experiment_id,
                ],
            )
            .map_err(database_error("mark experiment unreconciled"))?;
        let stored = read_experiment(&transaction, experiment_id)?;
        transaction
            .commit()
            .map_err(database_error("commit experiment unreconciled transition"))?;
        Ok(stored)
    }

    pub fn quarantine_accepted_identity(
        &self,
        experiment_id: &str,
        reason_code: &'static str,
        now: i64,
    ) -> Result<Experiment, AppError> {
        self.quarantine_accepted_identities(&[experiment_id.to_owned()], reason_code, now)?
            .into_iter()
            .next()
            .ok_or_else(|| validation_error("experiment_id", "must identify one experiment"))
    }

    pub fn quarantine_accepted_identities(
        &self,
        experiment_ids: &[String],
        reason_code: &'static str,
        now: i64,
    ) -> Result<Vec<Experiment>, AppError> {
        validate_failure_field("reason_code", reason_code)?;
        if experiment_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin accepted identity batch quarantine"))?;
        let mut identities = Vec::with_capacity(experiment_ids.len());
        for experiment_id in experiment_ids {
            let experiment = read_experiment(&transaction, experiment_id)?;
            let submission = read_submission(&transaction, &experiment.submission_id)?;
            let already_quarantined = experiment.status == ExperimentStatus::Unreconciled
                && experiment.failure_code.as_deref() == Some(reason_code)
                && submission.status == SubmissionStatus::Unreconciled;
            if !already_quarantined
                && (experiment.status != ExperimentStatus::Accepted
                    || submission.status != SubmissionStatus::Accepted
                    || experiment.pueue_task_id.is_none()
                    || experiment.task_signature.is_none()
                    || submission.pueue_task_id != experiment.pueue_task_id
                    || submission.task_signature != experiment.task_signature)
            {
                return Err(validation_error(
                    "experiment",
                    "only consistently accepted identities can be quarantined",
                ));
            }
            identities.push((experiment, submission, already_quarantined));
        }
        for (experiment, submission, already_quarantined) in &identities {
            if *already_quarantined {
                continue;
            }
            transaction
                .execute(
                    "UPDATE submissions SET status = ?1 WHERE submission_id = ?2",
                    params![SubmissionStatus::Unreconciled, submission.submission_id],
                )
                .map_err(database_error("quarantine accepted campaign submission"))?;
            transaction
                .execute(
                    "UPDATE experiments
                     SET status = ?1, failure_code = ?2, updated_at = ?3
                     WHERE experiment_id = ?4",
                    params![
                        ExperimentStatus::Unreconciled,
                        reason_code,
                        now,
                        experiment.experiment_id,
                    ],
                )
                .map_err(database_error("quarantine accepted experiment identity"))?;
        }
        let stored = experiment_ids
            .iter()
            .map(|experiment_id| read_experiment(&transaction, experiment_id))
            .collect::<Result<Vec<_>, _>>()?;
        transaction
            .commit()
            .map_err(database_error("commit accepted identity batch quarantine"))?;
        Ok(stored)
    }

    pub fn project_terminal_submission(
        &self,
        experiment_id: &str,
        task_id: i64,
        outcome: ExperimentTerminalOutcome<'_>,
        now: i64,
    ) -> Result<Experiment, AppError> {
        let (status, failure_code, failure_fingerprint) = match outcome {
            ExperimentTerminalOutcome::Succeeded => (ExperimentStatus::Succeeded, None, None),
            ExperimentTerminalOutcome::Failed {
                failure_code,
                failure_fingerprint,
            } => {
                validate_failure_field("failure_code", failure_code)?;
                validate_failure_field("failure_fingerprint", failure_fingerprint)?;
                (
                    ExperimentStatus::Failed,
                    Some(failure_code),
                    Some(failure_fingerprint),
                )
            }
            ExperimentTerminalOutcome::Cancelled => (ExperimentStatus::Cancelled, None, None),
        };
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin terminal experiment projection"))?;
        let experiment = read_experiment(&transaction, experiment_id)?;
        let submission = read_submission(&transaction, &experiment.submission_id)?;
        if experiment.pueue_task_id != Some(task_id)
            || experiment.task_signature.is_none()
            || submission.status != SubmissionStatus::Accepted
            || submission.pueue_task_id != experiment.pueue_task_id
            || submission.task_signature != experiment.task_signature
        {
            return Err(validation_error(
                "pueue_task_id",
                "does not match the experiment and submission task identity",
            ));
        }
        if is_terminal(experiment.status) {
            if experiment.status == status
                && experiment.failure_code.as_deref() == failure_code
                && experiment.failure_fingerprint.as_deref() == failure_fingerprint
                && reservation_is_consumed(&transaction, experiment_id)?
            {
                return Ok(experiment);
            }
            return Err(validation_error(
                "experiment",
                "conflicts with the existing terminal result",
            ));
        }
        if experiment.status != ExperimentStatus::Accepted {
            return Err(validation_error(
                "experiment",
                "only an accepted experiment can project a terminal result",
            ));
        }

        transaction
            .execute(
                "UPDATE experiments
                 SET status = ?1, failure_code = ?2, failure_fingerprint = ?3,
                     updated_at = ?4, finished_at = ?4
                 WHERE experiment_id = ?5",
                params![
                    status,
                    failure_code,
                    failure_fingerprint,
                    now,
                    experiment_id,
                ],
            )
            .map_err(database_error("project terminal experiment result"))?;
        let consumed = transaction
            .execute(
                "UPDATE budget_reservations
                 SET status = ?1, updated_at = ?2
                 WHERE experiment_id = ?3 AND dimension = ?4 AND status = ?5",
                params![
                    BudgetReservationStatus::Consumed,
                    now,
                    experiment_id,
                    BudgetDimension::Experiment,
                    BudgetReservationStatus::Reserved,
                ],
            )
            .map_err(database_error("consume terminal experiment reservation"))?;
        if consumed != 1 {
            return Err(validation_error(
                "budget_reservation",
                "experiment reservation is missing or already consumed",
            ));
        }
        let stored = read_experiment(&transaction, experiment_id)?;
        transaction
            .commit()
            .map_err(database_error("commit terminal experiment projection"))?;
        Ok(stored)
    }
}

fn validate_project_available(
    transaction: &Transaction<'_>,
    project_id: &str,
) -> Result<(), AppError> {
    if !project_is_available(transaction, project_id)? {
        return Err(validation_error(
            "project",
            "must be enabled, unpaused, and not halted",
        ));
    }
    Ok(())
}

fn project_is_available(
    transaction: &Transaction<'_>,
    project_id: &str,
) -> Result<bool, AppError> {
    let state = transaction
        .query_row(
            "SELECT enabled, paused, halted_reason FROM projects WHERE project_id = ?1",
            [project_id],
            |row| {
                Ok((
                    row.get::<_, bool>(0)?,
                    row.get::<_, bool>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(database_error("read project state for campaign admission"))?
        .ok_or_else(|| validation_error("project_id", "does not identify a registered project"))?;
    Ok(state.0 && !state.1 && state.2.is_none())
}

fn insert_proposal(
    transaction: &Transaction<'_>,
    proposal_id: &str,
    campaign_id: &str,
    proposal: &ValidatedProposal,
    status: ProposalStatus,
    argv_json: &str,
    evidence_json: &str,
    now: i64,
) -> Result<(), AppError> {
    transaction
        .execute(
            "INSERT INTO proposals (
                proposal_id, campaign_id, kind, status, hypothesis, source_experiment_id,
                argv_json, working_directory, expected_evidence_json, canonical_digest,
                reject_reason, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL, ?11, ?11)",
            params![
                proposal_id,
                campaign_id,
                proposal.kind(),
                status,
                proposal.hypothesis(),
                proposal.source_experiment_id(),
                argv_json,
                proposal.working_directory(),
                evidence_json,
                proposal.canonical_digest(),
                now,
            ],
        )
        .map_err(database_error("insert campaign proposal"))?;
    Ok(())
}

fn insert_submission(
    transaction: &Transaction<'_>,
    submission_id: &str,
    project_id: &str,
    argv_json: &str,
    metadata_json: &str,
    origin_agent_run_id: Option<i64>,
    now: i64,
) -> Result<(), AppError> {
    transaction
        .execute(
            "INSERT INTO submissions (
                submission_id, project_id, argv_json, created_at, pueue_task_id, task_signature,
                status, kind, metadata_json, origin_agent_run_id
             ) VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5, ?6, ?7, ?8)",
            params![
                submission_id,
                project_id,
                argv_json,
                now,
                SubmissionStatus::Pending,
                SubmissionKind::Experiment,
                metadata_json,
                origin_agent_run_id,
            ],
        )
        .map_err(database_error("insert campaign submission intent"))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_experiment(
    transaction: &Transaction<'_>,
    experiment_id: &str,
    campaign_id: &str,
    proposal_id: &str,
    submission_id: &str,
    parent_experiment_id: Option<&str>,
    attempt: i64,
    resume_of_experiment_id: Option<&str>,
    checkpoint_note: Option<&str>,
    code_change_run_id: Option<&str>,
    code_revision_sha: Option<&str>,
    now: i64,
) -> Result<(), AppError> {
    transaction
        .execute(
            "INSERT INTO experiments (
                experiment_id, campaign_id, proposal_id, submission_id, parent_experiment_id,
                attempt, status, pueue_task_id, task_signature, failure_code,
                failure_fingerprint, created_at, updated_at, finished_at,
                resume_of_experiment_id, checkpoint_note, code_change_run_id, code_revision_sha
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, NULL, NULL, ?8, ?8, NULL, ?9, ?10, ?11, ?12)",
            params![
                experiment_id,
                campaign_id,
                proposal_id,
                submission_id,
                parent_experiment_id,
                attempt,
                ExperimentStatus::Reserved,
                now,
                resume_of_experiment_id,
                checkpoint_note,
                code_change_run_id,
                code_revision_sha,
            ],
        )
        .map_err(database_error("insert reserved campaign experiment"))?;
    Ok(())
}

fn insert_experiment_reservation(
    transaction: &Transaction<'_>,
    campaign_id: &str,
    experiment_id: &str,
    now: i64,
    window_ends_at: i64,
) -> Result<(), AppError> {
    transaction
        .execute(
            "INSERT INTO budget_reservations (
                reservation_id, campaign_id, experiment_id, dimension, subject_key, status,
                window_started_at, window_ends_at, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?3, ?5, ?6, ?7, ?6, ?6)",
            params![
                format!("experiment:{experiment_id}"),
                campaign_id,
                experiment_id,
                BudgetDimension::Experiment,
                BudgetReservationStatus::Reserved,
                now,
                window_ends_at,
            ],
        )
        .map_err(database_error("insert experiment budget reservation"))?;
    Ok(())
}

fn insert_code_change_reservation(
    transaction: &Transaction<'_>,
    campaign_id: &str,
    proposal_id: &str,
    now: i64,
    window_ends_at: i64,
) -> Result<(), AppError> {
    transaction
        .execute(
            "INSERT INTO budget_reservations (
                reservation_id, campaign_id, experiment_id, dimension, subject_key, status,
                window_started_at, window_ends_at, created_at, updated_at
             ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, ?6, ?6)",
            params![
                format!("code-change:{proposal_id}"),
                campaign_id,
                BudgetDimension::CodeChange,
                proposal_id,
                BudgetReservationStatus::Consumed,
                now,
                window_ends_at,
            ],
        )
        .map_err(database_error("insert code-change budget reservation"))?;
    Ok(())
}

fn validate_atomic_code_change_run(
    run: &NewCodeChangeRun,
    campaign_id: &str,
    proposal_id: &str,
) -> Result<(), AppError> {
    if run.campaign_id != campaign_id || run.proposal_id != proposal_id {
        return Err(validation_error(
            "code_change.run",
            "must belong to the accepted campaign proposal",
        ));
    }
    validate_code_change_identifier("code_change_run_id", &run.code_change_run_id)?;
    validate_code_change_identifier("worktree_id", &run.worktree_id)?;
    validate_code_change_identifier("worktree_relative_path", &run.worktree_relative_path)?;
    validate_code_change_identifier("candidate_ref", &run.candidate_ref)?;
    validate_code_change_identifier("best_ref", &run.best_ref)?;
    if let Some(editor_session_id) = &run.editor_session_id {
        validate_code_change_identifier("editor_session_id", editor_session_id)?;
    }
    if run.candidate_ref != code_change::candidate_ref(campaign_id, proposal_id)?
        || run.best_ref != code_change::best_ref(campaign_id)?
        || run.worktree_relative_path
            != code_change::owned_worktree_relative_path(campaign_id, proposal_id)?
                .to_string_lossy()
    {
        return Err(validation_error(
            "code_change.run",
            "must use campaign-owned refs and worktree path",
        ));
    }
    super::code_changes::validate_sha("base_sha", &run.base_sha)
}

fn validate_code_change_identifier(field: &'static str, value: &str) -> Result<(), AppError> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(validation_error(
            field,
            "must be non-empty bounded text without control characters",
        ));
    }
    Ok(())
}

fn insert_code_change_run_in_transaction(
    transaction: &Transaction<'_>,
    run: &NewCodeChangeRun,
    project_id: &str,
) -> Result<(), AppError> {
    transaction
        .execute(
            "INSERT INTO code_change_runs (
                code_change_run_id, proposal_id, campaign_id, state, base_sha,
                candidate_sha, candidate_ref, best_ref, worktree_id,
                worktree_relative_path, editor_session_id, editor_attempts,
                diff_digest, changed_file_count, diff_bytes, experiment_id,
                rejection_code, rejection_summary, promotion_outcome,
                promotion_expected_best_experiment_id, promotion_expected_old_sha,
                promotion_target_sha, cleanup_completed_at, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8, ?9, ?10,
                       0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                       NULL, NULL, NULL, ?11, ?11)",
            params![
                run.code_change_run_id,
                run.proposal_id,
                run.campaign_id,
                CodeChangeState::Reserved,
                run.base_sha,
                run.candidate_ref,
                run.best_ref,
                run.worktree_id,
                run.worktree_relative_path,
                run.editor_session_id,
                run.created_at,
            ],
        )
        .map_err(database_error("insert code-change run"))?;
    let event = NewEvent::new(
        project_id.to_owned(),
        EventKind::CodeChange,
        format!(
            "code-change:v1:{}:{}:0",
            run.code_change_run_id,
            CodeChangeState::Reserved
        ),
        serde_json::json!({
            "code_change_run_id": run.code_change_run_id,
            "campaign_id": run.campaign_id,
            "proposal_id": run.proposal_id,
            "state": CodeChangeState::Reserved,
            "attempt": 0,
            "reason_code": null,
            "event_time": run.created_at,
        }),
        run.created_at,
        run.created_at,
    )
    .with_campaign_lineage(run.campaign_id.clone(), Option::<String>::None);
    super::insert_event_completed_in_transaction(transaction, &event)?;
    Ok(())
}

fn reject_code_change_in_transaction(
    transaction: &Transaction<'_>,
    campaign_id: &str,
    proposal_id: &str,
    project_id: &str,
    reason: &str,
    now: i64,
) -> Result<(), AppError> {
    let persisted_reason = bounded_redacted_text(reason);
    let changed = transaction
        .execute(
            "UPDATE proposals SET status = 'rejected', reject_reason = ?1, updated_at = ?2
             WHERE campaign_id = ?3 AND proposal_id = ?4 AND status = 'pending'",
            params![persisted_reason, now, campaign_id, proposal_id],
        )
        .map_err(database_error("reject code-change proposal"))?;
    if changed != 1 {
        return Err(validation_error(
            "proposal.status",
            "pending code-change proposal changed concurrently",
        ));
    }
    let event = NewEvent::new(
        project_id.to_owned(),
        EventKind::CodeChange,
        format!("code-change-proposal:v1:{proposal_id}:rejected"),
        serde_json::json!({
            "campaign_id": campaign_id,
            "proposal_id": proposal_id,
            "state": "rejected",
            "reason_code": persisted_reason,
            "event_time": now,
        }),
        now,
        now,
    )
    .with_campaign_lineage(campaign_id.to_owned(), Option::<String>::None);
    super::insert_event_completed_in_transaction(transaction, &event)?;
    Ok(())
}

fn count_live_reservations(
    transaction: &Transaction<'_>,
    campaign_id: &str,
    dimension: BudgetDimension,
    now: i64,
) -> Result<i64, AppError> {
    transaction
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations
             WHERE campaign_id = ?1 AND dimension = ?2
               AND status IN ('reserved','consumed') AND window_ends_at > ?3",
            params![campaign_id, dimension, now],
            |row| row.get(0),
        )
        .map_err(database_error("count live rolling budget reservations"))
}

/// Total resume repairs across the whole `resume_of_experiment_id` lineage,
/// counted from the origin experiment of `experiment_id` so repeated
/// single-resume generations exhaust a cumulative chain-depth budget.
pub(crate) fn count_live_repair_descendants(
    connection: &rusqlite::Connection,
    experiment_id: &str,
) -> rusqlite::Result<i64> {
    connection.query_row(
        "WITH RECURSIVE ancestry(experiment_id, depth) AS (
             SELECT experiment_id, 0 FROM experiments WHERE experiment_id = ?1
             UNION ALL
             SELECT e.resume_of_experiment_id, ancestry.depth + 1
             FROM experiments e
             JOIN ancestry ON e.experiment_id = ancestry.experiment_id
             WHERE e.resume_of_experiment_id IS NOT NULL
         ),
         origin AS (
             SELECT experiment_id FROM ancestry ORDER BY depth DESC LIMIT 1
         ),
         repairs(experiment_id) AS (
             SELECT experiment_id FROM experiments
              WHERE resume_of_experiment_id = (SELECT experiment_id FROM origin)
             UNION ALL
             SELECT e.experiment_id FROM experiments e
              JOIN repairs ON e.resume_of_experiment_id = repairs.experiment_id
         )
         SELECT COUNT(*) FROM repairs",
        [experiment_id],
        |row| row.get(0),
    )
}

fn earliest_live_reservation_expiry(
    transaction: &Transaction<'_>,
    campaign_id: &str,
    dimension: BudgetDimension,
    now: i64,
) -> Result<Option<i64>, AppError> {
    transaction
        .query_row(
            "SELECT MIN(window_ends_at) FROM budget_reservations
             WHERE campaign_id = ?1 AND dimension = ?2
               AND status IN ('reserved','consumed') AND window_ends_at > ?3",
            params![campaign_id, dimension, now],
            |row| row.get(0),
        )
        .map_err(database_error(
            "find earliest live rolling budget reservation expiry",
        ))
}

fn enter_proposal_budget_wait(
    transaction: Transaction<'_>,
    campaign_id: &str,
    dimension: BudgetDimension,
    reason: &'static str,
    now: i64,
) -> Result<ProposalAcceptance, AppError> {
    let next_eligible_at = earliest_live_reservation_expiry(
        &transaction,
        campaign_id,
        dimension,
        now,
    )?
    .ok_or_else(|| {
        validation_error(
            "campaign.budget",
            "exhausted rolling budget has no finite reservation expiry",
        )
    })?;
    let updated = transaction
        .execute(
            "UPDATE campaigns
             SET state = 'budget_waiting', state_reason = ?1,
                 next_eligible_at = ?2, updated_at = ?3
             WHERE campaign_id = ?4 AND state = 'active'",
            params![reason, next_eligible_at, now, campaign_id],
        )
        .map_err(database_error("wait for campaign proposal budget"))?;
    if updated != 1 {
        return Err(validation_error(
            "campaign",
            "state changed while waiting for proposal budget",
        ));
    }
    transaction
        .commit()
        .map_err(database_error("commit campaign proposal budget wait"))?;
    Ok(ProposalAcceptance::BudgetWaiting { next_eligible_at })
}

fn find_agent_decision_reservation(
    connection: &Connection,
    campaign_id: &str,
    decision_key: &str,
) -> Result<Option<BudgetReservation>, AppError> {
    connection
        .query_row(
            "SELECT reservation_id, campaign_id, experiment_id, dimension, subject_key, status,
                    window_started_at, window_ends_at, created_at, updated_at
             FROM budget_reservations
             WHERE campaign_id = ?1 AND dimension = 'agent_run' AND subject_key = ?2",
            params![campaign_id, decision_key],
            budget_reservation_from_row,
        )
        .optional()
        .map_err(database_error("find campaign agent decision reservation"))
}

fn read_budget_reservation(
    connection: &Connection,
    reservation_id: &str,
) -> Result<BudgetReservation, AppError> {
    connection
        .query_row(
            "SELECT reservation_id, campaign_id, experiment_id, dimension, subject_key, status,
                    window_started_at, window_ends_at, created_at, updated_at
             FROM budget_reservations WHERE reservation_id = ?1",
            [reservation_id],
            budget_reservation_from_row,
        )
        .map_err(database_error("read campaign budget reservation"))
}

fn reservation_is_consumed(
    transaction: &Transaction<'_>,
    experiment_id: &str,
) -> Result<bool, AppError> {
    transaction
        .query_row(
            "SELECT status = 'consumed' FROM budget_reservations
             WHERE experiment_id = ?1 AND dimension = 'experiment'",
            [experiment_id],
            |row| row.get(0),
        )
        .optional()
        .map(|value| value.unwrap_or(false))
        .map_err(database_error("read terminal experiment reservation"))
}

fn find_proposal_by_digest(
    connection: &Connection,
    campaign_id: &str,
    canonical_digest: &str,
) -> Result<Option<Proposal>, AppError> {
    connection
        .query_row(
            &format!(
                "{PROPOSAL_SELECT} WHERE campaign_id = ?1 AND canonical_digest = ?2"
            ),
            params![campaign_id, canonical_digest],
            proposal_from_row,
        )
        .optional()
        .map_err(database_error("find proposal by canonical digest"))
}

fn read_intent_by_proposal(
    connection: &Connection,
    proposal_id: &str,
) -> Result<ManagedSubmissionIntent, AppError> {
    let experiment_id: String = connection
        .query_row(
            "SELECT experiment_id FROM experiments
             WHERE proposal_id = ?1 ORDER BY attempt LIMIT 1",
            [proposal_id],
            |row| row.get(0),
        )
        .map_err(database_error("read accepted proposal experiment"))?;
    read_intent_by_experiment(connection, &experiment_id)
}

fn read_intent_by_experiment(
    connection: &Connection,
    experiment_id: &str,
) -> Result<ManagedSubmissionIntent, AppError> {
    let experiment = read_experiment(connection, experiment_id)?;
    Ok(ManagedSubmissionIntent {
        campaign: read_campaign(connection, &experiment.campaign_id)?,
        proposal: read_proposal(connection, &experiment.proposal_id)?,
        submission: read_submission(connection, &experiment.submission_id)?,
        experiment,
    })
}

fn find_campaign(connection: &Connection, campaign_id: &str) -> Result<Option<Campaign>, AppError> {
    connection
        .query_row(
            &format!("{CAMPAIGN_SELECT} WHERE campaign_id = ?1"),
            [campaign_id],
            campaign_from_row,
        )
        .optional()
        .map_err(database_error("find campaign by ID"))
}

fn find_latest_campaign_by_project(
    connection: &Connection,
    project_id: &str,
) -> Result<Option<Campaign>, AppError> {
    connection
        .query_row(
            &format!(
                "{CAMPAIGN_SELECT} WHERE project_id = ?1
                 ORDER BY (state = 'retired') ASC, created_at DESC, campaign_id DESC LIMIT 1"
            ),
            [project_id],
            campaign_from_row,
        )
        .optional()
        .map_err(database_error("find latest campaign by project"))
}

fn read_latest_campaign_by_project(
    connection: &Connection,
    project_id: &str,
) -> Result<Campaign, AppError> {
    find_latest_campaign_by_project(connection, project_id)?
        .ok_or_else(|| validation_error("campaign", "the project has no campaign"))
}

fn grouped_campaign_counts(
    connection: &Connection,
    sql: &str,
    campaign_id: &str,
    operation: &'static str,
) -> Result<BTreeMap<String, i64>, AppError> {
    let mut statement = connection.prepare(sql).map_err(database_error(operation))?;
    let counts = statement
        .query_map([campaign_id], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(database_error(operation))?
        .collect::<Result<BTreeMap<_, _>, _>>()
        .map_err(database_error(operation))?;
    Ok(counts)
}

fn update_campaign_state(
    transaction: &Transaction<'_>,
    campaign_id: &str,
    state: CampaignState,
    state_reason: Option<&str>,
    now: i64,
    operation: &'static str,
) -> Result<(), AppError> {
    let updated = transaction
        .execute(
            "UPDATE campaigns
             SET state = ?1, state_reason = ?2, next_eligible_at = NULL, updated_at = ?3
             WHERE campaign_id = ?4",
            params![state, state_reason, now, campaign_id],
        )
        .map_err(database_error(operation))?;
    if updated != 1 {
        return Err(validation_error(
            "campaign",
            "state changed concurrently during operator transition",
        ));
    }
    Ok(())
}

fn validate_review_note(note: &str) -> Result<(), AppError> {
    if note.len() > crate::decision_protocol::MAX_REVIEW_NOTE_BYTES
        || note.chars().any(char::is_control)
    {
        return Err(validation_error(
            "note",
            "must fit the review note limit without control characters",
        ));
    }
    if note.trim().is_empty() {
        return Err(validation_error(
            "note",
            "must be non-empty without control characters",
        ));
    }
    Ok(())
}

fn insert_review_operator_log(
    transaction: &Transaction<'_>,
    project_id: &str,
    pueue_group: &str,
    action: &'static str,
    details: &Value,
    now: i64,
) -> Result<(), AppError> {
    let details_json =
        serde_json::to_string(details).map_err(|source| AppError::Serialization {
            operation: "serialize review operator log details",
            source,
        })?;
    transaction
        .execute(
            "INSERT INTO operator_logs (project_id, pueue_group, action, details_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![project_id, pueue_group, action, details_json, now],
        )
        .map_err(database_error("insert review operator log"))?;
    Ok(())
}

fn validate_inspection_limit(limit: usize) -> Result<(), AppError> {
    if (1..=100).contains(&limit) {
        Ok(())
    } else {
        Err(validation_error(
            "limit",
            "must be between 1 and 100",
        ))
    }
}

fn read_campaign(connection: &Connection, campaign_id: &str) -> Result<Campaign, AppError> {
    find_campaign(connection, campaign_id)?
        .ok_or_else(|| validation_error("campaign_id", "does not identify a campaign"))
}

fn read_proposal(connection: &Connection, proposal_id: &str) -> Result<Proposal, AppError> {
    connection
        .query_row(
            &format!("{PROPOSAL_SELECT} WHERE proposal_id = ?1"),
            [proposal_id],
            proposal_from_row,
        )
        .map_err(database_error("read campaign proposal"))
}

fn find_experiment(
    connection: &Connection,
    experiment_id: &str,
) -> Result<Option<Experiment>, AppError> {
    connection
        .query_row(
            &format!("{EXPERIMENT_SELECT} WHERE experiment_id = ?1"),
            [experiment_id],
            experiment_from_row,
        )
        .optional()
        .map_err(database_error("find campaign experiment by ID"))
}

fn read_experiment(connection: &Connection, experiment_id: &str) -> Result<Experiment, AppError> {
    find_experiment(connection, experiment_id)?
        .ok_or_else(|| validation_error("experiment_id", "does not identify an experiment"))
}

fn read_submission(connection: &Connection, submission_id: &str) -> Result<Submission, AppError> {
    connection
        .query_row(
            &format!("{SUBMISSION_SELECT} WHERE submission_id = ?1"),
            [submission_id],
            submission_from_row,
        )
        .map_err(database_error("read campaign submission"))
}

fn campaign_from_row(row: &Row<'_>) -> rusqlite::Result<Campaign> {
    Ok(Campaign {
        campaign_id: row.get(0)?,
        project_id: row.get(1)?,
        objective_text: row.get(2)?,
        objective_digest: row.get(3)?,
        initial_argv: json_strings(row, 4)?,
        state: row.get(5)?,
        state_reason: row.get(6)?,
        baseline_experiment_id: row.get(7)?,
        next_eligible_at: row.get(8)?,
        created_at: row.get(9)?,
        updated_at: row.get(10)?,
        base_revision_sha: row.get(11)?,
    })
}

fn proposal_from_row(row: &Row<'_>) -> rusqlite::Result<Proposal> {
    Ok(Proposal {
        proposal_id: row.get(0)?,
        campaign_id: row.get(1)?,
        kind: row.get(2)?,
        status: row.get(3)?,
        hypothesis: row.get(4)?,
        source_experiment_id: row.get(5)?,
        argv: json_strings(row, 6)?,
        working_directory: row.get(7)?,
        expected_evidence: json_strings(row, 8)?,
        canonical_digest: row.get(9)?,
        reject_reason: row.get(10)?,
        created_at: row.get(11)?,
        updated_at: row.get(12)?,
    })
}

fn experiment_from_row(row: &Row<'_>) -> rusqlite::Result<Experiment> {
    Ok(Experiment {
        experiment_id: row.get(0)?,
        campaign_id: row.get(1)?,
        proposal_id: row.get(2)?,
        submission_id: row.get(3)?,
        parent_experiment_id: row.get(4)?,
        attempt: row.get(5)?,
        status: row.get(6)?,
        pueue_task_id: row.get(7)?,
        task_signature: row.get(8)?,
        failure_code: row.get(9)?,
        failure_fingerprint: row.get(10)?,
        created_at: row.get(11)?,
        updated_at: row.get(12)?,
        finished_at: row.get(13)?,
        code_change_run_id: row.get(14)?,
        code_revision_sha: row.get(15)?,
    })
}

fn submission_from_row(row: &Row<'_>) -> rusqlite::Result<Submission> {
    let metadata_json: String = row.get(8)?;
    let metadata: serde_json::Value = serde_json::from_str(&metadata_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(8, Type::Text, Box::new(source))
    })?;
    if !metadata.is_object() {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            8,
            Type::Text,
            "submission metadata must be a JSON object".into(),
        ));
    }
    Ok(Submission {
        submission_id: row.get(0)?,
        project_id: row.get(1)?,
        argv: json_strings(row, 2)?,
        created_at: row.get(3)?,
        pueue_task_id: row.get(4)?,
        task_signature: row.get(5)?,
        status: row.get(6)?,
        kind: row.get(7)?,
        metadata,
        origin_agent_run_id: row.get(9)?,
    })
}

fn budget_reservation_from_row(row: &Row<'_>) -> rusqlite::Result<BudgetReservation> {
    Ok(BudgetReservation {
        reservation_id: row.get(0)?,
        campaign_id: row.get(1)?,
        experiment_id: row.get(2)?,
        dimension: row.get(3)?,
        subject_key: row.get(4)?,
        status: row.get(5)?,
        window_started_at: row.get(6)?,
        window_ends_at: row.get(7)?,
        created_at: row.get(8)?,
        updated_at: row.get(9)?,
    })
}

fn json_strings(row: &Row<'_>, index: usize) -> rusqlite::Result<Vec<String>> {
    let value: String = row.get(index)?;
    serde_json::from_str(&value).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(source))
    })
}

fn serialize_strings(values: &[String], operation: &'static str) -> Result<String, AppError> {
    serde_json::to_string(values).map_err(|source| AppError::Serialization { operation, source })
}

fn serialize_submission_metadata(
    user_metadata: Option<&Value>,
    campaign_id: &str,
    proposal_id: &str,
    experiment_id: &str,
) -> Result<String, AppError> {
    let mut metadata = match user_metadata {
        Some(Value::Object(metadata)) => metadata.clone(),
        Some(_) => {
            return Err(validation_error(
                "submission.metadata",
                "must be a JSON object",
            ));
        }
        None => Map::new(),
    };
    metadata.insert(
        "campaign_id".to_owned(),
        Value::String(campaign_id.to_owned()),
    );
    metadata.insert(
        "proposal_id".to_owned(),
        Value::String(proposal_id.to_owned()),
    );
    metadata.insert(
        "experiment_id".to_owned(),
        Value::String(experiment_id.to_owned()),
    );
    serde_json::to_string(&Value::Object(metadata))
    .map_err(|source| AppError::Serialization {
        operation: "serialize campaign submission metadata",
        source,
    })
}

fn validate_submission_origin_agent_run(
    transaction: &Transaction<'_>,
    project_id: &str,
    origin_agent_run_id: Option<i64>,
) -> Result<(), AppError> {
    let Some(origin_agent_run_id) = origin_agent_run_id else {
        return Ok(());
    };
    let belongs_to_project: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM agent_runs WHERE run_id = ?1 AND project_id = ?2
             )",
            params![origin_agent_run_id, project_id],
            |row| row.get(0),
        )
        .map_err(database_error("validate campaign submission origin agent run"))?;
    if belongs_to_project {
        Ok(())
    } else {
        Err(validation_error(
            "origin_agent_run_id",
            "must identify an agent run in the submission project",
        ))
    }
}

fn rolling_window_end(now: i64) -> Result<i64, AppError> {
    now.checked_add(ROLLING_WINDOW_SECONDS).ok_or_else(|| {
        validation_error(
            "now",
            "cannot represent the end of the rolling campaign window",
        )
    })
}

fn is_terminal(status: ExperimentStatus) -> bool {
    matches!(
        status,
        ExperimentStatus::Succeeded | ExperimentStatus::Failed | ExperimentStatus::Cancelled
    )
}

fn validate_task_identity(task_id: i64, task_signature: &str) -> Result<(), AppError> {
    if task_id < 0 {
        return Err(validation_error(
            "pueue_task_id",
            "must be non-negative",
        ));
    }
    if task_signature.is_empty() || task_signature.chars().any(char::is_control) {
        return Err(validation_error(
            "task_signature",
            "must be non-empty and contain no control characters",
        ));
    }
    Ok(())
}

fn validate_failure_field(field: &'static str, value: &str) -> Result<(), AppError> {
    if value.is_empty()
        || value.len() > MAX_FAILURE_FIELD_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(validation_error(
            field,
            "must be 1 to 128 bytes without control characters",
        ));
    }
    Ok(())
}

fn validation_error(field: &'static str, message: &'static str) -> AppError {
    AppError::Validation { field, message }
}
