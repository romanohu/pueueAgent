use rusqlite::{
    params,
    types::Type,
    Connection, OptionalExtension, Row, Transaction, TransactionBehavior,
};
use serde_json::json;

use crate::{
    execution_policy::CampaignLimits,
    models::{
        BudgetDimension, BudgetReservationStatus, Campaign, CampaignState, Experiment,
        ExperimentStatus, ExperimentTerminalOutcome, Proposal, ProposalKind, ProposalStatus,
        Submission, SubmissionKind, SubmissionStatus,
    },
    proposals::ValidatedProposal,
    state::ObjectiveSnapshot,
    AppError,
};

use super::{database_error, Db};

const ROLLING_WINDOW_SECONDS: i64 = 24 * 60 * 60;
const MAX_FAILURE_FIELD_BYTES: usize = 128;

const CAMPAIGN_SELECT: &str = "SELECT campaign_id, project_id, objective_text, objective_digest,
        initial_argv_json, state, state_reason, baseline_experiment_id, next_eligible_at,
        created_at, updated_at
    FROM campaigns";
const PROPOSAL_SELECT: &str = "SELECT proposal_id, campaign_id, kind, status, hypothesis,
        source_experiment_id, argv_json, working_directory, expected_evidence_json,
        canonical_digest, reject_reason, created_at, updated_at
    FROM proposals";
const EXPERIMENT_SELECT: &str = "SELECT experiment_id, campaign_id, proposal_id, submission_id,
        parent_experiment_id, attempt, status, pueue_task_id, task_signature, failure_code,
        failure_fingerprint, created_at, updated_at, finished_at
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

pub struct StartCampaignRequest<'a> {
    pub campaign_id: &'a str,
    pub project_id: &'a str,
    pub objective: &'a ObjectiveSnapshot,
    pub initial_argv: &'a [String],
    pub baseline: &'a ValidatedProposal,
    pub submission_id: &'a str,
    pub experiment_id: &'a str,
    pub proposal_id: &'a str,
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
            request.campaign_id,
            request.proposal_id,
            request.experiment_id,
        )?;
        let window_ends_at = rolling_window_end(request.now)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin campaign baseline reservation"))?;

        validate_project_available(&transaction, request.project_id)?;
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

        transaction
            .execute(
                "INSERT INTO campaigns (
                    campaign_id, project_id, objective_text, objective_digest, initial_argv_json,
                    state, state_reason, baseline_experiment_id, next_eligible_at,
                    created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, NULL, ?7, ?7)",
                params![
                    request.campaign_id,
                    request.project_id,
                    request.objective.text,
                    request.objective.digest,
                    initial_argv_json,
                    CampaignState::Active,
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
    ) -> Result<Option<ManagedSubmissionIntent>, AppError> {
        let argv_json = serialize_strings(
            proposal.argv(),
            "serialize campaign proposal arguments",
        )?;
        let evidence_json = serialize_strings(
            proposal.expected_evidence(),
            "serialize campaign proposal expected evidence",
        )?;
        let metadata_json =
            serialize_submission_metadata(campaign_id, proposal_id, experiment_id)?;
        let window_ends_at = rolling_window_end(now)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin campaign proposal acceptance"))?;

        let campaign = read_campaign(&transaction, campaign_id)?;
        if campaign.state != CampaignState::Active {
            return Err(validation_error(
                "campaign",
                "must be active to accept a proposal",
            ));
        }
        validate_project_available(&transaction, &campaign.project_id)?;

        if let Some(existing) = find_proposal_by_digest(
            &transaction,
            campaign_id,
            proposal.canonical_digest(),
        )? {
            return match existing.status {
                ProposalStatus::Accepted => {
                    read_intent_by_proposal(&transaction, &existing.proposal_id).map(Some)
                }
                ProposalStatus::Pending if existing.kind == ProposalKind::CodeChange => Ok(None),
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
                 WHERE campaign_id = ?1 AND source_experiment_id = ?2",
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
            let code_change_count = count_live_reservations(
                &transaction,
                campaign_id,
                BudgetDimension::CodeChange,
                now,
            )?;
            if code_change_count >= i64::from(limits.max_code_change_proposals_per_24h) {
                return Err(validation_error(
                    "campaign_limits.max_code_change_proposals_per_24h",
                    "the rolling code-change proposal budget is exhausted",
                ));
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
            transaction
                .commit()
                .map_err(database_error("commit pending code-change proposal"))?;
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
            return Err(validation_error(
                "campaign_limits.max_new_experiments_per_24h",
                "the rolling experiment budget is exhausted",
            ));
        }
        let same_spec_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*)
                 FROM experiments AS experiment
                 JOIN proposals AS candidate ON candidate.proposal_id = experiment.proposal_id
                 WHERE experiment.campaign_id = ?1 AND candidate.argv_json = ?2",
                params![campaign_id, argv_json],
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
}

impl<'db> ExperimentRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn find_by_id(&self, experiment_id: &str) -> Result<Option<Experiment>, AppError> {
        let connection = self.db.connect()?;
        find_experiment(&connection, experiment_id)
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
        Ok(stored)
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
    if !state.0 || state.1 || state.2.is_some() {
        return Err(validation_error(
            "project",
            "must be enabled, unpaused, and not halted",
        ));
    }
    Ok(())
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
    now: i64,
) -> Result<(), AppError> {
    transaction
        .execute(
            "INSERT INTO submissions (
                submission_id, project_id, argv_json, created_at, pueue_task_id, task_signature,
                status, kind, metadata_json, origin_agent_run_id
             ) VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5, ?6, ?7, NULL)",
            params![
                submission_id,
                project_id,
                argv_json,
                now,
                SubmissionStatus::Pending,
                SubmissionKind::Experiment,
                metadata_json,
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
    now: i64,
) -> Result<(), AppError> {
    transaction
        .execute(
            "INSERT INTO experiments (
                experiment_id, campaign_id, proposal_id, submission_id, parent_experiment_id,
                attempt, status, pueue_task_id, task_signature, failure_code,
                failure_fingerprint, created_at, updated_at, finished_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, NULL, NULL, ?8, ?8, NULL)",
            params![
                experiment_id,
                campaign_id,
                proposal_id,
                submission_id,
                parent_experiment_id,
                attempt,
                ExperimentStatus::Reserved,
                now,
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
    campaign_id: &str,
    proposal_id: &str,
    experiment_id: &str,
) -> Result<String, AppError> {
    serde_json::to_string(&json!({
        "campaign_id": campaign_id,
        "proposal_id": proposal_id,
        "experiment_id": experiment_id,
    }))
    .map_err(|source| AppError::Serialization {
        operation: "serialize campaign submission metadata",
        source,
    })
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
