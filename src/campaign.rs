use std::{ffi::OsString, path::Path};

use serde_json::Value;
use uuid::Uuid;

use crate::{
    db::{
        CampaignRepository, Db, ExperimentRepository, ManagedSubmissionIntent,
        ProjectRepository, ProposalRepository, StartCampaignRequest, SubmissionRepository,
    },
    execution_policy::CampaignLimits,
    models::{
        Campaign, Experiment, ExperimentStatus, Project, Proposal, ProposalKind, Submission,
    },
    proposals::{self, ProposalInput},
    pueue::{validate_add_argv, PueueApi},
    state::ObjectiveSnapshot,
    AppError,
};

const BASELINE_HYPOTHESIS: &str = "Establish the initial campaign baseline";
const ADD_UNKNOWN_REASON: &str = "pueue_add_unknown";
const ADD_INTERRUPTED_REASON: &str = "pueue_add_interrupted";

pub struct CampaignCoordinator<'a, P: PueueApi + ?Sized> {
    db: &'a Db,
    pueue: &'a P,
    limits: CampaignLimits,
}

impl<'a, P: PueueApi + ?Sized> CampaignCoordinator<'a, P> {
    pub fn new(db: &'a Db, pueue: &'a P, limits: CampaignLimits) -> Self {
        Self { db, pueue, limits }
    }

    pub async fn start_baseline(
        &self,
        project: &Project,
        objective: &ObjectiveSnapshot,
        initial_argv: &[String],
        metadata: &Value,
        origin_agent_run_id: Option<i64>,
        now: i64,
    ) -> Result<Submission, AppError> {
        let baseline = proposals::validate_initial_baseline(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: BASELINE_HYPOTHESIS.to_owned(),
                source_experiment_id: None,
                argv: initial_argv.to_vec(),
                working_directory: ".".to_owned(),
                expected_evidence: Vec::new(),
            },
            &objective.digest,
        )?;
        let add_args = pueue_add_args(
            &project.pueue_group,
            &project.root_path,
            baseline.argv(),
        );
        validate_add_argv(&add_args)?;

        let campaign_id = Uuid::new_v4().to_string();
        let proposal_id = Uuid::new_v4().to_string();
        let experiment_id = Uuid::new_v4().to_string();
        let submission_id = Uuid::new_v4().to_string();
        let intent = CampaignRepository::new(self.db).start_with_baseline(
            StartCampaignRequest {
                campaign_id: &campaign_id,
                project_id: &project.project_id,
                objective,
                initial_argv,
                baseline: &baseline,
                submission_id: &submission_id,
                experiment_id: &experiment_id,
                proposal_id: &proposal_id,
                metadata,
                origin_agent_run_id,
                now,
            },
            &self.limits,
        )?;
        self.submit_accepted_intent(&intent, project, now).await
    }

    pub async fn submit_accepted_intent(
        &self,
        intent: &ManagedSubmissionIntent,
        project: &Project,
        now: i64,
    ) -> Result<Submission, AppError> {
        let experiments = ExperimentRepository::new(self.db);
        let current = experiments
            .find_by_id(&intent.experiment.experiment_id)?
            .ok_or(AppError::Runtime {
                operation: "read campaign experiment before Pueue submission",
            })?;
        let durable_campaign = CampaignRepository::new(self.db)
            .find_by_id(&current.campaign_id)?
            .ok_or(AppError::Runtime {
                operation: "read campaign before Pueue submission",
            })?;
        let durable_proposal = ProposalRepository::new(self.db)
            .find_by_id(&current.proposal_id)?
            .ok_or(AppError::Runtime {
                operation: "read campaign proposal before Pueue submission",
            })?;
        let durable_submission = SubmissionRepository::new(self.db)
            .find_by_id(&current.submission_id)?
            .ok_or(AppError::Runtime {
                operation: "read campaign submission before Pueue submission",
            })?;
        let durable_project = ProjectRepository::new(self.db)
            .find_by_id(&durable_campaign.project_id)?
            .ok_or(AppError::Runtime {
                operation: "read campaign project before Pueue submission",
            })?;
        validate_intent_identity(
            intent,
            project,
            &durable_campaign,
            &durable_proposal,
            &current,
            &durable_submission,
            &durable_project,
        )?;
        let add_args = pueue_add_args(
            &durable_project.pueue_group,
            &durable_project.root_path,
            &durable_submission.argv,
        );
        validate_add_argv(&add_args)?;

        match current.status {
            ExperimentStatus::Reserved => {
                experiments.mark_submitting(&current.experiment_id, now)?;
            }
            ExperimentStatus::Submitting => {
                experiments.mark_unreconciled(
                    &current.experiment_id,
                    ADD_INTERRUPTED_REASON,
                    now,
                )?;
                return Err(reconciliation_required());
            }
            ExperimentStatus::Unreconciled => return Err(reconciliation_required()),
            ExperimentStatus::Accepted
            | ExperimentStatus::Succeeded
            | ExperimentStatus::Failed
            | ExperimentStatus::Cancelled => {
                return SubmissionRepository::new(self.db)
                    .find_by_id(&current.submission_id)?
                    .ok_or(AppError::Runtime {
                        operation: "read accepted campaign submission",
                    });
            }
        }

        let task_id = match self.pueue.add(&add_args).await {
            Ok(task_id) => task_id,
            Err(error) => {
                experiments.mark_unreconciled(&current.experiment_id, ADD_UNKNOWN_REASON, now)?;
                return Err(error);
            }
        };
        let task_signature = provisional_task_signature(
            &durable_project.pueue_group,
            task_id,
            &durable_submission.submission_id,
        );
        if let Err(error) =
            experiments.mark_accepted(&current.experiment_id, task_id, &task_signature, now)
        {
            experiments.mark_unreconciled(&current.experiment_id, ADD_UNKNOWN_REASON, now)?;
            return Err(error);
        }
        SubmissionRepository::new(self.db)
            .find_by_id(&current.submission_id)?
            .ok_or(AppError::Runtime {
                operation: "read accepted campaign submission",
            })
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_intent_identity(
    intent: &ManagedSubmissionIntent,
    caller_project: &Project,
    campaign: &Campaign,
    proposal: &Proposal,
    experiment: &Experiment,
    submission: &Submission,
    project: &Project,
) -> Result<(), AppError> {
    if caller_project.project_id != project.project_id
        || campaign.project_id != project.project_id
        || proposal.campaign_id != campaign.campaign_id
        || experiment.campaign_id != campaign.campaign_id
        || experiment.proposal_id != proposal.proposal_id
        || experiment.submission_id != submission.submission_id
        || submission.project_id != project.project_id
        || intent.campaign.campaign_id != campaign.campaign_id
        || intent.campaign.project_id != campaign.project_id
        || intent.proposal.proposal_id != proposal.proposal_id
        || intent.proposal.campaign_id != proposal.campaign_id
        || intent.experiment.experiment_id != experiment.experiment_id
        || intent.experiment.campaign_id != experiment.campaign_id
        || intent.experiment.proposal_id != experiment.proposal_id
        || intent.experiment.submission_id != experiment.submission_id
        || intent.submission.submission_id != submission.submission_id
        || intent.submission.project_id != submission.project_id
    {
        return Err(AppError::Validation {
            field: "campaign.intent",
            message: "must match the durable managed submission identity",
        });
    }
    Ok(())
}

fn pueue_add_args(group: &str, project_root: &Path, argv: &[String]) -> Vec<OsString> {
    let mut add_args = Vec::with_capacity(argv.len() + 5);
    add_args.push(OsString::from("-g"));
    add_args.push(OsString::from(group));
    add_args.push(OsString::from("--working-directory"));
    add_args.push(project_root.as_os_str().to_owned());
    add_args.push(OsString::from("--"));
    add_args.extend(argv.iter().map(OsString::from));
    add_args
}

fn provisional_task_signature(group: &str, task_id: i64, submission_id: &str) -> String {
    format!("provisional-submit:v1:group={group}:task-id={task_id}:intent={submission_id}")
}

fn reconciliation_required() -> AppError {
    AppError::Validation {
        field: "experiment",
        message: "submission may already have reached Pueue; reconciliation is required",
    }
}
