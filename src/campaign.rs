use std::{ffi::OsString, path::Path};

use uuid::Uuid;

use crate::{
    db::{
        CampaignRepository, Db, ExperimentRepository, ManagedSubmissionIntent,
        StartCampaignRequest, SubmissionRepository,
    },
    execution_policy::CampaignLimits,
    models::{ExperimentStatus, Project, ProposalKind, Submission},
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
        validate_intent_project(intent, project)?;
        let add_args = pueue_add_args(
            &project.pueue_group,
            &project.root_path,
            &intent.submission.argv,
        );
        validate_add_argv(&add_args)?;

        let experiments = ExperimentRepository::new(self.db);
        let current = experiments
            .find_by_id(&intent.experiment.experiment_id)?
            .ok_or(AppError::Runtime {
                operation: "read campaign experiment before Pueue submission",
            })?;
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
            &project.pueue_group,
            task_id,
            &intent.submission.submission_id,
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

fn validate_intent_project(
    intent: &ManagedSubmissionIntent,
    project: &Project,
) -> Result<(), AppError> {
    if intent.campaign.project_id != project.project_id
        || intent.submission.project_id != project.project_id
    {
        return Err(AppError::Validation {
            field: "campaign.project_id",
            message: "must match the validated project",
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
