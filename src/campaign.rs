use std::{
    ffi::OsString,
    path::{Component, Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};

use serde_json::Value;
use uuid::Uuid;

use crate::{
    agent::{AgentRunner, AgentRunnerConfig},
    code_change,
    config,
    codex_command::probe_installed_codex_capabilities,
    db::{
        CampaignRepository, CheckpointDispatchSelection, CodeChangeReentry, CodeChangeRepository,
        Db, DecisionReservation, ExperimentRepository, ManagedSubmissionIntent, ProjectRepository,
        ProposalAcceptance, ProposalRepository, ResearchRepository, StartCampaignRequest,
        SubmissionRepository,
    },
    environment::ProjectAdmissionLock,
    execution_policy::{
        preflight_code_change_runtime, preflight_decision_runtime, resolve_decision_project_policy,
        CampaignLimits, ProjectRootAnchor, ResolvedExecutionPolicy, VerifiedProjectRoot,
        VerifiedWorkingDirectory,
    },
    models::{
        Campaign, CampaignState, CodeChangeRun, Experiment, ExperimentStatus, NewCodeChangeRun,
        ObjectiveMetric, Project, Proposal, ProposalKind,
        Submission,
    },
    output::{
        render_campaign_mutation, render_campaign_status_with_decision,
        render_experiment_inspection, render_experiment_list, render_proposal_inspection,
        render_proposal_list, DecisionStatusProjection,
    },
    proposals::{self, ProposalInput},
    pueue::{validate_add_argv, PueueApi},
    reconcile::{
        managed_task_run_signature, task_signature_group_matches, try_canonical_command_display_os,
    },
    research_checkpoint::verify_prepared_checkpoint,
    state::ObjectiveSnapshot,
    status::current_decision_projection,
    AppError,
};

pub const DEFAULT_INSPECTION_LIMIT: usize = 20;
pub const MAX_INSPECTION_LIMIT: usize = 100;

const BASELINE_HYPOTHESIS: &str = "Establish the initial campaign baseline";
const ADD_UNKNOWN_REASON: &str = "pueue_add_unknown";
const ADD_INTERRUPTED_REASON: &str = "pueue_add_interrupted";
const ADD_IDENTITY_REASON: &str = "pueue_identity_unresolved";
const CHECKPOINT_PRE_ADD_FAILURE_CODE: &str = "research_checkpoint_verification_failed";

#[cfg(test)]
static TEST_AFTER_CLONE_HOOKS:
    std::sync::LazyLock<
        std::sync::Mutex<std::collections::HashMap<(std::path::PathBuf, String), fn(&Db, &str)>>,
    > = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn set_test_after_clone_hook(
    db: &Db,
    experiment_id: &str,
    callback: Option<fn(&Db, &str)>,
) {
    let mut configured = TEST_AFTER_CLONE_HOOKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let key = (db.path().to_path_buf(), experiment_id.to_owned());
    match callback {
        Some(callback) => {
            configured.insert(key, callback);
        }
        None => {
            configured.remove(&key);
        }
    }
}

#[cfg(test)]
fn run_test_after_clone_hook(db: &Db, experiment_id: &str) {
    let mut configured = TEST_AFTER_CLONE_HOOKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let hook = configured.remove(&(db.path().to_path_buf(), experiment_id.to_owned()));
    drop(configured);
    if let Some(hook) = hook {
        hook(db, experiment_id);
    }
}
const RUNTIME_OUTPUT_RECOVERY_REASON: &str = "runtime_output_recovery_required";
const RUNTIME_OUTPUT_RECOVERY_SUMMARY: &str =
    "candidate runtime output scope could not be verified";
const MAX_GIT_OUTPUT_BYTES: usize = 64 * 1024;
const GIT_RUNTIME: Duration = Duration::from_secs(30);
const PINNED_GIT_ENVIRONMENT: &[(&str, &str)] = &[
    ("LANG", "C"),
    ("LC_ALL", "C"),
    ("GIT_CONFIG_NOSYSTEM", "1"),
    ("GIT_CONFIG_SYSTEM", "/dev/null"),
    ("GIT_CONFIG_GLOBAL", "/dev/null"),
    ("GIT_OPTIONAL_LOCKS", "0"),
    ("GIT_TERMINAL_PROMPT", "0"),
    ("GIT_PAGER", "cat"),
    ("PAGER", "cat"),
    ("GIT_ASKPASS", "/bin/false"),
    ("SSH_ASKPASS", "/bin/false"),
    ("GIT_EDITOR", "/bin/false"),
    ("GIT_SEQUENCE_EDITOR", "/bin/false"),
];

#[derive(Debug, Clone, PartialEq)]
pub enum CampaignSubmission {
    Submitted(Submission),
    Deferred,
}

#[cfg(unix)]
fn require_code_change_runtime_recovery(
    db: &Db,
    experiment: &Experiment,
    now: i64,
) -> Result<(), AppError> {
    if let Some(run_id) = experiment.code_change_run_id.as_deref() {
        CodeChangeRepository::new(db).require_recovery(
            run_id,
            RUNTIME_OUTPUT_RECOVERY_REASON,
            RUNTIME_OUTPUT_RECOVERY_SUMMARY,
            now,
        )?;
    }
    Ok(())
}

enum CodeChangeBaseResolution {
    Available(String),
    InvalidBest,
    Unavailable,
}

pub fn render_status_for_project(
    db: &Db,
    project: &Project,
    json: bool,
) -> Result<String, AppError> {
    let (campaign, proposal_count, experiment_counts, budget_usage, task_ids) =
        CampaignRepository::new(db).status_for_project(&project.project_id)?;
    let decision = current_decision_projection(
        db,
        &campaign.campaign_id,
        crate::status::status_timestamp()?,
    )?
    .as_ref()
    .map(DecisionStatusProjection::from);
    let research_repository = ResearchRepository::new(db);
    let research = research_repository.status_projection(&campaign.campaign_id)?;
    let research_history = research_repository.recent_status_summaries(
        &campaign.campaign_id,
        None,
        32,
    )?;
    render_campaign_status_with_decision(
        &campaign,
        proposal_count,
        &experiment_counts,
        &budget_usage,
        &task_ids,
        decision.as_ref(),
        Some(&research),
        &research_history,
        json,
    )
}

pub fn pause_for_project(
    db: &Db,
    project: &Project,
    now: i64,
    json: bool,
) -> Result<String, AppError> {
    let campaign = CampaignRepository::new(db).pause(&project.project_id, now)?;
    render_campaign_mutation(&campaign, "pause", json)
}

pub async fn resume_for_project(
    db: &Db,
    project: &Project,
    now: i64,
    json: bool,
    policy: Arc<ResolvedExecutionPolicy>,
) -> Result<String, AppError> {
    let repository = ResearchRepository::new(db);
    let Some(campaign) = CampaignRepository::new(db).find_latest_by_project(&project.project_id)?
    else {
        let campaign = CampaignRepository::new(db).resume(&project.project_id, now)?;
        return render_campaign_mutation(&campaign, "resume", json);
    };
    let initial_state = repository.state(&campaign.campaign_id)?;
    if initial_state.blocked_reason.is_none() {
        let campaign = CampaignRepository::new(db).resume(&project.project_id, now)?;
        return render_campaign_mutation(&campaign, "resume", json);
    }

    preflight_decision_runtime().map_err(AppError::from)?;
    let project_config = config::load(&project.config_path)?;
    let runner = AgentRunner::new(AgentRunnerConfig::production(), Arc::clone(&policy));
    let project_policy = runner
        .resolve_project_policy(project, &project_config)
        .map_err(AppError::from)?;
    let decision_policy = resolve_decision_project_policy(runner.execution_policy(), &project_policy)
        .map_err(AppError::from)?;
    let _project_lock = runner
        .try_acquire_project_admission_lock(&decision_policy)
        .map_err(AppError::from)?
        .ok_or(AppError::Runtime {
            operation: "acquire project recovery admission lock",
        })?;

    let expected = repository.state(&campaign.campaign_id)?;
    let Some(_) = expected.blocked_reason.as_deref() else {
        let campaign = CampaignRepository::new(db).resume(&project.project_id, now)?;
        return render_campaign_mutation(&campaign, "resume", json);
    };

    let capabilities = probe_installed_codex_capabilities(&decision_policy.agent_anchor)
        .await
        .map_err(AppError::from)?;
    if !capabilities.supports_research_policy() {
        return Err(AppError::from(
            crate::execution_policy::PolicyViolation::new(
                crate::execution_policy::PolicyViolationCode::UnsafeCodexArgument,
                crate::execution_policy::PolicyViolationStage::PreBinding,
            ),
        ));
    }
    if let Some(session_id) = expected.session_id.as_deref() {
        runner.research_context_for(&decision_policy, session_id)?;
    }

    repository.resume_after_validation(&project.project_id, &expected, now)?;
    let campaign = CampaignRepository::new(db).resume(&project.project_id, now)?;
    render_campaign_mutation(&campaign, "resume", json)
}

pub fn retire_for_project(
    db: &Db,
    project: &Project,
    now: i64,
    json: bool,
) -> Result<String, AppError> {
    let campaign = CampaignRepository::new(db).retire(&project.project_id, now)?;
    render_campaign_mutation(&campaign, "retire", json)
}

pub fn review_accept_for_project(
    db: &Db,
    project: &Project,
    note: Option<&str>,
    now: i64,
    json: bool,
) -> Result<String, AppError> {
    let campaign = CampaignRepository::new(db).review_accept(&project.project_id, note, now)?;
    render_campaign_mutation(&campaign, "review_accept", json)
}

pub fn review_reject_for_project(
    db: &Db,
    project: &Project,
    note: Option<&str>,
    now: i64,
    json: bool,
) -> Result<String, AppError> {
    let campaign = CampaignRepository::new(db).review_reject(&project.project_id, note, now)?;
    render_campaign_mutation(&campaign, "review_reject", json)
}

pub fn render_proposals_for_project(
    db: &Db,
    project: &Project,
    limit: usize,
    json: bool,
) -> Result<String, AppError> {
    validate_inspection_limit(limit)?;
    let campaign = latest_campaign(db, project)?;
    let proposals = ProposalRepository::new(db).list_for_campaign(&campaign.campaign_id, limit)?;
    render_proposal_list(&campaign, &proposals, json)
}

pub fn render_proposal_for_project(
    db: &Db,
    project: &Project,
    proposal_id: &str,
    json: bool,
) -> Result<String, AppError> {
    let campaign = latest_campaign(db, project)?;
    let proposal = ProposalRepository::new(db)
        .find_for_campaign(&campaign.campaign_id, proposal_id)?
        .ok_or(AppError::Validation {
            field: "proposal_id",
            message: "does not identify a proposal in this project campaign",
        })?;
    render_proposal_inspection(&campaign, &proposal, json)
}

pub fn render_experiments_for_project(
    db: &Db,
    project: &Project,
    limit: usize,
    json: bool,
) -> Result<String, AppError> {
    validate_inspection_limit(limit)?;
    let campaign = latest_campaign(db, project)?;
    let experiments =
        ExperimentRepository::new(db).list_for_campaign(&campaign.campaign_id, limit)?;
    render_experiment_list(&campaign, &experiments, json)
}

pub fn render_experiment_for_project(
    db: &Db,
    project: &Project,
    experiment_id: &str,
    json: bool,
) -> Result<String, AppError> {
    let campaign = latest_campaign(db, project)?;
    let (experiment, argv_digest) = ExperimentRepository::new(db)
        .inspect_for_campaign(&campaign.campaign_id, experiment_id)?
        .ok_or(AppError::Validation {
            field: "experiment_id",
            message: "does not identify an experiment in this project campaign",
        })?;
    let research_history = ResearchRepository::new(db).recent_status_summaries(
        &campaign.campaign_id,
        Some(&experiment.experiment_id),
        32,
    )?;
    render_experiment_inspection(
        &campaign,
        &experiment,
        &argv_digest,
        &research_history,
        json,
    )
}

fn latest_campaign(db: &Db, project: &Project) -> Result<Campaign, AppError> {
    CampaignRepository::new(db)
        .find_latest_by_project(&project.project_id)?
        .ok_or(AppError::Validation {
            field: "campaign",
            message: "the project has no campaign",
        })
}

fn validate_inspection_limit(limit: usize) -> Result<(), AppError> {
    if (1..=MAX_INSPECTION_LIMIT).contains(&limit) {
        Ok(())
    } else {
        Err(AppError::Validation {
            field: "limit",
            message: "must be between 1 and 100",
        })
    }
}

pub struct CampaignCoordinator<'a, P: PueueApi + ?Sized> {
    db: &'a Db,
    pueue: &'a P,
    limits: CampaignLimits,
    root_anchor: Option<ProjectRootAnchor>,
    execution_policy: Option<&'a ResolvedExecutionPolicy>,
}

pub(crate) struct CampaignAdmission {
    verified_root: VerifiedProjectRoot,
    _lock: ProjectAdmissionLock,
}

pub(crate) struct AdmittedCampaignProposal {
    pub(crate) intent: ManagedSubmissionIntent,
    pub(crate) matches_requested_intent: bool,
    admission: CampaignAdmission,
}

pub(crate) enum CampaignProposalAdmission {
    Experiment(AdmittedCampaignProposal),
    CodeChange(CodeChangeRun),
    CodeChangeRejected(Proposal),
    Deferred,
}

impl<'a, P: PueueApi + ?Sized> CampaignCoordinator<'a, P> {
    pub fn new(db: &'a Db, pueue: &'a P, limits: CampaignLimits) -> Self {
        Self {
            db,
            pueue,
            limits,
            root_anchor: None,
            execution_policy: None,
        }
    }

    pub fn with_root_anchor(mut self, root_anchor: ProjectRootAnchor) -> Self {
        self.root_anchor = Some(root_anchor);
        self
    }

    pub fn with_execution_policy(mut self, policy: &'a ResolvedExecutionPolicy) -> Self {
        self.execution_policy = Some(policy);
        self
    }

    pub async fn start_baseline(
        &self,
        project: &Project,
        objective: &ObjectiveSnapshot,
        initial_argv: &[String],
        metadata: &Value,
        origin_agent_run_id: Option<i64>,
        objective_metric: Option<&ObjectiveMetric>,
        now: i64,
    ) -> Result<Submission, AppError> {
        let admission = self.acquire_admission(project)?;
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
        let campaign_id = Uuid::new_v4().to_string();
        let proposal_id = Uuid::new_v4().to_string();
        let experiment_id = Uuid::new_v4().to_string();
        let submission_id = Uuid::new_v4().to_string();
        let runtime_argv = crate::environment::campaign_experiment_runtime_argv(
            &admission.verified_root.anchor.canonical_path,
            &campaign_id,
            &experiment_id,
            baseline.argv(),
        );
        try_canonical_command_display_os(&runtime_argv)?;
        let add_args = pueue_add_args(
            &project.pueue_group,
            &admission.verified_root.anchor.canonical_path,
            &runtime_argv,
        );
        validate_add_argv(&add_args)?;
        let base_revision_sha = self
            .capture_clean_head(&admission.verified_root.anchor.canonical_path)
            .await;
        let intent = CampaignRepository::new(self.db).start_with_baseline_at_revision(
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
                objective_metric,
                now,
            },
            &self.limits,
            base_revision_sha.as_deref(),
        )?;
        match self
            .submit_accepted_intent_inner(
                &intent,
                project,
                now,
                Some(admission),
                #[cfg(unix)]
                None,
                false,
            )
            .await?
        {
            CampaignSubmission::Submitted(submission) => Ok(submission),
            CampaignSubmission::Deferred => Err(AppError::Runtime {
                operation: "submit admitted campaign baseline",
            }),
        }
    }

    pub async fn submit_accepted_intent(
        &self,
        intent: &ManagedSubmissionIntent,
        project: &Project,
        now: i64,
    ) -> Result<Submission, AppError> {
        #[cfg(unix)]
        if let Some(run_id) = intent.experiment.code_change_run_id.as_deref() {
            return match self.submit_candidate_intent(run_id, project, now).await? {
                CampaignSubmission::Submitted(submission) => Ok(submission),
                CampaignSubmission::Deferred => Err(AppError::Validation {
                    field: "campaign",
                    message: "campaign and project authority must permit reserved submission",
                }),
            };
        }
        match self
            .submit_accepted_intent_inner(
                intent,
                project,
                now,
                None,
                #[cfg(unix)]
                None,
                false,
            )
            .await?
        {
            CampaignSubmission::Submitted(submission) => Ok(submission),
            CampaignSubmission::Deferred => Err(AppError::Validation {
                field: "campaign",
                message: "campaign and project authority must permit reserved submission",
            }),
        }
    }

    pub async fn submit_reserved_intent(
        &self,
        intent: &ManagedSubmissionIntent,
        project: &Project,
        now: i64,
    ) -> Result<CampaignSubmission, AppError> {
        #[cfg(unix)]
        if let Some(run_id) = intent.experiment.code_change_run_id.as_deref() {
            return self.submit_candidate_intent(run_id, project, now).await;
        }
        self.submit_accepted_intent_inner(
            intent,
            project,
            now,
            None,
            #[cfg(unix)]
            None,
            false,
        )
        .await
    }

    /// Accept and submit the immutable candidate belonging to one durable
    /// code-change run.  The candidate worktree and proposal cwd are opened
    /// before the atomic DB acceptance and retained until Pueue identity has
    /// been verified.
    #[cfg(unix)]
    pub async fn submit_candidate_intent(
        &self,
        run_id: &str,
        project: &Project,
        now: i64,
    ) -> Result<CampaignSubmission, AppError> {
        let policy = self.execution_policy.ok_or(AppError::Validation {
            field: "code_change.policy",
            message: "startup execution policy is required for candidate submission",
        })?;
        let repository = CodeChangeRepository::new(self.db);
        let run = repository.find_by_id(run_id)?.ok_or(AppError::Validation {
            field: "code_change_run_id",
            message: "does not identify a persisted code-change run",
        })?;
        let campaign = CampaignRepository::new(self.db)
            .find_by_id(&run.campaign_id)?
            .ok_or(AppError::Runtime {
                operation: "read code-change campaign before candidate submission",
            })?;
        let durable_project = ProjectRepository::new(self.db)
            .find_by_id(&campaign.project_id)?
            .ok_or(AppError::Runtime {
                operation: "read code-change project before candidate submission",
            })?;
        let proposal = ProposalRepository::new(self.db)
            .find_by_id(&run.proposal_id)?
            .ok_or(AppError::Runtime {
                operation: "read code-change proposal before candidate submission",
            })?;
        if proposal.campaign_id != campaign.campaign_id || proposal.kind != ProposalKind::CodeChange
        {
            return Err(AppError::Validation {
                field: "code_change.proposal",
                message: "must belong to the candidate campaign",
            });
        }
        let project_config = crate::config::load(&durable_project.config_path)?;
        let original_policy = crate::execution_policy::resolve_project_policy(
            policy,
            &durable_project,
            &project_config,
        )
        .map_err(AppError::from)?;
        let admission = self.acquire_admission(&durable_project)?;
        let existing_experiment = match run.experiment_id.as_deref() {
            Some(experiment_id) => Some(
                ExperimentRepository::new(self.db)
                    .find_by_id(experiment_id)?
                    .ok_or(AppError::Runtime {
                        operation: "read submitted code-change experiment identity",
                    })?,
            ),
            None => None,
        };
        let candidate_experiment_identity = existing_experiment
            .as_ref()
            .map(|experiment| experiment.experiment_id.clone())
            .unwrap_or_else(|| candidate_experiment_id(run_id));
        match ResearchRepository::new(self.db).checkpoint_dispatch_authority(
            &durable_project.project_id,
            &candidate_experiment_identity,
            now,
        )? {
            CheckpointDispatchSelection::NotCheckpoint => {}
            CheckpointDispatchSelection::Blocked => return Ok(CampaignSubmission::Deferred),
            CheckpointDispatchSelection::Ready(_) => {
                return Err(AppError::Validation {
                    field: "research.checkpoint",
                    message: "checkpoint successor cannot enter the candidate route",
                })
            }
        }
        if matches!(
            existing_experiment.as_ref().map(|experiment| experiment.status),
            None | Some(ExperimentStatus::Reserved)
        ) {
            let campaign_active = CampaignRepository::new(self.db)
                .find_by_id(&campaign.campaign_id)?
                .is_some_and(|campaign| campaign.state == CampaignState::Active);
            let project_available = ProjectRepository::new(self.db)
                .refresh_admission_authority(&durable_project)?
                .is_some();
            if !campaign_active || !project_available {
                return Ok(CampaignSubmission::Deferred);
            }
        }
        let candidate = match existing_experiment.as_ref().map(|experiment| experiment.status) {
            Some(
                ExperimentStatus::Submitting
                | ExperimentStatus::Unreconciled
                | ExperimentStatus::Accepted
                | ExperimentStatus::Succeeded
                | ExperimentStatus::Failed
                | ExperimentStatus::Cancelled,
            ) => {
                match code_change::reopen_code_change_candidate_for_run(
                    policy,
                    &durable_project,
                    &original_policy,
                    self.db,
                    run_id,
                )
                .await
                {
                    Ok(candidate) => candidate,
                    Err(_) => {
                        code_change::reopen_code_change_result_for_run(
                            policy,
                            &durable_project,
                            &original_policy,
                            self.db,
                            run_id,
                            &run.experiment_id.as_deref().unwrap_or_default(),
                        )
                        .await?
                    }
                }
            }
            _ => {
                code_change::reopen_code_change_candidate_for_run(
                    policy,
                    &durable_project,
                    &original_policy,
                    self.db,
                    run_id,
                )
                .await?
            }
        };
        let working_directory =
            open_candidate_working_directory(candidate.root(), &proposal.working_directory)?;
        match existing_experiment.as_ref().map(|experiment| experiment.status) {
            Some(ExperimentStatus::Reserved) | None => {
                candidate
                    .reverify_submission_boundary(&working_directory)
                    .await?;
            }
            Some(_) => {
                reverify_candidate_submission_or_runtime(
                    &candidate,
                    &working_directory,
                    &run.experiment_id.as_deref().unwrap_or_default(),
                )
                .await?;
            }
        }

        let (experiment_id, submission_id) = match existing_experiment.as_ref() {
            Some(experiment) => (experiment.experiment_id.clone(), experiment.submission_id.clone()),
            None => (
                candidate_experiment_id(run_id),
                candidate_submission_id(run_id),
            ),
        };
        let campaign_repository = CampaignRepository::new(self.db);
        let acceptance = if campaign_repository.candidate_requires_decision_route(run_id)? {
            campaign_repository.accept_decision_code_change_candidate(run_id, now, &self.limits)?
        } else {
            campaign_repository.accept_code_change_candidate(
                run_id,
                &experiment_id,
                &submission_id,
                now,
                &self.limits,
            )?
        };
        let intent = match acceptance {
            ProposalAcceptance::Accepted(intent) => intent,
            ProposalAcceptance::BudgetWaiting { .. } | ProposalAcceptance::CapacityDeferred => {
                return Ok(CampaignSubmission::Deferred)
            }
            ProposalAcceptance::PendingCodeChange => {
                return Err(AppError::Runtime {
                    operation: "accept code-change candidate intent",
                })
            }
        };
        repository.record_candidate_working_directory_identity(
            run_id,
            &intent.experiment.experiment_id,
            working_directory.identity(),
            now,
        )?;
        self.submit_accepted_intent_inner(
            &intent,
            project,
            now,
            Some(admission),
            Some((&candidate, &working_directory)),
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn admit_proposal(
        &self,
        project: &Project,
        campaign_id: &str,
        proposal_id: &str,
        experiment_id: &str,
        submission_id: &str,
        proposal: &proposals::ValidatedProposal,
        now: i64,
    ) -> Result<CampaignProposalAdmission, AppError> {
        self.admit_proposal_inner(
            project,
            campaign_id,
            proposal_id,
            experiment_id,
            submission_id,
            proposal,
            now,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn admit_decision_proposal(
        &self,
        project: &Project,
        campaign_id: &str,
        proposal_id: &str,
        experiment_id: &str,
        submission_id: &str,
        proposal: &proposals::ValidatedProposal,
        reservation: &DecisionReservation,
        now: i64,
    ) -> Result<CampaignProposalAdmission, AppError> {
        self.admit_proposal_inner(
            project,
            campaign_id,
            proposal_id,
            experiment_id,
            submission_id,
            proposal,
            now,
            Some(reservation),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn admit_proposal_inner(
        &self,
        project: &Project,
        campaign_id: &str,
        proposal_id: &str,
        experiment_id: &str,
        submission_id: &str,
        proposal: &proposals::ValidatedProposal,
        now: i64,
        decision_reservation: Option<&DecisionReservation>,
    ) -> Result<CampaignProposalAdmission, AppError> {
        let admission = self.acquire_admission(project)?;
        if proposal.kind() == ProposalKind::CodeChange {
            return self
                .admit_code_change(
                    admission,
                    project,
                    campaign_id,
                    proposal_id,
                    experiment_id,
                    submission_id,
                    proposal,
                    now,
                    decision_reservation,
                )
                .await;
        }
        let runtime_argv = crate::environment::campaign_experiment_runtime_argv(
            &admission.verified_root.anchor.canonical_path,
            campaign_id,
            experiment_id,
            proposal.argv(),
        );
        try_canonical_command_display_os(&runtime_argv)?;
        let add_args = pueue_add_args(
            &project.pueue_group,
            &explicit_working_directory(
                &admission.verified_root.anchor.canonical_path,
                proposal.working_directory(),
            ),
            &runtime_argv,
        );
        validate_add_argv(&add_args)?;
        let accepted = match decision_reservation {
            Some(reservation) => CampaignRepository::new(self.db).accept_decision_proposal(
                campaign_id,
                proposal_id,
                experiment_id,
                submission_id,
                proposal,
                &self.limits,
                now,
                reservation,
                None,
                None,
            )?,
            None => CampaignRepository::new(self.db).accept_proposal(
                campaign_id,
                proposal_id,
                experiment_id,
                submission_id,
                proposal,
                &self.limits,
                now,
            )?,
        };
        match accepted {
            ProposalAcceptance::Accepted(intent) => {
                let matches_requested_intent = intent.proposal.proposal_id == proposal_id;
                Ok(CampaignProposalAdmission::Experiment(
                    AdmittedCampaignProposal {
                        intent,
                        matches_requested_intent,
                        admission,
                    },
                ))
            }
            ProposalAcceptance::BudgetWaiting { .. } => Ok(CampaignProposalAdmission::Deferred),
            ProposalAcceptance::CapacityDeferred => Ok(CampaignProposalAdmission::Deferred),
            ProposalAcceptance::PendingCodeChange => Err(AppError::Validation {
                field: "proposal.kind",
                message: "unexpected code-change acceptance for an experiment proposal",
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn admit_code_change(
        &self,
        admission: CampaignAdmission,
        _project: &Project,
        campaign_id: &str,
        proposal_id: &str,
        experiment_id: &str,
        submission_id: &str,
        proposal: &proposals::ValidatedProposal,
        now: i64,
        decision_reservation: Option<&DecisionReservation>,
    ) -> Result<CampaignProposalAdmission, AppError> {
        let proposals_repository = ProposalRepository::new(self.db);
        match CampaignRepository::new(self.db).resolve_code_change_reentry(
            campaign_id,
            proposal_id,
            proposal.canonical_digest(),
            experiment_id,
            submission_id,
            proposal,
            &self.limits,
            now,
            decision_reservation,
        )? {
            CodeChangeReentry::Missing => {}
            CodeChangeReentry::Deferred => return Ok(CampaignProposalAdmission::Deferred),
            CodeChangeReentry::Rejected(proposal) => {
                return Ok(CampaignProposalAdmission::CodeChangeRejected(proposal))
            }
            CodeChangeReentry::Run(run) => return Ok(CampaignProposalAdmission::CodeChange(run)),
        }
        let live_code_change: i64 = self
            .db
            .connect()?
            .query_row(
                "SELECT COUNT(*) FROM code_change_runs
                 WHERE campaign_id = ?1 AND state NOT IN ('completed', 'rejected')",
                [campaign_id],
                |row| row.get(0),
            )
            .map_err(crate::db::database_error(
                "count live campaign code-change runs",
            ))?;
        if live_code_change != 0 {
            return Err(AppError::Validation {
                field: "code_change",
                message: "a live code-change run already exists in the campaign",
            });
        }

        let project_root = admission.verified_root.anchor.canonical_path.clone();
        let (base_sha, rejection_reason) = match preflight_code_change_runtime() {
            Err(error) => (None, Some(error.code.as_str())),
            Ok(()) => match self.code_change_base_sha(&project_root, campaign_id).await {
                CodeChangeBaseResolution::Available(base_sha) => (Some(base_sha), None),
                CodeChangeBaseResolution::InvalidBest => (None, Some("best_ref_invalid")),
                CodeChangeBaseResolution::Unavailable => (None, Some("base_revision_unavailable")),
            },
        };
        let code_change_run = if rejection_reason.is_none() {
            let base_sha = base_sha.clone().ok_or(AppError::Runtime {
                operation: "read validated code-change base revision",
            })?;
            let code_change_run_id = Uuid::new_v4().to_string();
            Some(NewCodeChangeRun::new(
                &code_change_run_id,
                proposal_id,
                campaign_id,
                base_sha,
                code_change::candidate_ref(campaign_id, proposal_id)?,
                code_change::best_ref(campaign_id)?,
                &code_change_run_id,
                code_change::owned_worktree_relative_path(campaign_id, proposal_id)?
                    .to_string_lossy()
                    .into_owned(),
                now,
            ))
        } else {
            None
        };
        let accepted = match decision_reservation {
            Some(reservation) => CampaignRepository::new(self.db).accept_decision_proposal(
                campaign_id,
                proposal_id,
                experiment_id,
                submission_id,
                proposal,
                &self.limits,
                now,
                reservation,
                code_change_run.as_ref(),
                rejection_reason,
            )?,
            None => CampaignRepository::new(self.db).accept_code_change_proposal(
                campaign_id,
                proposal_id,
                experiment_id,
                submission_id,
                proposal,
                &self.limits,
                now,
                code_change_run.as_ref(),
                rejection_reason,
            )?,
        };
        match accepted {
            ProposalAcceptance::BudgetWaiting { .. } | ProposalAcceptance::CapacityDeferred => {
                Ok(CampaignProposalAdmission::Deferred)
            }
            ProposalAcceptance::Accepted(_) => Err(AppError::Validation {
                field: "proposal.kind",
                message: "code-change proposal unexpectedly created an experiment",
            }),
            ProposalAcceptance::PendingCodeChange => {
                if rejection_reason.is_some() {
                    let rejected = proposals_repository
                        .find_for_campaign(campaign_id, proposal_id)?
                        .ok_or(AppError::Runtime {
                            operation: "read durably rejected code-change proposal",
                        })?;
                    Ok(CampaignProposalAdmission::CodeChangeRejected(rejected))
                } else {
                    let code_change_run_id = code_change_run
                        .as_ref()
                        .map(|run| run.code_change_run_id.as_str())
                        .ok_or(AppError::Runtime {
                            operation: "read validated code-change run identity",
                        })?;
                    let run = CodeChangeRepository::new(self.db)
                        .find_by_id(code_change_run_id)?
                        .ok_or(AppError::Runtime {
                            operation: "read durably reserved code-change run",
                        })?;
                    Ok(CampaignProposalAdmission::CodeChange(run))
                }
            }
        }
    }

    async fn capture_clean_head(&self, project_root: &Path) -> Option<String> {
        let anchor = self
            .execution_policy
            .and_then(ResolvedExecutionPolicy::code_change_git_anchor)?;
        anchor.verify_identity().ok()?;
        let head = run_pinned_git(
            anchor,
            project_root,
            &["rev-parse", "--verify", "HEAD^{commit}"],
        )
        .await
        .ok()?;
        if !head.status.success() {
            return None;
        }
        let status = run_pinned_git(
            anchor,
            project_root,
            &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
        )
        .await
        .ok()?;
        if !status.status.success() || !status.stdout.is_empty() {
            return None;
        }
        let head = String::from_utf8(head.stdout).ok()?;
        code_change::canonical_full_sha(head.trim()).ok()
    }

    async fn code_change_base_sha(
        &self,
        project_root: &Path,
        campaign_id: &str,
    ) -> CodeChangeBaseResolution {
        let anchor = self
            .execution_policy
            .and_then(ResolvedExecutionPolicy::code_change_git_anchor);
        let Some(anchor) = anchor else {
            return CodeChangeBaseResolution::Unavailable;
        };
        if anchor.verify_identity().is_err() {
            return CodeChangeBaseResolution::Unavailable;
        }
        let Ok(best) = code_change::best_ref(campaign_id) else {
            return CodeChangeBaseResolution::Unavailable;
        };
        let best_reference = format!("refs/heads/{best}");
        let best_resolution =
            match resolve_exact_git_ref(anchor, project_root, &best_reference).await {
                Ok(value) => value,
                Err(_) => return CodeChangeBaseResolution::Unavailable,
            };
        match best_resolution {
            GitRefResolution::Invalid => return CodeChangeBaseResolution::InvalidBest,
            GitRefResolution::Present(value) => {
                let Ok(reverified) =
                    resolve_exact_git_ref(anchor, project_root, &best_reference).await
                else {
                    return CodeChangeBaseResolution::InvalidBest;
                };
                if reverified == GitRefResolution::Present(value.clone()) {
                    return CodeChangeBaseResolution::Available(value);
                }
                return CodeChangeBaseResolution::InvalidBest;
            }
            GitRefResolution::Absent => {
                let Ok(reverified) =
                    resolve_exact_git_ref(anchor, project_root, &best_reference).await
                else {
                    return CodeChangeBaseResolution::InvalidBest;
                };
                if reverified != GitRefResolution::Absent {
                    return CodeChangeBaseResolution::InvalidBest;
                }
            }
        }
        let campaign = CampaignRepository::new(self.db)
            .find_by_id(campaign_id)
            .ok()
            .flatten();
        let Some(base) = campaign.and_then(|campaign| campaign.base_revision_sha) else {
            return CodeChangeBaseResolution::Unavailable;
        };
        let base_revision = format!("{base}^{{commit}}");
        let Ok(output) = run_pinned_git(
            anchor,
            project_root,
            &["rev-parse", "--verify", &base_revision],
        )
        .await
        else {
            return CodeChangeBaseResolution::Unavailable;
        };
        if !output.status.success() {
            return CodeChangeBaseResolution::Unavailable;
        }
        let Ok(value) = String::from_utf8(output.stdout) else {
            return CodeChangeBaseResolution::Unavailable;
        };
        let Ok(value) = code_change::canonical_full_sha(value.trim()) else {
            return CodeChangeBaseResolution::Unavailable;
        };
        if value != base {
            CodeChangeBaseResolution::Unavailable
        } else {
            let Ok(reverified) = resolve_exact_git_ref(anchor, project_root, &best_reference).await
            else {
                return CodeChangeBaseResolution::InvalidBest;
            };
            if reverified == GitRefResolution::Absent {
                CodeChangeBaseResolution::Available(value)
            } else {
                CodeChangeBaseResolution::InvalidBest
            }
        }
    }

    pub(crate) async fn submit_admitted_proposal(
        &self,
        admitted: AdmittedCampaignProposal,
        project: &Project,
        now: i64,
    ) -> Result<CampaignSubmission, AppError> {
        self.submit_accepted_intent_inner(
            &admitted.intent,
            project,
            now,
            Some(admitted.admission),
            #[cfg(unix)]
            None,
            false,
        )
        .await
    }

    pub(crate) async fn submit_checkpoint_intent_with_admission(
        &self,
        intent: &ManagedSubmissionIntent,
        project: &Project,
        admission: CampaignAdmission,
        now: i64,
    ) -> Result<CampaignSubmission, AppError> {
        self.submit_accepted_intent_inner(
            intent,
            project,
            now,
            Some(admission),
            #[cfg(unix)]
            None,
            true,
        )
        .await
    }

    async fn submit_accepted_intent_inner(
        &self,
        intent: &ManagedSubmissionIntent,
        project: &Project,
        now: i64,
        admission: Option<CampaignAdmission>,
        #[cfg(unix)] candidate: Option<(
            &code_change::VerifiedCodeChangeWorktree,
            &VerifiedWorkingDirectory,
        )>,
        checkpoint_required: bool,
    ) -> Result<CampaignSubmission, AppError> {
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
        #[cfg(test)]
        run_test_after_clone_hook(self.db, &current.experiment_id);
        let admission = match admission {
            Some(admission) => admission,
            None => self.acquire_admission(&durable_project)?,
        };
        if admission.verified_root.anchor.canonical_path != durable_project.root_path {
            return Err(AppError::Validation {
                field: "campaign.project_root",
                message: "must match the startup-pinned project root",
            });
        }

        let checkpoint_authority = match ResearchRepository::new(self.db)
            .checkpoint_dispatch_authority(
                &durable_project.project_id,
                &current.experiment_id,
                now,
            )? {
            CheckpointDispatchSelection::NotCheckpoint if checkpoint_required => {
                return Err(AppError::Validation {
                    field: "research.checkpoint",
                    message: "checkpoint dispatch authority is required",
                })
            }
            CheckpointDispatchSelection::NotCheckpoint => None,
            CheckpointDispatchSelection::Blocked => return Ok(CampaignSubmission::Deferred),
            CheckpointDispatchSelection::Ready(authority) => {
                let checkpoint = authority.checkpoint();
                #[cfg(unix)]
                if candidate.is_some() {
                    return Err(AppError::Validation {
                        field: "research.checkpoint",
                        message: "checkpoint successor cannot enter the candidate route",
                    });
                }
                if durable_project.project_id != checkpoint.project_id
                    || durable_project.root_path.to_str()
                        != Some(checkpoint.source_root_canonical_path.as_str())
                    || durable_campaign.campaign_id != checkpoint.campaign_id
                    || current.experiment_id != checkpoint.successor_ids.experiment_id
                    || current.campaign_id != checkpoint.campaign_id
                    || current.proposal_id != checkpoint.successor_ids.proposal_id
                    || current.submission_id != checkpoint.successor_ids.submission_id
                    || current.parent_experiment_id.as_deref()
                        != Some(checkpoint.source_experiment_id.as_str())
                    || durable_proposal.proposal_id != checkpoint.successor_ids.proposal_id
                    || durable_proposal.campaign_id != checkpoint.campaign_id
                    || durable_proposal.kind != ProposalKind::Experiment
                    || durable_proposal.argv != checkpoint.retained_argv
                    || durable_proposal.working_directory != checkpoint.source_working_directory
                    || durable_submission.submission_id != checkpoint.successor_ids.submission_id
                    || durable_submission.project_id != checkpoint.project_id
                    || durable_submission.argv != checkpoint.retained_argv
                    || current.code_change_run_id.is_some()
                    || current.code_revision_sha.is_some()
                    || !task_signature_group_matches(
                        &checkpoint.source_raw_task_signature,
                        &durable_project.pueue_group,
                    )
                {
                    return Err(AppError::Validation {
                        field: "research.checkpoint",
                        message: "durable submission values must match checkpoint authority",
                    });
                }
                Some(authority)
            }
        };
        let mut verified_checkpoint = None;
        let mut checkpoint_submitting_won = false;
        if let Some(authority) = checkpoint_authority.as_ref() {
            match authority.successor_status() {
                ExperimentStatus::Reserved => {
                    if current.status != ExperimentStatus::Reserved {
                        return Err(AppError::Validation {
                            field: "research.checkpoint",
                            message: "reserved checkpoint phase changed before dispatch",
                        });
                    }
                    let policy = self.execution_policy.ok_or(AppError::Validation {
                        field: "checkpoint.policy",
                        message: "startup execution policy is required for checkpoint submission",
                    })?;
                    let prepared = match verify_prepared_checkpoint(policy, authority.checkpoint()) {
                        Ok(prepared) => prepared,
                        Err(error) => {
                            experiments.fail_checkpoint_before_add(
                                authority,
                                CHECKPOINT_PRE_ADD_FAILURE_CODE,
                                now,
                            )?;
                            return Err(error);
                        }
                    };
                    verified_checkpoint = Some(prepared);
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
                        })
                        .map(CampaignSubmission::Submitted);
                }
            }
        }

        #[cfg(unix)]
        if candidate.is_some() != current.code_change_run_id.is_some() {
            return Err(AppError::Validation {
                field: "campaign.intent",
                message: "code-change experiments require a verified candidate submission",
            });
        }
        #[cfg(unix)]
        if candidate.is_some() {
            match current.status {
                ExperimentStatus::Submitting => {
                    experiments.mark_unreconciled(
                        &current.experiment_id,
                        ADD_INTERRUPTED_REASON,
                        now,
                    )?;
                    return Err(reconciliation_required());
                }
                ExperimentStatus::Unreconciled => return Err(reconciliation_required()),
                _ => {}
            }
        }
        #[cfg(not(unix))]
        if current.code_change_run_id.is_some() {
            return Err(AppError::Validation {
                field: "campaign.intent",
                message: "code-change experiments require a supported candidate platform",
            });
        }

        #[cfg(unix)]
        let (command_root, working_directory) = if let Some((candidate, cwd)) = candidate.as_ref() {
            match current.status {
                ExperimentStatus::Reserved => {
                    if let Err(error) = candidate.reverify_submission_boundary(cwd).await {
                        require_code_change_runtime_recovery(self.db, &current, now)?;
                        return Err(error);
                    }
                }
                ExperimentStatus::Accepted
                | ExperimentStatus::Succeeded
                | ExperimentStatus::Failed
                | ExperimentStatus::Cancelled => {
                    reverify_candidate_submission_or_runtime(
                        candidate,
                        cwd,
                        &current.experiment_id,
                    )
                    .await?;
                }
                ExperimentStatus::Submitting | ExperimentStatus::Unreconciled => {
                    unreachable!("candidate submission status handled before boundary verification")
                }
            }
            (
                candidate.root().anchor.canonical_path.clone(),
                cwd.canonical_path().to_owned(),
            )
        } else {
            (
                admission.verified_root.anchor.canonical_path.clone(),
                explicit_working_directory(
                    &admission.verified_root.anchor.canonical_path,
                    &durable_proposal.working_directory,
                ),
            )
        };
        #[cfg(not(unix))]
        let (command_root, working_directory) = (
            admission.verified_root.anchor.canonical_path.clone(),
            explicit_working_directory(
                &admission.verified_root.anchor.canonical_path,
                &durable_proposal.working_directory,
            ),
        );
        #[cfg(unix)]
        let mut prepared_runtime = None;

        match current.status {
            ExperimentStatus::Reserved => {
                admission
                    .verified_root
                    .anchor
                    .verify_identity()
                    .map_err(AppError::from)?;
                #[cfg(unix)]
                if let Some((candidate, cwd)) = candidate.as_ref() {
                    if let Err(error) = candidate.reverify_submission_boundary(cwd).await {
                        require_code_change_runtime_recovery(self.db, &current, now)?;
                        return Err(error);
                    }
                    let runtime = match candidate.prepare_runtime_outputs(&current.experiment_id) {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            require_code_change_runtime_recovery(self.db, &current, now)?;
                            return Err(error);
                        }
                    };
                    if let Err(error) = runtime.reverify(candidate.root()) {
                        require_code_change_runtime_recovery(self.db, &current, now)?;
                        return Err(error);
                    }
                    prepared_runtime = Some(runtime);
                }
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
                    })
                    .map(CampaignSubmission::Submitted);
            }
        }

        let mut runtime_argv = crate::environment::campaign_experiment_runtime_argv(
            &command_root,
            &durable_campaign.campaign_id,
            &current.experiment_id,
            &durable_submission.argv,
        );
        #[cfg(unix)]
        if let Some(runtime) = prepared_runtime.as_ref() {
            if let Err(error) = runtime.append_runtime_environment(&mut runtime_argv) {
                require_code_change_runtime_recovery(self.db, &current, now)?;
                return Err(error);
            }
        }
        let expected_command = try_canonical_command_display_os(&runtime_argv)?;
        let add_args = pueue_add_args(
            &durable_project.pueue_group,
            &working_directory,
            &runtime_argv,
        );
        validate_add_argv(&add_args)?;

        if current.status == ExperimentStatus::Reserved {
            let submitting = match checkpoint_authority.as_ref() {
                Some(authority) => {
                    experiments.begin_checkpoint_submitting_or_defer(authority, now)?
                }
                None => experiments.begin_submitting_or_defer(&current.experiment_id, now)?,
            };
            let Some(submitting) = submitting else {
                return Ok(CampaignSubmission::Deferred);
            };
            if checkpoint_authority.is_some()
                && submitting.status != ExperimentStatus::Submitting
            {
                return Err(AppError::Validation {
                    field: "research.checkpoint",
                    message: "checkpoint submitting transition returned an invalid phase",
                });
            }
            if checkpoint_authority.is_some() {
                checkpoint_submitting_won = true;
            }
        }

        if let Some(verified) = verified_checkpoint.as_ref() {
            let policy = self.execution_policy.ok_or(AppError::Validation {
                field: "checkpoint.policy",
                message: "startup execution policy is required for checkpoint submission",
            })?;
            if let Err(error) = verified.reverify(policy) {
                if checkpoint_submitting_won {
                    let authority = checkpoint_authority
                        .as_ref()
                        .expect("checkpoint CAS must retain its authority");
                    experiments.fail_checkpoint_before_add(
                        authority,
                        CHECKPOINT_PRE_ADD_FAILURE_CODE,
                        now,
                    )?;
                }
                return Err(error);
            }
        }

        let task_id = match self.pueue.add(&add_args).await {
            Ok(task_id) => task_id,
            Err(error) => {
                experiments.mark_unreconciled(&current.experiment_id, ADD_UNKNOWN_REASON, now)?;
                return Err(error);
            }
        };
        #[cfg(unix)]
        if let Some((candidate, cwd)) = candidate.as_ref() {
            let valid = match prepared_runtime.as_ref() {
                Some(runtime) => candidate
                    .reverify_submission_runtime_boundary(cwd, runtime)
                    .await
                    .is_ok(),
                None => candidate.reverify_submission_boundary(cwd).await.is_ok(),
            };
            if !valid {
                experiments.mark_unreconciled(&current.experiment_id, ADD_IDENTITY_REASON, now)?;
                return Err(reconciliation_required());
            }
        }
        let tasks = match self.pueue.status_json().await {
            Ok(tasks) => tasks,
            Err(error) => {
                experiments.mark_unreconciled(&current.experiment_id, ADD_IDENTITY_REASON, now)?;
                return Err(error);
            }
        };
        let mut id_matches = tasks.iter().filter(|task| task.id == task_id);
        let task = id_matches.next();
        if task.is_none() || id_matches.next().is_some() {
            experiments.mark_unreconciled(&current.experiment_id, ADD_IDENTITY_REASON, now)?;
            return Err(reconciliation_required());
        }
        let task = task.expect("checked one Pueue task ID match");
        let task_signature =
            if task.group == durable_project.pueue_group && task.command == expected_command {
                managed_task_run_signature(task)
            } else {
                None
            };
        let Some(task_signature) = task_signature else {
            experiments.mark_unreconciled(&current.experiment_id, ADD_IDENTITY_REASON, now)?;
            return Err(reconciliation_required());
        };
        #[cfg(unix)]
        if let Some((candidate, cwd)) = candidate.as_ref() {
            let valid = match prepared_runtime.as_ref() {
                Some(runtime) => candidate
                    .reverify_submission_runtime_boundary(cwd, runtime)
                    .await
                    .is_ok(),
                None => candidate.reverify_submission_boundary(cwd).await.is_ok(),
            };
            if !valid {
                experiments.mark_unreconciled(&current.experiment_id, ADD_IDENTITY_REASON, now)?;
                return Err(reconciliation_required());
            }
        }
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
            .map(CampaignSubmission::Submitted)
    }

    pub(crate) fn acquire_admission(
        &self,
        project: &Project,
    ) -> Result<CampaignAdmission, AppError> {
        let root_anchor = match self.root_anchor.as_ref() {
            Some(anchor) => anchor.clone(),
            None => ProjectRootAnchor::resolve(&project.root_path).map_err(AppError::from)?,
        };
        if root_anchor.canonical_path != project.root_path {
            return Err(AppError::Validation {
                field: "campaign.project_root",
                message: "must match the startup-pinned project root",
            });
        }
        let verified_root = root_anchor.verify_identity().map_err(AppError::from)?;
        let project_lock = ProjectAdmissionLock::try_acquire(&verified_root)
            .map_err(AppError::from)?
            .ok_or(AppError::Runtime {
                operation: "acquire project submission admission lock",
            })?;
        let verified_root = root_anchor.verify_identity().map_err(AppError::from)?;
        Ok(CampaignAdmission {
            verified_root,
            _lock: project_lock,
        })
    }
}

struct PinnedGitOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GitRefPresence {
    Present,
    Absent,
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum GitRefResolution {
    Present(String),
    Absent,
    Invalid,
}

fn read_bounded_git_file(path: &Path) -> Result<Vec<u8>, AppError> {
    use std::io::Read;

    let file = std::fs::File::open(path).map_err(|_| AppError::Runtime {
        operation: "read local Git metadata",
    })?;
    let mut bytes = Vec::new();
    file.take((MAX_GIT_OUTPUT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| AppError::Runtime {
            operation: "read local Git metadata",
        })?;
    if bytes.len() > MAX_GIT_OUTPUT_BYTES {
        return Err(AppError::Validation {
            field: "git.metadata",
            message: "exceeds the bounded Git metadata size",
        });
    }
    Ok(bytes)
}

fn local_git_dir(project_root: &Path) -> Result<Option<std::path::PathBuf>, AppError> {
    let dot_git = project_root.join(".git");
    let metadata = match std::fs::symlink_metadata(&dot_git) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(AppError::Runtime {
                operation: "inspect local Git metadata",
            })
        }
    };
    if metadata.is_dir() {
        return Ok(Some(dot_git));
    }
    if !metadata.is_file() {
        return Err(AppError::Validation {
            field: "git.metadata",
            message: "local Git metadata must be a regular directory or worktree file",
        });
    }
    let contents = read_bounded_git_file(&dot_git)?;
    let contents = std::str::from_utf8(&contents).map_err(|_| AppError::Validation {
        field: "git.metadata",
        message: "local Git metadata must be valid UTF-8",
    })?;
    let gitdir = contents
        .lines()
        .find_map(|line| line.trim().strip_prefix("gitdir:"))
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.contains('\0'))
        .ok_or(AppError::Validation {
            field: "git.metadata",
            message: "worktree Git metadata must identify a Git directory",
        })?;
    let gitdir = Path::new(gitdir);
    Ok(Some(if gitdir.is_absolute() {
        gitdir.to_owned()
    } else {
        project_root.join(gitdir)
    }))
}

fn validate_local_git_config_file(path: &Path) -> Result<(), AppError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| AppError::Runtime {
        operation: "inspect local Git configuration",
    })?;
    if !metadata.is_file() {
        return Err(AppError::Validation {
            field: "git.config",
            message: "local Git configuration must be a regular file",
        });
    }
    let contents = read_bounded_git_file(path)?;
    if contents.windows(3).any(|window| window == b"\xef\xbb\xbf") {
        return Err(AppError::Validation {
            field: "git.config",
            message: "local Git configuration contains a byte-order mark",
        });
    }
    let contents = std::str::from_utf8(&contents).map_err(|_| AppError::Validation {
        field: "git.config",
        message: "local Git configuration must be valid UTF-8",
    })?;
    if contents.lines().any(|line| line.trim_end().ends_with('\\')) {
        return Err(AppError::Validation {
            field: "git.config",
            message: "local Git configuration contains a line continuation",
        });
    }
    let mut section = String::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(header) = line.strip_prefix('[') {
            let Some(end) = header.find(']') else {
                return Err(AppError::Validation {
                    field: "git.config",
                    message: "local Git configuration has an invalid section",
                });
            };
            if !header[end + 1..].trim().is_empty() {
                return Err(AppError::Validation {
                    field: "git.config",
                    message: "local Git configuration has trailing section data",
                });
            }
            section = header[..end]
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_ascii_lowercase();
            if matches!(section.as_str(), "filter" | "include" | "includeif") {
                return Err(AppError::Validation {
                    field: "git.config",
                    message: "local Git configuration contains an execution channel",
                });
            }
            continue;
        }
        let key = line
            .split(['=', ' ', '\t'])
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        if key.is_empty() {
            return Err(AppError::Validation {
                field: "git.config",
                message: "local Git configuration has an invalid key",
            });
        }
        let full_key = if section.is_empty() {
            key.clone()
        } else {
            format!("{section}.{key}")
        };
        if section == "filter"
            || key.starts_with("filter.")
            || full_key == "core.worktree"
            || full_key == "include.path"
            || full_key == "includeif.path"
            || git_config_execution_channel(&full_key)
        {
            return Err(AppError::Validation {
                field: "git.config",
                message: "local Git configuration contains an execution or path channel",
            });
        }
    }
    Ok(())
}

fn git_config_execution_channel(full_key: &str) -> bool {
    let normalized = full_key.to_ascii_lowercase();
    let mut components = normalized.split('.');
    match components.next() {
        Some("filter") => true,
        Some("diff") => components.last().is_some_and(|key| {
            matches!(key, "command" | "external" | "textconv" | "trustexitcode")
        }),
        _ => false,
    }
}

fn validate_local_git_config(project_root: &Path) -> Result<(), AppError> {
    let Some(gitdir) = local_git_dir(project_root)? else {
        return Ok(());
    };
    let mut gitdirs = vec![gitdir.clone()];
    let commondir = gitdir.join("commondir");
    match std::fs::symlink_metadata(&commondir) {
        Ok(metadata) if metadata.is_file() => {
            let contents = read_bounded_git_file(&commondir)?;
            let contents = std::str::from_utf8(&contents).map_err(|_| AppError::Validation {
                field: "git.metadata",
                message: "common Git metadata must be valid UTF-8",
            })?;
            let common = contents
                .lines()
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty() && !value.contains('\0'))
                .ok_or(AppError::Validation {
                    field: "git.metadata",
                    message: "common Git metadata must identify a Git directory",
                })?;
            let common = Path::new(common);
            gitdirs.push(if common.is_absolute() {
                common.to_owned()
            } else {
                gitdir.join(common)
            });
        }
        Ok(_) => {
            return Err(AppError::Validation {
                field: "git.metadata",
                message: "common Git metadata must be a regular file",
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err(AppError::Runtime {
                operation: "inspect local Git metadata",
            })
        }
    }
    for gitdir in gitdirs {
        for name in ["config", "config.worktree"] {
            let path = gitdir.join(name);
            match std::fs::symlink_metadata(&path) {
                Ok(_) => validate_local_git_config_file(&path)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {
                    return Err(AppError::Runtime {
                        operation: "inspect local Git configuration",
                    })
                }
            }
        }
    }
    Ok(())
}

fn parse_packed_refs(contents: &[u8], reference: &str) -> GitRefPresence {
    let Ok(contents) = std::str::from_utf8(contents) else {
        return GitRefPresence::Invalid;
    };
    let mut presence = GitRefPresence::Absent;
    let mut target_entries = 0;
    let mut previous_was_tag = false;
    for line in contents.lines() {
        if line.is_empty() || line.starts_with('#') {
            previous_was_tag = false;
            continue;
        }
        if let Some(peeled) = line.strip_prefix('^') {
            if !previous_was_tag || code_change::canonical_full_sha(peeled).is_err() {
                return GitRefPresence::Invalid;
            }
            previous_was_tag = false;
            continue;
        }
        if line.trim() != line {
            return GitRefPresence::Invalid;
        }
        let mut fields = line.split(' ');
        let Some(object_id) = fields.next() else {
            return GitRefPresence::Invalid;
        };
        let Some(ref_name) = fields.next() else {
            return GitRefPresence::Invalid;
        };
        if fields.next().is_some()
            || code_change::canonical_full_sha(object_id).is_err()
            || !ref_name.starts_with("refs/")
        {
            return GitRefPresence::Invalid;
        }
        if ref_name == reference {
            if target_entries != 0 {
                return GitRefPresence::Invalid;
            }
            target_entries += 1;
            presence = GitRefPresence::Present;
        }
        previous_was_tag = ref_name.starts_with("refs/tags/");
    }
    presence
}

async fn resolve_exact_git_ref(
    anchor: &crate::execution_policy::ExecutableAnchor,
    project_root: &Path,
    reference: &str,
) -> Result<GitRefResolution, AppError> {
    match classify_exact_git_ref(anchor, project_root, reference).await? {
        GitRefPresence::Absent => Ok(GitRefResolution::Absent),
        GitRefPresence::Invalid => Ok(GitRefResolution::Invalid),
        GitRefPresence::Present => {
            let revision = format!("{reference}^{{commit}}");
            let output =
                run_pinned_git(anchor, project_root, &["rev-parse", "--verify", &revision]).await?;
            if !output.status.success() {
                return Ok(GitRefResolution::Invalid);
            }
            let Ok(value) = String::from_utf8(output.stdout) else {
                return Ok(GitRefResolution::Invalid);
            };
            let Ok(value) = code_change::canonical_full_sha(value.trim()) else {
                return Ok(GitRefResolution::Invalid);
            };
            Ok(GitRefResolution::Present(value))
        }
    }
}

fn path_has_symlink_component(path: &Path) -> Result<bool, AppError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(Path::new("/")),
            Component::CurDir => continue,
            Component::ParentDir => {
                return Err(AppError::Validation {
                    field: "git.metadata",
                    message: "Git metadata path must not traverse a parent",
                })
            }
            Component::Normal(name) => current.push(name),
        }
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => return Ok(true),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(_) => {
                return Err(AppError::Runtime {
                    operation: "inspect pinned Git metadata path",
                })
            }
        }
    }
    Ok(false)
}

async fn git_metadata_path(
    anchor: &crate::execution_policy::ExecutableAnchor,
    project_root: &Path,
    path_name: &str,
) -> Result<std::path::PathBuf, AppError> {
    let output = run_pinned_git(
        anchor,
        project_root,
        &["rev-parse", "--git-path", path_name],
    )
    .await?;
    if !output.status.success() {
        return Err(AppError::Runtime {
            operation: "resolve pinned Git metadata path",
        });
    }
    let path = String::from_utf8(output.stdout).map_err(|_| AppError::Runtime {
        operation: "read pinned Git metadata path",
    })?;
    let path = path.trim();
    if path.is_empty() || path.contains('\0') {
        return Err(AppError::Runtime {
            operation: "validate pinned Git metadata path",
        });
    }
    let path = Path::new(path);
    Ok(if path.is_absolute() {
        path.to_owned()
    } else {
        project_root.join(path)
    })
}

async fn classify_exact_git_ref(
    anchor: &crate::execution_policy::ExecutableAnchor,
    project_root: &Path,
    reference: &str,
) -> Result<GitRefPresence, AppError> {
    if !reference.starts_with("refs/heads/") {
        return Ok(GitRefPresence::Invalid);
    }
    let output = run_pinned_git(
        anchor,
        project_root,
        &["for-each-ref", "--format=%(refname)", "--", reference],
    )
    .await?;
    if !output.status.success() {
        return Ok(GitRefPresence::Invalid);
    }
    let git_present = String::from_utf8(output.stdout)
        .map_err(|_| AppError::Runtime {
            operation: "read pinned Git ref",
        })?
        .lines()
        .any(|line| line == reference);
    let loose_path = git_metadata_path(anchor, project_root, reference).await?;
    if path_has_symlink_component(&loose_path).unwrap_or(true) {
        return Ok(GitRefPresence::Invalid);
    }
    let loose_presence = match std::fs::symlink_metadata(loose_path) {
        Ok(metadata) if metadata.is_file() => GitRefPresence::Present,
        Ok(_) => GitRefPresence::Invalid,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => GitRefPresence::Absent,
        Err(_) => GitRefPresence::Invalid,
    };
    if loose_presence == GitRefPresence::Invalid {
        return Ok(GitRefPresence::Invalid);
    }
    let packed_path = git_metadata_path(anchor, project_root, "packed-refs").await?;
    if path_has_symlink_component(&packed_path).unwrap_or(true) {
        return Ok(GitRefPresence::Invalid);
    }
    let packed_presence = match std::fs::symlink_metadata(&packed_path) {
        Ok(metadata) if metadata.is_file() => match read_bounded_git_file(&packed_path) {
            Ok(contents) => parse_packed_refs(&contents, reference),
            Err(_) => GitRefPresence::Invalid,
        },
        Ok(_) => GitRefPresence::Invalid,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => GitRefPresence::Absent,
        Err(_) => GitRefPresence::Invalid,
    };
    if packed_presence == GitRefPresence::Invalid {
        return Ok(GitRefPresence::Invalid);
    }
    if git_present
        || loose_presence == GitRefPresence::Present
        || packed_presence == GitRefPresence::Present
    {
        Ok(GitRefPresence::Present)
    } else {
        Ok(GitRefPresence::Absent)
    }
}

fn pinned_git_argv(argv: &[&str]) -> Vec<OsString> {
    let mut result = Vec::with_capacity(argv.len() + 20);
    for argument in [
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.pager=cat",
        "-c",
        "credential.helper=",
        "-c",
        "core.askPass=",
        "-c",
        "core.sshCommand=",
        "-c",
        "commit.gpgSign=false",
        "-c",
        "tag.gpgSign=false",
        "-c",
        "user.signingKey=",
        "--no-pager",
    ] {
        result.push(OsString::from(argument));
    }
    result.extend(argv.iter().map(OsString::from));
    result
}

fn pinned_git_environment() -> &'static [(&'static str, &'static str)] {
    PINNED_GIT_ENVIRONMENT
}

fn validate_git_output_len(length: usize) -> Result<(), AppError> {
    if length <= MAX_GIT_OUTPUT_BYTES {
        Ok(())
    } else {
        Err(AppError::Validation {
            field: "git.output",
            message: "exceeds the bounded Git output size",
        })
    }
}

#[cfg(unix)]
fn verified_git_program(verified: &crate::execution_policy::VerifiedExecutable) -> OsString {
    if !cfg!(target_os = "linux") {
        return verified.anchor.canonical_path.as_os_str().to_owned();
    }
    OsString::from(format!(
        "/proc/self/fd/{}",
        std::os::unix::io::AsRawFd::as_raw_fd(&verified.file)
    ))
}

#[cfg(target_os = "linux")]
fn inherited_verified_git_fd_flags(flags: libc::c_int) -> libc::c_int {
    flags & !libc::FD_CLOEXEC
}

#[cfg(target_os = "linux")]
fn inherit_verified_git_fd(command: &mut std::process::Command, fd: libc::c_int) {
    use std::os::unix::process::CommandExt;

    unsafe {
        command.pre_exec(move || {
            let flags = libc::fcntl(fd, libc::F_GETFD);
            if flags < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::fcntl(fd, libc::F_SETFD, inherited_verified_git_fd_flags(flags)) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(unix)]
async fn run_pinned_git(
    anchor: &crate::execution_policy::ExecutableAnchor,
    project_root: &Path,
    argv: &[&str],
) -> Result<PinnedGitOutput, AppError> {
    validate_local_git_config(project_root)?;
    let verified = anchor.verify_identity().map_err(AppError::from)?;
    use std::os::unix::process::CommandExt;

    let mut command = std::process::Command::new(verified_git_program(&verified));
    command
        .args(pinned_git_argv(argv))
        .current_dir(project_root)
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (name, value) in pinned_git_environment() {
        command.env(name, value);
    }
    command.process_group(0);
    #[cfg(target_os = "linux")]
    inherit_verified_git_fd(
        &mut command,
        std::os::fd::AsRawFd::as_raw_fd(&verified.file),
    );
    let mut command = tokio::process::Command::from(command);
    command.kill_on_drop(true);
    let mut child = command.spawn().map_err(|_| AppError::Runtime {
        operation: "spawn pinned Git",
    })?;
    let pid = child.id().ok_or(AppError::Runtime {
        operation: "read pinned Git process ID",
    })? as libc::pid_t;
    let Some(stdout) = child.stdout.take() else {
        terminate_pinned_git_group(pid, &mut child).await;
        return Err(AppError::Runtime {
            operation: "capture pinned Git stdout",
        });
    };
    let Some(stderr) = child.stderr.take() else {
        terminate_pinned_git_group(pid, &mut child).await;
        return Err(AppError::Runtime {
            operation: "capture pinned Git stderr",
        });
    };
    let mut stdout_task = tokio::spawn(read_bounded_git_output(stdout));
    let mut stderr_task = tokio::spawn(read_bounded_git_output(stderr));
    let mut wait = Box::pin(child.wait());
    let mut timeout = Box::pin(tokio::time::sleep(GIT_RUNTIME));
    let mut status = None;
    let mut stdout_bytes = None;
    let mut stderr_bytes = None;
    loop {
        if status.is_some() && stdout_bytes.is_some() && stderr_bytes.is_some() {
            break;
        }
        tokio::select! {
            result = &mut wait, if status.is_none() => {
                match result {
                    Ok(value) => status = Some(value),
                    Err(_) => {
                        drop(wait);
                        abort_pinned_git(pid, &mut child, &mut stdout_task, &mut stderr_task).await;
                        return Err(AppError::Runtime { operation: "wait for pinned Git" });
                    }
                }
            }
            result = &mut stdout_task, if stdout_bytes.is_none() => {
                match result {
                    Ok(Ok(value)) => stdout_bytes = Some(value),
                    Ok(Err(error)) => {
                        drop(wait);
                        abort_pinned_git(pid, &mut child, &mut stdout_task, &mut stderr_task).await;
                        return Err(error);
                    }
                    Err(_) => {
                        drop(wait);
                        abort_pinned_git(pid, &mut child, &mut stdout_task, &mut stderr_task).await;
                        return Err(AppError::Runtime { operation: "read pinned Git stdout" });
                    }
                }
            }
            result = &mut stderr_task, if stderr_bytes.is_none() => {
                match result {
                    Ok(Ok(value)) => stderr_bytes = Some(value),
                    Ok(Err(error)) => {
                        drop(wait);
                        abort_pinned_git(pid, &mut child, &mut stdout_task, &mut stderr_task).await;
                        return Err(error);
                    }
                    Err(_) => {
                        drop(wait);
                        abort_pinned_git(pid, &mut child, &mut stdout_task, &mut stderr_task).await;
                        return Err(AppError::Runtime { operation: "read pinned Git stderr" });
                    }
                }
            }
            _ = &mut timeout => {
                drop(wait);
                abort_pinned_git(pid, &mut child, &mut stdout_task, &mut stderr_task).await;
                return Err(AppError::Runtime { operation: "pinned Git timeout" });
            }
        }
    }
    drop(wait);
    if let Err(error) = anchor.verify_identity() {
        abort_pinned_git(pid, &mut child, &mut stdout_task, &mut stderr_task).await;
        return Err(error.into());
    }
    Ok(PinnedGitOutput {
        status: status.expect("Git status was checked"),
        stdout: stdout_bytes.expect("Git stdout was checked"),
    })
}

#[cfg(not(unix))]
async fn run_pinned_git(
    _anchor: &crate::execution_policy::ExecutableAnchor,
    _project_root: &Path,
    _argv: &[&str],
) -> Result<PinnedGitOutput, AppError> {
    Err(AppError::Runtime {
        operation: "run pinned Git on this platform",
    })
}

#[cfg(unix)]
async fn read_bounded_git_output<R>(mut reader: R) -> Result<Vec<u8>, AppError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let mut bytes = Vec::with_capacity(MAX_GIT_OUTPUT_BYTES.min(8192));
    let mut buffer = [0u8; 8192];
    loop {
        let count = reader
            .read(&mut buffer)
            .await
            .map_err(|_| AppError::Runtime {
                operation: "read pinned Git output",
            })?;
        if count == 0 {
            return Ok(bytes);
        }
        let next_length = bytes.len().saturating_add(count);
        validate_git_output_len(next_length)?;
        bytes.extend_from_slice(&buffer[..count]);
    }
}

#[cfg(unix)]
async fn terminate_pinned_git_group(pid: libc::pid_t, child: &mut tokio::process::Child) {
    unsafe {
        libc::kill(-pid, libc::SIGKILL);
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[cfg(unix)]
async fn abort_pinned_git(
    pid: libc::pid_t,
    child: &mut tokio::process::Child,
    stdout_task: &mut tokio::task::JoinHandle<Result<Vec<u8>, AppError>>,
    stderr_task: &mut tokio::task::JoinHandle<Result<Vec<u8>, AppError>>,
) {
    terminate_pinned_git_group(pid, child).await;
    stdout_task.abort();
    stderr_task.abort();
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
        || intent.experiment.code_change_run_id != experiment.code_change_run_id
        || intent.experiment.code_revision_sha != experiment.code_revision_sha
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

fn pueue_add_args(group: &str, project_root: &Path, argv: &[OsString]) -> Vec<OsString> {
    let mut add_args = Vec::with_capacity(argv.len() + 5);
    add_args.push(OsString::from("-g"));
    add_args.push(OsString::from(group));
    add_args.push(OsString::from("--working-directory"));
    add_args.push(project_root.as_os_str().to_owned());
    add_args.push(OsString::from("--"));
    add_args.extend(argv.iter().cloned());
    add_args
}

fn explicit_working_directory(project_root: &Path, relative: &str) -> std::path::PathBuf {
    if relative == "." {
        project_root.to_owned()
    } else {
        project_root.join(relative)
    }
}

#[cfg(unix)]
fn open_candidate_working_directory(
    candidate: &VerifiedProjectRoot,
    relative: &str,
) -> Result<VerifiedWorkingDirectory, AppError> {
    VerifiedWorkingDirectory::open_descendant(candidate, Path::new(relative))
        .map_err(AppError::from)
}

#[cfg(unix)]
async fn reverify_candidate_submission_or_runtime(
    candidate: &code_change::VerifiedCodeChangeWorktree,
    working_directory: &VerifiedWorkingDirectory,
    experiment_id: &str,
) -> Result<(), AppError> {
    match candidate
        .reverify_submission_boundary(working_directory)
        .await
    {
        Ok(()) => Ok(()),
        Err(_) => {
            candidate
                .reverify_result_ingestion_boundary(working_directory, experiment_id)
                .await
                .map(|_| ())
        }
    }
}

#[cfg(unix)]
fn candidate_experiment_id(run_id: &str) -> String {
    format!("code-change-experiment:{run_id}")
}

#[cfg(unix)]
fn candidate_submission_id(run_id: &str) -> String {
    format!("code-change-submission:{run_id}")
}

fn reconciliation_required() -> AppError {
    AppError::Validation {
        field: "experiment",
        message: "submission may already have reached Pueue; reconciliation is required",
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        ffi::OsString,
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use super::*;
    use crate::{
        db::{
            CodeChangeRepository, DecisionRepository, ExperimentRepository,
        },
        execution_policy::ExecutableAnchor,
        models::{EventKind, ExperimentTerminalOutcome, NewCodeChangeRun, NewEvent, NewProject},
        proposals::{self, ProposalInput},
        state::ObjectiveSnapshot,
    };
    use crate::pueue::PueueTask;
    use rusqlite::TransactionBehavior;
    use tempfile::TempDir;

    struct CoordinatorFenceFixture {
        db: Db,
        _root: TempDir,
        project: Project,
    }

    struct LockCheckingPueue {
        root: PathBuf,
        add_lock_held: Arc<Mutex<Vec<bool>>>,
        status_lock_held: Arc<Mutex<Vec<bool>>>,
        task: Arc<Mutex<Option<PueueTask>>>,
    }

    struct CheckpointLeaseProbePueue {
        policy: crate::execution_policy::ResolvedExecutionPolicy,
        campaign_id: String,
        review_id: String,
        expected: crate::environment::ResearchFileRecord,
        task: Arc<Mutex<Option<PueueTask>>>,
        add_cleanup_blocked: Arc<Mutex<Vec<bool>>>,
        status_cleanup_blocked: Arc<Mutex<Vec<bool>>>,
    }

    impl CheckpointLeaseProbePueue {
        fn new(
            policy: &crate::execution_policy::ResolvedExecutionPolicy,
            campaign_id: &str,
            review_id: &str,
            expected: &crate::environment::ResearchFileRecord,
        ) -> Self {
            Self {
                policy: policy.clone(),
                campaign_id: campaign_id.to_owned(),
                review_id: review_id.to_owned(),
                expected: expected.clone(),
                task: Arc::new(Mutex::new(None)),
                add_cleanup_blocked: Arc::new(Mutex::new(Vec::new())),
                status_cleanup_blocked: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn cleanup_is_blocked(&self) -> bool {
            let retained_path = self
                .policy
                .code_change_state_root_path()
                .join(&self.expected.relative_path);
            assert!(retained_path.exists(), "retained file must exist during Pueue call");
            match crate::environment::cleanup_retained_research_file(
                &self.policy,
                &self.campaign_id,
                &self.review_id,
                &self.expected,
            ) {
                Err(error) => {
                    assert_eq!(
                        error.code,
                        crate::execution_policy::PolicyViolationCode::TempUnsafe
                    );
                    assert_eq!(
                        error.detail,
                        crate::execution_policy::PolicyViolationDetail::TempUnsafe(
                            crate::execution_policy::TempUnsafeReason::IoFailure
                        )
                    );
                    assert!(retained_path.exists(), "failed EX cleanup must preserve retained file");
                    true
                }
                Ok(()) => panic!("independent EX cleanup acquired while dispatcher was active"),
            }
        }
    }

    struct NoAddPueue {
        add_count: Arc<Mutex<usize>>,
    }

    impl NoAddPueue {
        fn new() -> Self {
            Self {
                add_count: Arc::new(Mutex::new(0)),
            }
        }
    }

    impl LockCheckingPueue {
        fn new(root: PathBuf) -> Self {
            Self {
                root,
                add_lock_held: Arc::new(Mutex::new(Vec::new())),
                status_lock_held: Arc::new(Mutex::new(Vec::new())),
                task: Arc::new(Mutex::new(None)),
            }
        }

        fn lock_is_held(&self) -> bool {
            let root = ProjectRootAnchor::resolve(&self.root)
                .unwrap()
                .verify_identity()
                .unwrap();
            ProjectAdmissionLock::try_acquire(&root)
                .unwrap()
                .is_none()
        }
    }

    #[async_trait]
    impl crate::pueue::PueueApi for LockCheckingPueue {
        async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
            self.status_lock_held
                .lock()
                .unwrap()
                .push(self.lock_is_held());
            Ok(vec![self.task.lock().unwrap().clone().unwrap()])
        }

        async fn add(&self, args: &[OsString]) -> Result<i64, AppError> {
            self.add_lock_held
                .lock()
                .unwrap()
                .push(self.lock_is_held());
            let separator = args
                .iter()
                .position(|argument| argument == &OsString::from("--"))
                .unwrap();
            let command = crate::reconcile::try_canonical_command_display_os(
                &args[separator + 1..],
            )?;
            *self.task.lock().unwrap() = Some(PueueTask {
                id: 17,
                group: args[1].to_string_lossy().into_owned(),
                command,
                state: "Running".to_owned(),
                enqueued_at: Some("900".to_owned()),
                started_at: Some("1000".to_owned()),
                ended_at: None,
                result: None,
            });
            Ok(17)
        }

        async fn kill(&self, _task_id: i64) -> Result<(), AppError> {
            Ok(())
        }

        async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
            Ok(())
        }

        async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
            Ok(())
        }
    }

    #[async_trait]
    impl crate::pueue::PueueApi for CheckpointLeaseProbePueue {
        async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
            self.status_cleanup_blocked
                .lock()
                .unwrap()
                .push(self.cleanup_is_blocked());
            Ok(vec![self.task.lock().unwrap().clone().unwrap()])
        }

        async fn add(&self, args: &[OsString]) -> Result<i64, AppError> {
            self.add_cleanup_blocked
                .lock()
                .unwrap()
                .push(self.cleanup_is_blocked());
            let separator = args
                .iter()
                .position(|argument| argument == &OsString::from("--"))
                .unwrap();
            let command = crate::reconcile::try_canonical_command_display_os(
                &args[separator + 1..],
            )?;
            *self.task.lock().unwrap() = Some(PueueTask {
                id: 77,
                group: args[1].to_string_lossy().into_owned(),
                command,
                state: "Running".to_owned(),
                enqueued_at: Some("900".to_owned()),
                started_at: Some("1000".to_owned()),
                ended_at: None,
                result: None,
            });
            Ok(77)
        }

        async fn kill(&self, _task_id: i64) -> Result<(), AppError> {
            Ok(())
        }

        async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
            Ok(())
        }

        async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
            Ok(())
        }
    }

    #[async_trait]
    impl crate::pueue::PueueApi for NoAddPueue {
        async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
            Ok(Vec::new())
        }

        async fn add(&self, _args: &[OsString]) -> Result<i64, AppError> {
            *self.add_count.lock().unwrap() += 1;
            Err(AppError::Runtime {
                operation: "unexpected checkpoint Pueue add",
            })
        }

        async fn kill(&self, _task_id: i64) -> Result<(), AppError> {
            Ok(())
        }

        async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
            Ok(())
        }

        async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
            Ok(())
        }
    }

    fn checkpoint_retained_path(db: &Db, successor_id: &str) -> PathBuf {
        let connection = db.connect().unwrap();
        let raw: String = connection
            .query_row(
                "SELECT checkpoint_json FROM research_reviews
                 WHERE successor_experiment_id = ?1",
                [successor_id],
                |row| row.get(0),
            )
            .unwrap();
        let checkpoint = crate::research_checkpoint::parse_prepared_checkpoint(&raw).unwrap();
        db.path()
            .parent()
            .unwrap()
            .join("state")
            .join(checkpoint.retained_checkpoint.relative_path)
    }

    fn tamper_checkpoint_retained_file(db: &Db, successor_id: &str) {
        let path = checkpoint_retained_path(db, successor_id);
        assert!(path.exists(), "checkpoint retained file must exist before tamper");
        std::fs::write(path, b"tampered by stale clone hook\n").unwrap();
    }

    fn stale_submitting_after_clone(db: &Db, successor_id: &str) {
        db.connect()
            .unwrap()
            .execute(
                "UPDATE experiments SET status = 'submitting'
                 WHERE experiment_id = ?1",
                [successor_id],
            )
            .unwrap();
        let authority = match ResearchRepository::new(db)
            .checkpoint_dispatch_authority("project", successor_id, 3_105)
            .unwrap()
        {
            CheckpointDispatchSelection::Ready(authority) => authority,
            other => panic!("expected fresh submitting authority, got {other:?}"),
        };
        assert_eq!(authority.successor_status(), ExperimentStatus::Submitting);
        tamper_checkpoint_retained_file(db, successor_id);
    }

    fn stale_unreconciled_after_clone(db: &Db, successor_id: &str) {
        let connection = db.connect().unwrap();
        connection
            .execute(
                "UPDATE experiments
                 SET status = 'unreconciled', failure_code = 'identity-mismatch'
                 WHERE experiment_id = ?1",
                [successor_id],
            )
            .unwrap();
        let submission_id: String = connection
            .query_row(
                "SELECT submission_id FROM experiments WHERE experiment_id = ?1",
                [successor_id],
                |row| row.get(0),
            )
            .unwrap();
        connection
            .execute(
                "UPDATE submissions SET status = 'unreconciled'
                 WHERE submission_id = ?1",
                [&submission_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE budget_reservations SET status = 'reserved'
                 WHERE experiment_id = ?1 AND dimension = 'experiment'",
                [successor_id],
            )
            .unwrap();
        let authority = match ResearchRepository::new(db)
            .checkpoint_dispatch_authority("project", successor_id, 3_105)
            .unwrap()
        {
            CheckpointDispatchSelection::Ready(authority) => authority,
            other => panic!("expected fresh unreconciled authority, got {other:?}"),
        };
        assert_eq!(authority.successor_status(), ExperimentStatus::Unreconciled);
        tamper_checkpoint_retained_file(db, successor_id);
    }

    fn coherent_group_after_clone(db: &Db, successor_id: &str) {
        crate::research_checkpoint::rewrite_checkpoint_group_coherently(db, successor_id);
        let authority = match ResearchRepository::new(db)
            .checkpoint_dispatch_authority("project", successor_id, 3_105)
            .unwrap()
        {
            CheckpointDispatchSelection::Ready(authority) => authority,
            other => panic!("expected fresh coherent-group authority, got {other:?}"),
        };
        assert_eq!(authority.successor_status(), ExperimentStatus::Reserved);
    }

    impl CoordinatorFenceFixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let project_root = root.path().join("project");
            std::fs::create_dir_all(&project_root).unwrap();
            let db = Db::open(&root.path().join("state.sqlite3")).unwrap();
            ProjectRepository::new(&db)
                .register(&NewProject::new(
                    "project-1",
                    &project_root,
                    "project-1-group",
                    project_root.join("config.toml"),
                    100,
                ))
                .unwrap();
            let project = ProjectRepository::new(&db)
                .find_by_id("project-1")
                .unwrap()
                .unwrap();
            let baseline = proposals::validate_initial_baseline(
                ProposalInput {
                    kind: ProposalKind::Experiment,
                    hypothesis: "baseline".to_owned(),
                    source_experiment_id: None,
                    argv: vec!["python".to_owned(), "train.py".to_owned()],
                    working_directory: ".".to_owned(),
                    expected_evidence: vec!["metric".to_owned()],
                },
                "objective-digest",
            )
            .unwrap();
            CampaignRepository::new(&db)
                .start_with_baseline(
                    StartCampaignRequest {
                        campaign_id: "campaign-1",
                        project_id: "project-1",
                        objective: &ObjectiveSnapshot {
                            text: "objective".to_owned(),
                            digest: "objective-digest".to_owned(),
                        },
                        initial_argv: baseline.argv(),
                        baseline: &baseline,
                        submission_id: "submission-source",
                        experiment_id: "experiment-source",
                        proposal_id: "proposal-source",
                        metadata: &serde_json::json!({}),
                        origin_agent_run_id: None,
                        objective_metric: None,
                        now: 100,
                    },
                    &CampaignLimits::default(),
                )
                .unwrap();
            let experiments = ExperimentRepository::new(&db);
            experiments
                .mark_submitting("experiment-source", 101)
                .unwrap();
            experiments
                .mark_accepted("experiment-source", 7, "pueue-managed-run:v1:source", 102)
                .unwrap();
            experiments
                .project_terminal_submission(
                    "experiment-source",
                    7,
                    ExperimentTerminalOutcome::Succeeded,
                    103,
                )
                .unwrap();
            let cycle = DecisionRepository::new(&db)
                .ensure_cycle_for_terminal("campaign-1", "experiment-source", 104)
                .unwrap();
            DecisionRepository::new(&db)
                .reserve_next_attempt("project-1", &cycle.cycle_id, 104)
                .unwrap()
                .unwrap();
            Self {
                db,
                _root: root,
                project,
            }
        }

        fn proposal(&self, kind: ProposalKind, hypothesis: &str) -> proposals::ValidatedProposal {
            proposals::validate(
                ProposalInput {
                    kind,
                    hypothesis: hypothesis.to_owned(),
                    source_experiment_id: Some("experiment-source".to_owned()),
                    argv: vec!["python".to_owned(), hypothesis.to_owned(), ".py".to_owned()],
                    working_directory: ".".to_owned(),
                    expected_evidence: vec!["metric".to_owned()],
                },
                "objective-digest",
            )
            .unwrap()
        }

        fn insert_malformed_open_owner(&self) {
            let mut connection = self.db.connect().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            let event = NewEvent::new(
                "project-1",
                EventKind::CampaignResearch,
                "research-owner-event",
                serde_json::json!({"review_id":"review-1"}),
                105,
                105,
            )
            .with_campaign_lineage("campaign-1", Some("experiment-source"));
            let (stored, _) = crate::db::insert_event_completed_in_transaction(
                &transaction,
                &event,
            )
            .unwrap();
            transaction
                .execute(
                    "INSERT INTO research_reviews (
                        review_id, campaign_id, experiment_id, task_signature, attempt,
                        state, operation_stage, agent_run_id, context_json, context_digest,
                        response_json, termination_request_id, successor_experiment_id,
                        evidence_schema_version, session_generation, event_id, not_before,
                        notes_json, failure_code, decision_cycle_id, checkpoint_json,
                        created_at, started_at, finished_at, updated_at
                     ) VALUES (
                        'review-1', 'campaign-1', 'experiment-source',
                        'pueue-managed-run:v1:source', 1, 'ready', 'intent', NULL,
                        NULL, NULL, NULL, NULL, NULL, NULL, 0, ?1, ?2, NULL, NULL,
                        NULL, NULL, 105, NULL, NULL, 105
                     )",
                    rusqlite::params![stored.event_id, 105],
                )
                .unwrap();
            transaction.commit().unwrap();
        }

        fn snapshot(&self) -> (
            Campaign,
            Option<Proposal>,
            Option<CodeChangeRun>,
            (i64, i64, i64, i64, i64, i64, i64),
            Vec<(String, String, Option<i64>, i64)>,
        ) {
            let connection = self.db.connect().unwrap();
            let decision_cycles = connection
                .prepare(
                    "SELECT cycle_id, state, next_wake_at, updated_at
                     FROM decision_cycles ORDER BY cycle_id",
                )
                .unwrap()
                .query_map([], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let row_counts = connection
                .query_row(
                    "SELECT
                         (SELECT COUNT(*) FROM proposals WHERE campaign_id = 'campaign-1'),
                         (SELECT COUNT(*) FROM experiments WHERE campaign_id = 'campaign-1'),
                         (SELECT COUNT(*) FROM submissions WHERE project_id = 'project-1'),
                         (SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = 'campaign-1'),
                         (SELECT COUNT(*) FROM code_change_runs WHERE campaign_id = 'campaign-1'),
                         (SELECT COUNT(*) FROM decision_attempts
                          WHERE cycle_id IN (SELECT cycle_id FROM decision_cycles
                                             WHERE campaign_id = 'campaign-1')),
                         (SELECT COUNT(*) FROM events WHERE project_id = 'project-1')",
                    [],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                        ))
                    },
                )
                .unwrap();
            (
                CampaignRepository::new(&self.db)
                    .find_by_id("campaign-1")
                    .unwrap()
                    .unwrap(),
                ProposalRepository::new(&self.db)
                    .find_for_campaign("campaign-1", "proposal-code")
                    .unwrap(),
                CodeChangeRepository::new(&self.db)
                    .find_by_proposal("proposal-code")
                    .unwrap(),
                row_counts,
                decision_cycles,
            )
        }
    }

    #[tokio::test]
    async fn coordinator_defers_normal_experiment_when_research_owns_source() {
        let fixture = CoordinatorFenceFixture::new();
        fixture.insert_malformed_open_owner();
        let before = fixture.snapshot();
        let proposal = fixture.proposal(ProposalKind::Experiment, "generic");
        let pueue = crate::pueue::CommandPueue::default();
        let admission = CampaignCoordinator::new(&fixture.db, &pueue, CampaignLimits::default())
            .admit_proposal(
                &fixture.project,
                "campaign-1",
                "proposal-generic",
                "experiment-generic",
                "submission-generic",
                &proposal,
                106,
            )
            .await
            .unwrap();
        assert!(matches!(admission, CampaignProposalAdmission::Deferred));
        assert_eq!(fixture.snapshot(), before);
    }

    #[tokio::test]
    async fn checkpoint_wrapper_fails_closed_without_checkpoint_authority() {
        let fixture = CoordinatorFenceFixture::new();
        let proposal = fixture.proposal(ProposalKind::Experiment, "checkpoint-wrapper");
        let intent = match CampaignRepository::new(&fixture.db)
            .accept_proposal(
                "campaign-1",
                "proposal-checkpoint-wrapper",
                "experiment-checkpoint-wrapper",
                "submission-checkpoint-wrapper",
                &proposal,
                &CampaignLimits::default(),
                106,
            )
            .unwrap()
        {
            ProposalAcceptance::Accepted(intent) => intent,
            other => panic!("expected accepted experiment, got {other:?}"),
        };
        let pueue = crate::pueue::CommandPueue::default();
        let coordinator = CampaignCoordinator::new(&fixture.db, &pueue, CampaignLimits::default());
        let admission = coordinator.acquire_admission(&fixture.project).unwrap();
        let result = coordinator
            .submit_checkpoint_intent_with_admission(&intent, &fixture.project, admission, 107)
            .await;
        assert!(matches!(
            result,
            Err(AppError::Validation {
                field: "research.checkpoint",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn campaign_admission_lock_survives_add_and_status_then_releases() {
        let fixture = CoordinatorFenceFixture::new();
        let proposal = fixture.proposal(ProposalKind::Experiment, "lease");
        let intent = match CampaignRepository::new(&fixture.db)
            .accept_proposal(
                "campaign-1",
                "proposal-lease",
                "experiment-lease",
                "submission-lease",
                &proposal,
                &CampaignLimits::default(),
                106,
            )
            .unwrap()
        {
            ProposalAcceptance::Accepted(intent) => intent,
            other => panic!("expected accepted experiment, got {other:?}"),
        };
        let pueue = LockCheckingPueue::new(fixture.project.root_path.clone());
        let add_lock_held = Arc::clone(&pueue.add_lock_held);
        let status_lock_held = Arc::clone(&pueue.status_lock_held);
        let coordinator = CampaignCoordinator::new(&fixture.db, &pueue, CampaignLimits::default());
        let admission = coordinator.acquire_admission(&fixture.project).unwrap();
        let result = coordinator
            .submit_admitted_proposal(
                AdmittedCampaignProposal {
                    intent,
                    matches_requested_intent: true,
                    admission,
                },
                &fixture.project,
                107,
            )
            .await
            .unwrap();
        assert!(matches!(result, CampaignSubmission::Submitted(_)));
        assert_eq!(*add_lock_held.lock().unwrap(), vec![true]);
        assert_eq!(*status_lock_held.lock().unwrap(), vec![true]);

        let root = ProjectRootAnchor::resolve(&fixture.project.root_path)
            .unwrap()
            .verify_identity()
            .unwrap();
        assert!(ProjectAdmissionLock::try_acquire(&root).unwrap().is_some());
    }

    #[tokio::test]
    async fn checkpoint_reserved_dispatch_keeps_retained_file_shared_lease_through_add_and_status() {
        let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
        assert_eq!(fixture.intent.experiment.status, ExperimentStatus::Reserved);
        let checkpoint = fixture.checkpoint.clone();
        let pueue = CheckpointLeaseProbePueue::new(
            &fixture.policy,
            &checkpoint.campaign_id,
            &checkpoint.review_id,
            &checkpoint.retained_checkpoint,
        );
        let add_cleanup_blocked = Arc::clone(&pueue.add_cleanup_blocked);
        let status_cleanup_blocked = Arc::clone(&pueue.status_cleanup_blocked);
        let coordinator = CampaignCoordinator::new(
            &fixture.db,
            &pueue,
            CampaignLimits::default(),
        )
        .with_execution_policy(&fixture.policy);
        let admission = coordinator.acquire_admission(&fixture.project).unwrap();
        let result = coordinator
            .submit_checkpoint_intent_with_admission(&fixture.intent, &fixture.project, admission, 3_105)
            .await
            .unwrap();
        let submission = match result {
            CampaignSubmission::Submitted(submission) => submission,
            CampaignSubmission::Deferred => panic!("checkpoint dispatch unexpectedly deferred"),
        };

        assert_eq!(submission.pueue_task_id, Some(77));
        assert_eq!(submission.status, crate::models::SubmissionStatus::Accepted);
        assert!(submission.task_signature.is_some());
        let experiment = ExperimentRepository::new(&fixture.db)
            .find_by_id(&fixture.intent.experiment.experiment_id)
            .unwrap()
            .unwrap();
        assert_eq!(experiment.status, ExperimentStatus::Accepted);
        assert_eq!(experiment.pueue_task_id, Some(77));
        assert!(experiment.task_signature.is_some());
        assert_eq!(*add_cleanup_blocked.lock().unwrap(), vec![true]);
        assert_eq!(*status_cleanup_blocked.lock().unwrap(), vec![true]);

        crate::environment::cleanup_retained_research_file(
            &fixture.policy,
            &checkpoint.campaign_id,
            &checkpoint.review_id,
            &checkpoint.retained_checkpoint,
        )
        .unwrap();
        let retained_path = fixture
            .policy
            .code_change_state_root_path()
            .join(&checkpoint.retained_checkpoint.relative_path);
        assert!(!retained_path.exists());
    }

    #[tokio::test]
    async fn checkpoint_accepted_reserved_replay_is_idempotent_and_consumed_corruption_blocks() {
        let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
        let checkpoint = fixture.checkpoint.clone();
        let successor_id = fixture.intent.experiment.experiment_id.clone();
        let pueue = CheckpointLeaseProbePueue::new(
            &fixture.policy,
            &checkpoint.campaign_id,
            &checkpoint.review_id,
            &checkpoint.retained_checkpoint,
        );
        let add_cleanup_blocked = Arc::clone(&pueue.add_cleanup_blocked);
        let coordinator = CampaignCoordinator::new(
            &fixture.db,
            &pueue,
            CampaignLimits::default(),
        )
        .with_execution_policy(&fixture.policy);
        let admission = coordinator.acquire_admission(&fixture.project).unwrap();
        let first = coordinator
            .submit_checkpoint_intent_with_admission(
                &fixture.intent,
                &fixture.project,
                admission,
                3_105,
            )
            .await
            .unwrap();
        assert!(matches!(first, CampaignSubmission::Submitted(_)));
        let experiment = ExperimentRepository::new(&fixture.db)
            .find_by_id(&successor_id)
            .unwrap()
            .unwrap();
        assert_eq!(experiment.status, ExperimentStatus::Accepted);
        assert_eq!(experiment.pueue_task_id, Some(77));
        let reservation_status: String = fixture
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status FROM budget_reservations
                 WHERE experiment_id = ?1 AND dimension = 'experiment'",
                [&successor_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reservation_status, "reserved");

        let replay_intent = {
            let mut connection = fixture.db.connect().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            let owner = match crate::db::research_ownership_in_transaction(
                &transaction,
                &fixture.project.project_id,
                &checkpoint.campaign_id,
                &checkpoint.source_experiment_id,
            )
            .unwrap()
            {
                crate::db::ResearchOwnership::Open(Some(owner)) => owner,
                other => panic!("unexpected accepted checkpoint owner: {other:?}"),
            };
            assert_eq!(owner.operation_stage.as_deref(), Some("successor_reserved"));
            assert!(!owner.recovery_required, "accepted+reserved owner must remain valid");
            let replay_intent = match crate::db::accept_checkpoint_successor_in_transaction(
                &transaction,
                &owner,
                &CampaignLimits::default(),
                3_106,
            )
            .unwrap()
            {
                crate::db::CheckpointSuccessorAdmission::Ready(intent) => intent,
                other => panic!("unexpected accepted checkpoint replay: {other:?}"),
            };
            transaction.commit().unwrap();
            replay_intent
        };
        let replay_admission = coordinator.acquire_admission(&fixture.project).unwrap();
        let replay = coordinator
            .submit_checkpoint_intent_with_admission(
                &replay_intent,
                &fixture.project,
                replay_admission,
                3_107,
            )
            .await
            .unwrap();
        assert!(matches!(replay, CampaignSubmission::Submitted(_)));
        assert_eq!(*add_cleanup_blocked.lock().unwrap(), vec![true]);

        fixture
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE budget_reservations SET status = 'consumed'
                 WHERE experiment_id = ?1 AND dimension = 'experiment'
                   AND status = 'reserved'",
                [&successor_id],
            )
            .unwrap();
        let mut connection = fixture.db.connect().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let corrupted_owner = match crate::db::research_ownership_in_transaction(
            &transaction,
            &fixture.project.project_id,
            &checkpoint.campaign_id,
            &checkpoint.source_experiment_id,
        )
        .unwrap()
        {
            crate::db::ResearchOwnership::Open(Some(owner)) => owner,
            other => panic!("unexpected corrupted checkpoint owner: {other:?}"),
        };
        assert!(corrupted_owner.recovery_required);
        assert!(matches!(
            crate::db::accept_checkpoint_successor_in_transaction(
                &transaction,
                &corrupted_owner,
                &CampaignLimits::default(),
                3_108,
            ),
            Err(AppError::Validation {
                field: "research.owner",
                ..
            })
        ));
        transaction.commit().unwrap();

        crate::environment::cleanup_retained_research_file(
            &fixture.policy,
            &checkpoint.campaign_id,
            &checkpoint.review_id,
            &checkpoint.retained_checkpoint,
        )
        .unwrap();
    }

    #[tokio::test]
    async fn checkpoint_stale_reserved_clone_observing_submitting_never_adds_or_preadd_fails() {
        let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
        let experiment_id = fixture.intent.experiment.experiment_id.clone();
        let pueue = NoAddPueue::new();
        let add_count = Arc::clone(&pueue.add_count);
        let coordinator = CampaignCoordinator::new(&fixture.db, &pueue, CampaignLimits::default())
            .with_execution_policy(&fixture.policy);
        set_test_after_clone_hook(
            &fixture.db,
            &experiment_id,
            Some(stale_submitting_after_clone),
        );
        let result = coordinator
            .submit_reserved_intent(&fixture.intent, &fixture.project, 3_105)
            .await;
        set_test_after_clone_hook(&fixture.db, &experiment_id, None);

        assert!(matches!(result, Err(AppError::Validation { field: "experiment", .. })));
        assert_eq!(*add_count.lock().unwrap(), 0);
        let experiment = ExperimentRepository::new(&fixture.db)
            .find_by_id(&experiment_id)
            .unwrap()
            .unwrap();
        assert_eq!(experiment.status, ExperimentStatus::Unreconciled);
        assert_eq!(experiment.failure_code.as_deref(), Some(ADD_INTERRUPTED_REASON));
        assert_ne!(experiment.failure_code.as_deref(), Some(CHECKPOINT_PRE_ADD_FAILURE_CODE));
        assert_eq!(experiment.pueue_task_id, None);
        assert_eq!(experiment.task_signature, None);
        let submission = SubmissionRepository::new(&fixture.db)
            .find_by_id(&fixture.intent.experiment.submission_id)
            .unwrap()
            .unwrap();
        assert_eq!(submission.status, crate::models::SubmissionStatus::Unreconciled);
        assert_eq!(submission.pueue_task_id, None);
        assert_eq!(submission.task_signature, None);
        let reservation_status: String = fixture
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status FROM budget_reservations
                 WHERE experiment_id = ?1 AND dimension = 'experiment'",
                [&experiment_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reservation_status, "reserved");

        let mut connection = fixture.db.connect().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let owner = match crate::db::research_ownership_in_transaction(
            &transaction,
            &fixture.project.project_id,
            &fixture.checkpoint.campaign_id,
            &fixture.checkpoint.source_experiment_id,
        )
        .unwrap()
        {
            crate::db::ResearchOwnership::Open(Some(owner)) => owner,
            other => panic!("unexpected unreconciled checkpoint owner: {other:?}"),
        };
        assert_eq!(owner.operation_stage.as_deref(), Some("successor_reserved"));
        assert!(
            !owner.recovery_required,
            "unreconciled+reserved owner must remain valid"
        );
        let replay_intent = match crate::db::accept_checkpoint_successor_in_transaction(
            &transaction,
            &owner,
            &CampaignLimits::default(),
            3_106,
        )
        .unwrap()
        {
            crate::db::CheckpointSuccessorAdmission::Ready(intent) => intent,
            other => panic!("unexpected unreconciled checkpoint replay: {other:?}"),
        };
        transaction.commit().unwrap();

        let replay_admission = coordinator.acquire_admission(&fixture.project).unwrap();
        let replay = coordinator
            .submit_checkpoint_intent_with_admission(
                &replay_intent,
                &fixture.project,
                replay_admission,
                3_107,
            )
            .await;
        assert!(matches!(
            replay,
            Err(AppError::Validation {
                field: "experiment",
                message: "submission may already have reached Pueue; reconciliation is required",
            })
        ));
        assert_eq!(*add_count.lock().unwrap(), 0);
        let replayed_experiment = ExperimentRepository::new(&fixture.db)
            .find_by_id(&experiment_id)
            .unwrap()
            .unwrap();
        assert_eq!(replayed_experiment.status, ExperimentStatus::Unreconciled);
        assert_eq!(
            replayed_experiment.failure_code.as_deref(),
            Some(ADD_INTERRUPTED_REASON)
        );
        assert_eq!(replayed_experiment.pueue_task_id, None);
        let replayed_submission = SubmissionRepository::new(&fixture.db)
            .find_by_id(&fixture.intent.experiment.submission_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            replayed_submission.status,
            crate::models::SubmissionStatus::Unreconciled
        );
        assert_eq!(replayed_submission.pueue_task_id, None);

        fixture
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE budget_reservations SET status = 'consumed'
                 WHERE experiment_id = ?1 AND dimension = 'experiment'
                   AND status = 'reserved'",
                [&experiment_id],
            )
            .unwrap();
        let mut connection = fixture.db.connect().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let corrupted_owner = match crate::db::research_ownership_in_transaction(
            &transaction,
            &fixture.project.project_id,
            &fixture.checkpoint.campaign_id,
            &fixture.checkpoint.source_experiment_id,
        )
        .unwrap()
        {
            crate::db::ResearchOwnership::Open(Some(owner)) => owner,
            other => panic!("unexpected corrupted unreconciled owner: {other:?}"),
        };
        assert!(corrupted_owner.recovery_required);
        assert!(matches!(
            crate::db::accept_checkpoint_successor_in_transaction(
                &transaction,
                &corrupted_owner,
                &CampaignLimits::default(),
                3_108,
            ),
            Err(AppError::Validation {
                field: "research.owner",
                ..
            })
        ));
        transaction.commit().unwrap();
    }

    #[tokio::test]
    async fn checkpoint_stale_reserved_clone_observing_unreconciled_never_adds_or_preadd_fails() {
        let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
        let experiment_id = fixture.intent.experiment.experiment_id.clone();
        let pueue = NoAddPueue::new();
        let add_count = Arc::clone(&pueue.add_count);
        let coordinator = CampaignCoordinator::new(&fixture.db, &pueue, CampaignLimits::default())
            .with_execution_policy(&fixture.policy);
        set_test_after_clone_hook(
            &fixture.db,
            &experiment_id,
            Some(stale_unreconciled_after_clone),
        );
        let result = coordinator
            .submit_reserved_intent(&fixture.intent, &fixture.project, 3_105)
            .await;
        set_test_after_clone_hook(&fixture.db, &experiment_id, None);

        assert!(matches!(result, Err(AppError::Validation { field: "experiment", .. })));
        assert_eq!(*add_count.lock().unwrap(), 0);
        let experiment = ExperimentRepository::new(&fixture.db)
            .find_by_id(&experiment_id)
            .unwrap()
            .unwrap();
        assert_eq!(experiment.status, ExperimentStatus::Unreconciled);
        assert_eq!(experiment.failure_code.as_deref(), Some("identity-mismatch"));
        assert_ne!(experiment.failure_code.as_deref(), Some(CHECKPOINT_PRE_ADD_FAILURE_CODE));
        assert_eq!(experiment.pueue_task_id, None);
        assert_eq!(experiment.task_signature, None);
        let submission = SubmissionRepository::new(&fixture.db)
            .find_by_id(&fixture.intent.experiment.submission_id)
            .unwrap()
            .unwrap();
        assert_eq!(submission.status, crate::models::SubmissionStatus::Unreconciled);
        assert_eq!(submission.pueue_task_id, None);
        assert_eq!(submission.task_signature, None);
    }

    #[tokio::test]
    async fn checkpoint_coherent_group_clone_binding_rejects_stale_group_before_add() {
        let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
        let experiment_id = fixture.intent.experiment.experiment_id.clone();
        let pueue = NoAddPueue::new();
        let add_count = Arc::clone(&pueue.add_count);
        let coordinator = CampaignCoordinator::new(&fixture.db, &pueue, CampaignLimits::default())
            .with_execution_policy(&fixture.policy);
        set_test_after_clone_hook(
            &fixture.db,
            &experiment_id,
            Some(coherent_group_after_clone),
        );
        let result = coordinator
            .submit_reserved_intent(&fixture.intent, &fixture.project, 3_105)
            .await;
        set_test_after_clone_hook(&fixture.db, &experiment_id, None);

        assert!(matches!(
            result,
            Err(AppError::Validation {
                field: "research.checkpoint",
                ..
            })
        ));
        assert_eq!(*add_count.lock().unwrap(), 0);
        let experiment = ExperimentRepository::new(&fixture.db)
            .find_by_id(&experiment_id)
            .unwrap()
            .unwrap();
        assert_eq!(experiment.status, ExperimentStatus::Reserved);
        assert_eq!(experiment.pueue_task_id, None);
        assert_eq!(experiment.task_signature, None);
        let submission = SubmissionRepository::new(&fixture.db)
            .find_by_id(&fixture.intent.experiment.submission_id)
            .unwrap()
            .unwrap();
        assert_eq!(submission.status, crate::models::SubmissionStatus::Pending);
        assert_eq!(submission.pueue_task_id, None);
        assert_eq!(submission.task_signature, None);
        let stored_group: String = fixture
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT pueue_group FROM projects WHERE project_id = ?1",
                [&fixture.project.project_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_group, "coherent-group");
    }

    #[tokio::test]
    async fn coordinator_defers_code_change_id_and_digest_reentry_before_reuse() {
        let fixture = CoordinatorFenceFixture::new();
        let proposal = fixture.proposal(ProposalKind::CodeChange, "code-change");
        let run = NewCodeChangeRun::new(
            "code-run-1",
            "proposal-code",
            "campaign-1",
            "a".repeat(40),
            code_change::candidate_ref("campaign-1", "proposal-code").unwrap(),
            code_change::best_ref("campaign-1").unwrap(),
            "worktree-code-run-1",
            ".pueue-agent/worktrees/campaign-1/proposal-code",
            105,
        );
        assert!(matches!(
            CampaignRepository::new(&fixture.db)
                .accept_code_change_proposal(
                    "campaign-1",
                    "proposal-code",
                    "experiment-code",
                    "submission-code",
                    &proposal,
                    &CampaignLimits::default(),
                    105,
                    Some(&run),
                    None,
                )
                .unwrap(),
            ProposalAcceptance::PendingCodeChange
        ));
        fixture.insert_malformed_open_owner();
        let before = fixture.snapshot();
        let pueue = crate::pueue::CommandPueue::default();
        let coordinator = CampaignCoordinator::new(&fixture.db, &pueue, CampaignLimits::default());
        for proposal_id in ["proposal-code", "proposal-code-retry"] {
            let admission = coordinator
                .admit_proposal(
                    &fixture.project,
                    "campaign-1",
                    proposal_id,
                    "experiment-code-retry",
                    "submission-code-retry",
                    &proposal,
                    106,
                )
                .await
                .unwrap();
            assert!(matches!(admission, CampaignProposalAdmission::Deferred));
            assert_eq!(fixture.snapshot(), before);
        }
    }

    #[test]
    fn candidate_submission_cwd_is_descriptor_bound_to_the_candidate_root() {
        let temporary = tempfile::tempdir().unwrap();
        let candidate_path = temporary.path().join("candidate");
        std::fs::create_dir_all(candidate_path.join("nested")).unwrap();
        let anchor = ProjectRootAnchor::resolve(&candidate_path).unwrap();
        let candidate = anchor.verify_identity().unwrap();

        let nested = open_candidate_working_directory(&candidate, "nested").unwrap();
        assert_eq!(nested.canonical_path(), candidate_path.join("nested"));
        assert!(open_candidate_working_directory(&candidate, "/tmp").is_err());
        assert!(open_candidate_working_directory(&candidate, "../outside").is_err());
    }

    #[test]
    fn pinned_git_invocation_disables_config_auth_and_unbounded_output() {
        let argv = pinned_git_argv(&["rev-parse", "HEAD"]);
        let argv = argv
            .iter()
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(
            pinned_git_argv(&["status", "--porcelain=v1", "-z", "--untracked-files=all"])
                .iter()
                .any(|value| value == "--untracked-files=all")
        );
        assert!(argv
            .windows(2)
            .any(|pair| pair == ["-c", "core.hooksPath=/dev/null"]));
        assert!(argv
            .windows(2)
            .any(|pair| pair == ["-c", "core.fsmonitor=false"]));
        assert!(argv
            .windows(2)
            .any(|pair| pair == ["-c", "credential.helper="]));
        assert!(!argv.windows(2).any(|pair| pair == ["-c", "diff.external="]));
        assert!(argv
            .windows(2)
            .any(|pair| pair == ["-c", "commit.gpgSign=false"]));
        assert!(argv.contains(&"--no-pager".to_owned()));
        let environment = pinned_git_environment();
        assert!(environment.contains(&("GIT_CONFIG_NOSYSTEM", "1")));
        assert!(environment.contains(&("GIT_CONFIG_SYSTEM", "/dev/null")));
        assert!(environment.contains(&("GIT_CONFIG_GLOBAL", "/dev/null")));
        assert!(environment.contains(&("GIT_TERMINAL_PROMPT", "0")));
        assert!(environment.contains(&("GIT_ASKPASS", "/bin/false")));
        assert!(environment.contains(&("SSH_ASKPASS", "/bin/false")));
        assert!(environment.contains(&("GIT_EDITOR", "/bin/false")));
        assert!(environment.contains(&("GIT_SEQUENCE_EDITOR", "/bin/false")));
        assert!(!environment.iter().any(|(name, _)| {
            matches!(
                *name,
                "HOME" | "PATH" | "AWS_SECRET_ACCESS_KEY" | "GITHUB_TOKEN"
            )
        }));
        assert!(validate_git_output_len(MAX_GIT_OUTPUT_BYTES).is_ok());
        assert!(validate_git_output_len(MAX_GIT_OUTPUT_BYTES + 1).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pinned_git_allows_modified_tracked_file_diff() {
        let temporary = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
        let git_path = temporary.path().join("git");
        std::fs::write(&git_path, b"#!/bin/sh\nexec /usr/bin/git \"$@\"\n").unwrap();
        let mut permissions = std::fs::metadata(&git_path).unwrap().permissions();
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(0o700);
        std::fs::set_permissions(&git_path, permissions).unwrap();
        let git = ExecutableAnchor::from_absolute(&git_path, &[]).unwrap();
        for args in [
            ["init", "-q"].as_slice(),
            ["config", "user.name", "fixture"].as_slice(),
            ["config", "user.email", "fixture@example.invalid"].as_slice(),
        ] {
            let output = std::process::Command::new("/usr/bin/git")
                .args(args)
                .current_dir(temporary.path())
                .output()
                .unwrap();
            assert!(output.status.success(), "git {:?}: {:?}", args, output);
        }
        std::fs::write(temporary.path().join("tracked"), "baseline\n").unwrap();
        let output = std::process::Command::new("/usr/bin/git")
            .args(["add", "."])
            .current_dir(temporary.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "git add: {:?}", output);
        let output = std::process::Command::new("/usr/bin/git")
            .args(["commit", "-qm", "baseline"])
            .current_dir(temporary.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "git commit: {:?}", output);
        std::fs::write(temporary.path().join("tracked"), "changed\n").unwrap();

        let output = run_pinned_git(&git, temporary.path(), &["diff", "--quiet"])
            .await
            .expect("ordinary Git diff must complete");

        assert_eq!(output.status.code(), Some(1));
    }

    #[test]
    fn local_git_config_rejects_all_diff_execution_keys_but_allows_remote_metadata() {
        let temporary = tempfile::tempdir().unwrap();
        for (name, contents) in [
            ("exact", "[diff]\n\texternal = /tmp/sentinel\n"),
            ("command", "[diff \"driver\"]\n\tcommand = /tmp/sentinel\n"),
            (
                "textconv",
                "[DiFf \"driver\"]\n\ttextConv = /tmp/sentinel\n",
            ),
            (
                "trust-exit-code",
                "[diff \"driver\"]\n\ttrustExitCode = true\n",
            ),
        ] {
            let path = temporary.path().join(name);
            std::fs::write(&path, contents).unwrap();
            assert!(
                matches!(
                    validate_local_git_config_file(&path),
                    Err(AppError::Validation {
                        field: "git.config",
                        ..
                    })
                ),
                "configuration {name} must be rejected"
            );
        }
        let remote = temporary.path().join("remote");
        std::fs::write(
            &remote,
            "[remote \"origin\"]\n\turl = https://example.invalid/repo.git\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n",
        )
        .unwrap();
        assert!(validate_local_git_config_file(&remote).is_ok());
    }

    #[test]
    fn local_git_config_rejects_dotted_filter_and_diff_execution_keys() {
        let temporary = tempfile::tempdir().unwrap();
        for (name, contents) in [
            ("filter-clean", "[filter.foo]\n\tclean = /tmp/sentinel\n"),
            (
                "filter-quoted-clean",
                "[filter \"foo\"]\n\tCLEAN = /tmp/sentinel\n",
            ),
            (
                "filter-dotted-key-clean",
                "FILTER.foo.CLEAN = /tmp/sentinel\n",
            ),
            (
                "filter-whitespace-smudge",
                "[ FILTER.foo ]\n\tSmUdGe = /tmp/sentinel\n",
            ),
            ("filter-smudge", "[FILTER.foo]\n\tSMUDGE = /tmp/sentinel\n"),
            (
                "filter-process",
                "[filter.foo]\n\tPROCESS = /tmp/sentinel\n",
            ),
            ("filter-required", "[filter.foo]\n\tREQUIRED = true\n"),
            ("diff-command", "[diff.foo]\n\tcommand = /tmp/sentinel\n"),
            ("diff-textconv", "[DIFF.foo]\n\tTEXTCONV = /tmp/sentinel\n"),
            (
                "diff-trust-exit-code",
                "[diff.foo]\n\tTRUSTEXITCODE = true\n",
            ),
        ] {
            let path = temporary.path().join(name);
            std::fs::write(&path, contents).unwrap();
            assert!(
                matches!(
                    validate_local_git_config_file(&path),
                    Err(AppError::Validation {
                        field: "git.config",
                        ..
                    })
                ),
                "configuration {name} must be rejected"
            );
        }
        let safe_diff = temporary.path().join("safe-diff");
        std::fs::write(&safe_diff, "[diff.foo]\n\talgorithm = histogram\n").unwrap();
        assert!(validate_local_git_config_file(&safe_diff).is_ok());
    }

    #[test]
    fn local_git_config_rejects_bom_and_continuation_syntax() {
        let temporary = tempfile::tempdir().unwrap();
        for (name, contents) in [
            (
                "bom-filter",
                "\u{feff}[filter.foo]\n\tclean = /tmp/sentinel\n",
            ),
            (
                "bom-include",
                "\u{feff}[include]\n\tpath = /tmp/malicious.config\n",
            ),
            (
                "continued-diff",
                "[DiFf.foo]\r\n\tsafe = x \\   \r\n[BAR]\r\n\tTeXtCoNv = /tmp/sentinel\r\n",
            ),
        ] {
            let path = temporary.path().join(name);
            std::fs::write(&path, contents).unwrap();
            assert!(
                matches!(
                    validate_local_git_config_file(&path),
                    Err(AppError::Validation {
                        field: "git.config",
                        ..
                    })
                ),
                "configuration {name} must be rejected"
            );
        }
    }

    #[test]
    fn packed_refs_reject_duplicate_targets_and_orphan_peeled_lines() {
        let reference = "refs/heads/campaign/demo/best";
        let sha = "a".repeat(40);
        let duplicate = format!("{sha} {reference}\n{sha} {reference}\n");
        assert_eq!(
            parse_packed_refs(duplicate.as_bytes(), reference),
            GitRefPresence::Invalid
        );
        let orphan = format!("^{}\n", "b".repeat(40));
        assert_eq!(
            parse_packed_refs(orphan.as_bytes(), reference),
            GitRefPresence::Invalid
        );
    }

    #[test]
    fn packed_refs_accept_unrelated_annotated_tag_peeled_records() {
        let reference = "refs/heads/campaign/demo/best";
        let branch = "a".repeat(40);
        let tag = "b".repeat(40);
        let peeled = "c".repeat(40);
        let contents = format!("{tag} refs/tags/unrelated\n^{peeled}\n{branch} {reference}\n");
        assert_eq!(
            parse_packed_refs(contents.as_bytes(), reference),
            GitRefPresence::Present
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pinned_git_cleans_up_after_final_anchor_identity_failure() {
        let temporary = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
        let script = temporary.path().join("git-script");
        let replacement = temporary.path().join("replacement");
        std::fs::write(&replacement, b"#!/bin/sh\nexit 0\n").unwrap();
        let mut permissions = std::fs::metadata(&replacement).unwrap().permissions();
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(0o700);
        std::fs::set_permissions(&replacement, permissions).unwrap();
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\n( /bin/sleep 30 ) >/dev/null 2>&1 &\nprintf '%s' \"$!\" > descendant.pid\n/bin/mv '{}' '{}'\nprintf done\nexit 0\n",
                replacement.display(),
                script.display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();
        let anchor = ExecutableAnchor::from_absolute(&script, &[]).unwrap();

        let result = match run_pinned_git(&anchor, temporary.path(), &["rev-parse", "HEAD"]).await {
            Err(error) => error,
            Ok(_) => panic!("replaced anchor must fail closed"),
        };
        assert!(matches!(result, AppError::PolicyViolation { .. }));

        let descendant = std::fs::read_to_string(temporary.path().join("descendant.pid"))
            .unwrap()
            .trim()
            .parse::<libc::pid_t>()
            .unwrap();
        for _ in 0..100 {
            if unsafe { libc::kill(descendant, 0) } == -1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        unsafe {
            libc::kill(descendant, libc::SIGKILL);
        }
        panic!("pinned Git descendant survived final identity cleanup");
    }

    #[test]
    fn pinned_git_program_uses_the_verified_descriptor() {
        let executable = std::env::current_exe().unwrap();
        let anchor = ExecutableAnchor::from_absolute(&executable, &[]).unwrap();
        let verified = anchor.verify_identity().unwrap();
        let program = verified_git_program(&verified);
        if cfg!(target_os = "linux") {
            assert!(program.to_string_lossy().starts_with("/proc/self/fd/"));
        } else {
            assert_eq!(program, executable.into_os_string());
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn verified_git_fd_inheritance_clears_only_close_on_exec() {
        assert_eq!(
            inherited_verified_git_fd_flags(libc::FD_CLOEXEC | libc::FD_CLOEXEC << 1),
            libc::FD_CLOEXEC << 1
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn pinned_git_runs_a_verified_shebang_script_anchor() {
        let temporary = tempfile::tempdir().unwrap();
        let script = temporary.path().join("git-script");
        std::fs::write(&script, b"#!/bin/sh\nprintf 'script-ran\\n'\n").unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();
        let anchor = ExecutableAnchor::from_absolute(&script, &[]).unwrap();

        let output = run_pinned_git(&anchor, temporary.path(), &["rev-parse", "HEAD"])
            .await
            .unwrap();

        assert!(output.status.success());
        assert_eq!(output.stdout, b"script-ran\n");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn pinned_git_kills_the_saved_process_group_after_leader_exit() {
        let temporary = tempfile::tempdir().unwrap();
        let script = temporary.path().join("git-script");
        std::fs::write(
            &script,
            b"#!/bin/sh\n( /bin/sleep 1; /bin/dd if=/dev/zero bs=70000 count=1 2>/dev/null; /bin/sleep 30 ) >&2 &\nprintf '%s' \"$!\" > descendant.pid\nexit 0\n",
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();
        let anchor = ExecutableAnchor::from_absolute(&script, &[]).unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            run_pinned_git(&anchor, temporary.path(), &["rev-parse", "HEAD"]),
        )
        .await
        .expect("pinned Git must remain bounded");
        let result = match result {
            Ok(_) => panic!("the fixture must exceed the bounded stderr limit"),
            Err(error) => error,
        };
        assert!(matches!(
            result,
            AppError::Validation {
                field: "git.output",
                ..
            }
        ));

        let descendant = std::fs::read_to_string(temporary.path().join("descendant.pid"))
            .unwrap()
            .trim()
            .parse::<libc::pid_t>()
            .unwrap();
        for _ in 0..100 {
            if unsafe { libc::kill(descendant, 0) } == -1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        unsafe {
            libc::kill(descendant, libc::SIGKILL);
        }
        panic!("pinned Git descendant survived process-group cleanup");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn pinned_git_does_not_execute_a_local_clean_filter() {
        let temporary = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
        for args in [
            ["init", "-q"].as_slice(),
            ["config", "user.name", "fixture"].as_slice(),
            ["config", "user.email", "fixture@example.invalid"].as_slice(),
        ] {
            let output = std::process::Command::new("/usr/bin/git")
                .args(args)
                .current_dir(temporary.path())
                .output()
                .unwrap();
            assert!(output.status.success(), "git {:?}: {:?}", args, output);
        }
        std::fs::write(
            temporary.path().join(".gitattributes"),
            "tracked filter=clean\n",
        )
        .unwrap();
        std::fs::write(temporary.path().join("tracked"), "baseline\n").unwrap();
        let output = std::process::Command::new("/usr/bin/git")
            .args(["add", "."])
            .current_dir(temporary.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "git add: {:?}", output);
        let output = std::process::Command::new("/usr/bin/git")
            .args(["commit", "-qm", "baseline"])
            .current_dir(temporary.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "git commit: {:?}", output);
        let git_path = temporary.path().join("git");
        std::fs::write(&git_path, b"#!/bin/sh\nexec /usr/bin/git \"$@\"\n").unwrap();
        let mut permissions = std::fs::metadata(&git_path).unwrap().permissions();
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(0o700);
        std::fs::set_permissions(&git_path, permissions).unwrap();
        let git = ExecutableAnchor::from_absolute(&git_path, &[]).unwrap();
        let marker = temporary.path().join("filter-executed");
        let filter = temporary.path().join("filter.sh");
        std::fs::write(
            &filter,
            format!("#!/bin/sh\n/bin/touch {}\n/bin/cat\n", marker.display()),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&filter).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&filter, permissions).unwrap();
        let config = temporary.path().join(".git/config");
        let mut contents = std::fs::read_to_string(&config).unwrap();
        contents.push_str(&format!(
            "\n[filter \"clean\"]\n\tclean = {}\n",
            filter.display()
        ));
        std::fs::write(config, contents).unwrap();
        std::fs::write(temporary.path().join("tracked"), "changed\n").unwrap();

        let _ = run_pinned_git(&git, temporary.path(), &["diff", "--quiet"]).await;

        assert!(!marker.exists(), "local clean filter was executed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pinned_git_rejects_bom_and_continuation_config_without_running_sentinel() {
        let temporary = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
        for args in [
            ["init", "-q"].as_slice(),
            ["config", "user.name", "fixture"].as_slice(),
            ["config", "user.email", "fixture@example.invalid"].as_slice(),
        ] {
            let output = std::process::Command::new("/usr/bin/git")
                .args(args)
                .current_dir(temporary.path())
                .output()
                .unwrap();
            assert!(output.status.success(), "git {:?}: {:?}", args, output);
        }
        std::fs::write(
            temporary.path().join(".gitattributes"),
            "tracked filter=foo\ntracked diff=foo\n",
        )
        .unwrap();
        std::fs::write(temporary.path().join("tracked"), "baseline\n").unwrap();
        let output = std::process::Command::new("/usr/bin/git")
            .args(["add", "."])
            .current_dir(temporary.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "git add: {:?}", output);
        let output = std::process::Command::new("/usr/bin/git")
            .args(["commit", "-qm", "baseline"])
            .current_dir(temporary.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "git commit: {:?}", output);
        let git_path = temporary.path().join("git");
        std::fs::write(&git_path, b"#!/bin/sh\nexec /usr/bin/git \"$@\"\n").unwrap();
        let mut permissions = std::fs::metadata(&git_path).unwrap().permissions();
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(0o700);
        std::fs::set_permissions(&git_path, permissions).unwrap();
        let git = ExecutableAnchor::from_absolute(&git_path, &[]).unwrap();
        let marker = temporary.path().join("filter-executed");
        let filter = temporary.path().join("filter.sh");
        std::fs::write(
            &filter,
            format!("#!/bin/sh\n/bin/touch {}\n/bin/cat\n", marker.display()),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&filter).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&filter, permissions).unwrap();
        let config = temporary.path().join(".git/config");
        std::fs::write(
            &config,
            format!("\u{feff}[filter.foo]\n\tclean = {}\n", filter.display()),
        )
        .unwrap();
        std::fs::write(temporary.path().join("tracked"), "changed\n").unwrap();

        let result = run_pinned_git(&git, temporary.path(), &["diff", "--quiet"]).await;

        assert!(
            matches!(
                result,
                Err(AppError::Validation {
                    field: "git.config",
                    ..
                })
            ),
            "BOM-prefixed local clean filter must fail closed"
        );
        assert!(!marker.exists(), "local clean filter was executed");

        let included = temporary.path().join("included.config");
        std::fs::write(
            &included,
            format!("[filter.foo]\n\tclean = {}\n", filter.display()),
        )
        .unwrap();
        std::fs::write(
            &config,
            format!("\u{feff}[include]\n\tpath = {}\n", included.display()),
        )
        .unwrap();
        let _ = std::fs::remove_file(&marker);
        let result = run_pinned_git(&git, temporary.path(), &["diff", "--quiet"]).await;
        assert!(
            matches!(
                result,
                Err(AppError::Validation {
                    field: "git.config",
                    ..
                })
            ),
            "BOM-prefixed local include must fail closed"
        );
        assert!(!marker.exists(), "included local clean filter was executed");

        std::fs::write(
            &config,
            format!(
                "[DiFf.foo]\r\n\tsafe = x \\   \r\n[BAR]\r\n\tTeXtCoNv = {}\r\n",
                filter.display()
            ),
        )
        .unwrap();
        let _ = std::fs::remove_file(&marker);
        let result = run_pinned_git(&git, temporary.path(), &["diff", "--quiet"]).await;
        assert!(
            matches!(
                result,
                Err(AppError::Validation {
                    field: "git.config",
                    ..
                })
            ),
            "continued local diff config must fail closed"
        );
        assert!(!marker.exists(), "continued local textconv was executed");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn pinned_git_rejects_a_local_core_worktree_redirect() {
        let temporary = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
        for args in [
            ["init", "-q"].as_slice(),
            ["config", "user.name", "fixture"].as_slice(),
            ["config", "user.email", "fixture@example.invalid"].as_slice(),
        ] {
            let output = std::process::Command::new("/usr/bin/git")
                .args(args)
                .current_dir(temporary.path())
                .output()
                .unwrap();
            assert!(output.status.success(), "git {:?}: {:?}", args, output);
        }
        std::fs::write(temporary.path().join("tracked"), "baseline\n").unwrap();
        let output = std::process::Command::new("/usr/bin/git")
            .args(["add", "."])
            .current_dir(temporary.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "git add: {:?}", output);
        let output = std::process::Command::new("/usr/bin/git")
            .args(["commit", "-qm", "baseline"])
            .current_dir(temporary.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "git commit: {:?}", output);
        let git_path = temporary.path().join("git");
        std::fs::write(&git_path, b"#!/bin/sh\nexec /usr/bin/git \"$@\"\n").unwrap();
        let mut permissions = std::fs::metadata(&git_path).unwrap().permissions();
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(0o700);
        std::fs::set_permissions(&git_path, permissions).unwrap();
        let git = ExecutableAnchor::from_absolute(&git_path, &[]).unwrap();
        let outside = temporary.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("outside.txt"), "outside\n").unwrap();
        let config = temporary.path().join(".git/config");
        let mut contents = std::fs::read_to_string(&config).unwrap();
        contents.push_str(&format!("\n[core]\n\tworktree = {}\n", outside.display()));
        std::fs::write(config, contents).unwrap();

        let result = run_pinned_git(
            &git,
            temporary.path(),
            &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
        )
        .await;

        assert!(matches!(
            result,
            Err(AppError::Validation {
                field: "git.config",
                ..
            })
        ));
    }
}
