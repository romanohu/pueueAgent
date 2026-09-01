use std::{ffi::OsString, path::Path, process::Command};

use serde_json::Value;
use uuid::Uuid;

use crate::{
    code_change,
    db::{
        CampaignRepository, CodeChangeRepository, Db, ExperimentRepository,
        ManagedSubmissionIntent, ProjectRepository, ProposalAcceptance, ProposalRepository,
        StartCampaignRequest, SubmissionRepository,
    },
    environment::ProjectAdmissionLock,
    execution_policy::{
        preflight_code_change_runtime, CampaignLimits, ProjectRootAnchor,
        ResolvedExecutionPolicy, VerifiedProjectRoot,
    },
    models::{
        Campaign, CodeChangeRun, Experiment, ExperimentStatus, NewCodeChangeRun, ObjectiveMetric,
        Project, Proposal, ProposalKind, ProposalStatus, Submission,
    },
    output::{
        render_campaign_mutation, render_campaign_status_with_decision,
        render_experiment_inspection, render_experiment_list, render_proposal_inspection,
        render_proposal_list, DecisionStatusProjection,
    },
    proposals::{self, ProposalInput},
    pueue::{validate_add_argv, PueueApi},
    reconcile::{managed_task_run_signature, try_canonical_command_display_os},
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

#[derive(Debug, Clone, PartialEq)]
pub enum CampaignSubmission {
    Submitted(Submission),
    Deferred,
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
    render_campaign_status_with_decision(
        &campaign,
        proposal_count,
        &experiment_counts,
        &budget_usage,
        &task_ids,
        decision.as_ref(),
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

pub fn resume_for_project(
    db: &Db,
    project: &Project,
    now: i64,
    json: bool,
) -> Result<String, AppError> {
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
    render_experiment_inspection(&campaign, &experiment, &argv_digest, json)
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
        let base_revision_sha =
            self.capture_clean_head(&admission.verified_root.anchor.canonical_path);
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
            .submit_accepted_intent_inner(&intent, project, now, Some(admission))
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
        match self
            .submit_accepted_intent_inner(intent, project, now, None)
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
        self.submit_accepted_intent_inner(intent, project, now, None)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn admit_proposal(
        &self,
        project: &Project,
        campaign_id: &str,
        proposal_id: &str,
        experiment_id: &str,
        submission_id: &str,
        proposal: &proposals::ValidatedProposal,
        now: i64,
    ) -> Result<CampaignProposalAdmission, AppError> {
        let admission = self.acquire_admission(project)?;
        if proposal.kind() == ProposalKind::CodeChange {
            return self.admit_code_change(
                admission,
                project,
                campaign_id,
                proposal_id,
                experiment_id,
                submission_id,
                proposal,
                now,
            );
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
        match CampaignRepository::new(self.db).accept_proposal(
            campaign_id,
            proposal_id,
            experiment_id,
            submission_id,
            proposal,
            &self.limits,
            now,
        )? {
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
            ProposalAcceptance::PendingCodeChange => Err(AppError::Validation {
                field: "proposal.kind",
                message: "unexpected code-change acceptance for an experiment proposal",
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn admit_code_change(
        &self,
        _admission: CampaignAdmission,
        project: &Project,
        campaign_id: &str,
        proposal_id: &str,
        experiment_id: &str,
        submission_id: &str,
        proposal: &proposals::ValidatedProposal,
        now: i64,
    ) -> Result<CampaignProposalAdmission, AppError> {
        let proposals_repository = ProposalRepository::new(self.db);
        if let Some(existing) = proposals_repository.find_for_campaign(campaign_id, proposal_id)? {
            if existing.canonical_digest != proposal.canonical_digest() {
                return Err(AppError::Validation {
                    field: "proposal_id",
                    message: "conflicts with a different canonical proposal digest",
                });
            }
            if existing.kind != ProposalKind::CodeChange {
                return Err(AppError::Validation {
                    field: "proposal.kind",
                    message: "proposal ID belongs to a non-code proposal",
                });
            }
            return self.existing_code_change_outcome(existing);
        }
        if let Some(existing) =
            proposals_repository.find_by_digest(campaign_id, proposal.canonical_digest())?
        {
            if existing.kind != ProposalKind::CodeChange {
                return Err(AppError::Validation {
                    field: "proposal.canonical_digest",
                    message: "matches a non-code proposal",
                });
            }
            return self.existing_code_change_outcome(existing);
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

        let (base_sha, rejection_reason) = match preflight_code_change_runtime() {
            Err(error) => (None, Some(error.code.as_str())),
            Ok(()) => match self.code_change_base_sha(project, campaign_id) {
                Some(base_sha) => (Some(base_sha), None),
                None => (None, Some("base_revision_unavailable")),
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
        let accepted = CampaignRepository::new(self.db).accept_code_change_proposal(
            campaign_id,
            proposal_id,
            experiment_id,
            submission_id,
            proposal,
            &self.limits,
            now,
            code_change_run.as_ref(),
            rejection_reason,
        )?;
        match accepted {
            ProposalAcceptance::BudgetWaiting { .. } => Ok(CampaignProposalAdmission::Deferred),
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

    fn existing_code_change_outcome(
        &self,
        proposal: Proposal,
    ) -> Result<CampaignProposalAdmission, AppError> {
        if proposal.status == ProposalStatus::Rejected {
            return Ok(CampaignProposalAdmission::CodeChangeRejected(proposal));
        }
        if let Some(run) =
            CodeChangeRepository::new(self.db).find_by_proposal(&proposal.proposal_id)?
        {
            return Ok(CampaignProposalAdmission::CodeChange(run));
        }
        Ok(CampaignProposalAdmission::Deferred)
    }

    fn capture_clean_head(&self, project_root: &Path) -> Option<String> {
        let anchor = self
            .execution_policy
            .and_then(ResolvedExecutionPolicy::code_change_git_anchor)?;
        anchor.verify_identity().ok()?;
        let head = run_pinned_git(
            anchor,
            project_root,
            &["rev-parse", "--verify", "HEAD^{commit}"],
        )?;
        let status = run_pinned_git(anchor, project_root, &["status", "--porcelain=v1", "-z"])?;
        if !status.is_empty() {
            return None;
        }
        let head = String::from_utf8(head).ok()?;
        code_change::canonical_full_sha(head.trim()).ok()
    }

    fn code_change_base_sha(&self, project: &Project, campaign_id: &str) -> Option<String> {
        let anchor = self
            .execution_policy
            .and_then(ResolvedExecutionPolicy::code_change_git_anchor)?;
        anchor.verify_identity().ok()?;
        let best = code_change::best_ref(campaign_id).ok()?;
        let best_revision = format!("{best}^{{commit}}");
        if let Some(value) = run_pinned_git(
            anchor,
            &project.root_path,
            &["rev-parse", "--verify", &best_revision],
        ) {
            if let Ok(value) = String::from_utf8(value) {
                if let Ok(value) = code_change::canonical_full_sha(value.trim()) {
                    return Some(value);
                }
            }
        }
        let campaign = CampaignRepository::new(self.db)
            .find_by_id(campaign_id)
            .ok()??;
        let base = campaign.base_revision_sha?;
        let base_revision = format!("{base}^{{commit}}");
        let value = run_pinned_git(
            anchor,
            &project.root_path,
            &["rev-parse", "--verify", &base_revision],
        )?;
        let value = String::from_utf8(value).ok()?;
        let value = code_change::canonical_full_sha(value.trim()).ok()?;
        (value == base).then_some(value)
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
        )
        .await
    }

    async fn submit_accepted_intent_inner(
        &self,
        intent: &ManagedSubmissionIntent,
        project: &Project,
        now: i64,
        admission: Option<CampaignAdmission>,
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
        let runtime_argv = crate::environment::campaign_experiment_runtime_argv(
            &admission.verified_root.anchor.canonical_path,
            &durable_campaign.campaign_id,
            &current.experiment_id,
            &durable_submission.argv,
        );
        let expected_command = try_canonical_command_display_os(&runtime_argv)?;
        let add_args = pueue_add_args(
            &durable_project.pueue_group,
            &explicit_working_directory(
                &admission.verified_root.anchor.canonical_path,
                &durable_proposal.working_directory,
            ),
            &runtime_argv,
        );
        validate_add_argv(&add_args)?;

        match current.status {
            ExperimentStatus::Reserved => {
                if experiments
                    .begin_submitting_or_defer(&current.experiment_id, now)?
                    .is_none()
                {
                    return Ok(CampaignSubmission::Deferred);
                }
                admission
                    .verified_root
                    .anchor
                    .verify_identity()
                    .map_err(AppError::from)?;
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

        let task_id = match self.pueue.add(&add_args).await {
            Ok(task_id) => task_id,
            Err(error) => {
                experiments.mark_unreconciled(&current.experiment_id, ADD_UNKNOWN_REASON, now)?;
                return Err(error);
            }
        };
        let tasks = match self.pueue.status_json().await {
            Ok(tasks) => tasks,
            Err(error) => {
                experiments.mark_unreconciled(
                    &current.experiment_id,
                    ADD_IDENTITY_REASON,
                    now,
                )?;
                return Err(error);
            }
        };
        let mut id_matches = tasks.iter().filter(|task| task.id == task_id);
        let task = id_matches.next();
        if task.is_none() || id_matches.next().is_some() {
            experiments.mark_unreconciled(
                &current.experiment_id,
                ADD_IDENTITY_REASON,
                now,
            )?;
            return Err(reconciliation_required());
        }
        let task = task.expect("checked one Pueue task ID match");
        let task_signature = if task.group == durable_project.pueue_group
            && task.command == expected_command
        {
            managed_task_run_signature(task)
        } else {
            None
        };
        let Some(task_signature) = task_signature else {
            experiments.mark_unreconciled(
                &current.experiment_id,
                ADD_IDENTITY_REASON,
                now,
            )?;
            return Err(reconciliation_required());
        };
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

fn run_pinned_git(
    anchor: &crate::execution_policy::ExecutableAnchor,
    project_root: &Path,
    argv: &[&str],
) -> Option<Vec<u8>> {
    let mut command = Command::new(&anchor.canonical_path);
    command
        .args(argv)
        .current_dir(project_root)
        .env_clear()
        .env("LC_ALL", "C")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_TERMINAL_PROMPT", "0");
    let output = command.output().ok()?;
    output.status.success().then_some(output.stdout)
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

fn reconciliation_required() -> AppError {
    AppError::Validation {
        field: "experiment",
        message: "submission may already have reached Pueue; reconciliation is required",
    }
}
