use std::{
    ffi::OsString,
    path::{Component, Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

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
        Campaign, CodeChangeRun, CodeChangeState, Experiment, ExperimentStatus,
        NewCodeChangeRun, ObjectiveMetric, Project, Proposal, ProposalKind, ProposalStatus,
        Submission,
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
            return self.existing_code_change_outcome(existing, now);
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
            return self.existing_code_change_outcome(existing, now);
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
                CodeChangeBaseResolution::Unavailable => {
                    (None, Some("base_revision_unavailable"))
                }
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
        now: i64,
    ) -> Result<CampaignProposalAdmission, AppError> {
        if proposal.status == ProposalStatus::Rejected {
            return Ok(CampaignProposalAdmission::CodeChangeRejected(proposal));
        }
        if let Some(run) =
            CodeChangeRepository::new(self.db).find_by_proposal(&proposal.proposal_id)?
        {
            if run.state == CodeChangeState::Rejected {
                return Ok(CampaignProposalAdmission::CodeChangeRejected(proposal));
            }
            return Ok(CampaignProposalAdmission::CodeChange(run));
        }
        let rejected = CampaignRepository::new(self.db).reject_orphan_code_change(
            &proposal.campaign_id,
            &proposal.proposal_id,
            "orphan_code_change_run",
            now,
        )?;
        Ok(CampaignProposalAdmission::CodeChangeRejected(rejected))
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
        let best_resolution = match resolve_exact_git_ref(anchor, project_root, &best_reference).await
        {
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
        .await else {
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
            let Ok(reverified) =
                resolve_exact_git_ref(anchor, project_root, &best_reference).await
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
    let contents = std::str::from_utf8(&contents).map_err(|_| AppError::Validation {
        field: "git.config",
        message: "local Git configuration must be valid UTF-8",
    })?;
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
        let diff_execution_channel = full_key == "diff.external"
            || (section == "diff"
                && matches!(key.as_str(), "command" | "textconv" | "trustexitcode"))
            || (full_key.starts_with("diff.")
                && matches!(
                    full_key.rsplit('.').next(),
                    Some("command" | "textconv" | "trustexitcode")
                ));
        if section == "filter"
            || key.starts_with("filter.")
            || full_key == "core.worktree"
            || full_key == "include.path"
            || full_key == "includeif.path"
            || diff_execution_channel
        {
            return Err(AppError::Validation {
                field: "git.config",
                message: "local Git configuration contains an execution or path channel",
            });
        }
    }
    Ok(())
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
            let output = run_pinned_git(
                anchor,
                project_root,
                &["rev-parse", "--verify", &revision],
            )
            .await?;
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
    inherit_verified_git_fd(&mut command, std::os::fd::AsRawFd::as_raw_fd(&verified.file));
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
        let count = reader.read(&mut buffer).await.map_err(|_| AppError::Runtime {
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::execution_policy::ExecutableAnchor;

    #[test]
    fn pinned_git_invocation_disables_config_auth_and_unbounded_output() {
        let argv = pinned_git_argv(&["rev-parse", "HEAD"]);
        let argv = argv
            .iter()
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(pinned_git_argv(&[
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all"
        ])
        .iter()
        .any(|value| value == "--untracked-files=all"));
        assert!(argv.windows(2).any(|pair| pair == ["-c", "core.hooksPath=/dev/null"]));
        assert!(argv.windows(2).any(|pair| pair == ["-c", "core.fsmonitor=false"]));
        assert!(argv.windows(2).any(|pair| pair == ["-c", "credential.helper="]));
        assert!(!argv.windows(2).any(|pair| pair == ["-c", "diff.external="]));
        assert!(argv.windows(2).any(|pair| pair == ["-c", "commit.gpgSign=false"]));
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
            matches!(*name, "HOME" | "PATH" | "AWS_SECRET_ACCESS_KEY" | "GITHUB_TOKEN")
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
            ("textconv", "[DiFf \"driver\"]\n\ttextConv = /tmp/sentinel\n"),
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
        let contents = format!(
            "{tag} refs/tags/unrelated\n^{peeled}\n{branch} {reference}\n"
        );
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

        let result = match run_pinned_git(&anchor, temporary.path(), &["rev-parse", "HEAD"]).await
        {
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
        .expect("pinned Git must remain bounded")
        .expect_err("the fixture must exceed the bounded stderr limit");
        assert!(matches!(result, AppError::Validation { field: "git.output", .. }));

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
        std::fs::write(temporary.path().join(".gitattributes"), "tracked filter=clean\n")
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
        contents.push_str(&format!("\n[filter \"clean\"]\n\tclean = {}\n", filter.display()));
        std::fs::write(config, contents).unwrap();
        std::fs::write(temporary.path().join("tracked"), "changed\n").unwrap();

        let _ = run_pinned_git(&git, temporary.path(), &["diff", "--quiet"]).await;

        assert!(!marker.exists(), "local clean filter was executed");
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
