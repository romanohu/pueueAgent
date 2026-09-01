use sha2::{Digest, Sha256};

use crate::{
    campaign::{CampaignCoordinator, CampaignProposalAdmission, CampaignSubmission},
    db::{
        CampaignRepository, DecisionRepository, DecisionReservation, ExperimentRepository,
        ProjectRepository, ReadyDecision,
    },
    decision_evidence::{validate_stored_decision_context, DECISION_CONTEXT_SCHEMA_VERSION},
    decision_protocol::{ValidatedDecision, parse_and_validate_decision},
    execution_policy::{CampaignLimits, ResolvedExecutionPolicy},
    models::{
        AgentRunStatus, CampaignState, DecisionAttemptState, DecisionCycleState, ExperimentStatus,
        ProposalKind,
    },
    pueue::PueueApi,
    AppError,
};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DecisionLoopReport {
    pub proposals_applied: usize,
    pub waits_scheduled: usize,
    pub deferred: usize,
    pub degraded: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DecisionRecoveryReport {
    pub requeued: usize,
    pub missing: usize,
    pub deferred: usize,
    pub degraded: usize,
}

pub struct DecisionCoordinator<'a, P: PueueApi + ?Sized> {
    db: &'a crate::db::Db,
    pueue: &'a P,
    limits: CampaignLimits,
    policy: Option<&'a ResolvedExecutionPolicy>,
}

impl<'a, P: PueueApi + ?Sized> DecisionCoordinator<'a, P> {
    pub fn new(db: &'a crate::db::Db, pueue: &'a P, limits: CampaignLimits) -> Self {
        Self {
            db,
            pueue,
            limits,
            policy: None,
        }
    }

    pub fn with_policy(mut self, policy: &'a ResolvedExecutionPolicy) -> Self {
        self.policy = Some(policy);
        self
    }

    pub fn recover_interrupted(
        &self,
        now: i64,
        limit: usize,
    ) -> Result<DecisionRecoveryReport, AppError> {
        let repository = DecisionRepository::new(self.db);
        let recoveries = repository.recoverable_attempts(limit)?;
        let mut report = DecisionRecoveryReport::default();
        for recovery in recoveries {
            match recovery.state {
                DecisionAttemptState::Reserved | DecisionAttemptState::EvidenceReady => {
                    let requeued = if let Some(event_id) = recovery.event_id {
                        let retry_at = now.checked_add(60).ok_or(AppError::Validation {
                            field: "now",
                            message: "cannot represent decision recovery retry time",
                        })?;
                        repository
                            .recover_unbound_attempt_event(
                                &recovery.reservation,
                                event_id,
                                now,
                                retry_at,
                            )?
                            .is_some()
                    } else {
                        repository
                            .try_requeue_unbound_attempt(&recovery.reservation, now)?
                            .is_some()
                    };
                    report.requeued += usize::from(requeued);
                }
                DecisionAttemptState::Running => match recovery.agent_run_status {
                    Some(AgentRunStatus::Starting | AgentRunStatus::Running) => {
                        report.deferred += 1;
                    }
                    Some(
                        AgentRunStatus::Completed
                        | AgentRunStatus::Failed
                        | AgentRunStatus::TimedOut
                        | AgentRunStatus::Cancelled,
                    ) => {
                        let cycle = repository.fail_attempt(
                            recovery.agent_run_id,
                            &recovery.reservation.cycle_id,
                            recovery.reservation.attempt_number,
                            "decision_missing",
                            "terminal decision agent had no persisted decision output",
                            self.limits,
                            now,
                        )?;
                        report.missing += 1;
                        if cycle.state == crate::models::DecisionCycleState::Degraded {
                            report.degraded += 1;
                        }
                    }
                    None => {
                        return Err(AppError::Validation {
                            field: "agent_run_id",
                            message: "running decision attempt must own an existing agent run",
                        });
                    }
                },
                DecisionAttemptState::Decided => report.deferred += 1,
                DecisionAttemptState::Failed => {}
            }
        }
        Ok(report)
    }

    pub async fn apply_ready(
        &self,
        now: i64,
        limit: usize,
    ) -> Result<DecisionLoopReport, AppError> {
        let repository = DecisionRepository::new(self.db);
        let ready = repository.ready_decisions(limit)?;
        let mut report = DecisionLoopReport::default();
        for stored in ready {
            let campaign = CampaignRepository::new(self.db)
                .find_by_id(&stored.reservation.campaign_id)?
                .ok_or(AppError::Validation {
                    field: "campaign",
                    message: "persisted decision campaign does not exist",
                })?;
            let project = ProjectRepository::new(self.db)
                .find_by_id(&campaign.project_id)?
                .ok_or(AppError::Validation {
                    field: "project",
                    message: "persisted decision project does not exist",
                })?;
            if campaign.state != CampaignState::Active
                || !project.enabled
                || project.paused
                || project.halted_reason.is_some()
            {
                report.deferred += 1;
                continue;
            }
            let source = ExperimentRepository::new(self.db)
                .find_by_id(&stored.reservation.source_experiment_id)?
                .ok_or(AppError::Validation {
                    field: "source_experiment_id",
                    message: "persisted decision source experiment does not exist",
                })?;
            let decision = match validate_ready_decision(
                &stored,
                &campaign.objective_digest,
                &source,
                self.limits,
            ) {
                Ok(decision) => decision,
                Err(_) => {
                    record_rejection(
                        &mut report,
                        &repository,
                        &stored.reservation.cycle_id,
                        stored.reservation.attempt_number,
                        "persisted decision failed application validation",
                        self.limits,
                        now,
                    )?;
                    continue;
                }
            };
            let coordinator = match self.policy {
                Some(policy) => CampaignCoordinator::new(self.db, self.pueue, self.limits)
                    .with_root_anchor(
                        policy
                            .project_root_anchor(&project.root_path)
                            .map_err(AppError::from)?,
                    )
                    .with_execution_policy(policy),
                None => CampaignCoordinator::new(self.db, self.pueue, self.limits),
            };

            match decision {
                ValidatedDecision::Wait(wait) => {
                    let _admission = match coordinator.acquire_admission(&project) {
                        Ok(admission) => admission,
                        Err(AppError::Runtime {
                            operation: "acquire project submission admission lock",
                        }) => {
                            report.deferred += 1;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    let requested_minutes = wait
                        .requested_wait_minutes
                        .min(self.limits.max_decision_wait_minutes);
                    let wait_seconds = i64::from(requested_minutes)
                        .checked_mul(60)
                        .ok_or(AppError::Validation {
                            field: "requested_wait_minutes",
                            message: "cannot represent the finite decision wait",
                        })?;
                    let next_wake_at = now.checked_add(wait_seconds).ok_or(
                        AppError::Validation {
                            field: "next_wake_at",
                            message: "cannot represent the finite decision wake time",
                        },
                    )?;
                    repository.mark_waiting(
                        &stored.reservation.cycle_id,
                        stored.reservation.attempt_number,
                        next_wake_at,
                        now,
                    )?;
                    report.waits_scheduled += 1;
                }
                ValidatedDecision::Proposal(proposal) => {
                    let proposal_id = decision_resource_id("proposal", &stored.reservation);
                    let experiment_id = decision_resource_id("experiment", &stored.reservation);
                    let submission_id = decision_resource_id("submission", &stored.reservation);
                    let admission = match coordinator
                        .admit_proposal(
                            &project,
                            &campaign.campaign_id,
                            &proposal_id,
                            &experiment_id,
                            &submission_id,
                            &proposal,
                            now,
                        )
                        .await
                    {
                        Ok(admission) => admission,
                        Err(AppError::Runtime {
                            operation: "acquire project submission admission lock",
                        }) => {
                            report.deferred += 1;
                            continue;
                        }
                        Err(AppError::Validation { .. }) => {
                            record_rejection(
                                &mut report,
                                &repository,
                                &stored.reservation.cycle_id,
                                stored.reservation.attempt_number,
                                "persisted proposal failed campaign validation",
                                self.limits,
                                now,
                            )?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    match admission {
                        CampaignProposalAdmission::Experiment(admitted) => {
                            if !admitted.matches_requested_intent {
                                record_rejection(
                                    &mut report,
                                    &repository,
                                    &stored.reservation.cycle_id,
                                    stored.reservation.attempt_number,
                                    "persisted proposal duplicates another decision attempt",
                                    self.limits,
                                    now,
                                )?;
                                continue;
                            }
                            repository.mark_completed(
                                &stored.reservation.cycle_id,
                                stored.reservation.attempt_number,
                                now,
                            )?;
                            match coordinator
                                .submit_admitted_proposal(admitted, &project, now)
                                .await?
                            {
                                CampaignSubmission::Submitted(_) => {
                                    report.proposals_applied += 1;
                                }
                                CampaignSubmission::Deferred => report.deferred += 1,
                            }
                        }
                        CampaignProposalAdmission::CodeChange(_run) => {
                            repository.mark_completed(
                                &stored.reservation.cycle_id,
                                stored.reservation.attempt_number,
                                now,
                            )?;
                            report.proposals_applied += 1;
                        }
                        CampaignProposalAdmission::CodeChangeRejected(_proposal) => {
                            repository.mark_completed(
                                &stored.reservation.cycle_id,
                                stored.reservation.attempt_number,
                                now,
                            )?;
                        }
                        CampaignProposalAdmission::Deferred => report.deferred += 1,
                    }
                }
                ValidatedDecision::GoalReached(goal) => {
                    // Atomic parking + cycle completion. Validation failures degrade
                    // like malformed decisions; database errors propagate.
                    let evidence_ref = goal.evidence_ref().to_owned();
                    match repository.complete_goal_claim_atomically(
                        &stored.reservation.cycle_id,
                        stored.reservation.attempt_number,
                        &evidence_ref,
                        now,
                    ) {
                        Ok(_) => {
                            // Further claims suppressed via campaign state; scheduler will defer.
                        }
                        Err(AppError::Validation { field, message }) => {
                            // Propagate the specific reason through the degradation counter.
                            let summary = if field == "evidence_ref" {
                                "goal evidence reference does not exist or mismatches campaign"
                            } else {
                                message
                            };
                            record_rejection(
                                &mut report,
                                &repository,
                                &stored.reservation.cycle_id,
                                stored.reservation.attempt_number,
                                summary,
                                self.limits,
                                now,
                            )?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        Ok(report)
    }
}

fn decision_resource_id(kind: &str, reservation: &DecisionReservation) -> String {
    let digest = Sha256::digest(
        format!(
            "campaign-decision-intent:v1\0{}\0{}\0{kind}",
            reservation.cycle_id, reservation.attempt_number
        )
        .as_bytes(),
    );
    format!("decision-{kind}:{digest:x}")
}

#[allow(clippy::too_many_arguments)]
fn record_rejection(
    report: &mut DecisionLoopReport,
    repository: &DecisionRepository<'_>,
    cycle_id: &str,
    attempt_number: i64,
    summary: &str,
    limits: CampaignLimits,
    now: i64,
) -> Result<(), AppError> {
    let cycle = repository.reject_decision(
        cycle_id,
        attempt_number,
        "decision_rejected",
        summary,
        limits,
        now,
    )?;
    report.degraded += usize::from(cycle.state == DecisionCycleState::Degraded);
    Ok(())
}

fn validate_ready_decision(
    stored: &ReadyDecision,
    objective_digest: &str,
    source: &crate::models::Experiment,
    limits: CampaignLimits,
) -> Result<ValidatedDecision, AppError> {
    if source.experiment_id != stored.reservation.source_experiment_id
        || source.campaign_id != stored.reservation.campaign_id
        || !matches!(
            source.status,
            ExperimentStatus::Succeeded | ExperimentStatus::Failed | ExperimentStatus::Cancelled
        )
    {
        return Err(AppError::Validation {
            field: "source_experiment_id",
            message: "must identify the exact terminal decision source experiment",
        });
    }
    validate_ready_context(stored, objective_digest, &source.experiment_id)?;
    let decision = parse_and_validate_decision(
        stored.decision_json.as_bytes(),
        objective_digest,
        limits,
    )?;
    let kind = match &decision {
        ValidatedDecision::Proposal(proposal) => {
            if proposal.source_experiment_id() != Some(source.experiment_id.as_str()) {
                return Err(AppError::Validation {
                    field: "source_experiment_id",
                    message: "must match the exact decision cycle source experiment",
                });
            }
            if proposal.kind() == ProposalKind::Repair
                && (source.status != ExperimentStatus::Failed
                    || source
                        .failure_fingerprint
                        .as_deref()
                        .is_none_or(str::is_empty))
            {
                return Err(AppError::Validation {
                    field: "source_experiment_id",
                    message: "repair decisions require a failed source with a trusted fingerprint",
                });
            }
            "proposal"
        }
        ValidatedDecision::Wait(wait) => {
            if wait.objective_digest() != objective_digest {
                return Err(AppError::Validation {
                    field: "objective_digest",
                    message: "must match the immutable campaign objective",
                });
            }
            "wait"
        }
        ValidatedDecision::GoalReached(goal) => {
            if goal.objective_digest() != objective_digest {
                return Err(AppError::Validation {
                    field: "objective_digest",
                    message: "must match the immutable campaign objective",
                });
            }
            if goal.evidence_ref().is_empty()
                || goal.evidence_ref().len() > crate::decision_protocol::MAX_EVIDENCE_REF_BYTES
                || goal.evidence_ref().chars().any(char::is_control)
            {
                return Err(AppError::Validation {
                    field: "evidence_ref",
                    message: "must be bounded without control characters",
                });
            }
            "goal_reached"
        }
    };
    if stored.decision_kind != kind || stored.decision_digest != decision.canonical_digest() {
        return Err(AppError::Validation {
            field: "decision_digest",
            message: "must match the reparsed persisted decision",
        });
    }
    Ok(decision)
}

fn validate_ready_context(
    stored: &ReadyDecision,
    objective_digest: &str,
    source_experiment_id: &str,
) -> Result<(), AppError> {
    if stored.context_schema_version != Some(i64::from(DECISION_CONTEXT_SCHEMA_VERSION)) {
        return Err(AppError::Validation {
            field: "decision_context.schema_version",
            message: "does not match the persisted decision attempt schema version",
        });
    }
    let context_json = stored.context_json.as_deref().ok_or(AppError::Validation {
        field: "decision_context",
        message: "must be persisted on the decided attempt",
    })?;
    let context_digest = stored.context_digest.as_deref().ok_or(AppError::Validation {
        field: "context_digest",
        message: "must be persisted on the decided attempt",
    })?;
    if format!("{:x}", Sha256::digest(context_json.as_bytes())) != context_digest {
        return Err(AppError::Validation {
            field: "context_digest",
            message: "does not match the exact persisted decision context",
        });
    }
    validate_stored_decision_context(context_json, objective_digest, source_experiment_id)?;
    Ok(())
}
