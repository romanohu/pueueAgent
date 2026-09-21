use rusqlite::{OptionalExtension, TransactionBehavior};
use serde_json::{json, Value};

use crate::{
    campaign::{CampaignAdmission, CampaignCoordinator, CampaignSubmission},
    config,
    db::{
        accept_checkpoint_successor_in_transaction,
        bind_checkpoint_termination_intent_in_transaction,
        bind_research_termination_intent_in_transaction, block_checkpoint_orphan_in_transaction,
        checkpoint_successor_preflight_in_transaction,
        checkpoint_review_claim_in_transaction, CheckpointReviewClaim,
        checkpoint_review_in_transaction,
        checkpoint_unsupported_after_preparation_in_transaction,
        complete_checkpoint_unsupported_in_transaction, complete_research_continue_in_transaction,
        discard_missing_undispatched_research_action_in_transaction,
        discard_undispatched_research_action_in_transaction,
        ready_research_action_selection_in_transaction, CheckpointSuccessorAdmission,
        ReadyResearchActionSelection,
    },
    db::{
        discard_ready_research_action_in_transaction, next_research_due,
        replacement_admission_available_in_transaction, research_ownership_in_transaction,
        CampaignRepository, Db, DecisionRepository, ExperimentRepository, IncidentRepository,
        ProjectRepository, ResearchOwnership, ResearchRepository,
        TerminalDecisionEventProjection, TerminationRequestRepository,
    },
    environment::cleanup_retained_research_file,
    execution_policy::{
        resolve_project_policy, PolicyViolationDetail, ResolvedExecutionPolicy, TempUnsafeReason,
    },
    models::{NewIncident, NewTerminationRequest},
    output::bounded_redacted_text,
    pueue::{PueueApi, PueueTask},
    reconcile::{managed_task_run_signature, parse_timestamp, task_incident_key},
    research_checkpoint::{
        prepare_checkpoint, serialize_prepared_checkpoint, PrepareCheckpointError,
    },
    termination::UNDISPATCHED_CONFIRMATION_PREFIX,
    AppError,
};

/// Consume bounded native research answers after they have passed the same
/// live-task and cleanup-proof checks as the launch path.  A stop action only
/// records its intent here; the regular termination manager owns dispatch and
/// confirmation of the stored raw Pueue signature.
pub async fn advance_research_actions<P: PueueApi + ?Sized>(
    db: &Db,
    pueue: &P,
    policy: &ResolvedExecutionPolicy,
    now: i64,
    limit: usize,
) -> Result<usize, AppError> {
    if limit == 0 {
        return Ok(0);
    }
    let repository = ResearchRepository::new(db);
    let mut advanced = 0;
    for review in repository.open_action_reviews(limit.max(32))? {
        if advanced >= limit {
            break;
        }
        if advance_open_research_action(db, pueue, policy, now, &review).await? {
            advanced += 1;
        } else {
            repository.rotate_open_action_review(&review.review_id, now)?;
        }
    }
    if policy.campaign_limits.research_interval_minutes == 0 {
        return Ok(advanced);
    }
    if advanced >= limit {
        return Ok(advanced);
    }
    let reviews = repository.ready_reviews(limit.saturating_sub(advanced).max(32))?;
    for review in reviews {
        if advanced >= limit {
            break;
        }
        let Some(campaign) = CampaignRepository::new(db).find_by_id(&review.campaign_id)? else {
            repository.rotate_ready_action_review(&review.review_id, now)?;
            continue;
        };
        let Some(project) = ProjectRepository::new(db).find_by_id(&campaign.project_id)? else {
            repository.rotate_ready_action_review(&review.review_id, now)?;
            continue;
        };
        let Some(experiment) = ExperimentRepository::new(db).find_by_id(&review.experiment_id)?
        else {
            repository.rotate_ready_action_review(&review.review_id, now)?;
            continue;
        };
        let Some(task_id) = experiment.pueue_task_id else {
            repository.rotate_ready_action_review(&review.review_id, now)?;
            continue;
        };
        let coordinator = CampaignCoordinator::new(db, pueue, policy.campaign_limits)
            .with_root_anchor(
                policy
                    .project_root_anchor(&project.root_path)
                    .map_err(AppError::from)?,
            )
            .with_execution_policy(policy);
        let admission = match coordinator.acquire_admission(&project) {
            Ok(admission) => admission,
            Err(AppError::Runtime {
                operation: "acquire project submission admission lock",
            }) => {
                repository.rotate_ready_action_review(&review.review_id, now)?;
                continue;
            }
            Err(error) => return Err(error),
        };
        let tasks = pueue.status_json().await?;
        let task = tasks.iter().find(|task| {
            task.id == task_id
                && task.group == project.pueue_group
                && managed_task_run_signature(task).as_deref()
                    == Some(review.task_signature.as_str())
        });
        let Some(task) = task else {
            let mut connection = db.connect()?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(crate::db::database_error(
                    "begin stale research action discard",
                ))?;
            let discarded = discard_ready_research_action_in_transaction(
                &transaction,
                &campaign.project_id,
                &review.review_id,
                "research_target_identity_stale",
                now,
            )?;
            transaction.commit().map_err(crate::db::database_error(
                "commit stale research action discard",
            ))?;
            if discarded {
                advanced += 1;
            } else {
                repository.rotate_ready_action_review(&review.review_id, now)?;
            }
            continue;
        };
        if !task.is_running() {
            if !task.is_terminal() {
                repository.rotate_ready_action_review(&review.review_id, now)?;
                continue;
            }
            let mut connection = db.connect()?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(crate::db::database_error(
                    "begin natural research action discard",
                ))?;
            let discarded = discard_ready_research_action_in_transaction(
                &transaction,
                &campaign.project_id,
                &review.review_id,
                "research_target_natural_terminal",
                now,
            )?;
            transaction.commit().map_err(crate::db::database_error(
                "commit natural research action discard",
            ))?;
            if discarded {
                advanced += 1;
            } else {
                repository.rotate_ready_action_review(&review.review_id, now)?;
            }
            continue;
        }

        enum ReadyActionTransactionOutcome {
            Resume(crate::db::ReadyResearchAction),
            Handled(bool),
        }

        let outcome = {
            let mut connection = db.connect()?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(crate::db::database_error("begin research action admission"))?;
            let selection = ready_research_action_selection_in_transaction(
                &transaction,
                &campaign.project_id,
                &review.review_id,
                task,
            )?;
            match selection {
                ReadyResearchActionSelection::NotReady => {
                    transaction.commit().map_err(crate::db::database_error(
                        "commit skipped research action",
                    ))?;
                    ReadyActionTransactionOutcome::Handled(false)
                }
                ReadyResearchActionSelection::CheckpointUnsupported(witness) => {
                    let next_due = next_research_due(
                        now,
                        policy.campaign_limits.research_interval_minutes,
                    )?;
                    let changed = complete_checkpoint_unsupported_in_transaction(
                        &transaction,
                        &witness,
                        next_due,
                        now,
                    )?;
                    transaction.commit().map_err(crate::db::database_error(
                        "commit unsupported checkpoint action",
                    ))?;
                    ReadyActionTransactionOutcome::Handled(changed)
                }
                ReadyResearchActionSelection::Authorized(action)
                    if action.answer.action == "resume_from_checkpoint" =>
                {
                    transaction.commit().map_err(crate::db::database_error(
                        "commit checkpoint preparation start",
                    ))?;
                    ReadyActionTransactionOutcome::Resume(action)
                }
                ReadyResearchActionSelection::Authorized(action) => {
                    match action.answer.action.as_str() {
                        "continue" => {
                            match serde_json::from_str::<Value>(&action.notes_json) {
                                Ok(mut notes) if notes.is_object() => {
                                    notes["saved_advice"] =
                                        json!(bounded_redacted_text(&action.answer.notes));
                                    let notes_json = serde_json::to_string(&notes).map_err(
                                        |source| AppError::Serialization {
                                            operation: "serialize continuing research notes",
                                            source,
                                        },
                                    )?;
                                    let next_due = next_research_due(
                                        now,
                                        policy.campaign_limits.research_interval_minutes,
                                    )?;
                                    let changed = complete_research_continue_in_transaction(
                                        &transaction,
                                        &action,
                                        &notes_json,
                                        next_due,
                                        now,
                                    )?;
                                    transaction.commit().map_err(crate::db::database_error(
                                        "commit research action",
                                    ))?;
                                    ReadyActionTransactionOutcome::Handled(changed)
                                }
                                _ => {
                                    transaction.commit().map_err(crate::db::database_error(
                                        "commit invalid research notes",
                                    ))?;
                                    ReadyActionTransactionOutcome::Handled(false)
                                }
                            }
                        }
                        "stop_and_next" => {
                            if !replacement_admission_available_in_transaction(
                                &transaction,
                                &action.owner.campaign_id,
                                &action.owner.source_experiment_id,
                                &policy.campaign_limits,
                                now,
                            )? {
                                transaction.commit().map_err(crate::db::database_error(
                                    "commit deferred research action",
                                ))?;
                                ReadyActionTransactionOutcome::Handled(false)
                            } else {
                                let reason = format!(
                                    "research_action:{}:{}",
                                    action.owner.review_id,
                                    bounded_redacted_text(&action.answer.reason)
                                );
                                let incident = NewIncident::new(
                                    campaign.project_id.clone(),
                                    "research_action",
                                    Some(task_incident_key(task)),
                                    format!("research_action:v1:{}", action.owner.review_id),
                                    now,
                                );
                                let incident = IncidentRepository::upsert_active_in_transaction(
                                    &transaction,
                                    &incident,
                                )?;
                                let request = NewTerminationRequest::new(
                                    incident.incident.incident_id,
                                    campaign.project_id.clone(),
                                    action.raw_task_signature.clone(),
                                    reason,
                                    now,
                                    None,
                                );
                                let request =
                                    TerminationRequestRepository::insert_idempotent_in_transaction(
                                        &transaction,
                                        &request,
                                    )?;
                                let bound = bind_research_termination_intent_in_transaction(
                                    &transaction,
                                    &action,
                                    &incident.incident,
                                    &request,
                                    now,
                                )?;
                                if !bound {
                                    transaction.rollback().map_err(crate::db::database_error(
                                        "rollback unbound research action",
                                    ))?;
                                    ReadyActionTransactionOutcome::Handled(false)
                                } else {
                                    transaction.commit().map_err(crate::db::database_error(
                                        "commit research action",
                                    ))?;
                                    ReadyActionTransactionOutcome::Handled(true)
                                }
                            }
                        }
                        _ => {
                            transaction.commit().map_err(crate::db::database_error(
                                "commit unsupported research action",
                            ))?;
                            ReadyActionTransactionOutcome::Handled(false)
                        }
                    }
                }
            }
        };
        match outcome {
            ReadyActionTransactionOutcome::Resume(action) => {
                let changed = advance_ready_checkpoint_action(
                    db, pueue, policy, &project, &campaign, task, &review, action, admission, now,
                )
                .await?;
                if changed {
                    advanced += 1;
                } else {
                    repository.rotate_ready_action_review(&review.review_id, now)?;
                }
            }
            ReadyActionTransactionOutcome::Handled(changed) => {
                if changed {
                    advanced += 1;
                } else {
                    repository.rotate_ready_action_review(&review.review_id, now)?;
                }
            }
        }
    }
    Ok(advanced)
}

pub(crate) fn cleanup_research_checkpoints<P: PueueApi + ?Sized>(
    db: &Db,
    pueue: &P,
    policy: &ResolvedExecutionPolicy,
    now: i64,
    limit: usize,
) -> Result<usize, AppError> {
    if limit == 0 {
        return Ok(0);
    }
    let repository = ResearchRepository::new(db);
    let mut cleaned = 0;
    let mut inspected = 0;
    for project in ProjectRepository::new(db).list_all()? {
        if inspected >= limit {
            break;
        }
        let root_anchor = policy
            .project_root_anchor(&project.root_path)
            .map_err(AppError::from)?;
        let coordinator = CampaignCoordinator::new(db, pueue, policy.campaign_limits)
            .with_root_anchor(root_anchor)
            .with_execution_policy(policy);
        let _admission = match coordinator.acquire_admission(&project) {
            Ok(admission) => admission,
            Err(AppError::Runtime {
                operation: "acquire project submission admission lock",
            }) => continue,
            Err(error) => return Err(error),
        };
        let review_ids = repository
            .checkpoint_cleanup_review_ids(&project.project_id, limit.saturating_sub(inspected))?;
        for review_id in review_ids {
            if inspected >= limit {
                break;
            }
            inspected += 1;
            let Some(authority) =
                repository.checkpoint_cleanup_authority(&project.project_id, &review_id, now)?
            else {
                continue;
            };
            match cleanup_retained_research_file(
                policy,
                authority.campaign_id(),
                authority.review_id(),
                authority.retained_checkpoint(),
            ) {
                Ok(()) => {}
                Err(error)
                    if error.detail
                        == PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure) =>
                {
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
            if repository.settle_checkpoint_cleanup(&authority, now)? {
                cleaned += 1;
            }
        }
    }
    Ok(cleaned)
}

async fn advance_ready_checkpoint_action<P: PueueApi + ?Sized>(
    db: &Db,
    pueue: &P,
    policy: &ResolvedExecutionPolicy,
    project: &crate::models::Project,
    campaign: &crate::models::Campaign,
    task: &PueueTask,
    review: &crate::db::ResearchReview,
    action: crate::db::ReadyResearchAction,
    admission: CampaignAdmission,
    now: i64,
) -> Result<bool, AppError> {
    let request = action
        .answer
        .checkpoint
        .clone()
        .ok_or_else(|| AppError::Validation {
            field: "research.checkpoint",
            message: "resume action is missing its checkpoint request",
        })?;
    let project_config = config::load(&project.config_path)?;
    if project_config.project_id != project.project_id
        || project_config.pueue_group != project.pueue_group
    {
        return Err(AppError::Validation {
            field: "project_id",
            message: "project config identity does not match the database project",
        });
    }
    let project_policy =
        resolve_project_policy(policy, project, &project_config).map_err(AppError::from)?;
    let prepared = match prepare_checkpoint(db, &action, &request, policy, &project_policy) {
        Ok(prepared) => prepared,
        Err(PrepareCheckpointError::Unsupported { reason }) => {
            return settle_preparation_unsupported(
                db, pueue, policy, project, campaign, task, review, &action, &request, &reason, now,
            )
            .await;
        }
        Err(PrepareCheckpointError::OrphanedRetainedCheckpoint) => {
            let mut connection = db.connect()?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(crate::db::database_error(
                    "begin orphaned checkpoint review block",
                ))?;
            let changed = block_checkpoint_orphan_in_transaction(&transaction, &action, now)?;
            transaction.commit().map_err(crate::db::database_error(
                "commit orphaned checkpoint review block",
            ))?;
            return Ok(changed);
        }
        Err(PrepareCheckpointError::Failed(error)) => return Err(error),
    };
    let checkpoint = prepared.checkpoint().clone();
    let checkpoint_json = match serialize_prepared_checkpoint(&checkpoint) {
        Ok(json) => json,
        Err(error) => {
            cleanup_unbound_checkpoint(policy, &action, prepared)?;
            return Err(error);
        }
    };

    let tasks = match pueue.status_json().await {
        Ok(tasks) => tasks,
        Err(error) => {
            cleanup_unbound_checkpoint(policy, &action, prepared)?;
            return Err(error);
        }
    };
    let Some(live_task) = tasks.iter().find(|candidate| {
        candidate.id == task.id
            && candidate.group == project.pueue_group
            && candidate.is_running()
            && managed_task_run_signature(candidate).as_deref()
                == Some(action.owner.managed_task_signature.as_str())
    }) else {
        cleanup_unbound_checkpoint(policy, &action, prepared)?;
        return Ok(false);
    };

    enum CheckpointAdmissionOutcome {
        Unsupported,
        Stale,
        Bound,
    }

    let admission_result = (|| -> Result<CheckpointAdmissionOutcome, AppError> {
        let mut connection = db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(crate::db::database_error(
                "begin final checkpoint action admission",
            ))?;
        let selection = ready_research_action_selection_in_transaction(
            &transaction,
            &project.project_id,
            &review.review_id,
            live_task,
        )?;
        let final_action = match selection {
            ReadyResearchActionSelection::Authorized(final_action)
                if ready_checkpoint_action_matches(&action, &final_action, &request) =>
            {
                final_action
            }
            ReadyResearchActionSelection::CheckpointUnsupported(_witness) => {
                transaction.commit().map_err(crate::db::database_error(
                    "commit refreshed unsupported checkpoint action",
                ))?;
                return Ok(CheckpointAdmissionOutcome::Unsupported);
            }
            _ => {
                transaction.commit().map_err(crate::db::database_error(
                    "commit stale final checkpoint action",
                ))?;
                return Ok(CheckpointAdmissionOutcome::Stale);
            }
        };
        let preflight = checkpoint_successor_preflight_in_transaction(
            &transaction,
            &final_action,
            &checkpoint,
            &policy.campaign_limits,
            now,
        )?;
        if matches!(preflight, crate::db::CheckpointSuccessorPreflight::Deferred) {
            transaction.commit().map_err(crate::db::database_error(
                "commit deferred checkpoint action",
            ))?;
            return Ok(CheckpointAdmissionOutcome::Stale);
        }

        let reason = format!(
            "research_action:{}:{}",
            final_action.owner.review_id,
            bounded_redacted_text(&final_action.answer.reason)
        );
        let incident = NewIncident::new(
            campaign.project_id.clone(),
            "research_action",
            Some(task_incident_key(live_task)),
            format!("research_action:v1:{}", final_action.owner.review_id),
            now,
        );
        let incident =
            IncidentRepository::upsert_active_in_transaction(&transaction, &incident)?;
        let request_row = NewTerminationRequest::new(
            incident.incident.incident_id,
            campaign.project_id.clone(),
            final_action.raw_task_signature.clone(),
            reason,
            now,
            None,
        );
        let request_row = TerminationRequestRepository::insert_idempotent_in_transaction(
            &transaction,
            &request_row,
        )?;
        let bound = bind_checkpoint_termination_intent_in_transaction(
            &transaction,
            &final_action,
            &checkpoint_json,
            &incident.incident,
            &request_row,
            now,
        )?;
        if !bound {
            transaction.rollback().map_err(crate::db::database_error(
                "rollback stale checkpoint action binding",
            ))?;
            return Ok(CheckpointAdmissionOutcome::Stale);
        }
        transaction.commit().map_err(crate::db::database_error(
            "commit checkpoint action binding",
        ))?;
        Ok(CheckpointAdmissionOutcome::Bound)
    })();
    match admission_result {
        Ok(CheckpointAdmissionOutcome::Unsupported) => {
            cleanup_unbound_checkpoint(policy, &action, prepared)?;
            settle_checkpoint_unsupported_after_cleanup(
                db,
                pueue,
                policy,
                project,
                task,
                review,
                &action,
                &request,
                now,
            )
            .await
        }
        Ok(CheckpointAdmissionOutcome::Stale) => {
            cleanup_unbound_checkpoint(policy, &action, prepared)?;
            Ok(false)
        }
        Ok(CheckpointAdmissionOutcome::Bound) => {
            let _ = admission;
            drop(prepared.release_lease());
            Ok(true)
        }
        Err(error) => {
            cleanup_unbound_checkpoint(policy, &action, prepared)?;
            Err(error)
        }
    }
}

fn cleanup_unbound_checkpoint(
    policy: &ResolvedExecutionPolicy,
    action: &crate::db::ReadyResearchAction,
    prepared: crate::research_checkpoint::VerifiedPreparedCheckpoint,
) -> Result<(), AppError> {
    let checkpoint = prepared.release_lease();
    cleanup_retained_research_file(
        policy,
        &action.owner.campaign_id,
        &action.owner.review_id,
        &checkpoint.retained_checkpoint,
    )
    .map_err(AppError::from)
}

fn ready_checkpoint_action_matches(
    expected: &crate::db::ReadyResearchAction,
    current: &crate::db::ReadyResearchAction,
    request: &crate::research_protocol::CheckpointRequest,
) -> bool {
    expected.owner == current.owner
        && expected.context_json == current.context_json
        && expected.context_digest == current.context_digest
        && expected.response_json == current.response_json
        && expected.notes_json == current.notes_json
        && expected.campaign_objective_digest == current.campaign_objective_digest
        && expected.raw_task_signature == current.raw_task_signature
        && current.answer.checkpoint.as_ref() == Some(request)
}

async fn settle_preparation_unsupported<P: PueueApi + ?Sized>(
    db: &Db,
    pueue: &P,
    policy: &ResolvedExecutionPolicy,
    project: &crate::models::Project,
    _campaign: &crate::models::Campaign,
    task: &PueueTask,
    review: &crate::db::ResearchReview,
    action: &crate::db::ReadyResearchAction,
    request: &crate::research_protocol::CheckpointRequest,
    reason: &str,
    now: i64,
) -> Result<bool, AppError> {
    let tasks = pueue.status_json().await?;
    let Some(live_task) = tasks.iter().find(|candidate| {
        candidate.id == task.id
            && candidate.group == project.pueue_group
            && candidate.is_running()
            && managed_task_run_signature(candidate).as_deref()
                == Some(action.owner.managed_task_signature.as_str())
    }) else {
        return Ok(false);
    };
    let mut connection = db.connect()?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(crate::db::database_error(
            "begin refreshed unsupported checkpoint action",
        ))?;
    let witness = match ready_research_action_selection_in_transaction(
        &transaction,
        &project.project_id,
        &review.review_id,
        live_task,
    )? {
        ReadyResearchActionSelection::Authorized(current)
            if ready_checkpoint_action_matches(action, &current, request) =>
        {
            checkpoint_unsupported_after_preparation_in_transaction(
                &transaction,
                &current,
                request,
                reason,
            )?
        }
        ReadyResearchActionSelection::CheckpointUnsupported(_) => {
            // The selector's witness is only a classification of the fresh
            // row.  Settlement must still bind the preparation-time action,
            // request, and exact unsupported reason in this transaction.
            checkpoint_unsupported_after_preparation_in_transaction(
                &transaction,
                action,
                request,
                reason,
            )?
        }
        _ => None,
    };
    let Some(witness) = witness else {
        transaction.commit().map_err(crate::db::database_error(
            "commit unchanged unsupported checkpoint action",
        ))?;
        return Ok(false);
    };
    let next_due = next_research_due(now, policy.campaign_limits.research_interval_minutes)?;
    let changed =
        complete_checkpoint_unsupported_in_transaction(&transaction, &witness, next_due, now)?;
    transaction.commit().map_err(crate::db::database_error(
        "commit prepared unsupported checkpoint action",
    ))?;
    Ok(changed)
}

async fn settle_checkpoint_unsupported_after_cleanup<P: PueueApi + ?Sized>(
    db: &Db,
    pueue: &P,
    policy: &ResolvedExecutionPolicy,
    project: &crate::models::Project,
    task: &PueueTask,
    review: &crate::db::ResearchReview,
    action: &crate::db::ReadyResearchAction,
    request: &crate::research_protocol::CheckpointRequest,
    now: i64,
) -> Result<bool, AppError> {
    let tasks = pueue.status_json().await?;
    let Some(live_task) = tasks.iter().find(|candidate| {
        candidate.id == task.id
            && candidate.group == project.pueue_group
            && candidate.is_running()
            && managed_task_run_signature(candidate).as_deref()
                == Some(action.owner.managed_task_signature.as_str())
    }) else {
        return Ok(false);
    };
    let next_due = next_research_due(now, policy.campaign_limits.research_interval_minutes)?;
    let mut connection = db.connect()?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(crate::db::database_error(
            "begin post-cleanup unsupported checkpoint settlement",
        ))?;
    let witness = match ready_research_action_selection_in_transaction(
        &transaction,
        &project.project_id,
        &review.review_id,
        live_task,
    )? {
        ReadyResearchActionSelection::CheckpointUnsupported(witness) => Some(witness),
        ReadyResearchActionSelection::Authorized(current)
            if ready_checkpoint_action_matches(action, &current, request) =>
        {
            None
        }
        _ => None,
    };
    let changed = match witness {
        Some(witness) => {
            complete_checkpoint_unsupported_in_transaction(&transaction, &witness, next_due, now)?
        }
        None => false,
    };
    transaction.commit().map_err(crate::db::database_error(
        "commit post-cleanup unsupported checkpoint settlement",
    ))?;
    Ok(changed)
}

async fn recover_checkpoint_successor<P: PueueApi + ?Sized>(
    db: &Db,
    policy: &ResolvedExecutionPolicy,
    project: &crate::models::Project,
    coordinator: &CampaignCoordinator<'_, P>,
    admission: &mut Option<CampaignAdmission>,
    review_id: &str,
    now: i64,
) -> Result<Option<bool>, AppError> {
    enum CheckpointRecoveryOutcome {
        NoClaim,
        Handled(bool),
        Submit(
            crate::db::ManagedSubmissionIntent,
            CampaignAdmission,
        ),
    }

    let recovery_result = (|| -> Result<CheckpointRecoveryOutcome, AppError> {
        let mut connection = db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(crate::db::database_error(
                "begin checkpoint successor recovery",
            ))?;
        let Some(review) = checkpoint_review_in_transaction(&transaction, review_id)? else {
            transaction.commit().map_err(crate::db::database_error(
                "commit missing checkpoint successor recovery",
            ))?;
            return Ok(CheckpointRecoveryOutcome::NoClaim);
        };
        match checkpoint_review_claim_in_transaction(&transaction, review_id)? {
            CheckpointReviewClaim::Checkpoint => {}
            CheckpointReviewClaim::Ordinary => {
                transaction.commit().map_err(crate::db::database_error(
                    "commit ordinary research action routing",
                ))?;
                return Ok(CheckpointRecoveryOutcome::NoClaim);
            }
            CheckpointReviewClaim::Invalid => {
                transaction.commit().map_err(crate::db::database_error(
                    "commit invalid research action routing",
                ))?;
                return Ok(if review.operation_stage.as_deref() == Some("stop_confirmed") {
                    CheckpointRecoveryOutcome::Handled(false)
                } else {
                    CheckpointRecoveryOutcome::NoClaim
                });
            }
        }
        let ownership = research_ownership_in_transaction(
            &transaction,
            &project.project_id,
            &review.campaign_id,
            &review.experiment_id,
        )?;
        let ResearchOwnership::Open(Some(owner)) = ownership else {
            transaction.commit().map_err(crate::db::database_error(
                "commit skipped checkpoint successor recovery",
            ))?;
            return Ok(CheckpointRecoveryOutcome::Handled(false));
        };
        if owner.review_id != review.review_id
            || owner.campaign_id != review.campaign_id
            || owner.source_experiment_id != review.experiment_id
        {
            transaction.commit().map_err(crate::db::database_error(
                "commit mismatched checkpoint successor recovery",
            ))?;
            return Ok(CheckpointRecoveryOutcome::Handled(false));
        }
        if !matches!(
            review.checkpoint_json_state,
            crate::db::CheckpointJsonState::BoundedText
        ) {
            let changed = crate::db::block_invalid_checkpoint_review_in_transaction(
                &transaction,
                &review,
                now,
            )?;
            transaction.commit().map_err(crate::db::database_error(
                "commit invalid checkpoint action routing",
            ))?;
            return Ok(CheckpointRecoveryOutcome::Handled(changed));
        }
        let successor = accept_checkpoint_successor_in_transaction(
            &transaction,
            &owner,
            &policy.campaign_limits,
            now,
        )?;
        match successor {
            CheckpointSuccessorAdmission::Ready(intent) => {
                let Some(admission) = admission.take() else {
                    transaction.commit().map_err(crate::db::database_error(
                        "commit unavailable checkpoint successor admission",
                    ))?;
                    return Ok(CheckpointRecoveryOutcome::Handled(false));
                };
                transaction.commit().map_err(crate::db::database_error(
                    "commit checkpoint successor recovery",
                ))?;
                Ok(CheckpointRecoveryOutcome::Submit(intent, admission))
            }
            CheckpointSuccessorAdmission::Deferred => {
                transaction.commit().map_err(crate::db::database_error(
                    "commit deferred checkpoint successor recovery",
                ))?;
                Ok(CheckpointRecoveryOutcome::Handled(false))
            }
            CheckpointSuccessorAdmission::Blocked => {
                transaction.commit().map_err(crate::db::database_error(
                    "commit blocked checkpoint successor recovery",
                ))?;
                Ok(CheckpointRecoveryOutcome::Handled(true))
            }
        }
    })();
    match recovery_result {
        Ok(CheckpointRecoveryOutcome::NoClaim) => Ok(None),
        Ok(CheckpointRecoveryOutcome::Handled(changed)) => Ok(Some(changed)),
        Ok(CheckpointRecoveryOutcome::Submit(intent, admission)) => {
            let result = coordinator
                .submit_checkpoint_intent_with_admission(&intent, project, admission, now)
                .await?;
            Ok(Some(matches!(result, CampaignSubmission::Submitted(_))))
        }
        Err(error) => Err(error),
    }
}

async fn advance_open_research_action<P: PueueApi + ?Sized>(
    db: &Db,
    pueue: &P,
    policy: &ResolvedExecutionPolicy,
    now: i64,
    review: &crate::db::ResearchReview,
) -> Result<bool, AppError> {
    let Some(campaign) = CampaignRepository::new(db).find_by_id(&review.campaign_id)? else {
        return Ok(false);
    };
    let Some(project) = ProjectRepository::new(db).find_by_id(&campaign.project_id)? else {
        return Ok(false);
    };
    let Some(experiment) = ExperimentRepository::new(db).find_by_id(&review.experiment_id)? else {
        return Ok(false);
    };
    let coordinator = CampaignCoordinator::new(db, pueue, policy.campaign_limits).with_root_anchor(
        policy
            .project_root_anchor(&project.root_path)
            .map_err(AppError::from)?,
        )
        .with_execution_policy(policy);
    let admission = match coordinator.acquire_admission(&project) {
        Ok(admission) => admission,
        Err(AppError::Runtime {
            operation: "acquire project submission admission lock",
        }) => return Ok(false),
        Err(error) => return Err(error),
    };
    let mut admission = Some(admission);
    if let Some(changed) = recover_checkpoint_successor(
            db,
            policy,
            &project,
            &coordinator,
            &mut admission,
            &review.review_id,
            now,
        )
        .await?
    {
        return Ok(changed);
    }
    let review = ResearchRepository::new(db).find(&review.review_id)?;
    let Some(task_id) = experiment.pueue_task_id else {
        return Ok(false);
    };
    let tasks = pueue.status_json().await?;
    let task = tasks.iter().find(|task| {
        task.id == task_id
            && task.group == project.pueue_group
            && managed_task_run_signature(task).as_deref() == Some(review.task_signature.as_str())
    });
    let Some(task) = task else {
        let Some(request_id) = review.termination_request_id else {
            return Ok(false);
        };
        let mut connection = db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(crate::db::database_error(
                "begin missing research target discard",
            ))?;
        let ownership = research_ownership_in_transaction(
            &transaction,
            &campaign.project_id,
            &campaign.campaign_id,
            &review.experiment_id,
        )?;
        let ResearchOwnership::Open(Some(owner)) = ownership else {
            transaction.commit().map_err(crate::db::database_error(
                "commit missing target ownership check",
            ))?;
            return Ok(false);
        };
        if owner.recovery_required
            || owner.review_id != review.review_id
            || owner.campaign_id != campaign.campaign_id
            || owner.source_experiment_id != review.experiment_id
            || owner.managed_task_signature != review.task_signature
            || owner.operation_stage.as_deref() != Some("intent")
            || owner.termination_request_id != Some(request_id)
            || owner.decision_cycle_id.is_some()
            || owner.successor_experiment_id.is_some()
        {
            transaction.commit().map_err(crate::db::database_error(
                "commit mismatched missing target ownership",
            ))?;
            return Ok(false);
        }
        let discarded = discard_missing_undispatched_research_action_in_transaction(
            &transaction,
            &campaign.project_id,
            &review.review_id,
            request_id,
            now,
        )?;
        transaction.commit().map_err(crate::db::database_error(
            "commit missing research target discard",
        ))?;
        return Ok(discarded);
    };

    enum OpenActionTransactionOutcome {
        RecoverCheckpoint(String),
        Handled(bool),
    }

    let transaction_result = (|| -> Result<OpenActionTransactionOutcome, AppError> {
        let mut connection = db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(crate::db::database_error(
                "begin open research action recovery",
            ))?;
        let ownership = research_ownership_in_transaction(
            &transaction,
            &campaign.project_id,
            &campaign.campaign_id,
            &review.experiment_id,
        )?;
        let ResearchOwnership::Open(Some(mut owner)) = ownership else {
            transaction.commit().map_err(crate::db::database_error(
                "commit skipped open research action",
            ))?;
            return Ok(OpenActionTransactionOutcome::Handled(false));
        };
        if owner.recovery_required
            || owner.operation_stage.as_deref() == Some("successor_reserved")
        {
            transaction.commit().map_err(crate::db::database_error(
                "commit invalid open research action",
            ))?;
            return Ok(OpenActionTransactionOutcome::Handled(false));
        }
        let Some(request_id) = owner.termination_request_id else {
            transaction.commit().map_err(crate::db::database_error(
                "commit unbound open research action",
            ))?;
            return Ok(OpenActionTransactionOutcome::Handled(false));
        };
        let request = transaction
            .query_row(
                "SELECT project_id, task_signature, status, grace_until, last_error
                 FROM termination_requests WHERE request_id = ?1",
                [request_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .optional()
            .map_err(crate::db::database_error("read open research termination"))?;
        let Some((request_project, raw_task_signature, request_status, grace_until, last_error)) =
            request
        else {
            transaction.commit().map_err(crate::db::database_error(
                "commit missing open research termination",
            ))?;
            return Ok(OpenActionTransactionOutcome::Handled(false));
        };
        if request_project != owner.project_id
            || !termination_request_targets_task(&raw_task_signature, task)
        {
            transaction.commit().map_err(crate::db::database_error(
                "commit mismatched open research termination",
            ))?;
            return Ok(OpenActionTransactionOutcome::Handled(false));
        }

        let source = transaction
            .query_row(
                "SELECT status, pueue_task_id, task_signature
                 FROM experiments
                 WHERE experiment_id = ?1 AND campaign_id = ?2",
                rusqlite::params![owner.source_experiment_id, owner.campaign_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(crate::db::database_error("read open research source"))?;
        let Some((source_status, source_task_id, source_managed_signature)) = source else {
            transaction.commit().map_err(crate::db::database_error(
                "commit missing open research source",
            ))?;
            return Ok(OpenActionTransactionOutcome::Handled(false));
        };
        if source_task_id != Some(task.id)
            || source_managed_signature.as_deref() != Some(owner.managed_task_signature.as_str())
        {
            transaction.commit().map_err(crate::db::database_error(
                "commit mismatched open research source",
            ))?;
            return Ok(OpenActionTransactionOutcome::Handled(false));
        }

        let stage = owner.operation_stage.as_deref();
        if stage == Some("intent") {
            let dispatched = request_status == "sent"
                || (request_status == "confirmed"
                    && (grace_until.is_some() || last_error.is_none()));
            if dispatched {
                let changed = transaction
                    .execute(
                        "UPDATE research_reviews
                         SET operation_stage = 'stop_requested', updated_at = ?1
                         WHERE review_id = ?2 AND state = 'ready'
                           AND operation_stage = 'intent'
                           AND termination_request_id = ?3
                           AND decision_cycle_id IS NULL
                           AND successor_experiment_id IS NULL",
                        rusqlite::params![now, owner.review_id, request_id],
                    )
                    .map_err(crate::db::database_error(
                        "repair research stop request stage",
                    ))?;
                if changed != 1 {
                    transaction.commit().map_err(crate::db::database_error(
                        "commit unchanged research stop stage",
                    ))?;
                    return Ok(OpenActionTransactionOutcome::Handled(false));
                }
                owner.operation_stage = Some("stop_requested".to_owned());
            } else if task.is_terminal()
                && (request_status == "requested"
                    || (request_status == "confirmed"
                        && grace_until.is_none()
                        && last_error.as_deref().is_some_and(|error| {
                            error.starts_with(UNDISPATCHED_CONFIRMATION_PREFIX)
                        })))
            {
                let discarded = discard_undispatched_research_action_in_transaction(
                    &transaction,
                    &owner.review_id,
                    request_id,
                    "research_natural_finish_before_dispatch",
                    now,
                )?;
                transaction.commit().map_err(crate::db::database_error(
                    "commit undispatched research discard",
                ))?;
                return Ok(OpenActionTransactionOutcome::Handled(discarded));
            } else {
                transaction
                    .commit()
                    .map_err(crate::db::database_error("commit deferred research intent"))?;
                return Ok(OpenActionTransactionOutcome::Handled(false));
            }
        }

        if !task.is_terminal()
            || !matches!(source_status.as_str(), "succeeded" | "failed" | "cancelled")
            || request_status != "confirmed"
            || !(grace_until.is_some() || last_error.is_none())
        {
            transaction.commit().map_err(crate::db::database_error(
                "commit deferred terminal research action",
            ))?;
            return Ok(OpenActionTransactionOutcome::Handled(false));
        }
        let mut confirmed_owner = owner;
        if confirmed_owner.operation_stage.as_deref() == Some("stop_requested") {
            let changed = transaction
                .execute(
                    "UPDATE research_reviews
                     SET operation_stage = 'stop_confirmed', updated_at = ?1
                     WHERE review_id = ?2 AND state = 'ready'
                       AND operation_stage = 'stop_requested'
                       AND termination_request_id = ?3
                       AND decision_cycle_id IS NULL
                       AND successor_experiment_id IS NULL",
                    rusqlite::params![now, confirmed_owner.review_id, request_id],
                )
                .map_err(crate::db::database_error("confirm research stop stage"))?;
            if changed != 1 {
                transaction
                    .commit()
                    .map_err(crate::db::database_error("commit unchanged confirmed stop"))?;
                return Ok(OpenActionTransactionOutcome::Handled(false));
            }
            confirmed_owner.operation_stage = Some("stop_confirmed".to_owned());
        }
        if confirmed_owner.operation_stage.as_deref() != Some("stop_confirmed") {
            transaction.commit().map_err(crate::db::database_error(
                "commit invalid confirmed stop stage",
            ))?;
            return Ok(OpenActionTransactionOutcome::Handled(false));
        }

        match checkpoint_review_claim_in_transaction(&transaction, &confirmed_owner.review_id)? {
            CheckpointReviewClaim::Checkpoint => {
                transaction.commit().map_err(crate::db::database_error(
                    "commit checkpoint successor routing",
                ))?;
                return Ok(OpenActionTransactionOutcome::RecoverCheckpoint(
                    confirmed_owner.review_id,
                ));
            }
            CheckpointReviewClaim::Ordinary => {}
            CheckpointReviewClaim::Invalid => {
                transaction.commit().map_err(crate::db::database_error(
                    "commit invalid research action routing",
                ))?;
                return Ok(OpenActionTransactionOutcome::Handled(false));
            }
        }

        let terminal_event = DecisionRepository::terminal_decision_event(
            &confirmed_owner.project_id,
            &confirmed_owner.campaign_id,
            &confirmed_owner.source_experiment_id,
            TerminalDecisionEventProjection {
                task_id: task.id,
                managed_task_signature: &confirmed_owner.managed_task_signature,
                group: &task.group,
                state: &task.state,
                enqueued_at: task.enqueued_at.as_deref().and_then(parse_timestamp),
                started_at: task.started_at.as_deref().and_then(parse_timestamp),
                ended_at: task.ended_at.as_deref().and_then(parse_timestamp),
                exit_code: task.result.as_ref().and_then(terminal_exit_code),
            },
            now,
        );
        DecisionRepository::attach_research_terminal_cycle_in_transaction(
            &transaction,
            &confirmed_owner,
            &terminal_event,
            now,
        )?;
        transaction.commit().map_err(crate::db::database_error(
            "commit confirmed research handoff",
        ))?;
        Ok(OpenActionTransactionOutcome::Handled(true))
    })();
    match transaction_result {
        Ok(OpenActionTransactionOutcome::RecoverCheckpoint(review_id)) => {
            if let Some(changed) = recover_checkpoint_successor(
                db,
                policy,
                &project,
                &coordinator,
                &mut admission,
                &review_id,
                now,
            )
            .await?
            {
                Ok(changed)
            } else {
                Ok(false)
            }
        }
        Ok(OpenActionTransactionOutcome::Handled(changed)) => Ok(changed),
        Err(error) => Err(error),
    }
}

fn termination_request_targets_task(raw_signature: &str, task: &PueueTask) -> bool {
    let Some(identity) = raw_signature
        .strip_prefix("pueue-task:v1:")
        .and_then(|value| serde_json::from_str::<Value>(value).ok())
    else {
        return false;
    };
    identity.get("group").and_then(Value::as_str) == Some(task.group.as_str())
        && identity.get("id").and_then(Value::as_i64) == Some(task.id)
        && identity.get("enqueued_at").and_then(Value::as_str) == task.enqueued_at.as_deref()
        && identity.get("started_at").and_then(Value::as_str) == task.started_at.as_deref()
}

fn terminal_exit_code(result: &Value) -> Option<i32> {
    result
        .as_object()
        .and_then(|object| object.get("Failed").or_else(|| object.get("Success")))
        .and_then(Value::as_i64)
        .and_then(|code| i32::try_from(code).ok())
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, sync::Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::{
        db::{CampaignRepository, ProjectRepository, ResearchRepository},
        pueue::{PueueApi, PueueTask},
    };

    struct PreparationRacePueue {
        db: Db,
        task: PueueTask,
        mutation: Mutex<Option<Box<dyn FnOnce(&Db) + Send>>>,
    }

    #[async_trait]
    impl PueueApi for PreparationRacePueue {
        async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
            if let Some(mutation) = self.mutation.lock().unwrap().take() {
                mutation(&self.db);
            }
            Ok(vec![self.task.clone()])
        }

        async fn add(&self, _args: &[OsString]) -> Result<i64, AppError> {
            Err(AppError::Runtime {
                operation: "unexpected preparation race add",
            })
        }

        async fn kill(&self, _task_id: i64) -> Result<(), AppError> {
            Err(AppError::Runtime {
                operation: "unexpected preparation race kill",
            })
        }

        async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
            Err(AppError::Runtime {
                operation: "unexpected preparation race remove",
            })
        }

        async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
            Err(AppError::Runtime {
                operation: "unexpected preparation race group",
            })
        }
    }

    fn fixture_task(db: &Db, action: &crate::db::ReadyResearchAction) -> PueueTask {
        let raw_identity = action
            .raw_task_signature
            .strip_prefix("pueue-task:v1:")
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            .expect("runtime fixture raw task identity");
        let command_json: String = db
            .connect()
            .expect("runtime fixture task observation connection")
            .query_row(
                "SELECT command_json FROM task_observations WHERE pueue_task_id = ?1",
                [action.owner.source_task_id.expect("runtime fixture task")],
                |row| row.get(0),
            )
            .expect("runtime fixture task observation");
        let command = serde_json::from_str::<Vec<String>>(&command_json)
            .expect("runtime fixture task command")
            .into_iter()
            .next()
            .expect("runtime fixture command entry");
        PueueTask {
            id: raw_identity["id"].as_i64().expect("runtime fixture task id"),
            group: raw_identity["group"]
                .as_str()
                .expect("runtime fixture task group")
                .to_owned(),
            command,
            state: raw_identity["state"]
                .as_str()
                .expect("runtime fixture task state")
                .to_owned(),
            enqueued_at: raw_identity["enqueued_at"].as_str().map(str::to_owned),
            started_at: raw_identity["started_at"].as_str().map(str::to_owned),
            ended_at: raw_identity["ended_at"].as_str().map(str::to_owned),
            result: None,
        }
    }

    #[tokio::test]
    async fn preparation_unsupported_refresh_requires_same_reason() {
        let fixture = crate::research_checkpoint::runtime_authority_fixture_bridge(false);
        let project = ProjectRepository::new(&fixture.db)
            .find_by_id(&fixture.action.owner.project_id)
            .unwrap()
            .expect("runtime fixture project");
        let campaign = CampaignRepository::new(&fixture.db)
            .find_by_id(&fixture.action.owner.campaign_id)
            .unwrap()
            .expect("runtime fixture campaign");
        let review = ResearchRepository::new(&fixture.db)
            .find(&fixture.action.owner.review_id)
            .unwrap();
        let task = fixture_task(&fixture.db, &fixture.action);
        let experiment_id = fixture.action.owner.source_experiment_id.clone();
        fixture
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE experiments
                 SET resume_of_experiment_id = ?1
                 WHERE experiment_id = ?1",
                rusqlite::params![experiment_id],
            )
            .unwrap();
        let prior_source_reason = match prepare_checkpoint(
            &fixture.db,
            &fixture.action,
            &fixture.request,
            &fixture.policy,
            &fixture.project_policy,
        ) {
            Err(PrepareCheckpointError::Unsupported { reason }) => reason,
            _ => panic!("expected preparation-origin unsupported result"),
        };
        let code_revision = "a".repeat(64);
        let pueue = PreparationRacePueue {
            db: fixture.db.clone(),
            task: task.clone(),
            mutation: Mutex::new(Some(Box::new(move |db| {
                db.connect()
                    .unwrap()
                    .execute(
                        "UPDATE experiments SET resume_of_experiment_id = ?1
                         WHERE experiment_id = ?1",
                        rusqlite::params![Option::<String>::None],
                    )
                    .unwrap();
                db.connect()
                    .unwrap()
                    .execute(
                        "UPDATE experiments SET code_revision_sha = ?1
                         WHERE experiment_id = 'experiment'",
                        rusqlite::params![code_revision],
                    )
                    .unwrap();
            }))),
        };

        let changed = settle_preparation_unsupported(
            &fixture.db,
            &pueue,
            &fixture.policy,
            &project,
            &campaign,
            &task,
            &review,
            &fixture.action,
            &fixture.request,
            &prior_source_reason,
            3_105,
        )
        .await
        .unwrap();
        assert!(!changed);
        assert_eq!(
            ResearchRepository::new(&fixture.db)
                .find(&review.review_id)
                .unwrap()
                .state,
            "ready"
        );

        let unchanged_reason = match prepare_checkpoint(
            &fixture.db,
            &fixture.action,
            &fixture.request,
            &fixture.policy,
            &fixture.project_policy,
        ) {
            Err(PrepareCheckpointError::Unsupported { reason }) => reason,
            _ => panic!("expected unchanged preparation-origin unsupported result"),
        };
        let unchanged_pueue = PreparationRacePueue {
            db: fixture.db.clone(),
            task: task.clone(),
            mutation: Mutex::new(None),
        };
        let changed = settle_preparation_unsupported(
            &fixture.db,
            &unchanged_pueue,
            &fixture.policy,
            &project,
            &campaign,
            &task,
            &review,
            &fixture.action,
            &fixture.request,
            &unchanged_reason,
            3_106,
        )
        .await
        .unwrap();
        assert!(changed);
        assert_eq!(
            ResearchRepository::new(&fixture.db)
                .find(&review.review_id)
                .unwrap()
                .state,
            "completed"
        );
    }

    fn complete_runtime_checkpoint_successor(
        fixture: &crate::research_checkpoint::RuntimeCheckpointDispatchFixtureBridge,
        task_id: i64,
        task_signature: &str,
    ) {
        let successor_id = fixture.intent.experiment.experiment_id.clone();
        let connection = fixture.db.connect().unwrap();
        connection
            .execute(
                "UPDATE experiments
                 SET status = 'succeeded', pueue_task_id = ?1,
                     task_signature = ?2, finished_at = ?3
                 WHERE experiment_id = ?4",
                rusqlite::params![task_id, task_signature, 3_200_i64, successor_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE submissions
                 SET status = 'accepted', pueue_task_id = ?1, task_signature = ?2
                 WHERE submission_id = ?3",
                rusqlite::params![task_id, task_signature, fixture.intent.submission.submission_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE budget_reservations SET status = 'consumed'
                 WHERE experiment_id = ?1 AND dimension = 'experiment'",
                [&successor_id],
            )
            .unwrap();
    }

    fn cleanup_runtime_pueue(
        fixture: &crate::research_checkpoint::RuntimeCheckpointDispatchFixtureBridge,
    ) -> PreparationRacePueue {
        PreparationRacePueue {
            db: fixture.db.clone(),
            task: PueueTask {
                id: 41,
                group: fixture.project.pueue_group.clone(),
                command: "fixture".to_owned(),
                state: "Running".to_owned(),
                enqueued_at: Some("900".to_owned()),
                started_at: Some("1000".to_owned()),
                ended_at: None,
                result: None,
            },
            mutation: Mutex::new(None),
        }
    }

    #[test]
    fn cleanup_controller_releases_terminal_checkpoint_with_real_filesystem_authority() {
        let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
        let successor_id = fixture.intent.experiment.experiment_id.clone();
        let successor_signature = "pueue-managed-run:v1:successor";
        complete_runtime_checkpoint_successor(&fixture, 77, successor_signature);
        let retained_path = fixture
            .policy
            .code_change_state_root_path()
            .join(&fixture.checkpoint.retained_checkpoint.relative_path);
        assert!(retained_path.exists());

        let pueue = cleanup_runtime_pueue(&fixture);
        assert_eq!(
            cleanup_research_checkpoints(
                &fixture.db,
                &pueue,
                &fixture.policy,
                3_300,
                1,
            )
            .unwrap(),
            1
        );
        assert!(!retained_path.exists());
        let review = ResearchRepository::new(&fixture.db)
            .find(&fixture.checkpoint.review_id)
            .unwrap();
        assert_eq!(review.state, "completed");
        assert_eq!(review.operation_stage, None);
        assert_eq!(review.successor_experiment_id, Some(successor_id));
        assert!(review.checkpoint_json.is_some());
    }

    #[test]
    fn cleanup_controller_releases_pre_add_and_undispatched_real_shapes() {
        let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
        let successor_id = fixture.intent.experiment.experiment_id.clone();
        let authority = match ResearchRepository::new(&fixture.db)
            .checkpoint_dispatch_authority(&fixture.project.project_id, &successor_id, 3_200)
            .unwrap()
        {
            crate::db::CheckpointDispatchSelection::Ready(authority) => authority,
            other => panic!("expected checkpoint dispatch authority, got {other:?}"),
        };
        ExperimentRepository::new(&fixture.db)
            .fail_checkpoint_before_add(
                &authority,
                "research_checkpoint_verification_failed",
                3_205,
            )
            .unwrap();
        let retained_path = fixture
            .policy
            .code_change_state_root_path()
            .join(&fixture.checkpoint.retained_checkpoint.relative_path);
        assert!(retained_path.exists());
        let pueue = cleanup_runtime_pueue(&fixture);
        assert_eq!(
            cleanup_research_checkpoints(&fixture.db, &pueue, &fixture.policy, 3_300, 1)
                .unwrap(),
            1
        );
        assert!(!retained_path.exists());
        let review = ResearchRepository::new(&fixture.db)
            .find(&fixture.checkpoint.review_id)
            .unwrap();
        assert_eq!(review.state, "completed");
        assert_eq!(review.operation_stage, None);

        let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
        let successor_id = fixture.intent.experiment.experiment_id.clone();
        let connection = fixture.db.connect().unwrap();
        connection
            .execute(
                "UPDATE research_reviews
                 SET state = 'ready', operation_stage = 'intent',
                     successor_experiment_id = NULL, failure_code = NULL,
                     finished_at = NULL, not_before = 3_200, updated_at = 3_200
                 WHERE review_id = ?1",
                [&fixture.checkpoint.review_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE termination_requests
                 SET status = 'confirmed', grace_until = NULL,
                     confirmed_at = 3_101,
                     last_error = 'termination_undispatched: source task absent'
                 WHERE request_id = (
                     SELECT termination_request_id FROM research_reviews
                     WHERE review_id = ?1
                 )",
                [&fixture.checkpoint.review_id],
            )
            .unwrap();
        for statement in [
            "DELETE FROM budget_reservations
             WHERE experiment_id = ?1 AND dimension = 'experiment'",
            "DELETE FROM experiments WHERE experiment_id = ?1",
            "DELETE FROM submissions WHERE submission_id = ?1",
            "DELETE FROM proposals WHERE proposal_id = ?1",
        ] {
            let id = match statement {
                s if s.contains("budget_reservations") || s.contains("experiments") => {
                    successor_id.as_str()
                }
                s if s.contains("submissions") => fixture.intent.submission.submission_id.as_str(),
                _ => fixture.intent.proposal.proposal_id.as_str(),
            };
            connection.execute(statement, [id]).unwrap();
        }
        drop(connection);
        let retained_path = fixture
            .policy
            .code_change_state_root_path()
            .join(&fixture.checkpoint.retained_checkpoint.relative_path);
        assert!(retained_path.exists());
        let pueue = cleanup_runtime_pueue(&fixture);
        assert_eq!(
            cleanup_research_checkpoints(&fixture.db, &pueue, &fixture.policy, 3_300, 1)
                .unwrap(),
            1
        );
        assert!(!retained_path.exists());
        let review = ResearchRepository::new(&fixture.db)
            .find(&fixture.checkpoint.review_id)
            .unwrap();
        assert_eq!(review.state, "discarded");
        assert_eq!(review.operation_stage, None);
        let failure_code: Option<String> = fixture
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT failure_code FROM research_reviews WHERE review_id = ?1",
                [&fixture.checkpoint.review_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            failure_code.as_deref(),
            Some("research_natural_finish_before_dispatch")
        );
    }

    #[test]
    fn cleanup_controller_defers_external_reader_then_cleans_after_release() {
        let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
        complete_runtime_checkpoint_successor(
            &fixture,
            93,
            "pueue-managed-run:v1:cleanup-external-reader",
        );
        let retained = crate::environment::reopen_retained_research_file(
            &fixture.policy,
            &fixture.checkpoint.campaign_id,
            &fixture.checkpoint.review_id,
            &fixture.checkpoint.retained_checkpoint,
        )
        .unwrap();
        let retained_path = fixture
            .policy
            .code_change_state_root_path()
            .join(&fixture.checkpoint.retained_checkpoint.relative_path);
        let pueue = cleanup_runtime_pueue(&fixture);
        assert_eq!(
            cleanup_research_checkpoints(&fixture.db, &pueue, &fixture.policy, 3_300, 1)
                .unwrap(),
            0
        );
        assert!(retained_path.exists());
        assert_eq!(
            ResearchRepository::new(&fixture.db)
                .find(&fixture.checkpoint.review_id)
                .unwrap()
                .state,
            "ready"
        );
        drop(retained);
        assert_eq!(
            cleanup_research_checkpoints(&fixture.db, &pueue, &fixture.policy, 3_301, 1)
                .unwrap(),
            1
        );
        assert!(!retained_path.exists());
    }

    #[test]
    fn cleanup_controller_keeps_historical_authority_after_source_removal_and_project_pause() {
        for (enabled, paused) in [(1_i64, 1_i64), (0, 1)] {
            let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
            complete_runtime_checkpoint_successor(
                &fixture,
                94,
                "pueue-managed-run:v1:cleanup-historical-source",
            );
            let source_root = std::path::Path::new(&fixture.checkpoint.source_root_canonical_path);
            std::fs::remove_file(source_root.join(&fixture.checkpoint.loader.root_relative_path))
                .unwrap();
            std::fs::remove_file(
                source_root.join(&fixture.checkpoint.source_checkpoint.root_relative_path),
            )
            .unwrap();
            fixture
                .db
                .connect()
                .unwrap()
                .execute(
                    "UPDATE projects SET enabled = ?1, paused = ?2 WHERE project_id = ?3",
                    rusqlite::params![enabled, paused, fixture.project.project_id],
                )
                .unwrap();
            let retained_path = fixture
                .policy
                .code_change_state_root_path()
                .join(&fixture.checkpoint.retained_checkpoint.relative_path);
            let pueue = cleanup_runtime_pueue(&fixture);
            assert_eq!(
                cleanup_research_checkpoints(&fixture.db, &pueue, &fixture.policy, 3_300, 1)
                    .unwrap(),
                1,
                "enabled={enabled} paused={paused}"
            );
            assert!(!retained_path.exists());
        }
    }

    #[test]
    fn cleanup_controller_refuses_live_and_ambiguous_successor_ownership() {
        let cases = ["reserved", "submitting", "unreconciled", "accepted", "orphan"];
        for (index, status) in cases.into_iter().enumerate() {
            let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
            let successor_id = fixture.intent.experiment.experiment_id.clone();
            let connection = fixture.db.connect().unwrap();
            match status {
                "accepted" => {
                    connection
                        .execute(
                            "UPDATE experiments
                             SET status = 'accepted', pueue_task_id = 95,
                                 task_signature = 'pueue-managed-run:v1:live'
                             WHERE experiment_id = ?1",
                            [&successor_id],
                        )
                        .unwrap();
                    connection
                        .execute(
                            "UPDATE submissions
                             SET status = 'accepted', pueue_task_id = 95,
                                 task_signature = 'pueue-managed-run:v1:live'
                             WHERE submission_id = ?1",
                            [&fixture.intent.submission.submission_id],
                        )
                        .unwrap();
                }
                "orphan" => {
                    connection
                        .execute(
                            "UPDATE research_reviews SET agent_run_id = NULL
                             WHERE review_id = ?1",
                            [&fixture.checkpoint.review_id],
                        )
                        .unwrap();
                }
                _ => {
                    connection
                        .execute(
                            "UPDATE experiments SET status = ?1 WHERE experiment_id = ?2",
                            rusqlite::params![status, successor_id],
                        )
                        .unwrap();
                }
            }
            drop(connection);
            let retained_path = fixture
                .policy
                .code_change_state_root_path()
                .join(&fixture.checkpoint.retained_checkpoint.relative_path);
            let pueue = cleanup_runtime_pueue(&fixture);
            assert_eq!(
                cleanup_research_checkpoints(&fixture.db, &pueue, &fixture.policy, 3_300, 1)
                    .unwrap(),
                0,
                "unexpected cleanup for {status} case {index}"
            );
            assert!(retained_path.exists(), "{status}: retained file removed");
        }

        let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
        let connection = fixture.db.connect().unwrap();
        connection
            .execute(
                "INSERT INTO events (
                    event_id, project_id, kind, dedup_key, payload_json,
                    status, attempts, not_before, lease_until, created_at,
                    completed_at, last_error
                 ) VALUES (9_995, ?1, 'operator_wake', ?2, '{}', 'completed',
                           0, 3_100, NULL, 3_100, 3_101, NULL)",
                rusqlite::params![
                    fixture.project.project_id,
                    format!("cleanup-ambiguous-{}", fixture.checkpoint.review_id),
                ],
            )
            .unwrap();
        let owner_run_id: i64 = connection
            .query_row(
                "SELECT agent_run_id FROM research_reviews WHERE review_id = ?1",
                [&fixture.checkpoint.review_id],
                |row| row.get(0),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO agent_run_events (project_id, run_id, event_id)
                 VALUES (?1, ?2, 9_995)",
                rusqlite::params![fixture.project.project_id, owner_run_id],
            )
            .unwrap();
        drop(connection);
        let retained_path = fixture
            .policy
            .code_change_state_root_path()
            .join(&fixture.checkpoint.retained_checkpoint.relative_path);
        let pueue = cleanup_runtime_pueue(&fixture);
        assert_eq!(
            cleanup_research_checkpoints(&fixture.db, &pueue, &fixture.policy, 3_300, 1)
                .unwrap(),
            0,
            "ambiguous owner was cleaned"
        );
        assert!(retained_path.exists(), "ambiguous owner removed retained file");
    }

    #[test]
    fn cleanup_controller_uses_one_inspection_budget_across_projects() {
        let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
        complete_runtime_checkpoint_successor(
            &fixture,
            96,
            "pueue-managed-run:v1:cleanup-global-budget",
        );
        let second_root = std::path::PathBuf::from(format!(
            "{}/",
            fixture.project.root_path.display()
        ));
        fixture
            .db
            .connect()
            .unwrap()
            .execute(
                "INSERT INTO projects (
                    project_id, root_path, pueue_group, config_path,
                    enabled, paused, halted_reason, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, 1, 0, NULL, 3_100, 3_100)",
                rusqlite::params![
                    "project-2",
                    second_root.to_string_lossy().as_ref(),
                    "research-second",
                    fixture.project.config_path.to_string_lossy().as_ref(),
                ],
            )
            .unwrap();
        let objective = crate::state::ObjectiveSnapshot {
            text: "second cleanup project".to_owned(),
            digest: "b".repeat(64),
        };
        let argv = vec!["python".to_owned(), "train.py".to_owned()];
        let baseline = crate::proposals::validate_initial_baseline(
            crate::proposals::ProposalInput {
                kind: crate::models::ProposalKind::Experiment,
                hypothesis: "second cleanup baseline".to_owned(),
                source_experiment_id: None,
                argv: argv.clone(),
                working_directory: ".".to_owned(),
                expected_evidence: vec!["loss".to_owned()],
            },
            &objective.digest,
        )
        .unwrap();
        CampaignRepository::new(&fixture.db)
            .start_with_baseline(
                crate::db::StartCampaignRequest {
                    campaign_id: "campaign-2",
                    project_id: "project-2",
                    objective: &objective,
                    initial_argv: &argv,
                    baseline: &baseline,
                    submission_id: "submission-2",
                    experiment_id: "experiment-2",
                    proposal_id: "proposal-2",
                    metadata: &serde_json::json!({}),
                    origin_agent_run_id: None,
                    objective_metric: None,
                    now: 3_100,
                },
                &crate::execution_policy::CampaignLimits::default(),
            )
            .unwrap();
        ExperimentRepository::new(&fixture.db)
            .mark_submitting("experiment-2", 3_101)
            .unwrap();
        let runtime_argv = crate::environment::campaign_experiment_runtime_argv(
            &second_root,
            "campaign-2",
            "experiment-2",
            &argv,
        );
        let runtime_command = crate::reconcile::try_canonical_command_display_os(&runtime_argv)
            .unwrap();
        let task = PueueTask {
            id: 42,
            group: "research-second".to_owned(),
            command: runtime_command.clone(),
            state: "Running".to_owned(),
            enqueued_at: Some("3100".to_owned()),
            started_at: Some("3101".to_owned()),
            ended_at: None,
            result: None,
        };
        let raw_signature = crate::reconcile::task_signature(&task);
        let managed_signature = crate::reconcile::managed_task_run_signature(&task).unwrap();
        ExperimentRepository::new(&fixture.db)
            .mark_accepted("experiment-2", task.id, &managed_signature, 3_102)
            .unwrap();
        crate::db::TaskObservationRepository::new(&fixture.db)
            .upsert(&crate::models::NewTaskObservation::new(
                "project-2",
                &raw_signature,
                task.id,
                &task.group,
                vec![runtime_command],
                "Running",
                Some(3_100),
                Some(3_101),
                None,
                None,
                3_103,
            ))
            .unwrap();
        let research = ResearchRepository::new(&fixture.db);
        research.ensure_campaign("campaign-2").unwrap();
        research
            .schedule_running("campaign-2", 3_101, 1, 3_104)
            .unwrap();
        let second_review = research
            .claim_due("campaign-2", "experiment-2", &managed_signature, 3_200)
            .unwrap()
            .expect("second project cleanup review");
        fixture
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE research_reviews
                 SET state = 'ready', operation_stage = 'successor_reserved',
                     successor_experiment_id = 'experiment-2', checkpoint_json = '{}',
                     updated_at = 4_000
                 WHERE review_id = ?1",
                [&second_review.review_id],
            )
            .unwrap();
        let held = crate::environment::reopen_retained_research_file(
            &fixture.policy,
            &fixture.checkpoint.campaign_id,
            &fixture.checkpoint.review_id,
            &fixture.checkpoint.retained_checkpoint,
        )
        .unwrap();
        let retained_path = fixture
            .policy
            .code_change_state_root_path()
            .join(&fixture.checkpoint.retained_checkpoint.relative_path);
        let pueue = cleanup_runtime_pueue(&fixture);
        assert_eq!(
            cleanup_research_checkpoints(&fixture.db, &pueue, &fixture.policy, 4_100, 1)
                .unwrap(),
            0
        );
        let second_state: (String, Option<String>) = fixture
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT state, operation_stage FROM research_reviews WHERE review_id = ?1",
                [&second_review.review_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            second_state,
            ("ready".to_owned(), Some("successor_reserved".to_owned()))
        );
        assert!(retained_path.exists());
        drop(held);
        assert_eq!(
            cleanup_research_checkpoints(&fixture.db, &pueue, &fixture.policy, 4_101, 1)
                .unwrap(),
            1
        );
        assert!(!retained_path.exists());
        let second_state_after: (String, Option<String>) = fixture
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT state, operation_stage FROM research_reviews WHERE review_id = ?1",
                [&second_review.review_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            second_state_after,
            ("ready".to_owned(), Some("successor_reserved".to_owned()))
        );
    }

    #[test]
    fn cleanup_controller_rejects_unbounded_review_and_request_text() {
        let cases = [
            (
                "context_json",
                "research_reviews",
                crate::research_evidence::MAX_RESEARCH_CONTEXT_BYTES + 1,
            ),
            (
                "response_json",
                "research_reviews",
                crate::research_protocol::MAX_RESEARCH_ANSWER_BYTES + 1,
            ),
            (
                "notes_json",
                "research_reviews",
                crate::research_evidence::MAX_RESEARCH_CONTEXT_BYTES * 128,
            ),
            (
                "failure_code",
                "research_reviews",
                129,
            ),
            (
                "reason",
                "termination_requests",
                crate::process::MAX_FIELD_SIZE + 1,
            ),
            (
                "last_error",
                "termination_requests",
                crate::process::MAX_FIELD_SIZE + 1,
            ),
        ];
        for (field, table, byte_len) in cases {
            let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
            let successor_id = fixture.intent.experiment.experiment_id.clone();
            let successor_signature = "pueue-managed-run:v1:cleanup-guard";
            let connection = fixture.db.connect().unwrap();
            connection
                .execute(
                    "UPDATE experiments
                     SET status = 'succeeded', pueue_task_id = ?1,
                         task_signature = ?2, finished_at = ?3
                     WHERE experiment_id = ?4",
                    rusqlite::params![88_i64, successor_signature, 3_200_i64, successor_id],
                )
                .unwrap();
            connection
                .execute(
                    "UPDATE submissions
                     SET status = 'accepted', pueue_task_id = ?1, task_signature = ?2
                     WHERE submission_id = ?3",
                    rusqlite::params![88_i64, successor_signature, fixture.intent.submission.submission_id],
                )
                .unwrap();
            connection
                .execute(
                    "UPDATE budget_reservations SET status = 'consumed'
                     WHERE experiment_id = ?1 AND dimension = 'experiment'",
                    [&successor_id],
                )
                .unwrap();
            let oversized_text = "x".repeat(byte_len);
            if table == "research_reviews" {
                connection
                    .execute(
                        &format!(
                            "UPDATE research_reviews SET {field} = ?1
                             WHERE review_id = ?2"
                        ),
                        rusqlite::params![oversized_text, fixture.checkpoint.review_id],
                    )
                    .unwrap();
            } else {
                connection
                    .execute(
                        &format!(
                            "UPDATE termination_requests SET {field} = ?1
                             WHERE request_id = (
                                 SELECT termination_request_id FROM research_reviews
                                 WHERE review_id = ?2
                             )"
                        ),
                        rusqlite::params![oversized_text, fixture.checkpoint.review_id],
                    )
                    .unwrap();
            }
            drop(connection);
            let retained_path = fixture
                .policy
                .code_change_state_root_path()
                .join(&fixture.checkpoint.retained_checkpoint.relative_path);
            assert!(retained_path.exists(), "{field}: fixture retained file");
            assert!(
                ResearchRepository::new(&fixture.db)
                    .checkpoint_cleanup_authority(
                        &fixture.project.project_id,
                        &fixture.checkpoint.review_id,
                        3_300,
                    )
                    .unwrap()
                    .is_none(),
                "{field}: malformed cleanup witness was accepted"
            );
            let persisted: (String, Option<String>) = fixture
                .db
                .connect()
                .unwrap()
                .query_row(
                    "SELECT state, operation_stage FROM research_reviews WHERE review_id = ?1",
                    [&fixture.checkpoint.review_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(persisted.0, "ready", "{field}: review mutated");
            assert_eq!(persisted.1.as_deref(), Some("successor_reserved"), "{field}: stage mutated");
            assert!(retained_path.exists(), "{field}: malformed witness removed file");
        }

        for (field, table) in [
            ("context_json", "research_reviews"),
            ("response_json", "research_reviews"),
            ("notes_json", "research_reviews"),
            ("failure_code", "research_reviews"),
            ("reason", "termination_requests"),
            ("last_error", "termination_requests"),
        ] {
            let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
            complete_runtime_checkpoint_successor(
                &fixture,
                89,
                "pueue-managed-run:v1:cleanup-guard-blob",
            );
            let connection = fixture.db.connect().unwrap();
            if table == "research_reviews" {
                connection
                    .execute(
                        &format!(
                            "UPDATE research_reviews SET {field} = zeroblob(8)
                             WHERE review_id = ?1"
                        ),
                        [&fixture.checkpoint.review_id],
                    )
                    .unwrap();
            } else {
                connection
                    .execute(
                        &format!(
                            "UPDATE termination_requests SET {field} = zeroblob(8)
                             WHERE request_id = (
                                 SELECT termination_request_id FROM research_reviews
                                 WHERE review_id = ?1
                             )"
                        ),
                        [&fixture.checkpoint.review_id],
                    )
                    .unwrap();
            }
            drop(connection);
            let retained_path = fixture
                .policy
                .code_change_state_root_path()
                .join(&fixture.checkpoint.retained_checkpoint.relative_path);
            assert!(retained_path.exists(), "{field}: fixture retained file");
            assert!(ResearchRepository::new(&fixture.db)
                .checkpoint_cleanup_authority(
                    &fixture.project.project_id,
                    &fixture.checkpoint.review_id,
                    3_300,
                )
                .unwrap()
                .is_none(),
                "{field}: wrong storage class accepted"
            );
            assert!(retained_path.exists(), "{field}: wrong type removed file");
        }

        let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
        let successor_id = fixture.intent.experiment.experiment_id.clone();
        let successor_signature = "pueue-managed-run:v1:cleanup-guard-cycle";
        let connection = fixture.db.connect().unwrap();
        connection
            .execute(
                "UPDATE experiments
                 SET status = 'succeeded', pueue_task_id = ?1,
                     task_signature = ?2, finished_at = ?3
                 WHERE experiment_id = ?4",
                rusqlite::params![90_i64, successor_signature, 3_200_i64, successor_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE submissions
                 SET status = 'accepted', pueue_task_id = ?1, task_signature = ?2
                 WHERE submission_id = ?3",
                rusqlite::params![90_i64, successor_signature, fixture.intent.submission.submission_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE budget_reservations SET status = 'consumed'
                 WHERE experiment_id = ?1 AND dimension = 'experiment'",
                [&successor_id],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO decision_cycles (
                    cycle_id, campaign_id, source_experiment_id, state,
                    next_wake_at, consecutive_failed_attempts, last_decision_kind,
                    last_failure_code, last_failure_summary, created_at, updated_at,
                    source_terminal_at
                 ) VALUES (?1, ?2, ?3, 'pending', NULL, 0, NULL, NULL, NULL,
                           3_100, 3_100, 1)",
                rusqlite::params![
                    "cleanup-cycle",
                    fixture.checkpoint.campaign_id,
                    fixture.checkpoint.source_experiment_id,
                ],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE research_reviews SET decision_cycle_id = 'cleanup-cycle'
                 WHERE review_id = ?1",
                [&fixture.checkpoint.review_id],
            )
            .unwrap();
        drop(connection);
        let retained_path = fixture
            .policy
            .code_change_state_root_path()
            .join(&fixture.checkpoint.retained_checkpoint.relative_path);
        assert!(retained_path.exists());
        assert!(ResearchRepository::new(&fixture.db)
            .checkpoint_cleanup_authority(
                &fixture.project.project_id,
                &fixture.checkpoint.review_id,
                3_300,
            )
            .unwrap()
            .is_none());
        assert!(retained_path.exists());
    }

    #[test]
    fn cleanup_controller_ignores_unrelated_owner_large_notes() {
        let fixture = crate::research_checkpoint::runtime_checkpoint_dispatch_fixture_bridge();
        let successor_id = fixture.intent.experiment.experiment_id.clone();
        let successor_signature = "pueue-managed-run:v1:cleanup-unrelated-owner";
        let connection = fixture.db.connect().unwrap();
        connection
            .execute(
                "UPDATE experiments
                 SET status = 'succeeded', pueue_task_id = ?1,
                     task_signature = ?2, finished_at = ?3
                 WHERE experiment_id = ?4",
                rusqlite::params![91_i64, successor_signature, 3_200_i64, successor_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE submissions
                 SET status = 'accepted', pueue_task_id = ?1, task_signature = ?2
                 WHERE submission_id = ?3",
                rusqlite::params![91_i64, successor_signature, fixture.intent.submission.submission_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE budget_reservations SET status = 'consumed'
                 WHERE experiment_id = ?1 AND dimension = 'experiment'",
                [&successor_id],
            )
            .unwrap();
        let unrelated_notes = "u".repeat(crate::research_evidence::MAX_RESEARCH_CONTEXT_BYTES * 128);
        connection
            .execute(
                "INSERT INTO events (
                    event_id, project_id, kind, dedup_key, payload_json,
                    status, attempts, not_before, lease_until, created_at,
                    completed_at, last_error
                 ) VALUES (?1, ?2, 'operator_wake', ?3, '{}', 'completed',
                           0, 3_100, NULL, 3_100, 3_101, NULL)",
                rusqlite::params![
                    9_991_i64,
                    fixture.project.project_id,
                    format!("cleanup-unrelated-event-{}", fixture.checkpoint.review_id),
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO agent_runs (
                    run_id, project_id, primary_event_id, pid, status, started_at,
                    finished_at, exit_code, log_path, last_error, launch_gate_state,
                    context_mode, context_session_id, context_lineage_json
                 ) VALUES (?1, ?2, ?3, NULL, 'completed', 3_100, 3_101, 0,
                           '/tmp/unrelated-owner.log', NULL, 'failed', 'fresh', NULL, '[]')",
                rusqlite::params![9_991_i64, fixture.project.project_id, 9_991_i64],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO agent_run_events (project_id, run_id, event_id)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![fixture.project.project_id, 9_991_i64, 9_991_i64],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO research_reviews (
                    review_id, campaign_id, experiment_id, task_signature, attempt,
                    state, operation_stage, agent_run_id, context_json, context_digest,
                    response_json, termination_request_id, successor_experiment_id,
                    evidence_schema_version, session_generation, event_id, not_before,
                    notes_json, failure_code, decision_cycle_id, checkpoint_json,
                    created_at, started_at, finished_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, 1, 'completed', NULL, ?5, '{}', NULL,
                           NULL, NULL, NULL, NULL, 0, ?6, 3_100, ?7, NULL, NULL,
                           NULL, 3_100, 3_100, 3_101, 3_101)",
                rusqlite::params![
                    format!("cleanup-unrelated-review-{}", fixture.checkpoint.review_id),
                    fixture.checkpoint.campaign_id,
                    fixture.checkpoint.source_experiment_id,
                    fixture.checkpoint.source_managed_task_signature,
                    9_991_i64,
                    9_991_i64,
                    unrelated_notes,
                ],
            )
            .unwrap();
        drop(connection);

        let retained_path = fixture
            .policy
            .code_change_state_root_path()
            .join(&fixture.checkpoint.retained_checkpoint.relative_path);
        assert!(retained_path.exists());
        let repository = ResearchRepository::new(&fixture.db);
        let authority = repository
            .checkpoint_cleanup_authority(
                &fixture.project.project_id,
                &fixture.checkpoint.review_id,
                3_300,
            )
            .unwrap()
            .expect("target cleanup authority despite unrelated owner");
        assert!(repository
            .settle_checkpoint_cleanup(&authority, 3_301)
            .unwrap());
        assert!(retained_path.exists());
    }
}
