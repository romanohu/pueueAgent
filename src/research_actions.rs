use rusqlite::{OptionalExtension, TransactionBehavior};
use serde_json::{json, Value};

use crate::{
    campaign::CampaignCoordinator,
    db::{
        bind_research_termination_intent_in_transaction, complete_research_continue_in_transaction,
        discard_missing_undispatched_research_action_in_transaction,
        discard_undispatched_research_action_in_transaction, ready_research_action_in_transaction,
    },
    db::{
        count_live_reservations, discard_ready_research_action_in_transaction, next_research_due,
        research_ownership_in_transaction, CampaignRepository, Db, DecisionRepository,
        ExperimentRepository, IncidentRepository, ProjectRepository, ResearchOwnership,
        ResearchRepository, TerminalDecisionEventProjection, TerminationRequestRepository,
    },
    execution_policy::ResolvedExecutionPolicy,
    models::{BudgetDimension, NewIncident, NewTerminationRequest},
    output::bounded_redacted_text,
    pueue::{PueueApi, PueueTask},
    reconcile::{managed_task_run_signature, parse_timestamp, task_incident_key},
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
            );
        let _admission = match coordinator.acquire_admission(&project) {
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

        let mut connection = db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(crate::db::database_error("begin research action admission"))?;
        let Some(action) = ready_research_action_in_transaction(
            &transaction,
            &campaign.project_id,
            &review.review_id,
            task,
        )?
        else {
            transaction
                .commit()
                .map_err(crate::db::database_error("commit skipped research action"))?;
            repository.rotate_ready_action_review(&review.review_id, now)?;
            continue;
        };

        let changed = match action.answer.action.as_str() {
            "continue" => {
                let mut notes = match serde_json::from_str::<Value>(&action.notes_json) {
                    Ok(value) if value.is_object() => value,
                    _ => {
                        transaction
                            .commit()
                            .map_err(crate::db::database_error("commit invalid research notes"))?;
                        repository.rotate_ready_action_review(&review.review_id, now)?;
                        continue;
                    }
                };
                notes["saved_advice"] = json!(bounded_redacted_text(&action.answer.notes));
                let notes_json =
                    serde_json::to_string(&notes).map_err(|source| AppError::Serialization {
                        operation: "serialize continuing research notes",
                        source,
                    })?;
                let next_due =
                    next_research_due(now, policy.campaign_limits.research_interval_minutes)?;
                complete_research_continue_in_transaction(
                    &transaction,
                    &action,
                    &notes_json,
                    next_due,
                    now,
                )?
            }
            "stop_and_next" => {
                if !replacement_admission_available(
                    &transaction,
                    &action.owner.campaign_id,
                    &action.owner.source_experiment_id,
                    policy.campaign_limits,
                    now,
                )? {
                    transaction
                        .commit()
                        .map_err(crate::db::database_error("commit deferred research action"))?;
                    repository.rotate_ready_action_review(&review.review_id, now)?;
                    continue;
                }
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
                let incident =
                    IncidentRepository::upsert_active_in_transaction(&transaction, &incident)?;
                let request = NewTerminationRequest::new(
                    incident.incident.incident_id,
                    campaign.project_id.clone(),
                    action.raw_task_signature.clone(),
                    reason,
                    now,
                    None,
                );
                let request = TerminationRequestRepository::insert_idempotent_in_transaction(
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
                    repository.rotate_ready_action_review(&review.review_id, now)?;
                    continue;
                }
                true
            }
            "resume_from_checkpoint" => false,
            _ => false,
        };
        transaction
            .commit()
            .map_err(crate::db::database_error("commit research action"))?;
        if changed {
            advanced += 1;
        } else {
            repository.rotate_ready_action_review(&review.review_id, now)?;
        }
    }
    Ok(advanced)
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
    let Some(task_id) = experiment.pueue_task_id else {
        return Ok(false);
    };
    let coordinator = CampaignCoordinator::new(db, pueue, policy.campaign_limits).with_root_anchor(
        policy
            .project_root_anchor(&project.root_path)
            .map_err(AppError::from)?,
    );
    let _admission = match coordinator.acquire_admission(&project) {
        Ok(admission) => admission,
        Err(AppError::Runtime {
            operation: "acquire project submission admission lock",
        }) => return Ok(false),
        Err(error) => return Err(error),
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
        return Ok(false);
    };
    if owner.recovery_required || owner.operation_stage.as_deref() == Some("successor_reserved") {
        transaction.commit().map_err(crate::db::database_error(
            "commit invalid open research action",
        ))?;
        return Ok(false);
    }
    let Some(request_id) = owner.termination_request_id else {
        transaction.commit().map_err(crate::db::database_error(
            "commit unbound open research action",
        ))?;
        return Ok(false);
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
        return Ok(false);
    };
    if request_project != owner.project_id
        || !termination_request_targets_task(&raw_task_signature, task)
    {
        transaction.commit().map_err(crate::db::database_error(
            "commit mismatched open research termination",
        ))?;
        return Ok(false);
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
        return Ok(false);
    };
    if source_task_id != Some(task.id)
        || source_managed_signature.as_deref() != Some(owner.managed_task_signature.as_str())
    {
        transaction.commit().map_err(crate::db::database_error(
            "commit mismatched open research source",
        ))?;
        return Ok(false);
    }

    let stage = owner.operation_stage.as_deref();
    if stage == Some("intent") {
        let dispatched = request_status == "sent"
            || (request_status == "confirmed" && (grace_until.is_some() || last_error.is_none()));
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
                return Ok(false);
            }
            owner.operation_stage = Some("stop_requested".to_owned());
        } else if task.is_terminal()
            && (request_status == "requested"
                || (request_status == "confirmed"
                    && grace_until.is_none()
                    && last_error
                        .as_deref()
                        .is_some_and(|error| error.starts_with(UNDISPATCHED_CONFIRMATION_PREFIX))))
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
            return Ok(discarded);
        } else {
            transaction
                .commit()
                .map_err(crate::db::database_error("commit deferred research intent"))?;
            return Ok(false);
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
        return Ok(false);
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
            return Ok(false);
        }
        confirmed_owner.operation_stage = Some("stop_confirmed".to_owned());
    }
    if confirmed_owner.operation_stage.as_deref() != Some("stop_confirmed") {
        transaction.commit().map_err(crate::db::database_error(
            "commit invalid confirmed stop stage",
        ))?;
        return Ok(false);
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
    Ok(true)
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

fn replacement_admission_available(
    transaction: &rusqlite::Transaction<'_>,
    campaign_id: &str,
    source_experiment_id: &str,
    limits: crate::execution_policy::CampaignLimits,
    now: i64,
) -> Result<bool, AppError> {
    let parallel_count: i64 = transaction
        .query_row(
            "SELECT COUNT(*) FROM experiments
             WHERE campaign_id = ?1 AND experiment_id <> ?2
               AND status IN ('reserved','submitting','accepted','unreconciled')",
            rusqlite::params![campaign_id, source_experiment_id],
            |row| row.get(0),
        )
        .map_err(crate::db::database_error(
            "count replacement experiment capacity",
        ))?;
    if parallel_count >= i64::from(limits.max_parallel_experiments) {
        return Ok(false);
    }
    let experiment_budget =
        count_live_reservations(transaction, campaign_id, BudgetDimension::Experiment, now)?;
    if experiment_budget >= i64::from(limits.max_new_experiments_per_24h) {
        return Ok(false);
    }
    let agent_budget =
        count_live_reservations(transaction, campaign_id, BudgetDimension::AgentRun, now)?;
    Ok(agent_budget < i64::from(limits.max_agent_runs_per_hour))
}
