//! Dedicated scheduler and restart coordinator for campaign research.
//!
//! CampaignResearch events have a durable review owner and are deliberately
//! kept out of the generic prompt scheduler.  This module is the only path
//! that claims those events, reserves their bounded campaign budget, and
//! hands the exact review/evidence identity to the native research role.

use std::collections::BTreeSet;

use crate::{
    agent::{AgentHandle, AgentRunner, AgentSpawnError, AgentSpawnStage, BoundCleanupHandle},
    config,
    db::{
        AgentDecisionReservation, AgentRunRepository, CampaignRepository, Db, EventRepository,
        ProjectRepository, ResearchRepository,
    },
    execution_policy::{preflight_decision_runtime, CampaignLimits, PolicyViolationCode},
    models::{CampaignState, EventStatus},
    research_evidence::build_research_evidence_with_policy,
    retry::{retry_backoff_seconds, RetryPolicy},
    AppError,
};

#[derive(Default)]
pub struct ResearchPassReport {
    pub started: Vec<AgentHandle>,
    pub cleanups: Vec<BoundCleanupHandle>,
    pub deferred: usize,
    pub blocked: usize,
}

/// Schedule and launch at most `limit` due research reviews.
pub async fn run_due_research(
    db: &Db,
    runner: &AgentRunner,
    limits: CampaignLimits,
    now: i64,
    limit: usize,
) -> Result<ResearchPassReport, AppError> {
    run_due_research_with_cleanup_blocked_projects(
        db,
        runner,
        limits,
        now,
        limit,
        &BTreeSet::new(),
    )
    .await
}

/// Schedule research while honoring in-memory cleanup owners retained by the
/// daemon. A terminal database row is not sufficient admission evidence until
/// its native owner has released the private run authority.
pub(crate) async fn run_due_research_with_cleanup_blocked_projects(
    db: &Db,
    runner: &AgentRunner,
    limits: CampaignLimits,
    now: i64,
    limit: usize,
    cleanup_blocked_projects: &BTreeSet<String>,
) -> Result<ResearchPassReport, AppError> {
    let mut report = ResearchPassReport::default();
    let mut cleanup_blocked_projects = cleanup_blocked_projects.clone();
    if limit == 0 || limits.research_interval_minutes == 0 {
        return Ok(report);
    }
    // Native research is a Codex decision role.  Keep unsupported platforms
    // a clean no-op; the Linux native suite exercises the real launch path.
    if preflight_decision_runtime().is_err() {
        return Ok(report);
    }

    let repository = ResearchRepository::new(db);
    repository.schedule_running_campaigns(limits.research_interval_minutes, now, limit)?;
    let mut due_reviews = repository.due_launch_reviews(
        now,
        limit,
        limits.max_decision_attempts_per_cycle,
    )?;
    let remaining = limit.saturating_sub(due_reviews.len());
    for review in repository.claim_due_campaigns(now, remaining)? {
        if due_reviews.len() >= limit {
            break;
        }
        if due_reviews
            .iter()
            .all(|claimed| claimed.review_id != review.review_id)
        {
            due_reviews.push(review);
        }
    }
    for review in due_reviews {
        if review.state == "retry_wait" && repository.retry_is_blocked(&review.review_id)? {
            let Some(reason) = repository.retry_failure_code(&review.review_id)? else {
                report.deferred += 1;
                continue;
            };
            let event_id = repository.event_id(&review.review_id)?;
            let Some(event) = EventRepository::new(db).find_by_id(event_id)? else {
                report.deferred += 1;
                continue;
            };
            if repository.settle_retry_failure(
                &review.review_id,
                &review.state,
                review.attempt,
                review.agent_run_id,
                event.status,
                &reason,
                now,
            )? {
                report.blocked += 1;
            } else {
                report.deferred += 1;
            }
            continue;
        }
        launch_review(
            db,
            runner,
            limits,
            now,
            review.review_id.as_str(),
            &cleanup_blocked_projects,
            &mut report,
        )
        .await?;
        for cleanup in &report.cleanups {
            if let Some(project_id) = cleanup.cleanup_blocked_project() {
                cleanup_blocked_projects.insert(project_id.to_owned());
            }
        }
    }
    Ok(report)
}

/// Reconcile durable research rows after generic startup recovery has
/// preserved campaign-research owners.  Active/unknown native ownership is
/// intentionally left untouched; only terminal child rows can become retry
/// candidates here.
pub async fn recover_research(db: &Db, now: i64, _limits: CampaignLimits) -> Result<(), AppError> {
    let repository = ResearchRepository::new(db);
    repository.block_invalid_lineage(now)?;
    let claimed = repository.claimed_unbound_event_ids()?;
    if !claimed.is_empty() {
        EventRepository::new(db).defer_claimed(&claimed)?;
    }
    repository.recover_terminal_runs(now)?;
    Ok(())
}

async fn launch_review(
    db: &Db,
    runner: &AgentRunner,
    limits: CampaignLimits,
    now: i64,
    review_id: &str,
    cleanup_blocked_projects: &BTreeSet<String>,
    report: &mut ResearchPassReport,
) -> Result<(), AppError> {
    let repository = ResearchRepository::new(db);
    let review = repository.find(review_id)?;
    if !matches!(review.state.as_str(), "pending" | "retry_wait")
        || (review.state == "pending" && review.agent_run_id.is_some())
    {
        return Ok(());
    }
    let event_id = repository.event_id(review_id)?;
    if review.state == "retry_wait" && !repository.retry_owner_ready(review_id)? {
        report.deferred += 1;
        return Ok(());
    }
    let cap_attempt = repository.next_attempt_for_launch(review_id)?;
    if cap_attempt > i64::from(limits.max_decision_attempts_per_cycle) {
        let Some(event) = EventRepository::new(db).find_by_id(event_id)? else {
            report.deferred += 1;
            return Ok(());
        };
        if repository.settle_attempt_limit(
            review_id,
            &review.state,
            review.attempt,
            review.agent_run_id,
            event.status,
            limits.max_decision_attempts_per_cycle,
            now,
        )? {
            report.blocked += 1;
        } else {
            report.deferred += 1;
        }
        return Ok(());
    }
    let Some(event) = EventRepository::new(db).claim_by_id(
        &project_id_for_review(db, &review.campaign_id)?,
        event_id,
        now.saturating_add(60),
    )?
    else {
        report.deferred += 1;
        return Ok(());
    };

    let Some(campaign) = CampaignRepository::new(db).find_by_id(&review.campaign_id)? else {
        repository.block_review(review_id, "research_lineage_corrupt", now)?;
        report.blocked += 1;
        return Ok(());
    };
    let Some(project) = ProjectRepository::new(db).find_by_id(&campaign.project_id)? else {
        repository.block_review(review_id, "research_lineage_corrupt", now)?;
        report.blocked += 1;
        return Ok(());
    };
    if campaign.state != CampaignState::Active
        || !project.enabled
        || project.paused
        || project.halted_reason.is_some()
    {
        EventRepository::new(db).defer_claimed(&[event_id])?;
        report.deferred += 1;
        return Ok(());
    }
    if cleanup_blocked_projects.contains(&project.project_id) {
        EventRepository::new(db).defer_claimed(&[event_id])?;
        report.deferred += 1;
        return Ok(());
    }
    if AgentRunRepository::new(db)
        .find_active_by_project(&project.project_id)?
        .is_some()
    {
        EventRepository::new(db).defer_claimed(&[event_id])?;
        report.deferred += 1;
        return Ok(());
    }

    let project_config = match config::load(&project.config_path) {
        Ok(config) => config,
        Err(_) => {
            let expected_failure_code = repository.review_failure_code(review_id)?;
            if repository.settle_pre_admission_failure(
                review_id,
                &review.state,
                review.attempt,
                review.agent_run_id,
                event.status,
                expected_failure_code.as_deref(),
                "research_policy_blocked",
                now,
            )? {
                report.blocked += 1;
            } else {
                EventRepository::new(db).defer_claimed(&[event_id])?;
                report.deferred += 1;
            }
            return Ok(());
        }
    };
    let project_policy = match runner.resolve_project_policy(&project, &project_config) {
        Ok(policy) => policy,
        Err(violation) => {
            let _ = violation;
            let expected_failure_code = repository.review_failure_code(review_id)?;
            if repository.settle_pre_admission_failure(
                review_id,
                &review.state,
                review.attempt,
                review.agent_run_id,
                event.status,
                expected_failure_code.as_deref(),
                "research_policy_blocked",
                now,
            )? {
                report.blocked += 1;
            } else {
                EventRepository::new(db).defer_claimed(&[event_id])?;
                report.deferred += 1;
            }
            return Ok(());
        }
    };

    // Acquire both admission locks before consuming budget.  A lock or run-ID
    // deferral therefore consumes neither a review attempt nor a reservation.
    let Some(run_id_guard) = runner
        .try_acquire_run_id_admission_guard(db)
        .map_err(AppError::from)?
    else {
        EventRepository::new(db).defer_claimed(&[event_id])?;
        report.deferred += 1;
        return Ok(());
    };
    let Some(project_lock) = runner
        .try_acquire_project_admission_lock(&project_policy)
        .map_err(AppError::from)?
    else {
        EventRepository::new(db).defer_claimed(&[event_id])?;
        report.deferred += 1;
        return Ok(());
    };

    let target_attempt = repository.next_attempt_for_launch(review_id)?;
    if target_attempt > i64::from(limits.max_decision_attempts_per_cycle) {
        if repository.settle_attempt_limit(
            review_id,
            &review.state,
            review.attempt,
            review.agent_run_id,
            event.status,
            limits.max_decision_attempts_per_cycle,
            now,
        )? {
            report.blocked += 1;
        } else {
            EventRepository::new(db).defer_claimed(&[event_id])?;
            report.deferred += 1;
        }
        return Ok(());
    }
    let decision_key = format!("research:{review_id}:attempt:{target_attempt}");
    let reservation = match CampaignRepository::new(db).reserve_agent_run(
        &review.campaign_id,
        &decision_key,
        &limits,
        now,
    )? {
        AgentDecisionReservation::Reserved(reservation) => reservation,
        AgentDecisionReservation::BudgetWaiting { .. }
        | AgentDecisionReservation::Deferred { .. } => {
            EventRepository::new(db).defer_claimed(&[event_id])?;
            report.deferred += 1;
            return Ok(());
        }
    };
    let Some(admitted_review) = repository.prepare_attempt(
        review_id,
        &reservation.reservation_id,
        limits.max_decision_attempts_per_cycle,
        now,
    )?
    else {
        EventRepository::new(db).defer_claimed(&[event_id])?;
        report.deferred += 1;
        return Ok(());
    };
    let evidence = match build_research_evidence_with_policy(
        db,
        &admitted_review,
        now,
        runner.execution_policy(),
        &project_policy,
    ) {
        Ok(evidence) => evidence,
        Err(_) => {
            let blocked = repository.fail_unbound_attempt(
                review_id,
                "research_output_invalid",
                now,
                limits.max_decision_attempts_per_cycle,
                false,
            )?;
            if !blocked {
                let not_before = now.saturating_add(retry_backoff_seconds(admitted_review.attempt));
                let _ = EventRepository::new(db).transition_many(
                    &[event_id],
                    EventStatus::RetryWait,
                    now,
                    Some(not_before),
                    Some("research_output_invalid"),
                )?;
            }
            if blocked {
                report.blocked += 1;
            } else {
                report.deferred += 1;
            }
            return Ok(());
        }
    };

    let mut research_agent_config = project_config.agent.clone();
    if let Some(session_id) = repository.state(&admitted_review.campaign_id)?.session_id {
        // Once a campaign session is owned, every later review must use the
        // exact durable resume identity.  Fresh is reserved for the first
        // review or a confirmed missing-session reconstruction.
        research_agent_config.context = match runner.research_context_for(&project_policy, &session_id) {
            Ok(context) => context,
            Err(_) => {
                if repository.settle_unbound_failure(
                    review_id,
                    &admitted_review.state,
                    admitted_review.attempt,
                    admitted_review.agent_run_id,
                    event.status,
                    "research_session_unsafe",
                    now,
                )? {
                    report.blocked += 1;
                } else {
                    EventRepository::new(db).defer_claimed(&[event_id])?;
                    report.deferred += 1;
                }
                return Ok(());
            }
        };
    }

    let retry_policy = RetryPolicy {
        max_retries: limits.max_decision_attempts_per_cycle.saturating_sub(1),
    };
    match runner
        .spawn_research(
            db,
            &project,
            &project_policy,
            &research_agent_config,
            retry_policy,
            event_id,
            &[event_id],
            &admitted_review,
            &evidence,
            &reservation.reservation_id,
            now,
            run_id_guard,
            project_lock,
        )
        .await
    {
        Ok(handle) => report.started.push(handle),
        Err(error) => handle_spawn_error(
            db,
            error,
            event_id,
            review_id,
            admitted_review.attempt,
            limits,
            now,
            report,
        )?,
    }
    Ok(())
}

fn handle_spawn_error(
    db: &Db,
    error: AgentSpawnError,
    event_id: i64,
    review_id: &str,
    attempt: i64,
    limits: CampaignLimits,
    now: i64,
    report: &mut ResearchPassReport,
) -> Result<(), AppError> {
    let AgentSpawnError {
        stage,
        source,
        policy,
        cleanup,
    } = error;
    if let Some(cleanup) = cleanup {
        report.cleanups.push(cleanup);
        report.deferred += 1;
        return Ok(());
    }
    if !matches!(stage, AgentSpawnStage::PreBinding) {
        // A bound run without a returned cleanup owner remains authoritative;
        // startup recovery will inspect its PID/gate evidence before any
        // retry is considered.
        report.deferred += 1;
        return Ok(());
    }
    let repository = ResearchRepository::new(db);
    let unsafe_session_probe = matches!(
        source,
        AppError::CodexSessionMetadata { .. }
    ) || matches!(
        policy.as_ref().map(|violation| violation.code),
        Some(PolicyViolationCode::SessionMissing | PolicyViolationCode::SessionNotOwned)
    );
    if unsafe_session_probe {
        let review = repository.find(review_id)?;
        let Some(event) = EventRepository::new(db).find_by_id(event_id)? else {
            report.deferred += 1;
            return Ok(());
        };
        let blocked = repository.settle_unbound_failure(
            review_id,
            &review.state,
            review.attempt,
            review.agent_run_id,
            event.status,
            "research_session_unsafe",
            now,
        )?;
        if blocked {
            report.blocked += 1;
        } else {
            EventRepository::new(db).defer_claimed(&[event_id])?;
            report.deferred += 1;
        }
        return Ok(());
    }
    if policy.is_some() {
        let review = repository.find(review_id)?;
        let Some(event) = EventRepository::new(db).find_by_id(event_id)? else {
            report.deferred += 1;
            return Ok(());
        };
        if repository.settle_unbound_failure(
            review_id,
            &review.state,
            review.attempt,
            review.agent_run_id,
            event.status,
            "research_policy_blocked",
            now,
        )? {
            report.blocked += 1;
        } else {
            EventRepository::new(db).defer_claimed(&[event_id])?;
            report.deferred += 1;
        }
        return Ok(());
    }
    let failure_code = match stage {
        AgentSpawnStage::PreBinding => "research_spawn_failed",
        AgentSpawnStage::RunBoundPreMarker { .. } | AgentSpawnStage::PostMarker { .. } => {
            "research_session_unsafe"
        }
    };
    let blocked = repository.fail_unbound_attempt(
        review_id,
        failure_code,
        now,
        limits.max_decision_attempts_per_cycle,
        failure_code == "research_session_unsafe",
    )?;
    if blocked {
        let _ = EventRepository::new(db).transition_many(
            &[event_id],
            EventStatus::Failed,
            now,
            None,
            Some(failure_code),
        )?;
        report.blocked += 1;
    } else {
        let not_before = now.saturating_add(retry_backoff_seconds(attempt));
        let _ = EventRepository::new(db).transition_many(
            &[event_id],
            EventStatus::RetryWait,
            now,
            Some(not_before),
            Some(failure_code),
        )?;
        report.deferred += 1;
    }
    Ok(())
}

fn project_id_for_review(db: &Db, campaign_id: &str) -> Result<String, AppError> {
    CampaignRepository::new(db)
        .find_by_id(campaign_id)?
        .map(|campaign| campaign.project_id)
        .ok_or_else(|| AppError::Validation {
            field: "campaign_id",
            message: "does not identify a persisted campaign",
        })
}
