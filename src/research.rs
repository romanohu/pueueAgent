//! Dedicated scheduler and restart coordinator for campaign research.
//!
//! CampaignResearch events have a durable review owner and are deliberately
//! kept out of the generic prompt scheduler.  This module is the only path
//! that claims those events, reserves their bounded campaign budget, and
//! hands the exact review/evidence identity to the native research role.

use crate::{
    agent::{AgentHandle, AgentRunner, AgentSpawnError, AgentSpawnStage, BoundCleanupHandle},
    config,
    db::{
        AgentDecisionReservation, AgentRunRepository, CampaignRepository, Db, EventRepository,
        ProjectRepository, ResearchRepository,
    },
    execution_policy::{preflight_decision_runtime, CampaignLimits},
    models::{CampaignState, EventStatus},
    research_evidence::build_research_evidence,
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
    let mut report = ResearchPassReport::default();
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
    let mut due_reviews = repository.claim_due_campaigns(now, limit)?;
    for review in repository.due_reviews(now, limit)? {
        if due_reviews
            .iter()
            .all(|claimed| claimed.review_id != review.review_id)
        {
            due_reviews.push(review);
        }
    }
    for review in due_reviews {
        if review.state == "retry_wait" && repository.retry_is_blocked(&review.review_id)? {
            let reason = repository
                .retry_failure_code(&review.review_id)?
                .unwrap_or_else(|| "research_policy_blocked".to_owned());
            repository.block_review(&review.review_id, &reason, now)?;
            let event_id = repository.event_id(&review.review_id)?;
            EventRepository::new(db).transition_many(
                &[event_id],
                EventStatus::Failed,
                now,
                None,
                Some(&reason),
            )?;
            report.blocked += 1;
            continue;
        }
        launch_review(
            db,
            runner,
            limits,
            now,
            review.review_id.as_str(),
            &mut report,
        )
        .await?;
    }
    Ok(report)
}

/// Reconcile durable research rows after generic startup recovery has
/// preserved campaign-research owners.  Active/unknown native ownership is
/// intentionally left untouched; only terminal child rows can become retry
/// candidates here.
pub async fn recover_research(db: &Db, now: i64, limits: CampaignLimits) -> Result<(), AppError> {
    let repository = ResearchRepository::new(db);
    repository.block_invalid_lineage(now)?;
    let claimed = repository.claimed_unbound_event_ids()?;
    if !claimed.is_empty() {
        EventRepository::new(db).defer_claimed(&claimed)?;
    }
    repository.recover_terminal_runs(now)?;
    for review in repository.due_reviews(now, 32)? {
        if review.state == "retry_wait" {
            repository.schedule_retry(
                &review.review_id,
                limits.max_decision_attempts_per_cycle,
                now,
            )?;
        }
    }
    Ok(())
}

async fn launch_review(
    db: &Db,
    runner: &AgentRunner,
    limits: CampaignLimits,
    now: i64,
    review_id: &str,
    report: &mut ResearchPassReport,
) -> Result<(), AppError> {
    let repository = ResearchRepository::new(db);
    let review = repository.find(review_id)?;
    if review.state != "pending" || review.agent_run_id.is_some() {
        return Ok(());
    }
    let event_id = repository.event_id(review_id)?;
    let Some(_event) = EventRepository::new(db).claim_by_id(
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
            repository.block_review(review_id, "research_policy_blocked", now)?;
            let _ = EventRepository::new(db).transition_many(
                &[event_id],
                EventStatus::Failed,
                now,
                None,
                Some("research_policy_blocked"),
            )?;
            report.blocked += 1;
            return Ok(());
        }
    };
    let project_policy = match runner.resolve_project_policy(&project, &project_config) {
        Ok(policy) => policy,
        Err(violation) => {
            repository.block_review(review_id, "research_policy_blocked", now)?;
            EventRepository::new(db).dead_letter_claimed_without_run(
                &project.project_id,
                &[event_id],
                now,
                &violation,
            )?;
            report.blocked += 1;
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
        repository.block_review(review_id, "research_attempt_limit", now)?;
        let _ = EventRepository::new(db).transition_many(
            &[event_id],
            EventStatus::Failed,
            now,
            None,
            Some("research_attempt_limit"),
        )?;
        report.blocked += 1;
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
    let evidence = match build_research_evidence(db, &admitted_review, now) {
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
        research_agent_config.context = runner
            .research_context_for(&project_policy, &session_id)
            .map_err(AppError::from)?;
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
        source: _source,
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
    if let Some(violation) = policy {
        repository.block_review(review_id, "research_policy_blocked", now)?;
        let project_id = EventRepository::new(db)
            .find_by_id(event_id)?
            .map(|event| event.project_id)
            .ok_or(AppError::Validation {
                field: "event_id",
                message: "does not identify a persisted research event",
            })?;
        EventRepository::new(db).dead_letter_claimed_without_run(
            &project_id,
            &[event_id],
            now,
            &violation,
        )?;
        report.blocked += 1;
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
