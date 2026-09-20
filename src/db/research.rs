use std::collections::BTreeSet;

use rusqlite::{params, Connection, OptionalExtension, Row, Transaction, TransactionBehavior};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{models::EventStatus, AppError};

use super::{database_error, Db};

const MAX_RESEARCH_REVIEW_LIST: i64 = 32;
const MAX_RESEARCH_CANDIDATES: i64 = 32;
const OPEN_REVIEW_STATES: &str = "('pending','running','ready','retry_wait')";
const OPEN_OPERATION_STAGES: &str =
    "('intent','stop_requested','stop_confirmed','successor_reserved')";
const RESEARCH_RETRY_FAILURE_UNSAFE: &str = "research_session_unsafe";
const RESEARCH_RETRY_FAILURE_POLICY: &str = "research_policy_blocked";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchState {
    pub campaign_id: String,
    pub session_id: Option<String>,
    pub session_generation: i64,
    pub next_due_at: Option<i64>,
    pub blocked_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchReview {
    pub review_id: String,
    pub campaign_id: String,
    pub experiment_id: String,
    pub task_signature: String,
    pub attempt: i64,
    pub state: String,
    pub operation_stage: Option<String>,
    pub agent_run_id: Option<i64>,
    pub context_json: Option<String>,
    pub context_digest: Option<String>,
    pub response_json: Option<String>,
    pub termination_request_id: Option<i64>,
    pub successor_experiment_id: Option<String>,
    pub session_generation: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchLaunchBinding {
    pub review_id: String,
    pub campaign_id: String,
    pub experiment_id: String,
    pub attempt: i64,
    pub session_generation: i64,
    pub prior_session_generation: i64,
    pub session_id: String,
    pub prior_session_id: Option<String>,
    pub context_json: String,
    pub context_digest: String,
    pub budget_reservation_id: String,
    pub recovery_reason: Option<String>,
}

pub struct ResearchRepository<'db> {
    db: &'db Db,
}

const REVIEW_SELECT: &str = "SELECT review_id, campaign_id, experiment_id,
        task_signature, attempt, state, operation_stage, agent_run_id,
        context_json, context_digest, response_json, termination_request_id,
        successor_experiment_id, evidence_schema_version, session_generation,
        event_id, not_before, notes_json, failure_code, decision_cycle_id,
        checkpoint_json, created_at, started_at, finished_at, updated_at
    FROM research_reviews";
const LAUNCH_REVIEW_SELECT: &str = "SELECT review.review_id, review.campaign_id,
        review.experiment_id, review.task_signature, review.attempt, review.state,
        review.operation_stage, review.agent_run_id, review.context_json,
        review.context_digest, review.response_json, review.termination_request_id,
        review.successor_experiment_id, review.evidence_schema_version,
        review.session_generation, review.event_id, review.not_before,
        review.notes_json, review.failure_code, review.decision_cycle_id,
        review.checkpoint_json, review.created_at, review.started_at,
        review.finished_at, review.updated_at
    FROM research_reviews AS review";

impl<'db> ResearchRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn ensure_campaign(&self, campaign_id: &str) -> Result<(), AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin research state creation"))?;
        ensure_campaign_in_transaction(&transaction, campaign_id)?;
        transaction
            .commit()
            .map_err(database_error("commit research state creation"))
    }

    pub fn state(&self, campaign_id: &str) -> Result<ResearchState, AppError> {
        let connection = self.db.connect()?;
        let campaign_exists: bool = connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM campaigns WHERE campaign_id = ?1
                 )",
                [campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error("check research campaign"))?;
        if !campaign_exists {
            return Err(validation_error(
                "campaign_id",
                "does not identify a persisted campaign",
            ));
        }
        connection
            .query_row(
                "SELECT campaign_id, session_id, session_generation,
                        next_due_at, blocked_reason
                 FROM campaign_research
                 WHERE campaign_id = ?1",
                [campaign_id],
                |row| {
                    Ok(ResearchState {
                        campaign_id: row.get(0)?,
                        session_id: row.get(1)?,
                        session_generation: row.get(2)?,
                        next_due_at: row.get(3)?,
                        blocked_reason: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(database_error("read research state"))
            .map(|state| {
                state.unwrap_or_else(|| ResearchState {
                    campaign_id: campaign_id.to_owned(),
                    session_id: None,
                    session_generation: 0,
                    next_due_at: None,
                    blocked_reason: None,
                })
            })
    }

    pub fn schedule_running(
        &self,
        campaign_id: &str,
        started_at: i64,
        interval_minutes: u32,
        now: i64,
    ) -> Result<(), AppError> {
        let candidate_due = next_research_due(started_at, interval_minutes)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin research scheduling"))?;
        ensure_campaign_in_transaction(&transaction, campaign_id)?;

        let (current_due, blocked_reason): (Option<i64>, Option<String>) = transaction
            .query_row(
                "SELECT next_due_at, blocked_reason
                 FROM campaign_research WHERE campaign_id = ?1",
                [campaign_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(database_error("read research scheduling state"))?;
        let has_open_review = has_open_review(&transaction, campaign_id)?;
        let next_due = if interval_minutes == 0 {
            None
        } else if blocked_reason.is_some() || has_open_review {
            current_due
        } else if current_due.is_some() {
            current_due
        } else {
            let completed_anchor: Option<i64> = transaction
                .query_row(
                    "SELECT COALESCE(finished_at, updated_at)
                     FROM research_reviews
                     WHERE campaign_id = ?1 AND state = 'completed'
                     ORDER BY COALESCE(finished_at, updated_at) DESC, review_id DESC
                     LIMIT 1",
                    [campaign_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(database_error("read latest completed research review"))?;
            completed_anchor
                .map(|anchor| next_research_due(anchor, interval_minutes))
                .unwrap_or(Ok(candidate_due))?
        };
        if next_due != current_due {
            transaction
                .execute(
                    "UPDATE campaign_research
                     SET next_due_at = ?1, updated_at = ?2
                     WHERE campaign_id = ?3",
                    params![next_due, now, campaign_id],
                )
                .map_err(database_error("schedule research state"))?;
        }
        transaction
            .commit()
            .map_err(database_error("commit research scheduling"))
    }

    /// Schedule each active campaign that still has an authoritative running
    /// task.  The durable observation is the clock source: a missing native
    /// `started_at` falls back to the first confirmed running observation, not
    /// the submission timestamp.
    pub fn schedule_running_campaigns(
        &self,
        interval_minutes: u32,
        now: i64,
        limit: usize,
    ) -> Result<usize, AppError> {
        if limit == 0 || interval_minutes == 0 {
            return Ok(0);
        }
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT c.campaign_id,
                        MIN(COALESCE(observation.started_at,
                                     observation.first_observed_at))
                 FROM campaigns AS c
                 JOIN projects AS p ON p.project_id = c.project_id
                 JOIN experiments AS e ON e.campaign_id = c.campaign_id
                 JOIN submissions AS s
                   ON s.submission_id = e.submission_id
                  AND s.project_id = c.project_id
                 JOIN task_observations AS observation
                   ON observation.project_id = c.project_id
                  AND observation.task_signature = e.task_signature
                  AND observation.pueue_task_id = e.pueue_task_id
                  AND observation.pueue_group = p.pueue_group
                  AND lower(observation.state) = 'running'
                 LEFT JOIN campaign_research AS research_state
                   ON research_state.campaign_id = c.campaign_id
                 WHERE c.state = 'active'
                   AND p.enabled = 1 AND p.paused = 0
                   AND p.halted_reason IS NULL
                   AND e.status = 'accepted'
                   AND e.pueue_task_id IS NOT NULL
                   AND s.status = 'accepted'
                   AND s.pueue_task_id = e.pueue_task_id
                   AND s.task_signature = e.task_signature
                   AND (
                       research_state.campaign_id IS NULL
                       OR (
                           research_state.next_due_at IS NULL
                           AND research_state.blocked_reason IS NULL
                           AND NOT EXISTS (
                               SELECT 1 FROM research_reviews AS open_review
                               WHERE open_review.campaign_id = c.campaign_id
                                 AND open_review.state IN ('pending','running','ready','retry_wait')
                           )
                       )
                   )
                 GROUP BY c.campaign_id
                 ORDER BY MIN(COALESCE(observation.started_at,
                                        observation.first_observed_at)),
                          c.campaign_id
                 LIMIT ?1",
            )
            .map_err(database_error("prepare running research campaign query"))?;
        let campaigns = statement
            .query_map([limit.min(MAX_RESEARCH_CANDIDATES as usize) as i64], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(database_error("query running research campaigns"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read running research campaigns"))?;
        drop(statement);
        let mut scheduled = 0;
        for (campaign_id, started_at) in campaigns {
            self.schedule_running(&campaign_id, started_at, interval_minutes, now)?;
            scheduled += 1;
        }
        Ok(scheduled)
    }

    /// Return pending or retry-wait reviews whose durable wake time has
    /// arrived.  CampaignResearch events are intentionally not exposed to the
    /// generic scheduler; this is the dedicated coordinator's queue.
    pub fn due_reviews(&self, now: i64, limit: usize) -> Result<Vec<ResearchReview>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{REVIEW_SELECT}
                 WHERE state IN ('pending','retry_wait') AND not_before <= ?1
                 ORDER BY not_before, created_at, review_id
                 LIMIT ?2"
            ))
            .map_err(database_error("prepare due research review query"))?;
        let rows = statement
            .query_map(params![now, limit.min(MAX_RESEARCH_REVIEW_LIST as usize) as i64], review_from_row)
            .map_err(database_error("query due research reviews"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read due research reviews"))
    }

    /// Return launchable research reviews without allowing a structurally
    /// deferred row to consume the bounded coordinator prefix.  Terminal
    /// capped or unsafe rows remain visible so the coordinator can settle
    /// them without reserving another budget slot.
    pub fn due_launch_reviews(
        &self,
        now: i64,
        limit: usize,
        max_attempts: u32,
    ) -> Result<Vec<ResearchReview>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{LAUNCH_REVIEW_SELECT}
                 JOIN campaign_research AS research_state
                   ON research_state.campaign_id = review.campaign_id
                 JOIN campaigns AS campaign
                   ON campaign.campaign_id = review.campaign_id
                 JOIN projects AS project
                   ON project.project_id = campaign.project_id
                 JOIN events AS event
                   ON event.event_id = review.event_id
                 WHERE review.state IN ('pending','retry_wait')
                   AND review.not_before <= ?1
                   AND NOT (review.state = 'pending' AND review.agent_run_id IS NOT NULL)
                   AND (
                       (
                           campaign.state = 'active'
                           AND project.enabled = 1
                           AND project.paused = 0
                           AND project.halted_reason IS NULL
                           AND NOT EXISTS (
                               SELECT 1
                               FROM agent_runs AS busy
                               WHERE busy.project_id = campaign.project_id
                                 AND busy.status IN ('starting','running')
                           )
                           AND event.status IN ('pending','retry_wait')
                           AND event.not_before <= ?1
                           AND NOT (
                               review.state = 'retry_wait'
                               AND (
                                   review.attempt >= ?3
                                   OR review.failure_code IN (
                                       'research_session_unsafe',
                                       'research_policy_blocked'
                                   )
                               )
                           )
                       )
                       OR (
                           review.state = 'retry_wait'
                           AND (
                               review.attempt >= ?3
                               OR review.failure_code IN (
                                   'research_session_unsafe',
                                   'research_policy_blocked'
                               )
                           )
                           AND event.status IN ('pending','retry_wait','failed','dead_letter')
                           AND (
                               event.status IN ('failed','dead_letter')
                               OR event.not_before <= ?1
                           )
                       )
                   )
                   AND (
                       review.agent_run_id IS NULL
                       OR EXISTS (
                           SELECT 1
                           FROM agent_runs AS owner
                           WHERE owner.run_id = review.agent_run_id
                             AND owner.status IN ('completed','failed','timed_out','cancelled')
                             AND owner.launch_gate_state IN ('released','failed')
                       )
                   )
                 ORDER BY review.not_before, review.created_at, review.review_id
                 LIMIT ?2"
            ))
            .map_err(database_error("prepare launchable research review query"))?;
        let rows = statement
            .query_map(
                params![
                    now,
                    limit.min(MAX_RESEARCH_REVIEW_LIST as usize) as i64,
                    i64::from(max_attempts),
                ],
                review_from_row,
            )
            .map_err(database_error("query launchable research reviews"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read launchable research reviews"))
    }

    /// Claim the oldest authoritative running experiment for each campaign
    /// whose durable interval has elapsed.  The claim itself remains the
    /// transactional source of truth; this method only supplies the bounded
    /// candidates to the dedicated coordinator.
    pub fn claim_due_campaigns(
        &self,
        now: i64,
        limit: usize,
    ) -> Result<Vec<ResearchReview>, AppError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT c.campaign_id, e.experiment_id, e.task_signature
                 FROM campaign_research AS state
                 JOIN campaigns AS c ON c.campaign_id = state.campaign_id
                 JOIN projects AS p ON p.project_id = c.project_id
                 JOIN experiments AS e ON e.campaign_id = c.campaign_id
                 JOIN submissions AS s
                   ON s.submission_id = e.submission_id
                  AND s.project_id = c.project_id
                 JOIN task_observations AS observation
                   ON observation.project_id = c.project_id
                  AND observation.task_signature = e.task_signature
                  AND observation.pueue_task_id = e.pueue_task_id
                  AND observation.pueue_group = p.pueue_group
                  AND lower(observation.state) = 'running'
                 WHERE state.next_due_at <= ?1
                   AND state.blocked_reason IS NULL
                   AND c.state = 'active'
                   AND p.enabled = 1 AND p.paused = 0
                   AND p.halted_reason IS NULL
                   AND e.status = 'accepted' AND e.pueue_task_id IS NOT NULL
                   AND s.status = 'accepted'
                   AND s.pueue_task_id = e.pueue_task_id
                   AND s.task_signature = e.task_signature
                   AND NOT EXISTS (
                       SELECT 1 FROM research_reviews AS open_review
                       WHERE open_review.campaign_id = c.campaign_id
                         AND open_review.state IN ('pending','running','ready','retry_wait')
                   )
                 ORDER BY state.next_due_at,
                          COALESCE(observation.started_at, observation.first_observed_at),
                          e.experiment_id
                 LIMIT ?2",
            )
            .map_err(database_error("prepare due research campaign claims"))?;
        let rows = statement
            .query_map(
                params![now, limit.min(MAX_RESEARCH_CANDIDATES as usize) as i64],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .map_err(database_error("query due research campaign claims"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read due research campaign claims"))?;
        drop(statement);
        let mut seen_campaigns = BTreeSet::new();
        let mut reviews = Vec::new();
        for (campaign_id, experiment_id, task_signature) in rows {
            if !seen_campaigns.insert(campaign_id.clone()) {
                continue;
            }
            if let Some(review) = self.claim_due(
                &campaign_id,
                &experiment_id,
                &task_signature,
                now,
            )? {
                reviews.push(review);
            }
        }
        Ok(reviews)
    }

    pub fn event_id(&self, review_id: &str) -> Result<i64, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT event_id FROM research_reviews WHERE review_id = ?1",
                [review_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("read research review event"))?
            .ok_or_else(|| validation_error("review_id", "does not identify a persisted research review"))
    }

    pub fn reservation_id_for_attempt(
        &self,
        campaign_id: &str,
        review_id: &str,
        attempt: i64,
    ) -> Result<Option<String>, AppError> {
        if attempt < 0 {
            return Err(validation_error("research.attempt", "must be non-negative"));
        }
        let connection = self.db.connect()?;
        let subject_key = format!("research:{review_id}:attempt:{attempt}");
        connection
            .query_row(
                "SELECT reservation_id FROM budget_reservations
                 WHERE campaign_id = ?1 AND experiment_id IS NULL
                   AND dimension = 'agent_run' AND subject_key = ?2
                   AND status = 'consumed'",
                params![campaign_id, subject_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("read research budget reservation"))
    }

    pub fn retry_is_blocked(&self, review_id: &str) -> Result<bool, AppError> {
        Ok(self.retry_failure_code(review_id)?.is_some())
    }

    pub fn retry_failure_code(&self, review_id: &str) -> Result<Option<String>, AppError> {
        let connection = self.db.connect()?;
        let failure_code: Option<String> = connection
            .query_row(
                "SELECT failure_code FROM research_reviews WHERE review_id = ?1",
                [review_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("read research retry policy"))?
            .flatten();
        Ok(failure_code.filter(|code| {
            code == RESEARCH_RETRY_FAILURE_UNSAFE || code == RESEARCH_RETRY_FAILURE_POLICY
        }))
    }

    /// A bound retry may be admitted only after its previous native owner is
    /// durably terminal.  Missing or active rows are deliberately treated as
    /// not ready so a wake cannot consume a second reservation around an
    /// unknown owner.
    pub fn retry_owner_ready(&self, review_id: &str) -> Result<bool, AppError> {
        let connection = self.db.connect()?;
        let Some((state, agent_run_id)) = connection
            .query_row(
                "SELECT state, agent_run_id FROM research_reviews WHERE review_id = ?1",
                [review_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .optional()
            .map_err(database_error("read research retry owner"))?
        else {
            return Err(validation_error(
                "review_id",
                "does not identify a persisted research review",
            ));
        };
        if state != "retry_wait" {
            return Ok(true);
        }
        let Some(agent_run_id) = agent_run_id else {
            return Ok(true);
        };
        let Some((status, gate_state)) = connection
            .query_row(
                "SELECT status, launch_gate_state FROM agent_runs WHERE run_id = ?1",
                [agent_run_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(database_error("read research retry owner state"))?
        else {
            return Ok(false);
        };
        Ok(matches!(
            status.as_str(),
            "completed" | "failed" | "timed_out" | "cancelled"
        ) && matches!(gate_state.as_str(), "released" | "failed"))
    }

    pub fn next_attempt_for_launch(&self, review_id: &str) -> Result<i64, AppError> {
        let connection = self.db.connect()?;
        let row: Option<(i64, Option<i64>, Option<String>)> = connection
            .query_row(
                "SELECT attempt, agent_run_id, failure_code
                 FROM research_reviews WHERE review_id = ?1",
                [review_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(database_error("read next research attempt"))?;
        let Some((attempt, agent_run_id, failure_code)) = row else {
            return Err(validation_error(
                "review_id",
                "does not identify a persisted research review",
            ));
        };
        if attempt > 0 && agent_run_id.is_none() && failure_code.is_none() {
            Ok(attempt)
        } else {
            attempt
                .checked_add(1)
                .ok_or_else(|| validation_error("research.attempt", "cannot advance review attempt"))
        }
    }

    /// Atomically settle a capped retry and its linked event.  The caller's
    /// state, attempt, owner, and event status are all compare-and-swap
    /// predicates; a stale coordinator therefore leaves every row untouched.
    pub fn settle_attempt_limit(
        &self,
        review_id: &str,
        expected_state: &str,
        expected_attempt: i64,
        expected_run_id: Option<i64>,
        expected_event_status: EventStatus,
        max_attempts: u32,
        now: i64,
    ) -> Result<bool, AppError> {
        if review_id.is_empty() || expected_state.is_empty() {
            return Err(validation_error(
                "research.review",
                "review and expected state must be non-empty",
            ));
        }
        if !matches!(expected_state, "pending" | "retry_wait") {
            return Err(validation_error(
                "research.state",
                "attempt cap settlement requires pending or retry_wait",
            ));
        }
        if !matches!(
            expected_event_status,
            EventStatus::Pending
                | EventStatus::Claimed
                | EventStatus::RetryWait
                | EventStatus::Failed
                | EventStatus::DeadLetter
        ) {
            return Err(validation_error(
                "event_status",
                "attempt cap settlement requires a claimable or terminal event",
            ));
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin research attempt cap settlement"))?;
        let current: Option<(
            String,
            String,
            i64,
            Option<i64>,
            Option<String>,
            i64,
            EventStatus,
            Option<String>,
            Option<String>,
            Option<String>,
        )> = transaction
            .query_row(
                "SELECT review.campaign_id, review.state, review.attempt,
                        review.agent_run_id, review.failure_code, event.event_id,
                        event.status, owner.status, owner.launch_gate_state,
                        research_state.blocked_reason
                 FROM research_reviews AS review
                 JOIN campaign_research AS research_state
                   ON research_state.campaign_id = review.campaign_id
                 JOIN campaigns AS campaign
                   ON campaign.campaign_id = review.campaign_id
                 JOIN events AS event
                   ON event.event_id = review.event_id
                 LEFT JOIN agent_runs AS owner
                   ON owner.run_id = review.agent_run_id
                 WHERE review.review_id = ?1",
                [review_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                    ))
                },
            )
            .optional()
            .map_err(database_error("read research attempt cap settlement"))?;
        let Some((
            campaign_id,
            state,
            attempt,
            run_id,
            failure_code,
            event_id,
            event_status,
            owner_status,
            owner_gate,
            campaign_blocked_reason,
        )) = current
        else {
            transaction
                .commit()
                .map_err(database_error("commit missing research cap settlement"))?;
            return Ok(false);
        };
        if state != expected_state
            || attempt != expected_attempt
            || run_id != expected_run_id
            || event_status != expected_event_status
            || campaign_blocked_reason.is_some()
        {
            transaction
                .commit()
                .map_err(database_error("commit stale research cap settlement"))?;
            return Ok(false);
        }
        if run_id.is_some() {
            if !matches!(
                owner_status.as_deref(),
                Some("completed" | "failed" | "timed_out" | "cancelled")
            ) || !matches!(owner_gate.as_deref(), Some("released" | "failed"))
            {
                transaction
                    .commit()
                    .map_err(database_error("commit active research cap owner"))?;
                return Ok(false);
            }
        }
        let target_attempt = if attempt > 0 && run_id.is_none() && failure_code.is_none() {
            attempt
        } else {
            attempt.checked_add(1).ok_or_else(|| {
                validation_error("research.attempt", "cannot advance review attempt")
            })?
        };
        if target_attempt <= i64::from(max_attempts) {
            transaction
                .commit()
                .map_err(database_error("commit non-capped research review"))?;
            return Ok(false);
        }
        let changed = transaction
            .execute(
                "UPDATE research_reviews
                 SET state = 'blocked', failure_code = 'research_attempt_limit',
                     finished_at = ?1, not_before = ?1, updated_at = ?1
                 WHERE review_id = ?2 AND state = ?3 AND attempt = ?4
                   AND ((agent_run_id IS NULL AND ?5 IS NULL) OR agent_run_id = ?5)
                   AND event_id = ?6",
                params![
                    now,
                    review_id,
                    expected_state,
                    expected_attempt,
                    expected_run_id,
                    event_id,
                ],
            )
            .map_err(database_error("settle capped research review"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "settle capped research review CAS",
            });
        }
        let changed = transaction
            .execute(
                "UPDATE campaign_research
                 SET blocked_reason = 'research_attempt_limit', next_due_at = NULL,
                     updated_at = ?1
                 WHERE campaign_id = ?2 AND blocked_reason IS NULL",
                params![now, campaign_id],
            )
            .map_err(database_error("block capped research campaign"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "settle capped research campaign CAS",
            });
        }
        let changed = transaction
            .execute(
                "UPDATE events
                 SET status = 'failed', lease_until = NULL,
                     completed_at = ?1, last_error = 'research_attempt_limit'
                 WHERE event_id = ?2 AND status = ?3",
                params![now, event_id, expected_event_status],
            )
            .map_err(database_error("settle capped research event"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "settle capped research event CAS",
            });
        }
        transaction
            .commit()
            .map_err(database_error("commit capped research settlement"))?;
        Ok(true)
    }

    pub fn claimed_unbound_event_ids(&self) -> Result<Vec<i64>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT r.event_id
                 FROM research_reviews AS r
                 JOIN events AS event ON event.event_id = r.event_id
                 WHERE r.state IN ('pending','retry_wait')
                   AND r.agent_run_id IS NULL
                   AND event.kind = 'campaign_research'
                   AND event.status = 'claimed'
                   AND NOT EXISTS (
                       SELECT 1 FROM agent_run_events link
                       WHERE link.project_id = event.project_id
                         AND link.event_id = event.event_id
                   )
                 ORDER BY r.event_id",
            )
            .map_err(database_error("prepare unbound research event recovery"))?;
        let event_ids = statement
            .query_map([], |row| row.get::<_, i64>(0))
            .map_err(database_error("query unbound research events"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read unbound research events"))?;
        Ok(event_ids)
    }

    /// Admit exactly one bounded review attempt.  A reservation-without-run
    /// recovery reuses its already-consumed attempt; all ordinary retries
    /// increment the attempt once and never refund the prior reservation.
    pub fn prepare_attempt(
        &self,
        review_id: &str,
        reservation_id: &str,
        max_attempts: u32,
        now: i64,
    ) -> Result<Option<ResearchReview>, AppError> {
        validate_budget_reservation_id(reservation_id)?;
        if max_attempts == 0 {
            return Err(validation_error(
                "max_decision_attempts_per_cycle",
                "must be positive",
            ));
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin research attempt admission"))?;
        let current: (
            String,
            String,
            i64,
            Option<i64>,
            i64,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = transaction
            .query_row(
                "SELECT campaign_id, state, attempt, agent_run_id,
                        session_generation, experiment_id, failure_code,
                        context_json, context_digest, notes_json
                 FROM research_reviews WHERE review_id = ?1",
                [review_id],
                |row| Ok((
                    row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?,
                    row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?,
                    row.get(8)?, row.get(9)?,
                )),
            )
            .optional()
            .map_err(database_error("read research attempt admission"))?
            .ok_or_else(|| validation_error("review_id", "does not identify a persisted research review"))?;
        let (
            campaign_id,
            state,
            current_attempt,
            current_run,
            _generation,
            _experiment_id,
            failure_code,
            context_json,
            context_digest,
            notes_json,
        ) = current;
        if !matches!(state.as_str(), "pending" | "retry_wait") {
            transaction
                .commit()
                .map_err(database_error("commit skipped research attempt admission"))?;
            return Ok(None);
        }
        if let Some(current_run) = current_run {
            let owner_ready: Option<(String, String)> = transaction
                .query_row(
                    "SELECT status, launch_gate_state FROM agent_runs WHERE run_id = ?1",
                    [current_run],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(database_error("read bound research retry owner"))?;
            let owner_ready = owner_ready.is_some_and(|(status, gate_state)| {
                matches!(
                    status.as_str(),
                    "completed" | "failed" | "timed_out" | "cancelled"
                ) && matches!(gate_state.as_str(), "released" | "failed")
            });
            if state != "retry_wait" || !owner_ready {
                transaction
                    .commit()
                    .map_err(database_error("commit skipped bound research retry"))?;
                return Ok(None);
            }
        }
        let reservation_matches = budget_reservation_matches(
            &transaction,
            &campaign_id,
            review_id,
            current_attempt,
            reservation_id,
        )?;
        let target_attempt = if current_attempt > 0
            && current_run.is_none()
            && failure_code.is_none()
            && reservation_matches
        {
            current_attempt
        } else {
            current_attempt.checked_add(1).ok_or_else(|| {
                validation_error("research.attempt", "cannot advance review attempt")
            })?
        };
        if target_attempt > i64::from(max_attempts) {
            block_review_in_transaction(
                &transaction,
                review_id,
                "research_attempt_limit",
                now,
            )?;
            transaction
                .commit()
                .map_err(database_error("commit research attempt limit"))?;
            return Ok(None);
        }
        if !budget_reservation_matches(
            &transaction,
            &campaign_id,
            review_id,
            target_attempt,
            reservation_id,
        )? {
            return Err(validation_error(
                "budget_reservation_id",
                "must be the consumed campaign reservation for this research review attempt",
            ));
        }
        let notes_json = if let Some(previous_run_id) = current_run {
            let mut notes = notes_json
                .as_deref()
                .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
                .filter(serde_json::Value::is_object)
                .unwrap_or_else(|| json!({}));
            if !notes
                .get("retry_history")
                .is_some_and(serde_json::Value::is_array)
            {
                notes["retry_history"] = json!([]);
            }
            let history = notes
                .get_mut("retry_history")
                .and_then(serde_json::Value::as_array_mut)
                .expect("retry history array just created");
            history.push(json!({
                "attempt": current_attempt,
                "agent_run_id": previous_run_id,
                "failure_code": failure_code,
                "context_json": context_json,
                "context_digest": context_digest,
            }));
            Some(notes.to_string())
        } else {
            notes_json
        };
        let changed = transaction
            .execute(
                "UPDATE research_reviews
                 SET attempt = ?1, state = 'pending', operation_stage = NULL,
                     agent_run_id = NULL, context_json = NULL, context_digest = NULL,
                     response_json = NULL, failure_code = NULL, started_at = NULL,
                     finished_at = NULL, notes_json = ?2, not_before = ?3, updated_at = ?3
                 WHERE review_id = ?4 AND state IN ('pending','retry_wait')
                   AND attempt = ?5",
                params![target_attempt, notes_json, now, review_id, current_attempt],
            )
            .map_err(database_error("admit research review attempt"))?;
        if changed != 1 {
            transaction
                .commit()
                .map_err(database_error("commit changed research attempt"))?;
            return Ok(None);
        }
        let review = transaction
            .query_row(
                &format!("{REVIEW_SELECT} WHERE review_id = ?1"),
                [review_id],
                review_from_row,
            )
            .map_err(database_error("read admitted research review"))?;
        transaction
            .commit()
            .map_err(database_error("commit research attempt admission"))?;
        Ok(Some(review))
    }

    /// Put a native failure behind a finite, redacted retry wake.  Unsafe and
    /// policy failures bypass retry immediately; the campaign is marked
    /// blocked so another daemon cannot launch around the decision.
    pub fn schedule_retry(
        &self,
        review_id: &str,
        max_attempts: u32,
        now: i64,
    ) -> Result<bool, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin research retry scheduling"))?;
        let row: Option<(i64, String, Option<String>)> = transaction
            .query_row(
                "SELECT attempt, state, failure_code FROM research_reviews
                 WHERE review_id = ?1",
                [review_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(database_error("read research retry state"))?;
        let Some((attempt, state, failure_code)) = row else {
            return Err(validation_error("review_id", "does not identify a persisted research review"));
        };
        if state != "retry_wait" {
            transaction
                .commit()
                .map_err(database_error("commit unchanged research retry state"))?;
            return Ok(false);
        }
        let blocked = failure_code.as_deref().is_some_and(|code| {
            code == RESEARCH_RETRY_FAILURE_UNSAFE || code == RESEARCH_RETRY_FAILURE_POLICY
        }) || attempt >= i64::from(max_attempts);
        if blocked {
            block_review_in_transaction(
                &transaction,
                review_id,
                failure_code.as_deref().unwrap_or("research_attempt_limit"),
                now,
            )?;
        } else {
            let not_before = now.saturating_add(crate::retry::retry_backoff_seconds(attempt.max(1)));
            transaction
                .execute(
                    "UPDATE research_reviews
                     SET not_before = CASE WHEN not_before > ?1 THEN not_before ELSE ?1 END,
                         updated_at = ?2
                     WHERE review_id = ?3 AND state = 'retry_wait'",
                    params![not_before, now, review_id],
                )
                .map_err(database_error("schedule bounded research retry"))?;
        }
        transaction
            .commit()
            .map_err(database_error("commit research retry scheduling"))?;
        Ok(blocked)
    }

    pub fn fail_unbound_attempt(
        &self,
        review_id: &str,
        failure_code: &str,
        now: i64,
        max_attempts: u32,
        block_immediately: bool,
    ) -> Result<bool, AppError> {
        if failure_code.is_empty() || failure_code.len() > 128 {
            return Err(validation_error("research.failure_code", "must be bounded"));
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin unbound research failure"))?;
        let attempt: i64 = transaction
            .query_row(
                "SELECT attempt FROM research_reviews
                 WHERE review_id = ?1 AND agent_run_id IS NULL
                   AND state IN ('pending','retry_wait')",
                [review_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("read unbound research failure"))?
            .ok_or_else(|| validation_error("research.review", "is no longer an unbound retry"))?;
        let blocked = block_immediately || attempt >= i64::from(max_attempts);
        if blocked {
            block_review_in_transaction(&transaction, review_id, failure_code, now)?;
        } else {
            transaction
                .execute(
                    "UPDATE research_reviews
                     SET state = 'retry_wait', failure_code = ?1,
                         finished_at = ?2, not_before = ?4, updated_at = ?2
                     WHERE review_id = ?3 AND agent_run_id IS NULL
                       AND state IN ('pending','retry_wait')",
                    params![
                        failure_code,
                        now,
                        review_id,
                        now.saturating_add(crate::retry::retry_backoff_seconds(attempt.max(1))),
                    ],
                )
                .map_err(database_error("record unbound research failure"))?;
        }
        transaction
            .commit()
            .map_err(database_error("commit unbound research failure"))?;
        Ok(blocked)
    }

    /// Reconcile a review whose bound process is terminal but whose native
    /// response never reached the durable `ready` state.  Missing lineage is
    /// treated as corruption and blocks rather than allowing a second owner.
    pub fn block_invalid_lineage(&self, now: i64) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin research lineage validation"))?;
        let mut statement = transaction
            .prepare(
                "SELECT r.review_id, r.state, r.agent_run_id,
                        r.context_json, r.context_digest,
                        campaign.project_id, run.project_id
                 FROM research_reviews AS r
                 JOIN campaigns AS campaign ON campaign.campaign_id = r.campaign_id
                 LEFT JOIN agent_runs AS run ON run.run_id = r.agent_run_id
                 WHERE r.state NOT IN ('ready','completed','discarded','blocked')",
            )
            .map_err(database_error("prepare research lineage validation"))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })
            .map_err(database_error("query research lineage validation"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read research lineage validation"))?;
        drop(statement);
        let mut blocked = 0;
        for (
            review_id,
            state,
            agent_run_id,
            context_json,
            context_digest,
            campaign_project_id,
            run_project_id,
        ) in rows
        {
            let context_valid = match (context_json.as_deref(), context_digest.as_deref()) {
                (None, None) if agent_run_id.is_none() => true,
                (Some(context), Some(digest)) => {
                    digest.len() == 64
                        && digest
                            .chars()
                            .all(|character| character.is_ascii_hexdigit() && !character.is_ascii_uppercase())
                        && format!("{:x}", Sha256::digest(context.as_bytes())) == digest
                }
                _ => false,
            };
            let run_project_matches = run_project_id
                .as_deref()
                .is_some_and(|run_project| run_project == campaign_project_id);
            let valid = match state.as_str() {
                "pending" => agent_run_id.is_none() && context_json.is_none() && context_digest.is_none(),
                "retry_wait" => agent_run_id.is_none() && context_json.is_none() && context_digest.is_none()
                    || agent_run_id.is_some() && context_valid && run_project_matches,
                "running" => agent_run_id.is_some() && context_valid && run_project_matches,
                _ => true,
            };
            if !valid || (agent_run_id.is_some() && !run_project_matches) {
                block_review_in_transaction(
                    &transaction,
                    &review_id,
                    "research_lineage_corrupt",
                    now,
                )?;
                blocked += 1;
            }
        }
        transaction
            .commit()
            .map_err(database_error("commit research lineage validation"))?;
        Ok(blocked)
    }

    pub fn recover_terminal_runs(&self, now: i64) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin persisted research response recovery"))?;
        transaction
            .execute(
                "UPDATE events
                 SET status = 'completed', lease_until = NULL,
                     completed_at = ?1, last_error = NULL
                 WHERE event_id IN (
                     SELECT event_id FROM research_reviews WHERE state = 'ready'
                 ) AND status IN ('in_flight','dispatched')",
                [now],
            )
            .map_err(database_error("complete persisted research response event"))?;
        transaction
            .commit()
            .map_err(database_error("commit persisted research response recovery"))?;

        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT r.review_id, r.agent_run_id, run.status
                 FROM research_reviews AS r
                 LEFT JOIN agent_runs AS run ON run.run_id = r.agent_run_id
                 WHERE r.state = 'running'",
            )
            .map_err(database_error("prepare research terminal recovery"))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(database_error("query research terminal recovery"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read research terminal recovery"))?;
        drop(statement);
        let mut recovered = 0;
        for (review_id, run_id, status) in rows {
            let Some(run_id) = run_id else {
                self.block_review(&review_id, "research_lineage_corrupt", now)?;
                recovered += 1;
                continue;
            };
            if status.is_none() {
                self.block_review(&review_id, "research_lineage_corrupt", now)?;
                recovered += 1;
            } else if !matches!(status.as_deref(), Some("starting" | "running")) {
                let mut connection = self.db.connect()?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(database_error("begin terminal research retry"))?;
                let retry_at = now.saturating_add(crate::retry::retry_backoff_seconds(1));
                let changed = transaction
                    .execute(
                        "UPDATE research_reviews
                         SET state = 'retry_wait', failure_code = 'research_interrupted',
                             finished_at = ?1, not_before = ?1, updated_at = ?1
                         WHERE review_id = ?2 AND state = 'running'
                           AND agent_run_id = ?3",
                        params![now, review_id, run_id],
                    )
                    .map_err(database_error("record terminal research retry"))?;
                transaction
                    .execute(
                        "UPDATE events
                         SET status = 'retry_wait', lease_until = NULL,
                             not_before = ?1, last_error = 'research_interrupted'
                         WHERE event_id = (
                             SELECT event_id FROM research_reviews WHERE review_id = ?2
                         ) AND status IN ('in_flight','dispatched')",
                        params![retry_at, review_id],
                    )
                    .map_err(database_error("requeue terminal research event"))?;
                transaction
                    .commit()
                    .map_err(database_error("commit terminal research retry"))?;
                recovered += changed;
            }
        }
        Ok(recovered)
    }

    pub fn block_review(
        &self,
        review_id: &str,
        reason: &str,
        now: i64,
    ) -> Result<(), AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin research block"))?;
        block_review_in_transaction(&transaction, review_id, reason, now)?;
        transaction
            .commit()
            .map_err(database_error("commit research block"))
    }

    pub fn claim_due(
        &self,
        campaign_id: &str,
        experiment_id: &str,
        task_signature: &str,
        now: i64,
    ) -> Result<Option<ResearchReview>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin research review claim"))?;
        ensure_campaign_in_transaction(&transaction, campaign_id)?;

        let (next_due_at, blocked_reason, session_generation): (Option<i64>, Option<String>, i64) =
            transaction
                .query_row(
                    "SELECT next_due_at, blocked_reason, session_generation
                 FROM campaign_research WHERE campaign_id = ?1",
                    [campaign_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(database_error("read research claim state"))?;
        if blocked_reason.is_some() || next_due_at.is_none_or(|due| due > now) {
            transaction
                .commit()
                .map_err(database_error("commit skipped research review claim"))?;
            return Ok(None);
        }
        if has_open_review(&transaction, campaign_id)? {
            transaction
                .commit()
                .map_err(database_error("commit existing research review claim"))?;
            return Ok(None);
        }
        // The caller's task signature is only a lookup key. The bounded query
        // below first chooses the oldest eligible live candidate, proving the
        // campaign, project, submission, experiment, and observed task
        // identity before a review is created.
        let candidate_query = format!(
            "SELECT c.project_id, e.experiment_id, e.task_signature, e.pueue_task_id
             FROM campaigns c
             JOIN projects p ON p.project_id = c.project_id
             JOIN experiments e ON e.campaign_id = c.campaign_id
             JOIN submissions s
               ON s.submission_id = e.submission_id
              AND s.project_id = c.project_id
             JOIN task_observations observation
               ON observation.project_id = c.project_id
              AND observation.task_signature = e.task_signature
              AND observation.pueue_task_id = e.pueue_task_id
              AND observation.pueue_group = p.pueue_group
              AND lower(observation.state) = 'running'
             WHERE c.campaign_id = ?1
               AND c.state = 'active'
               AND p.enabled = 1
               AND p.paused = 0
               AND p.halted_reason IS NULL
               AND e.status = 'accepted'
               AND e.pueue_task_id IS NOT NULL
               AND s.status = 'accepted'
               AND s.pueue_task_id = e.pueue_task_id
               AND s.task_signature = e.task_signature
               AND NOT EXISTS (
                   SELECT 1 FROM research_reviews ownership
                   WHERE ownership.experiment_id = e.experiment_id
                     AND ownership.operation_stage IN {OPEN_OPERATION_STAGES}
               )
             ORDER BY COALESCE(observation.started_at, observation.first_observed_at),
                      e.experiment_id
             LIMIT ?2"
        );
        let authority = {
            let mut statement = transaction
                .prepare(&candidate_query)
                .map_err(database_error("prepare authoritative research candidates"))?;
            let mut candidates = statement
                .query_map(params![campaign_id, MAX_RESEARCH_CANDIDATES], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                })
                .map_err(database_error("read authoritative research candidates"))?;
            candidates.next().transpose().map_err(database_error(
                "read oldest authoritative research candidate",
            ))?
        };
        let Some((project_id, canonical_experiment_id, canonical_signature, pueue_task_id)) =
            authority
        else {
            transaction
                .commit()
                .map_err(database_error("commit deferred research review claim"))?;
            return Ok(None);
        };
        if canonical_experiment_id != experiment_id || canonical_signature != task_signature {
            transaction
                .commit()
                .map_err(database_error("commit non-owner research review claim"))?;
            return Ok(None);
        }

        let review_id = research_review_id(
            campaign_id,
            &canonical_experiment_id,
            &canonical_signature,
            now,
        );
        let event_dedup_key = format!("campaign-research:v1:{review_id}");
        let payload_json = serde_json::to_string(&json!({
            "source": "campaign_research",
            "campaign_id": campaign_id,
            "experiment_id": experiment_id,
            "task_id": pueue_task_id,
            "task_signature": canonical_signature,
            "review_id": review_id,
        }))
        .map_err(|source| AppError::Serialization {
            operation: "serialize campaign research event",
            source,
        })?;
        transaction
            .execute(
                "INSERT INTO events (
                    project_id, campaign_id, experiment_id, kind, dedup_key,
                    payload_json, status, attempts, not_before, lease_until,
                    created_at, completed_at, last_error
                 ) VALUES (?1, ?2, ?3, 'campaign_research', ?4, ?5,
                           'pending', 0, ?6, NULL, ?6, NULL, NULL)
                 ON CONFLICT(project_id, dedup_key) DO NOTHING",
                params![
                    project_id,
                    campaign_id,
                    experiment_id,
                    event_dedup_key,
                    payload_json,
                    now,
                ],
            )
            .map_err(database_error("insert campaign research event"))?;
        let event_id: i64 = transaction
            .query_row(
                "SELECT event_id FROM events
                 WHERE project_id = ?1 AND dedup_key = ?2
                   AND kind = 'campaign_research'
                   AND campaign_id = ?3 AND experiment_id = ?4
                   AND payload_json = ?5 AND status = 'pending'",
                params![
                    project_id,
                    event_dedup_key,
                    campaign_id,
                    experiment_id,
                    payload_json,
                ],
                |row| row.get(0),
            )
            .map_err(database_error("read campaign research event"))?;
        transaction
            .execute(
                "INSERT INTO research_reviews (
                    review_id, campaign_id, experiment_id, task_signature, attempt,
                    state, operation_stage, agent_run_id, context_json, context_digest,
                    response_json, termination_request_id, successor_experiment_id,
                    evidence_schema_version, session_generation, event_id, not_before,
                    notes_json, failure_code, decision_cycle_id, checkpoint_json,
                    created_at, started_at, finished_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, 0, 'pending', NULL, NULL, NULL, NULL,
                           NULL, NULL, NULL, NULL, ?5, ?6, ?7, NULL, NULL, NULL,
                           NULL, ?7, NULL, NULL, ?7)",
                params![
                    review_id,
                    campaign_id,
                    experiment_id,
                    canonical_signature,
                    session_generation,
                    event_id,
                    now,
                ],
            )
            .map_err(database_error("insert campaign research review"))?;
        transaction
            .execute(
                "UPDATE campaign_research
                 SET next_due_at = NULL, last_review_id = ?1, updated_at = ?2
                 WHERE campaign_id = ?3",
                params![review_id, now, campaign_id],
            )
            .map_err(database_error("advance campaign research state"))?;
        let review = transaction
            .query_row(
                &format!("{REVIEW_SELECT} WHERE review_id = ?1"),
                [review_id.as_str()],
                review_from_row,
            )
            .map_err(database_error("read claimed campaign research review"))?;
        transaction
            .commit()
            .map_err(database_error("commit research review claim"))?;
        Ok(Some(review))
    }

    pub fn find(&self, review_id: &str) -> Result<ResearchReview, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{REVIEW_SELECT} WHERE review_id = ?1"),
                [review_id],
                review_from_row,
            )
            .optional()
            .map_err(database_error("find research review"))?
            .ok_or_else(|| {
                validation_error("review_id", "does not identify a persisted research review")
            })
    }

    pub fn recent(&self, campaign_id: &str, limit: usize) -> Result<Vec<ResearchReview>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{REVIEW_SELECT}
                 WHERE campaign_id = ?1
                 ORDER BY created_at DESC, review_id DESC
                 LIMIT ?2"
            ))
            .map_err(database_error("prepare recent research review query"))?;
        let rows = statement
            .query_map(
                params![
                    campaign_id,
                    limit.min(MAX_RESEARCH_REVIEW_LIST as usize) as i64
                ],
                review_from_row,
            )
            .map_err(database_error("query recent research reviews"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read recent research reviews"))
    }

    pub fn validate_agent_run_reservation(
        &self,
        campaign_id: &str,
        review_id: &str,
        attempt: i64,
        reservation_id: &str,
    ) -> Result<(), AppError> {
        validate_budget_reservation_id(reservation_id)?;
        if attempt < 0 {
            return Err(validation_error("research.attempt", "must be non-negative"));
        }
        let connection = self.db.connect()?;
        if !budget_reservation_matches(
            &connection,
            campaign_id,
            review_id,
            attempt,
            reservation_id,
        )? {
            return Err(validation_error(
                "budget_reservation_id",
                "must be the consumed campaign reservation for this research review attempt",
            ));
        }
        Ok(())
    }

    pub fn prepare_missing_session_reconstruction(
        &self,
        campaign_id: &str,
        project_id: &str,
        review_id: &str,
        prior_session_id: &str,
        prior_session_generation: i64,
    ) -> Result<i64, AppError> {
        validate_session_id(prior_session_id)?;
        if prior_session_generation < 0 {
            return Err(validation_error(
                "research.session_generation",
                "must be non-negative",
            ));
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin research session reconstruction check",
            ))?;
        let current: Option<(Option<String>, i64, String, Option<i64>)> = transaction
            .query_row(
                "SELECT c.session_id, c.session_generation, r.state, r.agent_run_id
                 FROM campaign_research AS c
                 JOIN campaigns AS campaign ON campaign.campaign_id = c.campaign_id
                 JOIN research_reviews AS r ON r.campaign_id = c.campaign_id
                 WHERE c.campaign_id = ?1 AND campaign.project_id = ?2
                   AND r.review_id = ?3",
                params![campaign_id, project_id, review_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(database_error("read research session reconstruction state"))?;
        let Some((current_session_id, current_generation, review_state, review_run_id)) = current
        else {
            return Err(validation_error(
                "research.review",
                "does not identify a review in the requested project and campaign",
            ));
        };
        if current_session_id.as_deref() != Some(prior_session_id)
            || current_generation != prior_session_generation
            || !matches!(review_state.as_str(), "pending" | "retry_wait")
            || review_run_id.is_some()
        {
            return Err(validation_error(
                "research.session",
                "cannot reconstruct a changed or already bound campaign session",
            ));
        }
        if research_has_active_or_unknown_owner(&transaction, campaign_id, review_id)? {
            return Err(validation_error(
                "research.session",
                "cannot reconstruct while a prior research run has active or unknown ownership",
            ));
        }
        let next_generation = current_generation.checked_add(1).ok_or_else(|| {
            validation_error(
                "research.session_generation",
                "cannot advance the session generation",
            )
        })?;
        transaction.commit().map_err(database_error(
            "commit research session reconstruction check",
        ))?;
        Ok(next_generation)
    }

    /// Bind one native research run before its launch gate is released.  The
    /// planned session and exact evidence context are written in the same
    /// transaction as the review's running identity; a successful response
    /// is persisted separately after native completion.
    pub fn bind_agent_run(
        &self,
        binding: &ResearchLaunchBinding,
        agent_run_id: i64,
        project_id: &str,
        now: i64,
    ) -> Result<(), AppError> {
        validate_research_binding(binding)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin research run binding"))?;
        let (
            campaign_id,
            current_experiment_id,
            current_session,
            current_generation,
            current_attempt,
            current_run,
            current_review_generation,
            current_notes,
            ): (String, String, Option<String>, i64, i64, Option<i64>, i64, Option<String>) = transaction
            .query_row(
                "SELECT r.campaign_id, r.experiment_id, c.session_id, c.session_generation,
                        r.attempt, r.agent_run_id, r.session_generation, r.notes_json
                 FROM research_reviews AS r
                 JOIN campaign_research AS c ON c.campaign_id = r.campaign_id
                 JOIN campaigns AS campaign ON campaign.campaign_id = r.campaign_id
                 WHERE r.review_id = ?1 AND campaign.project_id = ?2",
                params![binding.review_id, project_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ))
                },
            )
            .map_err(database_error("read research run binding"))?;
        let active_run: Option<(String, Option<String>)> = transaction
            .query_row(
                "SELECT project_id, execution_kind
                 FROM agent_runs
                 WHERE run_id = ?1 AND status IN ('starting','running')",
                [agent_run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(database_error("read research agent run"))?;
        let Some((run_project_id, execution_kind)) = active_run else {
            return Err(validation_error(
                "agent_run_id",
                "must identify an active agent run in the research project",
            ));
        };
        if run_project_id != project_id {
            return Err(validation_error(
                "agent_run_id",
                "must identify an active agent run in the research project",
            ));
        }
        if execution_kind.as_deref() != Some("campaign_research") {
            return Err(validation_error(
                "execution_kind",
                "must identify a campaign_research agent run",
            ));
        }
        if campaign_id != binding.campaign_id
            || current_experiment_id != binding.experiment_id
            || current_attempt != binding.attempt
            || current_generation != binding.prior_session_generation
            || current_review_generation != binding.prior_session_generation
            || current_run.is_some()
            || current_session != binding.prior_session_id
        {
            return Err(validation_error(
                "research.binding",
                "review, attempt, or campaign session changed before native binding",
            ));
        }
        if !budget_reservation_matches(
            &transaction,
            &campaign_id,
            &binding.review_id,
            binding.attempt,
            &binding.budget_reservation_id,
        )? {
            return Err(validation_error(
                "budget_reservation_id",
                "must be the consumed campaign reservation for this research review attempt",
            ));
        }
        if binding.recovery_reason.is_some()
            && research_has_active_or_unknown_owner(&transaction, &campaign_id, &binding.review_id)?
        {
            return Err(validation_error(
                "research.session",
                "cannot reconstruct while a prior research run has active or unknown ownership",
            ));
        }
        let mut recovery_notes = current_notes
            .as_deref()
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .filter(serde_json::Value::is_object)
            .unwrap_or_else(|| json!({}));
        recovery_notes["session_binding"] = json!("pending");
        recovery_notes["planned_session_id"] = json!(binding.session_id);
        recovery_notes["attempt"] = json!(binding.attempt);
        recovery_notes["budget_reservation_id"] = json!(binding.budget_reservation_id);
        recovery_notes["recovery_reason"] = json!(binding.recovery_reason);
        let recovery_notes = recovery_notes.to_string();
        let changed = transaction
            .execute(
                "UPDATE research_reviews
                 SET state = 'running', agent_run_id = ?1, context_json = ?2,
                     context_digest = ?3, started_at = COALESCE(started_at, ?4),
                     session_generation = ?5, notes_json = ?6,
                     updated_at = ?4
                 WHERE review_id = ?7 AND attempt = ?8
                   AND session_generation = ?9
                   AND agent_run_id IS NULL AND state IN ('pending','retry_wait')",
                params![
                    agent_run_id,
                    binding.context_json,
                    binding.context_digest,
                    now,
                    binding.session_generation,
                    recovery_notes,
                    binding.review_id,
                    binding.attempt,
                    binding.prior_session_generation,
                ],
            )
            .map_err(database_error("bind research review to agent run"))?;
        if changed != 1 {
            return Err(validation_error(
                "research.review",
                "review is no longer claimable for this native run",
            ));
        }
        let changed = transaction
            .execute(
                "UPDATE campaign_research
                 SET session_id = ?1, session_generation = ?2, updated_at = ?3
                 WHERE campaign_id = ?4 AND session_generation = ?5
                   AND ((session_id IS NULL AND ?6 IS NULL) OR session_id = ?6)",
                params![
                    binding.session_id,
                    binding.session_generation,
                    now,
                    campaign_id,
                    binding.prior_session_generation,
                    binding.prior_session_id,
                ],
            )
            .map_err(database_error("persist planned research session"))?;
        if changed != 1 {
            return Err(validation_error(
                "research.session",
                "campaign session changed before native binding",
            ));
        }
        transaction
            .commit()
            .map_err(database_error("commit research run binding"))
    }

    /// Persist a schema-validated research response and the owned session
    /// discovered after a fresh native launch exits.
    pub fn finish_agent_run(
        &self,
        binding: &ResearchLaunchBinding,
        agent_run_id: i64,
        session_id: &str,
        response_json: &str,
        _rebind_session: bool,
        now: i64,
    ) -> Result<(), AppError> {
        validate_research_binding(binding)?;
        validate_session_id(session_id)?;
        if response_json.is_empty() || response_json.len() > crate::research_protocol::MAX_RESEARCH_ANSWER_BYTES {
            return Err(validation_error(
                "research.response_json",
                "must be non-empty and bounded",
            ));
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin research response persistence"))?;
        let (
            current_campaign_id,
            current_experiment_id,
            current_context_json,
            current_context_digest,
            current_session,
            current_generation,
            current_attempt,
            current_run,
            current_review_generation,
            current_state,
        ): (
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            i64,
            i64,
            Option<i64>,
            i64,
            String,
        ) = transaction
            .query_row(
                "SELECT r.campaign_id, r.experiment_id, r.context_json, r.context_digest,
                        c.session_id, c.session_generation, r.attempt, r.agent_run_id,
                        r.session_generation, r.state
                 FROM research_reviews AS r
                 JOIN campaign_research AS c ON c.campaign_id = r.campaign_id
                 WHERE r.review_id = ?1",
                [&binding.review_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                    ))
                },
            )
            .map_err(database_error("read research response binding"))?;
        if current_campaign_id != binding.campaign_id
            || current_experiment_id != binding.experiment_id
            || current_context_json.as_deref() != Some(binding.context_json.as_str())
            || current_context_digest.as_deref() != Some(binding.context_digest.as_str())
            || current_run != Some(agent_run_id)
            || current_attempt != binding.attempt
            || current_generation != binding.session_generation
            || current_review_generation != binding.session_generation
            || current_session.as_deref() != Some(session_id)
            || current_state != "running"
        {
            return Err(validation_error(
                "research.binding",
                "response identity does not match the bound native run",
            ));
        }
        let changed = transaction
            .execute(
                "UPDATE research_reviews
                 SET state = 'ready', response_json = ?1, finished_at = ?2,
                     updated_at = ?2
                 WHERE review_id = ?3 AND campaign_id = ?4 AND experiment_id = ?5
                   AND context_json = ?6 AND context_digest = ?7
                   AND agent_run_id = ?8 AND attempt = ?9
                   AND session_generation = ?10 AND state = 'running'",
                params![
                    response_json,
                    now,
                    binding.review_id,
                    binding.campaign_id,
                    binding.experiment_id,
                    binding.context_json,
                    binding.context_digest,
                    agent_run_id,
                    binding.attempt,
                    binding.session_generation,
                ],
            )
            .map_err(database_error("persist research response"))?;
        if changed != 1 {
            return Err(validation_error(
                "research.review",
                "running review changed before response persistence",
            ));
        }
        transaction
            .execute(
                "UPDATE campaign_research
                 SET session_id = ?1, updated_at = ?2
                 WHERE campaign_id = ?3 AND session_generation = ?4
                   AND session_id = ?5",
                params![
                    session_id,
                    now,
                    binding.campaign_id,
                    binding.session_generation,
                    session_id,
                ],
            )
            .map_err(database_error("persist owned research session"))
            .and_then(|changed| {
                if changed == 1 {
                    Ok(())
                } else {
                    Err(validation_error(
                        "research.session",
                        "campaign session changed before response persistence",
                    ))
                }
            })?;
        transaction
            .commit()
            .map_err(database_error("commit research response persistence"))
    }

    /// Replace the supervisor's fresh-launch nonce with the exact session ID
    /// emitted by that bound child. The immutable review/run/generation
    /// linkage and the pending nonce are the CAS predicate.
    pub fn confirm_agent_run_session(
        &self,
        binding: &ResearchLaunchBinding,
        agent_run_id: i64,
        confirmed_session_id: &str,
        now: i64,
    ) -> Result<(), AppError> {
        validate_research_binding(binding)?;
        validate_session_id(confirmed_session_id)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin research session confirmation"))?;
        let (
            campaign_id,
            experiment_id,
            context_json,
            context_digest,
            current_session,
            current_generation,
            current_attempt,
            current_run,
            review_generation,
            state,
            notes_json,
        ): (
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            i64,
            i64,
            Option<i64>,
            i64,
            String,
            Option<String>,
        ) = transaction
            .query_row(
                "SELECT r.campaign_id, r.experiment_id, r.context_json, r.context_digest,
                        c.session_id, c.session_generation, r.attempt, r.agent_run_id,
                        r.session_generation, r.state, r.notes_json
                 FROM research_reviews AS r
                 JOIN campaign_research AS c ON c.campaign_id = r.campaign_id
                 WHERE r.review_id = ?1",
                [&binding.review_id],
                |row| Ok((
                    row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?,
                    row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?,
                    row.get(8)?, row.get(9)?, row.get(10)?,
                )),
            )
            .map_err(database_error("read research session confirmation"))?;
        if campaign_id != binding.campaign_id
            || experiment_id != binding.experiment_id
            || context_json.as_deref() != Some(binding.context_json.as_str())
            || context_digest.as_deref() != Some(binding.context_digest.as_str())
            || current_run != Some(agent_run_id)
            || current_attempt != binding.attempt
            || current_generation != binding.session_generation
            || review_generation != binding.session_generation
            || state != "running"
        {
            return Err(validation_error(
                "research.binding",
                "session confirmation does not match the bound native run",
            ));
        }
        let mut notes = notes_json
            .as_deref()
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .filter(serde_json::Value::is_object)
            .unwrap_or_else(|| json!({}));
        let already_confirmed = current_session.as_deref() == Some(confirmed_session_id)
            && notes.get("session_binding").and_then(serde_json::Value::as_str)
                == Some("confirmed")
            && notes
                .get("planned_session_id")
                .and_then(serde_json::Value::as_str)
                == Some(binding.session_id.as_str())
            && notes
                .get("confirmed_session_id")
                .and_then(serde_json::Value::as_str)
                == Some(confirmed_session_id);
        if already_confirmed {
            return transaction
                .commit()
                .map_err(database_error("commit idempotent research session confirmation"));
        }
        if current_session.as_deref() != Some(binding.session_id.as_str()) {
            return Err(validation_error(
                "research.binding",
                "session confirmation does not match the pending session nonce",
            ));
        }
        notes["session_binding"] = json!("confirmed");
        notes["planned_session_id"] = json!(binding.session_id);
        notes["confirmed_session_id"] = json!(confirmed_session_id);
        let changed = transaction
            .execute(
                "UPDATE campaign_research
                 SET session_id = ?1, updated_at = ?2
                 WHERE campaign_id = ?3 AND session_generation = ?4
                   AND session_id = ?5",
                params![
                    confirmed_session_id,
                    now,
                    binding.campaign_id,
                    binding.session_generation,
                    binding.session_id,
                ],
            )
            .map_err(database_error("confirm owned research session"))?;
        if changed != 1 {
            return Err(validation_error(
                "research.session",
                "campaign session changed before confirmation",
            ));
        }
        let changed = transaction
            .execute(
                "UPDATE research_reviews
                 SET notes_json = ?1, updated_at = ?2
                 WHERE review_id = ?3 AND campaign_id = ?4 AND experiment_id = ?5
                   AND context_json = ?6 AND context_digest = ?7
                   AND agent_run_id = ?8 AND attempt = ?9
                   AND session_generation = ?10 AND state = 'running'",
                params![
                    notes.to_string(),
                    now,
                    binding.review_id,
                    binding.campaign_id,
                    binding.experiment_id,
                    binding.context_json,
                    binding.context_digest,
                    agent_run_id,
                    binding.attempt,
                    binding.session_generation,
                ],
            )
            .map_err(database_error("persist research session confirmation"))?;
        if changed != 1 {
            return Err(validation_error(
                "research.binding",
                "research review changed before session confirmation",
            ));
        }
        transaction
            .commit()
            .map_err(database_error("commit research session confirmation"))
    }

    pub fn fail_agent_run(
        &self,
        binding: &ResearchLaunchBinding,
        agent_run_id: i64,
        failure_code: &str,
        now: i64,
    ) -> Result<(), AppError> {
        validate_research_binding(binding)?;
        if failure_code.is_empty() || failure_code.len() > 128 {
            return Err(validation_error("research.failure_code", "must be bounded"));
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin failed research run"))?;
        let (
            campaign_id,
            experiment_id,
            context_json,
            context_digest,
            current_session,
            current_generation,
            current_attempt,
            current_run,
            review_generation,
            state,
        ): (
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            i64,
            i64,
            Option<i64>,
            i64,
            String,
        ) = transaction
            .query_row(
                "SELECT r.campaign_id, r.experiment_id, r.context_json, r.context_digest,
                        c.session_id, c.session_generation, r.attempt, r.agent_run_id,
                        r.session_generation, r.state
                 FROM research_reviews AS r
                 JOIN campaign_research AS c ON c.campaign_id = r.campaign_id
                 WHERE r.review_id = ?1",
                [&binding.review_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                    ))
                },
            )
            .map_err(database_error("read failed research run"))?;
        if campaign_id != binding.campaign_id
            || experiment_id != binding.experiment_id
            || context_json.as_deref() != Some(binding.context_json.as_str())
            || context_digest.as_deref() != Some(binding.context_digest.as_str())
            || current_session.as_deref() != Some(binding.session_id.as_str())
            || current_generation != binding.session_generation
            || current_attempt != binding.attempt
            || current_run != Some(agent_run_id)
            || review_generation != binding.session_generation
            || state != "running"
        {
            return Err(validation_error(
                "research.binding",
                "failed research run is no longer the exact bound review",
            ));
        }
        let changed = transaction
            .execute(
                "UPDATE research_reviews
                 SET state = 'retry_wait', failure_code = ?1,
                     finished_at = ?2, updated_at = ?2
                 WHERE review_id = ?3 AND campaign_id = ?4 AND experiment_id = ?5
                   AND context_json = ?6 AND context_digest = ?7
                   AND agent_run_id = ?8 AND attempt = ?9
                   AND session_generation = ?10 AND state = 'running'",
                params![
                    failure_code,
                    now,
                    binding.review_id,
                    binding.campaign_id,
                    binding.experiment_id,
                    binding.context_json,
                    binding.context_digest,
                    agent_run_id,
                    binding.attempt,
                    binding.session_generation,
                ],
            )
            .map_err(database_error("record failed research run"))?;
        if changed != 1 {
            return Err(validation_error(
                "research.review",
                "failed research run is not the bound running review",
            ));
        }
        let retry_at = now.saturating_add(crate::retry::retry_backoff_seconds(binding.attempt));
        transaction
            .execute(
                "UPDATE research_reviews SET not_before = ?1
                 WHERE review_id = ?2 AND agent_run_id = ?3 AND state = 'retry_wait'",
                params![retry_at, binding.review_id, agent_run_id],
            )
            .map_err(database_error("schedule failed research retry"))?;
        transaction
            .commit()
            .map_err(database_error("commit failed research run"))
    }

    /// Set a failed run to retry and retire only its still-pending nonce.
    /// A previously confirmed session is intentionally retained for exact
    /// recovery after a schema or response failure.
    pub fn fail_agent_run_and_clear_session(
        &self,
        binding: &ResearchLaunchBinding,
        agent_run_id: i64,
        confirmed_session_id: Option<&str>,
        failure_code: &str,
        now: i64,
    ) -> Result<(), AppError> {
        validate_research_binding(binding)?;
        if let Some(confirmed_session_id) = confirmed_session_id {
            validate_session_id(confirmed_session_id)?;
        }
        if failure_code.is_empty() || failure_code.len() > 128 {
            return Err(validation_error("research.failure_code", "must be bounded"));
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin failed research session retirement"))?;
        let (
            campaign_id,
            experiment_id,
            context_json,
            context_digest,
            current_session,
            current_generation,
            current_attempt,
            current_run,
            review_generation,
            state,
            notes_json,
        ): (
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            i64,
            i64,
            Option<i64>,
            i64,
            String,
            Option<String>,
        ) = transaction
            .query_row(
                "SELECT r.campaign_id, r.experiment_id, r.context_json, r.context_digest,
                        c.session_id, c.session_generation, r.attempt, r.agent_run_id,
                        r.session_generation, r.state, r.notes_json
                 FROM research_reviews AS r
                 JOIN campaign_research AS c ON c.campaign_id = r.campaign_id
                 WHERE r.review_id = ?1 AND r.agent_run_id = ?2
                   AND r.state = 'running'",
                params![binding.review_id, agent_run_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                    ))
                },
            )
            .map_err(database_error("read failed research session retirement"))?;
        let notes = notes_json
            .as_deref()
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok());
        let confirmed = confirmed_session_id.is_some_and(|confirmed_session_id| {
            current_session.as_deref() == Some(confirmed_session_id)
                && notes
                    .as_ref()
                    .and_then(|notes| notes.get("session_binding"))
                    .and_then(serde_json::Value::as_str)
                    == Some("confirmed")
                && notes
                    .as_ref()
                    .and_then(|notes| notes.get("planned_session_id"))
                    .and_then(serde_json::Value::as_str)
                    == Some(binding.session_id.as_str())
                && notes
                    .as_ref()
                    .and_then(|notes| notes.get("confirmed_session_id"))
                    .and_then(serde_json::Value::as_str)
                    == Some(confirmed_session_id)
        });
        if campaign_id != binding.campaign_id
            || experiment_id != binding.experiment_id
            || context_json.as_deref() != Some(binding.context_json.as_str())
            || context_digest.as_deref() != Some(binding.context_digest.as_str())
            || current_generation != binding.session_generation
            || current_attempt != binding.attempt
            || current_run != Some(agent_run_id)
            || review_generation != binding.session_generation
            || state != "running"
            || (!confirmed && current_session.as_deref() != Some(binding.session_id.as_str()))
        {
            return Err(validation_error(
                "research.binding",
                "failed fresh research run is no longer the exact bound review",
            ));
        }
        let changed = transaction
            .execute(
                "UPDATE research_reviews
                 SET state = 'retry_wait', failure_code = ?1,
                     finished_at = ?2, updated_at = ?2
                 WHERE review_id = ?3 AND campaign_id = ?4 AND experiment_id = ?5
                   AND context_json = ?6 AND context_digest = ?7
                   AND agent_run_id = ?8 AND attempt = ?9
                   AND session_generation = ?10 AND state = 'running'",
                params![
                    failure_code,
                    now,
                    binding.review_id,
                    binding.campaign_id,
                    binding.experiment_id,
                    binding.context_json,
                    binding.context_digest,
                    agent_run_id,
                    binding.attempt,
                    binding.session_generation,
                ],
            )
            .map_err(database_error("record failed research run"))?;
        if changed != 1 {
            return Err(validation_error(
                "research.review",
                "failed research run is not the bound running review",
            ));
        }
        let retry_at = now.saturating_add(crate::retry::retry_backoff_seconds(binding.attempt));
        transaction
            .execute(
                "UPDATE research_reviews SET not_before = ?1
                 WHERE review_id = ?2 AND agent_run_id = ?3 AND state = 'retry_wait'",
                params![retry_at, binding.review_id, agent_run_id],
            )
            .map_err(database_error("schedule failed research session retry"))?;
        transaction
            .execute(
                "UPDATE campaign_research
                 SET session_id = NULL, updated_at = ?1
                 WHERE campaign_id = ?2 AND session_generation = ?3
                   AND session_id = ?4",
                params![now, binding.campaign_id, binding.session_generation, binding.session_id],
            )
            .map_err(database_error("retire pending research session"))
            .and_then(|cleared| {
                if confirmed || cleared == 1 {
                    Ok(())
                } else {
                    Err(validation_error(
                        "research.session",
                        "pending research session changed before retirement",
                    ))
                }
            })?;
        transaction
            .commit()
            .map_err(database_error("commit failed research session retirement"))
    }

    pub fn owns_successor(&self, experiment_id: &str) -> Result<bool, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM research_reviews
                     WHERE experiment_id = ?1
                       AND successor_experiment_id IS NOT NULL
                 )",
                [experiment_id],
                |row| row.get(0),
            )
            .map_err(database_error("check research successor ownership"))
    }
}

fn ensure_campaign_in_transaction(
    transaction: &Transaction<'_>,
    campaign_id: &str,
) -> Result<(), AppError> {
    let updated_at: Option<i64> = transaction
        .query_row(
            "SELECT updated_at FROM campaigns WHERE campaign_id = ?1",
            [campaign_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(database_error("check research campaign before creation"))?;
    let Some(updated_at) = updated_at else {
        return Err(validation_error(
            "campaign_id",
            "does not identify a persisted campaign",
        ));
    };
    transaction
        .execute(
            "INSERT INTO campaign_research (
                campaign_id, session_id, session_generation, next_due_at,
                blocked_reason, last_review_id, updated_at
             ) VALUES (?1, NULL, 0, NULL, NULL, NULL, ?2)
             ON CONFLICT(campaign_id) DO NOTHING",
            params![campaign_id, updated_at],
        )
        .map_err(database_error("ensure campaign research state"))?;
    Ok(())
}

fn has_open_review(transaction: &Transaction<'_>, campaign_id: &str) -> Result<bool, AppError> {
    transaction
        .query_row(
            &format!(
                "SELECT EXISTS(
                     SELECT 1 FROM research_reviews
                     WHERE campaign_id = ?1 AND state IN {OPEN_REVIEW_STATES}
                 )"
            ),
            [campaign_id],
            |row| row.get(0),
        )
        .map_err(database_error("check open campaign research review"))
}

fn block_review_in_transaction(
    transaction: &Transaction<'_>,
    review_id: &str,
    reason: &str,
    now: i64,
) -> Result<(), AppError> {
    if reason.is_empty() || reason.len() > 128 || reason.chars().any(char::is_control) {
        return Err(validation_error("research.blocked_reason", "must be bounded"));
    }
    let changed = transaction
        .execute(
            "UPDATE research_reviews
             SET state = 'blocked', failure_code = ?1, finished_at = ?2,
                 not_before = ?2, updated_at = ?2
             WHERE review_id = ?3 AND state <> 'ready'",
            params![reason, now, review_id],
        )
        .map_err(database_error("block research review"))?;
    if changed != 1 {
        return Err(validation_error(
            "research.review",
            "cannot block a missing or already terminal review",
        ));
    }
    transaction
        .execute(
            "UPDATE campaign_research
             SET blocked_reason = ?1, next_due_at = NULL, updated_at = ?2
             WHERE campaign_id = (
                 SELECT campaign_id FROM research_reviews WHERE review_id = ?3
             )",
            params![reason, now, review_id],
        )
        .map_err(database_error("block research campaign"))?;
    Ok(())
}

pub fn next_research_due(start: i64, interval_minutes: u32) -> Result<Option<i64>, AppError> {
    if interval_minutes == 0 {
        return Ok(None);
    }
    start
        .checked_add(i64::from(interval_minutes) * 60)
        .map(Some)
        .ok_or_else(|| validation_error("research.next_due_at", "timestamp overflow"))
}

fn research_review_id(
    campaign_id: &str,
    experiment_id: &str,
    task_signature: &str,
    now: i64,
) -> String {
    let mut digest = Sha256::new();
    digest.update(campaign_id.as_bytes());
    digest.update([0]);
    digest.update(experiment_id.as_bytes());
    digest.update([0]);
    digest.update(task_signature.as_bytes());
    digest.update([0]);
    digest.update(now.to_le_bytes());
    format!("research-review:v1:{:x}", digest.finalize())
}

fn review_from_row(row: &Row<'_>) -> rusqlite::Result<ResearchReview> {
    Ok(ResearchReview {
        review_id: row.get(0)?,
        campaign_id: row.get(1)?,
        experiment_id: row.get(2)?,
        task_signature: row.get(3)?,
        attempt: row.get(4)?,
        state: row.get(5)?,
        operation_stage: row.get(6)?,
        agent_run_id: row.get(7)?,
        context_json: row.get(8)?,
        context_digest: row.get(9)?,
        response_json: row.get(10)?,
        termination_request_id: row.get(11)?,
        successor_experiment_id: row.get(12)?,
        session_generation: row.get(14)?,
    })
}

fn validation_error(field: &'static str, message: &'static str) -> AppError {
    AppError::Validation { field, message }
}

fn validate_research_binding(binding: &ResearchLaunchBinding) -> Result<(), AppError> {
    validate_session_id(&binding.session_id)?;
    if let Some(prior_session_id) = binding.prior_session_id.as_deref() {
        validate_session_id(prior_session_id)?;
    }
    if [
        binding.review_id.as_str(),
        binding.campaign_id.as_str(),
        binding.experiment_id.as_str(),
    ]
    .into_iter()
    .any(|value| value.is_empty() || value.len() > 256 || value.chars().any(char::is_control))
        || binding.attempt < 0
        || binding.session_generation < 0
        || binding.prior_session_generation < 0
    {
        return Err(validation_error(
            "research.binding",
            "contains an invalid review, attempt, or generation",
        ));
    }
    if binding.recovery_reason.is_some() {
        let Some(reason) = binding.recovery_reason.as_deref() else {
            unreachable!();
        };
        if binding.prior_session_id.is_none()
            || binding.session_generation <= binding.prior_session_generation
            || reason.is_empty()
            || reason.len() > 128
            || !reason.chars().all(|character| {
                character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
            })
        {
            return Err(validation_error(
                "research.recovery_reason",
                "must identify a bounded session reconstruction",
            ));
        }
    } else if binding.session_generation != binding.prior_session_generation {
        return Err(validation_error(
            "research.session_generation",
            "must remain unchanged without a recovery reason",
        ));
    }
    validate_budget_reservation_id(&binding.budget_reservation_id)?;
    if binding.context_json.is_empty()
        || binding.context_json.len() > crate::research_evidence::MAX_RESEARCH_CONTEXT_BYTES
    {
        return Err(validation_error(
            "research.context_json",
            "must be non-empty and bounded",
        ));
    }
    if binding.context_digest.len() != 64
        || !binding.context_digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        || format!("{:x}", Sha256::digest(binding.context_json.as_bytes())) != binding.context_digest
    {
        return Err(validation_error(
            "research.context_digest",
            "must match the exact context JSON",
        ));
    }
    Ok(())
}

fn validate_budget_reservation_id(reservation_id: &str) -> Result<(), AppError> {
    if reservation_id.is_empty()
        || reservation_id.len() > 256
        || reservation_id.chars().any(char::is_control)
    {
        return Err(validation_error(
            "budget_reservation_id",
            "must be non-empty, bounded, and contain no control characters",
        ));
    }
    Ok(())
}

fn budget_reservation_matches(
    connection: &Connection,
    campaign_id: &str,
    review_id: &str,
    attempt: i64,
    reservation_id: &str,
) -> Result<bool, AppError> {
    let subject_key = format!("research:{review_id}:attempt:{attempt}");
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM budget_reservations
                 WHERE reservation_id = ?1 AND campaign_id = ?2
                   AND experiment_id IS NULL AND dimension = 'agent_run'
                   AND subject_key = ?3 AND status = 'consumed'
             )",
            params![reservation_id, campaign_id, subject_key],
            |row| row.get(0),
        )
        .map_err(database_error("validate research budget reservation"))
}

fn research_has_active_or_unknown_owner(
    transaction: &Transaction<'_>,
    campaign_id: &str,
    current_review_id: &str,
) -> Result<bool, AppError> {
    transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1
                 FROM research_reviews AS prior
                 LEFT JOIN agent_runs AS run ON run.run_id = prior.agent_run_id
                 WHERE prior.campaign_id = ?1 AND prior.review_id <> ?2
                   AND (
                       prior.state IN ('running','ready')
                       OR (
                           prior.agent_run_id IS NOT NULL
                           AND (run.run_id IS NULL OR run.status NOT IN (
                               'completed','failed','timed_out','cancelled'
                           ))
                       )
                   )
             )",
            params![campaign_id, current_review_id],
            |row| row.get(0),
        )
        .map_err(database_error("check research session owner"))
}

fn validate_session_id(session_id: &str) -> Result<(), AppError> {
    crate::codex_session::normalize_session_id(session_id).map(|_| ())
}
