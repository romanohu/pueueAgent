use std::{
    collections::BTreeSet,
    path::PathBuf,
    time::{Duration, Instant},
};

use rusqlite::{
    params,
    types::Type,
    Connection, OptionalExtension, Row, Transaction, TransactionBehavior,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    environment::{
        PrivateRunTempRecoveryIdentityV1, PrivateRunTempRecoveryRootIdentity,
        PrivateRunTempRecoveryTempIdentity,
        RecoveredPrivateRunTempCleanup,
    },
    models::{EventStatus, TaskObservation},
    reconcile::managed_task_run_signature_for_observation,
    AppError,
};

use super::{database_error, Db};

const MAX_RESEARCH_REVIEW_LIST: i64 = 32;
const MAX_RESEARCH_CANDIDATES: i64 = 32;
const OPEN_REVIEW_STATES: &str = "('pending','running','ready','retry_wait')";
const OPEN_OPERATION_STAGES: &str =
    "('intent','stop_requested','stop_confirmed','successor_reserved')";
const RESEARCH_RETRY_FAILURE_UNSAFE: &str = "research_session_unsafe";
const RESEARCH_RETRY_FAILURE_POLICY: &str = "research_policy_blocked";

#[derive(Debug)]
struct RunningResearchCandidate {
    campaign_id: String,
    experiment_id: String,
    managed_signature: String,
    pueue_task_id: i64,
    expected_group: String,
    order_at: i64,
    observation: TaskObservation,
}

impl RunningResearchCandidate {
    fn has_managed_identity(&self) -> bool {
        managed_task_run_signature_for_observation(&self.observation, &self.expected_group)
            .as_deref()
            == Some(self.managed_signature.as_str())
    }
}

fn running_research_candidate_from_row(row: &Row<'_>) -> rusqlite::Result<RunningResearchCandidate> {
    let campaign_id = row.get(0)?;
    let experiment_id = row.get(1)?;
    let managed_signature = row.get(2)?;
    let pueue_task_id = row.get(3)?;
    let expected_group = row.get(4)?;
    let order_at = row.get(5)?;
    let project_id: String = row.get(6)?;
    let task_signature = row.get(7)?;
    let observed_pueue_task_id = row.get(8)?;
    let pueue_group = row.get(9)?;
    let command_json: String = row.get(10)?;
    let command = serde_json::from_str(&command_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(10, Type::Text, Box::new(source))
    })?;
    Ok(RunningResearchCandidate {
        campaign_id,
        experiment_id,
        managed_signature,
        pueue_task_id,
        expected_group,
        order_at,
        observation: TaskObservation {
            project_id,
            task_signature,
            pueue_task_id: observed_pueue_task_id,
            pueue_group,
            command,
            state: row.get(11)?,
            enqueued_at: row.get(12)?,
            started_at: row.get(13)?,
            ended_at: row.get(14)?,
            result: row.get(15)?,
            observed_at: row.get(16)?,
        },
    })
}

enum RetryFailureExpectation<'a> {
    Any,
    Missing,
    Exact(Option<&'a str>),
}

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

    /// Return every project with a bound native research owner whose
    /// immutable authority is not an exact cleanup-complete proof.  This is
    /// observation only; startup ownership is adopted separately.
    pub(crate) fn native_cleanup_blocked_project_ids(
        &self,
    ) -> Result<BTreeSet<String>, AppError> {
        let connection = self.db.connect()?;
        native_cleanup_blocked_project_ids(&connection)
    }

    /// Transaction-scoped durable owner gate.  The insertion transaction uses
    /// this directly so terminal cleanup and a new generation cannot race
    /// between separate connections.
    pub(crate) fn project_has_unresolved_native_research_owner(
        connection: &Connection,
        project_id: &str,
    ) -> Result<bool, AppError> {
        project_has_unresolved_native_research_owner(connection, project_id)
    }

    pub(crate) fn startup_native_owner(
        &self,
        run_id: i64,
        marker_absent: bool,
    ) -> Result<Option<StartupResearchOwner>, AppError> {
        let connection = self.db.connect()?;
        if let Some(row) = native_research_owner_rows(&connection, None)?
            .into_iter()
            .find(|row| row.agent_run_id == run_id)
        {
            if native_research_owner_is_complete(&row) {
                return Ok(None);
            }
            return Ok(Some(startup_research_owner_from_row(row, marker_absent)));
        }
        let unknown = connection
            .query_row(
                "SELECT project_id, status, launch_gate_state, pid, log_path,
                        policy_code, failure_stage
                 FROM agent_runs
                 WHERE run_id = ?1 AND execution_kind = 'campaign_research'",
                [run_id],
                |row| {
                    Ok(StartupResearchOwner {
                        run_id,
                        project_id: row.get(0)?,
                        review_id: String::new(),
                        pid: row.get(3)?,
                        status: row.get(1)?,
                        gate_state: row.get(2)?,
                        policy_code: row.get(5)?,
                        failure_stage: row.get(6)?,
                        log_path: row.get::<_, Option<String>>(4)?.map(PathBuf::from),
                        review_state: String::new(),
                        failure_code: None,
                        notes_json: None,
                        marker_absent,
                        authority: None,
                        original_status: row.get(1)?,
                        original_gate_state: row.get(2)?,
                        original_policy_code: row.get(5)?,
                        original_failure_stage: row.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(database_error("read unbound native research startup owner"))?;
        Ok(unknown)
    }

    pub(crate) fn list_native_cleanup_pending_terminal_runs(
        &self,
    ) -> Result<Vec<StartupResearchOwner>, AppError> {
        let connection = self.db.connect()?;
        Ok(native_research_owner_rows(&connection, None)?
            .into_iter()
            .filter(|row| {
                matches!(
                    row.owner_status.as_deref(),
                    Some("completed" | "failed" | "timed_out" | "cancelled")
                ) && !native_research_owner_is_complete(row)
            })
            .map(|row| startup_research_owner_from_row(row, false))
            .collect())
    }

    pub(crate) fn mark_startup_recovery_required(
        &self,
        run_id: i64,
        now: i64,
    ) -> Result<bool, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin native research recovery block"))?;
        let changed = transaction
            .execute(
                "UPDATE campaign_research
                 SET blocked_reason = 'research_recovery_required',
                     next_due_at = NULL, updated_at = ?1
                 WHERE campaign_id = (
                     SELECT review.campaign_id
                     FROM research_reviews AS review
                     WHERE review.agent_run_id = ?2
                 )
                   AND (blocked_reason IS NULL
                        OR blocked_reason = 'research_recovery_required')",
                params![now, run_id],
            )
            .map_err(database_error("block native research recovery owner"))?;
        transaction
            .commit()
            .map_err(database_error("commit native research recovery block"))?;
        Ok(changed != 0)
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
                "SELECT c.campaign_id, e.experiment_id, e.task_signature,
                        e.pueue_task_id, p.pueue_group,
                        COALESCE(observation.started_at,
                                 observation.first_observed_at),
                        c.project_id, observation.task_signature,
                        observation.pueue_task_id, observation.pueue_group,
                        observation.command_json, observation.state,
                        observation.enqueued_at, observation.started_at,
                        observation.ended_at, observation.result,
                        observation.observed_at
                 FROM campaigns AS c
                 JOIN projects AS p ON p.project_id = c.project_id
                 JOIN experiments AS e ON e.campaign_id = c.campaign_id
                 JOIN submissions AS s
                   ON s.submission_id = e.submission_id
                  AND s.project_id = c.project_id
                 JOIN task_observations AS observation
                   ON observation.project_id = c.project_id
                  AND observation.pueue_task_id = e.pueue_task_id
                  AND observation.pueue_group = p.pueue_group
                  AND lower(observation.state) = 'running'
                  AND NOT EXISTS (
                      SELECT 1 FROM task_observations AS newer_observation
                      WHERE newer_observation.project_id = observation.project_id
                        AND newer_observation.pueue_task_id = observation.pueue_task_id
                        AND newer_observation.observed_at > observation.observed_at
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM task_observations AS tied_observation
                      WHERE tied_observation.project_id = observation.project_id
                        AND tied_observation.pueue_task_id = observation.pueue_task_id
                        AND tied_observation.observed_at = observation.observed_at
                        AND tied_observation.task_signature <> observation.task_signature
                  )
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
                 ORDER BY COALESCE(observation.started_at,
                                   observation.first_observed_at),
                          c.campaign_id, e.experiment_id,
                          observation.observed_at DESC,
                          observation.task_signature DESC",
            )
            .map_err(database_error("prepare running research campaign query"))?;
        let candidates = statement
            .query_map([], running_research_candidate_from_row)
            .map_err(database_error("query running research campaigns"))?;
        let mut accepted = Vec::new();
        let mut seen_campaigns = BTreeSet::new();
        for candidate in candidates {
            let candidate = candidate
                .map_err(database_error("read running research campaign candidate"))?;
            if candidate.has_managed_identity()
                && seen_campaigns.insert(candidate.campaign_id.clone())
            {
                accepted.push((candidate.campaign_id, candidate.order_at));
                if accepted.len() >= limit.min(MAX_RESEARCH_CANDIDATES as usize) {
                    break;
                }
            }
        }
        drop(statement);
        let mut scheduled = 0;
        for (campaign_id, order_at) in accepted {
            self.schedule_running(&campaign_id, order_at, interval_minutes, now)?;
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
                             AND owner.project_id = campaign.project_id
                             AND owner.execution_kind = 'campaign_research'
                       )
                   )
                   AND (
                       review.agent_run_id IS NULL
                       OR (
                           json_valid(review.notes_json) = 1
                           AND json_extract(review.notes_json, '$.native_recovery.cleanup.phase') = 'complete'
                           AND json_extract(review.notes_json, '$.native_recovery.version') = 1
                           AND json_extract(review.notes_json, '$.native_recovery.run_id') = review.agent_run_id
                           AND json_extract(review.notes_json, '$.native_recovery.review_id') = review.review_id
                           AND json_extract(review.notes_json, '$.native_recovery.campaign_id') = review.campaign_id
                           AND json_extract(review.notes_json, '$.native_recovery.experiment_id') = review.experiment_id
                           AND json_extract(review.notes_json, '$.native_recovery.attempt') = review.attempt
                           AND json_extract(review.notes_json, '$.native_recovery.session_generation') = review.session_generation
                           AND json_type(review.notes_json, '$.native_recovery.session_id') = 'text'
                           AND json_extract(review.notes_json, '$.native_recovery.session_id') <> ''
                           AND research_state.session_generation = review.session_generation
                           AND (
                               (
                                   json_extract(review.notes_json, '$.native_recovery.fresh_launch') = 0
                                   AND research_state.session_id = json_extract(review.notes_json, '$.native_recovery.session_id')
                               )
                               OR (
                                   json_extract(review.notes_json, '$.native_recovery.fresh_launch') = 1
                                   AND (
                                       (
                                           research_state.session_id IS NOT NULL
                                           AND json_extract(review.notes_json, '$.session_binding') = 'confirmed'
                                           AND json_extract(review.notes_json, '$.planned_session_id') = json_extract(review.notes_json, '$.native_recovery.session_id')
                                           AND json_extract(review.notes_json, '$.confirmed_session_id') = research_state.session_id
                                       )
                                       OR (
                                           research_state.session_id IS NULL
                                           AND review.state IN ('retry_wait','blocked')
                                           AND review.failure_code IS NOT NULL
                                           AND json_extract(review.notes_json, '$.confirmed_session_id') IS NULL
                                           AND json_extract(review.notes_json, '$.session_binding') = 'pending'
                                           AND json_extract(review.notes_json, '$.planned_session_id') = json_extract(review.notes_json, '$.native_recovery.session_id')
                                       )
                                   )
                               )
                           )
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
                "SELECT c.campaign_id, e.experiment_id, e.task_signature,
                        e.pueue_task_id, p.pueue_group,
                        COALESCE(observation.started_at,
                                 observation.first_observed_at),
                        c.project_id, observation.task_signature,
                        observation.pueue_task_id, observation.pueue_group,
                        observation.command_json, observation.state,
                        observation.enqueued_at, observation.started_at,
                        observation.ended_at, observation.result,
                        observation.observed_at
                 FROM campaign_research AS state
                 JOIN campaigns AS c ON c.campaign_id = state.campaign_id
                 JOIN projects AS p ON p.project_id = c.project_id
                 JOIN experiments AS e ON e.campaign_id = c.campaign_id
                 JOIN submissions AS s
                   ON s.submission_id = e.submission_id
                  AND s.project_id = c.project_id
                 JOIN task_observations AS observation
                   ON observation.project_id = c.project_id
                  AND observation.pueue_task_id = e.pueue_task_id
                  AND observation.pueue_group = p.pueue_group
                  AND lower(observation.state) = 'running'
                  AND NOT EXISTS (
                      SELECT 1 FROM task_observations AS newer_observation
                      WHERE newer_observation.project_id = observation.project_id
                        AND newer_observation.pueue_task_id = observation.pueue_task_id
                        AND newer_observation.observed_at > observation.observed_at
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM task_observations AS tied_observation
                      WHERE tied_observation.project_id = observation.project_id
                        AND tied_observation.pueue_task_id = observation.pueue_task_id
                        AND tied_observation.observed_at = observation.observed_at
                        AND tied_observation.task_signature <> observation.task_signature
                  )
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
                          c.campaign_id, e.experiment_id,
                          observation.observed_at DESC,
                          observation.task_signature DESC",
            )
            .map_err(database_error("prepare due research campaign claims"))?;
        let rows = statement
            .query_map([now], running_research_candidate_from_row)
            .map_err(database_error("query due research campaign claims"))?;
        let mut accepted = Vec::new();
        let mut seen_campaigns = BTreeSet::new();
        for candidate in rows {
            let candidate = candidate
                .map_err(database_error("read due research campaign candidate"))?;
            if candidate.has_managed_identity()
                && seen_campaigns.insert(candidate.campaign_id.clone())
            {
                accepted.push((
                    candidate.campaign_id,
                    candidate.experiment_id,
                    candidate.managed_signature,
                ));
                if accepted.len() >= limit.min(MAX_RESEARCH_CANDIDATES as usize) {
                    break;
                }
            }
        }
        drop(statement);
        let mut reviews = Vec::new();
        for (campaign_id, experiment_id, managed_signature) in accepted {
            if let Some(review) = self.claim_due(
                &campaign_id,
                &experiment_id,
                &managed_signature,
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

    pub fn review_failure_code(&self, review_id: &str) -> Result<Option<String>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT failure_code FROM research_reviews WHERE review_id = ?1",
                [review_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("read research review failure"))
            .map(|failure_code| failure_code.flatten())
    }

    pub fn retry_failure_code(&self, review_id: &str) -> Result<Option<String>, AppError> {
        let failure_code = self.review_failure_code(review_id)?;
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
        let Some((
            campaign_id,
            experiment_id,
            state,
            attempt,
            session_generation,
            agent_run_id,
            notes_json,
            failure_code,
            campaign_session,
            campaign_generation,
            campaign_project_id,
            owner_project_id,
            owner_execution_kind,
        )) = connection
            .query_row(
                "SELECT review.campaign_id, review.experiment_id, review.state,
                        review.attempt, review.session_generation,
                        review.agent_run_id, review.notes_json, review.failure_code,
                        research_state.session_id, research_state.session_generation,
                        campaign.project_id, owner.project_id, owner.execution_kind
                 FROM research_reviews AS review
                 JOIN campaign_research AS research_state
                   ON research_state.campaign_id = review.campaign_id
                 JOIN campaigns AS campaign
                   ON campaign.campaign_id = review.campaign_id
                 LEFT JOIN agent_runs AS owner
                   ON owner.run_id = review.agent_run_id
                 WHERE review.review_id = ?1",
                [review_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, i64>(9)?,
                        row.get::<_, String>(10)?,
                        row.get::<_, Option<String>>(11)?,
                        row.get::<_, Option<String>>(12)?,
                    ))
                },
            )
            .optional()
            .map_err(database_error("read research retry owner"))?
        else {
            return Err(validation_error(
                "review_id",
                "does not identify a persisted research review",
            ));
        };
        let Some(agent_run_id) = agent_run_id else {
            return Ok(true);
        };
        if state != "retry_wait" {
            return Ok(false);
        }
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
        ) && matches!(gate_state.as_str(), "released" | "failed")
            && campaign_generation == session_generation
            && owner_project_id.as_deref() == Some(campaign_project_id.as_str())
            && owner_execution_kind.as_deref() == Some("campaign_research")
            && native_recovery_cleanup_complete(
                notes_json.as_deref(),
                &NativeRecoveryCleanupExpectation {
                    review_id,
                    campaign_id: &campaign_id,
                    experiment_id: &experiment_id,
                    attempt,
                    session_generation,
                    agent_run_id,
                    state: &state,
                    failure_code: failure_code.as_deref(),
                    campaign_session: campaign_session.as_deref(),
                },
            ))
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
        self.settle_retry_block(
            review_id,
            expected_state,
            expected_attempt,
            expected_run_id,
            expected_event_status,
            "research_attempt_limit",
            RetryFailureExpectation::Any,
            Some(max_attempts),
            now,
        )
    }

    /// Atomically settle a retry blocked by a research session or policy
    /// safety failure.  The review, campaign state, and linked event are
    /// committed together, so a stale or partially failing coordinator cannot
    /// leave a retry permanently claimable or consume a second budget slot.
    pub fn settle_retry_failure(
        &self,
        review_id: &str,
        expected_state: &str,
        expected_attempt: i64,
        expected_run_id: Option<i64>,
        expected_event_status: EventStatus,
        failure_code: &str,
        now: i64,
    ) -> Result<bool, AppError> {
        if !matches!(failure_code, RESEARCH_RETRY_FAILURE_UNSAFE | RESEARCH_RETRY_FAILURE_POLICY) {
            return Err(validation_error(
                "research.failure_code",
                "retry settlement requires a session or policy safety failure",
            ));
        }
        if expected_state != "retry_wait" {
            return Err(validation_error(
                "research.state",
                "persisted safety retry settlement requires retry_wait",
            ));
        }
        self.settle_retry_block(
            review_id,
            expected_state,
            expected_attempt,
            expected_run_id,
            expected_event_status,
            failure_code,
            RetryFailureExpectation::Exact(Some(failure_code)),
            None,
            now,
        )
    }

    /// Atomically settle a pre-admission safety failure.  The caller supplies
    /// the exact failure code observed before claiming the event, so a stale
    /// retry cannot overwrite a newer ordinary failure.
    pub fn settle_pre_admission_failure(
        &self,
        review_id: &str,
        expected_state: &str,
        expected_attempt: i64,
        expected_run_id: Option<i64>,
        expected_event_status: EventStatus,
        expected_failure_code: Option<&str>,
        failure_code: &str,
        now: i64,
    ) -> Result<bool, AppError> {
        if !matches!(failure_code, RESEARCH_RETRY_FAILURE_UNSAFE | RESEARCH_RETRY_FAILURE_POLICY) {
            return Err(validation_error(
                "research.failure_code",
                "retry settlement requires a session or policy safety failure",
            ));
        }
        if !matches!(expected_state, "pending" | "retry_wait")
            || expected_event_status != EventStatus::Claimed
        {
            return Err(validation_error(
                "research.state",
                "pre-admission safety settlement requires a pending or retry_wait review and claimed event",
            ));
        }
        self.settle_retry_block(
            review_id,
            expected_state,
            expected_attempt,
            expected_run_id,
            expected_event_status,
            failure_code,
            RetryFailureExpectation::Exact(expected_failure_code),
            None,
            now,
        )
    }

    /// Atomically settle an unbound first-attempt safety failure.  The
    /// expected review must still be pending, have no owner or prior failure,
    /// and have a claimed linked event.
    pub fn settle_unbound_failure(
        &self,
        review_id: &str,
        expected_state: &str,
        expected_attempt: i64,
        expected_run_id: Option<i64>,
        expected_event_status: EventStatus,
        failure_code: &str,
        now: i64,
    ) -> Result<bool, AppError> {
        if !matches!(failure_code, RESEARCH_RETRY_FAILURE_UNSAFE | RESEARCH_RETRY_FAILURE_POLICY) {
            return Err(validation_error(
                "research.failure_code",
                "retry settlement requires a session or policy safety failure",
            ));
        }
        if expected_state != "pending"
            || expected_run_id.is_some()
            || expected_event_status != EventStatus::Claimed
        {
            return Err(validation_error(
                "research.state",
                "unbound safety settlement requires a pending review with no owner and claimed event",
            ));
        }
        self.settle_retry_block(
            review_id,
            expected_state,
            expected_attempt,
            expected_run_id,
            expected_event_status,
            failure_code,
            RetryFailureExpectation::Missing,
            None,
            now,
        )
    }

    fn settle_retry_block(
        &self,
        review_id: &str,
        expected_state: &str,
        expected_attempt: i64,
        expected_run_id: Option<i64>,
        expected_event_status: EventStatus,
        failure_code: &str,
        failure_expectation: RetryFailureExpectation<'_>,
        max_attempts: Option<u32>,
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
                "research retry settlement requires pending or retry_wait",
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
                "research retry settlement requires a claimable or terminal event",
            ));
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin research retry settlement"))?;
        let current: Option<(
            String,
            String,
            String,
            i64,
            Option<i64>,
            i64,
            Option<String>,
            Option<String>,
            Option<String>,
            i64,
            String,
            Option<String>,
            Option<String>,
            i64,
            EventStatus,
            Option<String>,
            Option<String>,
            Option<String>,
        )> = transaction
            .query_row(
                "SELECT review.campaign_id, review.experiment_id, review.state,
                        review.attempt, review.agent_run_id, review.session_generation,
                        review.failure_code, review.notes_json,
                        research_state.session_id, research_state.session_generation,
                        campaign.project_id, owner.project_id, owner.execution_kind,
                        event.event_id,
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
                        row.get(10)?,
                        row.get(11)?,
                        row.get(12)?,
                        row.get(13)?,
                        row.get(14)?,
                        row.get(15)?,
                        row.get(16)?,
                        row.get(17)?,
                    ))
                },
            )
            .optional()
            .map_err(database_error("read research retry settlement"))?;
        let Some((
            campaign_id,
            experiment_id,
            state,
            attempt,
            run_id,
            review_generation,
            current_failure_code,
            notes_json,
            campaign_session,
            campaign_generation,
            campaign_project_id,
            owner_project_id,
            owner_execution_kind,
            event_id,
            event_status,
            owner_status,
            owner_gate,
            campaign_blocked_reason,
        )) = current
        else {
            transaction
                .commit()
                .map_err(database_error("commit missing research retry settlement"))?;
            return Ok(false);
        };
        if state != expected_state
            || attempt != expected_attempt
            || run_id != expected_run_id
            || event_status != expected_event_status
            || campaign_blocked_reason.is_some()
            || !match failure_expectation {
                RetryFailureExpectation::Any => true,
                RetryFailureExpectation::Missing => current_failure_code.is_none(),
                RetryFailureExpectation::Exact(expected) => {
                    current_failure_code.as_deref() == expected
                }
            }
        {
            transaction
                .commit()
                .map_err(database_error("commit stale research retry settlement"))?;
            return Ok(false);
        }
        if run_id.is_some() {
            if !matches!(
                owner_status.as_deref(),
                Some("completed" | "failed" | "timed_out" | "cancelled")
            ) || !matches!(owner_gate.as_deref(), Some("released" | "failed"))
                || review_generation != campaign_generation
                || owner_project_id.as_deref() != Some(campaign_project_id.as_str())
                || owner_execution_kind.as_deref() != Some("campaign_research")
                || !native_recovery_cleanup_complete(
                    notes_json.as_deref(),
                    &NativeRecoveryCleanupExpectation {
                        review_id,
                        campaign_id: &campaign_id,
                        experiment_id: &experiment_id,
                        attempt,
                        session_generation: review_generation,
                        agent_run_id: run_id.expect("run id checked above"),
                        state: &state,
                        failure_code: current_failure_code.as_deref(),
                        campaign_session: campaign_session.as_deref(),
                    },
                )
            {
                transaction
                    .commit()
                    .map_err(database_error("commit active research retry owner"))?;
                return Ok(false);
            }
        }
        if let Some(max_attempts) = max_attempts {
            let target_attempt = if attempt > 0 && run_id.is_none() && current_failure_code.is_none() {
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
        }
        let changed = transaction
            .execute(
                "UPDATE research_reviews
                 SET state = 'blocked', failure_code = ?1,
                     finished_at = ?2, not_before = ?2, updated_at = ?2
                 WHERE review_id = ?3 AND state = ?4 AND attempt = ?5
                   AND ((agent_run_id IS NULL AND ?6 IS NULL) OR agent_run_id = ?6)
                   AND event_id = ?7",
                params![
                    failure_code,
                    now,
                    review_id,
                    expected_state,
                    expected_attempt,
                    expected_run_id,
                    event_id,
                ],
            )
            .map_err(database_error("settle research retry review"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "settle research retry review CAS",
            });
        }
        let changed = transaction
            .execute(
                "UPDATE campaign_research
                 SET blocked_reason = ?1, next_due_at = NULL,
                     updated_at = ?2
                 WHERE campaign_id = ?3 AND blocked_reason IS NULL",
                params![failure_code, now, campaign_id],
            )
            .map_err(database_error("block research retry campaign"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "settle research retry campaign CAS",
            });
        }
        let changed = transaction
            .execute(
                "UPDATE events
                 SET status = 'failed', lease_until = NULL,
                     completed_at = ?1, last_error = ?2
                 WHERE event_id = ?3 AND status = ?4",
                params![now, failure_code, event_id, expected_event_status],
            )
            .map_err(database_error("settle research retry event"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "settle research retry event CAS",
            });
        }
        transaction
            .commit()
            .map_err(database_error("commit research retry settlement"))?;
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
            Option<String>,
            i64,
            String,
            Option<String>,
            Option<String>,
        ) = transaction
            .query_row(
                "SELECT review.campaign_id, review.state, review.attempt,
                        review.agent_run_id, review.session_generation,
                        review.experiment_id, review.failure_code,
                        review.context_json, review.context_digest, review.notes_json,
                        campaign_research.session_id, campaign_research.session_generation,
                        campaign.project_id, owner.project_id, owner.execution_kind
                 FROM research_reviews AS review
                 JOIN campaign_research
                   ON campaign_research.campaign_id = review.campaign_id
                 JOIN campaigns AS campaign
                   ON campaign.campaign_id = review.campaign_id
                 LEFT JOIN agent_runs AS owner
                   ON owner.run_id = review.agent_run_id
                 WHERE review.review_id = ?1",
                [review_id],
                |row| Ok((
                    row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?,
                    row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?,
                    row.get(8)?, row.get(9)?, row.get(10)?, row.get(11)?,
                    row.get(12)?, row.get(13)?, row.get(14)?,
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
            campaign_session,
            campaign_generation,
            campaign_project_id,
            owner_project_id,
            owner_execution_kind,
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
            if state != "retry_wait"
                || !owner_ready
                || campaign_generation != _generation
                || owner_project_id.as_deref() != Some(campaign_project_id.as_str())
                || owner_execution_kind.as_deref() != Some("campaign_research")
                || !native_recovery_cleanup_complete(
                    notes_json.as_deref(),
                    &NativeRecoveryCleanupExpectation {
                        review_id,
                        campaign_id: &campaign_id,
                        experiment_id: &_experiment_id,
                        attempt: current_attempt,
                        session_generation: _generation,
                        agent_run_id: current_run,
                        state: &state,
                        failure_code: failure_code.as_deref(),
                        campaign_session: campaign_session.as_deref(),
                    },
                )
            {
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
            let mut history_entry = json!({
                "attempt": current_attempt,
                "agent_run_id": previous_run_id,
                "failure_code": failure_code,
                "context_json": context_json,
                "context_digest": context_digest,
            });
            for field in [
                "planned_session_id",
                "confirmed_session_id",
                "session_binding",
            ] {
                if let Some(value) = notes.get(field).cloned() {
                    history_entry[field] = value;
                }
            }
            if let Some(native_recovery) = notes.get("native_recovery").cloned() {
                history_entry["native_recovery"] = native_recovery;
                notes
                    .as_object_mut()
                    .expect("research notes object")
                    .remove("native_recovery");
            }
            let history = notes
                .get_mut("retry_history")
                .and_then(serde_json::Value::as_array_mut)
                .expect("retry history array just created");
            history.push(history_entry);
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
                "SELECT r.review_id, r.agent_run_id, r.attempt, run.status
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
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .map_err(database_error("query research terminal recovery"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read research terminal recovery"))?;
        drop(statement);
        let mut recovered = 0;
        for (review_id, run_id, attempt, status) in rows {
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
                let retry_at = now.saturating_add(crate::retry::retry_backoff_seconds(attempt.max(1)));
                let changed = transaction
                    .execute(
                        "UPDATE research_reviews
                         SET state = 'retry_wait', failure_code = 'research_interrupted',
                             finished_at = ?1, not_before = ?2, updated_at = ?1
                         WHERE review_id = ?3 AND state = 'running'
                           AND agent_run_id = ?4",
                        params![now, retry_at, review_id, run_id],
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

    /// Atomically retire one startup-preserved research owner.  The caller
    /// opens the original recovery capability while holding the project
    /// admission lock; this method revalidates the immutable row snapshot,
    /// performs bounded cleanup while the IMMEDIATE transaction is held, and
    /// commits the phase/run/review/event CAS as one unit.
    pub(crate) fn retire_startup_native_owner(
        &self,
        owner: &StartupResearchOwner,
        mut cleanup: Option<&mut RecoveredPrivateRunTempCleanup>,
        now: i64,
    ) -> Result<bool, AppError> {
        let run_id = owner.run_id;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin startup research owner retirement"))?;
        let Some(current) = native_research_owner_rows(&transaction, None)?
            .into_iter()
            .find(|row| row.agent_run_id == run_id)
        else {
            transaction
                .commit()
                .map_err(database_error("commit missing startup research owner"))?;
            return Ok(false);
        };
        if !native_research_owner_lineage_valid(&current)
            || current.project_id != owner.project_id
            || current.review_id != owner.review_id
            || current.owner_pid != owner.pid
            || current.owner_status.as_deref() != Some(owner.status.as_str())
            || current.owner_gate_state.as_deref() != Some(owner.gate_state.as_str())
            || current.owner_policy_code != owner.policy_code
            || current.owner_failure_stage != owner.failure_stage
            || current.owner_log_path != owner.log_path
            || current.state != owner.review_state
            || current.failure_code != owner.failure_code
            || !matches!(
                current.owner_status.as_deref(),
                Some("starting" | "running" | "completed" | "failed" | "timed_out" | "cancelled")
            )
            || native_recovery_immutable_notes(current.notes_json.as_deref())
                != native_recovery_immutable_notes(owner.notes_json.as_deref())
        {
            transaction
                .commit()
                .map_err(database_error("commit changed startup research owner"))?;
            return Ok(false);
        }

        let expected = NativeResearchBindingExpectation {
            review_id: &current.review_id,
            campaign_id: &current.campaign_id,
            experiment_id: &current.experiment_id,
            attempt: current.attempt,
            session_generation: current.review_generation,
            agent_run_id: current.agent_run_id,
            state: &current.state,
            failure_code: current.failure_code.as_deref(),
            campaign_session: current.campaign_session.as_deref(),
        };
        let parsed_authority = if matches!(current.state.as_str(), "completed" | "discarded") {
            native_research_historical_authority(current.notes_json.as_deref(), &expected)
        } else {
            native_research_authority(current.notes_json.as_deref(), &expected)
        };
        let Some(parsed_authority) = parsed_authority else {
            return Err(validation_error(
                "research.native_recovery",
                "startup owner lacks a strict cleanup authority",
            ));
        };
        let Some(original_authority) = owner.authority.as_ref() else {
            return Err(validation_error(
                "research.native_recovery",
                "startup owner lacks its original cleanup authority",
            ));
        };
        if !native_research_authority_immutable_matches(original_authority, &parsed_authority) {
            return Err(validation_error(
                "research.native_recovery",
                "startup owner cleanup authority changed generations",
            ));
        }

        let owner_status = current.owner_status.as_deref().unwrap_or_default();
        let event_id = current.event_id.ok_or_else(|| {
            validation_error("research.event_id", "startup owner event binding is missing")
        })?;
        let event_status: EventStatus = transaction
            .query_row(
                "SELECT status FROM events WHERE project_id = ?1 AND event_id = ?2",
                params![current.project_id, event_id],
                |row| row.get(0),
            )
            .map_err(database_error("read startup research event state"))?;
        let active_owner = matches!(owner_status, "starting" | "running");
        let terminal_owner = matches!(
            owner_status,
            "completed" | "failed" | "timed_out" | "cancelled"
        );
        let review_running = current.state == "running";
        let review_ready = current.state == "ready";
        let review_retry = matches!(current.state.as_str(), "retry_wait" | "blocked");
        let review_terminal = matches!(current.state.as_str(), "completed" | "discarded");
        if !matches!(current.state.as_str(), "running" | "ready" | "retry_wait" | "blocked" | "completed" | "discarded")
            || (!active_owner && !terminal_owner)
            || (review_terminal && active_owner)
        {
            transaction
                .commit()
                .map_err(database_error("commit startup research state mismatch"))?;
            return Ok(false);
        }
        if !active_owner
            && !matches!(current.owner_gate_state.as_deref(), Some("released" | "failed"))
        {
            transaction
                .commit()
                .map_err(database_error("commit startup research gate mismatch"))?;
            return Ok(false);
        }
        let event_transient = matches!(
            event_status,
            EventStatus::Claimed | EventStatus::InFlight | EventStatus::Dispatched
        );
        let event_already_terminal = matches!(
            event_status,
            EventStatus::Completed | EventStatus::RetryWait | EventStatus::Failed | EventStatus::DeadLetter
        );
        let event_state_valid = if review_running {
            event_transient
        } else if review_ready {
            event_transient || event_status == EventStatus::Completed
        } else if review_retry {
            event_transient || event_already_terminal
        } else {
            event_already_terminal
        };
        if !event_state_valid {
            return Err(validation_error(
                "research.event",
                "startup owner event is in an unexpected state",
            ));
        }
        if review_retry && current.failure_code.is_none() {
            return Err(validation_error(
                "research.failure_code",
                "typed startup retry must retain its failure code",
            ));
        }
        let persisted_retry_wake = if current.state == "retry_wait" {
            Some(
                transaction
                    .query_row(
                        "SELECT not_before
                         FROM research_reviews
                         WHERE review_id = ?1 AND state = 'retry_wait'
                           AND agent_run_id = ?2 AND attempt = ?3
                           AND session_generation = ?4 AND failure_code = ?5",
                        params![
                            current.review_id,
                            current.agent_run_id,
                            current.attempt,
                            current.review_generation,
                            current.failure_code,
                        ],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(database_error("read classified research retry wake"))?,
            )
        } else {
            None
        };
        if !parsed_authority.cleanup_complete && cleanup.is_none() {
            return Err(validation_error(
                "research.native_recovery.cleanup",
                "original cleanup capability is required",
            ));
        }

        let mut notes_json = current.notes_json.clone();
        if !parsed_authority.cleanup_complete {
            cleanup
                .as_mut()
                .expect("pending authority requires cleanup capability")
                .cleanup_contents_before(Some(
                    Instant::now() + Duration::from_secs(30),
                ))
                .map_err(AppError::from)?;
            let mut notes = parse_research_notes(current.notes_json.as_deref())?;
            let cleanup_object = notes
                .get_mut("native_recovery")
                .and_then(Value::as_object_mut)
                .and_then(|authority| authority.get_mut("cleanup"))
                .and_then(Value::as_object_mut)
                .ok_or_else(|| {
                    validation_error(
                        "research.native_recovery.cleanup",
                        "phase is missing",
                    )
                })?;
            if cleanup_object.get("phase").and_then(Value::as_str) != Some("pending") {
                return Err(validation_error(
                    "research.native_recovery.cleanup",
                    "phase must remain pending before startup completion",
                ));
            }
            cleanup_object.insert("phase".to_owned(), Value::String("complete".to_owned()));
            cleanup_object.insert("completed_at".to_owned(), json!(now));
            notes_json = Some(notes.to_string());
        }

        if active_owner && (review_running || review_ready || review_retry) {
            let (run_status, run_error, final_gate) = if review_ready {
                ("completed", None, "released")
            } else if review_retry {
                (
                    "failed",
                    current.failure_code.as_deref(),
                    "failed",
                )
            } else {
                let final_gate = if owner.gate_state == "released" {
                    "released"
                } else {
                    "failed"
                };
                ("failed", Some("research_interrupted"), final_gate)
            };
            let changed = transaction
                .execute(
                    "UPDATE agent_runs
                     SET status = ?1, finished_at = ?2,
                         last_error = ?3,
                         launch_gate_state = ?4
                     WHERE run_id = ?5 AND status IN ('starting','running')
                       AND pid IS ?6",
                    params![run_status, now, run_error, final_gate, run_id, current.owner_pid],
                )
                .map_err(database_error("retire startup research run"))?;
            if changed != 1 {
                return Err(AppError::Runtime {
                    operation: "retire startup research run CAS",
                });
            }
        }
        if review_running {
            let retry_at = now.saturating_add(
                crate::retry::retry_backoff_seconds(current.attempt.max(1)),
            );
            let changed = transaction
                .execute(
                    "UPDATE research_reviews
                     SET state = 'retry_wait', failure_code = 'research_interrupted',
                         finished_at = ?1, not_before = ?2, updated_at = ?1,
                         notes_json = ?3
                     WHERE review_id = ?4 AND state = 'running'
                       AND agent_run_id = ?5 AND attempt = ?6
                       AND session_generation = ?7
                       AND ((notes_json IS NULL AND ?3 IS NULL) OR notes_json = ?8)",
                    params![
                        now,
                        retry_at,
                        notes_json,
                        current.review_id,
                        run_id,
                        current.attempt,
                        current.review_generation,
                        current.notes_json,
                    ],
                )
                .map_err(database_error("retry startup research review"))?;
            if changed != 1 {
                return Err(AppError::Runtime {
                    operation: "retry startup research review CAS",
                });
            }
        } else if notes_json != current.notes_json {
            let changed = transaction
                .execute(
                    "UPDATE research_reviews
                     SET notes_json = ?1, updated_at = ?2
                     WHERE review_id = ?3 AND agent_run_id = ?4
                       AND ((notes_json IS NULL AND ?5 IS NULL) OR notes_json = ?5)",
                    params![
                        notes_json,
                        now,
                        current.review_id,
                        run_id,
                        current.notes_json,
                    ],
                )
                .map_err(database_error("complete startup research authority"))?;
            if changed != 1 {
                return Err(AppError::Runtime {
                    operation: "complete startup research authority CAS",
                });
            }
        }
        if review_running {
            let retry_at = now.saturating_add(
                crate::retry::retry_backoff_seconds(current.attempt.max(1)),
            );
            let changed = transaction
                .execute(
                    "UPDATE events
                     SET status = 'retry_wait', lease_until = NULL,
                         not_before = ?1, last_error = 'research_interrupted'
                     WHERE event_id = ?2 AND status = ?3",
                    params![retry_at, event_id, event_status],
                )
                .map_err(database_error("retry startup research event"))?;
            if changed != 1 {
                return Err(AppError::Runtime {
                    operation: "retry startup research event CAS",
                });
            }
        }
        if review_ready && event_transient {
            let changed = transaction
                .execute(
                    "UPDATE events
                     SET status = 'completed', lease_until = NULL,
                         completed_at = ?1, last_error = NULL
                     WHERE project_id = ?2 AND event_id = ?3 AND status = ?4",
                    params![now, current.project_id, event_id, event_status],
                )
                .map_err(database_error("complete startup ready research event"))?;
            if changed != 1 {
                return Err(AppError::Runtime {
                    operation: "complete startup ready research event CAS",
                });
            }
        }
        if review_retry && event_transient {
            let changed = if current.state == "blocked" {
                transaction
                    .execute(
                        "UPDATE events
                         SET status = 'failed', lease_until = NULL,
                             completed_at = ?1, last_error = ?2
                         WHERE event_id = ?3 AND status = ?4",
                        params![now, current.failure_code, event_id, event_status],
                    )
            } else {
                let retry_at = persisted_retry_wake.ok_or_else(|| AppError::Runtime {
                    operation: "read classified research retry wake",
                })?;
                transaction
                    .execute(
                        "UPDATE events
                         SET status = 'retry_wait', lease_until = NULL,
                             not_before = ?1, completed_at = NULL, last_error = ?2
                         WHERE event_id = ?3 AND status = ?4",
                        params![retry_at, current.failure_code, event_id, event_status],
                    )
            }
            .map_err(database_error("settle startup research event policy"))?;
            if changed != 1 {
                return Err(AppError::Runtime {
                    operation: "settle startup research event policy CAS",
                });
            }
        }
        if review_running && parsed_authority.fresh_launch && !parsed_authority.session_confirmed {
            let campaign_session = current.campaign_session.clone();
            let campaign_generation = current.campaign_generation;
            if campaign_session.is_some() {
            let changed = transaction
                .execute(
                    "UPDATE campaign_research
                     SET session_id = NULL, updated_at = ?1
                     WHERE campaign_id = ?2 AND session_generation = ?3
                       AND session_id = ?4",
                    params![
                        now,
                        current.campaign_id,
                        campaign_generation,
                        campaign_session,
                    ],
                )
                .map_err(database_error("clear interrupted fresh research session"))?;
            if changed != 1 {
                return Err(AppError::Runtime {
                    operation: "clear interrupted fresh research session CAS",
                });
            }
            }
        }
        transaction
            .commit()
            .map_err(database_error("commit startup research owner retirement"))?;
        Ok(true)
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
            "SELECT c.campaign_id, e.experiment_id, e.task_signature,
                    e.pueue_task_id, p.pueue_group,
                    COALESCE(observation.started_at,
                             observation.first_observed_at),
                    c.project_id, observation.task_signature,
                    observation.pueue_task_id, observation.pueue_group,
                    observation.command_json, observation.state,
                    observation.enqueued_at, observation.started_at,
                    observation.ended_at, observation.result,
                    observation.observed_at
             FROM campaigns c
             JOIN projects p ON p.project_id = c.project_id
             JOIN experiments e ON e.campaign_id = c.campaign_id
             JOIN submissions s
               ON s.submission_id = e.submission_id
              AND s.project_id = c.project_id
             JOIN task_observations observation
               ON observation.project_id = c.project_id
              AND observation.pueue_task_id = e.pueue_task_id
              AND observation.pueue_group = p.pueue_group
              AND lower(observation.state) = 'running'
              AND NOT EXISTS (
                  SELECT 1 FROM task_observations AS newer_observation
                  WHERE newer_observation.project_id = observation.project_id
                    AND newer_observation.pueue_task_id = observation.pueue_task_id
                    AND newer_observation.observed_at > observation.observed_at
              )
              AND NOT EXISTS (
                  SELECT 1 FROM task_observations AS tied_observation
                  WHERE tied_observation.project_id = observation.project_id
                    AND tied_observation.pueue_task_id = observation.pueue_task_id
                    AND tied_observation.observed_at = observation.observed_at
                    AND tied_observation.task_signature <> observation.task_signature
              )
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
                      e.experiment_id, observation.observed_at DESC,
                      observation.task_signature DESC"
        );
        let authority = {
            let mut statement = transaction
                .prepare(&candidate_query)
                .map_err(database_error("prepare authoritative research candidates"))?;
            let candidates = statement
                .query_map([campaign_id], running_research_candidate_from_row)
                .map_err(database_error("read authoritative research candidates"))?;
            let mut authority = None;
            for candidate in candidates {
                let candidate = candidate.map_err(database_error(
                    "read authoritative research candidate",
                ))?;
                if candidate.has_managed_identity() {
                    authority = Some(candidate);
                    break;
                }
            }
            authority
        };
        let Some(authority) = authority
        else {
            transaction
                .commit()
                .map_err(database_error("commit deferred research review claim"))?;
            return Ok(None);
        };
        let project_id = authority.observation.project_id.clone();
        let canonical_experiment_id = authority.experiment_id;
        let canonical_signature = authority.managed_signature;
        let pueue_task_id = authority.pueue_task_id;
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

    /// Record the immutable native research recovery authority after the
    /// private generation exists and before schema or child activity starts.
    /// The existing notes object is merged under one dedicated key and the
    /// joined review/session/run binding is a compare-and-swap predicate.
    pub fn record_native_recovery_authority(
        &self,
        binding: &ResearchLaunchBinding,
        agent_run_id: i64,
        identity: &PrivateRunTempRecoveryIdentityV1,
        fresh_launch: bool,
        now: i64,
    ) -> Result<(), AppError> {
        validate_research_binding(binding)?;
        let authority = json!({
            "version": PrivateRunTempRecoveryIdentityV1::VERSION,
            "run_id": agent_run_id,
            "review_id": binding.review_id,
            "campaign_id": binding.campaign_id,
            "experiment_id": binding.experiment_id,
            "attempt": binding.attempt,
            "session_generation": binding.session_generation,
            "fresh_launch": fresh_launch,
            "session_id": binding.session_id,
            "service_root_identity": identity.service_root_identity,
            "temp_identity": identity.temp_identity,
            "cleanup": {"phase": "pending"},
        });
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin native research recovery authority"))?;
        let current: (
            String,
            String,
            String,
            i64,
            Option<i64>,
            i64,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            i64,
            String,
            String,
            String,
            String,
        ) = transaction
            .query_row(
                "SELECT r.campaign_id, r.experiment_id, r.state, r.attempt,
                        r.agent_run_id, r.session_generation, r.context_json,
                        r.context_digest, r.notes_json, c.session_id,
                        c.session_generation, run.status, run.execution_kind,
                        campaign.project_id, run.project_id
                 FROM research_reviews AS r
                 JOIN campaign_research AS c ON c.campaign_id = r.campaign_id
                 JOIN campaigns AS campaign ON campaign.campaign_id = r.campaign_id
                 JOIN agent_runs AS run ON run.run_id = r.agent_run_id
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
                        row.get(10)?,
                        row.get(11)?,
                        row.get(12)?,
                        row.get(13)?,
                        row.get(14)?,
                    ))
                },
            )
            .map_err(database_error("read native research recovery binding"))?;
        let (
            campaign_id,
            experiment_id,
            state,
            attempt,
            current_run,
            review_generation,
            context_json,
            context_digest,
            current_notes,
            current_session,
            current_generation,
            run_status,
            execution_kind,
            campaign_project_id,
            run_project_id,
        ) = current;
        if campaign_id != binding.campaign_id
            || experiment_id != binding.experiment_id
            || state != "running"
            || attempt != binding.attempt
            || current_run != Some(agent_run_id)
            || review_generation != binding.session_generation
            || context_json.as_deref() != Some(binding.context_json.as_str())
            || context_digest.as_deref() != Some(binding.context_digest.as_str())
            || current_session.as_deref() != Some(binding.session_id.as_str())
            || current_generation != binding.session_generation
            || !matches!(run_status.as_str(), "starting" | "running")
            || execution_kind != "campaign_research"
            || campaign_project_id != run_project_id
        {
            return Err(validation_error(
                "research.native_recovery",
                "cannot record authority for a changed native research binding",
            ));
        }
        let mut notes = parse_research_notes(current_notes.as_deref())?;
        if let Some(existing) = notes.get("native_recovery") {
            let parsed = parse_native_recovery_authority(existing)?;
            let expected_identity = identity;
            if native_recovery_authority_matches(
                &parsed,
                binding,
                agent_run_id,
                expected_identity,
                fresh_launch,
            ) {
                transaction
                    .commit()
                    .map_err(database_error("commit idempotent native research authority"))?;
                return Ok(());
            }
            return Err(validation_error(
                "research.native_recovery",
                "an immutable recovery authority already exists for this run",
            ));
        }
        notes["native_recovery"] = authority;
        let notes_json = notes.to_string();
        let changed = transaction
            .execute(
                "UPDATE research_reviews
                 SET notes_json = ?1, updated_at = ?2
                 WHERE review_id = ?3 AND campaign_id = ?4 AND experiment_id = ?5
                   AND state = 'running' AND attempt = ?6 AND agent_run_id = ?7
                   AND session_generation = ?8 AND context_json = ?9
                   AND context_digest = ?10
                   AND ((notes_json IS NULL AND ?11 IS NULL) OR notes_json = ?11)",
                params![
                    notes_json,
                    now,
                    binding.review_id,
                    binding.campaign_id,
                    binding.experiment_id,
                    binding.attempt,
                    agent_run_id,
                    binding.session_generation,
                    binding.context_json,
                    binding.context_digest,
                    current_notes,
                ],
            )
            .map_err(database_error("persist native research recovery authority"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "persist native research recovery authority CAS",
            });
        }
        transaction
            .commit()
            .map_err(database_error("commit native research recovery authority"))
    }

    /// Mark only the mutable cleanup phase of a recorded native authority.
    /// Terminal research outcomes and business notes remain compare-and-swap
    /// protected and are never rewritten by this operation.
    pub fn mark_native_cleanup_complete(
        &self,
        binding: &ResearchLaunchBinding,
        agent_run_id: i64,
        identity: &PrivateRunTempRecoveryIdentityV1,
        fresh_launch: bool,
        now: i64,
    ) -> Result<(), AppError> {
        validate_research_binding(binding)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin native research cleanup completion"))?;
        let current: (
            String,
            String,
            String,
            i64,
            Option<i64>,
            i64,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
            String,
            i64,
            String,
            String,
            String,
        ) = transaction
            .query_row(
                "SELECT r.campaign_id, r.experiment_id, r.state, r.attempt,
                        r.agent_run_id, r.session_generation, r.context_json,
                        r.context_digest, r.notes_json, r.failure_code,
                        c.session_id, run.status,
                        run.launch_gate_state, c.session_generation,
                        campaign.project_id, run.project_id, run.execution_kind
                 FROM research_reviews AS r
                 JOIN campaign_research AS c ON c.campaign_id = r.campaign_id
                 JOIN campaigns AS campaign ON campaign.campaign_id = r.campaign_id
                 JOIN agent_runs AS run ON run.run_id = r.agent_run_id
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
                        row.get(10)?,
                        row.get(11)?,
                        row.get(12)?,
                        row.get(13)?,
                        row.get(14)?,
                        row.get(15)?,
                        row.get(16)?,
                    ))
                },
            )
            .map_err(database_error("read native research cleanup binding"))?;
        let (
            campaign_id,
            experiment_id,
            state,
            attempt,
            current_run,
            review_generation,
            context_json,
            context_digest,
            current_notes,
            failure_code,
            campaign_session,
            run_status,
            launch_gate_state,
            campaign_generation,
            campaign_project_id,
            run_project_id,
            execution_kind,
        ) = current;
        if campaign_id != binding.campaign_id
            || experiment_id != binding.experiment_id
            || !matches!(state.as_str(), "ready" | "retry_wait" | "blocked")
            || attempt != binding.attempt
            || current_run != Some(agent_run_id)
            || review_generation != binding.session_generation
            || context_json.as_deref() != Some(binding.context_json.as_str())
            || context_digest.as_deref() != Some(binding.context_digest.as_str())
            || campaign_generation != binding.session_generation
            || (!fresh_launch && campaign_session.as_deref() != Some(binding.session_id.as_str()))
            || (fresh_launch
                && campaign_session.is_none()
                && (!matches!(state.as_str(), "retry_wait" | "blocked")
                    || failure_code.is_none()))
            || !matches!(run_status.as_str(), "completed" | "failed" | "timed_out" | "cancelled")
            || !matches!(launch_gate_state.as_str(), "released" | "failed")
            || campaign_project_id != run_project_id
            || execution_kind != "campaign_research"
        {
            return Err(validation_error(
                "research.native_recovery",
                "cannot complete cleanup for a changed native research binding",
            ));
        }
        let mut notes = parse_research_notes(current_notes.as_deref())?;
        let authority = notes
            .get("native_recovery")
            .ok_or_else(|| validation_error("research.native_recovery", "authority is missing"))?;
        let parsed = parse_native_recovery_authority(authority)?;
        if !native_recovery_authority_matches(
            &parsed,
            binding,
            agent_run_id,
            identity,
            fresh_launch,
        ) {
            return Err(validation_error(
                "research.native_recovery",
                "cleanup authority identity does not match the bound native run",
            ));
        }
        if fresh_launch && campaign_session.is_some() {
            let confirmed_session = notes
                .get("confirmed_session_id")
                .and_then(Value::as_str);
            if confirmed_session != campaign_session.as_deref() {
                return Err(validation_error(
                    "research.session",
                    "fresh native cleanup requires the confirmed campaign session",
                ));
            }
        }
        let cleanup = notes
            .get_mut("native_recovery")
            .and_then(Value::as_object_mut)
            .and_then(|authority| authority.get_mut("cleanup"))
            .and_then(Value::as_object_mut)
            .ok_or_else(|| validation_error("research.native_recovery.cleanup", "phase is missing"))?;
        match cleanup.get("phase").and_then(Value::as_str) {
            Some("complete") => {
                transaction
                    .commit()
                    .map_err(database_error("commit idempotent native cleanup completion"))?;
                return Ok(());
            }
            Some("pending") => {}
            _ => {
                return Err(validation_error(
                    "research.native_recovery.cleanup",
                    "phase must be pending or complete",
                ));
            }
        }
        cleanup.insert("phase".to_owned(), Value::String("complete".to_owned()));
        cleanup.insert("completed_at".to_owned(), json!(now));
        let notes_json = notes.to_string();
        let changed = transaction
            .execute(
                "UPDATE research_reviews
                 SET notes_json = ?1, updated_at = ?2
                 WHERE review_id = ?3 AND campaign_id = ?4 AND experiment_id = ?5
                   AND state = ?6 AND attempt = ?7 AND agent_run_id = ?8
                   AND session_generation = ?9 AND context_json = ?10
                   AND context_digest = ?11
                   AND ((notes_json IS NULL AND ?12 IS NULL) OR notes_json = ?12)",
                params![
                    notes_json,
                    now,
                    binding.review_id,
                    binding.campaign_id,
                    binding.experiment_id,
                    state,
                    binding.attempt,
                    agent_run_id,
                    binding.session_generation,
                    binding.context_json,
                    binding.context_digest,
                    current_notes,
                ],
            )
            .map_err(database_error("persist native cleanup completion"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "persist native cleanup completion CAS",
            });
        }
        transaction
            .commit()
            .map_err(database_error("commit native cleanup completion"))
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
        if let Some(notes) = recovery_notes.as_object_mut() {
            // A new bind starts with a pending proof.  A prior confirmed
            // session belongs to the previous attempt and must not survive
            // as if this run had already confirmed its session.
            notes.remove("confirmed_session_id");
        }
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeRecoveryAuthority {
    version: u8,
    run_id: i64,
    review_id: String,
    campaign_id: String,
    experiment_id: String,
    attempt: i64,
    session_generation: i64,
    fresh_launch: bool,
    session_id: String,
    service_root_identity: PrivateRunTempRecoveryRootIdentity,
    temp_identity: PrivateRunTempRecoveryTempIdentity,
    cleanup: NativeRecoveryCleanup,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeRecoveryCleanup {
    phase: String,
    #[serde(default)]
    completed_at: Option<i64>,
}

/// The joined database fields used to validate one persisted native research
/// owner.  The authority itself is intentionally validated separately from
/// SQL lineage and project joins so every recovery/admission caller shares the
/// same strict notes parser.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NativeResearchBindingExpectation<'a> {
    pub review_id: &'a str,
    pub campaign_id: &'a str,
    pub experiment_id: &'a str,
    pub attempt: i64,
    pub session_generation: i64,
    pub agent_run_id: i64,
    pub state: &'a str,
    pub failure_code: Option<&'a str>,
    pub campaign_session: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub(crate) struct NativeResearchAuthorityView {
    pub version: u8,
    pub run_id: i64,
    pub review_id: String,
    pub campaign_id: String,
    pub experiment_id: String,
    pub attempt: i64,
    pub session_generation: i64,
    pub fresh_launch: bool,
    pub session_id: String,
    pub session_confirmed: bool,
    pub cleanup_complete: bool,
    pub identity: PrivateRunTempRecoveryIdentityV1,
}

struct NativeResearchOwnerRow {
    review_id: String,
    campaign_id: String,
    experiment_id: String,
    attempt: i64,
    review_generation: i64,
    agent_run_id: i64,
    state: String,
    failure_code: Option<String>,
    campaign_session: Option<String>,
    campaign_generation: i64,
    notes_json: Option<String>,
    event_id: Option<i64>,
    project_id: String,
    owner_project_id: Option<String>,
    owner_execution_kind: Option<String>,
    owner_status: Option<String>,
    owner_gate_state: Option<String>,
    owner_pid: Option<i64>,
    owner_primary_event_id: Option<i64>,
    owner_log_path: Option<PathBuf>,
    owner_policy_code: Option<String>,
    owner_failure_stage: Option<String>,
    event_project_id: Option<String>,
    event_kind: Option<String>,
    event_campaign_id: Option<String>,
    event_experiment_id: Option<String>,
    experiment_campaign_id: Option<String>,
    review_event_link_count: i64,
    total_event_link_count: i64,
    bound_review_count: i64,
    detached_cleanup_complete: bool,
    detached_campaign_project_ids: BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct StartupResearchOwner {
    pub run_id: i64,
    pub project_id: String,
    pub review_id: String,
    pub pid: Option<i64>,
    pub status: String,
    pub gate_state: String,
    pub policy_code: Option<String>,
    pub failure_stage: Option<String>,
    pub log_path: Option<PathBuf>,
    pub review_state: String,
    pub failure_code: Option<String>,
    pub notes_json: Option<String>,
    pub marker_absent: bool,
    pub authority: Option<NativeResearchAuthorityView>,
    pub original_status: String,
    pub original_gate_state: String,
    pub original_policy_code: Option<String>,
    pub original_failure_stage: Option<String>,
}

fn native_recovery_immutable_notes(notes_json: Option<&str>) -> Option<String> {
    let mut notes = serde_json::from_str::<Value>(notes_json?).ok()?;
    let authority = notes.get_mut("native_recovery")?.as_object_mut()?;
    authority.remove("cleanup");
    serde_json::to_string(&notes).ok()
}

pub(crate) fn native_research_authority_immutable_matches(
    expected: &NativeResearchAuthorityView,
    current: &NativeResearchAuthorityView,
) -> bool {
    expected.version == current.version
        && expected.run_id == current.run_id
        && expected.review_id == current.review_id
        && expected.campaign_id == current.campaign_id
        && expected.experiment_id == current.experiment_id
        && expected.attempt == current.attempt
        && expected.session_generation == current.session_generation
        && expected.fresh_launch == current.fresh_launch
        && expected.session_id == current.session_id
        && expected.identity == current.identity
}

fn parse_native_recovery_authority(value: &Value) -> Result<NativeRecoveryAuthority, AppError> {
    serde_json::from_value(value.clone()).map_err(|source| AppError::Serialization {
        operation: "parse native research recovery authority",
        source,
    })
}

/// Strictly validate the immutable native recovery proof against the current
/// joined research row.  Missing, malformed, foreign, and substituted proof
/// all fail closed as `None`; SQL callers classify a row with no binding
/// separately when needed.
pub(crate) fn native_research_authority(
    notes_json: Option<&str>,
    expected: &NativeResearchBindingExpectation<'_>,
) -> Option<NativeResearchAuthorityView> {
    let notes = serde_json::from_str::<Value>(notes_json?).ok()?;
    let parsed = parse_native_recovery_authority(notes.get("native_recovery")?).ok()?;
    if parsed.version != PrivateRunTempRecoveryIdentityV1::VERSION
        || parsed.run_id != expected.agent_run_id
        || parsed.review_id != expected.review_id
        || parsed.campaign_id != expected.campaign_id
        || parsed.experiment_id != expected.experiment_id
        || parsed.attempt != expected.attempt
        || parsed.session_generation != expected.session_generation
        || parsed.session_id.is_empty()
        || validate_session_id(&parsed.session_id).is_err()
        || !matches!(parsed.cleanup.phase.as_str(), "pending" | "complete")
        || (parsed.cleanup.phase == "complete" && parsed.cleanup.completed_at.is_none())
    {
        return None;
    }

    let planned_session = notes.get("planned_session_id").and_then(Value::as_str);
    let confirmed_session = notes.get("confirmed_session_id").and_then(Value::as_str);
    let session_binding = notes.get("session_binding").and_then(Value::as_str);
    let session_shape_matches = if parsed.fresh_launch {
        match expected.campaign_session {
            Some(campaign_session)
                if confirmed_session == Some(campaign_session)
                    && session_binding == Some("confirmed") =>
            {
                planned_session == Some(parsed.session_id.as_str())
                    && validate_session_id(campaign_session).is_ok()
                    && confirmed_session.is_some_and(|session| validate_session_id(session).is_ok())
            }
            Some(campaign_session)
                if expected.state == "running"
                    && campaign_session == parsed.session_id
                    && confirmed_session.is_none()
                    && session_binding == Some("pending") =>
            {
                planned_session == Some(parsed.session_id.as_str())
            }
            _ => false,
        }
    } else {
        let resumed_pending = matches!(expected.state, "running" | "retry_wait" | "blocked")
            && expected.campaign_session == Some(parsed.session_id.as_str())
            && planned_session == Some(parsed.session_id.as_str())
            && session_binding == Some("pending")
            && confirmed_session.is_none();
        let resumed_confirmed = expected.campaign_session == Some(parsed.session_id.as_str())
            && expected
                .campaign_session
                .is_some_and(|session| validate_session_id(session).is_ok())
            && planned_session == Some(parsed.session_id.as_str())
            && session_binding == Some("confirmed")
            && confirmed_session == Some(parsed.session_id.as_str())
            && confirmed_session
                .is_some_and(|session| validate_session_id(session).is_ok());
        resumed_pending || resumed_confirmed
    };
    let fresh_cleared = parsed.fresh_launch
        && expected.campaign_session.is_none()
        && matches!(expected.state, "retry_wait" | "blocked")
        && expected.failure_code.is_some()
        && confirmed_session.is_none()
        && planned_session == Some(parsed.session_id.as_str())
        && session_binding == Some("pending");
    if !session_shape_matches && !fresh_cleared {
        return None;
    }
    let session_confirmed = if parsed.fresh_launch {
        session_binding == Some("confirmed")
            && confirmed_session.is_some()
            && expected.campaign_session.is_some()
    } else {
        session_binding == Some("confirmed")
            && confirmed_session == Some(parsed.session_id.as_str())
    };

    Some(NativeResearchAuthorityView {
        version: parsed.version,
        run_id: parsed.run_id,
        review_id: parsed.review_id,
        campaign_id: parsed.campaign_id,
        experiment_id: parsed.experiment_id,
        attempt: parsed.attempt,
        session_generation: parsed.session_generation,
        fresh_launch: parsed.fresh_launch,
        session_id: parsed.session_id,
        session_confirmed,
        cleanup_complete: parsed.cleanup.phase == "complete",
        identity: PrivateRunTempRecoveryIdentityV1 {
            service_root_identity: parsed.service_root_identity,
            temp_identity: parsed.temp_identity,
        },
    })
}

fn native_research_historical_authority(
    notes_json: Option<&str>,
    expected: &NativeResearchBindingExpectation<'_>,
) -> Option<NativeResearchAuthorityView> {
    let notes = serde_json::from_str::<Value>(notes_json?).ok()?;
    let parsed = parse_native_recovery_authority(notes.get("native_recovery")?).ok()?;
    if parsed.version != PrivateRunTempRecoveryIdentityV1::VERSION
        || parsed.run_id != expected.agent_run_id
        || parsed.review_id != expected.review_id
        || parsed.campaign_id != expected.campaign_id
        || parsed.experiment_id != expected.experiment_id
        || parsed.attempt != expected.attempt
        || parsed.session_generation != expected.session_generation
        || parsed.session_id.is_empty()
        || validate_session_id(&parsed.session_id).is_err()
        || !matches!(parsed.cleanup.phase.as_str(), "pending" | "complete")
        || (parsed.cleanup.phase == "complete" && parsed.cleanup.completed_at.is_none())
        || notes.get("planned_session_id").and_then(Value::as_str)
            != Some(parsed.session_id.as_str())
        || notes.get("session_binding").and_then(Value::as_str) != Some("confirmed")
    {
        return None;
    }
    let confirmed_session = notes.get("confirmed_session_id").and_then(Value::as_str)?;
    if validate_session_id(confirmed_session).is_err()
        || (!parsed.fresh_launch && confirmed_session != parsed.session_id)
    {
        return None;
    }
    Some(NativeResearchAuthorityView {
        version: parsed.version,
        run_id: parsed.run_id,
        review_id: parsed.review_id,
        campaign_id: parsed.campaign_id,
        experiment_id: parsed.experiment_id,
        attempt: parsed.attempt,
        session_generation: parsed.session_generation,
        fresh_launch: parsed.fresh_launch,
        session_id: parsed.session_id,
        session_confirmed: true,
        cleanup_complete: parsed.cleanup.phase == "complete",
        identity: PrivateRunTempRecoveryIdentityV1 {
            service_root_identity: parsed.service_root_identity,
            temp_identity: parsed.temp_identity,
        },
    })
}

pub(crate) fn project_has_unresolved_native_research_owner(
    connection: &Connection,
    project_id: &str,
) -> Result<bool, AppError> {
    for row in native_research_owner_rows(connection, None)? {
        let implicated = row.project_id == project_id
            || row.owner_project_id.as_deref() == Some(project_id)
            || row.detached_campaign_project_ids.contains(project_id);
        if implicated && !native_research_owner_is_complete(&row) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn native_cleanup_blocked_project_ids(
    connection: &Connection,
) -> Result<BTreeSet<String>, AppError> {
    let mut blocked = BTreeSet::new();
    for row in native_research_owner_rows(connection, None)? {
        if !native_research_owner_is_complete(&row) {
            blocked.insert(row.project_id.clone());
            if let Some(owner_project_id) = row.owner_project_id {
                blocked.insert(owner_project_id);
            }
            blocked.extend(row.detached_campaign_project_ids);
        }
    }
    Ok(blocked)
}

fn native_research_owner_rows(
    connection: &Connection,
    project_id: Option<&str>,
) -> Result<Vec<NativeResearchOwnerRow>, AppError> {
    let filter = if project_id.is_some() {
        " AND (campaign.project_id = ?1 OR owner.project_id = ?1)"
    } else {
        ""
    };
    let query = format!(
        "SELECT review.review_id, review.campaign_id, review.experiment_id,
                review.attempt, review.session_generation,
                review.agent_run_id, review.state, review.failure_code,
                research_state.session_id, research_state.session_generation,
                review.notes_json, review.event_id, campaign.project_id,
                owner.project_id, owner.execution_kind, owner.status,
                owner.launch_gate_state, owner.pid, owner.primary_event_id,
                owner.log_path, owner.policy_code, owner.failure_stage,
                event.project_id, event.kind, event.campaign_id,
                event.experiment_id,
                experiment.campaign_id,
                (SELECT COUNT(*) FROM agent_run_events AS link
                   WHERE link.project_id = owner.project_id
                     AND link.run_id = owner.run_id
                     AND link.event_id = review.event_id),
                (SELECT COUNT(*) FROM agent_run_events AS link
                   WHERE link.project_id = owner.project_id
                     AND link.run_id = owner.run_id),
                (SELECT COUNT(*) FROM research_reviews AS bound_review
                   WHERE bound_review.agent_run_id = owner.run_id)
         FROM research_reviews AS review
         JOIN campaigns AS campaign ON campaign.campaign_id = review.campaign_id
         JOIN campaign_research AS research_state
           ON research_state.campaign_id = review.campaign_id
         LEFT JOIN experiments AS experiment
           ON experiment.experiment_id = review.experiment_id
         LEFT JOIN agent_runs AS owner ON owner.run_id = review.agent_run_id
         LEFT JOIN events AS event ON event.event_id = review.event_id
         WHERE review.agent_run_id IS NOT NULL{filter}
         ORDER BY campaign.project_id, review.review_id"
    );
    let mut statement = connection
        .prepare(&query)
        .map_err(database_error("prepare native research owner query"))?;
    let rows = match project_id {
        Some(project_id) => statement
            .query_map([project_id], native_research_owner_row_from_sql),
        None => statement.query_map([], native_research_owner_row_from_sql),
    }
    .map_err(database_error("query native research owners"))?;
    let mut owners = rows
        .map(|row| row.map_err(database_error("read native research owner")))
        .collect::<Result<Vec<_>, _>>()?;

    // Keep malformed or unbound native runs in the blocker universe.  The
    // joined review query above intentionally exposes the full lineage when
    // it exists, while this owner-rooted arm preserves a run whose review,
    // campaign, or campaign_research row has disappeared or never bound.
    let owner_filter = if project_id.is_some() {
        " AND owner.project_id = ?1"
    } else {
        ""
    };
    let unknown_query = format!(
        "SELECT owner.run_id, owner.project_id, owner.status,
                owner.launch_gate_state, owner.pid, owner.log_path,
                owner.policy_code, owner.failure_stage
         FROM agent_runs AS owner
         WHERE owner.execution_kind = 'campaign_research'{owner_filter}
           AND NOT EXISTS (
               SELECT 1
               FROM research_reviews AS review
               JOIN campaigns AS campaign
                 ON campaign.campaign_id = review.campaign_id
               JOIN campaign_research AS research_state
                 ON research_state.campaign_id = review.campaign_id
               WHERE review.agent_run_id = owner.run_id
           )
         ORDER BY owner.project_id, owner.run_id"
    );
    let mut statement = connection
        .prepare(&unknown_query)
        .map_err(database_error("prepare unbound native research owner query"))?;
    let mut unknown_rows = Vec::new();
    match project_id {
        Some(project_id) => {
            let rows = statement
                .query_map([project_id], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                    ))
                })
                .map_err(database_error("query unbound native research owners"))?;
            unknown_rows.extend(
                rows.map(|row| row.map_err(database_error("read unbound native research owner")))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        None => {
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                    ))
                })
                .map_err(database_error("query unbound native research owners"))?;
            unknown_rows.extend(
                rows.map(|row| row.map_err(database_error("read unbound native research owner")))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    for unknown in unknown_rows {
        let (
            agent_run_id,
            project_id,
            owner_status,
            owner_gate_state,
            owner_pid,
            owner_log_path,
            owner_policy_code,
            owner_failure_stage,
        ) = unknown;
        owners.push(NativeResearchOwnerRow {
            review_id: String::new(),
            campaign_id: String::new(),
            experiment_id: String::new(),
            attempt: 0,
            review_generation: -1,
            agent_run_id,
            state: String::new(),
            failure_code: None,
            campaign_session: None,
            campaign_generation: -1,
            notes_json: None,
            event_id: None,
            project_id: project_id.clone(),
            owner_project_id: Some(project_id.clone()),
            owner_execution_kind: Some("campaign_research".to_owned()),
            owner_status: Some(owner_status),
            owner_gate_state: Some(owner_gate_state),
            owner_pid,
            owner_primary_event_id: None,
            owner_log_path: owner_log_path.map(PathBuf::from),
            owner_policy_code,
            owner_failure_stage,
            event_project_id: None,
            event_kind: None,
            event_campaign_id: None,
            event_experiment_id: None,
            experiment_campaign_id: None,
            review_event_link_count: 0,
            total_event_link_count: 0,
            bound_review_count: 0,
            detached_cleanup_complete: false,
            detached_campaign_project_ids: BTreeSet::new(),
        });
    }
    drop(statement);
    for owner in &mut owners {
        if owner.review_id.is_empty()
            && matches!(
                owner.owner_status.as_deref(),
                Some("completed" | "failed" | "timed_out" | "cancelled")
            )
        {
            let evidence = detached_native_cleanup_evidence(connection, owner.agent_run_id)?;
            owner.detached_cleanup_complete = evidence.candidate_count == 1
                && evidence.valid_count == 1;
            owner.detached_campaign_project_ids = evidence.project_ids;
        }
    }
    Ok(owners)
}

fn native_research_owner_row_from_sql(
    row: &Row<'_>,
) -> rusqlite::Result<NativeResearchOwnerRow> {
    Ok(NativeResearchOwnerRow {
        review_id: row.get(0)?,
        campaign_id: row.get(1)?,
        experiment_id: row.get(2)?,
        attempt: row.get(3)?,
        review_generation: row.get(4)?,
        agent_run_id: row.get(5)?,
        state: row.get(6)?,
        failure_code: row.get(7)?,
        campaign_session: row.get(8)?,
        campaign_generation: row.get(9)?,
        notes_json: row.get(10)?,
        event_id: row.get(11)?,
        project_id: row.get(12)?,
        owner_project_id: row.get(13)?,
        owner_execution_kind: row.get(14)?,
        owner_status: row.get(15)?,
        owner_gate_state: row.get(16)?,
        owner_pid: row.get(17)?,
        owner_primary_event_id: row.get(18)?,
        owner_log_path: row.get::<_, Option<String>>(19)?.map(PathBuf::from),
        owner_policy_code: row.get(20)?,
        owner_failure_stage: row.get(21)?,
        event_project_id: row.get(22)?,
        event_kind: row.get(23)?,
        event_campaign_id: row.get(24)?,
        event_experiment_id: row.get(25)?,
        experiment_campaign_id: row.get(26)?,
        review_event_link_count: row.get(27)?,
        total_event_link_count: row.get(28)?,
        bound_review_count: row.get(29)?,
        detached_cleanup_complete: false,
        detached_campaign_project_ids: BTreeSet::new(),
    })
}

#[derive(Default)]
struct DetachedNativeCleanupEvidence {
    project_ids: BTreeSet<String>,
    candidate_count: usize,
    valid_count: usize,
}

fn retry_history_native_cleanup_counts(
    connection: &Connection,
    notes_json: Option<&str>,
    run_id: i64,
    review_id: &str,
    campaign_id: &str,
    experiment_id: &str,
    event_id: i64,
    campaign_project_id: &str,
) -> Result<(usize, usize), AppError> {
    let Some(notes_json) = notes_json else {
        return Ok((0, 0));
    };
    let Ok(notes) = serde_json::from_str::<Value>(notes_json) else {
        return Ok((0, 0));
    };
    let Some(history) = notes.get("retry_history").and_then(Value::as_array) else {
        return Ok((0, 0));
    };
    let mut candidate_entries = 0;
    let mut valid_entries = 0;
    for entry in history {
        if entry.get("agent_run_id").and_then(Value::as_i64) != Some(run_id) {
            continue;
        }
        candidate_entries += 1;
        let Some(attempt) = entry.get("attempt").and_then(Value::as_i64) else {
            continue;
        };
        let Some(native_recovery) = entry.get("native_recovery") else {
            continue;
        };
        let Some(failure_code) = entry
            .get("failure_code")
            .and_then(Value::as_str)
            .filter(|code| !code.is_empty())
        else {
            continue;
        };
        let Ok(authority) = parse_native_recovery_authority(native_recovery) else {
            continue;
        };
        if authority.run_id != run_id
            || authority.attempt != attempt
            || authority.review_id != review_id
            || authority.campaign_id != campaign_id
            || authority.experiment_id != experiment_id
        {
            continue;
        }
        let Some(entry_notes) = serde_json::to_string(entry).ok() else {
            continue;
        };
        let expected = NativeResearchBindingExpectation {
            review_id,
            campaign_id,
            experiment_id,
            attempt,
            session_generation: authority.session_generation,
            agent_run_id: run_id,
            state: "completed",
            failure_code: Some(failure_code),
            campaign_session: None,
        };
        let confirmed = native_research_historical_authority(Some(&entry_notes), &expected)
            .is_some_and(|parsed| parsed.cleanup_complete);
        let pending_campaign_session = if authority.fresh_launch {
            entry
                .get("confirmed_session_id")
                .and_then(Value::as_str)
        } else {
            Some(authority.session_id.as_str())
        };
        let pending = native_research_authority(
            Some(&entry_notes),
            &NativeResearchBindingExpectation {
                state: "retry_wait",
                campaign_session: pending_campaign_session,
                ..expected
            },
        )
        .is_some_and(|parsed| parsed.cleanup_complete);
        if (confirmed || pending)
            && detached_native_cleanup_lineage_valid(
                connection,
                run_id,
                campaign_id,
                experiment_id,
                event_id,
                campaign_project_id,
            )?
        {
            valid_entries += 1;
        }
    }
    Ok((candidate_entries, valid_entries))
}

fn detached_native_cleanup_lineage_valid(
    connection: &Connection,
    run_id: i64,
    campaign_id: &str,
    experiment_id: &str,
    event_id: i64,
    campaign_project_id: &str,
) -> Result<bool, AppError> {
    let lineage = connection
        .query_row(
            "SELECT owner.project_id, owner.execution_kind, owner.status,
                    owner.launch_gate_state, owner.primary_event_id,
                    event.project_id, event.kind, event.campaign_id,
                    event.experiment_id, experiment.campaign_id,
                    (SELECT COUNT(*) FROM campaign_research AS research_state
                       WHERE research_state.campaign_id = ?3),
                    (SELECT COUNT(*) FROM agent_run_events AS link
                       WHERE link.project_id = owner.project_id
                         AND link.run_id = owner.run_id
                         AND link.event_id = ?2),
                    (SELECT COUNT(*) FROM agent_run_events AS link
                       WHERE link.project_id = owner.project_id
                         AND link.run_id = owner.run_id)
             FROM agent_runs AS owner
             LEFT JOIN events AS event
               ON event.project_id = owner.project_id AND event.event_id = ?2
             LEFT JOIN experiments AS experiment
               ON experiment.experiment_id = ?4
             WHERE owner.run_id = ?1",
            params![run_id, event_id, campaign_id, experiment_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, i64>(11)?,
                    row.get::<_, i64>(12)?,
                ))
            },
        )
        .optional()
        .map_err(database_error("read detached native research lineage"))?;
    let Some((
        owner_project_id,
        owner_execution_kind,
        owner_status,
        owner_gate_state,
        owner_primary_event_id,
        event_project_id,
        event_kind,
        event_campaign_id,
        event_experiment_id,
        experiment_campaign_id,
        campaign_research_count,
        review_event_link_count,
        total_event_link_count,
    )) = lineage
    else {
        return Ok(false);
    };
    Ok(owner_project_id == campaign_project_id
        && owner_execution_kind == "campaign_research"
        && matches!(
            owner_status.as_str(),
            "completed" | "failed" | "timed_out" | "cancelled"
        )
        && matches!(owner_gate_state.as_str(), "released" | "failed")
        && owner_primary_event_id == event_id
        && event_project_id.as_deref() == Some(campaign_project_id)
        && event_kind.as_deref() == Some("campaign_research")
        && event_campaign_id.as_deref() == Some(campaign_id)
        && event_experiment_id.as_deref() == Some(experiment_id)
        && experiment_campaign_id.as_deref() == Some(campaign_id)
        && campaign_research_count == 1
        && review_event_link_count == 1
        && total_event_link_count == 1)
}

fn detached_native_cleanup_evidence(
    connection: &Connection,
    run_id: i64,
) -> Result<DetachedNativeCleanupEvidence, AppError> {
    let mut statement = connection
        .prepare(
            "SELECT review.review_id, review.campaign_id, review.experiment_id,
                    review.event_id, review.notes_json, campaign.project_id
             FROM research_reviews AS review
             JOIN campaigns AS campaign ON campaign.campaign_id = review.campaign_id",
        )
        .map_err(database_error("prepare detached native research history query"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
            ))
        })
        .map_err(database_error("query detached native research history"))?;
    let mut evidence = DetachedNativeCleanupEvidence::default();
    for row in rows {
        let (review_id, campaign_id, experiment_id, event_id, notes_json, project_id) =
            row.map_err(database_error("read detached native research history"))?;
        let (candidate_count, valid_count) = retry_history_native_cleanup_counts(
            connection,
            notes_json.as_deref(),
            run_id,
            &review_id,
            &campaign_id,
            &experiment_id,
            event_id,
            &project_id,
        )?;
        if candidate_count > 0 {
            evidence.project_ids.insert(project_id);
            evidence.candidate_count += candidate_count;
            evidence.valid_count += valid_count;
        }
    }
    Ok(evidence)
}

fn native_research_owner_lineage_valid(row: &NativeResearchOwnerRow) -> bool {
    row.event_id.is_some()
        && row.owner_project_id.as_deref() == Some(row.project_id.as_str())
        && row.owner_execution_kind.as_deref() == Some("campaign_research")
        && row.owner_primary_event_id == row.event_id
        && row.event_project_id.as_deref() == Some(row.project_id.as_str())
        && row.event_kind.as_deref() == Some("campaign_research")
        && row.event_campaign_id.as_deref() == Some(row.campaign_id.as_str())
        && row.event_experiment_id.as_deref() == Some(row.experiment_id.as_str())
        && row.experiment_campaign_id.as_deref() == Some(row.campaign_id.as_str())
        && row.review_event_link_count == 1
        && row.total_event_link_count == 1
        && row.bound_review_count == 1
}

fn native_research_owner_is_complete(row: &NativeResearchOwnerRow) -> bool {
    let terminal_owner = matches!(
        row.owner_status.as_deref(),
        Some("completed" | "failed" | "timed_out" | "cancelled")
    );
    let gate_released = matches!(row.owner_gate_state.as_deref(), Some("released" | "failed"));
    if !terminal_owner || !gate_released {
        return false;
    }
    if row.review_id.is_empty() {
        return row.detached_cleanup_complete
            && row.detached_campaign_project_ids.len() == 1
            && row.detached_campaign_project_ids.contains(&row.project_id);
    }
    if !native_research_owner_lineage_valid(row) {
        return false;
    }
    if matches!(row.state.as_str(), "completed" | "discarded") {
        return native_research_historical_authority_complete(row);
    }
    if row.review_generation != row.campaign_generation {
        return false;
    }
    let expected = NativeResearchBindingExpectation {
        review_id: &row.review_id,
        campaign_id: &row.campaign_id,
        experiment_id: &row.experiment_id,
        attempt: row.attempt,
        session_generation: row.review_generation,
        agent_run_id: row.agent_run_id,
        state: &row.state,
        failure_code: row.failure_code.as_deref(),
        campaign_session: row.campaign_session.as_deref(),
    };
    native_research_authority(row.notes_json.as_deref(), &expected)
        .is_some_and(|authority| authority.cleanup_complete)
}

fn native_research_historical_authority_complete(row: &NativeResearchOwnerRow) -> bool {
    let Some(notes_json) = row.notes_json.as_deref() else {
        return false;
    };
    let expected = NativeResearchBindingExpectation {
        review_id: &row.review_id,
        campaign_id: &row.campaign_id,
        experiment_id: &row.experiment_id,
        attempt: row.attempt,
        session_generation: row.review_generation,
        agent_run_id: row.agent_run_id,
        state: &row.state,
        failure_code: row.failure_code.as_deref(),
        campaign_session: None,
    };
    native_research_historical_authority(Some(notes_json), &expected).is_some_and(|authority| {
        authority.cleanup_complete
    })
}

fn startup_research_owner_from_row(
    row: NativeResearchOwnerRow,
    marker_absent: bool,
) -> StartupResearchOwner {
    let expected = NativeResearchBindingExpectation {
        review_id: &row.review_id,
        campaign_id: &row.campaign_id,
        experiment_id: &row.experiment_id,
        attempt: row.attempt,
        session_generation: row.review_generation,
        agent_run_id: row.agent_run_id,
        state: &row.state,
        failure_code: row.failure_code.as_deref(),
        campaign_session: row.campaign_session.as_deref(),
    };
    let authority = if matches!(row.state.as_str(), "completed" | "discarded") {
        native_research_historical_authority(row.notes_json.as_deref(), &expected)
    } else {
        native_research_authority(row.notes_json.as_deref(), &expected)
    };
    let status = row.owner_status.unwrap_or_default();
    let gate_state = row.owner_gate_state.unwrap_or_default();
    let policy_code = row.owner_policy_code;
    let failure_stage = row.owner_failure_stage;
    let review_state = row.state;
    let failure_code = row.failure_code;
    StartupResearchOwner {
        run_id: row.agent_run_id,
        project_id: row.project_id,
        review_id: row.review_id,
        pid: row.owner_pid,
        status: status.clone(),
        gate_state: gate_state.clone(),
        policy_code: policy_code.clone(),
        failure_stage: failure_stage.clone(),
        log_path: row.owner_log_path,
        review_state: review_state.clone(),
        failure_code: failure_code.clone(),
        notes_json: row.notes_json,
        marker_absent,
        authority,
        original_status: status,
        original_gate_state: gate_state,
        original_policy_code: policy_code,
        original_failure_stage: failure_stage,
    }
}

pub(crate) fn startup_research_owner_snapshot(
    db: &Db,
    preserved_run_ids: &[i64],
    absent_pending_marker_ids: &BTreeSet<i64>,
) -> Result<std::collections::BTreeMap<i64, StartupResearchOwner>, AppError> {
    let repository = ResearchRepository::new(db);
    let mut owners = std::collections::BTreeMap::new();
    for run_id in preserved_run_ids {
        if let Some(owner) = repository.startup_native_owner(
            *run_id,
            absent_pending_marker_ids.contains(run_id),
        )? {
            owners.insert(*run_id, owner);
        }
    }
    for owner in repository.list_native_cleanup_pending_terminal_runs()? {
        owners.entry(owner.run_id).or_insert(owner);
    }
    Ok(owners)
}

struct NativeRecoveryCleanupExpectation<'a> {
    review_id: &'a str,
    campaign_id: &'a str,
    experiment_id: &'a str,
    attempt: i64,
    session_generation: i64,
    agent_run_id: i64,
    state: &'a str,
    failure_code: Option<&'a str>,
    campaign_session: Option<&'a str>,
}

fn native_recovery_cleanup_complete(
    notes_json: Option<&str>,
    expected: &NativeRecoveryCleanupExpectation<'_>,
) -> bool {
    native_research_authority(
        notes_json,
        &NativeResearchBindingExpectation {
            review_id: expected.review_id,
            campaign_id: expected.campaign_id,
            experiment_id: expected.experiment_id,
            attempt: expected.attempt,
            session_generation: expected.session_generation,
            agent_run_id: expected.agent_run_id,
            state: expected.state,
            failure_code: expected.failure_code,
            campaign_session: expected.campaign_session,
        },
    )
    .is_some_and(|authority| authority.cleanup_complete)
}

fn parse_research_notes(notes_json: Option<&str>) -> Result<Value, AppError> {
    let notes = notes_json
        .map(|notes| {
            serde_json::from_str::<Value>(notes).map_err(|source| AppError::Serialization {
                operation: "parse research review notes",
                source,
            })
        })
        .transpose()?
        .unwrap_or_else(|| json!({}));
    if !notes.is_object() {
        return Err(validation_error(
            "research.notes_json",
            "must contain a JSON object",
        ));
    }
    Ok(notes)
}

fn native_recovery_authority_matches(
    authority: &NativeRecoveryAuthority,
    binding: &ResearchLaunchBinding,
    agent_run_id: i64,
    identity: &PrivateRunTempRecoveryIdentityV1,
    fresh_launch: bool,
) -> bool {
    authority.version == PrivateRunTempRecoveryIdentityV1::VERSION
        && authority.run_id == agent_run_id
        && authority.review_id == binding.review_id
        && authority.campaign_id == binding.campaign_id
        && authority.experiment_id == binding.experiment_id
        && authority.attempt == binding.attempt
        && authority.session_generation == binding.session_generation
        && authority.fresh_launch == fresh_launch
        && authority.session_id == binding.session_id
        && authority.service_root_identity == identity.service_root_identity
        && authority.temp_identity == identity.temp_identity
        && matches!(authority.cleanup.phase.as_str(), "pending" | "complete")
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

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::{
        db::{
            AgentRunRepository, CampaignRepository, EventRepository, ExperimentRepository,
            ProjectRepository, StartCampaignRequest, TaskObservationRepository,
        },
        execution_policy::CampaignLimits,
        models::{
            AgentContextMode, AgentRunStatus, ExecutionProjection, NewAgentRun, NewProject,
            NewTaskObservation, ProposalKind,
        },
        proposals::{self, ProposalInput},
        pueue::PueueTask,
        reconcile::{managed_task_run_signature, task_signature},
        state::ObjectiveSnapshot,
    };

    fn detached_history_fixture() -> (tempfile::TempDir, Db, i64, String) {
        let temp = tempfile::tempdir().expect("detached history tempdir");
        let root = temp.path().join("project");
        fs::create_dir_all(&root).expect("detached history project");
        let config_path = root.join("config.toml");
        fs::write(&config_path, "fixture").expect("detached history config");
        let db = Db::open(&temp.path().join("state.sqlite3")).expect("detached history db");
        let project_id = "detached-history-project";
        let campaign_id = "detached-history-campaign";
        let experiment_id = "detached-history-experiment";
        let task = PueueTask {
            id: 41,
            group: "detached-history-group".to_owned(),
            command: "python train.py".to_owned(),
            state: "Running".to_owned(),
            enqueued_at: Some("900".to_owned()),
            started_at: Some("1000".to_owned()),
            ended_at: None,
            result: None,
        };
        let raw_task_signature = task_signature(&task);
        let task_signature = managed_task_run_signature(&task)
            .expect("detached history managed task signature");
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                project_id,
                root,
                "detached-history-group",
                config_path,
                900,
            ))
            .expect("detached history project registration");
        let objective = ObjectiveSnapshot {
            text: "detached history objective".to_owned(),
            digest: "detached-history-objective".to_owned(),
        };
        let argv = vec![task.command.clone()];
        let baseline = proposals::validate_initial_baseline(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: "detached history baseline".to_owned(),
                source_experiment_id: None,
                argv: argv.clone(),
                working_directory: ".".to_owned(),
                expected_evidence: vec!["loss".to_owned()],
            },
            &objective.digest,
        )
        .expect("detached history baseline proposal");
        CampaignRepository::new(&db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id,
                    project_id,
                    objective: &objective,
                    initial_argv: &argv,
                    baseline: &baseline,
                    submission_id: "detached-history-submission",
                    experiment_id,
                    proposal_id: "detached-history-proposal",
                    metadata: &json!({}),
                    origin_agent_run_id: None,
                    objective_metric: None,
                    now: 900,
                },
                &CampaignLimits::default(),
            )
            .expect("detached history campaign");
        ExperimentRepository::new(&db)
            .mark_submitting(experiment_id, 901)
            .expect("detached history submission");
        ExperimentRepository::new(&db)
            .mark_accepted(experiment_id, 41, &task_signature, 902)
            .expect("detached history experiment");
        TaskObservationRepository::new(&db)
            .upsert(&NewTaskObservation::new(
                project_id,
                raw_task_signature,
                41,
                "detached-history-group",
                argv,
                "Running",
                Some(900),
                Some(1_000),
                None,
                None,
                1_001,
            ))
            .expect("detached history observation");
        let repository = ResearchRepository::new(&db);
        repository
            .ensure_campaign(campaign_id)
            .expect("detached history state");
        repository
            .schedule_running(campaign_id, 1_000, 30, 2_799)
            .expect("detached history schedule");
        let review = repository
            .claim_due(campaign_id, experiment_id, &task_signature, 2_800)
            .expect("detached history claim")
            .expect("detached history review");
        let event_id = repository
            .event_id(&review.review_id)
            .expect("detached history event");
        EventRepository::new(&db)
            .claim_by_id(project_id, event_id, 2_900)
            .expect("detached history event claim")
            .expect("detached history event row");
        let run = AgentRunRepository::new(&db)
            .insert_with_events(
                &NewAgentRun::with_context(
                    project_id,
                    event_id,
                    None,
                    AgentRunStatus::Starting,
                    2_901,
                    temp.path().join("agent.log"),
                    AgentContextMode::Fresh,
                    None,
                    Vec::new(),
                )
                .with_execution(
                    ExecutionProjection::new("campaign_research", "/bin/sh", "fixture")
                        .expect("detached history execution"),
                ),
                &[event_id],
            )
            .expect("detached history run");
        let authority = json!({
            "version": 1,
            "run_id": run.run_id,
            "review_id": review.review_id,
            "campaign_id": campaign_id,
            "experiment_id": experiment_id,
            "attempt": 1,
            "session_generation": 0,
            "fresh_launch": true,
            "session_id": "11111111-1111-4111-8111-111111111111",
            "service_root_identity": {
                "device": 1, "inode": 2, "owner": 3, "mode": 448,
                "resolution": "fixture-root"
            },
            "temp_identity": {
                "device": 1, "inode": 4, "owner": 3, "mode": 448,
                "mount": [1, 2],
                "service_identity": {"device": 1, "inode": 5, "owner": 3, "mode": 448},
                "parent_identity": {"device": 1, "inode": 6, "owner": 3, "mode": 448}
            },
            "cleanup": {"phase": "complete", "completed_at": 3_000}
        });
        let notes = json!({
            "retry_history": [{
                "attempt": 1,
                "agent_run_id": run.run_id,
                "failure_code": "research_output_invalid",
                "planned_session_id": "11111111-1111-4111-8111-111111111111",
                "confirmed_session_id": "11111111-1111-4111-8111-111111111111",
                "session_binding": "confirmed",
                "native_recovery": authority
            }]
        });
        let connection = db
            .connect()
            .expect("detached history update connection");
        connection
            .execute(
                "UPDATE agent_runs
                 SET status = 'failed', finished_at = 3_001,
                     launch_gate_state = 'failed'
                 WHERE run_id = ?1",
                [run.run_id],
            )
            .expect("detached history terminal run");
        connection
            .execute(
                "UPDATE research_reviews
                 SET state = 'retry_wait', attempt = 2, agent_run_id = NULL,
                     failure_code = 'research_output_invalid', notes_json = ?1
                 WHERE review_id = ?2",
                rusqlite::params![notes.to_string(), review.review_id],
            )
            .expect("detached history review");
        connection
            .execute(
                "UPDATE events SET status = 'retry_wait', lease_until = NULL,
                     not_before = 3_001, last_error = 'research_output_invalid'
                 WHERE event_id = ?1",
                [event_id],
            )
            .expect("detached history event");
        (temp, db, run.run_id, project_id.to_owned())
    }

    fn complete_owner_row() -> NativeResearchOwnerRow {
        NativeResearchOwnerRow {
            review_id: "review".to_owned(),
            campaign_id: "campaign".to_owned(),
            experiment_id: "experiment".to_owned(),
            attempt: 1,
            review_generation: 0,
            agent_run_id: 7,
            state: "completed".to_owned(),
            failure_code: None,
            campaign_session: Some("session".to_owned()),
            campaign_generation: 0,
            notes_json: None,
            event_id: Some(11),
            project_id: "project".to_owned(),
            owner_project_id: Some("project".to_owned()),
            owner_execution_kind: Some("campaign_research".to_owned()),
            owner_status: Some("completed".to_owned()),
            owner_gate_state: Some("released".to_owned()),
            owner_pid: None,
            owner_primary_event_id: Some(11),
            owner_log_path: Some(PathBuf::from("/tmp/research.log")),
            owner_policy_code: None,
            owner_failure_stage: None,
            event_project_id: Some("project".to_owned()),
            event_kind: Some("campaign_research".to_owned()),
            event_campaign_id: Some("campaign".to_owned()),
            event_experiment_id: Some("experiment".to_owned()),
            experiment_campaign_id: Some("campaign".to_owned()),
            review_event_link_count: 1,
            total_event_link_count: 1,
            bound_review_count: 1,
            detached_cleanup_complete: false,
            detached_campaign_project_ids: BTreeSet::new(),
        }
    }

    #[test]
    fn native_owner_lineage_rejects_extra_or_mismatched_event_links() {
        let mut row = complete_owner_row();
        assert!(native_research_owner_lineage_valid(&row));
        row.total_event_link_count = 2;
        assert!(!native_research_owner_lineage_valid(&row));
        row.total_event_link_count = 1;
        row.owner_primary_event_id = Some(12);
        assert!(!native_research_owner_lineage_valid(&row));
    }

    #[test]
    fn detached_cleanup_requires_one_valid_history_project() {
        let mut row = complete_owner_row();
        row.review_id.clear();
        row.event_id = None;
        row.detached_cleanup_complete = true;
        row.detached_campaign_project_ids
            .insert("project".to_owned());
        assert!(native_research_owner_is_complete(&row));

        row.detached_campaign_project_ids
            .insert("campaign-project".to_owned());
        assert!(!native_research_owner_is_complete(&row));
    }

    #[test]
    fn retire_startup_native_owner_preserves_classified_retry_wake() {
        let (_temp, db, run_id, project_id) = detached_history_fixture();
        let connection = db.connect().expect("classified retry read connection");
        let (review_id, event_id, notes_json): (String, i64, String) = connection
            .query_row(
                "SELECT review_id, event_id, notes_json
                 FROM research_reviews
                 WHERE campaign_id = 'detached-history-campaign'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("classified retry review");
        let mut notes: Value = serde_json::from_str(&notes_json).expect("classified retry notes");
        let authority = notes["retry_history"][0]["native_recovery"].clone();
        let planned_session = authority["session_id"]
            .as_str()
            .expect("classified retry planned session")
            .to_owned();
        let confirmed_session = "22222222-2222-4222-8222-222222222222";
        notes["native_recovery"] = authority;
        notes["planned_session_id"] = json!(planned_session);
        notes["confirmed_session_id"] = json!(confirmed_session);
        notes["session_binding"] = json!("confirmed");
        let notes_json = notes.to_string();
        let original_wake = 3_001_i64;
        connection
            .execute(
                "UPDATE campaign_research
                 SET session_id = ?1
                 WHERE campaign_id = 'detached-history-campaign'",
                [confirmed_session],
            )
            .expect("classified retry campaign session");
        connection
            .execute(
                "UPDATE research_reviews
                 SET state = 'retry_wait', attempt = 1, agent_run_id = ?1,
                     failure_code = 'research_output_invalid', not_before = ?2,
                     notes_json = ?3
                 WHERE review_id = ?4",
                rusqlite::params![run_id, original_wake, notes_json, review_id],
            )
            .expect("classified retry review binding");
        connection
            .execute(
                "UPDATE events
                 SET status = 'dispatched', lease_until = NULL, not_before = ?1,
                     last_error = NULL
                 WHERE event_id = ?2",
                rusqlite::params![original_wake, event_id],
            )
            .expect("classified retry event binding");
        drop(connection);

        let connection = db.connect().expect("classified retry owner connection");
        let row = native_research_owner_rows(&connection, None)
            .expect("read classified retry owner")
            .into_iter()
            .find(|row| row.agent_run_id == run_id)
            .expect("classified retry owner row");
        let owner = startup_research_owner_from_row(row, false);
        assert_eq!(owner.review_state, "retry_wait");
        assert!(owner.authority.is_some());
        drop(connection);

        let repository = ResearchRepository::new(&db);
        assert!(repository
            .retire_startup_native_owner(&owner, None, 5_000)
            .expect("retire classified retry owner"));

        let connection = db.connect().expect("classified retry result connection");
        let (event_status, event_not_before): (EventStatus, i64) = connection
            .query_row(
                "SELECT status, not_before FROM events WHERE event_id = ?1",
                [event_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("classified retry event result");
        let (review_state, review_failure, review_not_before, review_session, after_notes): (
            String,
            Option<String>,
            i64,
            Option<String>,
            String,
        ) = connection
            .query_row(
                "SELECT state, failure_code, not_before,
                        (SELECT session_id FROM campaign_research
                         WHERE campaign_id = 'detached-history-campaign'), notes_json
                 FROM research_reviews WHERE review_id = ?1",
                [review_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .expect("classified retry review result");
        assert_eq!(event_status, EventStatus::RetryWait);
        assert_eq!(event_not_before, original_wake);
        assert_eq!(review_state, "retry_wait");
        assert_eq!(review_failure.as_deref(), Some("research_output_invalid"));
        assert_eq!(review_not_before, original_wake);
        assert_eq!(review_session.as_deref(), Some(confirmed_session));
        assert_eq!(after_notes, notes_json);
        assert_eq!(project_id, "detached-history-project");
    }

    #[test]
    fn detached_history_missing_campaign_state_remains_a_blocker() {
        let (_temp, db, run_id, project_id) = detached_history_fixture();
        let connection = db.connect().expect("detached history read connection");
        let owners = native_research_owner_rows(&connection, None)
            .expect("read detached history owners");
        let detached = owners
            .iter()
            .find(|owner| owner.agent_run_id == run_id && owner.review_id.is_empty())
            .expect("old detached owner must remain in blocker universe");
        assert!(detached.detached_cleanup_complete);
        assert!(detached.detached_campaign_project_ids.contains(&project_id));

        connection
            .execute(
                "DELETE FROM campaign_research WHERE campaign_id = ?1",
                ["detached-history-campaign"],
            )
            .expect("remove campaign research state");
        let owners = native_research_owner_rows(&connection, None)
            .expect("read missing-state detached owners");
        let detached = owners
            .iter()
            .find(|owner| owner.agent_run_id == run_id && owner.review_id.is_empty())
            .expect("missing-state detached owner must remain");
        assert!(!detached.detached_cleanup_complete);
        assert!(detached.detached_campaign_project_ids.contains(&project_id));
        assert!(!native_research_owner_is_complete(detached));
    }
}
