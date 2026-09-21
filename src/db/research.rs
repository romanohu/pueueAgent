use std::{
    collections::BTreeSet,
    path::PathBuf,
    time::{Duration, Instant},
};

use rusqlite::{
    params,
    types::{Type, ValueRef},
    Connection, OptionalExtension, Row, Transaction, TransactionBehavior,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    environment::{
        campaign_experiment_runtime_argv, PrivateRunTempRecoveryIdentityV1, PrivateRunTempRecoveryRootIdentity,
        PrivateRunTempRecoveryTempIdentity,
        RecoveredPrivateRunTempCleanup, validate_research_id,
    },
    models::{
        CampaignState, EventStatus, ExperimentStatus, Incident, ProposalKind, ProposalStatus,
        Project, SubmissionKind, SubmissionStatus, TaskObservation, TerminationRequest,
    },
    pueue::PueueTask,
    proposals::{self, ProposalInput},
    reconcile::{
        managed_task_run_signature, managed_task_run_signature_for_observation, task_signature,
        try_canonical_command_display_os,
    },
    research_checkpoint::{
        checkpoint_source_layout, checkpoint_support_from_persisted_context,
        checkpoint_learning_spec_digest, parse_prepared_checkpoint, select_checkpoint_support,
        CheckpointSupportEvidenceV1, PreparedCheckpoint,
    },
    research_protocol::{parse_research_answer, CheckpointRequest, ResearchAnswer},
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
const MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES: usize =
    crate::research_checkpoint::MAX_PREPARED_CHECKPOINT_BYTES;
const _: () = assert!(MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES == 131_072);

/// Rows that claim a checkpoint successor either carry a non-null checkpoint
/// marker or retain the checkpoint-only successor stage.  A completed row
/// with the same successor shape and no decision-cycle link is also retained
/// as checkpoint history, even if its marker was lost.
pub(super) const CHECKPOINT_INCOMING_CLAIM_PREDICATE: &str =
    "(review.checkpoint_json IS NOT NULL
      OR (review.successor_experiment_id IS NOT NULL
          AND (review.operation_stage = 'successor_reserved'
               OR (review.decision_cycle_id IS NULL
                   AND review.state = 'completed'
                   AND review.operation_stage IS NULL))))";
pub(super) const CHECKPOINT_PRE_ADD_FAILURE_CODE: &str =
    "research_checkpoint_verification_failed";

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
    pub checkpoint_json: Option<String>,
    pub(crate) checkpoint_json_state: CheckpointJsonState,
    pub session_generation: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointSqliteStorageClass {
    Null,
    Integer,
    Real,
    Text,
    Blob,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointJsonState {
    Missing,
    BoundedText,
    Invalid {
        storage_class: CheckpointSqliteStorageClass,
        byte_len: Option<i64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResearchOwnership {
    None,
    Open(Option<ResearchOwnershipSnapshot>),
    Attached(ResearchOwnershipSnapshot),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResearchOwnershipSnapshot {
    pub review_id: String,
    pub project_id: String,
    pub campaign_id: String,
    pub source_experiment_id: String,
    pub managed_task_signature: String,
    pub source_task_id: Option<i64>,
    pub attempt: i64,
    pub session_generation: i64,
    pub event_id: Option<i64>,
    pub operation_stage: Option<String>,
    pub agent_run_id: Option<i64>,
    pub termination_request_id: Option<i64>,
    pub decision_cycle_id: Option<String>,
    pub successor_experiment_id: Option<String>,
    pub recovery_required: bool,
}

#[derive(Debug)]
pub(crate) struct ReadyResearchAction {
    pub owner: ResearchOwnershipSnapshot,
    pub context_json: String,
    pub context_digest: String,
    pub response_json: String,
    pub answer: ResearchAnswer,
    pub notes_json: String,
    pub campaign_objective_digest: String,
    pub raw_task_signature: String,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CheckpointSourceAuthority {
    pub(crate) project: Project,
    pub(crate) source: super::campaigns::ManagedSubmissionIntent,
    pub(crate) observation: TaskObservation,
    pub(crate) support: CheckpointSupportEvidenceV1,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CheckpointSourceAuthorityRead {
    Supported(CheckpointSourceAuthority),
    Unsupported { reason: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointSuccessorPreflight {
    Available,
    Deferred,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CheckpointSuccessorAdmission {
    Ready(super::campaigns::ManagedSubmissionIntent),
    Deferred,
    Blocked,
}

/// A dispatch witness contains the exact values rechecked by the later
/// reserved-to-submitting CAS.  The fields remain private so callers cannot
/// manufacture authority from an experiment id or pathname.
#[derive(Debug)]
pub(crate) struct CheckpointDispatchAuthority {
    pub(super) checkpoint: PreparedCheckpoint,
    pub(super) raw_checkpoint: String,
    pub(super) project_id: String,
    pub(super) campaign_id: String,
    pub(super) review_id: String,
    pub(super) source_experiment_id: String,
    pub(super) proposal_id: String,
    pub(super) submission_id: String,
    pub(super) successor_experiment_id: String,
    pub(super) successor_status: Option<ExperimentStatus>,
    pub(super) successor_attempt: Option<i64>,
    pub(super) reservation_window_ends_at: Option<i64>,
    pub(super) termination_request_id: Option<i64>,
}

impl CheckpointDispatchAuthority {
    pub(crate) fn checkpoint(&self) -> &PreparedCheckpoint {
        &self.checkpoint
    }

    pub(crate) fn successor_experiment_id(&self) -> &str {
        &self.successor_experiment_id
    }

    pub(crate) fn successor_status(&self) -> ExperimentStatus {
        self.successor_status
            .expect("selected checkpoint dispatch authority must include successor status")
    }
}

pub(super) fn checkpoint_successor_witness(
    transaction: &Transaction<'_>,
    successor_experiment_id: &str,
) -> Result<Option<(i64, i64)>, AppError> {
    if checkpoint_successor_reservation_count(transaction, successor_experiment_id)? != 1 {
        return Ok(None);
    }
    transaction
        .query_row(
            "SELECT experiment.attempt, reservation.window_ends_at
             FROM experiments AS experiment
             JOIN budget_reservations AS reservation
               ON reservation.experiment_id = experiment.experiment_id
              AND reservation.dimension = 'experiment'
             WHERE experiment.experiment_id = ?1",
            [successor_experiment_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(database_error("read checkpoint successor witness"))
}

pub(super) fn checkpoint_successor_reservation_count(
    transaction: &Transaction<'_>,
    successor_experiment_id: &str,
) -> Result<i64, AppError> {
    transaction
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations
             WHERE experiment_id = ?1 AND dimension = 'experiment'",
            [successor_experiment_id],
            |row| row.get(0),
        )
        .map_err(database_error("count checkpoint successor reservations"))
}

#[derive(Debug)]
pub(crate) enum CheckpointDispatchSelection {
    NotCheckpoint,
    Ready(CheckpointDispatchAuthority),
    Blocked,
}

enum SourceAuthorityExpectation<'a> {
    Fresh {
        expected: &'a ReadyResearchAction,
        request: &'a CheckpointRequest,
    },
    Historical {
        checkpoint: &'a PreparedCheckpoint,
    },
}

fn source_authority_owner_fields_from_row(
    row: &Row<'_>,
) -> rusqlite::Result<(Option<i64>, Option<String>, Option<String>)> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
}

#[derive(Debug)]
pub(crate) struct CompletedResearchHandoff {
    pub owner: ResearchOwnershipSnapshot,
    pub context_json: String,
    pub context_digest: String,
    pub response_json: String,
    pub answer: ResearchAnswer,
    pub notes_json: String,
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
        CASE
          WHEN typeof(checkpoint_json) = 'text'
           AND length(CAST(checkpoint_json AS BLOB)) BETWEEN 1 AND 131072
          THEN checkpoint_json
        END,
        created_at, started_at, finished_at, updated_at,
        typeof(checkpoint_json), length(CAST(checkpoint_json AS BLOB))
    FROM research_reviews";
const LAUNCH_REVIEW_SELECT: &str = "SELECT review.review_id, review.campaign_id,
        review.experiment_id, review.task_signature, review.attempt, review.state,
        review.operation_stage, review.agent_run_id, review.context_json,
        review.context_digest, review.response_json, review.termination_request_id,
        review.successor_experiment_id, review.evidence_schema_version,
        review.session_generation, review.event_id, review.not_before,
        review.notes_json, review.failure_code, review.decision_cycle_id,
        CASE
          WHEN typeof(review.checkpoint_json) = 'text'
           AND length(CAST(review.checkpoint_json AS BLOB)) BETWEEN 1 AND 131072
          THEN review.checkpoint_json
        END,
        review.created_at, review.started_at, review.finished_at,
        review.updated_at, typeof(review.checkpoint_json),
        length(CAST(review.checkpoint_json AS BLOB))
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
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction()
            .map_err(database_error("begin research successor ownership check"))?;
        let lineage: Option<(String, String)> = transaction
            .query_row(
                "SELECT source.campaign_id, campaign.project_id
                 FROM experiments AS source
                 JOIN campaigns AS campaign ON campaign.campaign_id = source.campaign_id
                 WHERE source.experiment_id = ?1",
                [experiment_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(database_error("read research successor lineage"))?;
        let Some((campaign_id, project_id)) = lineage else {
            transaction
                .commit()
                .map_err(database_error("commit absent research successor ownership"))?;
            return Ok(false);
        };
        let ownership = research_ownership_in_transaction(
            &transaction,
            &project_id,
            &campaign_id,
            experiment_id,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit research successor ownership check"))?;
        Ok(!matches!(ownership, ResearchOwnership::None))
    }

    pub(crate) fn ready_reviews(
        &self,
        limit: usize,
    ) -> Result<Vec<ResearchReview>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{REVIEW_SELECT}
                 WHERE state = 'ready' AND operation_stage IS NULL
                 ORDER BY updated_at, review_id
                 LIMIT ?1"
            ))
            .map_err(database_error("prepare ready research action query"))?;
        let rows = statement
            .query_map([limit.min(MAX_RESEARCH_REVIEW_LIST as usize) as i64], review_from_row)
            .map_err(database_error("query ready research actions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read ready research actions"))
    }

    pub(crate) fn rotate_ready_action_review(
        &self,
        review_id: &str,
        now: i64,
    ) -> Result<(), AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin ready research action rotation"))?;
        transaction
            .execute(
                "UPDATE research_reviews
                 SET updated_at = CASE WHEN updated_at < ?1 THEN ?1 ELSE updated_at END
                 WHERE review_id = ?2 AND state = 'ready'
                   AND operation_stage IS NULL",
                params![now, review_id],
            )
            .map_err(database_error("rotate ready research action"))?;
        transaction
            .commit()
            .map_err(database_error("commit ready research action rotation"))
    }

    pub(crate) fn open_action_reviews(
        &self,
        limit: usize,
    ) -> Result<Vec<ResearchReview>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{REVIEW_SELECT}
                 WHERE state = 'ready' AND operation_stage IN {OPEN_OPERATION_STAGES}
                 ORDER BY updated_at, review_id
                 LIMIT ?1"
            ))
            .map_err(database_error("prepare open research action query"))?;
        let rows = statement
            .query_map([limit.min(MAX_RESEARCH_REVIEW_LIST as usize) as i64], review_from_row)
            .map_err(database_error("query open research actions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read open research actions"))
    }

    pub(crate) fn checkpoint_dispatch_authority(
        &self,
        project_id: &str,
        successor_experiment_id: &str,
        now: i64,
    ) -> Result<CheckpointDispatchSelection, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin checkpoint dispatch authority"))?;
        let mut claims = transaction
            .prepare(&format!(
                "SELECT review.review_id, campaign.project_id, review.campaign_id,
                        review.experiment_id, review.task_signature, review.attempt,
                        review.successor_experiment_id, review.operation_stage,
                        CASE
                          WHEN typeof(review.checkpoint_json) = 'text'
                           AND length(CAST(review.checkpoint_json AS BLOB)) BETWEEN 1 AND 131072
                          THEN review.checkpoint_json
                        END,
                        typeof(review.checkpoint_json),
                        length(CAST(review.checkpoint_json AS BLOB))
                 FROM research_reviews AS review
                 LEFT JOIN campaigns AS campaign ON campaign.campaign_id = review.campaign_id
                 WHERE review.successor_experiment_id = ?1
                   AND {CHECKPOINT_INCOMING_CLAIM_PREDICATE}
                 ORDER BY review.review_id LIMIT 2"
            ))
            .map_err(database_error("prepare checkpoint dispatch claims"))?;
        let rows = claims
            .query_map([successor_experiment_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                ))
            })
            .map_err(database_error("query checkpoint dispatch claims"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read checkpoint dispatch claims"))?;
        drop(claims);
        if rows.is_empty() {
            transaction
                .commit()
                .map_err(database_error("commit ordinary dispatch classification"))?;
            return Ok(CheckpointDispatchSelection::NotCheckpoint);
        }
        if rows.len() != 1 {
            return Err(validation_error(
                "research.checkpoint",
                "successor has ambiguous incoming checkpoint claims",
            ));
        }
        let (
            review_id,
            claim_project_id,
            campaign_id,
            source_experiment_id,
            task_signature,
            attempt,
            linked_successor_id,
            operation_stage,
            raw,
            storage,
            byte_len,
        ) = rows.into_iter().next().expect("one checkpoint claim");
        let Some(claim_project_id) = claim_project_id else {
            return Err(validation_error(
                "research.checkpoint",
                "checkpoint claim has no provable campaign project",
            ));
        };
        if claim_project_id != project_id || linked_successor_id.as_deref() != Some(successor_experiment_id) {
            return Err(validation_error(
                "research.checkpoint",
                "checkpoint claim cannot be attributed to the requested project",
            ));
        }
        let Some(raw) = raw else {
            if storage == "null"
                && byte_len.is_none()
                && operation_stage.as_deref() == Some("successor_reserved")
            {
                if claim_project_id != project_id {
                    return Err(validation_error(
                        "research.checkpoint",
                        "checkpoint claim cannot be attributed to the requested project",
                    ));
                }
                let changed = block_missing_checkpoint_dispatch_review(
                    &transaction,
                    &review_id,
                    &campaign_id,
                    &source_experiment_id,
                    &task_signature,
                    attempt,
                    linked_successor_id.as_deref().unwrap_or_default(),
                    now,
                )?;
                if !changed {
                    return Err(validation_error(
                        "research.checkpoint",
                        "missing checkpoint review changed before blocking",
                    ));
                }
                transaction
                    .commit()
                    .map_err(database_error("commit missing checkpoint dispatch block"))?;
                return Ok(CheckpointDispatchSelection::Blocked);
            }
            let changed = block_checkpoint_dispatch_review(
                &transaction,
                &review_id,
                &campaign_id,
                &source_experiment_id,
                &task_signature,
                attempt,
                now,
            )?;
            if !changed {
                return Err(validation_error(
                    "research.checkpoint",
                    "invalid checkpoint review changed before blocking",
                ));
            }
            transaction
                .commit()
                .map_err(database_error("commit invalid checkpoint dispatch block"))?;
            return Ok(CheckpointDispatchSelection::Blocked);
        };
        if storage != "text"
            || !byte_len.is_some_and(|len| (1..=MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES as i64).contains(&len))
        {
            return Err(validation_error(
                "research.checkpoint",
                "checkpoint claim has invalid bounded storage",
            ));
        }
        let checkpoint = match parse_prepared_checkpoint(&raw) {
            Ok(checkpoint) => checkpoint,
            Err(_) => {
                let changed = block_checkpoint_dispatch_review(
                    &transaction,
                    &review_id,
                    &campaign_id,
                    &source_experiment_id,
                    &task_signature,
                    attempt,
                    now,
                )?;
                if !changed {
                    return Err(validation_error(
                        "research.checkpoint",
                        "invalid checkpoint review changed before blocking",
                    ));
                }
                transaction
                    .commit()
                    .map_err(database_error("commit invalid checkpoint dispatch block"))?;
                return Ok(CheckpointDispatchSelection::Blocked);
            }
        };
        if checkpoint.project_id != project_id
            || checkpoint.campaign_id != campaign_id
            || checkpoint.source_experiment_id != source_experiment_id
            || checkpoint.successor_ids.experiment_id != successor_experiment_id
        {
            let changed = block_checkpoint_dispatch_review(
                &transaction,
                &review_id,
                &campaign_id,
                &source_experiment_id,
                &task_signature,
                attempt,
                now,
            )?;
            if !changed {
                return Err(validation_error(
                    "research.checkpoint",
                    "inconsistent checkpoint review changed before blocking",
                ));
            }
            transaction
                .commit()
                .map_err(database_error("commit inconsistent checkpoint dispatch block"))?;
            return Ok(CheckpointDispatchSelection::Blocked);
        }
        if prepared_checkpoint_source_authority_in_connection(&transaction, &checkpoint).is_err() {
            let changed = block_checkpoint_dispatch_review(
                &transaction,
                &review_id,
                &campaign_id,
                &source_experiment_id,
                &task_signature,
                attempt,
                now,
            )?;
            if !changed {
                return Err(validation_error(
                    "research.checkpoint",
                    "checkpoint authority changed before blocking",
                ));
            }
            transaction
                .commit()
                .map_err(database_error("commit unsupported checkpoint dispatch block"))?;
            return Ok(CheckpointDispatchSelection::Blocked);
        }
        let proposal_id = checkpoint.successor_ids.proposal_id.clone();
        let submission_id = checkpoint.successor_ids.submission_id.clone();
        let successor_witness = super::research::checkpoint_successor_witness(
            &transaction,
            successor_experiment_id,
        )?;
        let termination_request_id: Option<i64> = transaction
            .query_row(
                "SELECT termination_request_id FROM research_reviews WHERE review_id = ?1",
                [&review_id],
                |row| row.get(0),
            )
            .map_err(database_error("read checkpoint dispatch termination"))?;
        let mut authority = CheckpointDispatchAuthority {
            checkpoint,
            raw_checkpoint: raw,
            project_id: project_id.to_owned(),
            campaign_id,
            review_id,
            source_experiment_id,
            proposal_id,
            submission_id,
            successor_experiment_id: successor_experiment_id.to_owned(),
            successor_status: None,
            successor_attempt: successor_witness.map(|(attempt, _)| attempt),
            reservation_window_ends_at: successor_witness.map(|(_, window_end)| window_end),
            termination_request_id,
        };
        if !super::campaigns::checkpoint_successor_graph_matches_authority(
            &transaction,
            &authority,
            None,
            None,
        )? {
            let changed = block_checkpoint_dispatch_review(
                &transaction,
                &authority.review_id,
                &authority.campaign_id,
                &authority.source_experiment_id,
                &task_signature,
                attempt,
                now,
            )?;
            if !changed {
                return Err(validation_error(
                    "research.checkpoint",
                    "checkpoint graph changed before blocking",
                ));
            }
            transaction
                .commit()
                .map_err(database_error("commit partial checkpoint dispatch block"))?;
            return Ok(CheckpointDispatchSelection::Blocked);
        }
        authority.successor_status = Some(
            super::campaigns::read_intent_by_experiment(&transaction, successor_experiment_id)?
                .experiment
                .status,
        );
        transaction
            .commit()
            .map_err(database_error("commit checkpoint dispatch authority"))?;
        Ok(CheckpointDispatchSelection::Ready(authority))
    }

    pub(crate) fn rotate_open_action_review(
        &self,
        review_id: &str,
        now: i64,
    ) -> Result<(), AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin open research action rotation"))?;
        transaction
            .execute(
                "UPDATE research_reviews
                 SET updated_at = CASE WHEN updated_at < ?1 THEN ?1 ELSE updated_at END
                 WHERE review_id = ?2 AND state = 'ready'
                   AND operation_stage IN ('intent','stop_requested',
                       'stop_confirmed','successor_reserved')",
                params![now, review_id],
            )
            .map_err(database_error("rotate open research action"))?;
        transaction
            .commit()
            .map_err(database_error("commit open research action rotation"))
    }
}

#[derive(Debug)]
struct ResearchOwnershipCandidate {
    review_id: String,
    project_id: Option<String>,
    campaign_id: String,
    source_experiment_id: String,
    managed_task_signature: String,
    source_task_id: Option<i64>,
    source_campaign_id: Option<String>,
    attempt: i64,
    session_generation: i64,
    event_id: Option<i64>,
    event_project_id: Option<String>,
    event_campaign_id: Option<String>,
    event_experiment_id: Option<String>,
    event_kind: Option<String>,
    event_status: Option<String>,
    operation_stage: Option<String>,
    agent_run_id: Option<i64>,
    run_project_id: Option<String>,
    run_execution_kind: Option<String>,
    run_status: Option<String>,
    run_gate_state: Option<String>,
    termination_request_id: Option<i64>,
    termination_project_id: Option<String>,
    termination_signature: Option<String>,
    termination_status: Option<String>,
    decision_cycle_id: Option<String>,
    cycle_campaign_id: Option<String>,
    cycle_source_experiment_id: Option<String>,
    successor_experiment_id: Option<String>,
    successor_campaign_id: Option<String>,
    successor_source_experiment_id: Option<String>,
    successor_parent_experiment_id: Option<String>,
    successor_proposal_campaign_id: Option<String>,
    successor_proposal_source_experiment_id: Option<String>,
    notes_json: Option<String>,
    failure_code: Option<String>,
    campaign_session: Option<String>,
    campaign_generation: Option<i64>,
    state: String,
}

fn research_ownership_candidate_from_row(
    row: &Row<'_>,
) -> rusqlite::Result<ResearchOwnershipCandidate> {
    Ok(ResearchOwnershipCandidate {
        review_id: row.get(0)?,
        project_id: row.get(1)?,
        campaign_id: row.get(2)?,
        source_experiment_id: row.get(3)?,
        managed_task_signature: row.get(4)?,
        source_task_id: row.get(5)?,
        source_campaign_id: row.get(6)?,
        attempt: row.get(7)?,
        session_generation: row.get(8)?,
        event_id: row.get(9)?,
        event_project_id: row.get(10)?,
        event_campaign_id: row.get(11)?,
        event_experiment_id: row.get(12)?,
        event_kind: row.get(13)?,
        event_status: row.get(14)?,
        operation_stage: row.get(15)?,
        agent_run_id: row.get(16)?,
        run_project_id: row.get(17)?,
        run_execution_kind: row.get(18)?,
        run_status: row.get(19)?,
        run_gate_state: row.get(20)?,
        termination_request_id: row.get(21)?,
        termination_project_id: row.get(22)?,
        termination_signature: row.get(23)?,
        termination_status: row.get(24)?,
        decision_cycle_id: row.get(25)?,
        cycle_campaign_id: row.get(26)?,
        cycle_source_experiment_id: row.get(27)?,
        successor_experiment_id: row.get(28)?,
        successor_campaign_id: row.get(29)?,
        successor_source_experiment_id: row.get(30)?,
        successor_parent_experiment_id: row.get(31)?,
        successor_proposal_campaign_id: row.get(32)?,
        successor_proposal_source_experiment_id: row.get(33)?,
        notes_json: row.get(34)?,
        failure_code: row.get(35)?,
        campaign_session: row.get(36)?,
        campaign_generation: row.get(37)?,
        state: row.get(38)?,
    })
}

/// Read the exclusive research owner while already inside the caller's
/// transaction.  Candidate filtering intentionally precedes cardinality and
/// lineage validation: a malformed owner must block discovery instead of
/// disappearing from the owner set.
fn checkpoint_ownership_lineage_valid(
    transaction: &Transaction<'_>,
    candidate: &ResearchOwnershipCandidate,
    project_id: &str,
    campaign_id: &str,
    source_experiment_id: &str,
) -> Result<bool, AppError> {
    let Some(successor_id) = candidate.successor_experiment_id.as_deref() else {
        return Ok(candidate.operation_stage.as_deref() != Some("successor_reserved"));
    };
    let row = transaction
        .query_row(
            "SELECT review.state, review.operation_stage,
                    CASE
                      WHEN typeof(review.checkpoint_json) = 'text'
                       AND length(CAST(review.checkpoint_json AS BLOB)) BETWEEN 1 AND 131072
                      THEN review.checkpoint_json
                    END,
                    typeof(review.checkpoint_json),
                    length(CAST(review.checkpoint_json AS BLOB)),
                    experiment.proposal_id, experiment.submission_id,
                    experiment.parent_experiment_id, experiment.resume_of_experiment_id,
                    experiment.status, experiment.pueue_task_id,
                    experiment.task_signature, experiment.failure_code,
                    experiment.failure_fingerprint, submission.status,
                    submission.project_id, submission.pueue_task_id,
                    submission.task_signature, reservation.status,
                    (SELECT COUNT(*) FROM budget_reservations AS r2
                     WHERE r2.experiment_id = experiment.experiment_id
                       AND r2.dimension = 'experiment')
             FROM research_reviews AS review
             LEFT JOIN experiments AS experiment
               ON experiment.experiment_id = review.successor_experiment_id
             LEFT JOIN submissions AS submission
               ON submission.submission_id = experiment.submission_id
             LEFT JOIN budget_reservations AS reservation
               ON reservation.experiment_id = experiment.experiment_id
              AND reservation.dimension = 'experiment'
             WHERE review.review_id = ?1 AND review.experiment_id = ?2
               AND review.successor_experiment_id = ?3",
            params![candidate.review_id, source_experiment_id, successor_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, Option<String>>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, Option<String>>(14)?,
                    row.get::<_, Option<String>>(15)?,
                    row.get::<_, Option<i64>>(16)?,
                    row.get::<_, Option<String>>(17)?,
                    row.get::<_, Option<String>>(18)?,
                    row.get::<_, i64>(19)?,
                ))
            },
        )
        .optional()
        .map_err(database_error("read checkpoint ownership graph"))?;
    let Some((
        review_state,
        operation_stage,
        raw,
        storage,
        byte_len,
        proposal_id,
        submission_id,
        parent_id,
        resume_id,
        experiment_status,
        experiment_task_id,
        experiment_signature,
        failure_code,
        failure_fingerprint,
        submission_status,
        submission_project,
        submission_task_id,
        submission_signature,
        reservation_status,
        reservation_count,
    )) = row
    else {
        return Ok(false);
    };
    if storage == "null" && byte_len.is_none() && raw.is_none() {
        return Ok(operation_stage.as_deref() != Some("successor_reserved"));
    }
    if storage != "text"
        || !byte_len.is_some_and(|len| (1..=MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES as i64).contains(&len))
    {
        return Ok(false);
    }
    let Some(raw) = raw else {
        return Ok(false);
    };
    let checkpoint = match parse_prepared_checkpoint(&raw) {
        Ok(checkpoint) => checkpoint,
        Err(_) => return Ok(false),
    };
    if checkpoint.project_id != project_id
        || checkpoint.campaign_id != campaign_id
        || checkpoint.source_experiment_id != source_experiment_id
        || checkpoint.successor_ids.experiment_id != successor_id
    {
        return Ok(false);
    }
    let successor_witness = checkpoint_successor_witness(transaction, successor_id)?;
    let authority = CheckpointDispatchAuthority {
        checkpoint: checkpoint.clone(),
        raw_checkpoint: raw.clone(),
        project_id: project_id.to_owned(),
        campaign_id: campaign_id.to_owned(),
        review_id: candidate.review_id.clone(),
        source_experiment_id: source_experiment_id.to_owned(),
        proposal_id: checkpoint.successor_ids.proposal_id.clone(),
        submission_id: checkpoint.successor_ids.submission_id.clone(),
        successor_experiment_id: successor_id.to_owned(),
        successor_status: None,
        successor_attempt: successor_witness.map(|(attempt, _)| attempt),
        reservation_window_ends_at: successor_witness.map(|(_, window_end)| window_end),
        termination_request_id: candidate.termination_request_id,
    };
    if !super::campaigns::checkpoint_successor_graph_matches_authority(
        transaction,
        &authority,
        None,
        None,
    )? {
        return Ok(false);
    }
    let exact_lineage = parent_id.as_deref() == Some(source_experiment_id)
        && resume_id.as_deref() == Some(source_experiment_id)
        && proposal_id.as_deref() == Some(checkpoint.successor_ids.proposal_id.as_str())
        && submission_id.as_deref() == Some(checkpoint.successor_ids.submission_id.as_str())
        && submission_project.as_deref() == Some(project_id)
        && reservation_count == 1;
    if !exact_lineage {
        return Ok(false);
    }
    let active = operation_stage.as_deref() == Some("successor_reserved")
        && review_state == "ready"
        && matches!(
            experiment_status.as_deref(),
            Some("reserved" | "submitting" | "accepted" | "unreconciled")
        )
        && matches!(
            submission_status.as_deref(),
            Some("pending" | "accepted" | "unreconciled")
        )
        && matches!(reservation_status.as_deref(), Some("reserved" | "consumed"));
    let settled_task_failure = experiment_status.as_deref() == Some("failed")
        && submission_status.as_deref() == Some("accepted")
        && experiment_task_id.is_some()
        && experiment_signature.is_some()
        && submission_task_id == experiment_task_id
        && submission_signature == experiment_signature
        && failure_code.is_some()
        && failure_code.as_deref() != Some(CHECKPOINT_PRE_ADD_FAILURE_CODE)
        && failure_fingerprint.is_some();
    let settled = review_state == "completed"
        && operation_stage.is_none()
        && matches!(experiment_status.as_deref(), Some("succeeded" | "failed" | "cancelled"))
        && submission_status.as_deref() == Some("accepted")
        && experiment_task_id.is_some()
        && experiment_signature.is_some()
        && submission_task_id == experiment_task_id
        && submission_signature == experiment_signature
        && reservation_status.as_deref() == Some("consumed")
        && (matches!(experiment_status.as_deref(), Some("succeeded" | "cancelled"))
            && failure_code.is_none()
            && failure_fingerprint.is_none()
            || settled_task_failure);
    let expected_pre_add_fingerprint = {
        let mut digest = Sha256::new();
        digest.update(CHECKPOINT_PRE_ADD_FAILURE_CODE.as_bytes());
        digest.update([0]);
        digest.update(raw.as_bytes());
        format!("research-checkpoint-pre-add:{:x}", digest.finalize())
    };
    let pre_add_failed = matches!(
        (review_state.as_str(), operation_stage.as_deref()),
        ("blocked", Some("successor_reserved")) | ("completed", None)
    )
        && experiment_status.as_deref() == Some("failed")
        && failure_code.as_deref() == Some(CHECKPOINT_PRE_ADD_FAILURE_CODE)
        && failure_fingerprint.as_deref() == Some(expected_pre_add_fingerprint.as_str())
        && experiment_task_id.is_none()
        && experiment_signature.is_none()
        && submission_status.as_deref() == Some("failed")
        && submission_task_id.is_none()
        && submission_signature.is_none()
        && reservation_status.as_deref() == Some("consumed");
    Ok(active || settled || pre_add_failed)
}

pub(crate) fn research_ownership_in_transaction(
    transaction: &Transaction<'_>,
    project_id: &str,
    campaign_id: &str,
    source_experiment_id: &str,
) -> Result<ResearchOwnership, AppError> {
    let mut statement = transaction
        .prepare(
            "SELECT review.review_id, campaign.project_id, review.campaign_id,
                    review.experiment_id, review.task_signature,
                    source.pueue_task_id, source.campaign_id,
                    review.attempt, review.session_generation, review.event_id,
                    event.project_id, event.campaign_id, event.experiment_id,
                    event.kind, event.status, review.operation_stage, review.agent_run_id,
                    run.project_id, run.execution_kind, run.status,
                    run.launch_gate_state, review.termination_request_id,
                    termination.project_id, termination.task_signature,
                    termination.status, review.decision_cycle_id,
                    cycle.campaign_id, cycle.source_experiment_id,
                    review.successor_experiment_id, successor.campaign_id,
                    successor.resume_of_experiment_id, successor.parent_experiment_id,
                    successor_proposal.campaign_id, successor_proposal.source_experiment_id,
                    review.notes_json,
                    review.failure_code, campaign_research.session_id,
                    campaign_research.session_generation, review.state
             FROM research_reviews AS review
             LEFT JOIN campaigns AS campaign
               ON campaign.campaign_id = review.campaign_id
             LEFT JOIN campaign_research
               ON campaign_research.campaign_id = review.campaign_id
             LEFT JOIN experiments AS source
               ON source.experiment_id = review.experiment_id
             LEFT JOIN events AS event
               ON event.event_id = review.event_id
             LEFT JOIN agent_runs AS run
               ON run.run_id = review.agent_run_id
             LEFT JOIN termination_requests AS termination
               ON termination.request_id = review.termination_request_id
             LEFT JOIN decision_cycles AS cycle
               ON cycle.cycle_id = review.decision_cycle_id
             LEFT JOIN experiments AS successor
               ON successor.experiment_id = review.successor_experiment_id
             LEFT JOIN proposals AS successor_proposal
               ON successor_proposal.proposal_id = successor.proposal_id
             WHERE review.experiment_id = ?1
               AND (
                   review.operation_stage IN ('intent','stop_requested',
                       'stop_confirmed','successor_reserved')
                   OR (
                       review.state = 'completed'
                       AND (review.decision_cycle_id IS NOT NULL
                            OR review.successor_experiment_id IS NOT NULL)
                   )
                   OR (
                       review.checkpoint_json IS NOT NULL
                       AND review.successor_experiment_id IS NOT NULL
                   )
               )
             ORDER BY review.review_id
             LIMIT 2",
        )
        .map_err(database_error("prepare research ownership query"))?;
    let candidates = statement
        .query_map([source_experiment_id], research_ownership_candidate_from_row)
        .map_err(database_error("query research ownership candidates"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(database_error("read research ownership candidates"))?;
    if candidates.is_empty() {
        return Ok(ResearchOwnership::None);
    }
    if candidates.len() != 1 {
        return Ok(ResearchOwnership::Open(None));
    }
    let candidate = candidates.into_iter().next().expect("one candidate");
    let checkpoint_lineage_valid = checkpoint_ownership_lineage_valid(
        transaction,
        &candidate,
        project_id,
        campaign_id,
        source_experiment_id,
    )?;
    let snapshot = ResearchOwnershipSnapshot {
        review_id: candidate.review_id.clone(),
        project_id: project_id.to_owned(),
        campaign_id: candidate.campaign_id.clone(),
        source_experiment_id: candidate.source_experiment_id.clone(),
        managed_task_signature: candidate.managed_task_signature.clone(),
        source_task_id: candidate.source_task_id,
        attempt: candidate.attempt,
        session_generation: candidate.session_generation,
        event_id: candidate.event_id,
        operation_stage: candidate.operation_stage.clone(),
        agent_run_id: candidate.agent_run_id,
        termination_request_id: candidate.termination_request_id,
        decision_cycle_id: candidate.decision_cycle_id.clone(),
        successor_experiment_id: candidate.successor_experiment_id.clone(),
        recovery_required: false,
    };
    let completed_handoff = candidate.state == "completed"
        && (candidate.decision_cycle_id.is_some() || candidate.successor_experiment_id.is_some());

    let mut recovery_required = candidate.project_id.as_deref() != Some(project_id)
        || candidate.campaign_id != campaign_id
        || candidate.source_experiment_id != source_experiment_id
        || candidate.source_campaign_id.as_deref() != Some(campaign_id)
        || candidate
            .managed_task_signature
            .strip_prefix("pueue-managed-run:v1:")
            .is_none()
        || candidate.event_id.is_none()
        || candidate.event_project_id.as_deref() != Some(project_id)
        || candidate.event_campaign_id.as_deref() != Some(campaign_id)
        || candidate.event_experiment_id.as_deref() != Some(source_experiment_id)
        || candidate.event_kind.as_deref() != Some("campaign_research")
        || candidate.event_status.as_deref() != Some("completed")
        || (!completed_handoff
            && candidate.campaign_generation != Some(candidate.session_generation));
    recovery_required |= !checkpoint_lineage_valid;

    if let Some(run_id) = candidate.agent_run_id {
        let run_ready = if completed_handoff {
            true
        } else {
            matches!(
                candidate.run_status.as_deref(),
                Some("completed" | "failed" | "timed_out" | "cancelled")
            ) && matches!(candidate.run_gate_state.as_deref(), Some("released" | "failed"))
                && candidate.run_project_id.as_deref() == Some(project_id)
                && candidate.run_execution_kind.as_deref() == Some("campaign_research")
                && native_recovery_cleanup_complete(
                    candidate.notes_json.as_deref(),
                    &NativeRecoveryCleanupExpectation {
                        review_id: &candidate.review_id,
                        campaign_id,
                        experiment_id: source_experiment_id,
                        attempt: candidate.attempt,
                        session_generation: candidate.session_generation,
                        agent_run_id: run_id,
                        state: &candidate.state,
                        failure_code: candidate.failure_code.as_deref(),
                        campaign_session: candidate.campaign_session.as_deref(),
                    },
                )
        };
        recovery_required |= !run_ready;
        let strict_owner = native_research_owner_rows(transaction, None)?
            .into_iter()
            .find(|row| row.review_id == candidate.review_id && row.agent_run_id == run_id);
        recovery_required |=
            !strict_owner.is_some_and(|row| native_research_owner_is_complete(&row));
    } else {
        recovery_required = true;
    }

    let source_identity_matches = transaction
        .query_row(
            "SELECT source.campaign_id, source.task_signature,
                    source.pueue_task_id, submission.project_id,
                    submission.task_signature, submission.pueue_task_id,
                    submission.status
             FROM experiments AS source
             JOIN submissions AS submission
               ON submission.submission_id = source.submission_id
             WHERE source.experiment_id = ?1",
            [source_experiment_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, String>(6)?,
                ))
            },
        )
        .optional()
        .map_err(database_error("read research ownership source identity"))?
        .is_some_and(
            |(
                source_campaign,
                source_signature,
                source_task_id,
                submission_project,
                submission_signature,
                submission_task_id,
                submission_status,
            )| {
                source_campaign == campaign_id
                    && source_signature.as_deref() == Some(candidate.managed_task_signature.as_str())
                    && submission_project == project_id
                    && submission_signature.as_deref()
                        == Some(candidate.managed_task_signature.as_str())
                    && submission_task_id == source_task_id
                    && submission_status == "accepted"
            },
        );
    recovery_required |= !source_identity_matches;

    let open_stage = matches!(
        candidate.operation_stage.as_deref(),
        Some("intent" | "stop_requested" | "stop_confirmed" | "successor_reserved")
    );
    if open_stage || completed_handoff {
        let request_matches = candidate
            .termination_request_id
            .is_some()
            && candidate.termination_project_id.as_deref() == Some(project_id)
            && candidate
                .termination_signature
                .as_deref()
                .is_some_and(|signature| signature.starts_with("pueue-task:v1:"));
        recovery_required |= !request_matches;
        if candidate.operation_stage.as_deref() == Some("stop_confirmed") || completed_handoff {
            recovery_required |= candidate.termination_status.as_deref() != Some("confirmed");
        }
        if request_matches {
            recovery_required |= !research_termination_signature_matches(
                transaction,
                project_id,
                candidate.source_task_id,
                candidate.termination_signature.as_deref(),
                &candidate.managed_task_signature,
            )?;
        }
    }

    let expected_cycle = super::decisions::DecisionRepository::terminal_cycle_id(
        campaign_id,
        source_experiment_id,
    );
    if let Some(cycle_id) = candidate.decision_cycle_id.as_deref() {
        recovery_required |= cycle_id != expected_cycle
            || candidate.cycle_campaign_id.as_deref() != Some(campaign_id)
            || candidate.cycle_source_experiment_id.as_deref() != Some(source_experiment_id);
    }
    if let Some(successor_id) = candidate.successor_experiment_id.as_deref() {
        let successor_identity_matches = candidate.successor_campaign_id.as_deref()
            == Some(campaign_id)
            && candidate.successor_proposal_campaign_id.as_deref() == Some(campaign_id)
            && candidate.successor_proposal_source_experiment_id.as_deref()
                == Some(source_experiment_id);
        let ordinary_successor = successor_identity_matches
            && candidate.state == "completed"
            && candidate.operation_stage.is_none()
            && candidate.decision_cycle_id.as_deref() == Some(expected_cycle.as_str())
            && candidate.successor_parent_experiment_id.as_deref() == Some(source_experiment_id)
            && candidate.successor_source_experiment_id.is_none();
        let checkpoint_successor = successor_identity_matches
            && candidate.operation_stage.as_deref() == Some("successor_reserved")
            && candidate.decision_cycle_id.is_none()
            && candidate.successor_parent_experiment_id.as_deref() == Some(source_experiment_id)
            && candidate.successor_source_experiment_id.as_deref() == Some(source_experiment_id);
        let checkpoint_settled_successor = successor_identity_matches
            && candidate.state == "completed"
            && candidate.operation_stage.is_none()
            && candidate.decision_cycle_id.is_none()
            && candidate.successor_parent_experiment_id.as_deref() == Some(source_experiment_id)
            && candidate.successor_source_experiment_id.as_deref() == Some(source_experiment_id)
            && checkpoint_lineage_valid;
        let successor_lineage_matches =
            ordinary_successor || checkpoint_successor || checkpoint_settled_successor;
        recovery_required |= successor_id.is_empty() || !successor_lineage_matches;
    }

    let attached = candidate.state == "completed"
        && candidate.operation_stage.is_none()
        && candidate.decision_cycle_id.as_deref() == Some(expected_cycle.as_str())
        && !recovery_required;
    let mut snapshot = snapshot;
    snapshot.recovery_required = recovery_required;
    if attached {
        Ok(ResearchOwnership::Attached(snapshot))
    } else {
        Ok(ResearchOwnership::Open(Some(snapshot)))
    }
}

/// Project one completed research handoff for the decision evidence builder
/// while retaining the caller's transaction and the historical Task 4 proof.
/// No owner returns `None`; an open, duplicate, stale, or malformed linked
/// handoff returns a validation error so callers cannot silently fall back to
/// a v1 decision context.
pub(crate) fn completed_research_handoff_in_transaction(
    transaction: &Transaction<'_>,
    project_id: &str,
    campaign_id: &str,
    source_experiment_id: &str,
    decision_cycle_id: &str,
) -> Result<Option<CompletedResearchHandoff>, AppError> {
    let ownership = research_ownership_in_transaction(
        transaction,
        project_id,
        campaign_id,
        source_experiment_id,
    )?;
    let owner = match ownership {
        ResearchOwnership::None => return Ok(None),
        ResearchOwnership::Open(_) => {
            return Err(validation_error(
                "research.handoff",
                "linked research ownership is still open or ambiguous",
            ));
        }
        ResearchOwnership::Attached(owner) => owner,
    };
    if owner.decision_cycle_id.as_deref() != Some(decision_cycle_id) {
        return Err(validation_error(
            "research.handoff",
            "linked research cycle does not match the requested cycle",
        ));
    };
    let Some(agent_run_id) = owner.agent_run_id else {
        return Err(validation_error(
            "research.handoff",
            "linked research run is missing",
        ));
    };
    let Some(event_id) = owner.event_id else {
        return Err(validation_error(
            "research.handoff",
            "linked research event is missing",
        ));
    };
    let Some(termination_request_id) = owner.termination_request_id else {
        return Err(validation_error(
            "research.handoff",
            "linked research termination request is missing",
        ));
    };

    // `research_ownership_in_transaction` already applies this oracle while
    // classifying an attached owner.  Reselect it here as part of the
    // projection so this helper cannot silently weaken the historical proof
    // if the ownership classifier later gains another attached path.
    let strict_owner = native_research_owner_rows(transaction, None)?
        .into_iter()
        .find(|row| row.review_id == owner.review_id && row.agent_run_id == agent_run_id);
    if !strict_owner.is_some_and(|row| native_research_owner_is_complete(&row)) {
        return Err(validation_error(
            "research.handoff",
            "linked research owner lacks the historical cleanup proof",
        ));
    }

    let objective_digest: Option<String> = transaction
        .query_row(
            "SELECT objective_digest FROM campaigns
             WHERE campaign_id = ?1 AND project_id = ?2",
            params![campaign_id, project_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(database_error("read completed research campaign objective"))?;
    let Some(objective_digest) = objective_digest else {
        return Err(validation_error(
            "research.handoff",
            "linked research campaign is missing",
        ));
    };

    let mut statement = transaction
        .prepare(
            "SELECT review_id, task_signature, attempt, session_generation,
                    agent_run_id, event_id, termination_request_id,
                    context_json, context_digest, response_json, notes_json
             FROM research_reviews
             WHERE campaign_id = ?1 AND experiment_id = ?2
               AND decision_cycle_id = ?3
               AND state = 'completed' AND operation_stage IS NULL
             ORDER BY review_id
             LIMIT 2",
        )
        .map_err(database_error("prepare completed research handoff query"))?;
    let rows = statement
        .query_map(
            params![campaign_id, source_experiment_id, decision_cycle_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<String>>(10)?,
                ))
            },
        )
        .map_err(database_error("query completed research handoff"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(database_error("read completed research handoff"))?;
    if rows.len() != 1 {
        return Err(validation_error(
            "research.handoff",
            "linked research handoff is missing or duplicated",
        ));
    }
    let (
        review_id,
        managed_task_signature,
        attempt,
        session_generation,
        persisted_agent_run_id,
        persisted_event_id,
        persisted_termination_request_id,
        context_json,
        context_digest,
        response_json,
        notes_json,
    ) = rows.into_iter().next().expect("one completed research handoff");
    if review_id != owner.review_id
        || managed_task_signature != owner.managed_task_signature
        || attempt != owner.attempt
        || session_generation != owner.session_generation
        || persisted_agent_run_id != Some(agent_run_id)
        || persisted_event_id != Some(event_id)
        || persisted_termination_request_id != Some(termination_request_id)
    {
        return Err(validation_error(
            "research.handoff",
            "linked research row does not match its owner snapshot",
        ));
    }
    let Some(context_json) = context_json else {
        return Err(validation_error(
            "research.handoff",
            "linked research context is missing",
        ));
    };
    let Some(context_digest) = context_digest else {
        return Err(validation_error(
            "research.handoff",
            "linked research context digest is missing",
        ));
    };
    if format!("{:x}", Sha256::digest(context_json.as_bytes())) != context_digest {
        return Err(validation_error(
            "research.handoff",
            "linked research context digest is invalid",
        ));
    }
    let Ok(context) = serde_json::from_str::<Value>(&context_json) else {
        return Err(validation_error(
            "research.handoff",
            "linked research context is invalid JSON",
        ));
    };
    if !research_context_identity_matches(
        &context,
        project_id,
        campaign_id,
        &owner.review_id,
        source_experiment_id,
        &owner.managed_task_signature,
        owner.source_task_id,
        &objective_digest,
    ) {
        return Err(validation_error(
            "research.handoff",
            "linked research context identity does not match its owner",
        ));
    }
    let Some(response_json) = response_json else {
        return Err(validation_error(
            "research.handoff",
            "linked research response is missing",
        ));
    };
    let Ok(answer) = parse_research_answer(response_json.as_bytes()) else {
        return Err(validation_error(
            "research.handoff",
            "linked research response is invalid",
        ));
    };
    if answer.action != "stop_and_next"
        || answer.review_id != owner.review_id
        || answer.experiment_id != source_experiment_id
        || answer.context_digest != context_digest
    {
        return Err(validation_error(
            "research.handoff",
            "linked research response does not match its owner",
        ));
    }
    if !research_answer_evidence_refs_are_bound(
        &context,
        &context_json,
        &context_digest,
        &answer,
    )
    .map_err(|_| {
        validation_error(
            "research.handoff",
            "linked research response cites invalid or unbound evidence",
        )
    })? {
        return Err(validation_error(
            "research.handoff",
            "linked research response cites evidence outside its context",
        ));
    }
    let Some(notes_json) = notes_json else {
        return Err(validation_error(
            "research.handoff",
            "linked research notes are missing",
        ));
    };
    Ok(Some(CompletedResearchHandoff {
        owner,
        context_json,
        context_digest,
        response_json,
        answer,
        notes_json,
    }))
}

/// Reselect and validate a ready answer against the live native task while
/// holding the caller's transaction.  A stale task, answer, cleanup proof, or
/// campaign gate simply produces no consumable action; it never authorizes a
/// kill from historical rows alone.
pub(crate) fn ready_research_action_in_transaction(
    transaction: &Transaction<'_>,
    project_id: &str,
    review_id: &str,
    live_task: &PueueTask,
) -> Result<Option<ReadyResearchAction>, AppError> {
    let current = transaction
        .query_row(
            "SELECT review.campaign_id, review.experiment_id,
                    review.task_signature, review.attempt, review.state,
                    review.operation_stage, review.agent_run_id,
                    review.context_json, review.context_digest,
                    review.response_json, review.termination_request_id,
                    review.session_generation, review.event_id,
                    review.notes_json, review.failure_code,
                    campaign.project_id, campaign.objective_digest, campaign.state,
                    project.pueue_group, project.enabled, project.paused,
                    project.halted_reason, source.campaign_id,
                    source.pueue_task_id, source.task_signature, source.status,
                    source.submission_id, research_state.session_id,
                    research_state.session_generation, run.project_id,
                    run.execution_kind, run.status, run.launch_gate_state,
                    event.project_id, event.kind, event.campaign_id,
                    event.experiment_id, event.status
             FROM research_reviews AS review
             JOIN campaigns AS campaign
               ON campaign.campaign_id = review.campaign_id
             JOIN projects AS project
               ON project.project_id = campaign.project_id
             LEFT JOIN experiments AS source
               ON source.experiment_id = review.experiment_id
             LEFT JOIN campaign_research AS research_state
               ON research_state.campaign_id = review.campaign_id
             LEFT JOIN agent_runs AS run
               ON run.run_id = review.agent_run_id
             LEFT JOIN events AS event
               ON event.event_id = review.event_id
             WHERE review.review_id = ?1
               AND campaign.project_id = ?2
               AND review.checkpoint_json IS NULL",
            params![review_id, project_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                    row.get::<_, i64>(11)?,
                    row.get::<_, Option<i64>>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, Option<String>>(14)?,
                    row.get::<_, String>(15)?,
                    row.get::<_, String>(16)?,
                    row.get::<_, String>(17)?,
                    row.get::<_, String>(18)?,
                    row.get::<_, i64>(19)?,
                    row.get::<_, i64>(20)?,
                    row.get::<_, Option<String>>(21)?,
                    row.get::<_, Option<String>>(22)?,
                    row.get::<_, Option<i64>>(23)?,
                    row.get::<_, Option<String>>(24)?,
                    row.get::<_, Option<String>>(25)?,
                    row.get::<_, Option<String>>(26)?,
                    row.get::<_, Option<String>>(27)?,
                    row.get::<_, Option<i64>>(28)?,
                    row.get::<_, Option<String>>(29)?,
                    row.get::<_, Option<String>>(30)?,
                    row.get::<_, Option<String>>(31)?,
                    row.get::<_, Option<String>>(32)?,
                    row.get::<_, Option<String>>(33)?,
                    row.get::<_, Option<String>>(34)?,
                    row.get::<_, Option<String>>(35)?,
                    row.get::<_, Option<String>>(36)?,
                    row.get::<_, Option<String>>(37)?,
                ))
            },
        )
        .optional()
        .map_err(database_error("read ready research action"))?;
    let Some((
        campaign_id,
        experiment_id,
        managed_task_signature,
        attempt,
        state,
        operation_stage,
        agent_run_id,
        context_json,
        context_digest,
        response_json,
        termination_request_id,
        session_generation,
        event_id,
        notes_json,
        failure_code,
        persisted_project_id,
        campaign_objective_digest,
        campaign_state,
        expected_group,
        project_enabled,
        project_paused,
        halted_reason,
        source_campaign_id,
        source_task_id,
        source_task_signature,
        source_status,
        source_submission_id,
        campaign_session,
        campaign_generation,
        run_project_id,
        run_execution_kind,
        run_status,
        run_gate_state,
        event_project_id,
        event_kind,
        event_campaign_id,
        event_experiment_id,
        event_status,
    )) = current
    else {
        return Ok(None);
    };
    let static_ok = state == "ready"
        && operation_stage.is_none()
        && termination_request_id.is_none()
        && persisted_project_id == project_id
        && campaign_state == "active"
        && project_enabled == 1
        && project_paused == 0
        && halted_reason.is_none()
        && source_campaign_id.as_deref() == Some(campaign_id.as_str())
        && source_status.as_deref() == Some("accepted")
        && source_task_signature.as_deref() == Some(managed_task_signature.as_str())
        && source_task_id == Some(live_task.id)
        && expected_group == live_task.group
        && live_task.is_running()
        && event_id.is_some()
        && event_project_id.as_deref() == Some(project_id)
        && event_kind.as_deref() == Some("campaign_research")
        && event_campaign_id.as_deref() == Some(campaign_id.as_str())
        && event_experiment_id.as_deref() == Some(experiment_id.as_str())
        && event_status.as_deref() == Some("completed")
        && agent_run_id.is_some()
        && run_project_id.as_deref() == Some(project_id)
        && run_execution_kind.as_deref() == Some("campaign_research")
        && matches!(run_status.as_deref(), Some("completed" | "failed" | "timed_out" | "cancelled"))
        && matches!(run_gate_state.as_deref(), Some("released" | "failed"))
        && campaign_generation == Some(session_generation);
    if !static_ok {
        return Ok(None);
    }
    let managed_live = managed_task_run_signature(live_task);
    if managed_live.as_deref() != Some(managed_task_signature.as_str()) {
        return Ok(None);
    }
    let raw_task_signature = task_signature(live_task);
    let Some(context_json) = context_json else {
        return Ok(None);
    };
    let Some(context_digest) = context_digest else {
        return Ok(None);
    };
    if format!("{:x}", Sha256::digest(context_json.as_bytes())) != context_digest {
        return Ok(None);
    }
    let Some(response_json) = response_json else {
        return Ok(None);
    };
    let Ok(answer) = parse_research_answer(response_json.as_bytes()) else {
        return Ok(None);
    };
    if answer.review_id != review_id
        || answer.experiment_id != experiment_id
        || answer.context_digest != context_digest
    {
        return Ok(None);
    }
    let Ok(context) = serde_json::from_str::<Value>(&context_json) else {
        return Ok(None);
    };
    if !research_context_identity_matches(
        &context,
        project_id,
        &campaign_id,
        review_id,
        &experiment_id,
        &managed_task_signature,
        source_task_id,
        &campaign_objective_digest,
    ) {
        return Ok(None);
    }
    if !research_answer_evidence_refs_are_bound(
        &context,
        &context_json,
        &context_digest,
        &answer,
    )
    .unwrap_or(false)
    {
        return Ok(None);
    }
    let agent_run_id = agent_run_id.expect("validated ready agent run");
    if !native_recovery_cleanup_complete(
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
    ) {
        return Ok(None);
    }
    let strict_owner = native_research_owner_rows(transaction, None)?
        .into_iter()
        .find(|row| row.review_id == review_id && row.agent_run_id == agent_run_id);
    if !strict_owner.is_some_and(|row| native_research_owner_is_complete(&row)) {
        return Ok(None);
    }
    let health_owned: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM running_health
                 WHERE experiment_id = ?1 AND state = 'action_pending'
             )",
            [experiment_id.as_str()],
            |row| row.get(0),
        )
        .map_err(database_error("check research health ownership"))?;
    if health_owned {
        return Ok(None);
    }
    let Some(source_submission_id) = source_submission_id else {
        return Ok(None);
    };
    let submission_matches = transaction
        .query_row(
            "SELECT project_id, pueue_task_id, task_signature, status
             FROM submissions WHERE submission_id = ?1",
            [source_submission_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(database_error("read ready research submission"))?
        .is_some_and(|(submission_project, submission_task_id, submission_signature, status)| {
            submission_project == project_id
                && submission_task_id == source_task_id
                && submission_signature.as_deref() == Some(managed_task_signature.as_str())
                && status == "accepted"
        });
    if !submission_matches {
        return Ok(None);
    }
    let owner = ResearchOwnershipSnapshot {
        review_id: review_id.to_owned(),
        project_id: project_id.to_owned(),
        campaign_id,
        source_experiment_id: experiment_id,
        managed_task_signature,
        source_task_id,
        attempt,
        session_generation,
        event_id,
        operation_stage,
        agent_run_id: Some(agent_run_id),
        termination_request_id,
        decision_cycle_id: None,
        successor_experiment_id: None,
        recovery_required: false,
    };
    Ok(Some(ReadyResearchAction {
        owner,
        context_json,
        context_digest,
        response_json,
        answer,
        notes_json: notes_json.unwrap_or_else(|| "{}".to_owned()),
        campaign_objective_digest,
        raw_task_signature,
    }))
}

pub(crate) fn checkpoint_source_authority_for_preparation(
    db: &Db,
    expected: &ReadyResearchAction,
    request: &CheckpointRequest,
) -> Result<CheckpointSourceAuthorityRead, AppError> {
    let mut connection = db.connect()?;
    let transaction = connection
        .transaction()
        .map_err(database_error("begin source authority read"))?;
    let authority = checkpoint_source_authority_in_connection(
        &transaction,
        SourceAuthorityExpectation::Fresh { expected, request },
    )?;
    transaction
        .commit()
        .map_err(database_error("commit source authority read"))?;
    Ok(authority)
}

pub(super) fn checkpoint_source_authority_for_ready_in_connection(
    connection: &Connection,
    expected: &ReadyResearchAction,
    request: &CheckpointRequest,
) -> Result<CheckpointSourceAuthorityRead, AppError> {
    checkpoint_source_authority_in_connection(
        connection,
        SourceAuthorityExpectation::Fresh { expected, request },
    )
}

pub(crate) fn prepared_checkpoint_source_authority_in_connection(
    connection: &Connection,
    checkpoint: &PreparedCheckpoint,
) -> Result<CheckpointSourceAuthority, AppError> {
    match checkpoint_source_authority_in_connection(
        connection,
        SourceAuthorityExpectation::Historical { checkpoint },
    )? {
        CheckpointSourceAuthorityRead::Supported(authority) => Ok(authority),
        CheckpointSourceAuthorityRead::Unsupported { .. } => Err(validation_error(
            "checkpoint.source",
            "prepared checkpoint source is unsupported",
        )),
    }
}

fn checkpoint_source_authority_in_connection(
    connection: &Connection,
    expectation: SourceAuthorityExpectation<'_>,
) -> Result<CheckpointSourceAuthorityRead, AppError> {
    let (
        review_id,
        expected_project_id,
        expected_campaign_id,
        expected_experiment_id,
        expected_managed_signature,
        expected_attempt,
        expected_session_generation,
        expected_agent_run_id,
        expected_event_id,
        expected_source_task_id,
        expected_raw_signature,
        expected_objective_digest,
    ) = match &expectation {
        SourceAuthorityExpectation::Fresh { expected, .. } => (
            expected.owner.review_id.as_str(),
            expected.owner.project_id.as_str(),
            expected.owner.campaign_id.as_str(),
            expected.owner.source_experiment_id.as_str(),
            expected.owner.managed_task_signature.as_str(),
            expected.owner.attempt,
            expected.owner.session_generation,
            expected.owner.agent_run_id,
            expected.owner.event_id,
            expected.owner.source_task_id,
            expected.raw_task_signature.as_str(),
            expected.campaign_objective_digest.as_str(),
        ),
        SourceAuthorityExpectation::Historical { checkpoint } => (
            checkpoint.review_id.as_str(),
            checkpoint.project_id.as_str(),
            checkpoint.campaign_id.as_str(),
            checkpoint.source_experiment_id.as_str(),
            checkpoint.source_managed_task_signature.as_str(),
            checkpoint.review_attempt,
            checkpoint.review_session_generation,
            Some(checkpoint.review_agent_run_id),
            Some(checkpoint.review_event_id),
            Some(checkpoint.source_task_id),
            checkpoint.source_raw_task_signature.as_str(),
            checkpoint.campaign_objective_digest.as_str(),
        ),
    };

    let review = connection
        .query_row(
            &format!("{REVIEW_SELECT} WHERE review_id = ?1"),
            [review_id],
            review_from_row,
        )
        .optional()
        .map_err(database_error("read source authority review"))?
        .ok_or_else(|| validation_error("research.review", "does not exist"))?;
    let (persisted_event_id, persisted_notes_json, persisted_decision_cycle_id) = connection
        .query_row(
            "SELECT event_id, notes_json, decision_cycle_id
             FROM research_reviews WHERE review_id = ?1",
            [review_id],
            source_authority_owner_fields_from_row,
        )
        .map_err(database_error("read source authority owner fields"))?;

    if review.review_id != review_id
        || review.campaign_id != expected_campaign_id
        || review.experiment_id != expected_experiment_id
        || review.task_signature != expected_managed_signature
        || review.attempt != expected_attempt
        || review.session_generation != expected_session_generation
        || review.agent_run_id != expected_agent_run_id
        || persisted_event_id != expected_event_id
    {
        return Err(validation_error(
            "research.review",
            "does not match the source authority expectation",
        ));
    }

    match &expectation {
        SourceAuthorityExpectation::Fresh { expected, .. } => {
            if review.state != "ready"
                || review.operation_stage.is_some()
                || review.termination_request_id.is_some()
                || review.successor_experiment_id.is_some()
                || persisted_decision_cycle_id.is_some()
                || !matches!(review.checkpoint_json_state, CheckpointJsonState::Missing)
                || expected.owner.operation_stage.is_some()
                || expected.owner.termination_request_id.is_some()
                || expected.owner.successor_experiment_id.is_some()
                || expected.owner.decision_cycle_id.is_some()
                || expected.owner.recovery_required
                || expected.owner.attempt <= 0
                || expected.owner.session_generation < 0
                || expected.owner.agent_run_id.is_none_or(|run_id| run_id <= 0)
                || expected.owner.event_id.is_none_or(|event_id| event_id <= 0)
                || expected.owner.source_task_id.is_none_or(|task_id| task_id < 0)
                || review.context_json.as_deref() != Some(expected.context_json.as_str())
                || review.context_digest.as_deref() != Some(expected.context_digest.as_str())
                || review.response_json.as_deref() != Some(expected.response_json.as_str())
                || persisted_notes_json.as_deref().unwrap_or("{}") != expected.notes_json
            {
                return Err(validation_error(
                    "research.review",
                    "is stale or differs from the ready authority",
                ));
            }
        }
        SourceAuthorityExpectation::Historical { checkpoint } => {
            if review.context_digest.as_deref() != Some(checkpoint.context_digest.as_str()) {
                return Err(validation_error(
                    "research.context_digest",
                    "does not match the prepared checkpoint",
                ));
            }
        }
    }

    if let SourceAuthorityExpectation::Historical { checkpoint } = &expectation {
        let Some(stored_json) = review.checkpoint_json.as_deref() else {
            return Err(validation_error(
                "research.checkpoint_json",
                "is missing from the historical review",
            ));
        };
        if !matches!(review.checkpoint_json_state, CheckpointJsonState::BoundedText) {
            return Err(validation_error(
                "research.checkpoint_json",
                "is not bounded text",
            ));
        }
        let stored_checkpoint = parse_prepared_checkpoint(stored_json)?;
        if stored_checkpoint != **checkpoint {
            return Err(validation_error(
                "research.checkpoint_json",
                "does not parse to the supplied prepared checkpoint",
            ));
        }
    }

    let source = super::campaigns::read_intent_by_experiment(connection, &review.experiment_id)?;
    if source.campaign.campaign_id != review.campaign_id
        || source.experiment.experiment_id != review.experiment_id
        || source.experiment.campaign_id != source.campaign.campaign_id
        || source.experiment.proposal_id != source.proposal.proposal_id
        || source.experiment.submission_id != source.submission.submission_id
        || source.proposal.campaign_id != source.campaign.campaign_id
        || source.submission.project_id != source.campaign.project_id
    {
        return Err(validation_error(
            "research.source",
            "does not form one coherent campaign graph",
        ));
    }
    if source.campaign.project_id != expected_project_id {
        return Err(validation_error(
            "research.project_id",
            "does not match the source authority",
        ));
    }
    let project = super::repositories::find_project_by_id_in_connection(
        connection,
        &source.campaign.project_id,
    )?
    .ok_or_else(|| validation_error("research.project_id", "does not identify a project"))?;
    if project.project_id != expected_project_id
        || source.campaign.objective_digest != expected_objective_digest
    {
        return Err(validation_error(
            "research.project",
            "does not match the source authority",
        ));
    }

    validate_research_id(&review.review_id).map_err(AppError::from)?;
    validate_research_id(&project.project_id).map_err(AppError::from)?;
    validate_research_id(&source.campaign.campaign_id).map_err(AppError::from)?;
    validate_research_id(&source.experiment.experiment_id).map_err(AppError::from)?;
    validate_research_id(&source.proposal.proposal_id).map_err(AppError::from)?;
    validate_research_id(&source.submission.submission_id).map_err(AppError::from)?;
    if source.proposal.status != ProposalStatus::Accepted {
        return Err(validation_error(
            "proposal_id",
            "does not identify an accepted proposal",
        ));
    }
    let validated = proposals::validate(
        ProposalInput {
            kind: source.proposal.kind,
            hypothesis: source.proposal.hypothesis.clone(),
            source_experiment_id: source.proposal.source_experiment_id.clone(),
            argv: source.proposal.argv.clone(),
            working_directory: source.proposal.working_directory.clone(),
            expected_evidence: source.proposal.expected_evidence.clone(),
        },
        &source.campaign.objective_digest,
    )?;
    if validated.canonical_digest() != source.proposal.canonical_digest
        || validated.kind() != source.proposal.kind
        || validated.hypothesis() != source.proposal.hypothesis
        || validated.source_experiment_id() != source.proposal.source_experiment_id.as_deref()
        || validated.argv() != source.proposal.argv.as_slice()
        || validated.working_directory() != source.proposal.working_directory
        || validated.expected_evidence() != source.proposal.expected_evidence.as_slice()
    {
        return Err(validation_error(
            "proposal.canonical_digest",
            "does not match the durable proposal fields",
        ));
    }
    if source.submission.kind != SubmissionKind::Experiment
        || source.submission.status != SubmissionStatus::Accepted
        || source.submission.project_id != project.project_id
        || source.submission.argv != source.proposal.argv
        || source.submission.pueue_task_id != source.experiment.pueue_task_id
        || source.submission.task_signature != source.experiment.task_signature
    {
        return Err(validation_error(
            "submission_id",
            "does not prove the selected experiment submission",
        ));
    }
    source_authority_require_submission_metadata(
        &source.submission.metadata,
        "campaign_id",
        &source.campaign.campaign_id,
    )?;
    source_authority_require_submission_metadata(
        &source.submission.metadata,
        "proposal_id",
        &source.proposal.proposal_id,
    )?;
    source_authority_require_submission_metadata(
        &source.submission.metadata,
        "experiment_id",
        &source.experiment.experiment_id,
    )?;

    let resume_of_experiment_id: Option<String> = connection
        .query_row(
            "SELECT resume_of_experiment_id FROM experiments WHERE experiment_id = ?1",
            [&source.experiment.experiment_id],
            |row| row.get(0),
        )
        .map_err(database_error("read source checkpoint lineage"))?;
    let incoming_checkpoint_successor: bool = connection
        .query_row(
            &format!(
                "SELECT EXISTS(
                     SELECT 1
                     FROM research_reviews AS review
                     WHERE review.successor_experiment_id = ?1
                       AND {CHECKPOINT_INCOMING_CLAIM_PREDICATE}
                 )"
            ),
            [&source.experiment.experiment_id],
            |row| row.get(0),
        )
        .map_err(database_error("check incoming checkpoint lineage"))?;
    let skip_ordinary_source_checks = source.experiment.code_change_run_id.is_some()
        || source.experiment.code_revision_sha.is_some()
        || source.proposal.kind == ProposalKind::CodeChange;
    let mut unsupported_reason = if source.experiment.code_change_run_id.is_some()
        || source.experiment.code_revision_sha.is_some()
        || source.proposal.kind == ProposalKind::CodeChange
    {
        Some("code-change experiments have no ordinary trainer source".to_owned())
    } else if source_authority_has_prior_checkpoint_source(&source.proposal.argv)
        || resume_of_experiment_id.is_some()
        || incoming_checkpoint_successor
    {
        Some("prior checkpoint sources require a durable checkpoint authority".to_owned())
    } else {
        None
    };
    let source_layout = match checkpoint_source_layout(
        &source.proposal.argv,
        &source.proposal.working_directory,
    ) {
        Ok(layout) => Some(layout),
        Err(_) => {
            if unsupported_reason.is_none() {
                unsupported_reason = Some("trainer source command shape is unsupported".to_owned());
            }
            None
        }
    };

    let source_task_id = source.experiment.pueue_task_id.ok_or_else(|| {
        validation_error("source_task_id", "is missing from the accepted experiment")
    })?;
    let source_managed_signature = source
        .experiment
        .task_signature
        .as_deref()
        .ok_or_else(|| validation_error("source_task_signature", "is missing"))?;
    if matches!(&expectation, SourceAuthorityExpectation::Fresh { .. })
        && (expected_attempt <= 0
            || expected_session_generation < 0
            || expected_agent_run_id.is_none_or(|run_id| run_id <= 0)
            || expected_event_id.is_none_or(|event_id| event_id <= 0)
            || expected_source_task_id.is_none_or(|task_id| task_id < 0)
            || source_task_id < 0)
    {
        return Err(validation_error(
            "research.source",
            "has invalid numeric authority identity",
        ));
    }
    if source_task_id != expected_source_task_id.unwrap_or(source_task_id)
        || source_managed_signature != expected_managed_signature
    {
        return Err(validation_error(
            "research.source",
            "does not match the expected task identity",
        ));
    }

    if let SourceAuthorityExpectation::Fresh { expected, .. } = &expectation {
        if source.experiment.status != ExperimentStatus::Accepted
            || source.campaign.state != CampaignState::Active
            || !project.enabled
            || project.paused
            || project.halted_reason.is_some()
        {
            return Err(validation_error(
                "research.source",
                "is not currently eligible for fresh preparation",
            ));
        }
        if expected.owner.source_task_id != Some(source_task_id) {
            return Err(validation_error(
                "research.source_task_id",
                "does not match the accepted experiment",
            ));
        }
    } else if !matches!(
        source.experiment.status,
        ExperimentStatus::Accepted
            | ExperimentStatus::Succeeded
            | ExperimentStatus::Failed
            | ExperimentStatus::Cancelled
    ) {
        return Err(validation_error(
            "research.source",
            "has an unsupported lifecycle status",
        ));
    }

    if let SourceAuthorityExpectation::Historical { checkpoint } = &expectation {
        if checkpoint.project_id != project.project_id
            || checkpoint.campaign_id != source.campaign.campaign_id
            || checkpoint.source_experiment_id != source.experiment.experiment_id
            || checkpoint.source_proposal_id != source.proposal.proposal_id
            || checkpoint.source_submission_id != source.submission.submission_id
            || checkpoint.source_task_id != source_task_id
            || checkpoint.source_managed_task_signature != source_managed_signature
            || checkpoint.campaign_objective_digest != source.campaign.objective_digest
            || checkpoint.source_proposal_canonical_digest != source.proposal.canonical_digest
            || project.root_path.to_str() != Some(checkpoint.source_root_canonical_path.as_str())
            || source.proposal.argv != checkpoint.source_argv
            || source.proposal.working_directory != checkpoint.source_working_directory
        {
            return Err(validation_error(
                "checkpoint.source",
                "does not match the durable source graph",
            ));
        }
    }

    let context_json = review
        .context_json
        .clone()
        .ok_or_else(|| validation_error("research.context", "is missing"))?;
    let context_digest = review
        .context_digest
        .clone()
        .ok_or_else(|| validation_error("research.context_digest", "is missing"))?;
    if format!("{:x}", Sha256::digest(context_json.as_bytes())) != context_digest {
        return Err(validation_error(
            "research.context_digest",
            "does not match the persisted context",
        ));
    }
    if let SourceAuthorityExpectation::Historical { checkpoint } = &expectation {
        if context_digest != checkpoint.context_digest {
            return Err(validation_error(
                "research.context_digest",
                "does not match the prepared checkpoint",
            ));
        }
    }
    let support = checkpoint_support_from_persisted_context(&context_json, &context_digest)?;
    let context: Value = serde_json::from_str(&context_json).map_err(|source| {
        AppError::Serialization {
            operation: "parse source authority context",
            source,
        }
    })?;
    if !research_context_identity_matches(
        &context,
        &project.project_id,
        &source.campaign.campaign_id,
        &review.review_id,
        &source.experiment.experiment_id,
        source_managed_signature,
        Some(source_task_id),
        &source.campaign.objective_digest,
    ) {
        return Err(validation_error(
            "research.context",
            "does not bind the source graph",
        ));
    }

    let response_json = review
        .response_json
        .clone()
        .ok_or_else(|| validation_error("research.response", "is missing"))?;
    if let SourceAuthorityExpectation::Fresh { expected, .. } = &expectation {
        if response_json != expected.response_json {
            return Err(validation_error(
                "research.response",
                "does not match the ready authority",
            ));
        }
    }
    if let SourceAuthorityExpectation::Historical { checkpoint } = &expectation {
        if format!("{:x}", Sha256::digest(response_json.as_bytes())) != checkpoint.response_digest
        {
            return Err(validation_error(
                "research.response_digest",
                "does not match the prepared checkpoint",
            ));
        }
    }
    let answer = parse_research_answer(response_json.as_bytes())?;
    let expected_request = match &expectation {
        SourceAuthorityExpectation::Fresh { request, .. } => *request,
        SourceAuthorityExpectation::Historical { checkpoint } => &checkpoint.request,
    };
    if let SourceAuthorityExpectation::Fresh { expected, .. } = &expectation {
        if !source_authority_answers_equal(&answer, &expected.answer) {
            return Err(validation_error(
                "research.response",
                "does not equal the ready authority answer",
            ));
        }
    }
    if answer.review_id != review.review_id
        || answer.experiment_id != source.experiment.experiment_id
        || answer.context_digest != context_digest
        || answer.action != "resume_from_checkpoint"
        || answer.checkpoint.as_ref() != Some(expected_request)
    {
        return Err(validation_error(
            "research.response",
            "does not bind the requested checkpoint action",
        ));
    }

    let selected = match &support {
        CheckpointSupportEvidenceV1::Unavailable { reason, .. } => {
            if unsupported_reason.is_none() {
                unsupported_reason = Some(reason.clone());
            }
            None
        }
        CheckpointSupportEvidenceV1::Available {
            support_version,
            source_experiment_id,
            source_proposal_id,
            source_submission_id,
            normalized_working_directory,
            working_directory_record,
            loader_support,
            checkpoint_candidates,
            ..
        } => {
            if *support_version != crate::research_checkpoint::CHECKPOINT_SUPPORT_VERSION
                || source_experiment_id != &source.experiment.experiment_id
                || source_proposal_id != &source.proposal.proposal_id
                || source_submission_id != &source.submission.submission_id
                || normalized_working_directory != &source.proposal.working_directory
                || loader_support.len() != 1
                || checkpoint_candidates.is_empty()
            {
                return Err(validation_error(
                    "checkpoint_support",
                    "does not bind the durable source graph",
                ));
            }
            let loader = &loader_support[0];
            if !skip_ordinary_source_checks {
                if let Some(source_layout) = source_layout.as_ref() {
                    if loader.argv_index != source_layout.entrypoint_index
                        || loader.argv_token != source_layout.entrypoint_token
                        || loader.root_relative_path
                            != source_layout.entrypoint_root_relative_path
                    {
                        return Err(validation_error(
                            "checkpoint_support.loader",
                            "does not match the source command layout",
                        ));
                    }
                }
            }
            if !research_answer_evidence_refs_are_bound(
                &context,
                &context_json,
                &context_digest,
                &answer,
            )? {
                return Err(validation_error(
                    "research.response",
                    "cites evidence outside the persisted support packet",
                ));
            }
            let selected = select_checkpoint_support(&support, expected_request)?;
            if let SourceAuthorityExpectation::Historical { checkpoint } = &expectation {
                if working_directory_record != &checkpoint.source_working_directory_record
                    || normalized_working_directory != &checkpoint.source_working_directory
                    || selected.loader != &checkpoint.loader
                    || selected.candidate != &checkpoint.source_checkpoint
                    || checkpoint.support_version != *support_version
                {
                    return Err(validation_error(
                        "checkpoint_support",
                        "does not match the prepared checkpoint evidence",
                    ));
                }
            }
            Some(selected)
        }
    };

    let observation = match &expectation {
        SourceAuthorityExpectation::Fresh { .. } => {
            let latest = super::repositories::read_latest_task_observations_in_connection(
                connection,
                &project.project_id,
                source_task_id,
            )?;
            if latest.len() != 1 {
                return Err(validation_error(
                    "task_signature",
                    "has ambiguous current task observations",
                ));
            }
            latest.into_iter().next().expect("one latest observation")
        }
        SourceAuthorityExpectation::Historical { .. } => {
            super::repositories::read_task_observation_by_pueue_task_and_signature_in_connection(
                connection,
                &project.project_id,
                source_task_id,
                expected_raw_signature,
            )?
            .ok_or_else(|| {
                validation_error(
                    "task_signature",
                    "does not identify one historical source observation",
                )
            })?
        }
    };
    if observation.task_signature != expected_raw_signature
        || observation.pueue_task_id != source_task_id
        || observation.project_id != project.project_id
        || observation.pueue_group != project.pueue_group
        || !observation.state.eq_ignore_ascii_case("running")
        || managed_task_run_signature_for_observation(&observation, &project.pueue_group)
            .as_deref()
            != Some(source_managed_signature)
    {
        return Err(validation_error(
            "task_signature",
            "does not identify the persisted managed running source",
        ));
    }

    if !skip_ordinary_source_checks {
        let runtime_argv = campaign_experiment_runtime_argv(
            &project.root_path,
            &source.campaign.campaign_id,
            &source.experiment.experiment_id,
            &source.proposal.argv,
        );
        let expected_command = try_canonical_command_display_os(&runtime_argv)?;
        if observation.command.len() != 1 || observation.command[0] != expected_command {
            return Err(validation_error(
                "task_observation.command",
                "does not match the selected experiment runtime command",
            ));
        }
    }
    if let Some(reason) = unsupported_reason {
        return Ok(CheckpointSourceAuthorityRead::Unsupported { reason });
    }

    if source_layout.is_none() {
        return Err(validation_error(
            "checkpoint_source",
            "has no validated trainer source layout",
        ));
    }

    if let SourceAuthorityExpectation::Historical { checkpoint } = &expectation {
        if checkpoint.source_raw_task_signature != observation.task_signature
            || checkpoint.source_task_id != observation.pueue_task_id
        {
            return Err(validation_error(
                "checkpoint.source_raw_task_signature",
                "does not match the captured source observation",
            ));
        }
    }
    let _ = selected;
    Ok(CheckpointSourceAuthorityRead::Supported(
        CheckpointSourceAuthority {
            project,
            source,
            observation,
            support,
        },
    ))
}

fn checkpoint_matches_fresh_authority(
    checkpoint: &PreparedCheckpoint,
    expected: &ReadyResearchAction,
    authority: &CheckpointSourceAuthority,
    request: &CheckpointRequest,
) -> Result<(), AppError> {
    let source_task_id = expected.owner.source_task_id.ok_or_else(|| {
        validation_error("checkpoint.source_task_id", "is missing from the ready owner")
    })?;
    let agent_run_id = expected.owner.agent_run_id.ok_or_else(|| {
        validation_error("checkpoint.agent_run_id", "is missing from the ready owner")
    })?;
    let event_id = expected.owner.event_id.ok_or_else(|| {
        validation_error("checkpoint.event_id", "is missing from the ready owner")
    })?;
    if checkpoint.project_id != expected.owner.project_id
        || checkpoint.campaign_id != expected.owner.campaign_id
        || checkpoint.review_id != expected.owner.review_id
        || checkpoint.review_attempt != expected.owner.attempt
        || checkpoint.review_session_generation != expected.owner.session_generation
        || checkpoint.review_agent_run_id != agent_run_id
        || checkpoint.review_event_id != event_id
        || checkpoint.source_experiment_id != expected.owner.source_experiment_id
        || checkpoint.source_proposal_id != authority.source.proposal.proposal_id
        || checkpoint.source_submission_id != authority.source.submission.submission_id
        || checkpoint.source_task_id != source_task_id
        || checkpoint.source_managed_task_signature != expected.owner.managed_task_signature
        || checkpoint.source_raw_task_signature != authority.observation.task_signature
        || checkpoint.context_digest != expected.context_digest
        || checkpoint.response_digest
            != format!("{:x}", Sha256::digest(expected.response_json.as_bytes()))
        || checkpoint.campaign_objective_digest != expected.campaign_objective_digest
        || checkpoint.source_proposal_canonical_digest
            != authority.source.proposal.canonical_digest
        || checkpoint.source_argv != authority.source.proposal.argv
        || checkpoint.source_working_directory != authority.source.proposal.working_directory
        || checkpoint.request != *request
        || checkpoint.source_root_canonical_path
            != authority.project.root_path.to_string_lossy().as_ref()
        || checkpoint.learning_spec_digest
            != checkpoint_learning_spec_digest(
                &authority.source.proposal.argv,
                &authority.source.proposal.working_directory,
            )?
        || checkpoint.successor_ids
            != crate::research_checkpoint::checkpoint_successor_ids(
                &checkpoint.review_id,
                checkpoint.review_attempt,
            )?
    {
        return Err(validation_error(
            "checkpoint.source",
            "does not match the current ready source authority",
        ));
    }
    let CheckpointSupportEvidenceV1::Available {
        support_version,
        normalized_working_directory,
        working_directory_record,
        loader_support,
        checkpoint_candidates,
        ..
    } = &authority.support
    else {
        return Err(validation_error(
            "checkpoint_support",
            "is unavailable for the current source",
        ));
    };
    let selected = select_checkpoint_support(&authority.support, request)?;
    if checkpoint.support_version != *support_version
        || checkpoint.support_version
            != crate::research_checkpoint::CHECKPOINT_SUPPORT_VERSION
        || loader_support.len() != 1
        || checkpoint_candidates.is_empty()
        || checkpoint.source_working_directory_record != *working_directory_record
        || checkpoint.source_working_directory != *normalized_working_directory
        || checkpoint.loader != *selected.loader
        || checkpoint.source_checkpoint != *selected.candidate
        || checkpoint.source_checkpoint.argv_path != request.path
        || checkpoint.retained_checkpoint.logical_bytes != checkpoint.source_checkpoint.length
        || checkpoint.retained_checkpoint.sha256 != checkpoint.source_checkpoint.sha256
    {
        return Err(validation_error(
            "checkpoint_support",
            "does not match the persisted source support packet",
        ));
    }
    if checkpoint.retained_checkpoint.relative_path
        != format!(
            "research-checkpoints/{}/{}/checkpoint",
            checkpoint.campaign_id, checkpoint.review_id
        )
    {
        return Err(validation_error(
            "checkpoint.retained_checkpoint",
            "has an unexpected durable path",
        ));
    }
    Ok(())
}

pub(crate) fn checkpoint_retry_admission_available_for_campaign(
    transaction: &Transaction<'_>,
    checkpoint: &PreparedCheckpoint,
    limits: &crate::execution_policy::CampaignLimits,
    now: i64,
) -> Result<bool, AppError> {
    Ok(checkpoint_retry_admission_count(transaction, checkpoint, limits, now)?.is_some())
}

pub(crate) fn checkpoint_retry_admission_count(
    transaction: &Transaction<'_>,
    checkpoint: &PreparedCheckpoint,
    limits: &crate::execution_policy::CampaignLimits,
    now: i64,
) -> Result<Option<i64>, AppError> {
    if !super::campaigns::replacement_admission_available_in_transaction(
        transaction,
        &checkpoint.campaign_id,
        &checkpoint.source_experiment_id,
        limits,
        now,
    )? {
        return Ok(None);
    }
    let live_repairs = super::campaigns::count_live_repair_descendants(
        transaction,
        &checkpoint.source_experiment_id,
    )
    .map_err(database_error("count checkpoint repair descendants"))?;
    if live_repairs >= i64::from(limits.max_live_repairs) {
        return Ok(None);
    }

    let same_spec_count = checkpoint_same_spec_count(transaction, checkpoint)?;
    Ok((same_spec_count <= i64::from(limits.max_same_spec_retries))
        .then_some(same_spec_count))
}

pub(crate) fn checkpoint_same_spec_count(
    transaction: &Transaction<'_>,
    checkpoint: &PreparedCheckpoint,
) -> Result<i64, AppError> {
    let source_argv_json = serde_json::to_string(&checkpoint.source_argv).map_err(|source| {
        AppError::Serialization {
            operation: "serialize checkpoint learning argv",
            source,
        }
    })?;
    let mut same_spec_ids = BTreeSet::new();
    let mut ordinary = transaction
        .prepare(
            "SELECT experiment.experiment_id
             FROM experiments AS experiment
             JOIN proposals AS proposal ON proposal.proposal_id = experiment.proposal_id
             WHERE experiment.campaign_id = ?1
               AND proposal.argv_json = ?2
               AND proposal.working_directory = ?3
             ORDER BY experiment.experiment_id",
        )
        .map_err(database_error("prepare checkpoint learning-spec query"))?;
    for row in ordinary
        .query_map(
            params![
                checkpoint.campaign_id,
                source_argv_json,
                checkpoint.source_working_directory,
            ],
            |row| row.get::<_, String>(0),
        )
        .map_err(database_error("query checkpoint learning-spec history"))?
    {
        same_spec_ids.insert(row.map_err(database_error("read checkpoint learning-spec history"))?);
    }

    let mut historical = transaction
        .prepare(&format!(
            "SELECT review.review_id, review.successor_experiment_id,
                    CASE
                      WHEN typeof(review.checkpoint_json) = 'text'
                       AND length(CAST(review.checkpoint_json AS BLOB)) BETWEEN 1 AND 131072
                      THEN review.checkpoint_json
                    END,
                    typeof(review.checkpoint_json), length(CAST(review.checkpoint_json AS BLOB))
             FROM research_reviews AS review
             LEFT JOIN experiments AS successor
               ON successor.experiment_id = review.successor_experiment_id
             WHERE (review.campaign_id = ?1 OR successor.campaign_id = ?1)
               AND review.successor_experiment_id IS NOT NULL
               AND {CHECKPOINT_INCOMING_CLAIM_PREDICATE}
             ORDER BY review.successor_experiment_id, review.review_id"
        ))
        .map_err(database_error("prepare checkpoint history query"))?;
    let rows = historical
        .query_map([checkpoint.campaign_id.as_str()], |row| {
            let review_id: String = row.get(0)?;
            let successor: String = row.get(1)?;
            let storage: String = row.get(3)?;
            let byte_len: Option<i64> = row.get(4)?;
            let raw = if storage == "text"
                && byte_len.is_some_and(|len| (1..=MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES as i64).contains(&len))
            {
                match row.get_ref(2)? {
                    ValueRef::Text(bytes) => Some(
                        String::from_utf8(bytes.to_vec()).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                Type::Text,
                                Box::new(error),
                            )
                        })?,
                    ),
                    _ => None,
                }
            } else {
                None
            };
            Ok((review_id, successor, storage, byte_len, raw))
        })
        .map_err(database_error("query checkpoint learning-spec history"))?;
    let mut seen_successors = BTreeSet::new();
    for row in rows {
        let (review_id, successor, storage, byte_len, raw) = row
            .map_err(database_error("read checkpoint learning-spec history"))?;
        if !seen_successors.insert(successor.clone()) {
            return Err(validation_error(
                "research.checkpoint",
                "historical successor link is duplicated",
            ));
        }
        if storage != "text"
            || !byte_len.is_some_and(|len| (1..=MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES as i64).contains(&len))
        {
            return Err(validation_error(
                "research.checkpoint",
                "historical checkpoint is not bounded text",
            ));
        }
        let raw = raw.ok_or_else(|| {
            validation_error("research.checkpoint", "historical checkpoint is not valid UTF-8")
        })?;
        let historical_checkpoint = parse_prepared_checkpoint(&raw)?;
        if historical_checkpoint.review_id != review_id
            || historical_checkpoint.campaign_id != checkpoint.campaign_id
            || historical_checkpoint.successor_ids.experiment_id != successor
        {
            return Err(validation_error(
                "research.checkpoint",
                "historical successor link does not match the checkpoint",
            ));
        }
        let successor_witness = checkpoint_successor_witness(transaction, &successor)?;
        let termination_request_id: Option<i64> = transaction
            .query_row(
                "SELECT termination_request_id FROM research_reviews WHERE review_id = ?1",
                [&review_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("read historical checkpoint termination"))?;
        let historical_authority = CheckpointDispatchAuthority {
            checkpoint: historical_checkpoint.clone(),
            raw_checkpoint: raw,
            project_id: historical_checkpoint.project_id.clone(),
            campaign_id: historical_checkpoint.campaign_id.clone(),
            review_id: review_id.clone(),
            source_experiment_id: historical_checkpoint.source_experiment_id.clone(),
            proposal_id: historical_checkpoint.successor_ids.proposal_id.clone(),
            submission_id: historical_checkpoint.successor_ids.submission_id.clone(),
            successor_experiment_id: historical_checkpoint.successor_ids.experiment_id.clone(),
            successor_status: None,
            successor_attempt: successor_witness.map(|(attempt, _)| attempt),
            reservation_window_ends_at: successor_witness.map(|(_, window_end)| window_end),
            termination_request_id,
        };
        if !super::campaigns::checkpoint_successor_graph_matches_authority(
            transaction,
            &historical_authority,
            None,
            None,
        )? {
            return Err(validation_error(
                "research.checkpoint",
                "historical successor graph is not exact",
            ));
        }
        if historical_checkpoint.learning_spec_digest
            == checkpoint.learning_spec_digest
        {
            same_spec_ids.insert(successor);
        }
    }
    Ok(same_spec_ids.len() as i64)
}

pub(crate) fn checkpoint_successor_preflight_in_transaction(
    transaction: &Transaction<'_>,
    expected: &ReadyResearchAction,
    checkpoint: &PreparedCheckpoint,
    limits: &crate::execution_policy::CampaignLimits,
    now: i64,
) -> Result<CheckpointSuccessorPreflight, AppError> {
    let request = expected.answer.checkpoint.as_ref().ok_or_else(|| {
        validation_error("research.checkpoint", "ready answer has no checkpoint request")
    })?;
    let authority = match checkpoint_source_authority_for_ready_in_connection(
        transaction,
        expected,
        request,
    )? {
        CheckpointSourceAuthorityRead::Supported(authority) => authority,
        CheckpointSourceAuthorityRead::Unsupported { .. } => {
            return Err(validation_error(
                "checkpoint.source",
                "is unsupported for checkpoint admission",
            ));
        }
    };
    checkpoint_matches_fresh_authority(checkpoint, expected, &authority, request)?;
    if checkpoint_retry_admission_available_for_campaign(transaction, checkpoint, limits, now)? {
        Ok(CheckpointSuccessorPreflight::Available)
    } else {
        Ok(CheckpointSuccessorPreflight::Deferred)
    }
}

pub(crate) fn bind_checkpoint_termination_intent_in_transaction(
    transaction: &Transaction<'_>,
    expected: &ReadyResearchAction,
    checkpoint_json: &str,
    incident: &Incident,
    request: &TerminationRequest,
    now: i64,
) -> Result<bool, AppError> {
    if checkpoint_json.as_bytes().is_empty()
        || checkpoint_json.as_bytes().len() > MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES
    {
        return Err(validation_error(
            "research.checkpoint_json",
            "must be 1 to 131072 bytes",
        ));
    }
    if incident.project_id != expected.owner.project_id
        || request.incident_id != incident.incident_id
        || request.project_id != expected.owner.project_id
        || request.task_signature != expected.raw_task_signature
        || !request
            .reason
            .starts_with(format!("research_action:{}:", expected.owner.review_id).as_str())
    {
        return Err(validation_error(
            "research.termination",
            "incident and request do not match the ready research owner",
        ));
    }
    let checkpoint = parse_prepared_checkpoint(checkpoint_json)?;
    let request_checkpoint = expected.answer.checkpoint.as_ref().ok_or_else(|| {
        validation_error(
            "research.checkpoint",
            "ready answer does not request a checkpoint",
        )
    })?;
    if expected.answer.action != "resume_from_checkpoint" || checkpoint.request != *request_checkpoint {
        return Err(validation_error(
            "research.checkpoint",
            "does not match the ready answer request",
        ));
    }
    let authority = match checkpoint_source_authority_for_ready_in_connection(
        transaction,
        expected,
        request_checkpoint,
    )? {
        CheckpointSourceAuthorityRead::Supported(authority) => authority,
        CheckpointSourceAuthorityRead::Unsupported { .. } => {
            return Err(validation_error(
                "checkpoint.source",
                "is unsupported for checkpoint intent",
            ));
        }
    };
    checkpoint_matches_fresh_authority(&checkpoint, expected, &authority, request_checkpoint)?;
    let event_id = expected.owner.event_id.ok_or_else(|| {
        validation_error("research.event_id", "is missing from the ready owner")
    })?;
    let changed = transaction
        .execute(
            "UPDATE research_reviews
             SET checkpoint_json = ?1, operation_stage = 'intent',
                 termination_request_id = ?2, updated_at = ?3
             WHERE review_id = ?4 AND campaign_id = ?5
               AND experiment_id = ?6 AND task_signature = ?7
               AND attempt = ?8 AND session_generation = ?9
               AND agent_run_id = ?10 AND context_digest = ?11
               AND event_id = ?12 AND state = 'ready'
               AND operation_stage IS NULL AND termination_request_id IS NULL
               AND checkpoint_json IS NULL AND decision_cycle_id IS NULL
               AND successor_experiment_id IS NULL",
            params![
                checkpoint_json,
                request.request_id,
                now,
                expected.owner.review_id,
                expected.owner.campaign_id,
                expected.owner.source_experiment_id,
                expected.owner.managed_task_signature,
                expected.owner.attempt,
                expected.owner.session_generation,
                expected.owner.agent_run_id,
                expected.context_digest,
                event_id,
            ],
        )
        .map_err(database_error("bind checkpoint research termination intent"))?;
    Ok(changed == 1)
}

pub(crate) fn block_checkpoint_orphan_in_transaction(
    transaction: &Transaction<'_>,
    expected: &ReadyResearchAction,
    now: i64,
) -> Result<bool, AppError> {
    let event_id = expected.owner.event_id.ok_or_else(|| {
        validation_error("research.event_id", "is missing from the ready owner")
    })?;
    let current = transaction
        .query_row(
            "SELECT campaign.project_id, campaign.objective_digest,
                    review.campaign_id, review.experiment_id, review.task_signature,
                    review.attempt, review.state, review.operation_stage,
                    review.agent_run_id, review.context_json, review.context_digest,
                    review.response_json, review.termination_request_id,
                    review.successor_experiment_id, review.session_generation,
                    review.event_id, COALESCE(review.notes_json, '{}'),
                    review.failure_code, review.decision_cycle_id
             FROM research_reviews AS review
             JOIN campaigns AS campaign ON campaign.campaign_id = review.campaign_id
             WHERE review.review_id = ?1 AND review.checkpoint_json IS NULL",
            [&expected.owner.review_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, Option<i64>>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, i64>(14)?,
                    row.get::<_, Option<i64>>(15)?,
                    row.get::<_, String>(16)?,
                    row.get::<_, Option<String>>(17)?,
                    row.get::<_, Option<String>>(18)?,
                ))
            },
        )
        .optional()
        .map_err(database_error("read orphan checkpoint authority"))?;
    let Some((
        project_id,
        objective_digest,
        campaign_id,
        source_experiment_id,
        task_signature,
        attempt,
        state,
        operation_stage,
        agent_run_id,
        context_json,
        context_digest,
        response_json,
        termination_request_id,
        successor_experiment_id,
        session_generation,
        persisted_event_id,
        notes_json,
        failure_code,
        decision_cycle_id,
    )) = current
    else {
        return Ok(false);
    };
    if project_id != expected.owner.project_id
        || objective_digest != expected.campaign_objective_digest
        || campaign_id != expected.owner.campaign_id
        || source_experiment_id != expected.owner.source_experiment_id
        || task_signature != expected.owner.managed_task_signature
        || attempt != expected.owner.attempt
        || state != "ready"
        || operation_stage != expected.owner.operation_stage
        || agent_run_id != expected.owner.agent_run_id
        || context_json.as_deref() != Some(expected.context_json.as_str())
        || context_digest != Some(expected.context_digest.clone())
        || response_json.as_deref() != Some(expected.response_json.as_str())
        || termination_request_id != expected.owner.termination_request_id
        || successor_experiment_id != expected.owner.successor_experiment_id
        || session_generation != expected.owner.session_generation
        || persisted_event_id != Some(event_id)
        || notes_json != expected.notes_json
        || failure_code.is_some()
        || decision_cycle_id != expected.owner.decision_cycle_id
        || expected.owner.recovery_required
    {
        return Ok(false);
    }
    let changed = transaction
        .execute(
            "UPDATE research_reviews
             SET state = 'blocked', failure_code = 'research_checkpoint_orphaned',
                 finished_at = ?1, not_before = ?1, updated_at = ?1
             WHERE review_id = ?2 AND campaign_id = ?3
               AND experiment_id = ?4 AND task_signature = ?5
               AND attempt = ?6 AND session_generation = ?7
               AND agent_run_id = ?8 AND event_id = ?9
               AND context_json = ?10 AND context_digest IS ?11
               AND response_json = ?12 AND COALESCE(notes_json, '{}') = ?13
               AND state = 'ready' AND operation_stage IS NULL
               AND termination_request_id IS NULL AND checkpoint_json IS NULL
               AND decision_cycle_id IS NULL AND successor_experiment_id IS NULL
               AND failure_code IS NULL
               AND EXISTS (
                   SELECT 1 FROM campaigns
                   WHERE campaign_id = research_reviews.campaign_id
                     AND project_id = ?14
                     AND objective_digest = ?15
               )",
            params![
                now,
                expected.owner.review_id,
                expected.owner.campaign_id,
                expected.owner.source_experiment_id,
                expected.owner.managed_task_signature,
                expected.owner.attempt,
                expected.owner.session_generation,
                expected.owner.agent_run_id,
                event_id,
                expected.context_json,
                Some(expected.context_digest.as_str()),
                expected.response_json,
                expected.notes_json,
                expected.owner.project_id,
                expected.campaign_objective_digest,
            ],
        )
        .map_err(database_error("block orphaned checkpoint review"))?;
    Ok(changed == 1)
}

pub(crate) fn block_invalid_checkpoint_review_in_transaction(
    transaction: &Transaction<'_>,
    expected: &ResearchReview,
    now: i64,
) -> Result<bool, AppError> {
    let current = transaction
        .query_row(
            "SELECT campaign_id, experiment_id, task_signature, attempt,
                    state, operation_stage, agent_run_id, context_digest,
                    termination_request_id, successor_experiment_id,
                    session_generation,
                    CASE
                      WHEN typeof(checkpoint_json) = 'text'
                       AND length(CAST(checkpoint_json AS BLOB)) BETWEEN 1 AND 131072
                      THEN checkpoint_json
                    END,
                    typeof(checkpoint_json), length(CAST(checkpoint_json AS BLOB))
             FROM research_reviews WHERE review_id = ?1",
            [&expected.review_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, String>(12)?,
                    row.get::<_, Option<i64>>(13)?,
                ))
            },
        )
        .optional()
        .map_err(database_error("read invalid checkpoint authority"))?;
    let Some((
        campaign_id,
        experiment_id,
        task_signature,
        attempt,
        state,
        operation_stage,
        agent_run_id,
        context_digest,
        termination_request_id,
        successor_experiment_id,
        session_generation,
        raw,
        storage,
        byte_len,
    )) = current
    else {
        return Ok(false);
    };
    let storage_class = checkpoint_sqlite_storage_class(&storage, 12)
        .map_err(database_error("classify invalid checkpoint storage"))?;
    let observed_state = match storage_class {
        CheckpointSqliteStorageClass::Text
            if byte_len.is_some_and(|len| (1..=MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES as i64).contains(&len)) =>
        {
            if raw.as_deref() != expected.checkpoint_json.as_deref() {
                return Ok(false);
            }
            CheckpointJsonState::BoundedText
        }
        CheckpointSqliteStorageClass::Null if byte_len.is_none() => CheckpointJsonState::Missing,
        storage_class => CheckpointJsonState::Invalid {
            storage_class,
            byte_len,
        },
    };
    if campaign_id != expected.campaign_id
        || experiment_id != expected.experiment_id
        || task_signature != expected.task_signature
        || attempt != expected.attempt
        || state != "ready"
        || operation_stage.as_deref() != Some("successor_reserved")
        || agent_run_id != expected.agent_run_id
        || context_digest != expected.context_digest
        || termination_request_id != expected.termination_request_id
        || successor_experiment_id != expected.successor_experiment_id
        || session_generation != expected.session_generation
        || observed_state != expected.checkpoint_json_state
    {
        return Ok(false);
    }
    let changed = transaction
        .execute(
            "UPDATE research_reviews
             SET state = 'blocked', failure_code = 'research_checkpoint_authority_corrupt',
                 finished_at = ?1, not_before = ?1, updated_at = ?1
             WHERE review_id = ?2 AND campaign_id = ?3 AND experiment_id = ?4
               AND task_signature = ?5 AND attempt = ?6
               AND session_generation = ?7 AND agent_run_id IS ?8
               AND context_digest IS ?9 AND termination_request_id IS ?10
               AND successor_experiment_id IS ?11
               AND state = 'ready' AND operation_stage = 'successor_reserved'
               AND decision_cycle_id IS NULL",
            params![
                now,
                expected.review_id,
                expected.campaign_id,
                expected.experiment_id,
                expected.task_signature,
                expected.attempt,
                expected.session_generation,
                expected.agent_run_id,
                expected.context_digest,
                expected.termination_request_id,
                expected.successor_experiment_id,
            ],
        )
        .map_err(database_error("block invalid checkpoint review"))?;
    Ok(changed == 1)
}

fn source_authority_require_submission_metadata(
    metadata: &Value,
    key: &'static str,
    expected: &str,
) -> Result<(), AppError> {
    let object = metadata.as_object().ok_or_else(|| {
        validation_error("submission.metadata", "must be an object with campaign lineage")
    })?;
    if object.get(key).and_then(Value::as_str) != Some(expected) {
        return Err(validation_error(
            "submission.metadata",
            "does not prove the selected campaign lineage",
        ));
    }
    Ok(())
}

fn source_authority_answers_equal(left: &ResearchAnswer, right: &ResearchAnswer) -> bool {
    left.schema_version == right.schema_version
        && left.review_id == right.review_id
        && left.experiment_id == right.experiment_id
        && left.context_digest == right.context_digest
        && left.action == right.action
        && left.reason == right.reason
        && left.evidence_refs == right.evidence_refs
        && left.notes == right.notes
        && left.next_direction == right.next_direction
        && left.checkpoint == right.checkpoint
}

fn source_authority_has_prior_checkpoint_source(argv: &[String]) -> bool {
    argv.iter()
        .any(|argument| argument == "--resume" || argument.starts_with("--resume="))
}

/// Discard a ready answer whose source task has naturally terminated or whose
/// live numeric identity no longer matches the persisted managed target.  The
/// project and campaign gates are repeated in this CAS so a pause or disable
/// preserves the ready answer for a later admission pass.
pub(crate) fn discard_ready_research_action_in_transaction(
    transaction: &Transaction<'_>,
    project_id: &str,
    review_id: &str,
    reason: &str,
    now: i64,
) -> Result<bool, AppError> {
    if reason.is_empty() || reason.len() > 128 || reason.chars().any(char::is_control) {
        return Err(validation_error("research.failure_code", "must be bounded"));
    }
    let changed = transaction
        .execute(
            "UPDATE research_reviews
             SET state = 'discarded', failure_code = ?1,
                 finished_at = ?2, not_before = ?2, updated_at = ?2
             WHERE review_id = ?3 AND state = 'ready'
               AND operation_stage IS NULL
               AND termination_request_id IS NULL
               AND EXISTS (
                   SELECT 1
                   FROM campaigns AS campaign
                   JOIN projects AS project ON project.project_id = campaign.project_id
                   WHERE campaign.campaign_id = research_reviews.campaign_id
                     AND campaign.project_id = ?4
                     AND campaign.state = 'active'
                     AND project.enabled = 1
                     AND project.paused = 0
                     AND project.halted_reason IS NULL
               )",
            params![reason, now, review_id, project_id],
        )
        .map_err(database_error("discard stale research action"))?;
    Ok(changed == 1)
}

pub(crate) fn discard_undispatched_research_action_in_transaction(
    transaction: &Transaction<'_>,
    review_id: &str,
    request_id: i64,
    reason: &str,
    now: i64,
) -> Result<bool, AppError> {
    if reason.is_empty() || reason.len() > 128 || reason.chars().any(char::is_control) {
        return Err(validation_error("research.failure_code", "must be bounded"));
    }
    let changed = transaction
        .execute(
            "UPDATE research_reviews
             SET state = 'discarded', operation_stage = NULL, failure_code = ?1,
                 finished_at = ?2, not_before = ?2, updated_at = ?2
             WHERE review_id = ?3 AND state = 'ready'
               AND operation_stage = 'intent'
               AND termination_request_id = ?4
               AND decision_cycle_id IS NULL
               AND successor_experiment_id IS NULL",
            params![reason, now, review_id, request_id],
        )
        .map_err(database_error("discard undispatched research action"))?;
    Ok(changed == 1)
}

pub(crate) fn discard_missing_undispatched_research_action_in_transaction(
    transaction: &Transaction<'_>,
    project_id: &str,
    review_id: &str,
    request_id: i64,
    now: i64,
) -> Result<bool, AppError> {
    let changed = transaction
        .execute(
            "UPDATE research_reviews
             SET state = 'discarded', operation_stage = NULL,
                 failure_code = 'research_natural_finish_before_dispatch',
                 finished_at = ?1, not_before = ?1, updated_at = ?1
             WHERE review_id = ?2 AND state = 'ready'
               AND operation_stage = 'intent'
               AND termination_request_id = ?3
               AND decision_cycle_id IS NULL
               AND successor_experiment_id IS NULL
               AND EXISTS (
                   SELECT 1 FROM termination_requests
                   WHERE request_id = ?3 AND project_id = ?4
                     AND status = 'confirmed' AND grace_until IS NULL
                     AND last_error LIKE 'termination_undispatched:%'
               )",
            params![now, review_id, request_id, project_id],
        )
        .map_err(database_error(
            "discard missing undispatched research action",
        ))?;
    Ok(changed == 1)
}

pub(super) fn research_context_identity_matches(
    context: &Value,
    project_id: &str,
    campaign_id: &str,
    review_id: &str,
    experiment_id: &str,
    managed_task_signature: &str,
    source_task_id: Option<i64>,
    objective_digest: &str,
) -> bool {
    let Some(source_task_id) = source_task_id else {
        return false;
    };
    context
        .get("schema_version")
        .and_then(Value::as_i64)
        == Some(1)
        && context
            .get("facts")
            .and_then(|facts| facts.get("review"))
            .and_then(|review| review.get("review_id"))
            .and_then(Value::as_str)
            == Some(review_id)
        && context
            .get("facts")
            .and_then(|facts| facts.get("review"))
            .and_then(|review| review.get("experiment_id"))
            .and_then(Value::as_str)
            == Some(experiment_id)
        && context
            .get("facts")
            .and_then(|facts| facts.get("review"))
            .and_then(|review| review.get("task_signature"))
            .and_then(Value::as_str)
            == Some(managed_task_signature)
        && context
            .get("facts")
            .and_then(|facts| facts.get("campaign"))
            .and_then(|campaign| campaign.get("campaign_id"))
            .and_then(Value::as_str)
            == Some(campaign_id)
        && context
            .get("facts")
            .and_then(|facts| facts.get("project"))
            .and_then(|project| project.get("project_id"))
            .and_then(Value::as_str)
            == Some(project_id)
        && context
            .get("facts")
            .and_then(|facts| facts.get("objective"))
            .and_then(|objective| objective.get("digest"))
            .and_then(Value::as_str)
            == Some(objective_digest)
        && context
            .get("facts")
            .and_then(|facts| facts.get("target"))
            .and_then(|target| target.get("experiment_id"))
            .and_then(Value::as_str)
            == Some(experiment_id)
        && context
            .get("facts")
            .and_then(|facts| facts.get("target"))
            .and_then(|target| target.get("pueue_task_id"))
            .and_then(Value::as_i64)
            == Some(source_task_id)
        && context
            .get("facts")
            .and_then(|facts| facts.get("target"))
            .and_then(|target| target.get("task_signature"))
            .and_then(Value::as_str)
            == Some(managed_task_signature)
}

pub(super) fn context_evidence_refs(value: &Value) -> BTreeSet<String> {
    let mut refs = BTreeSet::new();
    fn visit(value: &Value, refs: &mut BTreeSet<String>) {
        match value {
            Value::Object(object) => {
                if let Some(reference) = object.get("evidence_ref").and_then(Value::as_str) {
                    refs.insert(reference.to_owned());
                }
                for child in object.values() {
                    visit(child, refs);
                }
            }
            Value::Array(values) => {
                for child in values {
                    visit(child, refs);
                }
            }
            _ => {}
        }
    }
    visit(value, &mut refs);
    refs
}

pub(super) fn research_answer_evidence_refs_are_bound(
    context: &Value,
    context_json: &str,
    context_digest: &str,
    answer: &ResearchAnswer,
) -> Result<bool, AppError> {
    let legacy_refs = context_evidence_refs(context);
    let has_nonlegacy_reference = answer
        .evidence_refs
        .iter()
        .any(|reference| !legacy_refs.contains(reference));
    if answer.checkpoint.is_none() && !has_nonlegacy_reference {
        return Ok(true);
    }

    let support = checkpoint_support_from_persisted_context(context_json, context_digest)?;
    let CheckpointSupportEvidenceV1::Available {
        loader_support,
        checkpoint_candidates,
        ..
    } = &support
    else {
        return Ok(false);
    };
    let mut allowed = legacy_refs;
    allowed.extend(loader_support.iter().map(|loader| loader.reference.clone()));
    allowed.extend(
        checkpoint_candidates
            .iter()
            .map(|candidate| candidate.reference.clone()),
    );
    if answer
        .evidence_refs
        .iter()
        .any(|reference| !allowed.contains(reference))
    {
        return Ok(false);
    }
    if let Some(checkpoint) = answer.checkpoint.as_ref() {
        let selected = select_checkpoint_support(&support, checkpoint)?;
        if checkpoint.support_evidence_refs.len() != 2
            || !checkpoint
                .support_evidence_refs
                .iter()
                .any(|reference| reference == &selected.loader.reference)
            || !checkpoint
                .support_evidence_refs
                .iter()
                .any(|reference| reference == &selected.candidate.reference)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn bind_research_termination_intent_in_transaction(
    transaction: &Transaction<'_>,
    expected: &ReadyResearchAction,
    incident: &Incident,
    request: &TerminationRequest,
    now: i64,
) -> Result<bool, AppError> {
    if incident.project_id != expected.owner.project_id
        || request.incident_id != incident.incident_id
        || request.project_id != expected.owner.project_id
        || request.task_signature != expected.raw_task_signature
        || !request
            .reason
            .starts_with(format!("research_action:{}:", expected.owner.review_id).as_str())
    {
        return Err(validation_error(
            "research.termination",
            "incident and request do not match the ready research owner",
        ));
    }
    let Some(event_id) = expected.owner.event_id else {
        return Ok(false);
    };
    let changed = transaction
        .execute(
            "UPDATE research_reviews
             SET operation_stage = 'intent', termination_request_id = ?1,
                 updated_at = ?2
             WHERE review_id = ?3 AND campaign_id = ?4
               AND experiment_id = ?5 AND task_signature = ?6
               AND attempt = ?7 AND session_generation = ?8
               AND agent_run_id = ?9 AND context_digest = ?10
               AND event_id = ?11 AND state = 'ready'
               AND operation_stage IS NULL
               AND termination_request_id IS NULL",
            params![
                request.request_id,
                now,
                expected.owner.review_id,
                expected.owner.campaign_id,
                expected.owner.source_experiment_id,
                expected.owner.managed_task_signature,
                expected.owner.attempt,
                expected.owner.session_generation,
                expected.owner.agent_run_id,
                expected.context_digest,
                event_id,
            ],
        )
        .map_err(database_error("bind research termination intent"))?;
    Ok(changed == 1)
}

pub(crate) fn complete_research_continue_in_transaction(
    transaction: &Transaction<'_>,
    expected: &ReadyResearchAction,
    notes_json: &str,
    next_due_at: Option<i64>,
    now: i64,
) -> Result<bool, AppError> {
    let Some(event_id) = expected.owner.event_id else {
        return Ok(false);
    };
    let changed = transaction
        .execute(
            "UPDATE research_reviews
             SET state = 'completed', operation_stage = NULL,
                 notes_json = ?1, finished_at = ?2, updated_at = ?2
             WHERE review_id = ?3 AND campaign_id = ?4
               AND experiment_id = ?5 AND task_signature = ?6
               AND attempt = ?7 AND session_generation = ?8
               AND agent_run_id = ?9 AND context_digest = ?10
               AND event_id = ?11 AND state = 'ready'
               AND operation_stage IS NULL
               AND termination_request_id IS NULL",
            params![
                notes_json,
                now,
                expected.owner.review_id,
                expected.owner.campaign_id,
                expected.owner.source_experiment_id,
                expected.owner.managed_task_signature,
                expected.owner.attempt,
                expected.owner.session_generation,
                expected.owner.agent_run_id,
                expected.context_digest,
                event_id,
            ],
        )
        .map_err(database_error("complete continuing research review"))?;
    if changed != 1 {
        return Ok(false);
    }
    let campaign_changed = transaction
        .execute(
            "UPDATE campaign_research
             SET next_due_at = ?1, updated_at = ?2
             WHERE campaign_id = ?3 AND session_generation = ?4",
            params![
                next_due_at,
                now,
                expected.owner.campaign_id,
                expected.owner.session_generation,
            ],
        )
        .map_err(database_error("schedule continuing research campaign"))?;
    if campaign_changed != 1 {
        return Err(AppError::Runtime {
            operation: "schedule continuing research campaign",
        });
    }
    Ok(true)
}

pub(crate) fn mark_research_stop_requested_if_sent(
    db: &Db,
    request_id: i64,
    now: i64,
) -> Result<(), AppError> {
    let mut connection = db.connect()?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(database_error("begin research stop request transition"))?;
    transaction
        .execute(
            "UPDATE research_reviews
             SET operation_stage = 'stop_requested', updated_at = ?1
             WHERE termination_request_id = ?2 AND state = 'ready'
               AND operation_stage = 'intent'
               AND EXISTS (
                   SELECT 1 FROM termination_requests
                   WHERE request_id = ?2 AND status = 'sent'
               )",
            params![now, request_id],
        )
        .map_err(database_error("mark research stop request dispatched"))?;
    transaction
        .commit()
        .map_err(database_error("commit research stop request transition"))
}

fn research_termination_signature_matches(
    transaction: &Transaction<'_>,
    project_id: &str,
    source_task_id: Option<i64>,
    raw_signature: Option<&str>,
    managed_signature: &str,
) -> Result<bool, AppError> {
    let (Some(source_task_id), Some(raw_signature)) = (source_task_id, raw_signature) else {
        return Ok(false);
    };
    let expected_group: Option<String> = transaction
        .query_row(
            "SELECT pueue_group FROM projects WHERE project_id = ?1",
            [project_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(database_error("read research termination group"))?;
    let Some(expected_group) = expected_group else {
        return Ok(false);
    };
    let observation = transaction
        .query_row(
            "SELECT task_signature, pueue_task_id, pueue_group, command_json,
                    state, enqueued_at, started_at, ended_at, result,
                    observed_at
             FROM task_observations
             WHERE project_id = ?1 AND task_signature = ?2
               AND pueue_task_id = ?3",
            params![project_id, raw_signature, source_task_id],
            |row| {
                let command_json: String = row.get(3)?;
                let command = serde_json::from_str(&command_json).map_err(|source| {
                    rusqlite::Error::FromSqlConversionFailure(3, Type::Text, Box::new(source))
                })?;
                Ok(TaskObservation {
                    project_id: project_id.to_owned(),
                    task_signature: row.get(0)?,
                    pueue_task_id: row.get(1)?,
                    pueue_group: row.get(2)?,
                    command,
                    state: row.get(4)?,
                    enqueued_at: row.get(5)?,
                    started_at: row.get(6)?,
                    ended_at: row.get(7)?,
                    result: row.get(8)?,
                    observed_at: row.get(9)?,
                })
            },
        )
        .optional()
        .map_err(database_error("read research termination observation"))?;
    Ok(observation.is_some_and(|observation| {
        managed_task_run_signature_for_observation(&observation, &expected_group).as_deref()
            == Some(managed_signature)
    }))
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

pub(super) fn checkpoint_native_research_owner_complete(
    connection: &Connection,
    authority: &CheckpointDispatchAuthority,
) -> Result<bool, AppError> {
    let Some(row) = native_research_owner_rows(connection, Some(&authority.project_id))?
        .into_iter()
        .find(|row| {
            row.review_id == authority.review_id
                && row.campaign_id == authority.campaign_id
                && row.experiment_id == authority.source_experiment_id
                && row.agent_run_id == authority.checkpoint.review_agent_run_id
        })
    else {
        return Ok(false);
    };
    Ok(native_research_owner_is_complete(&row))
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

fn block_checkpoint_dispatch_review(
    transaction: &Transaction<'_>,
    review_id: &str,
    campaign_id: &str,
    source_experiment_id: &str,
    task_signature: &str,
    attempt: i64,
    now: i64,
) -> Result<bool, AppError> {
    let changed = transaction
        .execute(
            "UPDATE research_reviews
             SET state = 'blocked', failure_code = 'research_checkpoint_authority_corrupt',
                 finished_at = ?1, not_before = ?1, updated_at = ?1
             WHERE review_id = ?2 AND campaign_id = ?3 AND experiment_id = ?4
               AND task_signature = ?5 AND attempt = ?6
               AND state = 'ready' AND successor_experiment_id IS NOT NULL",
            params![now, review_id, campaign_id, source_experiment_id, task_signature, attempt],
        )
        .map_err(database_error("block checkpoint dispatch review"))?;
    Ok(changed == 1)
}

fn block_missing_checkpoint_dispatch_review(
    transaction: &Transaction<'_>,
    review_id: &str,
    campaign_id: &str,
    source_experiment_id: &str,
    task_signature: &str,
    attempt: i64,
    successor_experiment_id: &str,
    now: i64,
) -> Result<bool, AppError> {
    let changed = transaction
        .execute(
            "UPDATE research_reviews
             SET state = 'blocked', failure_code = 'research_checkpoint_authority_corrupt',
                 finished_at = ?1, not_before = ?1, updated_at = ?1
             WHERE review_id = ?2 AND campaign_id = ?3 AND experiment_id = ?4
               AND task_signature = ?5 AND attempt = ?6
               AND state = 'ready' AND operation_stage = 'successor_reserved'
               AND successor_experiment_id = ?7 AND checkpoint_json IS NULL",
            params![
                now,
                review_id,
                campaign_id,
                source_experiment_id,
                task_signature,
                attempt,
                successor_experiment_id,
            ],
        )
        .map_err(database_error("block missing checkpoint dispatch review"))?;
    Ok(changed == 1)
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
    let (checkpoint_json, checkpoint_json_state) = checkpoint_json_from_row(row)?;
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
        checkpoint_json,
        checkpoint_json_state,
        session_generation: row.get(14)?,
    })
}

fn checkpoint_sqlite_storage_class(
    value: &str,
    column: usize,
) -> rusqlite::Result<CheckpointSqliteStorageClass> {
    match value {
        "null" => Ok(CheckpointSqliteStorageClass::Null),
        "integer" => Ok(CheckpointSqliteStorageClass::Integer),
        "real" => Ok(CheckpointSqliteStorageClass::Real),
        "text" => Ok(CheckpointSqliteStorageClass::Text),
        "blob" => Ok(CheckpointSqliteStorageClass::Blob),
        _ => Err(rusqlite::Error::FromSqlConversionFailure(
            column,
            Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unknown SQLite storage class",
            )),
        )),
    }
}

fn checkpoint_json_from_row(
    row: &Row<'_>,
) -> rusqlite::Result<(Option<String>, CheckpointJsonState)> {
    let storage_class = checkpoint_sqlite_storage_class(&row.get::<_, String>(25)?, 25)?;
    let byte_len = row.get::<_, Option<i64>>(26)?;
    if storage_class == CheckpointSqliteStorageClass::Null {
        return Ok((None, CheckpointJsonState::Missing));
    }
    if storage_class != CheckpointSqliteStorageClass::Text {
        return Ok((
            None,
            CheckpointJsonState::Invalid {
                storage_class,
                byte_len,
            },
        ));
    }
    let Some(byte_len) = byte_len else {
        return Ok((
            None,
            CheckpointJsonState::Invalid {
                storage_class,
                byte_len: None,
            },
        ));
    };
    let Ok(byte_len_usize) = usize::try_from(byte_len) else {
        return Ok((
            None,
            CheckpointJsonState::Invalid {
                storage_class,
                byte_len: Some(byte_len),
            },
        ));
    };
    if !(1..=MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES).contains(&byte_len_usize) {
        return Ok((
            None,
            CheckpointJsonState::Invalid {
                storage_class,
                byte_len: Some(byte_len),
            },
        ));
    }
    let value = match row.get_ref(20)? {
        ValueRef::Text(bytes) if bytes.len() == byte_len_usize => {
            String::from_utf8(bytes.to_vec()).ok()
        }
        _ => None,
    };
    match value {
        Some(value) => Ok((Some(value), CheckpointJsonState::BoundedText)),
        None => Ok((
            None,
            CheckpointJsonState::Invalid {
                storage_class,
                byte_len: Some(byte_len),
            },
        )),
    }
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
            NewTaskObservation, ProposalKind, IncidentStatus, TerminationRequestStatus,
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


    fn available_checkpoint_context_response(
        project_id: &str,
        campaign_id: &str,
        experiment_id: &str,
        review_id: &str,
        task_signature: &str,
        objective_digest: &str,
        proposal_id: &str,
        submission_id: &str,
        action: &str,
    ) -> (String, String, String) {
        let root = crate::environment::ResearchDirectoryRecord {
            device: 1,
            inode: 2,
            owner: 3,
            mode: 0o700,
            mount_identity: [4, 5],
        };
        let source = "x = 1\n";
        let source_digest = format!("{:x}", Sha256::digest(source.as_bytes()));
        let source_file = crate::environment::ResearchFileRecord {
            relative_path: "train.py".to_owned(),
            root: root.clone(),
            parent: root.clone(),
            device: 6,
            inode: 7,
            owner: 3,
            mode: 0o600,
            mount_identity: [4, 5],
            logical_bytes: source.len() as u64,
            allocated_bytes: 512,
            sha256: source_digest.clone(),
        };
        let candidate = b"checkpoint";
        let candidate_digest = format!("{:x}", Sha256::digest(candidate));
        let candidate_path = format!(
            ".pueue-agent/artifacts/{experiment_id}/checkpoint.json"
        );
        let candidate_file = crate::environment::ResearchFileRecord {
            relative_path: candidate_path.clone(),
            root: root.clone(),
            parent: root,
            device: 8,
            inode: 9,
            owner: 3,
            mode: 0o600,
            mount_identity: [4, 5],
            logical_bytes: candidate.len() as u64,
            allocated_bytes: 512,
            sha256: candidate_digest.clone(),
        };
        let loader_reference = format!("loader-source:{source_digest}");
        let candidate_reference = format!("checkpoint:{experiment_id}:0:{candidate_digest}");
        let support = CheckpointSupportEvidenceV1::Available {
            support_version: crate::research_checkpoint::CHECKPOINT_SUPPORT_VERSION,
            source_experiment_id: experiment_id.to_owned(),
            source_proposal_id: proposal_id.to_owned(),
            source_submission_id: submission_id.to_owned(),
            normalized_working_directory: ".".to_owned(),
            working_directory_record: source_file.root.clone(),
            loader_support: vec![crate::research_checkpoint::CheckpointLoaderEvidenceV1 {
                reference: loader_reference.clone(),
                role: crate::research_checkpoint::CheckpointLoaderRole::Entrypoint,
                argv_index: 1,
                argv_token: "train.py".to_owned(),
                root_relative_path: "train.py".to_owned(),
                length: source.len() as u64,
                sha256: source_digest,
                file: source_file,
                content: source.to_owned(),
            }],
            checkpoint_candidates: vec![
                crate::research_checkpoint::CheckpointCandidateEvidenceV1 {
                    reference: candidate_reference.clone(),
                    source_experiment_id: experiment_id.to_owned(),
                    argv_path: candidate_path.clone(),
                    root_relative_path: candidate_path.clone(),
                    length: candidate.len() as u64,
                    sha256: candidate_digest,
                    file: candidate_file,
                },
            ],
            candidates_complete: true,
            candidates_omitted_at_least: 0,
            candidate_limit: crate::research_checkpoint::MAX_CHECKPOINT_CANDIDATES,
        };
        let context = json!({
            "schema_version": crate::research_evidence::RESEARCH_CONTEXT_SCHEMA_VERSION,
            "facts": {
                "review": {
                    "review_id": review_id,
                    "experiment_id": experiment_id,
                    "task_signature": task_signature,
                },
                "campaign": {"campaign_id": campaign_id},
                "project": {"project_id": project_id},
                "objective": {"digest": objective_digest},
                "target": {
                    "experiment_id": experiment_id,
                    "pueue_task_id": 41,
                    "task_signature": task_signature,
                    "proposal_id": proposal_id,
                    "submission_id": submission_id,
                }
            },
            "operations": {"checkpoint_support": support}
        });
        let context_json = context.to_string();
        let context_digest = format!("{:x}", Sha256::digest(context_json.as_bytes()));
        let checkpoint = if action == "resume_from_checkpoint" {
            Some(json!({
                "path": candidate_path,
                "argv": ["python", "train.py", "--resume", candidate_path],
                "working_directory": ".",
                "support_evidence_refs": [loader_reference.clone(), candidate_reference.clone()]
            }))
        } else {
            None
        };
        let mut response = json!({
            "schema_version": 1,
            "review_id": review_id,
            "experiment_id": experiment_id,
            "context_digest": context_digest,
            "action": action,
            "reason": "use the verified checkpoint evidence",
            "evidence_refs": [loader_reference, candidate_reference],
            "notes": "verified support",
            "checkpoint": checkpoint,
        });
        if action == "stop_and_next" {
            response["next_direction"] = json!("continue the bounded experiment");
        }
        let response_json = response.to_string();
        (context_json, context_digest, response_json)
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

    #[test]
    fn completed_handoff_projection_requires_exact_attached_cycle() {
        let (_temp, db, _run_id, _project_id) = detached_history_fixture();
        let mut connection = db.connect().expect("handoff projection connection");
        let transaction = connection
            .transaction()
            .expect("handoff projection transaction");
        let projection = completed_research_handoff_in_transaction(
            &transaction,
            "detached-history-project",
            "detached-history-campaign",
            "detached-history-experiment",
            "research-terminal-cycle:detached-history-campaign:detached-history-experiment",
        )
        .expect("handoff projection query");
        assert!(projection.is_none());
    }

    #[test]
    fn completed_handoff_projection_rejects_linked_open_owner() {
        let (_temp, db, _run_id, _project_id) = detached_history_fixture();
        let connection = db.connect().expect("open handoff projection connection");
        let review_id: String = connection
            .query_row(
                "SELECT review_id FROM research_reviews
                 WHERE campaign_id = 'detached-history-campaign'",
                [],
                |row| row.get(0),
            )
            .expect("open handoff review");
        connection
            .execute(
                "UPDATE research_reviews
                 SET operation_stage = 'intent'
                 WHERE review_id = ?1",
                [&review_id],
            )
            .expect("open handoff owner");
        drop(connection);

        let mut connection = db.connect().expect("open handoff transaction connection");
        let transaction = connection
            .transaction()
            .expect("open handoff transaction");
        let result = completed_research_handoff_in_transaction(
            &transaction,
            "detached-history-project",
            "detached-history-campaign",
            "detached-history-experiment",
            "research-terminal-cycle:detached-history-campaign:detached-history-experiment",
        );
        assert!(matches!(
            result,
            Err(AppError::Validation {
                field: "research.handoff",
                ..
            })
        ));
    }

    #[test]
    fn research_context_identity_requires_current_schema_and_target_binding() {
        let context = json!({
            "schema_version": 1,
            "facts": {
                "review": {
                    "review_id": "review",
                    "experiment_id": "experiment",
                    "task_signature": "pueue-managed-run:v1:managed"
                },
                "campaign": {"campaign_id": "campaign"},
                "project": {"project_id": "project"},
                "objective": {"digest": "objective"},
                "target": {
                    "experiment_id": "experiment",
                    "pueue_task_id": 41,
                    "task_signature": "pueue-managed-run:v1:managed"
                }
            }
        });
        assert!(research_context_identity_matches(
            &context,
            "project",
            "campaign",
            "review",
            "experiment",
            "pueue-managed-run:v1:managed",
            Some(41),
            "objective",
        ));

        let mut wrong_schema = context.clone();
        wrong_schema["schema_version"] = json!(2);
        assert!(!research_context_identity_matches(
            &wrong_schema,
            "project",
            "campaign",
            "review",
            "experiment",
            "pueue-managed-run:v1:managed",
            Some(41),
            "objective",
        ));

        let mut wrong_review_signature = context.clone();
        wrong_review_signature["facts"]["review"]["task_signature"] =
            json!("pueue-managed-run:v1:other");
        assert!(!research_context_identity_matches(
            &wrong_review_signature,
            "project",
            "campaign",
            "review",
            "experiment",
            "pueue-managed-run:v1:managed",
            Some(41),
            "objective",
        ));

        let mut wrong_target = context.clone();
        wrong_target["facts"]["target"]["pueue_task_id"] = json!(42);
        assert!(!research_context_identity_matches(
            &wrong_target,
            "project",
            "campaign",
            "review",
            "experiment",
            "pueue-managed-run:v1:managed",
            Some(41),
            "objective",
        ));
    }

    #[test]
    fn bounded_checkpoint_row_projection_classifies_sql_storage() {
        let connection = Connection::open_in_memory().unwrap();
        let projection = |value: &dyn rusqlite::ToSql| {
            connection
                .query_row(
                    "SELECT 'review', 'campaign', 'experiment', 'task', 0,
                            'pending', NULL, NULL, NULL, NULL, NULL, NULL,
                            NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL,
                            CASE WHEN typeof(?1) = 'text'
                                      AND length(CAST(?1 AS BLOB)) BETWEEN 1 AND 131072
                                 THEN ?1 END,
                            0, NULL, NULL, 0, typeof(?1),
                            length(CAST(?1 AS BLOB))",
                    [value],
                    review_from_row,
                )
                .unwrap()
        };

        let missing = projection(&rusqlite::types::Null);
        assert_eq!(missing.checkpoint_json, None);
        assert_eq!(missing.checkpoint_json_state, CheckpointJsonState::Missing);

        let blob = projection(&vec![0xff_u8]);
        assert_eq!(blob.checkpoint_json, None);
        assert!(matches!(
            blob.checkpoint_json_state,
            CheckpointJsonState::Invalid {
                storage_class: CheckpointSqliteStorageClass::Blob,
                byte_len: Some(1),
            }
        ));

        let empty = projection(&String::new());
        assert_eq!(empty.checkpoint_json, None);
        assert_eq!(
            empty.checkpoint_json_state,
            CheckpointJsonState::Invalid {
                storage_class: CheckpointSqliteStorageClass::Text,
                byte_len: Some(0),
            }
        );

        let bounded = "é{}".to_owned();
        let bounded_row = projection(&bounded);
        assert_eq!(bounded_row.checkpoint_json.as_deref(), Some(bounded.as_str()));
        assert_eq!(
            bounded_row.checkpoint_json_state,
            CheckpointJsonState::BoundedText
        );

        let exact = "x".repeat(MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES);
        let exact_row = projection(&exact);
        assert_eq!(exact_row.checkpoint_json.as_deref(), Some(exact.as_str()));
        assert_eq!(
            exact_row.checkpoint_json_state,
            CheckpointJsonState::BoundedText
        );

        let oversized = "x".repeat(MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES + 1);
        let oversized_row = projection(&oversized);
        assert_eq!(oversized_row.checkpoint_json, None);
        assert_eq!(
            oversized_row.checkpoint_json_state,
            CheckpointJsonState::Invalid {
                storage_class: CheckpointSqliteStorageClass::Text,
                byte_len: Some((MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES + 1) as i64),
            }
        );

        let integer = projection(&7_i64);
        assert_eq!(integer.checkpoint_json, None);
        assert!(matches!(
            integer.checkpoint_json_state,
            CheckpointJsonState::Invalid {
                storage_class: CheckpointSqliteStorageClass::Integer,
                byte_len: Some(_),
            }
        ));

        let real = projection(&1.5_f64);
        assert_eq!(real.checkpoint_json, None);
        assert!(matches!(
            real.checkpoint_json_state,
            CheckpointJsonState::Invalid {
                storage_class: CheckpointSqliteStorageClass::Real,
                byte_len: Some(_),
            }
        ));

        assert_eq!(
            MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES,
            crate::research_checkpoint::MAX_PREPARED_CHECKPOINT_BYTES
        );
        assert!(REVIEW_SELECT.contains("BETWEEN 1 AND 131072"));
        assert!(LAUNCH_REVIEW_SELECT.contains("BETWEEN 1 AND 131072"));
    }

    #[test]
    fn evidence_binding_keeps_legacy_refs_and_rejects_unrelated_reference_keys() {
        let context = json!({
            "legacy": {"evidence_ref": "legacy:1"},
            "nested": {"reference": "unrelated:1"},
            "operations": {"checkpoint_support": {"status": "malformed"}}
        });
        let context_json = serde_json::to_string(&context).unwrap();
        let context_digest = format!("{:x}", Sha256::digest(context_json.as_bytes()));
        let mut answer = ResearchAnswer {
            schema_version: 1,
            review_id: "review".to_owned(),
            experiment_id: "experiment".to_owned(),
            context_digest: context_digest.clone(),
            action: "continue".to_owned(),
            reason: "continue".to_owned(),
            evidence_refs: vec!["legacy:1".to_owned()],
            notes: "notes".to_owned(),
            next_direction: None,
            checkpoint: None,
        };
        assert!(research_answer_evidence_refs_are_bound(
            &context,
            &context_json,
            &context_digest,
            &answer,
        )
        .unwrap());

        answer.evidence_refs = vec!["unrelated:1".to_owned()];
        assert!(research_answer_evidence_refs_are_bound(
            &context,
            &context_json,
            &context_digest,
            &answer,
        )
        .is_err());
    }

    #[test]
    fn evidence_binding_rejects_unavailable_support_for_new_reference() {
        let context = json!({
            "schema_version": crate::research_evidence::RESEARCH_CONTEXT_SCHEMA_VERSION,
            "operations": {
                "checkpoint_support": {
                    "status": "unavailable",
                    "support_version": crate::research_checkpoint::CHECKPOINT_SUPPORT_VERSION,
                    "reason": "discovery unavailable",
                    "loader_support": [],
                    "checkpoint_candidates": [],
                    "candidates_complete": false,
                    "candidates_omitted_at_least": 0,
                    "candidate_limit": crate::research_checkpoint::MAX_CHECKPOINT_CANDIDATES
                }
            }
        });
        let context_json = serde_json::to_string(&context).unwrap();
        let context_digest = format!("{:x}", Sha256::digest(context_json.as_bytes()));
        let answer = ResearchAnswer {
            schema_version: 1,
            review_id: "review".to_owned(),
            experiment_id: "experiment".to_owned(),
            context_digest: context_digest.clone(),
            action: "continue".to_owned(),
            reason: "continue".to_owned(),
            evidence_refs: vec!["loader-source:unknown".to_owned()],
            notes: "notes".to_owned(),
            next_direction: None,
            checkpoint: None,
        };
        assert!(!research_answer_evidence_refs_are_bound(
            &context,
            &context_json,
            &context_digest,
            &answer,
        )
        .unwrap());
    }

    #[test]
    fn evidence_binding_requires_exact_available_loader_and_candidate_selection() {
        let root = crate::environment::ResearchDirectoryRecord {
            device: 1,
            inode: 2,
            owner: 3,
            mode: 0o700,
            mount_identity: [4, 5],
        };
        let source = "x = 1\n";
        let source_digest = format!("{:x}", Sha256::digest(source.as_bytes()));
        let source_file = crate::environment::ResearchFileRecord {
            relative_path: "train.py".to_owned(),
            root: root.clone(),
            parent: root.clone(),
            device: 6,
            inode: 7,
            owner: 3,
            mode: 0o600,
            mount_identity: [4, 5],
            logical_bytes: source.len() as u64,
            allocated_bytes: 512,
            sha256: source_digest.clone(),
        };
        let candidate = b"checkpoint";
        let candidate_digest = format!("{:x}", Sha256::digest(candidate));
        let candidate_path = ".pueue-agent/artifacts/experiment/checkpoint.json";
        let candidate_file = crate::environment::ResearchFileRecord {
            relative_path: candidate_path.to_owned(),
            root: root.clone(),
            parent: root,
            device: 8,
            inode: 9,
            owner: 3,
            mode: 0o600,
            mount_identity: [4, 5],
            logical_bytes: candidate.len() as u64,
            allocated_bytes: 512,
            sha256: candidate_digest.clone(),
        };
        let loader_reference = format!("loader-source:{source_digest}");
        let candidate_reference = format!("checkpoint:experiment:0:{candidate_digest}");
        let support = CheckpointSupportEvidenceV1::Available {
            support_version: crate::research_checkpoint::CHECKPOINT_SUPPORT_VERSION,
            source_experiment_id: "experiment".to_owned(),
            source_proposal_id: "proposal".to_owned(),
            source_submission_id: "submission".to_owned(),
            normalized_working_directory: ".".to_owned(),
            working_directory_record: source_file.root.clone(),
            loader_support: vec![crate::research_checkpoint::CheckpointLoaderEvidenceV1 {
                reference: loader_reference.clone(),
                role: crate::research_checkpoint::CheckpointLoaderRole::Entrypoint,
                argv_index: 1,
                argv_token: "train.py".to_owned(),
                root_relative_path: "train.py".to_owned(),
                length: source.len() as u64,
                sha256: source_digest,
                file: source_file,
                content: source.to_owned(),
            }],
            checkpoint_candidates: vec![
                crate::research_checkpoint::CheckpointCandidateEvidenceV1 {
                    reference: candidate_reference.clone(),
                    source_experiment_id: "experiment".to_owned(),
                    argv_path: candidate_path.to_owned(),
                    root_relative_path: candidate_path.to_owned(),
                    length: candidate.len() as u64,
                    sha256: candidate_digest,
                    file: candidate_file,
                },
            ],
            candidates_complete: true,
            candidates_omitted_at_least: 0,
            candidate_limit: crate::research_checkpoint::MAX_CHECKPOINT_CANDIDATES,
        };
        let context = json!({
            "schema_version": crate::research_evidence::RESEARCH_CONTEXT_SCHEMA_VERSION,
            "facts": {
                "review": {"experiment_id": "experiment"},
                "target": {
                    "experiment_id": "experiment",
                    "proposal_id": "proposal",
                    "submission_id": "submission"
                }
            },
            "operations": {"checkpoint_support": support}
        });
        let context_json = serde_json::to_string(&context).unwrap();
        let context_digest = format!("{:x}", Sha256::digest(context_json.as_bytes()));
        let request = crate::research_protocol::CheckpointRequest {
            path: candidate_path.to_owned(),
            argv: vec![
                "python".to_owned(),
                "train.py".to_owned(),
                "--resume".to_owned(),
                candidate_path.to_owned(),
            ],
            working_directory: ".".to_owned(),
            support_evidence_refs: vec![loader_reference.clone(), candidate_reference.clone()],
        };
        let answer = ResearchAnswer {
            schema_version: 1,
            review_id: "review".to_owned(),
            experiment_id: "experiment".to_owned(),
            context_digest: context_digest.clone(),
            action: "resume_from_checkpoint".to_owned(),
            reason: "resume".to_owned(),
            evidence_refs: vec![loader_reference, candidate_reference],
            notes: "notes".to_owned(),
            next_direction: None,
            checkpoint: Some(request),
        };
        assert!(research_answer_evidence_refs_are_bound(
            &context,
            &context_json,
            &context_digest,
            &answer,
        )
        .unwrap());

        let mut wrong_path = answer;
        wrong_path.checkpoint.as_mut().unwrap().path = "other.json".to_owned();
        assert!(research_answer_evidence_refs_are_bound(
            &context,
            &context_json,
            &context_digest,
            &wrong_path,
        )
        .is_err());
    }

    #[test]
    fn evidence_binding_available_citation_matrix_is_strict_for_callers() {
        let (context_json, context_digest, continue_response) =
            available_checkpoint_context_response(
                "project",
                "campaign",
                "experiment",
                "review",
                "pueue-managed-run:v1:managed",
                "objective",
                "proposal",
                "submission",
                "continue",
            );
        let context: Value = serde_json::from_str(&context_json).unwrap();
        let continue_answer = parse_research_answer(continue_response.as_bytes()).unwrap();
        assert!(research_answer_evidence_refs_are_bound(
            &context,
            &context_json,
            &context_digest,
            &continue_answer,
        )
        .unwrap());

        let (stop_context_json, stop_context_digest, stop_response) =
            available_checkpoint_context_response(
                "project",
                "campaign",
                "experiment",
                "review",
                "pueue-managed-run:v1:managed",
                "objective",
                "proposal",
                "submission",
                "stop_and_next",
            );
        let stop_context: Value = serde_json::from_str(&stop_context_json).unwrap();
        let stop_answer = parse_research_answer(stop_response.as_bytes()).unwrap();
        assert!(research_answer_evidence_refs_are_bound(
            &stop_context,
            &stop_context_json,
            &stop_context_digest,
            &stop_answer,
        )
        .unwrap());

        let (_, _, resume_response) = available_checkpoint_context_response(
            "project",
            "campaign",
            "experiment",
            "review",
            "pueue-managed-run:v1:managed",
            "objective",
            "proposal",
            "submission",
            "resume_from_checkpoint",
        );
        let _resume_answer = parse_research_answer(resume_response.as_bytes()).unwrap();

        let wrong_context_digest = "0".repeat(64);
        let mut wrong_digest_answer = continue_answer;
        wrong_digest_answer.context_digest = wrong_context_digest.clone();
        assert!(research_answer_evidence_refs_are_bound(
            &context,
            &context_json,
            &wrong_context_digest,
            &wrong_digest_answer,
        )
        .is_err());

        let mut foreign_answer = parse_research_answer(continue_response.as_bytes()).unwrap();
        foreign_answer.evidence_refs = vec!["foreign:reference".to_owned()];
        assert!(!research_answer_evidence_refs_are_bound(
            &context,
            &context_json,
            &context_digest,
            &foreign_answer,
        )
        .unwrap());

        let mut duplicate_answer = parse_research_answer(resume_response.as_bytes()).unwrap();
        let duplicate_loader = duplicate_answer
            .checkpoint
            .as_ref()
            .unwrap()
            .support_evidence_refs[0]
            .clone();
        duplicate_answer
            .checkpoint
            .as_mut()
            .unwrap()
            .support_evidence_refs[1] = duplicate_loader;
        assert!(research_answer_evidence_refs_are_bound(
            &context,
            &context_json,
            &context_digest,
            &duplicate_answer,
        )
        .is_err());

        let mut pruned_context = context.clone();
        let candidates = pruned_context["operations"]["checkpoint_support"]
            ["checkpoint_candidates"]
            .as_array_mut()
            .unwrap();
        let candidate = &mut candidates[0];
        let alternate_path = ".pueue-agent/artifacts/experiment/other.json";
        candidate["argv_path"] = json!(alternate_path);
        candidate["root_relative_path"] = json!(alternate_path);
        candidate["file"]["relative_path"] = json!(alternate_path);
        let alternate_digest = "0".repeat(64);
        candidate["sha256"] = json!(&alternate_digest);
        candidate["file"]["sha256"] = json!(&alternate_digest);
        candidate["reference"] = json!(format!("checkpoint:experiment:0:{alternate_digest}"));
        pruned_context["operations"]["checkpoint_support"]["candidates_complete"] =
            json!(false);
        pruned_context["operations"]["checkpoint_support"]["candidates_omitted_at_least"] =
            json!(1);
        let pruned_context_json = pruned_context.to_string();
        let pruned_context_digest =
            format!("{:x}", Sha256::digest(pruned_context_json.as_bytes()));
        let mut pruned_answer = parse_research_answer(resume_response.as_bytes()).unwrap();
        pruned_answer.context_digest = pruned_context_digest.clone();
        pruned_answer.checkpoint.as_mut().unwrap().path = alternate_path.to_owned();
        assert!(!research_answer_evidence_refs_are_bound(
            &pruned_context,
            &pruned_context_json,
            &pruned_context_digest,
            &pruned_answer,
        )
        .unwrap());

        let mut wrong_path_answer = parse_research_answer(resume_response.as_bytes()).unwrap();
        wrong_path_answer.checkpoint.as_mut().unwrap().path = "other.json".to_owned();
        assert!(research_answer_evidence_refs_are_bound(
            &context,
            &context_json,
            &context_digest,
            &wrong_path_answer,
        )
        .is_err());

        let mut unrelated_context = context.clone();
        unrelated_context["nested"] = json!({"reference": "unrelated:reference"});
        let unrelated_context_json = unrelated_context.to_string();
        let unrelated_context_digest =
            format!("{:x}", Sha256::digest(unrelated_context_json.as_bytes()));
        let mut unrelated_answer = parse_research_answer(continue_response.as_bytes()).unwrap();
        unrelated_answer.context_digest = unrelated_context_digest.clone();
        unrelated_answer.evidence_refs = vec!["unrelated:reference".to_owned()];
        assert!(!research_answer_evidence_refs_are_bound(
            &unrelated_context,
            &unrelated_context_json,
            &unrelated_context_digest,
            &unrelated_answer,
        )
        .unwrap());
    }

    #[test]
    fn all_research_review_routes_preserve_bounded_checkpoint_classification() {
        let (_temp, db, _run_id, _project_id) = detached_history_fixture();
        let review_id: String = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT review_id FROM research_reviews
                 WHERE campaign_id = 'detached-history-campaign'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let repository = ResearchRepository::new(&db);

        let assert_routes = |value: &dyn rusqlite::ToSql,
                             expected_state: CheckpointJsonState,
                             expected_json: Option<&str>,
                             expected_storage_class: &str,
                             expected_byte_len: Option<i64>,
                             expected_bytes: Option<&[u8]>| {
            let connection = db.connect().unwrap();
            connection
                .execute(
                    "UPDATE research_reviews
                     SET state = 'retry_wait', operation_stage = NULL,
                         agent_run_id = NULL, not_before = 3001,
                         checkpoint_json = ?1
                     WHERE review_id = ?2",
                    rusqlite::params![value, review_id.as_str()],
                )
                .unwrap();
            let persisted: (String, Option<i64>, Option<Vec<u8>>) = connection
                .query_row(
                    "SELECT typeof(checkpoint_json),
                            length(CAST(checkpoint_json AS BLOB)),
                            CAST(checkpoint_json AS BLOB)
                     FROM research_reviews WHERE review_id = ?1",
                    [&review_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(persisted.0, expected_storage_class);
            assert_eq!(persisted.1, expected_byte_len);
            assert_eq!(persisted.2.as_deref(), expected_bytes);
            drop(connection);

            let assert_row = |row: &ResearchReview| {
                assert_eq!(row.checkpoint_json.as_deref(), expected_json);
                assert_eq!(row.checkpoint_json_state, expected_state);
            };
            let assert_persisted = || {
                let connection = db.connect().unwrap();
                let persisted: (String, Option<i64>, Option<Vec<u8>>) = connection
                    .query_row(
                        "SELECT typeof(checkpoint_json),
                                length(CAST(checkpoint_json AS BLOB)),
                                CAST(checkpoint_json AS BLOB)
                         FROM research_reviews WHERE review_id = ?1",
                        [&review_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .unwrap();
                assert_eq!(persisted.0, expected_storage_class);
                assert_eq!(persisted.1, expected_byte_len);
                assert_eq!(persisted.2.as_deref(), expected_bytes);
            };
            let due = repository.due_reviews(3_002, 10).unwrap();
            assert_persisted();
            assert_eq!(due.len(), 1);
            assert_row(&due[0]);
            let launch = repository.due_launch_reviews(3_002, 10, 10).unwrap();
            assert_persisted();
            assert_eq!(launch.len(), 1);
            assert_row(&launch[0]);
            let found = repository.find(&review_id).unwrap();
            assert_persisted();
            assert_row(&found);
            let recent = repository
                .recent("detached-history-campaign", 10)
                .unwrap();
            assert_persisted();
            assert_eq!(recent.len(), 1);
            assert_row(&recent[0]);

            let connection = db.connect().unwrap();
            connection
                .execute(
                    "UPDATE research_reviews
                     SET state = 'ready', operation_stage = NULL
                     WHERE review_id = ?1",
                    [&review_id],
                )
                .unwrap();
            drop(connection);
            let ready = repository.ready_reviews(10).unwrap();
            assert_persisted();
            assert_eq!(ready.len(), 1);
            assert_row(&ready[0]);

            let connection = db.connect().unwrap();
            connection
                .execute(
                    "UPDATE research_reviews
                     SET operation_stage = 'intent'
                     WHERE review_id = ?1",
                    [&review_id],
                )
                .unwrap();
            drop(connection);
            let open = repository.open_action_reviews(10).unwrap();
            assert_persisted();
            assert_eq!(open.len(), 1);
            assert_row(&open[0]);
        };

        let missing = rusqlite::types::Null;
        assert_routes(&missing, CheckpointJsonState::Missing, None, "null", None, None);
        let blob = vec![0xff_u8];
        assert_routes(
            &blob,
            CheckpointJsonState::Invalid {
                storage_class: CheckpointSqliteStorageClass::Blob,
                byte_len: Some(1),
            },
            None,
            "blob",
            Some(1),
            Some(blob.as_slice()),
        );
        let empty = String::new();
        assert_routes(
            &empty,
            CheckpointJsonState::Invalid {
                storage_class: CheckpointSqliteStorageClass::Text,
                byte_len: Some(0),
            },
            None,
            "text",
            Some(0),
            Some(&[]),
        );
        let exact = "x".repeat(MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES);
        assert_routes(
            &exact,
            CheckpointJsonState::BoundedText,
            Some(&exact),
            "text",
            Some(MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES as i64),
            Some(exact.as_bytes()),
        );
        let oversized = "x".repeat(MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES + 1);
        assert_routes(
            &oversized,
            CheckpointJsonState::Invalid {
                storage_class: CheckpointSqliteStorageClass::Text,
                byte_len: Some((MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES + 1) as i64),
            },
            None,
            "text",
            Some((MAX_RESEARCH_CHECKPOINT_COLUMN_BYTES + 1) as i64),
            Some(oversized.as_bytes()),
        );
    }

    #[test]
    fn ready_action_accepts_available_support_then_rejects_non_null_checkpoint_column() {
        let (_temp, db, run_id, project_id) = detached_history_fixture();
        let review_id: String = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT review_id FROM research_reviews
                 WHERE campaign_id = 'detached-history-campaign'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let managed_task_signature: String = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT task_signature FROM research_reviews WHERE review_id = ?1",
                [&review_id],
                |row| row.get(0),
            )
            .unwrap();
        let (context_json, context_digest, response_json) =
            available_checkpoint_context_response(
                &project_id,
                "detached-history-campaign",
                "detached-history-experiment",
                &review_id,
                &managed_task_signature,
                "detached-history-objective",
                "detached-history-proposal",
                "detached-history-submission",
                "resume_from_checkpoint",
            );
        let authority = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT json_extract(notes_json, '$.retry_history[0].native_recovery')
                 FROM research_reviews WHERE review_id = ?1",
                [&review_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        let session_id = "11111111-1111-4111-8111-111111111111";
        let notes_json = json!({
            "native_recovery": serde_json::from_str::<Value>(&authority).unwrap(),
            "planned_session_id": session_id,
            "confirmed_session_id": session_id,
            "session_binding": "confirmed",
        })
        .to_string();
        let connection = db.connect().unwrap();
        connection
            .execute(
                "UPDATE campaign_research
                 SET session_id = ?1, session_generation = 0
                 WHERE campaign_id = 'detached-history-campaign'",
                [session_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE events
                 SET status = 'completed', lease_until = NULL,
                     completed_at = 3_002
                 WHERE event_id = (SELECT event_id FROM research_reviews WHERE review_id = ?1)",
                [&review_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE research_reviews
                 SET state = 'ready', attempt = 1, operation_stage = NULL,
                     agent_run_id = ?1, context_json = ?2, context_digest = ?3,
                     response_json = ?4, termination_request_id = NULL,
                     failure_code = NULL, notes_json = ?5, checkpoint_json = NULL,
                     not_before = 3_003, finished_at = NULL, updated_at = 3_003
                 WHERE review_id = ?6",
                rusqlite::params![
                    run_id,
                    context_json,
                    context_digest,
                    response_json,
                    notes_json,
                    review_id,
                ],
            )
            .unwrap();
        drop(connection);

        let live_task = PueueTask {
            id: 41,
            group: "detached-history-group".to_owned(),
            command: "python train.py".to_owned(),
            state: "Running".to_owned(),
            enqueued_at: Some("900".to_owned()),
            started_at: Some("1000".to_owned()),
            ended_at: None,
            result: None,
        };
        let mut connection = db.connect().unwrap();
        let transaction = connection.transaction().unwrap();
        let read_mutation_snapshot = |transaction: &Transaction<'_>| {
            transaction
                .query_row(
                    "SELECT
                         (SELECT COUNT(*) FROM incidents WHERE project_id = ?1),
                         (SELECT COUNT(*) FROM termination_requests WHERE project_id = ?1),
                         (SELECT COUNT(*) FROM proposals WHERE campaign_id = 'detached-history-campaign'),
                         (SELECT COUNT(*) FROM submissions WHERE project_id = ?1),
                         (SELECT COUNT(*) FROM experiments WHERE campaign_id = 'detached-history-campaign'),
                         (SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = 'detached-history-campaign'),
                         state, operation_stage, agent_run_id, termination_request_id,
                         successor_experiment_id, checkpoint_json, context_digest,
                         response_json, notes_json, failure_code, attempt
                     FROM research_reviews WHERE review_id = ?2",
                    rusqlite::params![project_id, review_id],
                    |row| {
                        Ok((
                            (
                                row.get::<_, i64>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, i64>(2)?,
                                row.get::<_, i64>(3)?,
                                row.get::<_, i64>(4)?,
                                row.get::<_, i64>(5)?,
                            ),
                            (
                                row.get::<_, String>(6)?,
                                row.get::<_, Option<String>>(7)?,
                                row.get::<_, Option<i64>>(8)?,
                                row.get::<_, Option<i64>>(9)?,
                                row.get::<_, Option<String>>(10)?,
                                row.get::<_, Option<String>>(11)?,
                                row.get::<_, Option<String>>(12)?,
                                row.get::<_, Option<String>>(13)?,
                                row.get::<_, Option<String>>(14)?,
                                row.get::<_, Option<String>>(15)?,
                                row.get::<_, i64>(16)?,
                            ),
                        ))
                    },
                )
                .unwrap()
        };
        let before = read_mutation_snapshot(&transaction);
        let first = ready_research_action_in_transaction(
            &transaction,
            &project_id,
            &review_id,
            &live_task,
        )
        .unwrap();
        assert!(first.is_some(), "verified Available support must be consumable");
        assert_eq!(read_mutation_snapshot(&transaction), before);
        transaction
            .execute(
                "UPDATE research_reviews SET checkpoint_json = '{}' WHERE review_id = ?1",
                [&review_id],
            )
            .unwrap();
        let with_non_null_checkpoint = read_mutation_snapshot(&transaction);
        let second = ready_research_action_in_transaction(
            &transaction,
            &project_id,
            &review_id,
            &live_task,
        )
        .unwrap();
        assert!(second.is_none(), "non-NULL checkpoint column must be rejected");
        assert_eq!(
            read_mutation_snapshot(&transaction),
            with_non_null_checkpoint
        );
        transaction.commit().unwrap();

        let persisted: (String, Option<String>, Option<String>) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT state, operation_stage, termination_request_id
                 FROM research_reviews WHERE review_id = ?1",
                [&review_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(persisted, ("ready".to_owned(), None, None));
    }


    struct SourceAuthorityFixture {
        _temp: tempfile::TempDir,
        db: Db,
        project_id: String,
        campaign_id: String,
        experiment_id: String,
        review_id: String,
        proposal_id: String,
        submission_id: String,
        live_task: PueueTask,
        wrapped_command: String,
        request: CheckpointRequest,
        expected: ReadyResearchAction,
        checkpoint: crate::research_checkpoint::PreparedCheckpoint,
        candidate_reference: String,
    }

    fn source_authority_file(
        relative_path: &str,
        bytes: &[u8],
        root: &crate::environment::ResearchDirectoryRecord,
        parent: &crate::environment::ResearchDirectoryRecord,
        device: u64,
        inode: u64,
    ) -> crate::environment::ResearchFileRecord {
        crate::environment::ResearchFileRecord {
            relative_path: relative_path.to_owned(),
            root: root.clone(),
            parent: parent.clone(),
            device,
            inode,
            owner: 3,
            mode: 0o600,
            mount_identity: root.mount_identity,
            logical_bytes: bytes.len() as u64,
            allocated_bytes: 512,
            sha256: format!("{:x}", Sha256::digest(bytes)),
        }
    }

    fn source_authority_fixture() -> SourceAuthorityFixture {
        source_authority_fixture_with_layout(41, ".", "train.py")
    }

    fn source_authority_nested_fixture() -> SourceAuthorityFixture {
        source_authority_fixture_with_layout(41, ".pueue-agent", "trainer/train.py")
    }

    fn source_authority_fixture_with_layout(
        task_id: i64,
        working_directory: &str,
        loader_path: &str,
    ) -> SourceAuthorityFixture {
        let temp = tempfile::tempdir().expect("source authority tempdir");
        let root = temp.path().join("project");
        fs::create_dir_all(&root).expect("source authority project");
        let root = fs::canonicalize(&root).expect("source authority canonical project");
        let config_path = root.join("config.toml");
        fs::write(&config_path, "fixture").expect("source authority config");
        let db = Db::open(&temp.path().join("state.sqlite3")).expect("source authority db");
        let project_id = "source-authority-project".to_owned();
        let campaign_id = "source-authority-campaign".to_owned();
        let experiment_id = "source-authority-experiment".to_owned();
        let proposal_id = "source-authority-proposal".to_owned();
        let submission_id = "source-authority-submission".to_owned();
        let group = "source-authority-group";
        let argv = vec!["python".to_owned(), loader_path.to_owned()];
        let runtime_argv = campaign_experiment_runtime_argv(
            &root,
            &campaign_id,
            &experiment_id,
            &argv,
        );
        let wrapped_command = try_canonical_command_display_os(&runtime_argv)
            .expect("source authority wrapped command");
        let live_task = PueueTask {
            id: task_id,
            group: group.to_owned(),
            command: wrapped_command.clone(),
            state: "Running".to_owned(),
            enqueued_at: Some("900".to_owned()),
            started_at: Some("1000".to_owned()),
            ended_at: None,
            result: None,
        };
        let raw_task_signature = task_signature(&live_task);
        let managed_task_signature = managed_task_run_signature(&live_task)
            .expect("source authority managed task signature");
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                &project_id,
                root.clone(),
                group,
                config_path,
                900,
            ))
            .expect("source authority project registration");
        let objective = ObjectiveSnapshot {
            text: "source authority objective".to_owned(),
            digest: "a".repeat(64),
        };
        let baseline = proposals::validate_initial_baseline(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: "tokenized source authority baseline".to_owned(),
                source_experiment_id: None,
                argv: argv.clone(),
                working_directory: working_directory.to_owned(),
                expected_evidence: vec!["loss".to_owned()],
            },
            &objective.digest,
        )
        .expect("source authority baseline proposal");
        CampaignRepository::new(&db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id: &campaign_id,
                    project_id: &project_id,
                    objective: &objective,
                    initial_argv: &argv,
                    baseline: &baseline,
                    submission_id: &submission_id,
                    experiment_id: &experiment_id,
                    proposal_id: &proposal_id,
                    metadata: &json!({"fixture": "source-authority"}),
                    origin_agent_run_id: None,
                    objective_metric: None,
                    now: 900,
                },
                &CampaignLimits::default(),
            )
            .expect("source authority campaign");
        ExperimentRepository::new(&db)
            .mark_submitting(&experiment_id, 901)
            .expect("source authority submitting");
        ExperimentRepository::new(&db)
            .mark_accepted(&experiment_id, live_task.id, &managed_task_signature, 902)
            .expect("source authority accepted experiment");
        TaskObservationRepository::new(&db)
            .upsert(&NewTaskObservation::new(
                &project_id,
                &raw_task_signature,
                live_task.id,
                group,
                vec![wrapped_command.clone()],
                "Running",
                Some(900),
                Some(1_000),
                None,
                None,
                1_001,
            ))
            .expect("source authority wrapped observation");
        let repository = ResearchRepository::new(&db);
        repository
            .ensure_campaign(&campaign_id)
            .expect("source authority state");
        repository
            .schedule_running(&campaign_id, 1_000, 30, 2_799)
            .expect("source authority schedule");
        let review = repository
            .claim_due(&campaign_id, &experiment_id, &managed_task_signature, 2_800)
            .expect("source authority claim")
            .expect("source authority review");
        let event_id = repository
            .event_id(&review.review_id)
            .expect("source authority event");
        EventRepository::new(&db)
            .claim_by_id(&project_id, event_id, 2_900)
            .expect("source authority event claim")
            .expect("source authority event row");
        let run = AgentRunRepository::new(&db)
            .insert_with_events(
                &NewAgentRun::with_context(
                    &project_id,
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
                        .expect("source authority execution"),
                ),
                &[event_id],
            )
            .expect("source authority run");
        let session_id = "11111111-1111-4111-8111-111111111111";
        let native_authority = json!({
            "version": 1,
            "run_id": run.run_id,
            "review_id": review.review_id,
            "campaign_id": campaign_id,
            "experiment_id": experiment_id,
            "attempt": 1,
            "session_generation": 0,
            "fresh_launch": true,
            "session_id": session_id,
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
        let retry_notes = json!({
            "retry_history": [{
                "attempt": 1,
                "agent_run_id": run.run_id,
                "failure_code": "research_output_invalid",
                "planned_session_id": session_id,
                "confirmed_session_id": session_id,
                "session_binding": "confirmed",
                "native_recovery": native_authority
            }]
        });
        let connection = db
            .connect()
            .expect("source authority setup connection");
        connection
            .execute(
                "UPDATE agent_runs
                 SET status = 'failed', finished_at = 3_001,
                     launch_gate_state = 'failed'
                 WHERE run_id = ?1",
                [run.run_id],
            )
            .expect("source authority terminal run");
        connection
            .execute(
                "UPDATE research_reviews
                 SET state = 'retry_wait', attempt = 2, agent_run_id = NULL,
                     failure_code = 'research_output_invalid', notes_json = ?1
                 WHERE review_id = ?2",
                rusqlite::params![retry_notes.to_string(), review.review_id],
            )
            .expect("source authority retry review");
        connection
            .execute(
                "UPDATE events SET status = 'retry_wait', lease_until = NULL,
                     not_before = 3_001, last_error = 'research_output_invalid'
                 WHERE event_id = ?1",
                [event_id],
            )
            .expect("source authority retry event");

        let root_record = crate::environment::ResearchDirectoryRecord {
            device: 1,
            inode: 2,
            owner: 3,
            mode: 0o700,
            mount_identity: [4, 5],
        };
        let working_directory_record = if working_directory == "." {
            root_record.clone()
        } else {
            crate::environment::ResearchDirectoryRecord {
                device: 1,
                inode: 10,
                owner: 3,
                mode: 0o700,
                mount_identity: [4, 5],
            }
        };
        let loader_bytes = b"print('trainer')\n";
        let loader_root_path = if working_directory == "." {
            loader_path.to_owned()
        } else {
            format!("{working_directory}/{loader_path}")
        };
        let loader_parent_record = if working_directory == "." {
            working_directory_record.clone()
        } else {
            crate::environment::ResearchDirectoryRecord {
                device: 1,
                inode: 11,
                owner: 3,
                mode: 0o700,
                mount_identity: [4, 5],
            }
        };
        let loader_file = source_authority_file(
            &loader_root_path,
            loader_bytes,
            &root_record,
            &loader_parent_record,
            6,
            7,
        );
        let loader_digest = loader_file.sha256.clone();
        let loader_reference = format!("loader-source:{loader_digest}");
        let mut candidates = Vec::new();
        for (ordinal, bytes) in [(0_usize, b"checkpoint-zero".as_slice()), (1, b"checkpoint-one")] {
            let path = format!(
                ".pueue-agent/artifacts/{experiment_id}/checkpoint-{ordinal}.json"
            );
            let argv_path = if working_directory == "." {
                path.clone()
            } else {
                path.strip_prefix(&format!("{working_directory}/"))
                    .expect("source authority nested candidate path")
                    .to_owned()
            };
            let file = source_authority_file(&path, bytes, &root_record, &root_record, 8 + ordinal as u64, 9 + ordinal as u64);
            candidates.push(crate::research_checkpoint::CheckpointCandidateEvidenceV1 {
                reference: format!("checkpoint:{experiment_id}:{ordinal}:{}", file.sha256),
                source_experiment_id: experiment_id.clone(),
                argv_path,
                root_relative_path: path,
                length: file.logical_bytes,
                sha256: file.sha256.clone(),
                file,
            });
        }
        let candidate_reference = candidates[1].reference.clone();
        let request = CheckpointRequest {
            path: candidates[1].argv_path.clone(),
            argv: vec![
                argv[0].clone(),
                loader_path.to_owned(),
                "--resume".to_owned(),
                candidates[1].argv_path.clone(),
            ],
            working_directory: working_directory.to_owned(),
            support_evidence_refs: vec![loader_reference.clone(), candidate_reference.clone()],
        };
        let support = crate::research_checkpoint::CheckpointSupportEvidenceV1::Available {
            support_version: crate::research_checkpoint::CHECKPOINT_SUPPORT_VERSION,
            source_experiment_id: experiment_id.clone(),
            source_proposal_id: proposal_id.clone(),
            source_submission_id: submission_id.clone(),
            normalized_working_directory: working_directory.to_owned(),
            working_directory_record: working_directory_record.clone(),
            loader_support: vec![crate::research_checkpoint::CheckpointLoaderEvidenceV1 {
                reference: loader_reference,
                role: crate::research_checkpoint::CheckpointLoaderRole::Entrypoint,
                argv_index: 1,
                argv_token: loader_path.to_owned(),
                root_relative_path: loader_root_path,
                length: loader_bytes.len() as u64,
                sha256: loader_digest,
                file: loader_file,
                content: String::from_utf8(loader_bytes.to_vec()).expect("loader evidence text"),
            }],
            checkpoint_candidates: candidates,
            candidates_complete: true,
            candidates_omitted_at_least: 0,
            candidate_limit: crate::research_checkpoint::MAX_CHECKPOINT_CANDIDATES,
        };
        let context = json!({
            "schema_version": crate::research_evidence::RESEARCH_CONTEXT_SCHEMA_VERSION,
            "facts": {
                "review": {
                    "review_id": review.review_id,
                    "experiment_id": experiment_id,
                    "task_signature": managed_task_signature,
                },
                "campaign": {"campaign_id": campaign_id},
                "project": {"project_id": project_id},
                "objective": {"digest": objective.digest},
                "target": {
                    "experiment_id": experiment_id,
                    "pueue_task_id": live_task.id,
                    "task_signature": managed_task_signature,
                    "proposal_id": proposal_id,
                    "submission_id": submission_id,
                }
            },
            "operations": {"checkpoint_support": support}
        });
        let context_json = context.to_string();
        let context_digest = format!("{:x}", Sha256::digest(context_json.as_bytes()));
        let response_json = json!({
            "schema_version": 1,
            "review_id": review.review_id,
            "experiment_id": experiment_id,
            "context_digest": context_digest,
            "action": "resume_from_checkpoint",
            "reason": "use the verified checkpoint evidence",
            "evidence_refs": request.support_evidence_refs.clone(),
            "notes": "verified support",
            "checkpoint": request,
        })
        .to_string();
        let native_recovery = connection
            .query_row(
                "SELECT json_extract(notes_json, '$.retry_history[0].native_recovery')
                 FROM research_reviews WHERE review_id = ?1",
                [&review.review_id],
                |row| row.get::<_, String>(0),
            )
            .expect("source authority native recovery");
        let ready_notes = json!({
            "native_recovery": serde_json::from_str::<Value>(&native_recovery)
                .expect("source authority native recovery JSON"),
            "planned_session_id": session_id,
            "confirmed_session_id": session_id,
            "session_binding": "confirmed",
        })
        .to_string();
        connection
            .execute(
                "UPDATE campaign_research
                 SET session_id = ?1, session_generation = 0
                 WHERE campaign_id = ?2",
                rusqlite::params![session_id, campaign_id],
            )
            .expect("source authority session");
        connection
            .execute(
                "UPDATE events
                 SET status = 'completed', lease_until = NULL,
                     completed_at = 3_002
                 WHERE event_id = ?1",
                [event_id],
            )
            .expect("source authority completed event");
        connection
            .execute(
                "UPDATE research_reviews
                 SET state = 'ready', attempt = 1, operation_stage = NULL,
                     agent_run_id = ?1, context_json = ?2, context_digest = ?3,
                     response_json = ?4, termination_request_id = NULL,
                     failure_code = NULL, notes_json = ?5, checkpoint_json = NULL,
                     not_before = 3_003, finished_at = NULL, updated_at = 3_003
                 WHERE review_id = ?6",
                rusqlite::params![
                    run.run_id,
                    context_json,
                    context_digest,
                    response_json,
                    ready_notes,
                    review.review_id,
                ],
            )
            .expect("source authority ready review");
        drop(connection);

        let mut connection = db.connect().expect("source authority action connection");
        let transaction = connection
            .transaction()
            .expect("source authority action transaction");
        let expected = ready_research_action_in_transaction(
            &transaction,
            &project_id,
            &review.review_id,
            &live_task,
        )
        .expect("source authority ready action query")
        .expect("source authority ready action");
        transaction.commit().expect("source authority action commit");

        let source_authority = match checkpoint_source_authority_for_preparation(
            &db,
            &expected,
            &request,
        )
        .expect("source authority preparation read")
        {
            CheckpointSourceAuthorityRead::Supported(authority) => authority,
            CheckpointSourceAuthorityRead::Unsupported { reason } => {
                panic!("source authority fixture unexpectedly unsupported: {reason}")
            }
        };
        let selected = select_checkpoint_support(&source_authority.support, &request)
            .expect("source authority selected support");
        assert_eq!(selected.candidate.reference, candidate_reference);
        let proposal_digest: String = db
            .connect()
            .expect("source authority proposal digest connection")
            .query_row(
                "SELECT canonical_digest FROM proposals WHERE proposal_id = ?1",
                [&proposal_id],
                |row| row.get(0),
            )
            .expect("source authority proposal digest");
        let checkpoint = source_authority_prepared_checkpoint(
            &expected,
            &source_authority,
            &request,
            &proposal_digest,
        );

        SourceAuthorityFixture {
            _temp: temp,
            db,
            project_id,
            campaign_id,
            experiment_id,
            review_id: review.review_id,
            proposal_id,
            submission_id,
            live_task,
            wrapped_command,
            request,
            expected,
            checkpoint,
            candidate_reference,
        }
    }

    fn source_authority_prepared_checkpoint(
        expected: &ReadyResearchAction,
        authority: &CheckpointSourceAuthority,
        request: &CheckpointRequest,
        proposal_digest: &str,
    ) -> crate::research_checkpoint::PreparedCheckpoint {
        let selected = select_checkpoint_support(&authority.support, request)
            .expect("source authority checkpoint selection");
        let source_argv = authority.source.proposal.argv.clone();
        let delta = crate::research_checkpoint::validate_checkpoint_argv_delta(
            &source_argv,
            &request.argv,
            &request.path,
        )
        .expect("source authority checkpoint argv delta");
        let retained_path = format!(
            "/private/state/research-checkpoints/{}/{}/checkpoint",
            authority.source.campaign.campaign_id,
            expected.owner.review_id
        );
        let mut retained_argv = request.argv.clone();
        match delta.form() {
            crate::research_checkpoint::CheckpointArgvDeltaForm::Pair => {
                retained_argv[delta.index() + 1] = retained_path;
            }
            crate::research_checkpoint::CheckpointArgvDeltaForm::Equals => {
                retained_argv[delta.index()] = format!("{}={retained_path}", delta.flag());
            }
        }
        let retained_root = crate::environment::ResearchDirectoryRecord {
            device: 11,
            inode: 12,
            owner: 3,
            mode: 0o700,
            mount_identity: [14, 15],
        };
        let retained_parent = crate::environment::ResearchDirectoryRecord {
            device: 11,
            inode: 13,
            owner: 3,
            mode: 0o700,
            mount_identity: retained_root.mount_identity,
        };
        let response_digest = format!("{:x}", Sha256::digest(expected.response_json.as_bytes()));
        crate::research_checkpoint::PreparedCheckpoint {
            schema_version: crate::research_checkpoint::PREPARED_CHECKPOINT_VERSION,
            project_id: authority.project.project_id.clone(),
            campaign_id: authority.source.campaign.campaign_id.clone(),
            review_id: expected.owner.review_id.clone(),
            review_attempt: expected.owner.attempt,
            review_session_generation: expected.owner.session_generation,
            review_agent_run_id: expected.owner.agent_run_id.expect("fixture agent run"),
            review_event_id: expected.owner.event_id.expect("fixture event"),
            source_experiment_id: authority.source.experiment.experiment_id.clone(),
            source_proposal_id: authority.source.proposal.proposal_id.clone(),
            source_submission_id: authority.source.submission.submission_id.clone(),
            source_task_id: expected.owner.source_task_id.expect("fixture source task"),
            source_managed_task_signature: expected.owner.managed_task_signature.clone(),
            source_raw_task_signature: expected.raw_task_signature.clone(),
            context_digest: expected.context_digest.clone(),
            response_digest,
            campaign_objective_digest: expected.campaign_objective_digest.clone(),
            source_proposal_canonical_digest: proposal_digest.to_owned(),
            learning_spec_digest: crate::research_checkpoint::checkpoint_learning_spec_digest(
                &source_argv,
                &authority.source.proposal.working_directory,
            )
            .expect("fixture learning digest"),
            source_runtime: crate::research_checkpoint::CheckpointSourceRuntimeV1::OriginalProjectRoot,
            source_root_canonical_path: authority
                .project
                .root_path
                .to_str()
                .expect("fixture root path")
                .to_owned(),
            source_root_resolution_fingerprint: "fixture-root-fingerprint".to_owned(),
            source_root_record: selected.loader.file.root.clone(),
            source_working_directory_record: match &authority.support {
                CheckpointSupportEvidenceV1::Available {
                    working_directory_record,
                    ..
                } => working_directory_record.clone(),
                CheckpointSupportEvidenceV1::Unavailable { .. } => {
                    panic!("fixture support must be available")
                }
            },
            support_version: crate::research_checkpoint::CHECKPOINT_SUPPORT_VERSION,
            loader: selected.loader.clone(),
            source_checkpoint: selected.candidate.clone(),
            retained_checkpoint: crate::environment::ResearchFileRecord {
                relative_path: format!(
                    "research-checkpoints/{}/{}/checkpoint",
                    authority.source.campaign.campaign_id,
                    expected.owner.review_id
                ),
                root: retained_root.clone(),
                parent: retained_parent,
                device: 16,
                inode: 17,
                owner: 3,
                mode: 0o600,
                mount_identity: retained_root.mount_identity,
                logical_bytes: selected.candidate.length,
                allocated_bytes: 512,
                sha256: selected.candidate.sha256.clone(),
            },
            source_argv,
            source_working_directory: authority.source.proposal.working_directory.clone(),
            request: request.clone(),
            delta,
            retained_argv,
            successor_ids: crate::research_checkpoint::checkpoint_successor_ids(
                &expected.owner.review_id,
                expected.owner.attempt,
            )
            .expect("fixture successor IDs"),
        }
    }

    struct CheckpointDispatchFixture {
        fixture: SourceAuthorityFixture,
        encoded: String,
        authority: CheckpointDispatchAuthority,
    }

    fn checkpoint_confirmed_fixture() -> (
        SourceAuthorityFixture,
        String,
        ResearchOwnershipSnapshot,
    ) {
        confirm_checkpoint_fixture(source_authority_fixture())
    }

    fn confirm_checkpoint_fixture(
        fixture: SourceAuthorityFixture,
    ) -> (
        SourceAuthorityFixture,
        String,
        ResearchOwnershipSnapshot,
    ) {
        let incident = Incident {
            incident_id: 81,
            project_id: fixture.project_id.clone(),
            kind: "research_checkpoint".to_owned(),
            task_key: Some(fixture.expected.owner.source_experiment_id.clone()),
            fingerprint: "checkpoint-fingerprint-2".to_owned(),
            status: IncidentStatus::Open,
            first_seen_at: 3_100,
            last_seen_at: 3_100,
            acknowledged_at: None,
            resolved_at: None,
        };
        let request = TerminationRequest {
            request_id: 82,
            incident_id: incident.incident_id,
            project_id: fixture.project_id.clone(),
            task_signature: fixture.expected.raw_task_signature.clone(),
            reason: format!("research_action:{}:checkpoint", fixture.review_id),
            status: TerminationRequestStatus::Confirmed,
            requested_at: 3_100,
            dispatch_lease_until: None,
            grace_until: None,
            confirmed_at: Some(3_101),
            last_error: None,
        };
        let connection = fixture.db.connect().expect("checkpoint dispatch setup connection");
        connection
            .execute(
                "INSERT INTO incidents (
                    incident_id, project_id, kind, task_key, fingerprint, status,
                    first_seen_at, last_seen_at, acknowledged_at, resolved_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, NULL, NULL)",
                rusqlite::params![
                    incident.incident_id,
                    incident.project_id,
                    incident.kind,
                    incident.task_key,
                    incident.fingerprint,
                    incident.status,
                    incident.first_seen_at,
                ],
            )
            .expect("checkpoint dispatch incident");
        connection
            .execute(
                "INSERT INTO termination_requests (
                    request_id, incident_id, project_id, task_signature, reason,
                    status, requested_at, dispatch_lease_until, grace_until,
                    confirmed_at, last_error
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, NULL)",
                rusqlite::params![
                    request.request_id,
                    request.incident_id,
                    request.project_id,
                    request.task_signature,
                    request.reason,
                    request.status,
                    request.requested_at,
                    request.confirmed_at,
                ],
            )
            .expect("checkpoint dispatch termination");
        let encoded = crate::research_checkpoint::serialize_prepared_checkpoint(&fixture.checkpoint)
            .expect("checkpoint dispatch encoding");
        let mut connection = connection;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("checkpoint dispatch intent transaction");
        assert!(bind_checkpoint_termination_intent_in_transaction(
            &transaction,
            &fixture.expected,
            &encoded,
            &incident,
            &request,
            3_100,
        )
        .expect("checkpoint dispatch intent bind"));
        transaction
            .commit()
            .expect("checkpoint dispatch intent commit");
        connection
            .execute(
                "UPDATE research_reviews
                 SET operation_stage = 'stop_confirmed', updated_at = ?1
                 WHERE review_id = ?2 AND operation_stage = 'intent'
                   AND termination_request_id = ?3",
                rusqlite::params![3_101, fixture.review_id, request.request_id],
            )
            .expect("checkpoint dispatch confirmed review");

        let mut owner = fixture.expected.owner.clone();
        owner.operation_stage = Some("stop_confirmed".to_owned());
        owner.termination_request_id = Some(request.request_id);
        (fixture, encoded, owner)
    }

    fn checkpoint_dispatch_fixture() -> CheckpointDispatchFixture {
        let (fixture, encoded, owner) = checkpoint_confirmed_fixture();
        let mut connection = fixture.db.connect().expect("checkpoint dispatch transaction connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("checkpoint dispatch transaction");
        let admission = super::super::campaigns::accept_checkpoint_successor_in_transaction(
            &transaction,
            &owner,
            &CampaignLimits::default(),
            3_102,
        )
        .expect("checkpoint dispatch admission");
        let successor = match admission {
            CheckpointSuccessorAdmission::Ready(intent) => intent,
            other => panic!("unexpected checkpoint dispatch admission: {other:?}"),
        };
        assert_eq!(
            successor.experiment.experiment_id,
            fixture.checkpoint.successor_ids.experiment_id
        );
        transaction.commit().expect("checkpoint dispatch commit");

        let repository = ResearchRepository::new(&fixture.db);
        let selection = repository
            .checkpoint_dispatch_authority(
                &fixture.project_id,
                &fixture.checkpoint.successor_ids.experiment_id,
                3_103,
            )
            .expect("checkpoint dispatch authority");
        let authority = match selection {
            CheckpointDispatchSelection::Ready(authority) => authority,
            other => panic!("unexpected checkpoint dispatch selection: {other:?}"),
        };

        CheckpointDispatchFixture {
            fixture,
            encoded,
            authority,
        }
    }

    #[test]
    fn checkpoint_dispatch_authority_captures_selected_successor_phase() {
        let cases: Vec<(&str, ExperimentStatus, fn(&CheckpointDispatchFixture))> = vec![
            ("reserved", ExperimentStatus::Reserved, |_| {}),
            ("submitting", ExperimentStatus::Submitting, |fixture| {
                fixture
                    .fixture
                    .db
                    .connect()
                    .expect("phase submitting connection")
                    .execute(
                        "UPDATE experiments SET status = 'submitting'
                         WHERE experiment_id = ?1",
                        [&fixture.authority.successor_experiment_id],
                    )
                    .expect("phase submitting update");
            }),
            ("unreconciled", ExperimentStatus::Unreconciled, |fixture| {
                let connection = fixture
                    .fixture
                    .db
                    .connect()
                    .expect("phase unreconciled connection");
                connection
                    .execute(
                        "UPDATE experiments
                         SET status = 'unreconciled', failure_code = 'identity-mismatch'
                         WHERE experiment_id = ?1",
                        [&fixture.authority.successor_experiment_id],
                    )
                    .expect("phase unreconciled experiment update");
                connection
                    .execute(
                        "UPDATE submissions SET status = 'unreconciled'
                         WHERE submission_id = ?1",
                        [&fixture.authority.submission_id],
                    )
                    .expect("phase unreconciled submission update");
                connection
                    .execute(
                        "UPDATE budget_reservations SET status = 'consumed'
                         WHERE experiment_id = ?1 AND dimension = 'experiment'",
                        [&fixture.authority.successor_experiment_id],
                    )
                    .expect("phase unreconciled reservation update");
            }),
            ("succeeded", ExperimentStatus::Succeeded, |fixture| {
                let connection = fixture
                    .fixture
                    .db
                    .connect()
                    .expect("phase succeeded connection");
                mutate_checkpoint_terminal(fixture, &connection, "succeeded");
            }),
            ("failed", ExperimentStatus::Failed, |fixture| {
                let connection = fixture
                    .fixture
                    .db
                    .connect()
                    .expect("phase failed connection");
                mutate_checkpoint_terminal(fixture, &connection, "failed");
            }),
            ("cancelled", ExperimentStatus::Cancelled, |fixture| {
                let connection = fixture
                    .fixture
                    .db
                    .connect()
                    .expect("phase cancelled connection");
                mutate_checkpoint_terminal(fixture, &connection, "cancelled");
            }),
        ];
        for (label, expected_status, mutate) in cases {
            let fixture = checkpoint_dispatch_fixture();
            mutate(&fixture);
            let selection = ResearchRepository::new(&fixture.fixture.db)
                .checkpoint_dispatch_authority(
                    &fixture.fixture.project_id,
                    &fixture.authority.successor_experiment_id,
                    3_104,
                )
                .expect("phase dispatch authority");
            let authority = match selection {
                CheckpointDispatchSelection::Ready(authority) => authority,
                other => panic!("{label}: unexpected phase selection: {other:?}"),
            };
            assert_eq!(authority.successor_status(), expected_status, "{label}");
        }
    }

    #[derive(Debug, PartialEq)]
    struct CheckpointGraphSnapshot {
        review: Vec<rusqlite::types::Value>,
        proposal: Vec<rusqlite::types::Value>,
        submission: Vec<rusqlite::types::Value>,
        experiment: Vec<rusqlite::types::Value>,
        reservation: Vec<rusqlite::types::Value>,
        termination: Vec<rusqlite::types::Value>,
        resource_counts: Vec<rusqlite::types::Value>,
    }

    fn snapshot_sql_row<P: rusqlite::Params>(
        connection: &Connection,
        sql: &str,
        params: P,
    ) -> Vec<rusqlite::types::Value> {
        connection
            .query_row(sql, params, |row| {
                (0..row.as_ref().column_count())
                    .map(|index| row.get(index))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .expect("checkpoint graph snapshot row")
    }

    fn checkpoint_limit_snapshot(fixture: &SourceAuthorityFixture) -> Vec<rusqlite::types::Value> {
        let connection = fixture.db.connect().expect("checkpoint limit snapshot connection");
        snapshot_sql_row(
            &connection,
            "SELECT
                (SELECT state FROM research_reviews WHERE review_id = ?1),
                (SELECT operation_stage FROM research_reviews WHERE review_id = ?1),
                (SELECT checkpoint_json FROM research_reviews WHERE review_id = ?1),
                (SELECT termination_request_id FROM research_reviews WHERE review_id = ?1),
                (SELECT successor_experiment_id FROM research_reviews WHERE review_id = ?1),
                (SELECT status FROM experiments WHERE experiment_id = ?2),
                (SELECT pueue_task_id FROM experiments WHERE experiment_id = ?2),
                (SELECT task_signature FROM experiments WHERE experiment_id = ?2),
                (SELECT state FROM campaigns WHERE campaign_id = ?3),
                (SELECT state_reason FROM campaigns WHERE campaign_id = ?3),
                (SELECT next_eligible_at FROM campaigns WHERE campaign_id = ?3),
                (SELECT session_id FROM campaign_research WHERE campaign_id = ?3),
                (SELECT session_generation FROM campaign_research WHERE campaign_id = ?3),
                (SELECT next_due_at FROM campaign_research WHERE campaign_id = ?3),
                (SELECT blocked_reason FROM campaign_research WHERE campaign_id = ?3),
                (SELECT last_review_id FROM campaign_research WHERE campaign_id = ?3),
                (SELECT COUNT(*) FROM proposals WHERE campaign_id = ?3),
                (SELECT COUNT(*) FROM submissions WHERE project_id = ?4),
                (SELECT COUNT(*) FROM experiments WHERE campaign_id = ?3),
                (SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = ?3),
                (SELECT COUNT(*) FROM incidents WHERE project_id = ?4),
                (SELECT COUNT(*) FROM termination_requests WHERE project_id = ?4)",
            rusqlite::params![fixture.review_id, fixture.experiment_id, fixture.campaign_id, fixture.project_id],
        )
    }

    fn insert_limit_experiment(
        fixture: &SourceAuthorityFixture,
        suffix: &str,
        argv: Vec<String>,
        working_directory: &str,
        resume_of_experiment_id: Option<&str>,
        now: i64,
    ) {
        let proposal_id = format!("limits-proposal-{suffix}");
        let submission_id = format!("limits-submission-{suffix}");
        let experiment_id = format!("limits-experiment-{suffix}");
        let proposal = proposals::validate(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: format!("limits fixture {suffix}"),
                source_experiment_id: resume_of_experiment_id.map(str::to_owned),
                argv,
                working_directory: working_directory.to_owned(),
                expected_evidence: vec!["loss".to_owned()],
            },
            &"a".repeat(64),
        )
        .expect("limits fixture proposal");
        let argv_json = serde_json::to_string(proposal.argv()).expect("limits fixture argv");
        let evidence_json = serde_json::to_string(proposal.expected_evidence())
            .expect("limits fixture evidence");
        let mut connection = fixture.db.connect().expect("limits fixture connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("limits fixture transaction");
        super::super::campaigns::insert_proposal(
            &transaction,
            &proposal_id,
            &fixture.campaign_id,
            &proposal,
            ProposalStatus::Accepted,
            &argv_json,
            &evidence_json,
            now,
        )
        .expect("limits fixture proposal insert");
        super::super::campaigns::insert_submission(
            &transaction,
            &submission_id,
            &fixture.project_id,
            &argv_json,
            "{}",
            None,
            now,
        )
        .expect("limits fixture submission insert");
        super::super::campaigns::insert_experiment(
            &transaction,
            &experiment_id,
            &fixture.campaign_id,
            &proposal_id,
            &submission_id,
            resume_of_experiment_id,
            1,
            resume_of_experiment_id,
            None,
            None,
            None,
            now,
        )
        .expect("limits fixture experiment insert");
        super::super::campaigns::insert_experiment_reservation(
            &transaction,
            &fixture.campaign_id,
            &experiment_id,
            now,
            now + 3_600,
        )
        .expect("limits fixture experiment reservation");
        transaction.commit().expect("limits fixture commit");
    }

    fn insert_limit_agent_reservation(
        fixture: &SourceAuthorityFixture,
        suffix: &str,
        now: i64,
    ) {
        let connection = fixture.db.connect().expect("agent budget fixture connection");
        connection
            .execute(
                "INSERT INTO budget_reservations (
                    reservation_id, campaign_id, experiment_id, dimension, subject_key,
                    status, window_started_at, window_ends_at, created_at, updated_at
                 ) VALUES (?1, ?2, NULL, 'agent_run', ?3, 'reserved', ?4, ?5, ?4, ?4)",
                rusqlite::params![
                    format!("limits-agent:{suffix}"),
                    fixture.campaign_id,
                    format!("limits-agent-subject:{suffix}"),
                    now,
                    now + 3_600,
                ],
            )
            .expect("agent budget fixture reservation");
    }

    fn checkpoint_preflight(
        fixture: &SourceAuthorityFixture,
        limits: &CampaignLimits,
        now: i64,
    ) -> CheckpointSuccessorPreflight {
        let mut connection = fixture.db.connect().expect("checkpoint limit preflight connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("checkpoint limit preflight transaction");
        let result = checkpoint_successor_preflight_in_transaction(
            &transaction,
            &fixture.expected,
            &fixture.checkpoint,
            limits,
            now,
        )
        .expect("checkpoint limit preflight");
        transaction.commit().expect("checkpoint limit preflight commit");
        result
    }

    fn assert_checkpoint_limit_deferred_after_intent<F>(
        fixture: SourceAuthorityFixture,
        limits: &CampaignLimits,
        insert_competitor: F,
        label: &str,
    ) where
        F: FnOnce(&SourceAuthorityFixture),
    {
        assert_eq!(
            checkpoint_preflight(&fixture, limits, 3_100),
            CheckpointSuccessorPreflight::Available,
            "{label}: preflight should be available before intent"
        );
        let (fixture, _encoded, owner) = confirm_checkpoint_fixture(fixture);
        insert_competitor(&fixture);
        let final_snapshot = checkpoint_limit_snapshot(&fixture);
        let mut connection = fixture
            .db
            .connect()
            .expect("post-intent final connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("post-intent final transaction");
        assert_eq!(
            super::super::campaigns::accept_checkpoint_successor_in_transaction(
                &transaction,
                &owner,
                limits,
                3_101,
            )
            .expect("post-intent final admission"),
            CheckpointSuccessorAdmission::Deferred,
            "{label}: final admission should defer after intent contention"
        );
        transaction
            .commit()
            .expect("post-intent final commit");
        assert_eq!(
            checkpoint_limit_snapshot(&fixture),
            final_snapshot,
            "{label}: deferred final admission mutated checkpoint resources"
        );
    }

    fn rewrite_checkpoint_retained_path(
        fixture: &CheckpointDispatchFixture,
        retained_path: &str,
    ) {
        let mut checkpoint = fixture.fixture.checkpoint.clone();
        let path_index = checkpoint
            .request
            .argv
            .iter()
            .zip(&checkpoint.retained_argv)
            .position(|(requested, retained)| requested != retained)
            .expect("checkpoint retained path index");
        if let Some((flag, _)) = checkpoint.retained_argv[path_index].split_once('=') {
            checkpoint.retained_argv[path_index] = format!("{flag}={retained_path}");
        } else {
            checkpoint.retained_argv[path_index] = retained_path.to_owned();
        }
        let encoded = crate::research_checkpoint::serialize_prepared_checkpoint(&checkpoint)
            .expect("rewritten checkpoint encoding");
        let connection = fixture
            .fixture
            .db
            .connect()
            .expect("rewritten checkpoint connection");
        let expected_evidence: Vec<String> = connection
            .query_row(
                "SELECT expected_evidence_json FROM proposals WHERE proposal_id = ?1",
                [&fixture.fixture.proposal_id],
                |row| row.get::<_, String>(0),
            )
            .map(|json| serde_json::from_str(&json).expect("rewritten checkpoint evidence"))
            .expect("rewritten checkpoint source evidence");
        let objective_digest: String = connection
            .query_row(
                "SELECT objective_digest FROM campaigns WHERE campaign_id = ?1",
                [&fixture.fixture.campaign_id],
                |row| row.get(0),
            )
            .expect("rewritten checkpoint objective");
        let proposal = proposals::validate(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: format!(
                    "Resume {} from its verified checkpoint",
                    fixture.fixture.experiment_id
                ),
                source_experiment_id: Some(fixture.fixture.experiment_id.clone()),
                argv: checkpoint.retained_argv.clone(),
                working_directory: checkpoint.source_working_directory.clone(),
                expected_evidence,
            },
            &objective_digest,
        )
        .expect("rewritten checkpoint proposal");
        let argv_json = serde_json::to_string(proposal.argv()).expect("rewritten checkpoint argv");
        let checkpoint_note = format!(
            "research-checkpoint:{:x}",
            Sha256::digest(encoded.as_bytes())
        );
        connection
            .execute(
                "UPDATE proposals
                 SET argv_json = ?1, canonical_digest = ?2
                 WHERE proposal_id = ?3",
                rusqlite::params![argv_json, proposal.canonical_digest(), fixture.authority.proposal_id],
            )
            .expect("rewritten checkpoint proposal row");
        connection
            .execute(
                "UPDATE submissions SET argv_json = ?1 WHERE submission_id = ?2",
                rusqlite::params![argv_json, fixture.authority.submission_id],
            )
            .expect("rewritten checkpoint submission row");
        connection
            .execute(
                "UPDATE experiments SET checkpoint_note = ?1 WHERE experiment_id = ?2",
                rusqlite::params![checkpoint_note, fixture.authority.successor_experiment_id],
            )
            .expect("rewritten checkpoint experiment row");
        connection
            .execute(
                "UPDATE research_reviews SET checkpoint_json = ?1 WHERE review_id = ?2",
                rusqlite::params![encoded, fixture.fixture.review_id],
            )
            .expect("rewritten checkpoint review row");
    }

    fn settle_checkpoint_history(fixture: &CheckpointDispatchFixture) {
        let connection = fixture
            .fixture
            .db
            .connect()
            .expect("history completion connection");
        connection
            .execute(
                "UPDATE experiments
                 SET status = 'succeeded', pueue_task_id = 501,
                     task_signature = 'history-terminal-task',
                     finished_at = 4_000, updated_at = 4_000
                 WHERE experiment_id = ?1",
                [&fixture.authority.successor_experiment_id],
            )
            .expect("history completed experiment");
        connection
            .execute(
                "UPDATE submissions
                 SET status = 'accepted', pueue_task_id = 501,
                     task_signature = 'history-terminal-task'
                 WHERE submission_id = ?1",
                [&fixture.authority.submission_id],
            )
            .expect("history completed submission");
        connection
            .execute(
                "UPDATE budget_reservations SET status = 'consumed', updated_at = 4_000
                 WHERE experiment_id = ?1 AND dimension = 'experiment'",
                [&fixture.authority.successor_experiment_id],
            )
            .expect("history completed reservation");
        connection
            .execute(
                "UPDATE research_reviews
                 SET state = 'completed', operation_stage = NULL,
                     finished_at = 4_000, updated_at = 4_000
                 WHERE review_id = ?1",
                [&fixture.fixture.review_id],
            )
            .expect("history completed review");
    }

    fn assert_checkpoint_history_count(fixture: &CheckpointDispatchFixture, expected: i64) {
        let mut connection = fixture
            .fixture
            .db
            .connect()
            .expect("history count connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("history count transaction");
        assert_eq!(
            checkpoint_same_spec_count(&transaction, &fixture.fixture.checkpoint)
                .expect("history count"),
            expected
        );
        transaction.commit().expect("history count commit");
    }

    fn checkpoint_graph_snapshot(fixture: &CheckpointDispatchFixture) -> CheckpointGraphSnapshot {
        let connection = fixture
            .fixture
            .db
            .connect()
            .expect("checkpoint graph snapshot connection");
        let review = snapshot_sql_row(
            &connection,
            "SELECT state, attempt, operation_stage, checkpoint_json,
                    termination_request_id, successor_experiment_id, failure_code,
                    finished_at, updated_at, notes_json
             FROM research_reviews WHERE review_id = ?1",
            [&fixture.fixture.review_id],
        );
        let proposal = snapshot_sql_row(
            &connection,
            "SELECT proposal_id, campaign_id, kind, status, hypothesis,
                    source_experiment_id, argv_json, working_directory,
                    expected_evidence_json, canonical_digest, reject_reason,
                    created_at, updated_at
             FROM proposals WHERE proposal_id = ?1",
            [&fixture.authority.proposal_id],
        );
        let submission = snapshot_sql_row(
            &connection,
            "SELECT submission_id, project_id, argv_json, created_at,
                    pueue_task_id, task_signature, status, kind, metadata_json,
                    origin_agent_run_id
             FROM submissions WHERE submission_id = ?1",
            [&fixture.authority.submission_id],
        );
        let experiment = snapshot_sql_row(
            &connection,
            "SELECT experiment_id, campaign_id, proposal_id, submission_id,
                    parent_experiment_id, attempt, status, pueue_task_id,
                    task_signature, failure_code, failure_fingerprint, created_at,
                    updated_at, finished_at, resume_of_experiment_id,
                    checkpoint_note, code_change_run_id, code_revision_sha
             FROM experiments WHERE experiment_id = ?1",
            [&fixture.authority.successor_experiment_id],
        );
        let reservation = snapshot_sql_row(
            &connection,
            "SELECT reservation_id, campaign_id, experiment_id, dimension,
                    subject_key, status, window_started_at, window_ends_at,
                    created_at, updated_at
             FROM budget_reservations
             WHERE experiment_id = ?1 AND dimension = 'experiment'",
            [&fixture.authority.successor_experiment_id],
        );
        let termination = snapshot_sql_row(
            &connection,
            "SELECT request_id, incident_id, project_id, task_signature, reason,
                    status, requested_at, dispatch_lease_until, grace_until,
                    confirmed_at, last_error
             FROM termination_requests
             WHERE request_id = (
                 SELECT termination_request_id FROM research_reviews WHERE review_id = ?1
             )",
            [&fixture.fixture.review_id],
        );
        let resource_counts = snapshot_sql_row(
            &connection,
            "SELECT
                    (SELECT COUNT(*) FROM projects),
                    (SELECT COUNT(*) FROM campaigns),
                    (SELECT COUNT(*) FROM proposals),
                    (SELECT COUNT(*) FROM submissions),
                    (SELECT COUNT(*) FROM experiments),
                    (SELECT COUNT(*) FROM budget_reservations),
                    (SELECT COUNT(*) FROM incidents),
                    (SELECT COUNT(*) FROM termination_requests)",
            [],
        );
        CheckpointGraphSnapshot {
            review,
            proposal,
            submission,
            experiment,
            reservation,
            termination,
            resource_counts,
        }
    }

    fn run_stale_dispatch_case<F>(label: &str, mutate: F) -> Result<(), String>
    where
        F: FnOnce(&CheckpointDispatchFixture, &Connection),
    {
        let fixture = checkpoint_dispatch_fixture();
        let connection = fixture
            .fixture
            .db
            .connect()
            .expect("stale dispatch foreign setup connection");
        insert_foreign_campaign(&fixture, &connection);
        drop(connection);
        let baseline = checkpoint_graph_snapshot(&fixture);
        let connection = fixture
            .fixture
            .db
            .connect()
            .expect("stale dispatch mutation connection");
        mutate(&fixture, &connection);
        drop(connection);
        let mutated = checkpoint_graph_snapshot(&fixture);
        assert_ne!(baseline, mutated, "{label}: mutation did not change setup");

        let result = ExperimentRepository::new(&fixture.fixture.db)
            .begin_checkpoint_submitting_or_defer(&fixture.authority, 3_104);
        if result.is_ok() {
            return Err(format!("{label}: stale authority was accepted: {result:?}"));
        }
        if checkpoint_graph_snapshot(&fixture) != mutated {
            return Err(format!(
                "{label}: rejected stale authority mutated unrelated resources"
            ));
        }
        Ok(())
    }

    fn insert_foreign_campaign(
        fixture: &CheckpointDispatchFixture,
        connection: &Connection,
    ) {
        let project_id = "checkpoint-foreign-project";
        let campaign_id = "checkpoint-foreign-campaign";
        connection
            .execute(
                "INSERT INTO projects (
                    project_id, root_path, pueue_group, config_path,
                    enabled, paused, halted_reason, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, 1, 0, NULL, 900, 900)",
                rusqlite::params![
                    project_id,
                    format!("/private/checkpoint-foreign-root/{}", fixture.fixture.review_id),
                    format!("checkpoint-foreign-group-{}", fixture.fixture.review_id),
                    format!("/private/checkpoint-foreign-config/{}", fixture.fixture.review_id),
                ],
            )
            .expect("checkpoint foreign project");
        connection
            .execute(
                "INSERT INTO campaigns (
                    campaign_id, project_id, objective_text, objective_digest,
                    initial_argv_json, state, state_reason, baseline_experiment_id,
                    next_eligible_at, created_at, updated_at
                 ) VALUES (?1, ?2, 'foreign', ?3, '[]', 'active', NULL, NULL, NULL, 900, 900)",
                rusqlite::params![campaign_id, project_id, "f".repeat(64)],
            )
            .expect("checkpoint foreign campaign");
    }

    fn attach_checkpoint_decision_cycle(
        fixture: &CheckpointDispatchFixture,
        connection: &Connection,
    ) {
        connection
            .execute(
                "INSERT INTO decision_cycles (
                    cycle_id, campaign_id, source_experiment_id, state,
                    next_wake_at, consecutive_failed_attempts, last_decision_kind,
                    last_failure_code, last_failure_summary, created_at, updated_at,
                    source_terminal_at
                 ) VALUES (?1, ?2, ?3, 'pending', NULL, 0, NULL, NULL, NULL, 3_100, 3_100, 1)",
                rusqlite::params![
                    "checkpoint-hybrid-cycle",
                    fixture.fixture.campaign_id,
                    fixture.fixture.experiment_id,
                ],
            )
            .expect("checkpoint hybrid decision cycle");
        connection
            .execute(
                "UPDATE research_reviews
                 SET decision_cycle_id = ?1
                 WHERE review_id = ?2",
                rusqlite::params!["checkpoint-hybrid-cycle", fixture.fixture.review_id],
            )
            .expect("checkpoint hybrid review cycle");
    }

    fn mutate_review_notes<F: FnOnce(&mut Value)>(
        fixture: &CheckpointDispatchFixture,
        connection: &Connection,
        mutate: F,
    ) {
        let notes_json: String = connection
            .query_row(
                "SELECT notes_json FROM research_reviews WHERE review_id = ?1",
                [&fixture.fixture.review_id],
                |row| row.get(0),
            )
            .expect("checkpoint review notes");
        let mut notes: Value = serde_json::from_str(&notes_json).expect("checkpoint notes JSON");
        mutate(&mut notes);
        connection
            .execute(
                "UPDATE research_reviews SET notes_json = ?1 WHERE review_id = ?2",
                rusqlite::params![notes.to_string(), fixture.fixture.review_id],
            )
            .expect("checkpoint mutated review notes");
    }

    fn assert_source_authority_fresh_error<F>(label: &str, mutate: F)
    where
        F: FnOnce(&mut SourceAuthorityFixture, &Connection),
    {
        let mut fixture = source_authority_fixture();
        let connection = fixture
            .db
            .connect()
            .expect("fresh source authority mutation connection");
        mutate(&mut fixture, &connection);
        let result = checkpoint_source_authority_for_ready_in_connection(
            &connection,
            &fixture.expected,
            &fixture.request,
        );
        assert!(result.is_err(), "{label}: fresh source authority mutation unexpectedly passed");
    }

    #[test]
    fn source_authority_fixture_proves_ready_available_and_codec_roundtrip() {
        let fixture = source_authority_fixture();
        assert_eq!(fixture.project_id, "source-authority-project");
        assert_eq!(fixture.campaign_id, "source-authority-campaign");
        assert_eq!(fixture.experiment_id, "source-authority-experiment");
        assert_eq!(fixture.review_id, fixture.expected.owner.review_id);
        assert_eq!(fixture.proposal_id, "source-authority-proposal");
        assert_eq!(fixture.submission_id, "source-authority-submission");
        assert_eq!(fixture.live_task.id, 41);
        assert_eq!(fixture.expected.owner.source_task_id, Some(41));
        assert!(fixture.expected.answer.checkpoint.is_some());
        let support = checkpoint_support_from_persisted_context(
            &fixture.expected.context_json,
            &fixture.expected.context_digest,
        )
        .expect("fixture persisted support");
        let selected = select_checkpoint_support(&support, &fixture.request)
            .expect("fixture candidate selection");
        assert_eq!(selected.candidate.reference, fixture.candidate_reference);
        assert!(selected.candidate.reference.starts_with(
            "checkpoint:source-authority-experiment:1:"
        ));
        let encoded = crate::research_checkpoint::serialize_prepared_checkpoint(
            &fixture.checkpoint,
        )
        .expect("fixture prepared checkpoint serialization");
        assert_eq!(
            crate::research_checkpoint::parse_prepared_checkpoint(&encoded)
                .expect("fixture prepared checkpoint parsing"),
            fixture.checkpoint
        );
        let fresh = checkpoint_source_authority_for_preparation(
            &fixture.db,
            &fixture.expected,
            &fixture.request,
        )
        .expect("fixture fresh source authority");
        let CheckpointSourceAuthorityRead::Supported(fresh) = fresh else {
            panic!("fixture source authority unexpectedly unsupported")
        };
        assert_eq!(fresh.observation.command, vec![fixture.wrapped_command]);

        let connection = fixture
            .db
            .connect()
            .expect("fixture caller source authority connection");
        let caller = checkpoint_source_authority_for_ready_in_connection(
            &connection,
            &fixture.expected,
            &fixture.request,
        )
        .expect("fixture caller source authority");
        assert!(matches!(
            caller,
            CheckpointSourceAuthorityRead::Supported(_)
        ));
    }

    #[test]
    fn checkpoint_preflight_proves_capacity_and_learning_spec_in_one_transaction() {
        let fixture = source_authority_fixture();
        let mut connection = fixture.db.connect().expect("checkpoint preflight connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("checkpoint preflight transaction");
        assert_eq!(
            checkpoint_successor_preflight_in_transaction(
                &transaction,
                &fixture.expected,
                &fixture.checkpoint,
                &CampaignLimits::default(),
                3_100,
            )
            .expect("checkpoint preflight"),
            CheckpointSuccessorPreflight::Available
        );
        transaction.commit().expect("checkpoint preflight commit");
    }

    #[test]
    fn checkpoint_limits_defer_parallel_capacity_before_and_after_confirmation() {
        let fixture = source_authority_fixture();
        let mut limits = CampaignLimits::default();
        limits.max_parallel_experiments = 1;
        limits.max_new_experiments_per_24h = 10;
        limits.max_agent_runs_per_hour = 10;
        limits.max_same_spec_retries = 10;
        limits.max_live_repairs = 10;
        let baseline = checkpoint_limit_snapshot(&fixture);
        assert_eq!(
            checkpoint_preflight(&fixture, &limits, 3_100),
            CheckpointSuccessorPreflight::Available
        );
        assert_eq!(checkpoint_limit_snapshot(&fixture), baseline);

        insert_limit_experiment(
            &fixture,
            "parallel",
            vec!["python".to_owned(), "other.py".to_owned()],
            ".",
            None,
            3_100,
        );
        let saturated = checkpoint_limit_snapshot(&fixture);
        assert_eq!(
            checkpoint_preflight(&fixture, &limits, 3_100),
            CheckpointSuccessorPreflight::Deferred
        );
        assert_eq!(checkpoint_limit_snapshot(&fixture), saturated);

        assert_checkpoint_limit_deferred_after_intent(
            source_authority_fixture(),
            &limits,
            |fixture| {
                insert_limit_experiment(
                    fixture,
                    "parallel-post-intent",
                    vec!["python".to_owned(), "other.py".to_owned()],
                    ".",
                    None,
                    3_100,
                );
            },
            "parallel",
        );
    }

    #[test]
    fn checkpoint_limits_defer_rolling_experiment_budget_before_and_after_confirmation() {
        let fixture = source_authority_fixture();
        let mut limits = CampaignLimits::default();
        limits.max_parallel_experiments = 10;
        limits.max_new_experiments_per_24h = 2;
        limits.max_agent_runs_per_hour = 10;
        limits.max_same_spec_retries = 10;
        limits.max_live_repairs = 10;
        assert_eq!(
            checkpoint_preflight(&fixture, &limits, 3_100),
            CheckpointSuccessorPreflight::Available
        );
        insert_limit_experiment(
            &fixture,
            "experiment-budget",
            vec!["python".to_owned(), "other.py".to_owned()],
            ".",
            None,
            3_100,
        );
        let saturated = checkpoint_limit_snapshot(&fixture);
        assert_eq!(
            checkpoint_preflight(&fixture, &limits, 3_100),
            CheckpointSuccessorPreflight::Deferred
        );
        assert_eq!(checkpoint_limit_snapshot(&fixture), saturated);

        assert_checkpoint_limit_deferred_after_intent(
            source_authority_fixture(),
            &limits,
            |fixture| {
                insert_limit_experiment(
                    fixture,
                    "experiment-budget-post-intent",
                    vec!["python".to_owned(), "other.py".to_owned()],
                    ".",
                    None,
                    3_100,
                );
            },
            "experiment budget",
        );
    }

    #[test]
    fn checkpoint_limits_defer_rolling_agent_budget_before_and_after_confirmation() {
        let fixture = source_authority_fixture();
        let mut limits = CampaignLimits::default();
        limits.max_parallel_experiments = 10;
        limits.max_new_experiments_per_24h = 10;
        limits.max_agent_runs_per_hour = 1;
        limits.max_same_spec_retries = 10;
        limits.max_live_repairs = 10;
        assert_eq!(
            checkpoint_preflight(&fixture, &limits, 3_100),
            CheckpointSuccessorPreflight::Available
        );
        insert_limit_agent_reservation(&fixture, "agent-budget", 3_100);
        let saturated = checkpoint_limit_snapshot(&fixture);
        assert_eq!(
            checkpoint_preflight(&fixture, &limits, 3_100),
            CheckpointSuccessorPreflight::Deferred
        );
        assert_eq!(checkpoint_limit_snapshot(&fixture), saturated);

        assert_checkpoint_limit_deferred_after_intent(
            source_authority_fixture(),
            &limits,
            |fixture| insert_limit_agent_reservation(fixture, "agent-budget-post-intent", 3_100),
            "agent budget",
        );
    }

    #[test]
    fn checkpoint_limits_defer_same_spec_retries_at_exact_greater_boundary() {
        let fixture = source_authority_fixture();
        let mut limits = CampaignLimits::default();
        limits.max_parallel_experiments = 10;
        limits.max_new_experiments_per_24h = 10;
        limits.max_agent_runs_per_hour = 10;
        limits.max_same_spec_retries = 1;
        limits.max_live_repairs = 10;
        assert_eq!(
            checkpoint_preflight(&fixture, &limits, 3_100),
            CheckpointSuccessorPreflight::Available
        );
        insert_limit_experiment(
            &fixture,
            "same-spec",
            vec!["python".to_owned(), "train.py".to_owned()],
            ".",
            None,
            3_100,
        );
        let saturated = checkpoint_limit_snapshot(&fixture);
        assert_eq!(
            checkpoint_preflight(&fixture, &limits, 3_100),
            CheckpointSuccessorPreflight::Deferred
        );
        assert_eq!(checkpoint_limit_snapshot(&fixture), saturated);

        assert_checkpoint_limit_deferred_after_intent(
            source_authority_fixture(),
            &limits,
            |fixture| {
                insert_limit_experiment(
                    fixture,
                    "same-spec-post-intent",
                    vec!["python".to_owned(), "train.py".to_owned()],
                    ".",
                    None,
                    3_100,
                );
            },
            "same spec",
        );
    }

    #[test]
    fn checkpoint_limits_defer_live_repairs_at_exact_greater_or_equal_boundary() {
        let fixture = source_authority_fixture();
        let mut limits = CampaignLimits::default();
        limits.max_parallel_experiments = 10;
        limits.max_new_experiments_per_24h = 10;
        limits.max_agent_runs_per_hour = 10;
        limits.max_same_spec_retries = 10;
        limits.max_live_repairs = 1;
        assert_eq!(
            checkpoint_preflight(&fixture, &limits, 3_100),
            CheckpointSuccessorPreflight::Available
        );
        insert_limit_experiment(
            &fixture,
            "live-repair",
            vec!["python".to_owned(), "repair.py".to_owned()],
            ".",
            Some(&fixture.experiment_id),
            3_100,
        );
        let saturated = checkpoint_limit_snapshot(&fixture);
        assert_eq!(
            checkpoint_preflight(&fixture, &limits, 3_100),
            CheckpointSuccessorPreflight::Deferred
        );
        assert_eq!(checkpoint_limit_snapshot(&fixture), saturated);

        assert_checkpoint_limit_deferred_after_intent(
            source_authority_fixture(),
            &limits,
            |fixture| {
                insert_limit_experiment(
                    fixture,
                    "live-repair-post-intent",
                    vec!["python".to_owned(), "repair.py".to_owned()],
                    ".",
                    Some(&fixture.experiment_id),
                    3_100,
                );
            },
            "live repair",
        );
    }

    #[test]
    fn checkpoint_retry_history_counts_completed_same_spec_and_distinct_learning_specs() {
        let fixture = checkpoint_dispatch_fixture();
        assert_checkpoint_history_count(&fixture, 2);
        rewrite_checkpoint_retained_path(
            &fixture,
            "/private/state/research-checkpoints/source-authority-campaign/source-authority-review/alternate",
        );
        insert_limit_experiment(
            &fixture.fixture,
            "different-learning-argv",
            vec!["python".to_owned(), "different.py".to_owned()],
            ".",
            None,
            3_104,
        );
        insert_limit_experiment(
            &fixture.fixture,
            "different-learning-cwd",
            vec!["python".to_owned(), "train.py".to_owned()],
            "different",
            None,
            3_104,
        );
        assert_checkpoint_history_count(&fixture, 2);
        settle_checkpoint_history(&fixture);
        assert_checkpoint_history_count(&fixture, 2);
    }

    #[test]
    fn checkpoint_retry_history_rejects_malformed_oversized_and_duplicate_links() {
        for (label, raw) in [
            ("malformed", "{".to_owned()),
            (
                "oversized",
                "x".repeat(crate::research_checkpoint::MAX_PREPARED_CHECKPOINT_BYTES + 1),
            ),
        ] {
            let fixture = checkpoint_dispatch_fixture();
            settle_checkpoint_history(&fixture);
            assert_checkpoint_history_count(&fixture, 2);
            fixture
                .fixture
                .db
                .connect()
                .expect("history corrupt connection")
                .execute(
                    "UPDATE research_reviews SET checkpoint_json = ?1 WHERE review_id = ?2",
                    rusqlite::params![raw, fixture.fixture.review_id],
                )
                .expect("history corrupt row");
            let mut connection = fixture
                .fixture
                .db
                .connect()
                .expect("history corrupt count connection");
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .expect("history corrupt count transaction");
            assert!(
                checkpoint_same_spec_count(&transaction, &fixture.fixture.checkpoint).is_err(),
                "{label} history must fail closed"
            );
            transaction.commit().expect("history corrupt count commit");
        }

        let fixture = checkpoint_dispatch_fixture();
        settle_checkpoint_history(&fixture);
        assert_checkpoint_history_count(&fixture, 2);
        fixture
            .fixture
            .db
            .connect()
            .expect("history duplicate connection")
            .execute(
                "INSERT INTO research_reviews (
                    review_id, campaign_id, experiment_id, task_signature, attempt,
                    state, operation_stage, agent_run_id, context_json, context_digest,
                    response_json, termination_request_id, successor_experiment_id,
                    evidence_schema_version, session_generation, event_id, not_before,
                    notes_json, failure_code, decision_cycle_id, checkpoint_json,
                    created_at, started_at, finished_at, updated_at
                 )
                 SELECT 'duplicate-history-review', campaign_id, experiment_id,
                    task_signature, attempt, 'completed', NULL, agent_run_id,
                    context_json, context_digest, response_json, termination_request_id,
                    successor_experiment_id, evidence_schema_version, session_generation,
                    event_id, not_before, notes_json, NULL, decision_cycle_id,
                    checkpoint_json, created_at, started_at, 4_000, 4_000
                 FROM research_reviews WHERE review_id = ?1",
                [&fixture.fixture.review_id],
            )
            .expect("history duplicate row");
        let mut connection = fixture
            .fixture
            .db
            .connect()
            .expect("history duplicate count connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("history duplicate count transaction");
        assert!(checkpoint_same_spec_count(&transaction, &fixture.fixture.checkpoint).is_err());
        transaction.commit().expect("history duplicate count commit");
    }

    #[test]
    fn checkpoint_retry_history_rejects_missing_linked_checkpoint() {
        let fixture = checkpoint_dispatch_fixture();
        settle_checkpoint_history(&fixture);
        assert_checkpoint_history_count(&fixture, 2);
        fixture
            .fixture
            .db
            .connect()
            .expect("history missing checkpoint connection")
            .execute(
                "UPDATE research_reviews SET checkpoint_json = NULL WHERE review_id = ?1",
                [&fixture.fixture.review_id],
            )
            .expect("history missing checkpoint row");
        let mut connection = fixture
            .fixture
            .db
            .connect()
            .expect("history missing checkpoint count connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("history missing checkpoint count transaction");
        assert!(
            checkpoint_same_spec_count(&transaction, &fixture.fixture.checkpoint).is_err(),
            "missing linked checkpoint must fail closed"
        );
        transaction
            .commit()
            .expect("history missing checkpoint count commit");
    }

    #[test]
    fn checkpoint_retry_history_rejects_foreign_campaign_link_instead_of_disappearing() {
        let fixture = checkpoint_dispatch_fixture();
        settle_checkpoint_history(&fixture);
        assert_checkpoint_history_count(&fixture, 2);
        let connection = fixture
            .fixture
            .db
            .connect()
            .expect("history foreign setup connection");
        insert_foreign_campaign(&fixture, &connection);
        connection
            .execute(
                "UPDATE research_reviews
                 SET campaign_id = 'checkpoint-foreign-campaign'
                 WHERE review_id = ?1",
                [&fixture.fixture.review_id],
            )
            .expect("history foreign review campaign");
        drop(connection);
        let mut connection = fixture
            .fixture
            .db
            .connect()
            .expect("history foreign count connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("history foreign count transaction");
        assert!(
            checkpoint_same_spec_count(&transaction, &fixture.fixture.checkpoint).is_err(),
            "foreign linked history must fail closed"
        );
        transaction.commit().expect("history foreign count commit");
    }

    #[test]
    fn checkpoint_retry_insertion_attempt_stays_stable_after_later_same_spec_admission() {
        let (fixture, _encoded, owner) = checkpoint_confirmed_fixture();
        let limits = CampaignLimits::default();
        let mut connection = fixture.db.connect().expect("attempt admission connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("attempt admission transaction");
        let successor = match super::super::campaigns::accept_checkpoint_successor_in_transaction(
            &transaction,
            &owner,
            &limits,
            3_101,
        )
        .expect("attempt admission")
        {
            CheckpointSuccessorAdmission::Ready(intent) => intent,
            other => panic!("unexpected attempt admission: {other:?}"),
        };
        let inserted_attempt = successor.experiment.attempt;
        transaction.commit().expect("attempt admission commit");
        assert_eq!(inserted_attempt, 1);

        insert_limit_experiment(
            &fixture,
            "later-same-spec",
            vec!["python".to_owned(), "train.py".to_owned()],
            ".",
            None,
            3_102,
        );
        let mut connection = fixture.db.connect().expect("attempt replay connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("attempt replay transaction");
        let replay_owner = match research_ownership_in_transaction(
            &transaction,
            &fixture.project_id,
            &fixture.campaign_id,
            &fixture.experiment_id,
        )
        .expect("attempt replay owner")
        {
            ResearchOwnership::Open(Some(owner)) => owner,
            other => panic!("unexpected attempt replay owner: {other:?}"),
        };
        let replay = match super::super::campaigns::accept_checkpoint_successor_in_transaction(
            &transaction,
            &replay_owner,
            &limits,
            3_103,
        )
        .expect("attempt replay")
        {
            CheckpointSuccessorAdmission::Ready(intent) => intent,
            other => panic!("unexpected attempt replay result: {other:?}"),
        };
        assert_eq!(replay.experiment.attempt, inserted_attempt);
        transaction.commit().expect("attempt replay commit");
    }

    #[test]
    fn checkpoint_intent_binds_checkpoint_and_termination_atomically() {
        let fixture = source_authority_fixture();
        let incident = Incident {
            incident_id: 71,
            project_id: fixture.project_id.clone(),
            kind: "research_checkpoint".to_owned(),
            task_key: Some(fixture.expected.owner.source_experiment_id.clone()),
            fingerprint: "checkpoint-fingerprint".to_owned(),
            status: IncidentStatus::Open,
            first_seen_at: 3_100,
            last_seen_at: 3_100,
            acknowledged_at: None,
            resolved_at: None,
        };
        let request = TerminationRequest {
            request_id: 72,
            incident_id: incident.incident_id,
            project_id: fixture.project_id.clone(),
            task_signature: fixture.expected.raw_task_signature.clone(),
            reason: format!("research_action:{}:checkpoint", fixture.review_id),
            status: TerminationRequestStatus::Confirmed,
            requested_at: 3_100,
            dispatch_lease_until: None,
            grace_until: None,
            confirmed_at: Some(3_101),
            last_error: None,
        };
        let connection = fixture.db.connect().expect("checkpoint intent setup connection");
        connection
            .execute(
                "INSERT INTO incidents (
                    incident_id, project_id, kind, task_key, fingerprint, status,
                    first_seen_at, last_seen_at, acknowledged_at, resolved_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, NULL, NULL)",
                rusqlite::params![
                    incident.incident_id,
                    incident.project_id,
                    incident.kind,
                    incident.task_key,
                    incident.fingerprint,
                    incident.status,
                    incident.first_seen_at,
                ],
            )
            .expect("checkpoint incident row");
        connection
            .execute(
                "INSERT INTO termination_requests (
                    request_id, incident_id, project_id, task_signature, reason,
                    status, requested_at, dispatch_lease_until, grace_until,
                    confirmed_at, last_error
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, NULL)",
                rusqlite::params![
                    request.request_id,
                    request.incident_id,
                    request.project_id,
                    request.task_signature,
                    request.reason,
                    request.status,
                    request.requested_at,
                    request.confirmed_at,
                ],
            )
            .expect("checkpoint termination row");
        let encoded = crate::research_checkpoint::serialize_prepared_checkpoint(&fixture.checkpoint)
            .expect("checkpoint intent encoding");
        let mut connection = connection;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("checkpoint intent transaction");
        assert!(bind_checkpoint_termination_intent_in_transaction(
            &transaction,
            &fixture.expected,
            &encoded,
            &incident,
            &request,
            3_102,
        )
        .expect("checkpoint intent bind"));
        transaction.commit().expect("checkpoint intent commit");
        let stored: (String, String, i64) = connection
            .query_row(
                "SELECT operation_stage, checkpoint_json, termination_request_id
                 FROM research_reviews WHERE review_id = ?1",
                [&fixture.review_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("checkpoint intent persisted row");
        assert_eq!(stored.0, "intent");
        assert_eq!(stored.1, encoded);
        assert_eq!(stored.2, request.request_id);
    }

    #[test]
    fn checkpoint_orphan_block_is_review_only_and_exact() {
        let fixture = source_authority_fixture();
        let mut connection = fixture.db.connect().expect("checkpoint orphan connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("checkpoint orphan transaction");
        assert!(block_checkpoint_orphan_in_transaction(
            &transaction,
            &fixture.expected,
            3_102,
        )
        .expect("checkpoint orphan block"));
        let stored: (String, Option<String>, Option<String>) = transaction
            .query_row(
                "SELECT state, failure_code, checkpoint_json
                 FROM research_reviews WHERE review_id = ?1",
                [&fixture.review_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("checkpoint orphan review");
        assert_eq!(
            stored,
            (
                "blocked".to_owned(),
                Some("research_checkpoint_orphaned".to_owned()),
                None,
            )
        );
        let campaign_research_blocked: Option<String> = transaction
            .query_row(
                "SELECT blocked_reason FROM campaign_research WHERE campaign_id = ?1",
                [&fixture.campaign_id],
                |row| row.get(0),
            )
            .expect("checkpoint orphan campaign research");
        assert!(campaign_research_blocked.is_none());
        transaction.commit().expect("checkpoint orphan commit");
    }

    #[test]
    fn invalid_checkpoint_review_block_preserves_corrupt_bytes() {
        let fixture = source_authority_fixture();
        let corrupt = b"checkpoint-corrupt-bytes".to_vec();
        fixture
            .db
            .connect()
            .expect("invalid checkpoint setup connection")
            .execute(
                "UPDATE research_reviews
                 SET operation_stage = 'successor_reserved',
                     successor_experiment_id = ?2,
                     checkpoint_json = ?1
                 WHERE review_id = ?3",
                rusqlite::params![corrupt.clone(), fixture.experiment_id, fixture.review_id],
            )
            .expect("invalid checkpoint setup");
        let review = ResearchRepository::new(&fixture.db)
            .open_action_reviews(1)
            .expect("invalid checkpoint open review")
            .into_iter()
            .next()
            .expect("invalid checkpoint review");
        let mut connection = fixture.db.connect().expect("invalid checkpoint connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("invalid checkpoint transaction");
        assert!(block_invalid_checkpoint_review_in_transaction(
            &transaction,
            &review,
            3_102,
        )
        .expect("invalid checkpoint block"));
        transaction.commit().expect("invalid checkpoint commit");
        let stored: (String, String, Vec<u8>) = fixture
            .db
            .connect()
            .expect("invalid checkpoint final connection")
            .query_row(
                "SELECT state, typeof(checkpoint_json), CAST(checkpoint_json AS BLOB)
                 FROM research_reviews WHERE review_id = ?1",
                [&fixture.review_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("invalid checkpoint final review");
        assert_eq!(stored, ("blocked".to_owned(), "blob".to_owned(), corrupt));
    }

    #[test]
    fn invalid_checkpoint_review_cas_rejects_stale_value_storage_and_identity() {
        let fixture = source_authority_fixture();
        let corrupt = "corrupt".to_owned();
        fixture
            .db
            .connect()
            .expect("bounded checkpoint setup connection")
            .execute(
                "UPDATE research_reviews
                 SET operation_stage = 'successor_reserved',
                     successor_experiment_id = ?2,
                     checkpoint_json = ?1
                 WHERE review_id = ?3",
                rusqlite::params![corrupt, fixture.experiment_id, fixture.review_id],
            )
            .expect("bounded checkpoint setup");
        let review = ResearchRepository::new(&fixture.db)
            .open_action_reviews(1)
            .expect("bounded checkpoint review list")
            .into_iter()
            .next()
            .expect("bounded checkpoint review");
        assert_eq!(review.checkpoint_json.as_deref(), Some("corrupt"));
        let mut connection = fixture.db.connect().expect("bounded checkpoint CAS connection");
        connection
            .execute(
                "UPDATE research_reviews SET checkpoint_json = 'changed'
                 WHERE review_id = ?1",
                [&fixture.review_id],
            )
            .expect("bounded checkpoint stale value");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("bounded checkpoint CAS transaction");
        assert!(!block_invalid_checkpoint_review_in_transaction(
            &transaction,
            &review,
            3_102,
        )
        .expect("bounded checkpoint stale CAS"));
        let stored: (String, String) = transaction
            .query_row(
                "SELECT state, checkpoint_json FROM research_reviews WHERE review_id = ?1",
                [&fixture.review_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("bounded checkpoint stale result");
        assert_eq!(stored, ("ready".to_owned(), "changed".to_owned()));
        transaction.commit().expect("bounded checkpoint stale commit");

        let fixture = source_authority_fixture();
        let corrupt = vec![1_u8, 2, 3];
        fixture
            .db
            .connect()
            .expect("invalid storage setup connection")
            .execute(
                "UPDATE research_reviews
                 SET operation_stage = 'successor_reserved',
                     successor_experiment_id = ?2,
                     checkpoint_json = ?1
                 WHERE review_id = ?3",
                rusqlite::params![corrupt, fixture.experiment_id, fixture.review_id],
            )
            .expect("invalid storage setup");
        let review = ResearchRepository::new(&fixture.db)
            .open_action_reviews(1)
            .expect("invalid storage review list")
            .into_iter()
            .next()
            .expect("invalid storage review");
        assert_eq!(
            review.checkpoint_json_state,
            CheckpointJsonState::Invalid {
                storage_class: CheckpointSqliteStorageClass::Blob,
                byte_len: Some(3),
            }
        );
        let mut connection = fixture.db.connect().expect("invalid storage CAS connection");
        connection
            .execute(
                "UPDATE research_reviews SET checkpoint_json = zeroblob(4)
                 WHERE review_id = ?1",
                [&fixture.review_id],
            )
            .expect("invalid storage stale value");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("invalid storage CAS transaction");
        assert!(!block_invalid_checkpoint_review_in_transaction(
            &transaction,
            &review,
            3_102,
        )
        .expect("invalid storage stale CAS"));
        let stored: (String, String, i64) = transaction
            .query_row(
                "SELECT state, typeof(checkpoint_json), length(CAST(checkpoint_json AS BLOB))
                 FROM research_reviews WHERE review_id = ?1",
                [&fixture.review_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("invalid storage stale result");
        assert_eq!(stored, ("ready".to_owned(), "blob".to_owned(), 4));
        transaction.commit().expect("invalid storage stale commit");

        let fixture = source_authority_fixture();
        fixture
            .db
            .connect()
            .expect("identity setup connection")
            .execute(
                "UPDATE research_reviews
                 SET operation_stage = 'successor_reserved',
                     successor_experiment_id = ?2,
                     checkpoint_json = X'0102'
                 WHERE review_id = ?3",
                rusqlite::params![fixture.experiment_id, fixture.experiment_id, fixture.review_id],
            )
            .expect("identity setup");
        let review = ResearchRepository::new(&fixture.db)
            .open_action_reviews(1)
            .expect("identity review list")
            .into_iter()
            .next()
            .expect("identity review");
        let mut connection = fixture.db.connect().expect("identity CAS connection");
        connection
            .execute(
                "UPDATE research_reviews SET session_generation = session_generation + 1
                 WHERE review_id = ?1",
                [&fixture.review_id],
            )
            .expect("identity stale generation");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("identity CAS transaction");
        assert!(!block_invalid_checkpoint_review_in_transaction(
            &transaction,
            &review,
            3_102,
        )
        .expect("identity stale CAS"));
        let state: String = transaction
            .query_row(
                "SELECT state FROM research_reviews WHERE review_id = ?1",
                [&fixture.review_id],
                |row| row.get(0),
            )
            .expect("identity stale result");
        assert_eq!(state, "ready");
        transaction.commit().expect("identity stale commit");
    }

    #[test]
    fn checkpoint_orphan_cas_rejects_changed_action_authority_without_mutation() {
        let fixture = source_authority_fixture();
        let mut connection = fixture.db.connect().expect("orphan stale connection");
        connection
            .execute(
                "UPDATE research_reviews SET notes_json = '{\"stale\":true}'
                 WHERE review_id = ?1",
                [&fixture.review_id],
            )
            .expect("orphan stale notes");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("orphan stale transaction");
        assert!(!block_checkpoint_orphan_in_transaction(
            &transaction,
            &fixture.expected,
            3_102,
        )
        .expect("orphan stale CAS"));
        let stored: (String, String) = transaction
            .query_row(
                "SELECT state, notes_json FROM research_reviews WHERE review_id = ?1",
                [&fixture.review_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("orphan stale result");
        assert_eq!(stored, ("ready".to_owned(), "{\"stale\":true}".to_owned()));
        transaction.commit().expect("orphan stale commit");
    }

    #[test]
    fn checkpoint_dispatch_partial_successor_graph_blocks_only_review() {
        let fixture = checkpoint_dispatch_fixture();
        fixture
            .fixture
            .db
            .connect()
            .expect("partial graph connection")
            .execute(
                "DELETE FROM budget_reservations
                 WHERE experiment_id = ?1 AND dimension = 'experiment'",
                [&fixture.authority.successor_experiment_id],
            )
            .expect("partial graph reservation removal");
        let selection = ResearchRepository::new(&fixture.fixture.db)
            .checkpoint_dispatch_authority(
                &fixture.fixture.project_id,
                &fixture.authority.successor_experiment_id,
                3_104,
            )
            .expect("partial graph dispatch classification");
        assert!(matches!(selection, CheckpointDispatchSelection::Blocked));
        let stored: (String, String, i64) = fixture
            .fixture
            .db
            .connect()
            .expect("partial graph result connection")
            .query_row(
                "SELECT state, failure_code,
                        (SELECT COUNT(*) FROM budget_reservations
                         WHERE experiment_id = ?2 AND dimension = 'experiment')
                 FROM research_reviews WHERE review_id = ?1",
                rusqlite::params![fixture.fixture.review_id, fixture.authority.successor_experiment_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("partial graph result");
        assert_eq!(
            stored,
            (
                "blocked".to_owned(),
                "research_checkpoint_authority_corrupt".to_owned(),
                0,
            )
        );
    }

    #[test]
    fn checkpoint_dispatch_duplicate_global_reservation_blocks_only_review() {
        let fixture = checkpoint_dispatch_fixture();
        let connection = fixture
            .fixture
            .db
            .connect()
            .expect("duplicate reservation connection");
        insert_foreign_campaign(&fixture, &connection);
        connection
            .execute(
                "INSERT INTO budget_reservations (
                    reservation_id, campaign_id, experiment_id, dimension,
                    subject_key, status, window_started_at, window_ends_at,
                    created_at, updated_at
                 ) VALUES (?1, 'checkpoint-foreign-campaign',
                           ?2, 'experiment', ?2,
                           'reserved', 3_102, 3_202, 3_102, 3_102)",
                rusqlite::params![
                    format!("duplicate:{}", fixture.authority.successor_experiment_id),
                    fixture.authority.successor_experiment_id,
                ],
            )
            .expect("duplicate global reservation");
        drop(connection);
        let selection = ResearchRepository::new(&fixture.fixture.db)
            .checkpoint_dispatch_authority(
                &fixture.fixture.project_id,
                &fixture.authority.successor_experiment_id,
                3_104,
            )
            .expect("duplicate graph dispatch classification");
        assert!(matches!(selection, CheckpointDispatchSelection::Blocked));
        let state: String = fixture
            .fixture
            .db
            .connect()
            .expect("duplicate graph result connection")
            .query_row(
                "SELECT state FROM research_reviews WHERE review_id = ?1",
                [&fixture.fixture.review_id],
                |row| row.get(0),
            )
            .expect("duplicate graph result");
        assert_eq!(state, "blocked");
    }

    #[test]
    fn checkpoint_dispatch_ignores_malformed_foreign_native_owner() {
        let fixture = checkpoint_dispatch_fixture();
        let connection = fixture
            .fixture
            .db
            .connect()
            .expect("foreign native owner connection");
        insert_foreign_campaign(&fixture, &connection);
        connection
            .execute(
                "INSERT INTO events (
                    event_id, project_id, kind, dedup_key, payload_json, status,
                    attempts, not_before, lease_until, created_at,
                    completed_at, last_error
                 ) VALUES (?1, 'checkpoint-foreign-project', 'operator_wake',
                           'foreign-native-owner', '{}', 'pending', 0, 900, NULL,
                           900, NULL, NULL)",
                [99_001_i64],
            )
            .expect("foreign native owner event");
        connection
            .execute(
                "INSERT INTO agent_runs (
                    run_id, project_id, primary_event_id, pid, status, started_at,
                    finished_at, exit_code, log_path, last_error, launch_gate_state,
                    context_mode, context_session_id, context_lineage_json,
                    execution_kind, executable_path, executable_identity,
                    policy_code, failure_stage
                 ) VALUES (99_001, 'checkpoint-foreign-project', 99_001, NULL, 1,
                           900, NULL, NULL, '/tmp/foreign-native-owner.log', NULL,
                           'released', 'fresh', NULL, '[]', 'campaign_research',
                           '/bin/sh', 'malformed', NULL, NULL)",
                [],
            )
            .expect("foreign malformed native owner");
        drop(connection);

        let selection = ResearchRepository::new(&fixture.fixture.db)
            .checkpoint_dispatch_authority(
                &fixture.fixture.project_id,
                &fixture.authority.successor_experiment_id,
                3_104,
            )
            .expect("target dispatch must ignore foreign malformed owner");
        assert!(matches!(selection, CheckpointDispatchSelection::Ready(_)));
    }

    #[test]
    fn checkpoint_dispatch_claim_cardinality_and_list_before_block_are_fail_closed() {
        let fixture = checkpoint_dispatch_fixture();
        fixture
            .fixture
            .db
            .connect()
            .expect("zero claim connection")
            .execute(
                "UPDATE research_reviews
                 SET checkpoint_json = NULL, operation_stage = NULL,
                     successor_experiment_id = NULL
                 WHERE review_id = ?1",
                [&fixture.fixture.review_id],
            )
            .expect("zero claim mutation");
        assert!(matches!(
            ResearchRepository::new(&fixture.fixture.db)
                .checkpoint_dispatch_authority(
                    &fixture.fixture.project_id,
                    &fixture.authority.successor_experiment_id,
                    3_104,
                )
                .expect("zero claim selection"),
            CheckpointDispatchSelection::NotCheckpoint
        ));

        let fixture = checkpoint_dispatch_fixture();
        fixture
            .fixture
            .db
            .connect()
            .expect("missing marker connection")
            .execute(
                "UPDATE research_reviews SET checkpoint_json = NULL
                 WHERE review_id = ?1",
                [&fixture.fixture.review_id],
            )
            .expect("missing marker mutation");
        assert!(matches!(
            ResearchRepository::new(&fixture.fixture.db)
                .checkpoint_dispatch_authority(
                    &fixture.fixture.project_id,
                    &fixture.authority.successor_experiment_id,
                    3_104,
                )
                .expect("missing marker selection"),
            CheckpointDispatchSelection::Blocked
        ));
        assert!(ExperimentRepository::new(&fixture.fixture.db)
            .begin_submitting_or_defer(&fixture.authority.successor_experiment_id, 3_105)
            .is_err());
        let missing_marker_state: String = fixture
            .fixture
            .db
            .connect()
            .expect("missing marker result connection")
            .query_row(
                "SELECT state FROM research_reviews WHERE review_id = ?1",
                [&fixture.fixture.review_id],
                |row| row.get(0),
            )
            .expect("missing marker result");
        assert_eq!(missing_marker_state, "blocked");

        let fixture = checkpoint_dispatch_fixture();
        let connection = fixture
            .fixture
            .db
            .connect()
            .expect("missing marker hybrid connection");
        connection
            .execute(
                "INSERT INTO decision_cycles (
                    cycle_id, campaign_id, source_experiment_id, state,
                    next_wake_at, consecutive_failed_attempts, last_decision_kind,
                    last_failure_code, last_failure_summary, created_at, updated_at,
                    source_terminal_at
                 ) VALUES (?1, ?2, ?3, 'pending', NULL, 0, NULL, NULL, NULL, 3_100, 3_100, 1)",
                rusqlite::params![
                    "checkpoint-hybrid-cycle",
                    fixture.fixture.campaign_id,
                    fixture.fixture.experiment_id,
                ],
            )
            .expect("missing marker hybrid decision cycle");
        connection
            .execute(
                "UPDATE research_reviews
                 SET checkpoint_json = NULL, decision_cycle_id = ?1
                 WHERE review_id = ?2",
                rusqlite::params!["checkpoint-hybrid-cycle", fixture.fixture.review_id],
            )
            .expect("missing marker hybrid mutation");
        assert!(matches!(
            ResearchRepository::new(&fixture.fixture.db)
                .checkpoint_dispatch_authority(
                    &fixture.fixture.project_id,
                    &fixture.authority.successor_experiment_id,
                    3_104,
                )
                .expect("missing marker hybrid selection"),
            CheckpointDispatchSelection::Blocked
        ));

        let fixture = checkpoint_dispatch_fixture();
        fixture
            .fixture
            .db
            .connect()
            .expect("completed missing marker connection")
            .execute(
                "UPDATE research_reviews
                 SET state = 'completed', operation_stage = NULL,
                     checkpoint_json = NULL, decision_cycle_id = NULL
                 WHERE review_id = ?1",
                [&fixture.fixture.review_id],
            )
            .expect("completed missing marker mutation");
        assert!(ResearchRepository::new(&fixture.fixture.db)
            .checkpoint_dispatch_authority(
                &fixture.fixture.project_id,
                &fixture.authority.successor_experiment_id,
                3_104,
            )
            .is_err());

        let fixture = checkpoint_dispatch_fixture();
        let one = ResearchRepository::new(&fixture.fixture.db)
            .checkpoint_dispatch_authority(
                &fixture.fixture.project_id,
                &fixture.authority.successor_experiment_id,
                3_104,
            )
            .expect("one claim selection");
        assert!(matches!(one, CheckpointDispatchSelection::Ready(_)));

        let fixture = checkpoint_dispatch_fixture();
        let connection = fixture
            .fixture
            .db
            .connect()
            .expect("two claim connection");
        connection
            .execute(
                "INSERT INTO research_reviews (
                    review_id, campaign_id, experiment_id, task_signature, attempt,
                    state, operation_stage, agent_run_id, context_json, context_digest,
                    response_json, termination_request_id, successor_experiment_id,
                    evidence_schema_version, session_generation, event_id, not_before,
                    notes_json, failure_code, decision_cycle_id, checkpoint_json,
                    created_at, started_at, finished_at, updated_at
                 )
                 SELECT 'checkpoint-duplicate-claim', campaign_id, experiment_id,
                        task_signature, attempt, 'completed', NULL, agent_run_id,
                        context_json, context_digest, response_json,
                        termination_request_id, successor_experiment_id,
                        evidence_schema_version, session_generation, event_id,
                        not_before, notes_json, failure_code, decision_cycle_id,
                        checkpoint_json, created_at, started_at, finished_at, updated_at
                 FROM research_reviews WHERE review_id = ?1",
                [&fixture.fixture.review_id],
            )
            .expect("two claims mutation");
        drop(connection);
        let before: String = fixture
            .fixture
            .db
            .connect()
            .expect("two claims before connection")
            .query_row(
                "SELECT state FROM research_reviews WHERE review_id = ?1",
                [&fixture.fixture.review_id],
                |row| row.get(0),
            )
            .expect("two claims before state");
        assert!(ResearchRepository::new(&fixture.fixture.db)
            .checkpoint_dispatch_authority(
                &fixture.fixture.project_id,
                &fixture.authority.successor_experiment_id,
                3_104,
            )
            .is_err());
        let after: String = fixture
            .fixture
            .db
            .connect()
            .expect("two claims after connection")
            .query_row(
                "SELECT state FROM research_reviews WHERE review_id = ?1",
                [&fixture.fixture.review_id],
                |row| row.get(0),
            )
            .expect("two claims after state");
        assert_eq!(after, before);

        let fixture = checkpoint_dispatch_fixture();
        let connection = fixture
            .fixture
            .db
            .connect()
            .expect("foreign claim connection");
        insert_foreign_campaign(&fixture, &connection);
        connection
            .execute(
                "UPDATE research_reviews SET campaign_id = 'checkpoint-foreign-campaign'
                 WHERE review_id = ?1",
                [&fixture.fixture.review_id],
            )
            .expect("foreign claim mutation");
        drop(connection);
        assert!(ResearchRepository::new(&fixture.fixture.db)
            .checkpoint_dispatch_authority(
                &fixture.fixture.project_id,
                &fixture.authority.successor_experiment_id,
                3_104,
            )
            .is_err());

        let fixture = checkpoint_dispatch_fixture();
        let authority = match ResearchRepository::new(&fixture.fixture.db)
            .checkpoint_dispatch_authority(
                &fixture.fixture.project_id,
                &fixture.authority.successor_experiment_id,
                3_104,
            )
            .expect("list-before-block selection")
        {
            CheckpointDispatchSelection::Ready(authority) => authority,
            other => panic!("unexpected list-before-block selection: {other:?}"),
        };
        let mut connection = fixture.fixture.db.connect().expect("list-before-block mutation");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("list-before-block transaction");
        assert!(block_checkpoint_dispatch_review(
            &transaction,
            &fixture.fixture.review_id,
            &fixture.fixture.campaign_id,
            &fixture.fixture.experiment_id,
            &fixture.fixture.expected.owner.managed_task_signature,
            fixture.fixture.expected.owner.attempt,
            3_105,
        )
        .expect("list-before-block mutation"));
        transaction.commit().expect("list-before-block commit");
        assert!(ExperimentRepository::new(&fixture.fixture.db)
            .begin_checkpoint_submitting_or_defer(&authority, 3_106)
            .is_err());
    }

    #[test]
    fn checkpoint_dispatch_rejects_decision_cycle_hybrid_and_stale_cas() {
        let fixture = checkpoint_dispatch_fixture();
        let connection = fixture
            .fixture
            .db
            .connect()
            .expect("checkpoint cycle hybrid connection");
        attach_checkpoint_decision_cycle(&fixture, &connection);
        drop(connection);
        let before = checkpoint_graph_snapshot(&fixture);
        assert!(matches!(
            ResearchRepository::new(&fixture.fixture.db)
                .checkpoint_dispatch_authority(
                    &fixture.fixture.project_id,
                    &fixture.authority.successor_experiment_id,
                    3_104,
                )
                .expect("checkpoint cycle hybrid dispatch"),
            CheckpointDispatchSelection::Blocked
        ));
        let after = checkpoint_graph_snapshot(&fixture);
        assert_eq!(after.proposal, before.proposal);
        assert_eq!(after.submission, before.submission);
        assert_eq!(after.experiment, before.experiment);
        assert_eq!(after.reservation, before.reservation);
        assert_eq!(after.termination, before.termination);
        assert_eq!(after.resource_counts, before.resource_counts);
        assert_eq!(after.review[0], rusqlite::types::Value::Text("blocked".to_owned()));
        assert_eq!(after.review[3], before.review[3]);
        assert_eq!(after.review[5], before.review[5]);

        let fixture = checkpoint_dispatch_fixture();
        let authority = match ResearchRepository::new(&fixture.fixture.db)
            .checkpoint_dispatch_authority(
                &fixture.fixture.project_id,
                &fixture.authority.successor_experiment_id,
                3_104,
            )
            .expect("checkpoint cycle stale authority selection")
        {
            CheckpointDispatchSelection::Ready(authority) => authority,
            other => panic!("unexpected checkpoint cycle authority: {other:?}"),
        };
        let connection = fixture
            .fixture
            .db
            .connect()
            .expect("checkpoint cycle stale connection");
        attach_checkpoint_decision_cycle(&fixture, &connection);
        drop(connection);
        let before = checkpoint_graph_snapshot(&fixture);
        assert!(ExperimentRepository::new(&fixture.fixture.db)
            .begin_checkpoint_submitting_or_defer(&authority, 3_105)
            .is_err());
        let after = checkpoint_graph_snapshot(&fixture);
        assert_eq!(after.proposal, before.proposal);
        assert_eq!(after.submission, before.submission);
        assert_eq!(after.experiment, before.experiment);
        assert_eq!(after.reservation, before.reservation);
        assert_eq!(after.termination, before.termination);
        assert_eq!(after.resource_counts, before.resource_counts);
        assert_eq!(after.review, before.review);

        let fixture = checkpoint_dispatch_fixture();
        let connection = fixture
            .fixture
            .db
            .connect()
            .expect("checkpoint cycle history connection");
        attach_checkpoint_decision_cycle(&fixture, &connection);
        drop(connection);
        let mut connection = fixture.fixture.db.connect().expect("checkpoint cycle history read");
        let transaction = connection
            .transaction()
            .expect("checkpoint cycle history transaction");
        assert!(checkpoint_same_spec_count(&transaction, &fixture.fixture.checkpoint).is_err());
    }

    #[test]
    fn checkpoint_prepared_intent_caller_rollback_removes_rows_after_authority_mutation() {
        let fixture = source_authority_fixture();
        let incident = Incident {
            incident_id: 171,
            project_id: fixture.project_id.clone(),
            kind: "research_checkpoint".to_owned(),
            task_key: Some(fixture.expected.owner.source_experiment_id.clone()),
            fingerprint: "checkpoint-rollback-fingerprint".to_owned(),
            status: IncidentStatus::Open,
            first_seen_at: 3_100,
            last_seen_at: 3_100,
            acknowledged_at: None,
            resolved_at: None,
        };
        let request = TerminationRequest {
            request_id: 172,
            incident_id: incident.incident_id,
            project_id: fixture.project_id.clone(),
            task_signature: fixture.expected.raw_task_signature.clone(),
            reason: format!("research_action:{}:checkpoint", fixture.review_id),
            status: TerminationRequestStatus::Confirmed,
            requested_at: 3_100,
            dispatch_lease_until: None,
            grace_until: None,
            confirmed_at: Some(3_101),
            last_error: None,
        };
        let encoded = crate::research_checkpoint::serialize_prepared_checkpoint(&fixture.checkpoint)
            .expect("checkpoint rollback encoding");
        let mut connection = fixture.db.connect().expect("checkpoint rollback connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("checkpoint rollback transaction");
        transaction
            .execute(
                "INSERT INTO incidents (
                    incident_id, project_id, kind, task_key, fingerprint, status,
                    first_seen_at, last_seen_at, acknowledged_at, resolved_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, NULL, NULL)",
                rusqlite::params![
                    incident.incident_id,
                    incident.project_id,
                    incident.kind,
                    incident.task_key,
                    incident.fingerprint,
                    incident.status,
                    incident.first_seen_at,
                ],
            )
            .expect("checkpoint rollback incident");
        transaction
            .execute(
                "INSERT INTO termination_requests (
                    request_id, incident_id, project_id, task_signature, reason,
                    status, requested_at, dispatch_lease_until, grace_until,
                    confirmed_at, last_error
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, NULL)",
                rusqlite::params![
                    request.request_id,
                    request.incident_id,
                    request.project_id,
                    request.task_signature,
                    request.reason,
                    request.status,
                    request.requested_at,
                    request.confirmed_at,
                ],
            )
            .expect("checkpoint rollback request");
        assert!(bind_checkpoint_termination_intent_in_transaction(
            &transaction,
            &fixture.expected,
            &encoded,
            &incident,
            &request,
            3_102,
        )
        .expect("checkpoint rollback bind"));
        transaction
            .execute(
                "UPDATE research_reviews SET response_json = ?1 WHERE review_id = ?2",
                rusqlite::params!["{\"mutated_after_bind\":true}", fixture.review_id],
            )
            .expect("checkpoint rollback authority mutation");
        transaction.rollback().expect("checkpoint rollback transaction rollback");

        let connection = fixture.db.connect().expect("checkpoint rollback result connection");
        let counts: (i64, i64) = connection
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM incidents WHERE incident_id = ?1),
                    (SELECT COUNT(*) FROM termination_requests WHERE request_id = ?2)",
                rusqlite::params![incident.incident_id, request.request_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("checkpoint rollback counts");
        assert_eq!(counts, (0, 0));
        let review: (Option<String>, Option<String>, Option<i64>) = connection
            .query_row(
                "SELECT operation_stage, checkpoint_json, termination_request_id
                 FROM research_reviews WHERE review_id = ?1",
                [&fixture.review_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("checkpoint rollback review");
        assert_eq!(review, (None, None, None));
    }

    #[test]
    fn checkpoint_successor_reserve_dispatch_and_pre_add_failure_are_atomic() {
        let fixture = source_authority_fixture();
        let incident = Incident {
            incident_id: 81,
            project_id: fixture.project_id.clone(),
            kind: "research_checkpoint".to_owned(),
            task_key: Some(fixture.expected.owner.source_experiment_id.clone()),
            fingerprint: "checkpoint-fingerprint-2".to_owned(),
            status: IncidentStatus::Open,
            first_seen_at: 3_100,
            last_seen_at: 3_100,
            acknowledged_at: None,
            resolved_at: None,
        };
        let request = TerminationRequest {
            request_id: 82,
            incident_id: incident.incident_id,
            project_id: fixture.project_id.clone(),
            task_signature: fixture.expected.raw_task_signature.clone(),
            reason: format!("research_action:{}:checkpoint", fixture.review_id),
            status: TerminationRequestStatus::Confirmed,
            requested_at: 3_100,
            dispatch_lease_until: None,
            grace_until: None,
            confirmed_at: Some(3_101),
            last_error: None,
        };
        let connection = fixture.db.connect().expect("checkpoint successor setup connection");
        connection
            .execute(
                "INSERT INTO incidents (
                    incident_id, project_id, kind, task_key, fingerprint, status,
                    first_seen_at, last_seen_at, acknowledged_at, resolved_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, NULL, NULL)",
                rusqlite::params![
                    incident.incident_id,
                    incident.project_id,
                    incident.kind,
                    incident.task_key,
                    incident.fingerprint,
                    incident.status,
                    incident.first_seen_at,
                ],
            )
            .expect("checkpoint successor incident");
        connection
            .execute(
                "INSERT INTO termination_requests (
                    request_id, incident_id, project_id, task_signature, reason,
                    status, requested_at, dispatch_lease_until, grace_until,
                    confirmed_at, last_error
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, NULL)",
                rusqlite::params![
                    request.request_id,
                    request.incident_id,
                    request.project_id,
                    request.task_signature,
                    request.reason,
                    request.status,
                    request.requested_at,
                    request.confirmed_at,
                ],
            )
            .expect("checkpoint successor termination");
        let encoded = crate::research_checkpoint::serialize_prepared_checkpoint(&fixture.checkpoint)
            .expect("checkpoint successor encoding");
        connection
            .execute(
                "UPDATE research_reviews
                 SET checkpoint_json = ?1, operation_stage = 'stop_confirmed',
                     termination_request_id = ?2
                 WHERE review_id = ?3",
                rusqlite::params![encoded, request.request_id, fixture.review_id],
            )
            .expect("checkpoint successor confirmed review");
        drop(connection);

        let mut owner = fixture.expected.owner.clone();
        owner.operation_stage = Some("stop_confirmed".to_owned());
        owner.termination_request_id = Some(request.request_id);
        fixture
            .db
            .connect()
            .expect("checkpoint paused campaign connection")
            .execute(
                "UPDATE campaigns SET state = 'paused' WHERE campaign_id = ?1",
                [&fixture.campaign_id],
            )
            .expect("checkpoint pause campaign");
        let mut connection = fixture
            .db
            .connect()
            .expect("checkpoint paused admission connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("checkpoint paused admission transaction");
        assert_eq!(
            super::super::campaigns::accept_checkpoint_successor_in_transaction(
                &transaction,
                &owner,
                &CampaignLimits::default(),
                3_101,
            )
            .expect("checkpoint paused admission"),
            CheckpointSuccessorAdmission::Deferred
        );
        transaction.commit().expect("checkpoint paused admission commit");
        let successor_count: i64 = fixture
            .db
            .connect()
            .expect("checkpoint paused count connection")
            .query_row(
                "SELECT COUNT(*) FROM experiments WHERE experiment_id = ?1",
                [&fixture.checkpoint.successor_ids.experiment_id],
                |row| row.get(0),
            )
            .expect("checkpoint paused successor count");
        assert_eq!(successor_count, 0);
        fixture
            .db
            .connect()
            .expect("checkpoint resume campaign connection")
            .execute(
                "UPDATE campaigns SET state = 'active' WHERE campaign_id = ?1",
                [&fixture.campaign_id],
            )
            .expect("checkpoint resume campaign");
        let mut connection = fixture.db.connect().expect("checkpoint successor transaction connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("checkpoint successor transaction");
        let admission = super::super::campaigns::accept_checkpoint_successor_in_transaction(
            &transaction,
            &owner,
            &CampaignLimits::default(),
            3_102,
        )
        .expect("checkpoint successor admission");
        let successor = match admission {
            CheckpointSuccessorAdmission::Ready(intent) => intent,
            other => panic!("unexpected checkpoint successor admission: {other:?}"),
        };
        transaction.commit().expect("checkpoint successor commit");
        assert_eq!(
            successor.experiment.experiment_id,
            fixture.checkpoint.successor_ids.experiment_id
        );
        let original_proposal: (String, String) = fixture
            .db
            .connect()
            .expect("checkpoint proposal snapshot connection")
            .query_row(
                "SELECT hypothesis, canonical_digest FROM proposals WHERE proposal_id = ?1",
                [&fixture.checkpoint.successor_ids.proposal_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("checkpoint proposal snapshot");

        let mut connection = fixture
            .db
            .connect()
            .expect("checkpoint successor replay connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("checkpoint successor replay transaction");
        let replay_owner = match research_ownership_in_transaction(
            &transaction,
            &fixture.project_id,
            &fixture.campaign_id,
            &fixture.experiment_id,
        )
        .expect("checkpoint successor replay owner")
        {
            ResearchOwnership::Open(Some(owner)) => owner,
            other => panic!("unexpected checkpoint successor replay owner: {other:?}"),
        };
        assert_eq!(
            replay_owner.operation_stage.as_deref(),
            Some("successor_reserved")
        );
        assert!(matches!(
            super::super::campaigns::accept_checkpoint_successor_in_transaction(
                &transaction,
                &replay_owner,
                &CampaignLimits::default(),
                3_103,
            )
            .expect("checkpoint successor replay"),
            CheckpointSuccessorAdmission::Ready(_)
        ));
        transaction.commit().expect("checkpoint successor replay commit");

        fixture
            .db
            .connect()
            .expect("checkpoint partial replay mutation connection")
            .execute(
                "UPDATE proposals SET hypothesis = ?1 WHERE proposal_id = ?2",
                rusqlite::params![
                    "partial replay mutation",
                    fixture.checkpoint.successor_ids.proposal_id,
                ],
            )
            .expect("checkpoint partial replay mutation");
        let mut connection = fixture
            .db
            .connect()
            .expect("checkpoint partial replay transaction connection");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("checkpoint partial replay transaction");
        assert!(matches!(
            super::super::campaigns::accept_checkpoint_successor_in_transaction(
                &transaction,
                &replay_owner,
                &CampaignLimits::default(),
                3_104,
            )
            .expect("checkpoint partial replay classification"),
            CheckpointSuccessorAdmission::Blocked
        ));
        transaction.commit().expect("checkpoint partial replay commit");
        let blocked_replay: String = fixture
            .db
            .connect()
            .expect("checkpoint partial replay review connection")
            .query_row(
                "SELECT failure_code FROM research_reviews WHERE review_id = ?1",
                [&fixture.review_id],
                |row| row.get(0),
            )
            .expect("checkpoint partial replay review");
        assert_eq!(blocked_replay, "research_checkpoint_authority_corrupt");
        fixture
            .db
            .connect()
            .expect("checkpoint partial replay restore connection")
            .execute(
                "UPDATE proposals SET hypothesis = ?1, canonical_digest = ?2
                 WHERE proposal_id = ?3",
                rusqlite::params![
                    original_proposal.0,
                    original_proposal.1,
                    fixture.checkpoint.successor_ids.proposal_id,
                ],
            )
            .expect("checkpoint partial replay proposal restore");
        fixture
            .db
            .connect()
            .expect("checkpoint partial replay review restore connection")
            .execute(
                "UPDATE research_reviews
                 SET state = 'ready', failure_code = NULL,
                     finished_at = NULL, not_before = ?1
                 WHERE review_id = ?2",
                rusqlite::params![3_104, fixture.review_id],
            )
            .expect("checkpoint partial replay review restore");

        let repository = ResearchRepository::new(&fixture.db);
        let selection = repository
            .checkpoint_dispatch_authority(
                &fixture.project_id,
                &fixture.checkpoint.successor_ids.experiment_id,
                3_103,
            )
            .expect("checkpoint dispatch authority");
        let authority = match selection {
            CheckpointDispatchSelection::Ready(authority) => authority,
            other => panic!("unexpected checkpoint dispatch selection: {other:?}"),
        };
        let experiment_repository = ExperimentRepository::new(&fixture.db);
        fixture
            .db
            .connect()
            .expect("checkpoint proposal mutation connection")
            .execute(
                "UPDATE proposals SET hypothesis = ?1 WHERE proposal_id = ?2",
                rusqlite::params![
                    "mutated after checkpoint dispatch witness",
                    fixture.checkpoint.successor_ids.proposal_id,
                ],
            )
            .expect("checkpoint proposal mutation");
        assert!(experiment_repository
            .begin_checkpoint_submitting_or_defer(&authority, 3_104)
            .is_err());
        let connection = fixture
            .db
            .connect()
            .expect("checkpoint proposal restore connection");
        connection
            .execute(
                "UPDATE proposals SET hypothesis = ?1, canonical_digest = ?2
                 WHERE proposal_id = ?3",
                rusqlite::params![
                    original_proposal.0,
                    original_proposal.1,
                    fixture.checkpoint.successor_ids.proposal_id,
                ],
            )
            .expect("checkpoint proposal restore");
        let submitting = experiment_repository
            .begin_checkpoint_submitting_or_defer(&authority, 3_104)
            .expect("checkpoint submitting CAS")
            .expect("checkpoint submitting transition");
        assert_eq!(submitting.status, ExperimentStatus::Submitting);
        experiment_repository
            .fail_checkpoint_before_add(
                &authority,
                "research_checkpoint_verification_failed",
                3_105,
            )
            .expect("checkpoint pre-add failure");
        experiment_repository
            .fail_checkpoint_before_add(
                &authority,
                "research_checkpoint_verification_failed",
                3_106,
            )
            .expect("checkpoint pre-add idempotence");
        let connection = fixture.db.connect().expect("checkpoint successor final connection");
        let final_state: (String, String, String, String, String) = connection
            .query_row(
                "SELECT review.state, experiment.status, submission.status,
                        reservation.status, review.checkpoint_json
                 FROM research_reviews AS review
                 JOIN experiments AS experiment
                   ON experiment.experiment_id = review.successor_experiment_id
                 JOIN submissions AS submission
                   ON submission.submission_id = experiment.submission_id
                 JOIN budget_reservations AS reservation
                   ON reservation.experiment_id = experiment.experiment_id
                  AND reservation.dimension = 'experiment'
                 WHERE review.review_id = ?1",
                [&fixture.review_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .expect("checkpoint successor final graph");
        assert_eq!(final_state.0, "blocked");
        assert_eq!(final_state.1, "failed");
        assert_eq!(final_state.2, "failed");
        assert_eq!(final_state.3, "consumed");
        assert_eq!(final_state.4, encoded);
    }

    fn assert_checkpoint_pre_add_failure_shape(label: &str, begin_submitting: bool) {
        let prepared = checkpoint_dispatch_fixture();
        let fixture = &prepared.fixture;
        let experiment_repository = ExperimentRepository::new(&fixture.db);
        if begin_submitting {
            let submitting = experiment_repository
                .begin_checkpoint_submitting_or_defer(&prepared.authority, 3_104)
                .expect("checkpoint submitting CAS")
                .expect("checkpoint submitting transition");
            assert_eq!(submitting.status, ExperimentStatus::Submitting);
        }
        experiment_repository
            .fail_checkpoint_before_add(
                &prepared.authority,
                "research_checkpoint_verification_failed",
                3_105,
            )
            .expect("checkpoint pre-add failure");
        let settled = checkpoint_graph_snapshot(&prepared);
        let final_state: (String, String, String, String, String, Option<i64>, Option<String>) =
            fixture
                .db
                .connect()
                .expect("checkpoint pre-add settled connection")
                .query_row(
                    "SELECT review.state, experiment.status, submission.status,
                            reservation.status, review.checkpoint_json,
                            experiment.pueue_task_id, experiment.task_signature
                     FROM research_reviews AS review
                     JOIN experiments AS experiment
                       ON experiment.experiment_id = review.successor_experiment_id
                     JOIN submissions AS submission
                       ON submission.submission_id = experiment.submission_id
                     JOIN budget_reservations AS reservation
                       ON reservation.experiment_id = experiment.experiment_id
                      AND reservation.dimension = 'experiment'
                     WHERE review.review_id = ?1",
                    [&fixture.review_id],
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
                .expect("checkpoint pre-add settled graph");
        assert_eq!(final_state.0, "blocked", "{label}");
        assert_eq!(final_state.1, "failed", "{label}");
        assert_eq!(final_state.2, "failed", "{label}");
        assert_eq!(final_state.3, "consumed", "{label}");
        assert_eq!(final_state.4, prepared.encoded, "{label}");
        assert_eq!(final_state.5, None, "{label}");
        assert_eq!(final_state.6, None, "{label}");

        experiment_repository
            .fail_checkpoint_before_add(
                &prepared.authority,
                "research_checkpoint_verification_failed",
                3_106,
            )
            .expect("exact settled checkpoint pre-add replay");
        assert_eq!(checkpoint_graph_snapshot(&prepared), settled, "{label}: replay mutated graph");
    }

    #[test]
    fn checkpoint_pre_add_failure_covers_reserved_and_submitting_idempotence() {
        assert_checkpoint_pre_add_failure_shape("reserved", false);
        assert_checkpoint_pre_add_failure_shape("submitting", true);
    }

    #[test]
    fn checkpoint_pre_add_failure_rejects_mutated_settled_graph() {
        let prepared = checkpoint_dispatch_fixture();
        let fixture = &prepared.fixture;
        ExperimentRepository::new(&fixture.db)
            .fail_checkpoint_before_add(
                &prepared.authority,
                "research_checkpoint_verification_failed",
                3_105,
            )
            .expect("checkpoint pre-add failure");
        let settled = checkpoint_graph_snapshot(&prepared);
        let connection = fixture
            .db
            .connect()
            .expect("checkpoint settled mutation connection");
        connection
            .execute(
                "UPDATE research_reviews SET failure_code = 'tampered-settled-review'
                 WHERE review_id = ?1",
                [&fixture.review_id],
            )
            .expect("checkpoint settled review mutation");
        drop(connection);
        let mutated = checkpoint_graph_snapshot(&prepared);
        assert_ne!(settled, mutated, "settled mutation did not change setup");
        let result = ExperimentRepository::new(&fixture.db).fail_checkpoint_before_add(
            &prepared.authority,
            "research_checkpoint_verification_failed",
            3_106,
        );
        assert!(result.is_err(), "mutated settled graph was accepted: {result:?}");
        assert_eq!(
            checkpoint_graph_snapshot(&prepared),
            mutated,
            "rejected settled replay mutated unrelated resources"
        );
    }

    #[test]
    fn checkpoint_dispatch_stale_authority_matrix_rejects_without_resource_mutation() {
        let cases = vec![
            (
                "proposal hypothesis",
                run_stale_dispatch_case("proposal hypothesis", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE proposals SET hypothesis = 'stale hypothesis'
                             WHERE proposal_id = ?1",
                            [&fixture.authority.proposal_id],
                        )
                        .unwrap();
                }),
            ),
            (
                "proposal expected evidence",
                run_stale_dispatch_case("proposal expected evidence", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE proposals SET expected_evidence_json = '[\"stale\"]'
                             WHERE proposal_id = ?1",
                            [&fixture.authority.proposal_id],
                        )
                        .unwrap();
                }),
            ),
            (
                "proposal canonical digest",
                run_stale_dispatch_case("proposal canonical digest", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE proposals SET canonical_digest = ?1 WHERE proposal_id = ?2",
                            rusqlite::params![
                                "d".repeat(64),
                                fixture.authority.proposal_id
                            ],
                        )
                        .unwrap();
                }),
            ),
            (
                "submission argv",
                run_stale_dispatch_case("submission argv", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE submissions SET argv_json = '[\"python\",\"stale.py\"]'
                             WHERE submission_id = ?1",
                            [&fixture.authority.submission_id],
                        )
                        .unwrap();
                }),
            ),
            (
                "submission metadata",
                run_stale_dispatch_case("submission metadata", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE submissions SET metadata_json = '{\"stale\":true}'
                             WHERE submission_id = ?1",
                            [&fixture.authority.submission_id],
                        )
                        .unwrap();
                }),
            ),
            (
                "successor experiment parent",
                run_stale_dispatch_case("successor experiment parent", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE experiments SET parent_experiment_id = NULL
                             WHERE experiment_id = ?1",
                            [&fixture.authority.successor_experiment_id],
                        )
                        .unwrap();
                }),
            ),
            (
                "successor experiment status",
                run_stale_dispatch_case("successor experiment status", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE experiments SET status = 'failed' WHERE experiment_id = ?1",
                            [&fixture.authority.successor_experiment_id],
                        )
                        .unwrap();
                }),
            ),
            (
                "successor attempt",
                run_stale_dispatch_case("successor attempt", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE experiments SET attempt = attempt + 1
                             WHERE experiment_id = ?1",
                            [&fixture.authority.successor_experiment_id],
                        )
                        .unwrap();
                }),
            ),
            (
                "reservation campaign",
                run_stale_dispatch_case("reservation campaign", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE budget_reservations SET campaign_id = ?1
                             WHERE experiment_id = ?2 AND dimension = 'experiment'",
                            rusqlite::params![
                                "checkpoint-foreign-campaign",
                                fixture.authority.successor_experiment_id
                            ],
                        )
                        .unwrap();
                }),
            ),
            (
                "reservation window",
                run_stale_dispatch_case("reservation window", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE budget_reservations SET window_ends_at = window_ends_at + 1
                             WHERE experiment_id = ?1 AND dimension = 'experiment'",
                            [&fixture.authority.successor_experiment_id],
                        )
                        .unwrap();
                }),
            ),
            (
                "reservation identity",
                run_stale_dispatch_case("reservation identity", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE budget_reservations SET reservation_id = ?1
                             WHERE experiment_id = ?2 AND dimension = 'experiment'",
                            rusqlite::params![
                                "stale-reservation-id",
                                fixture.authority.successor_experiment_id
                            ],
                        )
                        .unwrap();
                }),
            ),
            (
                "reservation status",
                run_stale_dispatch_case("reservation status", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE budget_reservations SET status = 'consumed'
                             WHERE experiment_id = ?1 AND dimension = 'experiment'",
                            [&fixture.authority.successor_experiment_id],
                        )
                        .unwrap();
                }),
            ),
            (
                "native owner",
                run_stale_dispatch_case("native owner", |fixture, connection| {
                    mutate_review_notes(fixture, connection, |notes| {
                        notes["native_recovery"]["run_id"] = json!(999_999);
                    });
                }),
            ),
            (
                "native cleanup",
                run_stale_dispatch_case("native cleanup", |fixture, connection| {
                    mutate_review_notes(fixture, connection, |notes| {
                        notes["native_recovery"]["cleanup"]["phase"] = json!("pending");
                        notes["native_recovery"]["cleanup"]["completed_at"] = Value::Null;
                    });
                }),
            ),
            (
                "termination confirmation",
                run_stale_dispatch_case("termination confirmation", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE termination_requests
                             SET status = 'requested', confirmed_at = NULL
                             WHERE request_id = (
                                 SELECT termination_request_id FROM research_reviews
                                 WHERE review_id = ?1
                             )",
                            [&fixture.fixture.review_id],
                        )
                        .unwrap();
                }),
            ),
            (
                "termination raw signature",
                run_stale_dispatch_case("termination raw signature", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE termination_requests SET task_signature = 'stale-signature'
                             WHERE request_id = (
                                 SELECT termination_request_id FROM research_reviews
                                 WHERE review_id = ?1
                             )",
                            [&fixture.fixture.review_id],
                        )
                        .unwrap();
                }),
            ),
            (
                "termination request identity",
                run_stale_dispatch_case("termination request identity", |fixture, connection| {
                    connection
                        .execute(
                            "INSERT INTO incidents (
                                incident_id, project_id, kind, task_key, fingerprint, status,
                                first_seen_at, last_seen_at, acknowledged_at, resolved_at
                             ) VALUES (181, ?1, 'research_checkpoint', ?2,
                                       'checkpoint-fingerprint-replacement', 'open',
                                       3_100, 3_100, NULL, NULL)",
                            rusqlite::params![
                                fixture.fixture.project_id,
                                fixture.fixture.experiment_id
                            ],
                        )
                        .unwrap();
                    connection
                        .execute(
                            "INSERT INTO termination_requests (
                                request_id, incident_id, project_id, task_signature, reason,
                                status, requested_at, dispatch_lease_until, grace_until,
                                confirmed_at, last_error
                             ) VALUES (182, 181, ?1, ?2, 'replacement', 'confirmed',
                                       3_100, NULL, NULL, 3_101, NULL)",
                            rusqlite::params![
                                fixture.fixture.project_id,
                                fixture.fixture.expected.raw_task_signature
                            ],
                        )
                        .unwrap();
                    connection
                        .execute(
                            "UPDATE research_reviews SET termination_request_id = 182
                             WHERE review_id = ?1",
                            [&fixture.fixture.review_id],
                        )
                        .unwrap();
                }),
            ),
            (
                "review failure code",
                run_stale_dispatch_case("review failure code", |fixture, connection| {
                    connection
                        .execute(
                            "UPDATE research_reviews SET failure_code = 'stale-review-failure'
                             WHERE review_id = ?1",
                            [&fixture.fixture.review_id],
                        )
                        .unwrap();
                }),
            ),
        ];
        let failures: Vec<_> = cases
            .into_iter()
            .filter_map(|(label, result)| result.err().map(|error| (label, error)))
            .collect();
        assert!(failures.is_empty(), "stale authority matrix failures: {failures:?}");
    }

    fn checkpoint_ownership_case<F>(
        label: &str,
        mutate: F,
    ) -> Result<(), String>
    where
        F: FnOnce(&CheckpointDispatchFixture, &Connection),
    {
        let prepared = checkpoint_dispatch_fixture();
        let connection = prepared
            .fixture
            .db
            .connect()
            .map_err(|error| format!("{label}: connect: {error}"))?;
        mutate(&prepared, &connection);
        drop(connection);
        let mut connection = prepared
            .fixture
            .db
            .connect()
            .map_err(|error| format!("{label}: ownership connect: {error}"))?;
        let transaction = connection
            .transaction()
            .map_err(|error| format!("{label}: ownership transaction: {error}"))?;
        let ownership = research_ownership_in_transaction(
            &transaction,
            &prepared.fixture.project_id,
            &prepared.fixture.campaign_id,
            &prepared.fixture.experiment_id,
        )
        .map_err(|error| format!("{label}: ownership query: {error}"))?;
        match ownership {
            ResearchOwnership::Open(Some(owner)) if !owner.recovery_required => Ok(()),
            other => Err(format!("{label}: expected non-recovery Open owner, got {other:?}")),
        }
    }

    struct CheckpointAdmissionFixture {
        fixture: SourceAuthorityFixture,
        owner: ResearchOwnershipSnapshot,
    }

    fn checkpoint_stop_confirmed_fixture() -> CheckpointAdmissionFixture {
        let fixture = source_authority_fixture();
        let incident = Incident {
            incident_id: 81,
            project_id: fixture.project_id.clone(),
            kind: "research_checkpoint".to_owned(),
            task_key: Some(fixture.expected.owner.source_experiment_id.clone()),
            fingerprint: "checkpoint-fingerprint-2".to_owned(),
            status: IncidentStatus::Open,
            first_seen_at: 3_100,
            last_seen_at: 3_100,
            acknowledged_at: None,
            resolved_at: None,
        };
        let request = TerminationRequest {
            request_id: 82,
            incident_id: incident.incident_id,
            project_id: fixture.project_id.clone(),
            task_signature: fixture.expected.raw_task_signature.clone(),
            reason: format!("research_action:{}:checkpoint", fixture.review_id),
            status: TerminationRequestStatus::Confirmed,
            requested_at: 3_100,
            dispatch_lease_until: None,
            grace_until: None,
            confirmed_at: Some(3_101),
            last_error: None,
        };
        let connection = fixture.db.connect().expect("checkpoint admission setup connection");
        connection
            .execute(
                "INSERT INTO incidents (
                    incident_id, project_id, kind, task_key, fingerprint, status,
                    first_seen_at, last_seen_at, acknowledged_at, resolved_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, NULL, NULL)",
                rusqlite::params![
                    incident.incident_id,
                    incident.project_id,
                    incident.kind,
                    incident.task_key,
                    incident.fingerprint,
                    incident.status,
                    incident.first_seen_at,
                ],
            )
            .expect("checkpoint admission incident");
        connection
            .execute(
                "INSERT INTO termination_requests (
                    request_id, incident_id, project_id, task_signature, reason,
                    status, requested_at, dispatch_lease_until, grace_until,
                    confirmed_at, last_error
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, NULL)",
                rusqlite::params![
                    request.request_id,
                    request.incident_id,
                    request.project_id,
                    request.task_signature,
                    request.reason,
                    request.status,
                    request.requested_at,
                    request.confirmed_at,
                ],
            )
            .expect("checkpoint admission termination");
        let encoded = crate::research_checkpoint::serialize_prepared_checkpoint(&fixture.checkpoint)
            .expect("checkpoint admission encoding");
        connection
            .execute(
                "UPDATE research_reviews
                 SET checkpoint_json = ?1, operation_stage = 'stop_confirmed',
                     termination_request_id = ?2
                 WHERE review_id = ?3",
                rusqlite::params![encoded, request.request_id, fixture.review_id],
            )
            .expect("checkpoint admission confirmed review");
        drop(connection);

        let mut owner = fixture.expected.owner.clone();
        owner.operation_stage = Some("stop_confirmed".to_owned());
        owner.termination_request_id = Some(request.request_id);
        CheckpointAdmissionFixture {
            fixture,
            owner,
        }
    }

    fn call_checkpoint_admission(
        prepared: &CheckpointAdmissionFixture,
        now: i64,
    ) -> Result<CheckpointSuccessorAdmission, AppError> {
        let mut connection = prepared
            .fixture
            .db
            .connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("admission transaction"))?;
        match super::super::campaigns::accept_checkpoint_successor_in_transaction(
            &transaction,
            &prepared.owner,
            &CampaignLimits::default(),
            now,
        ) {
            Ok(admission) => {
                transaction
                    .commit()
                    .map_err(database_error("admission commit"))?;
                Ok(admission)
            }
            Err(error) => {
                drop(transaction);
                Err(error)
            }
        }
    }

    #[derive(Debug, PartialEq)]
    struct CheckpointAdmissionExistingSnapshot {
        reviews: Vec<Vec<rusqlite::types::Value>>,
        proposals: Vec<Vec<rusqlite::types::Value>>,
        submissions: Vec<Vec<rusqlite::types::Value>>,
        experiments: Vec<Vec<rusqlite::types::Value>>,
        reservations: Vec<Vec<rusqlite::types::Value>>,
        termination_requests: Vec<Vec<rusqlite::types::Value>>,
        incidents: Vec<Vec<rusqlite::types::Value>>,
        campaign_research: Vec<Vec<rusqlite::types::Value>>,
        events: Vec<Vec<rusqlite::types::Value>>,
        task_observations: Vec<Vec<rusqlite::types::Value>>,
    }

    fn snapshot_sql_rows<P: rusqlite::Params>(
        connection: &Connection,
        sql: &str,
        params: P,
    ) -> Vec<Vec<rusqlite::types::Value>> {
        connection
            .prepare(sql)
            .expect("checkpoint admission snapshot statement")
            .query_map(params, |row| {
                (0..row.as_ref().column_count())
                    .map(|index| row.get(index))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .expect("checkpoint admission snapshot query")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("checkpoint admission snapshot rows")
    }

    fn checkpoint_admission_existing_snapshot(
        fixture: &SourceAuthorityFixture,
    ) -> CheckpointAdmissionExistingSnapshot {
        let connection = fixture.db.connect().expect("checkpoint admission snapshot connection");
        CheckpointAdmissionExistingSnapshot {
            reviews: snapshot_sql_rows(
                &connection,
                "SELECT * FROM research_reviews ORDER BY review_id",
                rusqlite::params![],
            ),
            proposals: snapshot_sql_rows(
                &connection,
                "SELECT * FROM proposals ORDER BY proposal_id",
                rusqlite::params![],
            ),
            submissions: snapshot_sql_rows(
                &connection,
                "SELECT * FROM submissions ORDER BY submission_id",
                rusqlite::params![],
            ),
            experiments: snapshot_sql_rows(
                &connection,
                "SELECT * FROM experiments ORDER BY experiment_id",
                rusqlite::params![],
            ),
            reservations: snapshot_sql_rows(
                &connection,
                "SELECT * FROM budget_reservations ORDER BY reservation_id",
                rusqlite::params![],
            ),
            termination_requests: snapshot_sql_rows(
                &connection,
                "SELECT * FROM termination_requests ORDER BY request_id",
                rusqlite::params![],
            ),
            incidents: snapshot_sql_rows(
                &connection,
                "SELECT * FROM incidents ORDER BY incident_id",
                rusqlite::params![],
            ),
            campaign_research: snapshot_sql_rows(
                &connection,
                "SELECT * FROM campaign_research ORDER BY campaign_id",
                rusqlite::params![],
            ),
            events: snapshot_sql_rows(
                &connection,
                "SELECT * FROM events ORDER BY event_id",
                rusqlite::params![],
            ),
            task_observations: snapshot_sql_rows(
                &connection,
                "SELECT * FROM task_observations ORDER BY task_signature",
                rusqlite::params![],
            ),
        }
    }

    fn checkpoint_successor_resource_counts(
        fixture: &SourceAuthorityFixture,
        successor_ids: &crate::research_checkpoint::CheckpointSuccessorIds,
    ) -> (i64, i64, i64, i64) {
        let connection = fixture
            .db
            .connect()
            .expect("checkpoint successor resource count connection");
        let proposal_count = connection
            .query_row(
                "SELECT COUNT(*) FROM proposals WHERE proposal_id = ?1",
                [&successor_ids.proposal_id],
                |row| row.get(0),
            )
            .expect("checkpoint successor proposal count");
        let submission_count = connection
            .query_row(
                "SELECT COUNT(*) FROM submissions WHERE submission_id = ?1",
                [&successor_ids.submission_id],
                |row| row.get(0),
            )
            .expect("checkpoint successor submission count");
        let experiment_count = connection
            .query_row(
                "SELECT COUNT(*) FROM experiments WHERE experiment_id = ?1",
                [&successor_ids.experiment_id],
                |row| row.get(0),
            )
            .expect("checkpoint successor experiment count");
        let reservation_count = connection
            .query_row(
                "SELECT COUNT(*) FROM budget_reservations
                 WHERE experiment_id = ?1 AND dimension = 'experiment'",
                [&successor_ids.experiment_id],
                |row| row.get(0),
            )
            .expect("checkpoint successor reservation count");
        (
            proposal_count,
            submission_count,
            experiment_count,
            reservation_count,
        )
    }

    fn insert_successor_id_collision(
        prepared: &CheckpointAdmissionFixture,
        kind: &str,
    ) {
        let ids = &prepared.fixture.checkpoint.successor_ids;
        let connection = prepared
            .fixture
            .db
            .connect()
            .expect("checkpoint collision connection");
        match kind {
            "proposal" => {
                connection
                    .execute(
                        "INSERT INTO proposals (
                            proposal_id, campaign_id, kind, status, hypothesis,
                            source_experiment_id, argv_json, working_directory,
                            expected_evidence_json, canonical_digest, reject_reason,
                            created_at, updated_at
                         ) VALUES (?1, ?2, 'experiment', 'accepted', 'collision',
                                   NULL, '[\"python\",\"collision.py\"]', '.',
                                   '[]', 'collision-proposal-digest', NULL, 3_100, 3_100)",
                        rusqlite::params![ids.proposal_id, prepared.fixture.campaign_id],
                    )
                    .expect("proposal ID collision row");
            }
            "submission" => {
                connection
                    .execute(
                        "INSERT INTO submissions (
                            submission_id, project_id, argv_json, created_at,
                            pueue_task_id, task_signature, status, kind,
                            metadata_json, origin_agent_run_id
                         ) VALUES (?1, ?2, '[\"python\",\"collision.py\"]', 3_100,
                                   NULL, NULL, 'pending', 'experiment', '{}', NULL)",
                        rusqlite::params![ids.submission_id, prepared.fixture.project_id],
                    )
                    .expect("submission ID collision row");
            }
            "experiment" => {
                connection
                    .execute(
                        "INSERT INTO proposals (
                            proposal_id, campaign_id, kind, status, hypothesis,
                            source_experiment_id, argv_json, working_directory,
                            expected_evidence_json, canonical_digest, reject_reason,
                            created_at, updated_at
                         ) VALUES ('collision-support-proposal', ?1, 'experiment',
                                   'accepted', 'collision support', NULL,
                                   '[\"python\",\"collision-support.py\"]', '.',
                                   '[]', 'collision-support-digest', NULL, 3_100, 3_100)",
                        [&prepared.fixture.campaign_id],
                    )
                    .expect("experiment collision proposal support");
                connection
                    .execute(
                        "INSERT INTO submissions (
                            submission_id, project_id, argv_json, created_at,
                            pueue_task_id, task_signature, status, kind,
                            metadata_json, origin_agent_run_id
                         ) VALUES ('collision-support-submission', ?1,
                                   '[\"python\",\"collision-support.py\"]', 3_100,
                                   NULL, NULL, 'pending', 'experiment', '{}', NULL)",
                        [&prepared.fixture.project_id],
                    )
                    .expect("experiment collision submission support");
                connection
                    .execute(
                        "INSERT INTO experiments (
                            experiment_id, campaign_id, proposal_id, submission_id,
                            parent_experiment_id, attempt, status, pueue_task_id,
                            task_signature, failure_code, failure_fingerprint,
                            created_at, updated_at, finished_at,
                            resume_of_experiment_id, checkpoint_note,
                            code_change_run_id, code_revision_sha
                         ) VALUES (?1, ?2, 'collision-support-proposal',
                                   'collision-support-submission', ?3, 1, 'reserved',
                                   NULL, NULL, NULL, NULL, 3_100, 3_100, NULL,
                                   ?3, 'collision', NULL, NULL)",
                        rusqlite::params![
                            ids.experiment_id,
                            prepared.fixture.campaign_id,
                            prepared.fixture.experiment_id,
                        ],
                    )
                    .expect("experiment ID collision row");
            }
            "reservation" => {
                connection
                    .execute(
                        "INSERT INTO budget_reservations (
                            reservation_id, campaign_id, experiment_id, dimension,
                            subject_key, status, window_started_at, window_ends_at,
                            created_at, updated_at
                         ) VALUES (?1, ?2, NULL, 'agent_run',
                                   'reservation-collision', 'reserved', 3_100,
                                   4_100, 3_100, 3_100)",
                        rusqlite::params![
                            format!("experiment:{}", ids.experiment_id),
                            prepared.fixture.campaign_id,
                        ],
                    )
                    .expect("reservation ID collision row");
            }
            other => panic!("unknown collision kind: {other}"),
        }
    }

    fn checkpoint_successor_proposal(
        fixture: &SourceAuthorityFixture,
    ) -> crate::proposals::ValidatedProposal {
        let connection = fixture
            .db
            .connect()
            .expect("checkpoint successor proposal connection");
        let expected_evidence_json: String = connection
            .query_row(
                "SELECT expected_evidence_json FROM proposals WHERE proposal_id = ?1",
                [&fixture.proposal_id],
                |row| row.get(0),
            )
            .expect("checkpoint successor source evidence");
        let expected_evidence: Vec<String> = serde_json::from_str(&expected_evidence_json)
            .expect("checkpoint successor source evidence JSON");
        proposals::validate(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: format!(
                    "Resume {} from its verified checkpoint",
                    fixture.experiment_id
                ),
                source_experiment_id: Some(fixture.experiment_id.clone()),
                argv: fixture.checkpoint.retained_argv.clone(),
                working_directory: fixture.checkpoint.source_working_directory.clone(),
                expected_evidence,
            },
            &fixture.checkpoint.campaign_objective_digest,
        )
        .expect("checkpoint successor proposal validation")
    }

    fn insert_canonical_digest_conflict(prepared: &CheckpointAdmissionFixture) {
        let proposal = checkpoint_successor_proposal(&prepared.fixture);
        let argv_json = serde_json::to_string(proposal.argv()).expect("conflict argv JSON");
        let evidence_json =
            serde_json::to_string(proposal.expected_evidence()).expect("conflict evidence JSON");
        prepared
            .fixture
            .db
            .connect()
            .expect("canonical conflict connection")
            .execute(
                "INSERT INTO proposals (
                    proposal_id, campaign_id, kind, status, hypothesis,
                    source_experiment_id, argv_json, working_directory,
                    expected_evidence_json, canonical_digest, reject_reason,
                    created_at, updated_at
                 ) VALUES ('canonical-conflict-proposal', ?1, ?2, 'accepted', ?3,
                           ?4, ?5, ?6, ?7, ?8, NULL, 3_100, 3_100)",
                rusqlite::params![
                    prepared.fixture.campaign_id,
                    proposal.kind(),
                    proposal.hypothesis(),
                    proposal.source_experiment_id(),
                    argv_json,
                    proposal.working_directory(),
                    evidence_json,
                    proposal.canonical_digest(),
                ],
            )
            .expect("canonical digest conflict row");
    }

    #[test]
    fn checkpoint_admission_rejects_each_successor_id_collision_without_mutation() {
        let mut failures: Vec<(&str, String)> = Vec::new();
        for kind in ["proposal", "submission", "experiment", "reservation"] {
            let prepared = checkpoint_stop_confirmed_fixture();
            insert_successor_id_collision(&prepared, kind);
            let before = checkpoint_admission_existing_snapshot(&prepared.fixture);
            let result = call_checkpoint_admission(&prepared, 3_102);
            let after = checkpoint_admission_existing_snapshot(&prepared.fixture);
            let collision_rejected = if kind == "reservation" {
                matches!(
                    &result,
                    Err(AppError::Database { operation, .. })
                        if *operation == "insert experiment budget reservation"
                )
            } else {
                matches!(&result, Ok(CheckpointSuccessorAdmission::Blocked))
            };
            if !collision_rejected {
                failures.push((kind, format!("collision was not rejected: {result:?}")));
            }
            if before != after {
                failures.push((kind, "ID collision mutated an existing row".to_owned()));
            }
        }
        assert!(
            failures.is_empty(),
            "successor ID collision failures: {failures:?}"
        );
    }

    #[test]
    fn checkpoint_admission_rejects_canonical_digest_conflict_without_resources() {
        let prepared = checkpoint_stop_confirmed_fixture();
        insert_canonical_digest_conflict(&prepared);
        let before = checkpoint_admission_existing_snapshot(&prepared.fixture);
        let result = call_checkpoint_admission(&prepared, 3_102);
        let after = checkpoint_admission_existing_snapshot(&prepared.fixture);
        assert!(
            matches!(
                &result,
                Err(AppError::Database { operation, .. })
                    if *operation == "insert campaign proposal"
            ),
            "canonical conflict did not reach proposal insertion: {result:?}"
        );
        assert_eq!(before, after, "canonical conflict mutated an existing row");
        assert_eq!(
            checkpoint_successor_resource_counts(&prepared.fixture, &prepared.fixture.checkpoint.successor_ids),
            (0, 0, 0, 0),
            "canonical conflict left successor resources behind"
        );
    }

    #[test]
    fn checkpoint_admission_final_review_cas_loss_rolls_back_all_resources() {
        let prepared = checkpoint_stop_confirmed_fixture();
        let successor = &prepared.fixture.checkpoint.successor_ids;
        let quote = |value: &str| value.replace('\'', "''");
        let trigger = format!(
            "CREATE TRIGGER force_checkpoint_final_cas_loss\n\
             AFTER INSERT ON budget_reservations\n\
             WHEN NEW.experiment_id = '{experiment}'\n\
               AND EXISTS (SELECT 1 FROM proposals WHERE proposal_id = '{proposal}')\n\
               AND EXISTS (SELECT 1 FROM submissions WHERE submission_id = '{submission}')\n\
               AND EXISTS (SELECT 1 FROM experiments WHERE experiment_id = '{experiment}')\n\
             BEGIN\n\
               UPDATE research_reviews\n\
               SET state = 'blocked', failure_code = 'test_final_cas_loss'\n\
               WHERE review_id = '{review}' AND state = 'ready';\n\
             END;",
            experiment = quote(&successor.experiment_id),
            proposal = quote(&successor.proposal_id),
            submission = quote(&successor.submission_id),
            review = quote(&prepared.fixture.review_id),
        );
        prepared
            .fixture
            .db
            .connect()
            .expect("final CAS trigger connection")
            .execute_batch(&trigger)
            .expect("final CAS trigger");
        let before = checkpoint_admission_existing_snapshot(&prepared.fixture);
        let result = call_checkpoint_admission(&prepared, 3_102);
        let after = checkpoint_admission_existing_snapshot(&prepared.fixture);
        assert!(
            matches!(
                &result,
                Err(AppError::Validation { field, message })
                    if *field == "research.review"
                        && *message == "changed before checkpoint successor reservation"
            ),
            "final review CAS loss did not reach the guarded final CAS: {result:?}"
        );
        assert_eq!(
            before, after,
            "final review CAS loss changed an existing row after rollback"
        );
        assert_eq!(
            checkpoint_successor_resource_counts(&prepared.fixture, successor),
            (0, 0, 0, 0),
            "final review CAS loss left one or more successor resources"
        );
    }

    fn mutate_checkpoint_terminal(
        prepared: &CheckpointDispatchFixture,
        connection: &Connection,
        status: &str,
    ) {
        let (failure_code, failure_fingerprint) = if status == "failed" {
            (Some("pueue_failed"), Some("terminal-fingerprint"))
        } else {
            (None, None)
        };
        connection
            .execute(
                "UPDATE research_reviews
                 SET state = 'completed', operation_stage = NULL,
                     failure_code = NULL, finished_at = 3_200, updated_at = 3_200
                 WHERE review_id = ?1",
                [&prepared.fixture.review_id],
            )
            .expect("checkpoint terminal review");
        connection
            .execute(
                "UPDATE submissions
                 SET status = 'accepted', pueue_task_id = 99,
                     task_signature = 'pueue-managed-run:v1:terminal'
                 WHERE submission_id = ?1",
                [&prepared.authority.submission_id],
            )
            .expect("checkpoint terminal submission");
        connection
            .execute(
                "UPDATE experiments
                 SET status = ?1, pueue_task_id = 99,
                     task_signature = 'pueue-managed-run:v1:terminal',
                     failure_code = ?2, failure_fingerprint = ?3,
                     finished_at = 3_200, updated_at = 3_200
                 WHERE experiment_id = ?4",
                rusqlite::params![status, failure_code, failure_fingerprint, prepared.authority.successor_experiment_id],
            )
            .expect("checkpoint terminal experiment");
        connection
            .execute(
                "UPDATE budget_reservations SET status = 'consumed', updated_at = 3_200
                 WHERE experiment_id = ?1 AND dimension = 'experiment'",
                [&prepared.authority.successor_experiment_id],
            )
            .expect("checkpoint terminal reservation");
    }

    fn completed_task_failed_checkpoint_fixture() -> CheckpointDispatchFixture {
        let prepared = checkpoint_dispatch_fixture();
        let connection = prepared
            .fixture
            .db
            .connect()
            .expect("completed task failure connection");
        mutate_checkpoint_terminal(&prepared, &connection, "failed");
        drop(connection);
        prepared
    }

    fn completed_pre_add_checkpoint_fixture() -> CheckpointDispatchFixture {
        let prepared = checkpoint_dispatch_fixture();
        ExperimentRepository::new(&prepared.fixture.db)
            .fail_checkpoint_before_add(
                &prepared.authority,
                "research_checkpoint_verification_failed",
                3_205,
            )
            .expect("completed pre-add failure");
        prepared
            .fixture
            .db
            .connect()
            .expect("completed pre-add connection")
            .execute(
                "UPDATE research_reviews
                 SET state = 'completed', operation_stage = NULL,
                     finished_at = 3_206, updated_at = 3_206
                 WHERE review_id = ?1",
                [&prepared.fixture.review_id],
            )
            .expect("completed pre-add review");
        prepared
    }

    fn checkpoint_ownership_for_dispatch(
        prepared: &CheckpointDispatchFixture,
    ) -> Result<ResearchOwnership, String> {
        let mut connection = prepared
            .fixture
            .db
            .connect()
            .map_err(|error| format!("ownership connection: {error}"))?;
        let transaction = connection
            .transaction()
            .map_err(|error| format!("ownership transaction: {error}"))?;
        let ownership = research_ownership_in_transaction(
            &transaction,
            &prepared.fixture.project_id,
            &prepared.fixture.campaign_id,
            &prepared.fixture.experiment_id,
        )
        .map_err(|error| format!("ownership query: {error}"))?;
        transaction
            .commit()
            .map_err(|error| format!("ownership commit: {error}"))?;
        Ok(ownership)
    }

    #[test]
    fn checkpoint_ownership_completed_phase_hybrid_mutations_require_recovery() {
        let mutations: Vec<(&str, fn(&CheckpointDispatchFixture, &Connection))> = vec![
            (
                "review failure",
                |prepared, connection| {
                    connection
                        .execute(
                            "UPDATE research_reviews SET failure_code = 'hybrid-review'
                             WHERE review_id = ?1",
                            [&prepared.fixture.review_id],
                        )
                        .unwrap();
                },
            ),
            (
                "missing experiment failure code",
                |prepared, connection| {
                    connection
                        .execute(
                            "UPDATE experiments SET failure_code = NULL
                             WHERE experiment_id = ?1",
                            [&prepared.authority.successor_experiment_id],
                        )
                        .unwrap();
                },
            ),
            (
                "task identity",
                |prepared, connection| {
                    connection
                        .execute(
                            "UPDATE experiments
                             SET pueue_task_id = 100, task_signature = 'hybrid-task'
                             WHERE experiment_id = ?1",
                            [&prepared.authority.successor_experiment_id],
                        )
                        .unwrap();
                },
            ),
            (
                "submission phase",
                |prepared, connection| {
                    connection
                        .execute(
                            "UPDATE submissions
                             SET status = 'pending', pueue_task_id = NULL,
                                 task_signature = NULL
                             WHERE submission_id = ?1",
                            [&prepared.authority.submission_id],
                        )
                        .unwrap();
                },
            ),
            (
                "reservation phase",
                |prepared, connection| {
                    connection
                        .execute(
                            "UPDATE budget_reservations SET status = 'reserved'
                             WHERE experiment_id = ?1 AND dimension = 'experiment'",
                            [&prepared.authority.successor_experiment_id],
                        )
                        .unwrap();
                },
            ),
            (
                "review stage",
                |prepared, connection| {
                    connection
                        .execute(
                            "UPDATE research_reviews SET operation_stage = 'successor_reserved'
                             WHERE review_id = ?1",
                            [&prepared.fixture.review_id],
                        )
                        .unwrap();
                },
            ),
        ];
        let bases: Vec<(&str, fn() -> CheckpointDispatchFixture)> = vec![
            ("completed task-backed failed", completed_task_failed_checkpoint_fixture),
            ("completed known pre-add failed", completed_pre_add_checkpoint_fixture),
        ];
        let mut failures: Vec<(String, String)> = Vec::new();
        for (base_label, make_fixture) in bases {
            let baseline = make_fixture();
            match checkpoint_ownership_for_dispatch(&baseline) {
                Ok(ResearchOwnership::Open(Some(owner))) if !owner.recovery_required => {}
                Ok(other) => failures.push((
                    base_label.to_owned(),
                    format!("valid completed baseline was not non-recovery Open: {other:?}"),
                )),
                Err(error) => failures.push((base_label.to_owned(), error)),
            }
            for (mutation_label, mutate) in &mutations {
                let prepared = make_fixture();
                let connection = prepared.fixture.db.connect().unwrap();
                mutate(&prepared, &connection);
                drop(connection);
                match checkpoint_ownership_for_dispatch(&prepared) {
                    Ok(ResearchOwnership::Open(Some(owner))) if owner.recovery_required => {}
                    Ok(other) => failures.push((
                        format!("{base_label}/{mutation_label}"),
                        format!("expected recovery Open: {other:?}"),
                    )),
                    Err(error) => failures.push((
                        format!("{base_label}/{mutation_label}"),
                        error,
                    )),
                }
            }
        }
        assert!(
            failures.is_empty(),
            "completed phase hybrid failures: {failures:?}"
        );
    }

    fn mutate_terminal_submission_failed(
        prepared: &CheckpointDispatchFixture,
        connection: &Connection,
    ) {
        connection
            .execute(
                "UPDATE submissions
                 SET status = 'failed'
                 WHERE submission_id = ?1",
                [&prepared.authority.submission_id],
            )
            .unwrap();
    }

    fn mutate_terminal_tasks_cleared(
        prepared: &CheckpointDispatchFixture,
        connection: &Connection,
    ) {
        connection
            .execute(
                "UPDATE submissions
                 SET pueue_task_id = NULL, task_signature = NULL
                 WHERE submission_id = ?1",
                [&prepared.authority.submission_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE experiments
                 SET pueue_task_id = NULL, task_signature = NULL
                 WHERE experiment_id = ?1",
                [&prepared.authority.successor_experiment_id],
            )
            .unwrap();
    }

    fn mutate_terminal_failure_fields_swapped(
        prepared: &CheckpointDispatchFixture,
        connection: &Connection,
    ) {
        connection
            .execute(
                "UPDATE research_reviews
                 SET failure_code = 'research_checkpoint_verification_failed'
                 WHERE review_id = ?1",
                [&prepared.fixture.review_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE experiments
                 SET failure_code = NULL, failure_fingerprint = NULL
                 WHERE experiment_id = ?1",
                [&prepared.authority.successor_experiment_id],
            )
            .unwrap();
    }

    fn mutate_terminal_failure_fields_cleared(
        prepared: &CheckpointDispatchFixture,
        connection: &Connection,
    ) {
        connection
            .execute(
                "UPDATE experiments
                 SET failure_code = NULL, failure_fingerprint = NULL
                 WHERE experiment_id = ?1",
                [&prepared.authority.successor_experiment_id],
            )
            .unwrap();
    }

    fn mutate_terminal_pre_add_failure_fields(
        prepared: &CheckpointDispatchFixture,
        connection: &Connection,
    ) {
        let mut digest = Sha256::new();
        digest.update(b"research_checkpoint_verification_failed");
        digest.update([0]);
        digest.update(prepared.encoded.as_bytes());
        let failure_fingerprint = format!("research-checkpoint-pre-add:{:x}", digest.finalize());
        connection
            .execute(
                "UPDATE experiments
                 SET failure_code = 'research_checkpoint_verification_failed',
                     failure_fingerprint = ?1
                 WHERE experiment_id = ?2",
                rusqlite::params![failure_fingerprint, prepared.authority.successor_experiment_id],
            )
            .unwrap();
    }

    fn mutate_pre_add_submission_accepted(
        prepared: &CheckpointDispatchFixture,
        connection: &Connection,
    ) {
        connection
            .execute(
                "UPDATE submissions
                 SET status = 'accepted'
                 WHERE submission_id = ?1",
                [&prepared.authority.submission_id],
            )
            .unwrap();
    }

    fn mutate_pre_add_tasks_added(
        prepared: &CheckpointDispatchFixture,
        connection: &Connection,
    ) {
        connection
            .execute(
                "UPDATE submissions
                 SET pueue_task_id = 99,
                     task_signature = 'pueue-managed-run:v1:terminal'
                 WHERE submission_id = ?1",
                [&prepared.authority.submission_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE experiments
                 SET pueue_task_id = 99,
                     task_signature = 'pueue-managed-run:v1:terminal'
                 WHERE experiment_id = ?1",
                [&prepared.authority.successor_experiment_id],
            )
            .unwrap();
    }

    fn mutate_pre_add_failure_fields_swapped(
        prepared: &CheckpointDispatchFixture,
        connection: &Connection,
    ) {
        connection
            .execute(
                "UPDATE research_reviews
                 SET failure_code = NULL
                 WHERE review_id = ?1",
                [&prepared.fixture.review_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE experiments
                 SET failure_code = 'pueue_failed',
                     failure_fingerprint = 'terminal-fingerprint'
                 WHERE experiment_id = ?1",
                [&prepared.authority.successor_experiment_id],
            )
            .unwrap();
    }

    fn mutate_pre_add_failure_fields_cleared(
        prepared: &CheckpointDispatchFixture,
        connection: &Connection,
    ) {
        connection
            .execute(
                "UPDATE research_reviews
                 SET failure_code = NULL
                 WHERE review_id = ?1",
                [&prepared.fixture.review_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE experiments
                 SET failure_code = NULL, failure_fingerprint = NULL
                 WHERE experiment_id = ?1",
                [&prepared.authority.successor_experiment_id],
            )
            .unwrap();
    }

    #[test]
    fn checkpoint_ownership_cross_projection_hybrids_require_recovery_open() {
        let cases: Vec<(
            &str,
            fn() -> CheckpointDispatchFixture,
            fn(&CheckpointDispatchFixture, &Connection),
        )> = vec![
            (
                "terminal accepted-to-failed submission with tasks",
                completed_task_failed_checkpoint_fixture,
                mutate_terminal_submission_failed,
            ),
            (
                "terminal accepted with both tasks cleared",
                completed_task_failed_checkpoint_fixture,
                mutate_terminal_tasks_cleared,
            ),
            (
                "terminal review and experiment failure fields swapped",
                completed_task_failed_checkpoint_fixture,
                mutate_terminal_failure_fields_swapped,
            ),
            (
                "terminal experiment failure fields cleared",
                completed_task_failed_checkpoint_fixture,
                mutate_terminal_failure_fields_cleared,
            ),
            (
                "terminal exact pre-add failure fields",
                completed_task_failed_checkpoint_fixture,
                mutate_terminal_pre_add_failure_fields,
            ),
            (
                "pre-add failed-to-accepted submission with null tasks",
                completed_pre_add_checkpoint_fixture,
                mutate_pre_add_submission_accepted,
            ),
            (
                "pre-add failed with matching non-null tasks",
                completed_pre_add_checkpoint_fixture,
                mutate_pre_add_tasks_added,
            ),
            (
                "pre-add review and experiment failure fields swapped",
                completed_pre_add_checkpoint_fixture,
                mutate_pre_add_failure_fields_swapped,
            ),
            (
                "pre-add review and experiment failure fields cleared",
                completed_pre_add_checkpoint_fixture,
                mutate_pre_add_failure_fields_cleared,
            ),
        ];
        let mut failures: Vec<(String, String)> = Vec::new();
        for (label, make_fixture, mutate) in cases {
            let prepared = make_fixture();
            let connection = prepared.fixture.db.connect().unwrap();
            mutate(&prepared, &connection);
            drop(connection);
            match checkpoint_ownership_for_dispatch(&prepared) {
                Ok(ResearchOwnership::Open(Some(owner))) if owner.recovery_required => {}
                Ok(other) => failures.push((
                    label.to_owned(),
                    format!("expected recovery Open: {other:?}"),
                )),
                Err(error) => failures.push((label.to_owned(), error)),
            }
        }
        assert!(
            failures.is_empty(),
            "cross-projection hybrid failures: {failures:?}"
        );
    }

    #[test]
    fn checkpoint_ownership_lifecycle_active_and_terminal_shapes_are_nonrecovery_open() {
        let cases = vec![
            (
                "submitting",
                checkpoint_ownership_case("submitting", |prepared, connection| {
                    connection
                        .execute(
                            "UPDATE experiments SET status = 'submitting'
                             WHERE experiment_id = ?1",
                            [&prepared.authority.successor_experiment_id],
                        )
                        .unwrap();
                }),
            ),
            (
                "unreconciled",
                checkpoint_ownership_case("unreconciled", |prepared, connection| {
                    connection
                        .execute(
                            "UPDATE experiments
                             SET status = 'unreconciled', failure_code = 'identity-mismatch'
                             WHERE experiment_id = ?1",
                            [&prepared.authority.successor_experiment_id],
                        )
                        .unwrap();
                    connection
                        .execute(
                            "UPDATE submissions SET status = 'unreconciled'
                             WHERE submission_id = ?1",
                            [&prepared.authority.submission_id],
                        )
                        .unwrap();
                    connection
                        .execute(
                            "UPDATE budget_reservations SET status = 'consumed'
                             WHERE experiment_id = ?1 AND dimension = 'experiment'",
                            [&prepared.authority.successor_experiment_id],
                        )
                        .unwrap();
                }),
            ),
            (
                "succeeded",
                checkpoint_ownership_case("succeeded", |prepared, connection| {
                    mutate_checkpoint_terminal(prepared, connection, "succeeded");
                }),
            ),
            (
                "failed",
                checkpoint_ownership_case("failed", |prepared, connection| {
                    mutate_checkpoint_terminal(prepared, connection, "failed");
                }),
            ),
            (
                "cancelled",
                checkpoint_ownership_case("cancelled", |prepared, connection| {
                    mutate_checkpoint_terminal(prepared, connection, "cancelled");
                }),
            ),
        ];
        let failures: Vec<_> = cases
            .into_iter()
            .filter_map(|(label, result)| result.err().map(|error| (label, error)))
            .collect();
        assert!(failures.is_empty(), "ownership lifecycle failures: {failures:?}");
    }

    #[test]
    fn checkpoint_ownership_known_pre_add_failed_completed_history_is_nonrecovery_open() {
        let prepared = checkpoint_dispatch_fixture();
        ExperimentRepository::new(&prepared.fixture.db)
            .fail_checkpoint_before_add(
                &prepared.authority,
                "research_checkpoint_verification_failed",
                3_105,
            )
            .expect("checkpoint known pre-add failure");
        prepared
            .fixture
            .db
            .connect()
            .expect("checkpoint known pre-add history connection")
            .execute(
                "UPDATE research_reviews
                 SET state = 'completed', operation_stage = NULL,
                     finished_at = 3_200, updated_at = 3_200
                 WHERE review_id = ?1",
                [&prepared.fixture.review_id],
            )
            .expect("checkpoint known pre-add history");
        let mut connection = prepared
            .fixture
            .db
            .connect()
            .expect("checkpoint known pre-add ownership connection");
        let transaction = connection
            .transaction()
            .expect("checkpoint known pre-add ownership transaction");
        let ownership = research_ownership_in_transaction(
            &transaction,
            &prepared.fixture.project_id,
            &prepared.fixture.campaign_id,
            &prepared.fixture.experiment_id,
        )
        .expect("checkpoint known pre-add ownership");
        assert!(
            matches!(
                ownership,
                ResearchOwnership::Open(Some(ResearchOwnershipSnapshot {
                    recovery_required: false,
                    ..
                }))
            ),
            "ownership: {ownership:?}"
        );
    }

    #[test]
    fn checkpoint_ownership_phase_mutations_require_recovery_open() {
        let cases: Vec<(
            &str,
            fn(&CheckpointDispatchFixture, &Connection),
        )> = vec![
            (
                "accepted without task",
                |prepared: &CheckpointDispatchFixture, connection: &Connection| {
                    connection
                        .execute(
                            "UPDATE experiments SET status = 'accepted'
                             WHERE experiment_id = ?1",
                            [&prepared.authority.successor_experiment_id],
                        )
                        .unwrap();
                },
            ),
            (
                "consumed reserved",
                |prepared: &CheckpointDispatchFixture, connection: &Connection| {
                    connection
                        .execute(
                            "UPDATE budget_reservations SET status = 'consumed'
                             WHERE experiment_id = ?1 AND dimension = 'experiment'",
                            [&prepared.authority.successor_experiment_id],
                        )
                        .unwrap();
                },
            ),
            (
                "reserved failure",
                |prepared: &CheckpointDispatchFixture, connection: &Connection| {
                    connection
                        .execute(
                            "UPDATE experiments SET failure_code = 'unexpected'
                             WHERE experiment_id = ?1",
                            [&prepared.authority.successor_experiment_id],
                        )
                        .unwrap();
                },
            ),
            (
                "task identity mismatch",
                |prepared: &CheckpointDispatchFixture, connection: &Connection| {
                    connection
                        .execute(
                            "UPDATE submissions SET pueue_task_id = 98,
                             task_signature = 'pueue-managed-run:v1:wrong'
                             WHERE submission_id = ?1",
                            [&prepared.authority.submission_id],
                        )
                        .unwrap();
                },
            ),
        ];
        let failures: Vec<_> = cases
            .into_iter()
            .filter_map(|(label, mutate)| {
                let prepared = checkpoint_dispatch_fixture();
                let connection = prepared.fixture.db.connect().unwrap();
                mutate(&prepared, &connection);
                drop(connection);
                let mut connection = prepared.fixture.db.connect().unwrap();
                let transaction = connection.transaction().unwrap();
                let ownership = research_ownership_in_transaction(
                    &transaction,
                    &prepared.fixture.project_id,
                    &prepared.fixture.campaign_id,
                    &prepared.fixture.experiment_id,
                )
                .unwrap();
                match ownership {
                    ResearchOwnership::Open(Some(ResearchOwnershipSnapshot {
                        recovery_required: true,
                        ..
                    })) => None,
                    other => Some((label, format!("expected recovery Open, got {other:?}"))),
                }
            })
            .collect();
        assert!(failures.is_empty(), "ownership mutation failures: {failures:?}");
    }

    #[test]
    fn checkpoint_pre_add_failure_rejects_post_add_and_unreconciled_without_writes() {
        let cases: Vec<(
            &str,
            fn(&CheckpointDispatchFixture, &Connection),
        )> = vec![
            (
                "post-add accepted",
                |prepared, connection| {
                    connection
                        .execute(
                            "UPDATE submissions SET status = 'accepted', pueue_task_id = 99,
                             task_signature = 'pueue-managed-run:v1:post-add'
                             WHERE submission_id = ?1",
                            [&prepared.authority.submission_id],
                        )
                        .unwrap();
                    connection
                        .execute(
                            "UPDATE experiments
                             SET status = 'accepted', pueue_task_id = 99,
                                 task_signature = 'pueue-managed-run:v1:post-add'
                             WHERE experiment_id = ?1",
                            [&prepared.authority.successor_experiment_id],
                        )
                        .unwrap();
                    connection
                        .execute(
                            "UPDATE budget_reservations SET status = 'consumed'
                             WHERE experiment_id = ?1 AND dimension = 'experiment'",
                            [&prepared.authority.successor_experiment_id],
                        )
                        .unwrap();
                },
            ),
            (
                "unreconciled",
                |prepared, connection| {
                    connection
                        .execute(
                            "UPDATE submissions SET status = 'unreconciled'
                             WHERE submission_id = ?1",
                            [&prepared.authority.submission_id],
                        )
                        .unwrap();
                    connection
                        .execute(
                            "UPDATE experiments
                             SET status = 'unreconciled', failure_code = 'identity-mismatch'
                             WHERE experiment_id = ?1",
                            [&prepared.authority.successor_experiment_id],
                        )
                        .unwrap();
                    connection
                        .execute(
                            "UPDATE budget_reservations SET status = 'consumed'
                             WHERE experiment_id = ?1 AND dimension = 'experiment'",
                            [&prepared.authority.successor_experiment_id],
                        )
                        .unwrap();
                },
            ),
        ];
        let failures: Vec<_> = cases
            .into_iter()
            .filter_map(|(label, mutate)| {
                let prepared = checkpoint_dispatch_fixture();
                let connection = prepared.fixture.db.connect().unwrap();
                mutate(&prepared, &connection);
                drop(connection);
                let before = checkpoint_graph_snapshot(&prepared);
                let result = ExperimentRepository::new(&prepared.fixture.db).fail_checkpoint_before_add(
                    &prepared.authority,
                    "research_checkpoint_verification_failed",
                    3_205,
                );
                let after = checkpoint_graph_snapshot(&prepared);
                if result.is_ok() {
                    Some((label, format!("post-shape was accepted: {result:?}")))
                } else if before != after {
                    Some((label, "rejected post-shape mutated graph".to_owned()))
                } else {
                    None
                }
            })
            .collect();
        assert!(failures.is_empty(), "pre-add rejection failures: {failures:?}");
    }

    #[test]
    fn checkpoint_pre_add_history_shared_oracle_preserves_failure_semantics() {
        let completed = checkpoint_dispatch_fixture();
        ExperimentRepository::new(&completed.fixture.db)
            .fail_checkpoint_before_add(
                &completed.authority,
                "research_checkpoint_verification_failed",
                3_206,
            )
            .expect("checkpoint completed pre-add failure");
        completed
            .fixture
            .db
            .connect()
            .expect("checkpoint completed pre-add connection")
            .execute(
                "UPDATE research_reviews
                 SET state = 'completed', operation_stage = NULL,
                     finished_at = 3_207, updated_at = 3_207
                 WHERE review_id = ?1",
                [&completed.fixture.review_id],
            )
            .expect("checkpoint completed pre-add review");
        let mut connection = completed.fixture.db.connect().unwrap();
        let transaction = connection.transaction().unwrap();
        let completed_graph = super::super::campaigns::checkpoint_successor_graph_matches_authority(
            &transaction,
            &completed.authority,
            None,
            None,
        )
        .unwrap();
        transaction.commit().unwrap();

        let arbitrary = checkpoint_dispatch_fixture();
        ExperimentRepository::new(&arbitrary.fixture.db)
            .fail_checkpoint_before_add(
                &arbitrary.authority,
                "research_checkpoint_verification_failed",
                3_208,
            )
            .expect("checkpoint arbitrary pre-add failure");
        arbitrary
            .fixture
            .db
            .connect()
            .expect("checkpoint arbitrary pre-add connection")
            .execute(
                "UPDATE experiments
                 SET failure_code = 'unrelated-failure',
                     failure_fingerprint = 'unrelated-fingerprint'
                 WHERE experiment_id = ?1",
                [&arbitrary.authority.successor_experiment_id],
            )
            .expect("checkpoint arbitrary pre-add experiment");
        let mut connection = arbitrary.fixture.db.connect().unwrap();
        let transaction = connection.transaction().unwrap();
        let arbitrary_graph = super::super::campaigns::checkpoint_successor_graph_matches_authority(
            &transaction,
            &arbitrary.authority,
            None,
            None,
        )
        .unwrap();
        transaction.commit().unwrap();

        assert!(
            completed_graph && !arbitrary_graph,
            "completed_graph={completed_graph}, arbitrary_graph={arbitrary_graph}"
        );
    }

    #[test]
    fn checkpoint_successor_replay_is_exact_and_partial_graph_blocks_without_resources() {
        let prepared = checkpoint_dispatch_fixture();
        let before = checkpoint_graph_snapshot(&prepared);
        let mut connection = prepared.fixture.db.connect().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let owner = match research_ownership_in_transaction(
            &transaction,
            &prepared.fixture.project_id,
            &prepared.fixture.campaign_id,
            &prepared.fixture.experiment_id,
        )
        .unwrap()
        {
            ResearchOwnership::Open(Some(owner)) => owner,
            other => panic!("unexpected replay owner: {other:?}"),
        };
        assert!(matches!(
            super::super::campaigns::accept_checkpoint_successor_in_transaction(
                &transaction,
                &owner,
                &CampaignLimits::default(),
                3_210,
            )
            .unwrap(),
            CheckpointSuccessorAdmission::Ready(_)
        ));
        transaction.commit().unwrap();
        assert_eq!(checkpoint_graph_snapshot(&prepared), before);

        let partial = checkpoint_dispatch_fixture();
        let mut connection = partial.fixture.db.connect().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let owner = match research_ownership_in_transaction(
            &transaction,
            &partial.fixture.project_id,
            &partial.fixture.campaign_id,
            &partial.fixture.experiment_id,
        )
        .unwrap()
        {
            ResearchOwnership::Open(Some(owner)) => owner,
            other => panic!("unexpected partial owner: {other:?}"),
        };
        transaction.commit().unwrap();
        partial
            .fixture
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE proposals SET hypothesis = 'partial replay mutation'
                 WHERE proposal_id = ?1",
                [&partial.authority.proposal_id],
            )
            .unwrap();
        let mutated = checkpoint_graph_snapshot(&partial);
        let mut connection = partial.fixture.db.connect().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert!(matches!(
            super::super::campaigns::accept_checkpoint_successor_in_transaction(
                &transaction,
                &owner,
                &CampaignLimits::default(),
                3_211,
            )
            .unwrap(),
            CheckpointSuccessorAdmission::Blocked
        ));
        transaction.commit().unwrap();
        let after = checkpoint_graph_snapshot(&partial);
        assert_eq!(after.proposal, mutated.proposal);
        assert_eq!(after.submission, mutated.submission);
        assert_eq!(after.experiment, mutated.experiment);
        assert_eq!(after.reservation, mutated.reservation);
        assert_eq!(after.termination, mutated.termination);
        assert_eq!(after.resource_counts, mutated.resource_counts);
        assert_eq!(after.review[0], rusqlite::types::Value::Text("blocked".to_owned()));
    }

    #[test]
    fn source_authority_fresh_reader_rejects_owner_mutation_and_ambiguous_latest() {
        let fixture = source_authority_fixture();
        let connection = fixture
            .db
            .connect()
            .expect("fresh mutation source authority connection");
        connection
            .execute(
                "UPDATE research_reviews SET operation_stage = 'intent'
                 WHERE review_id = ?1",
                [&fixture.review_id],
            )
            .expect("fresh mutation operation stage");
        let owner_mutation = checkpoint_source_authority_for_ready_in_connection(
            &connection,
            &fixture.expected,
            &fixture.request,
        );
        assert!(matches!(
            owner_mutation,
            Err(AppError::Validation {
                field: "research.review",
                ..
            })
        ));
        connection
            .execute(
                "UPDATE research_reviews SET operation_stage = NULL
                 WHERE review_id = ?1",
                [&fixture.review_id],
            )
            .expect("restore fresh mutation operation stage");

        let mut ambiguous_task = fixture.live_task.clone();
        ambiguous_task.started_at = Some("1001".to_owned());
        let ambiguous_signature = task_signature(&ambiguous_task);
        TaskObservationRepository::new(&fixture.db)
            .upsert(&NewTaskObservation::new(
                &fixture.project_id,
                &ambiguous_signature,
                ambiguous_task.id,
                &ambiguous_task.group,
                vec![fixture.wrapped_command.clone()],
                "Running",
                Some(900),
                Some(1_001),
                None,
                None,
                1_001,
            ))
            .expect("fresh mutation ambiguous observation");
        let ambiguous = checkpoint_source_authority_for_ready_in_connection(
            &connection,
            &fixture.expected,
            &fixture.request,
        );
        assert!(matches!(
            ambiguous,
            Err(AppError::Validation {
                field: "task_signature",
                ..
            })
        ));
    }

    #[test]
    fn source_authority_fresh_reader_rejects_graph_phase_and_observation_mutations() {
        assert_source_authority_fresh_error("proposal fields", |fixture, connection| {
            connection
                .execute(
                    "UPDATE proposals SET hypothesis = ?1 WHERE proposal_id = ?2",
                    rusqlite::params!["changed hypothesis", fixture.proposal_id],
                )
                .unwrap();
        });
        assert_source_authority_fresh_error("submission argv", |fixture, connection| {
            connection
                .execute(
                    "UPDATE submissions SET argv_json = ?1 WHERE submission_id = ?2",
                    rusqlite::params![
                        serde_json::to_string(&vec!["python", "other.py"]).unwrap(),
                        fixture.submission_id
                    ],
                )
                .unwrap();
        });
        assert_source_authority_fresh_error("submission metadata", |fixture, connection| {
            connection
                .execute(
                    "UPDATE submissions SET metadata_json = ?1 WHERE submission_id = ?2",
                    rusqlite::params!["{}", fixture.submission_id],
                )
                .unwrap();
        });
        assert_source_authority_fresh_error("graph ids", |fixture, _connection| {
            fixture.expected.owner.campaign_id = "wrong-campaign".to_owned();
        });
        assert_source_authority_fresh_error("context digest", |fixture, connection| {
            connection
                .execute(
                    "UPDATE research_reviews SET context_digest = ?1 WHERE review_id = ?2",
                    rusqlite::params!["b".repeat(64), fixture.review_id],
                )
                .unwrap();
        });
        assert_source_authority_fresh_error("response bytes", |fixture, connection| {
            connection
                .execute(
                    "UPDATE research_reviews SET response_json = ?1 WHERE review_id = ?2",
                    rusqlite::params!["{}", fixture.review_id],
                )
                .unwrap();
        });
        assert_source_authority_fresh_error("request path", |fixture, _connection| {
            fixture.request.path = "wrong-path".to_owned();
        });
        assert_source_authority_fresh_error("review phase", |fixture, connection| {
            connection
                .execute(
                    "UPDATE research_reviews SET state = 'completed' WHERE review_id = ?1",
                    [&fixture.review_id],
                )
                .unwrap();
        });
        assert_source_authority_fresh_error("checkpoint column", |fixture, connection| {
            connection
                .execute(
                    "UPDATE research_reviews SET checkpoint_json = ?1 WHERE review_id = ?2",
                    rusqlite::params!["{}", fixture.review_id],
                )
                .unwrap();
        });
        assert_source_authority_fresh_error("successor link", |fixture, connection| {
            connection
                .execute(
                "UPDATE research_reviews SET successor_experiment_id = ?1
                     WHERE review_id = ?2",
                    rusqlite::params![fixture.experiment_id, fixture.review_id],
                )
                .unwrap();
        });
        assert_source_authority_fresh_error("raw signature", |fixture, _connection| {
            fixture.expected.raw_task_signature = "wrong-raw-signature".to_owned();
        });
        assert_source_authority_fresh_error("managed signature", |fixture, _connection| {
            fixture.expected.owner.managed_task_signature = "wrong-managed-signature".to_owned();
        });
        assert_source_authority_fresh_error("observation group", |fixture, connection| {
            connection
                .execute(
                    "UPDATE task_observations SET pueue_group = 'wrong-group'
                     WHERE project_id = ?1 AND task_signature = ?2",
                    rusqlite::params![
                        fixture.project_id,
                        fixture.expected.raw_task_signature
                    ],
                )
                .unwrap();
        });
        assert_source_authority_fresh_error("stale latest observation", |fixture, _connection| {
            let terminal_task = PueueTask {
                state: "Succeeded".to_owned(),
                ended_at: Some("1_100".to_owned()),
                ..fixture.live_task.clone()
            };
            let terminal_signature = task_signature(&terminal_task);
            TaskObservationRepository::new(&fixture.db)
                .upsert(&NewTaskObservation::new(
                    &fixture.project_id,
                    &terminal_signature,
                    terminal_task.id,
                    &terminal_task.group,
                    vec![fixture.wrapped_command.clone()],
                    "Succeeded",
                    terminal_task
                        .enqueued_at
                        .as_deref()
                        .and_then(|value| value.parse().ok()),
                    terminal_task
                        .started_at
                        .as_deref()
                        .and_then(|value| value.parse().ok()),
                    terminal_task
                        .ended_at
                        .as_deref()
                        .and_then(|value| value.parse().ok()),
                    Some("0".to_owned()),
                    1_100,
                ))
                .unwrap();
        });
    }

    #[test]
    fn source_authority_fresh_reader_accepts_pueue_task_zero() {
        let fixture = source_authority_fixture_with_layout(0, ".", "train.py");
        let result = checkpoint_source_authority_for_preparation(
            &fixture.db,
            &fixture.expected,
            &fixture.request,
        )
        .expect("task zero source authority");
        assert!(matches!(
            result,
            CheckpointSourceAuthorityRead::Supported(_)
        ));
    }

    #[test]
    fn source_authority_preparation_then_new_caller_snapshot_rejects_mutation() {
        let fixture = source_authority_fixture();
        let prepared = checkpoint_source_authority_for_preparation(
            &fixture.db,
            &fixture.expected,
            &fixture.request,
        )
        .expect("prepared source authority");
        assert!(matches!(
            prepared,
            CheckpointSourceAuthorityRead::Supported(_)
        ));

        let mut connection = fixture
            .db
            .connect()
            .expect("caller mutation connection");
        connection
            .execute(
                "UPDATE research_reviews SET response_json = ?1 WHERE review_id = ?2",
                rusqlite::params!["mutated-response", fixture.review_id],
            )
            .expect("caller mutation response");
        let transaction = connection
            .transaction()
            .expect("caller mutation transaction");
        let reread = checkpoint_source_authority_for_ready_in_connection(
            &transaction,
            &fixture.expected,
            &fixture.request,
        );
        assert!(matches!(reread, Err(AppError::Validation { .. })));
        transaction.rollback().expect("caller mutation rollback");
    }

    #[test]
    fn source_authority_prior_source_checks_command_before_unsupported() {
        let fixture = source_authority_fixture();
        let objective_digest = "a".repeat(64);
        let prior_argv = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "--steps".to_owned(),
            "200".to_owned(),
        ];
        let prior = proposals::validate_initial_baseline(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: "tokenized source authority baseline".to_owned(),
                source_experiment_id: None,
                argv: prior_argv.clone(),
                working_directory: ".".to_owned(),
                expected_evidence: vec!["loss".to_owned()],
            },
            &objective_digest,
        )
        .expect("prior source proposal");
        let argv_json = serde_json::to_string(&prior_argv).expect("prior source argv JSON");
        let connection = fixture
            .db
            .connect()
            .expect("prior source authority connection");
        connection
            .execute(
                "UPDATE experiments SET resume_of_experiment_id = ?1
                 WHERE experiment_id = ?1",
                [&fixture.experiment_id],
            )
            .expect("prior source lineage update");

        let coherent_unsupported = checkpoint_source_authority_for_preparation(
            &fixture.db,
            &fixture.expected,
            &fixture.request,
        )
        .expect("coherent prior source authority");
        assert!(matches!(
            coherent_unsupported,
            CheckpointSourceAuthorityRead::Unsupported { .. }
        ));

        connection
            .execute(
                "UPDATE proposals
                 SET argv_json = ?1, canonical_digest = ?2
                 WHERE proposal_id = ?3",
                rusqlite::params![argv_json, prior.canonical_digest(), fixture.proposal_id],
            )
            .expect("prior source proposal update");
        connection
            .execute(
                "UPDATE submissions SET argv_json = ?1 WHERE submission_id = ?2",
                rusqlite::params![
                    serde_json::to_string(&prior_argv).unwrap(),
                    fixture.submission_id
                ],
            )
            .expect("prior source submission update");

        let corrupt = checkpoint_source_authority_for_ready_in_connection(
            &connection,
            &fixture.expected,
            &fixture.request,
        );
        assert!(matches!(
            corrupt,
            Err(AppError::Validation {
                field: "task_observation.command",
                ..
            })
        ));
    }

    #[test]
    fn source_authority_fresh_reader_classifies_unavailable_support() {
        let mut fixture = source_authority_fixture();
        let mut context: Value = serde_json::from_str(&fixture.expected.context_json)
            .expect("unavailable source context");
        context["operations"]["checkpoint_support"] = serde_json::to_value(
            CheckpointSupportEvidenceV1::Unavailable {
                support_version: crate::research_checkpoint::CHECKPOINT_SUPPORT_VERSION,
                reason: "fixture unavailable".to_owned(),
                loader_support: Vec::new(),
                checkpoint_candidates: Vec::new(),
                candidates_complete: false,
                candidates_omitted_at_least: 0,
                candidate_limit: crate::research_checkpoint::MAX_CHECKPOINT_CANDIDATES,
            },
        )
        .expect("unavailable source support JSON");
        let context_json = context.to_string();
        let context_digest = format!("{:x}", Sha256::digest(context_json.as_bytes()));
        let mut response: Value = serde_json::from_str(&fixture.expected.response_json)
            .expect("unavailable source response");
        response["context_digest"] = Value::String(context_digest.clone());
        let response_json = response.to_string();
        let answer = parse_research_answer(response_json.as_bytes())
            .expect("unavailable source answer");
        fixture.expected.context_json = context_json.clone();
        fixture.expected.context_digest = context_digest.clone();
        fixture.expected.response_json = response_json.clone();
        fixture.expected.answer = answer;
        let connection = fixture
            .db
            .connect()
            .expect("unavailable source connection");
        connection
            .execute(
                "UPDATE research_reviews
                 SET context_json = ?1, context_digest = ?2, response_json = ?3
                 WHERE review_id = ?4",
                rusqlite::params![
                    context_json,
                    context_digest,
                    response_json,
                    fixture.review_id
                ],
            )
            .expect("unavailable source persisted proof");
        let result = checkpoint_source_authority_for_ready_in_connection(
            &connection,
            &fixture.expected,
            &fixture.request,
        )
        .expect("unavailable source classification");
        match result {
            CheckpointSourceAuthorityRead::Unsupported { reason } => {
                assert_eq!(reason, "fixture unavailable")
            }
            CheckpointSourceAuthorityRead::Supported(_) => {
                panic!("unavailable source was treated as supported")
            }
        }
    }

    #[test]
    fn source_authority_fresh_reader_rejects_valid_but_wrong_loader_record() {
        let mut fixture = source_authority_fixture();
        let connection = fixture
            .db
            .connect()
            .expect("wrong loader prior source connection");
        connection
            .execute(
                "UPDATE experiments SET resume_of_experiment_id = ?1
                 WHERE experiment_id = ?1",
                [&fixture.experiment_id],
            )
            .expect("wrong loader prior source lineage");
        let mut context: Value = serde_json::from_str(&fixture.expected.context_json)
            .expect("wrong loader source context");
        let loader = &mut context["operations"]["checkpoint_support"]["loader_support"][0];
        loader["argv_token"] = Value::String("trainer.py".to_owned());
        loader["root_relative_path"] = Value::String("trainer.py".to_owned());
        loader["file"]["relative_path"] = Value::String("trainer.py".to_owned());
        let context_json = context.to_string();
        let context_digest = format!("{:x}", Sha256::digest(context_json.as_bytes()));
        let mut response: Value = serde_json::from_str(&fixture.expected.response_json)
            .expect("wrong loader source response");
        response["context_digest"] = Value::String(context_digest.clone());
        let response_json = response.to_string();
        fixture.expected.context_json = context_json.clone();
        fixture.expected.context_digest = context_digest.clone();
        fixture.expected.response_json = response_json.clone();
        fixture.expected.answer = parse_research_answer(response_json.as_bytes())
            .expect("wrong loader source answer");
        connection
            .execute(
                "UPDATE research_reviews
                 SET context_json = ?1, context_digest = ?2, response_json = ?3
                 WHERE review_id = ?4",
                rusqlite::params![
                    context_json,
                    context_digest,
                    response_json,
                    fixture.review_id
                ],
            )
            .expect("wrong loader source persisted proof");
        let result = checkpoint_source_authority_for_ready_in_connection(
            &connection,
            &fixture.expected,
            &fixture.request,
        );
        assert!(matches!(
            result,
            Err(AppError::Validation {
                field: "checkpoint_support.loader",
                ..
            })
        ));
    }

    #[test]
    fn source_authority_fresh_reader_classifies_candidate_runtime_before_source_checks() {
        let mut fixture = source_authority_fixture();
        let revision = "a".repeat(64);
        let mut candidate_task = fixture.live_task.clone();
        candidate_task.command = format!("{} --candidate-runtime", fixture.wrapped_command);
        let candidate_managed_signature = managed_task_run_signature(&candidate_task)
            .expect("candidate managed task signature");
        let mut context: Value = serde_json::from_str(&fixture.expected.context_json)
            .expect("candidate source context");
        context["facts"]["review"]["task_signature"] =
            Value::String(candidate_managed_signature.clone());
        context["facts"]["target"]["task_signature"] =
            Value::String(candidate_managed_signature.clone());
        let context_json = context.to_string();
        let context_digest = format!("{:x}", Sha256::digest(context_json.as_bytes()));
        let mut response: Value = serde_json::from_str(&fixture.expected.response_json)
            .expect("candidate source response");
        response["context_digest"] = Value::String(context_digest.clone());
        let response_json = response.to_string();
        fixture.expected.owner.managed_task_signature = candidate_managed_signature.clone();
        fixture.expected.context_json = context_json.clone();
        fixture.expected.context_digest = context_digest.clone();
        fixture.expected.response_json = response_json.clone();
        fixture.expected.answer = parse_research_answer(response_json.as_bytes())
            .expect("candidate source answer");
        let connection = fixture
            .db
            .connect()
            .expect("candidate source connection");
        connection
            .execute(
                "UPDATE experiments SET code_revision_sha = ?1, task_signature = ?2
                 WHERE experiment_id = ?3",
                rusqlite::params![revision, candidate_managed_signature, fixture.experiment_id],
            )
            .expect("candidate source revision");
        connection
            .execute(
                "UPDATE submissions SET task_signature = ?1 WHERE submission_id = ?2",
                rusqlite::params![candidate_managed_signature, fixture.submission_id],
            )
            .expect("candidate source submission signature");
        connection
            .execute(
                "UPDATE research_reviews
                 SET task_signature = ?1, context_json = ?2, context_digest = ?3,
                     response_json = ?4
                 WHERE review_id = ?5",
                rusqlite::params![
                    candidate_managed_signature,
                    context_json,
                    context_digest,
                    response_json,
                    fixture.review_id
                ],
            )
            .expect("candidate source review identity");
        connection
            .execute(
                "UPDATE task_observations SET command_json = ?1
                 WHERE project_id = ?2 AND task_signature = ?3",
                rusqlite::params![
                    serde_json::to_string(&vec![candidate_task.command]).unwrap(),
                    fixture.project_id,
                    fixture.expected.raw_task_signature
                ],
            )
            .expect("candidate source observation command");
        let result = checkpoint_source_authority_for_ready_in_connection(
            &connection,
            &fixture.expected,
            &fixture.request,
        )
        .expect("candidate source classification");
        assert!(matches!(
            result,
            CheckpointSourceAuthorityRead::Unsupported { .. }
        ));
    }

    fn store_historical_checkpoint(fixture: &SourceAuthorityFixture, serialized: &str) {
        let connection = fixture.db.connect().unwrap();
        connection
            .execute(
                "UPDATE research_reviews
                 SET state = 'completed', checkpoint_json = ?1,
                     finished_at = 4_000, updated_at = 4_000
                 WHERE review_id = ?2",
                rusqlite::params![serialized, fixture.review_id],
            )
            .unwrap();
    }

    fn store_historical_checkpoint_blob(fixture: &SourceAuthorityFixture, bytes: &[u8]) {
        let connection = fixture.db.connect().unwrap();
        connection
            .execute(
                "UPDATE research_reviews
                 SET state = 'completed', checkpoint_json = ?1,
                     finished_at = 4_000, updated_at = 4_000
                 WHERE review_id = ?2",
                rusqlite::params![bytes, fixture.review_id],
            )
            .unwrap();
    }

    fn stored_checkpoint_json(fixture: &SourceAuthorityFixture) -> String {
        fixture
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT checkpoint_json FROM research_reviews WHERE review_id = ?1",
                [&fixture.review_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn read_historical_checkpoint(
        fixture: &SourceAuthorityFixture,
        checkpoint: &crate::research_checkpoint::PreparedCheckpoint,
    ) -> Result<CheckpointSourceAuthority, AppError> {
        let connection = fixture.db.connect()?;
        prepared_checkpoint_source_authority_in_connection(&connection, checkpoint)
    }

    #[test]
    fn source_authority_historical_reader_uses_stored_strict_checkpoint_and_ordinal_one() {
        let fixture = source_authority_fixture();
        let encoded = crate::research_checkpoint::serialize_prepared_checkpoint(
            &fixture.checkpoint,
        )
        .unwrap();
        store_historical_checkpoint(&fixture, &encoded);

        assert_eq!(stored_checkpoint_json(&fixture), encoded);
        assert_eq!(
            crate::research_checkpoint::parse_prepared_checkpoint(&stored_checkpoint_json(
                &fixture,
            ))
            .unwrap(),
            fixture.checkpoint
        );
        let authority = read_historical_checkpoint(&fixture, &fixture.checkpoint).unwrap();
        let selected = select_checkpoint_support(&authority.support, &fixture.checkpoint.request)
            .unwrap();
        assert_eq!(selected.candidate.reference, fixture.candidate_reference);
        assert!(selected
            .candidate
            .reference
            .starts_with("checkpoint:source-authority-experiment:1:"));
    }

    #[test]
    fn source_authority_historical_reader_keeps_captured_running_source_after_closed_gates() {
        let fixture = source_authority_fixture();
        let encoded = crate::research_checkpoint::serialize_prepared_checkpoint(
            &fixture.checkpoint,
        )
        .unwrap();
        store_historical_checkpoint(&fixture, &encoded);

        let terminal_task = PueueTask {
            state: "Succeeded".to_owned(),
            ended_at: Some("3_500".to_owned()),
            ..fixture.live_task.clone()
        };
        let terminal_signature = task_signature(&terminal_task);
        let connection = fixture.db.connect().unwrap();
        connection
            .execute(
                "UPDATE experiments SET status = 'succeeded', finished_at = 3_500
                 WHERE experiment_id = ?1",
                [&fixture.experiment_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE campaigns SET state = 'paused', state_reason = 'historical_fixture',
                         updated_at = 3_501
                 WHERE campaign_id = ?1",
                [&fixture.campaign_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE projects SET enabled = 0, paused = 1, halted_reason = 'historical_fixture',
                         updated_at = 3_502
                 WHERE project_id = ?1",
                [&fixture.project_id],
            )
            .unwrap();
        drop(connection);
        TaskObservationRepository::new(&fixture.db)
            .upsert(&NewTaskObservation::new(
                &fixture.project_id,
                &terminal_signature,
                terminal_task.id,
                &terminal_task.group,
                vec![fixture.wrapped_command.clone()],
                "Succeeded",
                terminal_task
                    .enqueued_at
                    .as_deref()
                    .and_then(|value| value.parse().ok()),
                terminal_task
                    .started_at
                    .as_deref()
                    .and_then(|value| value.parse().ok()),
                terminal_task
                    .ended_at
                    .as_deref()
                    .and_then(|value| value.parse().ok()),
                Some("0".to_owned()),
                4_001,
            ))
            .unwrap();

        let authority = read_historical_checkpoint(&fixture, &fixture.checkpoint).unwrap();
        assert_eq!(
            authority.observation.task_signature,
            fixture.checkpoint.source_raw_task_signature
        );
        assert_eq!(authority.observation.state, "Running");
        assert_eq!(authority.source.experiment.status, ExperimentStatus::Succeeded);
        assert_eq!(authority.source.campaign.state, CampaignState::Paused);
        assert!(!authority.project.enabled);
        assert!(authority.project.paused);
        assert_eq!(
            authority.project.halted_reason.as_deref(),
            Some("historical_fixture")
        );
    }

    #[test]
    fn source_authority_historical_reader_rejects_immutable_mutations_and_invalid_stored_checkpoint() {
        let fixture = source_authority_fixture();
        let encoded = crate::research_checkpoint::serialize_prepared_checkpoint(
            &fixture.checkpoint,
        )
        .unwrap();
        store_historical_checkpoint(&fixture, &encoded);
        let connection = fixture.db.connect().unwrap();
        let changed_argv = serde_json::to_string(&vec!["python", "changed.py"]).unwrap();
        connection
            .execute(
                "UPDATE proposals SET argv_json = ?1 WHERE proposal_id = ?2",
                rusqlite::params![changed_argv, fixture.proposal_id],
            )
            .unwrap();
        drop(connection);
        assert!(read_historical_checkpoint(&fixture, &fixture.checkpoint).is_err());

        let fixture = source_authority_fixture();
        let encoded = crate::research_checkpoint::serialize_prepared_checkpoint(
            &fixture.checkpoint,
        )
        .unwrap();
        store_historical_checkpoint(&fixture, &encoded);
        let connection = fixture.db.connect().unwrap();
        connection
            .execute(
                "UPDATE task_observations SET state = 'Succeeded'
                 WHERE project_id = ?1 AND task_signature = ?2",
                rusqlite::params![
                    fixture.project_id,
                    fixture.checkpoint.source_raw_task_signature
                ],
            )
            .unwrap();
        drop(connection);
        assert!(read_historical_checkpoint(&fixture, &fixture.checkpoint).is_err());

        let fixture = source_authority_fixture();
        store_historical_checkpoint(&fixture, "{");
        assert!(read_historical_checkpoint(&fixture, &fixture.checkpoint).is_err());

        let fixture = source_authority_fixture();
        let blob = b"not-a-text-checkpoint".to_vec();
        store_historical_checkpoint_blob(&fixture, &blob);
        let stored: (String, Vec<u8>) = fixture
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT typeof(checkpoint_json), CAST(checkpoint_json AS BLOB)
                 FROM research_reviews WHERE review_id = ?1",
                [&fixture.review_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, ("blob".to_owned(), blob.clone()));
        assert!(read_historical_checkpoint(&fixture, &fixture.checkpoint).is_err());
        let after: (String, Vec<u8>) = fixture
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT typeof(checkpoint_json), CAST(checkpoint_json AS BLOB)
                 FROM research_reviews WHERE review_id = ?1",
                [&fixture.review_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(after, stored);

        let fixture = source_authority_fixture();
        let oversized = "x".repeat(crate::research_checkpoint::MAX_PREPARED_CHECKPOINT_BYTES + 1);
        store_historical_checkpoint(&fixture, &oversized);
        assert!(read_historical_checkpoint(&fixture, &fixture.checkpoint).is_err());

        let fixture = source_authority_fixture();
        let encoded = crate::research_checkpoint::serialize_prepared_checkpoint(
            &fixture.checkpoint,
        )
        .unwrap();
        store_historical_checkpoint(&fixture, &encoded);
        let mut supplied = fixture.checkpoint.clone();
        supplied.response_digest = "b".repeat(64);
        let supplied_encoded = crate::research_checkpoint::serialize_prepared_checkpoint(&supplied)
            .unwrap();
        assert_eq!(
            crate::research_checkpoint::parse_prepared_checkpoint(&supplied_encoded).unwrap(),
            supplied
        );
        assert_ne!(supplied_encoded, encoded);
        assert!(read_historical_checkpoint(&fixture, &supplied).is_err());
    }

    #[test]
    fn source_authority_historical_reader_accepts_whitespace_checkpoint_without_rewriting_bytes() {
        let fixture = source_authority_fixture();
        let encoded = crate::research_checkpoint::serialize_prepared_checkpoint(
            &fixture.checkpoint,
        )
        .unwrap();
        let whitespace_encoded = format!(" \n\t{encoded}\n");
        store_historical_checkpoint(&fixture, &whitespace_encoded);
        assert_eq!(stored_checkpoint_json(&fixture), whitespace_encoded);

        let authority = read_historical_checkpoint(&fixture, &fixture.checkpoint).unwrap();
        assert_eq!(authority.source.experiment.experiment_id, fixture.experiment_id);
        assert_eq!(stored_checkpoint_json(&fixture), whitespace_encoded);
    }

    #[test]
    fn source_authority_historical_reader_rejects_nested_cwd_record_mutation_after_codec_roundtrip() {
        let fixture = source_authority_nested_fixture();
        let baseline_encoded = crate::research_checkpoint::serialize_prepared_checkpoint(
            &fixture.checkpoint,
        )
        .unwrap();
        assert_eq!(
            crate::research_checkpoint::parse_prepared_checkpoint(&baseline_encoded).unwrap(),
            fixture.checkpoint
        );
        store_historical_checkpoint(&fixture, &baseline_encoded);
        assert_eq!(
            crate::research_checkpoint::parse_prepared_checkpoint(&stored_checkpoint_json(
                &fixture,
            ))
            .unwrap(),
            fixture.checkpoint
        );
        let baseline = read_historical_checkpoint(&fixture, &fixture.checkpoint).unwrap();
        assert_eq!(
            baseline.source.proposal.working_directory,
            ".pueue-agent"
        );

        let mut mutated = fixture.checkpoint.clone();
        let mut mutated_cwd = mutated.source_working_directory_record.clone();
        mutated_cwd.inode += 1;
        mutated.source_working_directory_record = mutated_cwd.clone();
        let mutated_encoded = crate::research_checkpoint::serialize_prepared_checkpoint(&mutated)
            .unwrap();
        assert_eq!(
            crate::research_checkpoint::parse_prepared_checkpoint(&mutated_encoded).unwrap(),
            mutated
        );
        store_historical_checkpoint(&fixture, &mutated_encoded);
        assert_eq!(
            crate::research_checkpoint::parse_prepared_checkpoint(&stored_checkpoint_json(
                &fixture,
            ))
            .unwrap(),
            mutated
        );
        assert!(read_historical_checkpoint(&fixture, &mutated).is_err());
    }
}
