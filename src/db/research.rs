use rusqlite::{params, Connection, OptionalExtension, Row, Transaction, TransactionBehavior};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::AppError;

use super::{database_error, Db};

const MAX_RESEARCH_REVIEW_LIST: i64 = 32;
const MAX_RESEARCH_CANDIDATES: i64 = 32;
const OPEN_REVIEW_STATES: &str = "('pending','running','ready','retry_wait')";
const OPEN_OPERATION_STAGES: &str =
    "('intent','stop_requested','stop_confirmed','successor_reserved')";

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
             ORDER BY observation.started_at, e.experiment_id
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
            current_session,
            current_generation,
            current_attempt,
            current_run,
            current_review_generation,
        ): (String, Option<String>, i64, i64, Option<i64>, i64) = transaction
            .query_row(
                "SELECT r.campaign_id, c.session_id, c.session_generation,
                        r.attempt, r.agent_run_id, r.session_generation
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
        if current_attempt != binding.attempt
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
        let recovery_notes = json!({
            "session_binding": "pending",
            "planned_session_id": binding.session_id,
            "attempt": binding.attempt,
            "budget_reservation_id": binding.budget_reservation_id,
            "recovery_reason": binding.recovery_reason,
        })
        .to_string();
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
        review_id: &str,
        agent_run_id: i64,
        attempt: i64,
        session_generation: i64,
        session_id: &str,
        response_json: &str,
        _rebind_session: bool,
        now: i64,
    ) -> Result<(), AppError> {
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
        let (campaign_id, current_session, current_generation, current_attempt, current_run): (
            String,
            Option<String>,
            i64,
            i64,
            Option<i64>,
        ) = transaction
            .query_row(
                "SELECT campaign_id, (SELECT session_id FROM campaign_research
                                      WHERE campaign_id = research_reviews.campaign_id),
                        session_generation, attempt, agent_run_id
                 FROM research_reviews WHERE review_id = ?1",
                [review_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .map_err(database_error("read research response binding"))?;
        if current_run != Some(agent_run_id)
            || current_attempt != attempt
            || current_generation != session_generation
            || current_session.as_deref() != Some(session_id)
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
                 WHERE review_id = ?3 AND agent_run_id = ?4 AND attempt = ?5
                   AND session_generation = ?6 AND state = 'running'",
                params![
                    response_json,
                    now,
                    review_id,
                    agent_run_id,
                    attempt,
                    session_generation,
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
                 WHERE campaign_id = ?3 AND session_generation = ?4",
                params![session_id, now, campaign_id, session_generation],
            )
            .map_err(database_error("persist owned research session"))?;
        transaction
            .commit()
            .map_err(database_error("commit research response persistence"))
    }

    /// Replace the supervisor's fresh-launch nonce with the exact session ID
    /// emitted by that bound child. The immutable review/run/generation
    /// linkage and the pending nonce are the CAS predicate.
    pub fn confirm_agent_run_session(
        &self,
        review_id: &str,
        agent_run_id: i64,
        attempt: i64,
        session_generation: i64,
        planned_session_id: &str,
        confirmed_session_id: &str,
        now: i64,
    ) -> Result<(), AppError> {
        validate_session_id(planned_session_id)?;
        validate_session_id(confirmed_session_id)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin research session confirmation"))?;
        let (
            campaign_id,
            current_session,
            current_generation,
            current_attempt,
            current_run,
            review_generation,
            state,
            notes_json,
        ): (
            String,
            Option<String>,
            i64,
            i64,
            Option<i64>,
            i64,
            String,
            Option<String>,
        ) = transaction
            .query_row(
                "SELECT r.campaign_id, c.session_id, c.session_generation,
                        r.attempt, r.agent_run_id, r.session_generation,
                        r.state, r.notes_json
                 FROM research_reviews AS r
                 JOIN campaign_research AS c ON c.campaign_id = r.campaign_id
                 WHERE r.review_id = ?1",
                [review_id],
                |row| Ok((
                    row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?,
                    row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?,
                )),
            )
            .map_err(database_error("read research session confirmation"))?;
        if current_run != Some(agent_run_id)
            || current_attempt != attempt
            || current_generation != session_generation
            || review_generation != session_generation
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
                == Some(planned_session_id)
            && notes
                .get("confirmed_session_id")
                .and_then(serde_json::Value::as_str)
                == Some(confirmed_session_id);
        if already_confirmed {
            return transaction
                .commit()
                .map_err(database_error("commit idempotent research session confirmation"));
        }
        if current_session.as_deref() != Some(planned_session_id) {
            return Err(validation_error(
                "research.binding",
                "session confirmation does not match the pending session nonce",
            ));
        }
        notes["session_binding"] = json!("confirmed");
        notes["planned_session_id"] = json!(planned_session_id);
        notes["confirmed_session_id"] = json!(confirmed_session_id);
        let changed = transaction
            .execute(
                "UPDATE campaign_research
                 SET session_id = ?1, updated_at = ?2
                 WHERE campaign_id = ?3 AND session_generation = ?4
                   AND session_id = ?5",
                params![confirmed_session_id, now, campaign_id, session_generation, planned_session_id],
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
                 WHERE review_id = ?3 AND agent_run_id = ?4 AND attempt = ?5
                   AND session_generation = ?6 AND state = 'running'",
                params![notes.to_string(), now, review_id, agent_run_id, attempt, session_generation],
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
        review_id: &str,
        agent_run_id: i64,
        failure_code: &str,
        now: i64,
    ) -> Result<(), AppError> {
        if failure_code.is_empty() || failure_code.len() > 128 {
            return Err(validation_error("research.failure_code", "must be bounded"));
        }
        let connection = self.db.connect()?;
        let changed = connection
            .execute(
                "UPDATE research_reviews
                 SET state = 'retry_wait', failure_code = ?1,
                     finished_at = ?2, updated_at = ?2
                 WHERE review_id = ?3 AND agent_run_id = ?4 AND state = 'running'",
                params![failure_code, now, review_id, agent_run_id],
            )
            .map_err(database_error("record failed research run"))?;
        if changed != 1 {
            return Err(validation_error(
                "research.review",
                "failed research run is not the bound running review",
            ));
        }
        Ok(())
    }

    /// Set a failed run to retry and retire only its still-pending nonce.
    /// A previously confirmed session is intentionally retained for exact
    /// recovery after a schema or response failure.
    pub fn fail_agent_run_and_clear_session(
        &self,
        review_id: &str,
        agent_run_id: i64,
        attempt: i64,
        session_generation: i64,
        planned_session_id: &str,
        failure_code: &str,
        now: i64,
    ) -> Result<(), AppError> {
        validate_session_id(planned_session_id)?;
        if failure_code.is_empty() || failure_code.len() > 128 {
            return Err(validation_error("research.failure_code", "must be bounded"));
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin failed research session retirement"))?;
        let (campaign_id, current_generation): (String, i64) = transaction
            .query_row(
                "SELECT r.campaign_id, c.session_generation
                 FROM research_reviews AS r
                 JOIN campaign_research AS c ON c.campaign_id = r.campaign_id
                 WHERE r.review_id = ?1 AND r.agent_run_id = ?2
                   AND r.attempt = ?3 AND r.session_generation = ?4
                   AND r.state = 'running'",
                params![review_id, agent_run_id, attempt, session_generation],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(database_error("read failed research session retirement"))?;
        let changed = transaction
            .execute(
                "UPDATE research_reviews
                 SET state = 'retry_wait', failure_code = ?1,
                     finished_at = ?2, updated_at = ?2
                 WHERE review_id = ?3 AND agent_run_id = ?4 AND state = 'running'",
                params![failure_code, now, review_id, agent_run_id],
            )
            .map_err(database_error("record failed research run"))?;
        if changed != 1 {
            return Err(validation_error(
                "research.review",
                "failed research run is not the bound running review",
            ));
        }
        transaction
            .execute(
                "UPDATE campaign_research
                 SET session_id = NULL, updated_at = ?1
                 WHERE campaign_id = ?2 AND session_generation = ?3
                   AND session_id = ?4",
                params![now, campaign_id, current_generation, planned_session_id],
            )
            .map_err(database_error("retire pending research session"))?;
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

fn next_research_due(start: i64, interval_minutes: u32) -> Result<Option<i64>, AppError> {
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
    if binding.review_id.is_empty()
        || binding.review_id.len() > 256
        || binding.review_id.chars().any(char::is_control)
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
