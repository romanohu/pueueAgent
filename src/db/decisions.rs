use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use sha2::{Digest, Sha256};

use crate::{
    decision_evidence::{
        decision_context_schema_version, validate_stored_decision_context,
    },
    diagnostics::MAX_EVENT_LIST_LIMIT,
    execution_policy::CampaignLimits,
    models::{
        AgentRunStatus, CampaignState, DecisionAttempt, DecisionAttemptState, DecisionCycle,
        DecisionCycleState, Event, EventKind, EventStatus, ExperimentStatus, NewEvent,
    },
    output::bounded_redacted_text,
    research_protocol::parse_research_answer,
    AppError,
};

use super::{
    database_error,
    repositories::insert_event_idempotent_in_transaction,
    research::{
        context_evidence_refs, research_context_identity_matches,
        research_ownership_in_transaction, ResearchOwnership, ResearchOwnershipSnapshot,
    },
    Db,
};

const MAX_DECISION_PAYLOAD_BYTES: usize = 128 * 1024;
const MAX_DECISION_DIGEST_BYTES: usize = 256;
const MAX_DECISION_CODE_BYTES: usize = 128;
const MAX_DECISION_SUMMARY_BYTES: usize = 2_048;
const MAX_TERMINAL_DECISION_BACKFILL: i64 = 128;

pub(crate) fn campaign_decision_dedup_key(cycle_id: &str) -> String {
    format!("campaign-decision:v1:{cycle_id}")
}

pub(crate) struct TerminalDecisionEventProjection<'a> {
    pub task_id: i64,
    pub managed_task_signature: &'a str,
    pub group: &'a str,
    pub state: &'a str,
    pub enqueued_at: Option<i64>,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub exit_code: Option<i32>,
}

const DECISION_CYCLE_SELECT: &str = "SELECT
    cycle_id, campaign_id, source_experiment_id, source_terminal_at, state, next_wake_at,
    consecutive_failed_attempts, last_decision_kind, last_failure_code,
    last_failure_summary, created_at, updated_at
    FROM decision_cycles";
const DECISION_ATTEMPT_SELECT: &str = "SELECT
    cycle_id, attempt_number, state, context_schema_version, context_json, context_digest,
    agent_run_id, decision_json, decision_digest, decision_kind, failure_code,
    failure_summary, created_at, started_at, finished_at
    FROM decision_attempts";
const GLOBAL_DUE_WAIT_DECISION_SQL: &str =
    "SELECT cycle_id
     FROM decision_cycles INDEXED BY decision_cycles_state_wake_source_order_idx
     WHERE state = 'waiting' AND next_wake_at <= ?1
     ORDER BY next_wake_at, source_terminal_at, source_experiment_id, cycle_id
     LIMIT ?2";
const CAMPAIGN_PENDING_DECISION_SQL: &str =
    "SELECT cycle_id
     FROM decision_cycles INDEXED BY decision_cycles_campaign_state_source_order_idx
     WHERE campaign_id = ?1 AND state = 'pending'
     ORDER BY source_terminal_at, source_experiment_id, cycle_id
     LIMIT 1";

fn query_decision_cycle_ids(
    connection: &Connection,
    sql: &str,
    parameters: &[&dyn rusqlite::ToSql],
    operation: &'static str,
) -> Result<Vec<String>, AppError> {
    let mut statement = connection
        .prepare(sql)
        .map_err(database_error(operation))?;
    let candidates = statement
        .query_map(rusqlite::params_from_iter(parameters.iter()), |row| row.get(0))
        .map_err(database_error(operation))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(database_error(operation))?;
    Ok(candidates)
}

pub struct DecisionRepository<'db> {
    db: &'db Db,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionReservation {
    pub cycle_id: String,
    pub campaign_id: String,
    pub source_experiment_id: String,
    pub attempt_number: i64,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionRecovery {
    pub reservation: DecisionReservation,
    pub state: DecisionAttemptState,
    pub agent_run_id: Option<i64>,
    pub agent_run_status: Option<AgentRunStatus>,
    pub event_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadyDecision {
    pub(crate) reservation: DecisionReservation,
    pub(crate) context_schema_version: Option<i64>,
    pub(crate) context_json: Option<String>,
    pub(crate) context_digest: Option<String>,
    pub(crate) decision_json: String,
    pub(crate) decision_digest: String,
    pub(crate) decision_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionDoctorProjection {
    pub cycle: DecisionCycle,
    pub attempt_count: i64,
    pub active_attempt_number: Option<i64>,
    pub active_attempt_state: Option<DecisionAttemptState>,
    pub active_agent_run_id: Option<i64>,
}

struct TerminalDecisionBackfill {
    project_id: String,
    campaign_id: String,
    experiment_id: String,
    experiment_status: ExperimentStatus,
    pueue_task_id: i64,
    managed_task_signature: String,
    pueue_group: String,
    state: String,
    enqueued_at: Option<i64>,
    started_at: Option<i64>,
    ended_at: Option<i64>,
    terminal_result_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResearchCycleAttachment {
    pub cycle: DecisionCycle,
    pub event: Event,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TerminalCyclePublication {
    Published { cycle: DecisionCycle, event: Event },
    Existing { cycle: DecisionCycle, event: Event },
    Deferred { cycle: DecisionCycle },
}

struct DecisionAuthority {
    cycle: DecisionCycle,
    project_id: String,
    campaign_state: CampaignState,
    project_enabled: bool,
    project_paused: bool,
    project_halted: bool,
    experiment_campaign_id: String,
    experiment_status: ExperimentStatus,
    objective_digest: String,
}

struct RequeuedDecisionAttempt {
    cycle: DecisionCycle,
    transitioned: bool,
}

impl<'db> DecisionRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn validate_launch_authority(
        &self,
        project_id: &str,
        reservation: &DecisionReservation,
    ) -> Result<(), AppError> {
        let connection = self.db.connect()?;
        let authority = read_authority(&connection, &reservation.cycle_id)?;
        validate_reservation_lineage(&authority, reservation)?;
        validate_active_authority(&authority, Some(project_id))
    }

    pub fn validate_launch_context(
        &self,
        project_id: &str,
        reservation: &DecisionReservation,
        context_json: &str,
        context_digest: &str,
    ) -> Result<String, AppError> {
        let connection = self.db.connect()?;
        let authority = read_authority(&connection, &reservation.cycle_id)?;
        validate_reservation_lineage(&authority, reservation)?;
        validate_active_authority(&authority, Some(project_id))?;
        let attempt = read_attempt(
            &connection,
            &reservation.cycle_id,
            reservation.attempt_number,
        )?;
        let context_schema_version = decision_context_schema_version(context_json)?;
        if attempt.state != DecisionAttemptState::EvidenceReady
            || attempt.context_schema_version != Some(i64::from(context_schema_version))
            || attempt.context_json.as_deref() != Some(context_json)
            || attempt.context_digest.as_deref() != Some(context_digest)
        {
            return Err(validation_error(
                "decision_context",
                "must exactly match the reserved evidence-ready attempt",
            ));
        }
        validate_payload("context_json", context_json)?;
        validate_token("context_digest", context_digest, MAX_DECISION_DIGEST_BYTES)?;
        if format!("{:x}", Sha256::digest(context_json.as_bytes())) != context_digest {
            return Err(validation_error(
                "context_digest",
                "does not match the persisted decision context",
            ));
        }
        validate_stored_decision_context(
            context_json,
            &authority.objective_digest,
            &authority.cycle.source_experiment_id,
        )
    }

    pub fn ensure_cycle_for_terminal(
        &self,
        campaign_id: &str,
        experiment_id: &str,
        now: i64,
    ) -> Result<DecisionCycle, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin terminal decision cycle creation"))?;
        let cycle = ensure_terminal_cycle_in_transaction(
            &transaction,
            campaign_id,
            experiment_id,
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit terminal decision cycle creation"))?;
        Ok(cycle)
    }

    /// Attach one confirmed research stop to the deterministic terminal
    /// decision cycle while the caller's transaction is still open.  The
    /// caller owns the transaction and therefore also owns the final commit.
    pub(crate) fn attach_research_terminal_cycle_in_transaction(
        transaction: &Transaction<'_>,
        expected: &ResearchOwnershipSnapshot,
        terminal_event: &NewEvent,
        now: i64,
    ) -> Result<ResearchCycleAttachment, AppError> {
        if !matches!(
            expected.operation_stage.as_deref(),
            Some("stop_confirmed") | None
        )
            || expected.recovery_required
            || expected.successor_experiment_id.is_some()
            || expected.agent_run_id.is_none()
            || expected.event_id.is_none()
            || expected.termination_request_id.is_none()
        {
            return Err(validation_error(
                "research.handoff",
                "must be the exact confirmed research owner without a successor",
            ));
        }

        let ownership = research_ownership_in_transaction(
            transaction,
            &expected.project_id,
            &expected.campaign_id,
            &expected.source_experiment_id,
        )?;
        let (owner, already_attached) = match ownership {
            ResearchOwnership::Open(Some(owner)) => {
                if !research_snapshot_matches(&owner, expected)
                    || owner.operation_stage.as_deref() != Some("stop_confirmed")
                    || owner.recovery_required
                {
                    return Err(validation_error(
                        "research.handoff",
                        "research ownership changed before terminal attachment",
                    ));
                }
                (owner, false)
            }
            ResearchOwnership::Attached(owner) => {
                if !research_snapshot_stable_matches(&owner, expected)
                    || owner.decision_cycle_id.as_deref()
                        != Some(decision_cycle_id(
                            &expected.campaign_id,
                            &expected.source_experiment_id,
                        )
                            .as_str())
                    || owner.successor_experiment_id.is_some()
                {
                    return Err(validation_error(
                        "research.handoff",
                        "already attached research ownership has conflicting lineage",
                    ));
                }
                (owner, true)
            }
            ResearchOwnership::None | ResearchOwnership::Open(None) => {
                return Err(validation_error(
                    "research.handoff",
                    "confirmed research ownership is missing or ambiguous",
                ));
            }
        };

        let source_status: Option<ExperimentStatus> = transaction
            .query_row(
                "SELECT status FROM experiments
                 WHERE experiment_id = ?1 AND campaign_id = ?2",
                params![owner.source_experiment_id, owner.campaign_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("read confirmed research source"))?;
        if !source_status.is_some_and(is_terminal) {
            return Err(validation_error(
                "research.handoff",
                "source experiment must be terminal before attachment",
            ));
        }
        let confirmed_request: bool = transaction
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM termination_requests
                     WHERE request_id = ?1 AND project_id = ?2
                       AND status = 'confirmed'
                       AND task_signature LIKE 'pueue-task:v1:%'
                 )",
                params![owner.termination_request_id, owner.project_id],
                |row| row.get(0),
            )
            .map_err(database_error("read confirmed research termination"))?;
        if !confirmed_request {
            return Err(validation_error(
                "research.handoff",
                "research termination request must be confirmed",
            ));
        }

        let campaign_project: String = transaction
            .query_row(
                "SELECT project_id FROM campaigns WHERE campaign_id = ?1",
                [&owner.campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error("read research handoff campaign project"))?;
        if campaign_project != owner.project_id {
            return Err(validation_error(
                "research.handoff",
                "research campaign project does not match its owner",
            ));
        }

        let cycle_id = decision_cycle_id(&owner.campaign_id, &owner.source_experiment_id);
        validate_research_terminal_event_input(
            terminal_event,
            &owner.project_id,
            &owner.campaign_id,
            &owner.source_experiment_id,
            &cycle_id,
        )?;
        let cycle = ensure_terminal_cycle_in_transaction(
            transaction,
            &owner.campaign_id,
            &owner.source_experiment_id,
            now,
        )?;

        let (
            state,
            operation_stage,
            context_json,
            context_digest,
            response_json,
            notes_json,
            linked_cycle_id,
            successor_experiment_id,
            objective_digest,
        ): (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
        ) = transaction
            .query_row(
                "SELECT research_reviews.state, operation_stage, context_json, context_digest,
                        response_json, notes_json, decision_cycle_id,
                        successor_experiment_id, campaigns.objective_digest
                 FROM research_reviews
                 JOIN campaigns ON campaigns.campaign_id = research_reviews.campaign_id
                 WHERE research_reviews.review_id = ?1",
                [&owner.review_id],
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
                    ))
                },
            )
            .map_err(database_error("read research handoff review"))?;
        if successor_experiment_id.is_some() {
            return Err(validation_error(
                "research.handoff",
                "research handoff cannot already have a successor",
            ));
        }

        let expected_cycle = Some(cycle_id.as_str());
        if already_attached {
            if state != "completed"
                || operation_stage.is_some()
                || linked_cycle_id.as_deref() != expected_cycle
            {
                return Err(validation_error(
                    "research.handoff",
                    "already attached research review is not complete",
                ));
            }
        } else if state != "ready"
            || operation_stage.as_deref() != Some("stop_confirmed")
            || linked_cycle_id.is_some()
        {
            return Err(validation_error(
                "research.handoff",
                "research review is not at the exact stop-confirmed stage",
            ));
        }

        let notes_json = research_handoff_notes(
            &owner,
            context_json.as_deref(),
            context_digest.as_deref(),
            response_json.as_deref(),
            notes_json.as_deref(),
            &objective_digest,
        )?;

        let existing_event = read_research_terminal_event_in_transaction(
            transaction,
            &owner.project_id,
            &cycle_id,
        )?;
        let event = if let Some(event) = existing_event {
            validate_research_terminal_event(
                &event,
                terminal_event,
                &owner.project_id,
                &owner.campaign_id,
                &owner.source_experiment_id,
                &cycle_id,
            )?;
            if !already_attached {
                validate_unclaimed_research_terminal_event(transaction, &event, &cycle_id)?;
            }
            event
        } else {
            if already_attached {
                return Err(validation_error(
                    "research.handoff",
                    "attached research review is missing its terminal event",
                ));
            }
            // The event is inserted only after the review CAS below.  Both
            // mutations remain in this caller-owned transaction.
            Event {
                event_id: 0,
                project_id: terminal_event.project_id.clone(),
                campaign_id: terminal_event.campaign_id.clone(),
                experiment_id: terminal_event.experiment_id.clone(),
                kind: terminal_event.kind,
                dedup_key: terminal_event.dedup_key.clone(),
                payload: terminal_event.payload.clone(),
                status: EventStatus::Pending,
                attempts: 0,
                not_before: terminal_event.not_before,
                lease_until: None,
                created_at: terminal_event.created_at,
                completed_at: None,
                last_error: None,
            }
        };

        if already_attached {
            return Ok(ResearchCycleAttachment { cycle, event });
        }

        let changed = transaction
            .execute(
                "UPDATE research_reviews
                 SET decision_cycle_id = ?1, state = 'completed',
                     operation_stage = NULL, notes_json = ?2,
                     finished_at = ?3, updated_at = ?3
                 WHERE review_id = ?4 AND campaign_id = ?5
                   AND experiment_id = ?6 AND task_signature = ?7
                   AND attempt = ?8 AND session_generation = ?9
                   AND agent_run_id = ?10 AND event_id = ?11
                   AND termination_request_id = ?12
                   AND state = 'ready' AND operation_stage = 'stop_confirmed'
                   AND decision_cycle_id IS NULL
                   AND successor_experiment_id IS NULL",
                params![
                    cycle_id,
                    notes_json,
                    now,
                    owner.review_id,
                    owner.campaign_id,
                    owner.source_experiment_id,
                    owner.managed_task_signature,
                    owner.attempt,
                    owner.session_generation,
                    owner.agent_run_id,
                    owner.event_id,
                    owner.termination_request_id,
                ],
            )
            .map_err(database_error("attach research terminal decision cycle"))?;
        if changed != 1 {
            return Err(validation_error(
                "research.handoff",
                "research review changed before terminal attachment",
            ));
        }

        let event = if event.event_id == 0 {
            let (event, _) = insert_event_idempotent_in_transaction(transaction, terminal_event)?;
            event
        } else {
            event
        };
        Ok(ResearchCycleAttachment { cycle, event })
    }

    pub(crate) fn terminal_cycle_id(campaign_id: &str, experiment_id: &str) -> String {
        decision_cycle_id(campaign_id, experiment_id)
    }

    pub(crate) fn terminal_decision_event(
        project_id: &str,
        campaign_id: &str,
        source_experiment_id: &str,
        observation: TerminalDecisionEventProjection<'_>,
        now: i64,
    ) -> NewEvent {
        let cycle_id = Self::terminal_cycle_id(campaign_id, source_experiment_id);
        NewEvent::new(
            project_id,
            EventKind::CampaignDecision,
            campaign_decision_dedup_key(&cycle_id),
            serde_json::json!({
                "source": "terminal_experiment",
                "cycle_id": cycle_id,
                "source_experiment_id": source_experiment_id,
                "terminal_observation": {
                    "task_id": observation.task_id,
                    "task_signature": observation.managed_task_signature,
                    "group": observation.group,
                    "state": observation.state,
                    "enqueued_at": observation.enqueued_at,
                    "started_at": observation.started_at,
                    "ended_at": observation.ended_at,
                    "exit_code": observation.exit_code,
                },
            }),
            now,
            now,
        )
        .with_campaign_lineage(
            campaign_id.to_owned(),
            Some(source_experiment_id.to_owned()),
        )
    }

    pub fn publish_terminal_cycle_event(
        &self,
        campaign_id: &str,
        experiment_id: &str,
        event: &NewEvent,
        now: i64,
    ) -> Result<(DecisionCycle, Event), AppError> {
        match self.publish_terminal_cycle_event_outcome(campaign_id, experiment_id, event, now)? {
            TerminalCyclePublication::Published { cycle, event }
            | TerminalCyclePublication::Existing { cycle, event } => Ok((cycle, event)),
            TerminalCyclePublication::Deferred { .. } => Err(AppError::Runtime {
                operation: "defer terminal decision publication",
            }),
        }
    }

    pub(crate) fn publish_terminal_cycle_event_outcome(
        &self,
        campaign_id: &str,
        experiment_id: &str,
        event: &NewEvent,
        now: i64,
    ) -> Result<TerminalCyclePublication, AppError> {
        let expected_cycle_id = decision_cycle_id(campaign_id, experiment_id);
        if event.project_id.is_empty()
            || event.kind != EventKind::CampaignDecision
            || event.dedup_key != campaign_decision_dedup_key(&expected_cycle_id)
            || event.campaign_id.as_deref() != Some(campaign_id)
            || event.experiment_id.as_deref() != Some(experiment_id)
            || event.payload.get("source").and_then(serde_json::Value::as_str)
                != Some("terminal_experiment")
            || event.payload.get("cycle_id").and_then(serde_json::Value::as_str)
                != Some(expected_cycle_id.as_str())
            || event
                .payload
                .get("source_experiment_id")
                .and_then(serde_json::Value::as_str)
                != Some(experiment_id)
        {
            return Err(validation_error(
                "campaign_decision_event",
                "must carry the exact terminal cycle and experiment lineage",
            ));
        }
        let expected_payload = event.payload.clone();
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin terminal decision publication"))?;
        let cycle = ensure_terminal_cycle_in_transaction(
            &transaction,
            campaign_id,
            experiment_id,
            now,
        )?;
        let expected_project_id: String = transaction
            .query_row(
                "SELECT project_id FROM campaigns WHERE campaign_id = ?1",
                [campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error("read terminal decision project lineage"))?;
        if event.project_id != expected_project_id {
            return Err(validation_error(
                "project_id",
                "terminal decision event must belong to the campaign project",
            ));
        }
        let ownership = research_ownership_in_transaction(
            &transaction,
            &event.project_id,
            campaign_id,
            experiment_id,
        )?;
        if matches!(ownership, ResearchOwnership::Open(_)) {
            validate_existing_terminal_event_in_transaction(
                &transaction,
                event,
                &expected_cycle_id,
            )?;
            transaction
                .commit()
                .map_err(database_error("commit deferred terminal decision publication"))?;
            return Ok(TerminalCyclePublication::Deferred { cycle });
        }
        let (event, inserted) = insert_event_idempotent_in_transaction(&transaction, event)?;
        if event.kind != EventKind::CampaignDecision
            || event.payload != expected_payload
            || event.payload.get("source").and_then(serde_json::Value::as_str)
                != Some("terminal_experiment")
            || event.payload.get("cycle_id").and_then(serde_json::Value::as_str)
                != Some(expected_cycle_id.as_str())
            || event
                .payload
                .get("source_experiment_id")
                .and_then(serde_json::Value::as_str)
                != Some(experiment_id)
        {
            return Err(validation_error(
                "campaign_decision_event",
                "conflicts with the existing terminal decision projection",
            ));
        }
        transaction
            .commit()
            .map_err(database_error("commit terminal decision publication"))?;
        Ok(if inserted {
            TerminalCyclePublication::Published { cycle, event }
        } else {
            TerminalCyclePublication::Existing { cycle, event }
        })
    }

    pub fn backfill_terminal_cycle_events(&self, now: i64) -> Result<usize, AppError> {
        let connection = self.db.connect()?;
        let backfills = {
            let mut statement = connection
                .prepare(
                    "SELECT p.project_id, e.campaign_id, e.experiment_id, e.status,
                            e.pueue_task_id, e.task_signature, observation.pueue_group,
                            observation.state, observation.enqueued_at,
                            observation.started_at, observation.ended_at,
                            observation.result
                     FROM experiments e
                     JOIN campaigns c ON c.campaign_id = e.campaign_id
                     JOIN projects p ON p.project_id = c.project_id
                     JOIN task_observations observation
                       ON observation.project_id = p.project_id
                      AND observation.pueue_task_id = e.pueue_task_id
                      AND observation.pueue_group = p.pueue_group
                      AND lower(observation.state) IN
                          ('done','failed','killed','finished','success')
                      AND observation.task_signature = (
                          SELECT MIN(candidate.task_signature)
                          FROM task_observations candidate
                          WHERE candidate.project_id = p.project_id
                            AND candidate.pueue_task_id = e.pueue_task_id
                            AND candidate.pueue_group = p.pueue_group
                            AND lower(candidate.state) IN
                                ('done','failed','killed','finished','success')
                          HAVING COUNT(*) = 1
                      )
                     LEFT JOIN decision_cycles dc
                       ON dc.campaign_id = e.campaign_id
                      AND dc.source_experiment_id = e.experiment_id
                     LEFT JOIN events ev
                       ON ev.project_id = p.project_id
                      AND ev.campaign_id = e.campaign_id
                      AND ev.experiment_id = e.experiment_id
                      AND ev.kind = 'campaign_decision'
                      AND ev.dedup_key = 'campaign-decision:v1:' || dc.cycle_id
                      AND json_extract(ev.payload_json, '$.source') = 'terminal_experiment'
                      AND json_extract(ev.payload_json, '$.cycle_id') = dc.cycle_id
                      AND json_extract(ev.payload_json, '$.source_experiment_id') = e.experiment_id
                     WHERE p.enabled = 1
                       AND e.status IN ('succeeded','failed','cancelled')
                       AND ev.event_id IS NULL
                     ORDER BY e.finished_at, e.experiment_id
                     LIMIT ?1",
                )
                .map_err(database_error("prepare terminal decision backfill"))?;
            let rows = statement
                .query_map([MAX_TERMINAL_DECISION_BACKFILL], |row| {
                    Ok(TerminalDecisionBackfill {
                        project_id: row.get(0)?,
                        campaign_id: row.get(1)?,
                        experiment_id: row.get(2)?,
                        experiment_status: row.get(3)?,
                        pueue_task_id: row.get(4)?,
                        managed_task_signature: row.get(5)?,
                        pueue_group: row.get(6)?,
                        state: row.get(7)?,
                        enqueued_at: row.get(8)?,
                        started_at: row.get(9)?,
                        ended_at: row.get(10)?,
                        terminal_result_json: row.get(11)?,
                    })
                })
                .map_err(database_error("query terminal decision backfill"))?;
            rows
                .collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read terminal decision backfill"))?
        };
        drop(connection);

        let mut count = 0;
        for backfill in backfills {
            let terminal_result = match backfill
                .terminal_result_json
                .as_deref()
                .map(serde_json::from_str::<serde_json::Value>)
                .transpose()
            {
                Ok(result) => result,
                Err(_) => continue,
            };
            if !terminal_observation_matches_experiment(
                backfill.experiment_status,
                &backfill.state,
                terminal_result.as_ref(),
            ) {
                continue;
            }
            let exit_code = terminal_result.as_ref().and_then(stored_terminal_exit_code);
            let event = Self::terminal_decision_event(
                &backfill.project_id,
                &backfill.campaign_id,
                &backfill.experiment_id,
                TerminalDecisionEventProjection {
                    task_id: backfill.pueue_task_id,
                    managed_task_signature: &backfill.managed_task_signature,
                    group: &backfill.pueue_group,
                    state: &backfill.state,
                    enqueued_at: backfill.enqueued_at,
                    started_at: backfill.started_at,
                    ended_at: backfill.ended_at,
                    exit_code,
                },
                now,
            );
            let outcome = self.publish_terminal_cycle_event_outcome(
                &backfill.campaign_id,
                &backfill.experiment_id,
                &event,
                now,
            )?;
            if matches!(
                outcome,
                TerminalCyclePublication::Published { .. }
                    | TerminalCyclePublication::Existing { .. }
            ) {
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn reserve_next_attempt(
        &self,
        project_id: &str,
        cycle_id: &str,
        now: i64,
    ) -> Result<Option<DecisionReservation>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin decision attempt reservation"))?;
        let authority = read_authority(&transaction, cycle_id)?;
        validate_active_authority(&authority, Some(project_id))?;
        validate_research_decision_ownership(&transaction, &authority, cycle_id)?;

        if authority.cycle.state != DecisionCycleState::Pending {
            transaction
                .commit()
                .map_err(database_error("commit unavailable decision cycle reservation"))?;
            return Ok(None);
        }

        let reusable_attempt_number = transaction
            .query_row(
                "SELECT attempt_number FROM decision_attempts
                 WHERE cycle_id = ?1 AND state IN ('reserved','evidence_ready')
                   AND agent_run_id IS NULL
                 ORDER BY attempt_number DESC LIMIT 1",
                [cycle_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(database_error("find reusable unbound decision attempt"))?;
        let campaign_active_attempt_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*)
                    FROM decision_attempts da
                    JOIN decision_cycles dc ON dc.cycle_id = da.cycle_id
                    WHERE dc.campaign_id = ?1
                      AND da.state IN ('reserved','evidence_ready','running')",
                [&authority.cycle.campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error("count active campaign decision attempts"))?;
        if let Some(attempt_number) = reusable_attempt_number {
            if campaign_active_attempt_count != 1 {
                return Err(validation_error(
                    "decision_attempt",
                    "reusable unbound attempt must be the campaign's only active attempt",
                ));
            }
            let attempt = read_attempt(&transaction, cycle_id, attempt_number)?;
            let updated = transaction
                .execute(
                    "UPDATE decision_cycles
                     SET state = 'analyzing', next_wake_at = NULL, updated_at = ?1
                     WHERE cycle_id = ?2 AND state = 'pending'",
                    params![now, cycle_id],
                )
                .map_err(database_error("reactivate reusable decision attempt"))?;
            if updated != 1 {
                return Err(validation_error(
                    "decision_cycle",
                    "state changed while reactivating an unbound attempt",
                ));
            }
            let reservation = DecisionReservation {
                cycle_id: cycle_id.to_owned(),
                campaign_id: authority.cycle.campaign_id,
                source_experiment_id: authority.cycle.source_experiment_id,
                attempt_number,
                created_at: attempt.created_at,
            };
            transaction
                .commit()
                .map_err(database_error("commit reusable decision attempt reservation"))?;
            return Ok(Some(reservation));
        }
        if campaign_active_attempt_count != 0 {
            transaction
                .commit()
                .map_err(database_error("commit contended decision attempt reservation"))?;
            return Ok(None);
        }

        let attempt_number: i64 = transaction
            .query_row(
                "SELECT COALESCE(MAX(attempt_number), 0) + 1
                 FROM decision_attempts WHERE cycle_id = ?1",
                [cycle_id],
                |row| row.get(0),
            )
            .map_err(database_error("allocate decision attempt number"))?;
        transaction
            .execute(
                "INSERT INTO decision_attempts (
                    cycle_id, attempt_number, state, created_at
                 ) VALUES (?1, ?2, 'reserved', ?3)",
                params![cycle_id, attempt_number, now],
            )
            .map_err(database_error("insert decision attempt reservation"))?;
        let updated = transaction
            .execute(
                "UPDATE decision_cycles
                 SET state = 'analyzing', next_wake_at = NULL, updated_at = ?1
                 WHERE cycle_id = ?2 AND state = 'pending'",
                params![now, cycle_id],
            )
            .map_err(database_error("activate reserved decision cycle"))?;
        if updated != 1 {
            return Err(validation_error(
                "decision_cycle",
                "state changed while reserving a decision attempt",
            ));
        }
        let reservation = DecisionReservation {
            cycle_id: cycle_id.to_owned(),
            campaign_id: authority.cycle.campaign_id,
            source_experiment_id: authority.cycle.source_experiment_id,
            attempt_number,
            created_at: now,
        };
        transaction
            .commit()
            .map_err(database_error("commit decision attempt reservation"))?;
        Ok(Some(reservation))
    }

    pub fn requeue_unbound_attempt(
        &self,
        reservation: &DecisionReservation,
        now: i64,
    ) -> Result<DecisionCycle, AppError> {
        self.try_requeue_unbound_attempt(reservation, now)?
            .ok_or_else(|| {
                validation_error(
                    "decision_attempt",
                    "only an unbound reserved or evidence-ready attempt can be requeued",
                )
            })
    }

    pub fn try_requeue_unbound_attempt(
        &self,
        reservation: &DecisionReservation,
        now: i64,
    ) -> Result<Option<DecisionCycle>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin unbound decision attempt requeue"))?;
        let requeued = Self::try_requeue_unbound_attempt_in_transaction(
            &transaction,
            reservation,
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit unbound decision attempt requeue"))?;
        Ok(requeued.map(|requeued| requeued.cycle))
    }

    pub fn recover_unbound_attempt_event(
        &self,
        reservation: &DecisionReservation,
        event_id: i64,
        now: i64,
        retry_at: i64,
    ) -> Result<Option<DecisionCycle>, AppError> {
        if retry_at <= now {
            return Err(validation_error(
                "retry_at",
                "must be later than the recovery timestamp",
            ));
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin unbound decision event recovery"))?;
        let recovered = Self::recover_unbound_attempt_event_in_transaction(
            &transaction,
            reservation,
            event_id,
            now,
            retry_at,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit unbound decision event recovery"))?;
        Ok(recovered)
    }

    pub(super) fn recover_unbound_attempt_event_in_transaction(
        transaction: &Transaction<'_>,
        reservation: &DecisionReservation,
        event_id: i64,
        now: i64,
        retry_at: i64,
    ) -> Result<Option<DecisionCycle>, AppError> {
        let requeued = Self::try_requeue_unbound_attempt_in_transaction(
            transaction,
            reservation,
            now,
        )?;
        let Some(requeued) = requeued else {
            return Ok(None);
        };
        let dedup_key = campaign_decision_dedup_key(&reservation.cycle_id);
        let (event_project_id, event_status) = transaction
            .query_row(
                "SELECT ev.project_id, ev.status
                 FROM campaigns c
                 JOIN events ev
                   ON ev.project_id = c.project_id AND ev.dedup_key = ?2
                 WHERE c.campaign_id = ?1 AND ev.event_id = ?3
                   AND ev.campaign_id = ?1 AND ev.experiment_id = ?4
                   AND ev.kind = 'campaign_decision'
                 LIMIT 1",
                params![
                    reservation.campaign_id,
                    dedup_key,
                    event_id,
                    reservation.source_experiment_id,
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, crate::models::EventStatus>(1)?,
                    ))
                },
            )
            .optional()
            .map_err(database_error("validate unbound decision recovery event"))?
            .ok_or_else(|| {
                validation_error(
                    "campaign_decision_event",
                    "must carry the exact unbound decision reservation lineage",
                )
            })?;
        let changed = match event_status {
            crate::models::EventStatus::Claimed => transaction
                .execute(
                    "UPDATE events
                     SET status = 'pending', lease_until = NULL, completed_at = NULL,
                         attempts = attempts - 1
                     WHERE project_id = ?1 AND dedup_key = ?2 AND event_id = ?3
                       AND status = 'claimed' AND attempts > 0",
                    params![event_project_id, dedup_key, event_id],
                )
                .map_err(database_error("defer recovered claimed decision event"))?,
            crate::models::EventStatus::RetryWait if requeued.transitioned => transaction
                .execute(
                    "UPDATE events SET attempts = attempts - 1
                     WHERE project_id = ?1 AND dedup_key = ?2 AND event_id = ?3
                       AND status = 'retry_wait' AND attempts > 0",
                    params![event_project_id, dedup_key, event_id],
                )
                .map_err(database_error("restore recovered decision event retry count"))?,
            crate::models::EventStatus::RetryWait => 1,
            crate::models::EventStatus::DeadLetter => transaction
                .execute(
                    "UPDATE events
                     SET status = 'retry_wait', not_before = ?1, lease_until = NULL,
                         completed_at = NULL, attempts = attempts - 1
                     WHERE project_id = ?2 AND dedup_key = ?3 AND event_id = ?4
                       AND status = 'dead_letter' AND attempts > 0",
                    params![retry_at, event_project_id, dedup_key, event_id],
                )
                .map_err(database_error("revive unowned decision bind event"))?,
            _ => {
                return Err(validation_error(
                    "campaign_decision_event",
                    "unbound decision recovery requires a claimed, retry-wait, or dead-letter event",
                ));
            }
        };
        if changed != 1 {
            return Err(validation_error(
                "campaign_decision_event",
                "status changed during unbound decision event recovery",
            ));
        }
        Ok(Some(requeued.cycle))
    }

    fn try_requeue_unbound_attempt_in_transaction(
        transaction: &Transaction<'_>,
        reservation: &DecisionReservation,
        now: i64,
    ) -> Result<Option<RequeuedDecisionAttempt>, AppError> {
        let authority = read_authority(transaction, &reservation.cycle_id)?;
        validate_reservation_lineage(&authority, reservation)?;
        let attempt = read_attempt(
            transaction,
            &reservation.cycle_id,
            reservation.attempt_number,
        )?;
        if !matches!(
            attempt.state,
            DecisionAttemptState::Reserved | DecisionAttemptState::EvidenceReady
        ) || attempt.agent_run_id.is_some()
        {
            return Ok(None);
        }
        let evidence_discarded = attempt.state == DecisionAttemptState::EvidenceReady;
        if evidence_discarded {
            let updated = transaction
                .execute(
                    "UPDATE decision_attempts
                     SET state = 'reserved', context_schema_version = NULL,
                         context_json = NULL, context_digest = NULL
                     WHERE cycle_id = ?1 AND attempt_number = ?2
                       AND state = 'evidence_ready' AND agent_run_id IS NULL",
                    params![reservation.cycle_id, reservation.attempt_number],
                )
                .map_err(database_error("discard stale unbound decision evidence"))?;
            if updated != 1 {
                return Err(validation_error(
                    "decision_attempt",
                    "state changed while discarding stale unbound evidence",
                ));
            }
        }
        if authority.cycle.state == DecisionCycleState::Pending {
            return Ok(Some(RequeuedDecisionAttempt {
                cycle: authority.cycle,
                transitioned: evidence_discarded,
            }));
        }
        if authority.cycle.state != DecisionCycleState::Analyzing {
            return Ok(None);
        }
        let updated = transaction
            .execute(
                "UPDATE decision_cycles
                 SET state = 'pending', next_wake_at = NULL, updated_at = ?1
                 WHERE cycle_id = ?2 AND state = 'analyzing'",
                params![now, reservation.cycle_id],
            )
            .map_err(database_error("requeue unbound decision attempt"))?;
        if updated != 1 {
            return Err(validation_error(
                "decision_cycle",
                "state changed while requeuing an unbound attempt",
            ));
        }
        let cycle = read_cycle(transaction, &reservation.cycle_id)?;
        Ok(Some(RequeuedDecisionAttempt {
            cycle,
            transitioned: true,
        }))
    }

    pub fn store_evidence(
        &self,
        reservation: &DecisionReservation,
        context_json: &str,
        context_digest: &str,
        now: i64,
    ) -> Result<(), AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin decision evidence storage"))?;
        let authority = read_authority(&transaction, &reservation.cycle_id)?;
        validate_reservation_lineage(&authority, reservation)?;
        validate_active_authority(&authority, None)?;
        validate_research_decision_ownership(
            &transaction,
            &authority,
            &reservation.cycle_id,
        )?;
        validate_payload("context_json", context_json)?;
        validate_token("context_digest", context_digest, MAX_DECISION_DIGEST_BYTES)?;
        let context_schema_version = decision_context_schema_version(context_json)?;
        let attempt = read_attempt(
            &transaction,
            &reservation.cycle_id,
            reservation.attempt_number,
        )?;
        if attempt.state == DecisionAttemptState::EvidenceReady
            && attempt.context_schema_version == Some(i64::from(context_schema_version))
            && attempt.context_json.as_deref() == Some(context_json)
            && attempt.context_digest.as_deref() == Some(context_digest)
        {
            transaction
                .commit()
                .map_err(database_error("commit existing decision evidence"))?;
            return Ok(());
        }
        if attempt.state != DecisionAttemptState::Reserved {
            return Err(validation_error(
                "decision_attempt",
                "only a reserved attempt can store evidence",
            ));
        }
        transaction
            .execute(
                "UPDATE decision_attempts
                 SET state = 'evidence_ready', context_schema_version = ?1,
                     context_json = ?2, context_digest = ?3
                 WHERE cycle_id = ?4 AND attempt_number = ?5 AND state = 'reserved'",
                params![
                    i64::from(context_schema_version),
                    context_json,
                    context_digest,
                    reservation.cycle_id,
                    reservation.attempt_number
                ],
            )
            .map_err(database_error("store decision evidence"))?;
        transaction
            .execute(
                "UPDATE decision_cycles SET updated_at = ?1 WHERE cycle_id = ?2",
                params![now, reservation.cycle_id],
            )
            .map_err(database_error("touch decision cycle after evidence storage"))?;
        transaction
            .commit()
            .map_err(database_error("commit decision evidence storage"))
    }

    pub fn bind_agent_run(
        &self,
        reservation: &DecisionReservation,
        run_id: i64,
        now: i64,
    ) -> Result<(), AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin decision agent-run binding"))?;
        let authority = read_authority(&transaction, &reservation.cycle_id)?;
        validate_reservation_lineage(&authority, reservation)?;
        validate_active_authority(&authority, None)?;
        validate_research_decision_ownership(&transaction, &authority, &reservation.cycle_id)?;
        let run_project_id: Option<String> = transaction
            .query_row(
                "SELECT project_id FROM agent_runs
                 WHERE run_id = ?1 AND status IN ('starting','running')",
                [run_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("read decision agent run"))?;
        if run_project_id.as_deref() != Some(authority.project_id.as_str()) {
            return Err(validation_error(
                "agent_run_id",
                "must identify an active agent run in the decision project",
            ));
        }
        let attempt = read_attempt(
            &transaction,
            &reservation.cycle_id,
            reservation.attempt_number,
        )?;
        if attempt.state == DecisionAttemptState::Running
            && attempt.agent_run_id == Some(run_id)
        {
            transaction
                .commit()
                .map_err(database_error("commit existing decision agent-run binding"))?;
            return Ok(());
        }
        if attempt.state != DecisionAttemptState::EvidenceReady || attempt.agent_run_id.is_some() {
            return Err(validation_error(
                "decision_attempt",
                "only an unbound evidence-ready attempt can bind an agent run",
            ));
        }
        transaction
            .execute(
                "UPDATE decision_attempts
                 SET state = 'running', agent_run_id = ?1, started_at = ?2
                 WHERE cycle_id = ?3 AND attempt_number = ?4
                   AND state = 'evidence_ready' AND agent_run_id IS NULL",
                params![
                    run_id,
                    now,
                    reservation.cycle_id,
                    reservation.attempt_number
                ],
            )
            .map_err(database_error("bind decision agent run"))?;
        transaction
            .execute(
                "UPDATE decision_cycles SET updated_at = ?1 WHERE cycle_id = ?2",
                params![now, reservation.cycle_id],
            )
            .map_err(database_error("touch decision cycle after agent-run binding"))?;
        transaction
            .commit()
            .map_err(database_error("commit decision agent-run binding"))
    }

    pub fn store_decision(
        &self,
        run_id: i64,
        decision_json: &str,
        decision_digest: &str,
        decision_kind: &str,
        now: i64,
    ) -> Result<DecisionAttempt, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin decision output storage"))?;
        let attempt = read_attempt_for_run(&transaction, run_id)?;
        let authority = read_authority(&transaction, &attempt.cycle_id)?;
        validate_authority_lineage(&authority)?;
        validate_payload("decision_json", decision_json)?;
        validate_token("decision_digest", decision_digest, MAX_DECISION_DIGEST_BYTES)?;
        if !matches!(decision_kind, "proposal" | "wait" | "goal_reached") {
            return Err(validation_error(
                "decision_kind",
                "must be proposal, wait, or goal_reached",
            ));
        }
        if attempt.state == DecisionAttemptState::Decided
            && attempt.decision_json.as_deref() == Some(decision_json)
            && attempt.decision_digest.as_deref() == Some(decision_digest)
            && attempt.decision_kind.as_deref() == Some(decision_kind)
        {
            transaction
                .commit()
                .map_err(database_error("commit existing decision output"))?;
            return Ok(attempt);
        }
        if attempt.state != DecisionAttemptState::Running {
            return Err(validation_error(
                "decision_attempt",
                "only a running attempt can store a decision",
            ));
        }
        transaction
            .execute(
                "UPDATE decision_attempts
                 SET state = 'decided', decision_json = ?1, decision_digest = ?2,
                     decision_kind = ?3, finished_at = ?4
                 WHERE agent_run_id = ?5 AND state = 'running'",
                params![decision_json, decision_digest, decision_kind, now, run_id],
            )
            .map_err(database_error("store decision output"))?;
        transaction
            .execute(
                "UPDATE decision_cycles SET updated_at = ?1 WHERE cycle_id = ?2",
                params![now, attempt.cycle_id],
            )
            .map_err(database_error("touch decision cycle after output storage"))?;
        let stored = read_attempt(&transaction, &attempt.cycle_id, attempt.attempt_number)?;
        transaction
            .commit()
            .map_err(database_error("commit decision output storage"))?;
        Ok(stored)
    }

    pub fn fail_attempt(
        &self,
        run_id: Option<i64>,
        cycle_id: &str,
        attempt_number: i64,
        code: &str,
        summary: &str,
        limits: CampaignLimits,
        now: i64,
    ) -> Result<DecisionCycle, AppError> {
        self.record_attempt_failure(
            run_id,
            cycle_id,
            attempt_number,
            code,
            summary,
            limits,
            now,
            false,
        )
    }

    pub fn reject_decision(
        &self,
        cycle_id: &str,
        attempt_number: i64,
        code: &str,
        summary: &str,
        limits: CampaignLimits,
        now: i64,
    ) -> Result<DecisionCycle, AppError> {
        self.record_attempt_failure(
            None,
            cycle_id,
            attempt_number,
            code,
            summary,
            limits,
            now,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn record_attempt_failure(
        &self,
        run_id: Option<i64>,
        cycle_id: &str,
        attempt_number: i64,
        code: &str,
        summary: &str,
        limits: CampaignLimits,
        now: i64,
        allow_decided: bool,
    ) -> Result<DecisionCycle, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin decision attempt failure"))?;
        let authority = read_authority(&transaction, cycle_id)?;
        validate_authority_lineage(&authority)?;
        validate_token("failure_code", code, MAX_DECISION_CODE_BYTES)?;
        validate_bounded_text("failure_summary", summary, MAX_DECISION_SUMMARY_BYTES)?;
        let attempt = read_attempt(&transaction, cycle_id, attempt_number)?;
        if let Some(run_id) = run_id {
            if attempt.agent_run_id != Some(run_id) {
                return Err(validation_error(
                    "agent_run_id",
                    "does not match the failed decision attempt",
                ));
            }
        }
        if attempt.state == DecisionAttemptState::Failed {
            if attempt.failure_code.as_deref() == Some(code)
                && attempt.failure_summary.as_deref() == Some(summary)
            {
                transaction
                    .commit()
                    .map_err(database_error("commit existing decision attempt failure"))?;
                return Ok(authority.cycle);
            }
            return Err(validation_error(
                "decision_attempt",
                "conflicts with the existing failure",
            ));
        }
        if attempt.state == DecisionAttemptState::Decided && !allow_decided {
            return Err(validation_error(
                "decision_attempt",
                "a decided attempt cannot fail",
            ));
        }
        transaction
            .execute(
                "UPDATE decision_attempts
                 SET state = 'failed', failure_code = ?1, failure_summary = ?2, finished_at = ?3
                 WHERE cycle_id = ?4 AND attempt_number = ?5
                   AND state IN ('reserved','evidence_ready','running','decided')",
                params![code, summary, now, cycle_id, attempt_number],
            )
            .map_err(database_error("fail decision attempt"))?;
        let failures = authority
            .cycle
            .consecutive_failed_attempts
            .checked_add(1)
            .ok_or_else(|| validation_error("decision_cycle", "failure counter overflow"))?;
        let exhausted = failures >= i64::from(limits.max_decision_attempts_per_cycle);
        let state = if exhausted {
            DecisionCycleState::Degraded
        } else {
            DecisionCycleState::Pending
        };
        transaction
            .execute(
                "UPDATE decision_cycles
                 SET state = ?1, next_wake_at = NULL, consecutive_failed_attempts = ?2,
                     last_failure_code = ?3, last_failure_summary = ?4, updated_at = ?5
                 WHERE cycle_id = ?6",
                params![state, failures, code, summary, now, cycle_id],
            )
            .map_err(database_error("record decision cycle failure"))?;
        if !exhausted {
            let dedup_key = campaign_decision_dedup_key(cycle_id);
            let requeued = transaction
                .execute(
                    "UPDATE events
                     SET status = 'pending', not_before = ?1, lease_until = NULL,
                         completed_at = NULL, attempts = 0, last_error = NULL
                     WHERE project_id = ?2 AND dedup_key = ?3
                       AND campaign_id = ?4 AND experiment_id = ?5
                       AND kind = 'campaign_decision'
                       AND (status IN ('completed','failed','dead_letter')
                            OR (status = 'retry_wait' AND not_before <= ?1))",
                    params![
                        now,
                        authority.project_id,
                        dedup_key,
                        authority.cycle.campaign_id,
                        authority.cycle.source_experiment_id,
                    ],
                )
                .map_err(database_error("requeue retryable decision event"))?;
            if requeued > 1 {
                return Err(validation_error(
                    "campaign_decision_event",
                    "retryable failure matched multiple lineage events",
                ));
            }
        }
        if exhausted {
            match authority.campaign_state {
                CampaignState::Active
                | CampaignState::BudgetWaiting
                | CampaignState::GoalReachedPendingReview => {
                    let updated = transaction
                        .execute(
                            "UPDATE campaigns
                             SET state = 'degraded',
                                 state_reason = 'decision_attempts_exhausted',
                                 next_eligible_at = NULL, updated_at = ?1
                             WHERE campaign_id = ?2 AND state = ?3",
                            params![
                                now,
                                authority.cycle.campaign_id,
                                authority.campaign_state
                            ],
                        )
                        .map_err(database_error(
                            "degrade campaign after decision failures",
                        ))?;
                    if updated != 1 {
                        return Err(validation_error(
                            "campaign",
                            "state changed while degrading exhausted decision attempts",
                        ));
                    }
                }
                CampaignState::Paused
                | CampaignState::Degraded
                | CampaignState::Halted
                | CampaignState::Retired => {}
            }
        }
        let cycle = read_cycle(&transaction, cycle_id)?;
        transaction
            .commit()
            .map_err(database_error("commit decision attempt failure"))?;
        Ok(cycle)
    }

    pub fn mark_waiting(
        &self,
        cycle_id: &str,
        attempt_number: i64,
        next_wake_at: i64,
        now: i64,
    ) -> Result<DecisionCycle, AppError> {
        if next_wake_at <= now {
            return Err(validation_error(
                "next_wake_at",
                "must be a finite future timestamp",
            ));
        }
        self.finish_attempt(cycle_id, attempt_number, Some(next_wake_at), now)
    }

    pub fn mark_completed(
        &self,
        cycle_id: &str,
        attempt_number: i64,
        now: i64,
    ) -> Result<DecisionCycle, AppError> {
        self.finish_attempt(cycle_id, attempt_number, None, now)
    }

    pub fn complete_goal_claim_atomically(
        &self,
        cycle_id: &str,
        attempt_number: i64,
        evidence_ref: &str,
        now: i64,
    ) -> Result<DecisionCycle, AppError> {
        if evidence_ref.is_empty()
            || evidence_ref.len() > crate::decision_protocol::MAX_EVIDENCE_REF_BYTES
            || evidence_ref.chars().any(char::is_control)
            || evidence_ref.trim().is_empty()
        {
            return Err(validation_error(
                "evidence_ref",
                "must be bounded without control characters",
            ));
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin goal claim atomic transition"))?;
        let authority = read_authority(&transaction, cycle_id)?;
        if attempt_number != {
            let attempt = read_attempt(&transaction, cycle_id, attempt_number)?;
            if attempt.state != DecisionAttemptState::Decided
                || attempt.decision_kind.as_deref() != Some("goal_reached")
            {
                return Err(validation_error(
                    "decision_attempt",
                    "goal claim requires a decided goal_reached attempt",
                ));
            }
            // Validate evidence_ref matches the persisted decision payload.
            let decision_json = attempt.decision_json.as_deref().ok_or_else(|| {
                validation_error("decision_json", "goal claim decision payload is missing")
            })?;
            let parsed: serde_json::Value =
                serde_json::from_str(decision_json).map_err(|source| AppError::Serialization {
                    operation: "parse goal claim decision payload",
                    source,
                })?;
            let payload_ref = parsed
                .get("evidence_ref")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            if payload_ref != evidence_ref {
                return Err(validation_error(
                    "evidence_ref",
                    "must match the persisted goal decision payload",
                ));
            }
            attempt.attempt_number
        } {
            return Err(validation_error(
                "decision_attempt",
                "attempt number mismatch during goal claim",
            ));
        }
        // Idempotent: if already parked and cycle completed, return existing.
        if authority.cycle.state == DecisionCycleState::Completed
            && authority.cycle.last_decision_kind.as_deref() == Some("goal_reached")
        {
            let campaign_state: String = transaction
                .query_row(
                    "SELECT state FROM campaigns WHERE campaign_id = ?1",
                    [&authority.cycle.campaign_id],
                    |row| row.get(0),
                )
                .map_err(database_error(
                    "check campaign state for idempotent goal claim",
                ))?;
            if campaign_state == CampaignState::GoalReachedPendingReview.as_str() {
                let cycle = read_cycle(&transaction, cycle_id)?;
                transaction
                    .commit()
                    .map_err(database_error("commit idempotent goal claim"))?;
                return Ok(cycle);
            }
        }
        validate_active_authority(&authority, None)?;
        // Campaign must be active before parking.
        let campaign_state: CampaignState = transaction
            .query_row(
                "SELECT state FROM campaigns WHERE campaign_id = ?1",
                [&authority.cycle.campaign_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("read campaign state for goal claim"))?
            .ok_or_else(|| validation_error("campaign", "does not exist for goal claim"))?;
        if campaign_state != CampaignState::Active {
            return Err(validation_error(
                "campaign",
                "only an active campaign can be parked for goal review",
            ));
        }
        // Evidence must belong to same campaign via experiment_metrics -> experiments.
        let evidence_exists: bool = transaction
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM experiment_metrics m
                    JOIN experiments e ON e.experiment_id = m.experiment_id
                    WHERE m.experiment_id = ?1 AND e.campaign_id = ?2 AND m.artifact_defect IS NULL
                )",
                rusqlite::params![evidence_ref, authority.cycle.campaign_id],
                |row| row.get(0),
            )
            .map_err(database_error("check goal evidence reference"))?;
        if !evidence_exists {
            return Err(validation_error(
                "evidence_ref",
                "must reference an existing metrics row for this campaign",
            ));
        }
        let reason = crate::output::bounded_redacted_text(&format!("goal_reached:{evidence_ref}"));
        let updated = transaction
            .execute(
                "UPDATE campaigns SET state = ?1, state_reason = ?2, next_eligible_at = NULL, updated_at = ?3 WHERE campaign_id = ?4 AND state = 'active'",
                rusqlite::params![
                    CampaignState::GoalReachedPendingReview,
                    reason,
                    now,
                    authority.cycle.campaign_id
                ],
            )
            .map_err(database_error("park campaign for goal review"))?;
        if updated != 1 {
            return Err(validation_error(
                "campaign",
                "state changed while parking for goal review",
            ));
        }
        // Complete the decision cycle atomically.
        let updated_cycle = transaction
            .execute(
                "UPDATE decision_cycles SET state = ?1, next_wake_at = NULL, consecutive_failed_attempts = 0, last_decision_kind = ?2, last_failure_code = NULL, last_failure_summary = NULL, updated_at = ?3 WHERE cycle_id = ?4 AND state = 'analyzing'",
                rusqlite::params![
                    DecisionCycleState::Completed,
                    "goal_reached",
                    now,
                    cycle_id
                ],
            )
            .map_err(database_error("complete goal decision cycle"))?;
        if updated_cycle != 1 {
            return Err(validation_error(
                "decision_cycle",
                "state changed while completing goal claim",
            ));
        }
        // Ensure attempt finished_at is set (store_decision already set, but ensure).
        transaction
            .execute(
                "UPDATE decision_attempts SET finished_at = COALESCE(finished_at, ?1) WHERE cycle_id = ?2 AND attempt_number = ?3",
                rusqlite::params![now, cycle_id, attempt_number],
            )
            .map_err(database_error("touch goal decision attempt"))?;
        let cycle = read_cycle(&transaction, cycle_id)?;
        transaction
            .commit()
            .map_err(database_error("commit goal claim atomic transition"))?;
        Ok(cycle)
    }

    pub fn find_cycle_for_source(
        &self,
        campaign_id: &str,
        source_experiment_id: &str,
    ) -> Result<Option<DecisionCycle>, AppError> {
        let connection = self.db.connect()?;
        let cycle_id = connection
            .query_row(
                "SELECT cycle_id FROM decision_cycles
                 WHERE campaign_id = ?1 AND source_experiment_id = ?2",
                params![campaign_id, source_experiment_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(database_error("find decision cycle by source experiment"))?;
        cycle_id
            .map(|cycle_id| read_cycle(&connection, &cycle_id))
            .transpose()
    }

    pub fn due_cycles(&self, now: i64, limit: usize) -> Result<Vec<DecisionCycle>, AppError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if limit > MAX_EVENT_LIST_LIMIT {
            return Err(AppError::Configuration {
                field: "event_claim_limit",
            });
        }
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin due decision cycle query"))?;
        let cycle_ids = query_decision_cycle_ids(
            &transaction,
            GLOBAL_DUE_WAIT_DECISION_SQL,
            &[&now, &limit],
            "query due waiting decision cycles",
        )?;
        let mut cycles = Vec::with_capacity(cycle_ids.len());
        for cycle_id in cycle_ids {
            let cycle = read_cycle(&transaction, &cycle_id)?;
            let project_id: String = transaction
                .query_row(
                    "SELECT project_id FROM campaigns WHERE campaign_id = ?1",
                    [&cycle.campaign_id],
                    |row| row.get(0),
                )
                .map_err(database_error("read due decision event project"))?;
            if !research_decision_work_allowed(
                &transaction,
                &project_id,
                &cycle.campaign_id,
                &cycle.source_experiment_id,
                &cycle_id,
            )? {
                continue;
            }
            let promoted = transaction
                .execute(
                    "UPDATE decision_cycles
                     SET state = 'pending', next_wake_at = NULL, updated_at = ?1
                     WHERE cycle_id = ?2 AND state = 'waiting' AND next_wake_at <= ?1",
                    params![now, cycle_id],
                )
                .map_err(database_error("wake due decision cycle during query"))?;
            if promoted != 1 {
                return Err(validation_error(
                    "decision_cycle",
                    "due waiting cycle changed during wake promotion",
                ));
            }
            let cycle = read_cycle(&transaction, &cycle_id)?;
            let dedup_key = campaign_decision_dedup_key(&cycle_id);
            let promoted_event = transaction
                .execute(
                    "UPDATE events
                     SET status = 'pending', not_before = ?1, lease_until = NULL,
                         completed_at = NULL, attempts = 0, last_error = NULL
                     WHERE project_id = ?2 AND dedup_key = ?3
                       AND campaign_id = ?4 AND experiment_id = ?5
                       AND kind = 'campaign_decision'
                       AND (status IN ('completed','failed','dead_letter')
                            OR (status = 'retry_wait' AND not_before <= ?1))",
                    params![
                        now,
                        project_id,
                        dedup_key,
                        cycle.campaign_id,
                        cycle.source_experiment_id,
                    ],
                )
                .map_err(database_error("wake due decision event"))?;
            if promoted_event != 1 {
                return Err(validation_error(
                    "campaign_decision_event",
                    "due wake requires exactly one matching lineage event",
                ));
            }
            cycles.push(cycle);
        }
        transaction
            .commit()
            .map_err(database_error("commit due decision cycle query"))?;
        Ok(cycles)
    }

    pub fn oldest_pending_cycle_for_campaign(
        &self,
        project_id: &str,
        campaign_id: &str,
    ) -> Result<Option<DecisionCycle>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin oldest due decision cycle query"))?;
        let campaign_active: bool = transaction
            .query_row(
                "SELECT c.state = 'active'
                        AND p.enabled = 1 AND p.paused = 0 AND p.halted_reason IS NULL
                 FROM campaigns c
                 JOIN projects p ON p.project_id = c.project_id
                 WHERE c.campaign_id = ?1 AND c.project_id = ?2",
                params![campaign_id, project_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("query decision campaign authority"))?
            .unwrap_or(false);
        let cycle_id = if campaign_active {
            query_decision_cycle_ids(
                &transaction,
                CAMPAIGN_PENDING_DECISION_SQL,
                &[&campaign_id],
                "query oldest pending campaign decision",
            )?
                .into_iter()
                .next()
        } else {
            None
        };
        let Some(cycle_id) = cycle_id else {
            transaction
                .commit()
                .map_err(database_error("commit empty oldest due decision cycle query"))?;
            return Ok(None);
        };
        let authority = read_authority(&transaction, &cycle_id)?;
        validate_active_authority(&authority, Some(project_id))?;
        if !research_decision_work_allowed(
            &transaction,
            project_id,
            &authority.cycle.campaign_id,
            &authority.cycle.source_experiment_id,
            &authority.cycle.cycle_id,
        )? {
            transaction
                .commit()
                .map_err(database_error("commit deferred oldest decision cycle query"))?;
            return Ok(None);
        }
        let cycle = authority.cycle;
        transaction
            .commit()
            .map_err(database_error("commit oldest due decision cycle query"))?;
        Ok(Some(cycle))
    }

    pub(crate) fn ready_decisions(&self, limit: usize) -> Result<Vec<ReadyDecision>, AppError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT da.cycle_id, dc.campaign_id, dc.source_experiment_id,
                        da.attempt_number, da.created_at, da.context_schema_version,
                        da.context_json, da.context_digest, da.decision_json,
                        da.decision_digest, da.decision_kind
                 FROM decision_attempts da
                 JOIN decision_cycles dc ON dc.cycle_id = da.cycle_id
                 WHERE dc.state = 'analyzing' AND da.state = 'decided'
                   AND da.attempt_number = (
                       SELECT MAX(latest.attempt_number)
                       FROM decision_attempts latest WHERE latest.cycle_id = da.cycle_id
                   )
                 ORDER BY COALESCE(da.finished_at, da.created_at), da.cycle_id,
                          da.attempt_number
                 LIMIT ?1",
            )
            .map_err(database_error("prepare ready decision query"))?;
        let decisions = statement
            .query_map([i64::try_from(limit).unwrap_or(i64::MAX)], |row| {
                Ok(ReadyDecision {
                    reservation: DecisionReservation {
                        cycle_id: row.get(0)?,
                        campaign_id: row.get(1)?,
                        source_experiment_id: row.get(2)?,
                        attempt_number: row.get(3)?,
                        created_at: row.get(4)?,
                    },
                    context_schema_version: row.get(5)?,
                    context_json: row.get(6)?,
                    context_digest: row.get(7)?,
                    decision_json: row.get(8)?,
                    decision_digest: row.get(9)?,
                    decision_kind: row.get(10)?,
                })
            })
            .map_err(database_error("query ready decisions"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read ready decisions"))?;
        Ok(decisions)
    }

    pub fn recoverable_attempts(&self, limit: usize) -> Result<Vec<DecisionRecovery>, AppError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT da.cycle_id, dc.campaign_id, dc.source_experiment_id,
                        da.attempt_number, da.created_at, da.state, da.agent_run_id,
                        run.status,
                        (SELECT ev.event_id
                         FROM events ev
                         JOIN campaigns c ON c.campaign_id = dc.campaign_id
                         WHERE ev.project_id = c.project_id
                           AND ev.campaign_id = dc.campaign_id
                           AND ev.experiment_id = dc.source_experiment_id
                           AND ev.kind = 'campaign_decision'
                           AND json_extract(ev.payload_json, '$.source') = 'terminal_experiment'
                           AND json_extract(ev.payload_json, '$.cycle_id') = dc.cycle_id
                           AND json_extract(ev.payload_json, '$.source_experiment_id') =
                               dc.source_experiment_id
                         ORDER BY ev.event_id LIMIT 1)
                 FROM decision_attempts da
                 JOIN decision_cycles dc ON dc.cycle_id = da.cycle_id
                 LEFT JOIN agent_runs run ON run.run_id = da.agent_run_id
                 WHERE dc.state = 'analyzing'
                   AND da.state IN ('reserved','evidence_ready','running','decided')
                 ORDER BY da.created_at, da.cycle_id, da.attempt_number
                 LIMIT ?1",
            )
            .map_err(database_error("prepare recoverable decision attempt query"))?;
        let attempts = statement
            .query_map([i64::try_from(limit).unwrap_or(i64::MAX)], |row| {
                Ok(DecisionRecovery {
                    reservation: DecisionReservation {
                        cycle_id: row.get(0)?,
                        campaign_id: row.get(1)?,
                        source_experiment_id: row.get(2)?,
                        attempt_number: row.get(3)?,
                        created_at: row.get(4)?,
                    },
                    state: row.get(5)?,
                    agent_run_id: row.get(6)?,
                    agent_run_status: row.get(7)?,
                    event_id: row.get(8)?,
                })
            })
            .map_err(database_error("query recoverable decision attempts"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read recoverable decision attempts"))?;
        Ok(attempts)
    }

    fn finish_attempt(
        &self,
        cycle_id: &str,
        attempt_number: i64,
        next_wake_at: Option<i64>,
        now: i64,
    ) -> Result<DecisionCycle, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin decision cycle completion"))?;
        let authority = read_authority(&transaction, cycle_id)?;
        validate_authority_lineage(&authority)?;
        let attempt = read_attempt(&transaction, cycle_id, attempt_number)?;
        let latest_attempt_number: i64 = transaction
            .query_row(
                "SELECT COALESCE(MAX(attempt_number), 0)
                 FROM decision_attempts WHERE cycle_id = ?1",
                [cycle_id],
                |row| row.get(0),
            )
            .map_err(database_error("read latest decision attempt number"))?;
        let expected_kind = if next_wake_at.is_some() {
            "wait"
        } else {
            "proposal"
        };
        let expected_cycle_state = if next_wake_at.is_some() {
            DecisionCycleState::Waiting
        } else {
            DecisionCycleState::Completed
        };
        if attempt.state == DecisionAttemptState::Decided
            && attempt.decision_kind.as_deref() == Some(expected_kind)
            && authority.cycle.state == expected_cycle_state
            && authority.cycle.next_wake_at == next_wake_at
            && latest_attempt_number == attempt_number
        {
            transaction
                .commit()
                .map_err(database_error("commit existing decision cycle completion"))?;
            return Ok(authority.cycle);
        }
        validate_active_authority(&authority, None)?;
        let legal_attempt_state = if next_wake_at.is_some() {
            matches!(
                attempt.state,
                DecisionAttemptState::Reserved | DecisionAttemptState::Decided
            )
        } else {
            attempt.state == DecisionAttemptState::Decided
        };
        if latest_attempt_number != attempt_number
            || authority.cycle.state != DecisionCycleState::Analyzing
            || !legal_attempt_state
            || attempt
                .decision_kind
                .as_deref()
                .is_some_and(|kind| kind != expected_kind)
        {
            return Err(validation_error(
                "decision_attempt",
                "cannot apply the requested terminal decision transition",
            ));
        }
        transaction
            .execute(
                "UPDATE decision_attempts
                 SET state = 'decided', decision_kind = COALESCE(decision_kind, ?1),
                     finished_at = COALESCE(finished_at, ?2)
                 WHERE cycle_id = ?3 AND attempt_number = ?4
                   AND state IN ('reserved','decided')",
                params![expected_kind, now, cycle_id, attempt_number],
            )
            .map_err(database_error("finish applied decision attempt"))?;
        let updated = transaction
            .execute(
                "UPDATE decision_cycles
                 SET state = ?1, next_wake_at = ?2, consecutive_failed_attempts = 0,
                     last_decision_kind = ?3, last_failure_code = NULL,
                     last_failure_summary = NULL, updated_at = ?4
                 WHERE cycle_id = ?5 AND state = 'analyzing'",
                params![
                    expected_cycle_state,
                    next_wake_at,
                    expected_kind,
                    now,
                    cycle_id
                ],
            )
            .map_err(database_error("finish decision cycle"))?;
        if updated != 1 {
            return Err(validation_error(
                "decision_cycle",
                "state changed while finishing the current attempt",
            ));
        }
        let cycle = read_cycle(&transaction, cycle_id)?;
        transaction
            .commit()
            .map_err(database_error("commit decision cycle completion"))?;
        Ok(cycle)
    }
}

fn read_authority(
    connection: &Connection,
    cycle_id: &str,
) -> Result<DecisionAuthority, AppError> {
    connection
        .query_row(
            &format!(
                "SELECT dc.cycle_id, dc.campaign_id, dc.source_experiment_id,
                        dc.source_terminal_at, dc.state,
                        dc.next_wake_at, dc.consecutive_failed_attempts, dc.last_decision_kind,
                        dc.last_failure_code, dc.last_failure_summary, dc.created_at, dc.updated_at,
                        c.project_id, c.state, p.enabled, p.paused,
                        p.halted_reason IS NOT NULL, e.campaign_id, e.status,
                        c.objective_digest
                 FROM decision_cycles dc
                 JOIN campaigns c ON c.campaign_id = dc.campaign_id
                 JOIN projects p ON p.project_id = c.project_id
                 JOIN experiments e ON e.experiment_id = dc.source_experiment_id
                 WHERE dc.cycle_id = ?1"
            ),
            [cycle_id],
            |row| {
                Ok(DecisionAuthority {
                    cycle: decision_cycle_from_row(row)?,
                    project_id: row.get(12)?,
                    campaign_state: row.get(13)?,
                    project_enabled: row.get(14)?,
                    project_paused: row.get(15)?,
                    project_halted: row.get(16)?,
                    experiment_campaign_id: row.get(17)?,
                    experiment_status: row.get(18)?,
                    objective_digest: row.get(19)?,
                })
            },
        )
        .optional()
        .map_err(database_error("read decision cycle authority"))?
        .ok_or_else(|| {
            validation_error(
                "decision_cycle",
                "must have complete project, campaign, and experiment lineage",
            )
        })
}

fn ensure_terminal_cycle_in_transaction(
    transaction: &Transaction<'_>,
    campaign_id: &str,
    experiment_id: &str,
    now: i64,
) -> Result<DecisionCycle, AppError> {
    let lineage = transaction
        .query_row(
            "SELECT c.project_id, e.campaign_id, e.status,
                    COALESCE(e.finished_at, e.updated_at)
             FROM campaigns c
             JOIN projects p ON p.project_id = c.project_id
             JOIN experiments e ON e.experiment_id = ?2
             WHERE c.campaign_id = ?1",
            params![campaign_id, experiment_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, ExperimentStatus>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()
        .map_err(database_error("read terminal decision lineage"))?;
    let Some((
        _project_id,
        experiment_campaign_id,
        experiment_status,
        source_terminal_at,
    )) = lineage
    else {
        return Err(validation_error(
            "source_experiment_id",
            "must identify an experiment linked to an existing campaign and project",
        ));
    };
    if experiment_campaign_id != campaign_id
        || !is_terminal(experiment_status)
        || source_terminal_at <= 0
    {
        return Err(validation_error(
            "source_experiment_id",
            "must identify a terminal experiment in the same campaign",
        ));
    }

    let cycle_id = decision_cycle_id(campaign_id, experiment_id);
    transaction
        .execute(
            "INSERT INTO decision_cycles (
                cycle_id, campaign_id, source_experiment_id, state, next_wake_at,
                consecutive_failed_attempts, last_decision_kind, last_failure_code,
                last_failure_summary, created_at, updated_at, source_terminal_at
             ) VALUES (?1, ?2, ?3, 'pending', NULL, 0, NULL, NULL, NULL, ?4, ?4, ?5)
             ON CONFLICT(campaign_id, source_experiment_id) DO NOTHING",
            params![cycle_id, campaign_id, experiment_id, now, source_terminal_at],
        )
        .map_err(database_error("insert terminal decision cycle"))?;
    read_cycle_for_source(transaction, campaign_id, experiment_id)
}

fn research_snapshot_matches(
    current: &ResearchOwnershipSnapshot,
    expected: &ResearchOwnershipSnapshot,
) -> bool {
    current == expected
}

fn research_snapshot_stable_matches(
    current: &ResearchOwnershipSnapshot,
    expected: &ResearchOwnershipSnapshot,
) -> bool {
    current.review_id == expected.review_id
        && current.project_id == expected.project_id
        && current.campaign_id == expected.campaign_id
        && current.source_experiment_id == expected.source_experiment_id
        && current.managed_task_signature == expected.managed_task_signature
        && current.source_task_id == expected.source_task_id
        && current.attempt == expected.attempt
        && current.session_generation == expected.session_generation
        && current.event_id == expected.event_id
        && current.agent_run_id == expected.agent_run_id
        && current.termination_request_id == expected.termination_request_id
        && current.successor_experiment_id == expected.successor_experiment_id
        && !current.recovery_required
        && !expected.recovery_required
}

fn validate_research_terminal_event_input(
    event: &NewEvent,
    project_id: &str,
    campaign_id: &str,
    experiment_id: &str,
    cycle_id: &str,
) -> Result<(), AppError> {
    if event.project_id != project_id
        || event.kind != EventKind::CampaignDecision
        || event.campaign_id.as_deref() != Some(campaign_id)
        || event.experiment_id.as_deref() != Some(experiment_id)
        || event.dedup_key != campaign_decision_dedup_key(cycle_id)
        || event.payload.get("source").and_then(serde_json::Value::as_str)
            != Some("terminal_experiment")
        || event.payload.get("cycle_id").and_then(serde_json::Value::as_str)
            != Some(cycle_id)
        || event
            .payload
            .get("source_experiment_id")
            .and_then(serde_json::Value::as_str)
            != Some(experiment_id)
    {
        return Err(validation_error(
            "research.handoff",
            "terminal event must carry the exact research cycle lineage",
        ));
    }
    Ok(())
}

fn read_research_terminal_event_in_transaction(
    transaction: &Transaction<'_>,
    project_id: &str,
    cycle_id: &str,
) -> Result<Option<Event>, AppError> {
    transaction
        .query_row(
            "SELECT event_id, project_id, campaign_id, experiment_id, kind,
                    dedup_key, payload_json, status, attempts, not_before,
                    lease_until, created_at, completed_at, last_error
             FROM events
             WHERE project_id = ?1 AND dedup_key = ?2",
            params![project_id, campaign_decision_dedup_key(cycle_id)],
            |row| {
                let payload_json: String = row.get(6)?;
                let payload = serde_json::from_str(&payload_json).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        6,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
                Ok(Event {
                    event_id: row.get(0)?,
                    project_id: row.get(1)?,
                    campaign_id: row.get(2)?,
                    experiment_id: row.get(3)?,
                    kind: row.get(4)?,
                    dedup_key: row.get(5)?,
                    payload,
                    status: row.get(7)?,
                    attempts: row.get(8)?,
                    not_before: row.get(9)?,
                    lease_until: row.get(10)?,
                    created_at: row.get(11)?,
                    completed_at: row.get(12)?,
                    last_error: row.get(13)?,
                })
            },
        )
        .optional()
        .map_err(database_error("read research terminal event"))
}

fn validate_research_terminal_event(
    existing: &Event,
    expected: &NewEvent,
    project_id: &str,
    campaign_id: &str,
    experiment_id: &str,
    cycle_id: &str,
) -> Result<(), AppError> {
    validate_research_terminal_event_input(
        expected,
        project_id,
        campaign_id,
        experiment_id,
        cycle_id,
    )?;
    if existing.project_id != project_id
        || existing.campaign_id.as_deref() != Some(campaign_id)
        || existing.experiment_id.as_deref() != Some(experiment_id)
        || existing.kind != EventKind::CampaignDecision
        || existing.dedup_key != expected.dedup_key
        || existing.payload != expected.payload
    {
        return Err(validation_error(
            "research.handoff",
            "existing terminal event conflicts with the research lineage",
        ));
    }
    Ok(())
}

fn validate_unclaimed_research_terminal_event(
    transaction: &Transaction<'_>,
    event: &Event,
    cycle_id: &str,
) -> Result<(), AppError> {
    if !matches!(event.status, EventStatus::Pending | EventStatus::RetryWait)
        || event.attempts != 0
        || event.lease_until.is_some()
        || event.completed_at.is_some()
        || event.last_error.is_some()
    {
        return Err(validation_error(
            "research.handoff",
            "existing terminal event is already claimed or resolved",
        ));
    }
    let owned: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM agent_run_events
                 WHERE project_id = ?1 AND event_id = ?2
             ) OR EXISTS(
                 SELECT 1 FROM agent_runs
                 WHERE project_id = ?1 AND primary_event_id = ?2
             ) OR EXISTS(
                 SELECT 1 FROM decision_attempts
                 WHERE cycle_id = ?3
             ) OR EXISTS(
                 SELECT 1 FROM decision_attempts
                 WHERE agent_run_id IN (
                     SELECT run_id FROM agent_run_events
                     WHERE project_id = ?1 AND event_id = ?2
                 )
             )",
            params![event.project_id, event.event_id, cycle_id],
            |row| row.get(0),
        )
        .map_err(database_error("check research terminal event ownership"))?;
    if owned {
        return Err(validation_error(
            "research.handoff",
            "existing terminal event has durable run ownership",
        ));
    }
    Ok(())
}

fn research_handoff_notes(
    owner: &ResearchOwnershipSnapshot,
    context_json: Option<&str>,
    context_digest: Option<&str>,
    response_json: Option<&str>,
    notes_json: Option<&str>,
    objective_digest: &str,
) -> Result<String, AppError> {
    let Some(context_json) = context_json else {
        return Err(validation_error(
            "research.handoff",
            "research context is missing",
        ));
    };
    if context_json.is_empty()
        || context_json.len() > crate::research_evidence::MAX_RESEARCH_CONTEXT_BYTES
    {
        return Err(validation_error(
            "research.handoff",
            "research context is empty or oversized",
        ));
    }
    let Some(context_digest) = context_digest else {
        return Err(validation_error(
            "research.handoff",
            "research context digest is missing",
        ));
    };
    if format!("{:x}", Sha256::digest(context_json.as_bytes())) != context_digest {
        return Err(validation_error(
            "research.handoff",
            "research context digest is invalid",
        ));
    }
    let context = serde_json::from_str::<serde_json::Value>(context_json).map_err(|_| {
        validation_error("research.handoff", "research context is invalid JSON")
    })?;
    if !research_context_identity_matches(
        &context,
        &owner.project_id,
        &owner.campaign_id,
        &owner.review_id,
        &owner.source_experiment_id,
        &owner.managed_task_signature,
        owner.source_task_id,
        objective_digest,
    ) {
        return Err(validation_error(
            "research.handoff",
            "research context identity does not match the confirmed owner",
        ));
    }
    let Some(response_json) = response_json else {
        return Err(validation_error(
            "research.handoff",
            "research response is missing",
        ));
    };
    let answer = parse_research_answer(response_json.as_bytes()).map_err(|_| {
        validation_error("research.handoff", "research response is invalid")
    })?;
    if answer.action != "stop_and_next"
        || answer.review_id != owner.review_id
        || answer.experiment_id != owner.source_experiment_id
        || answer.context_digest != context_digest
    {
        return Err(validation_error(
            "research.handoff",
            "research response does not match the confirmed owner",
        ));
    }
    let context_evidence_refs = context_evidence_refs(&context);
    if answer
        .evidence_refs
        .iter()
        .any(|evidence_ref| !context_evidence_refs.contains(evidence_ref))
        || answer.checkpoint.as_ref().is_some_and(|checkpoint| {
            checkpoint
                .support_evidence_refs
                .iter()
                .any(|evidence_ref| !context_evidence_refs.contains(evidence_ref))
        })
    {
        return Err(validation_error(
            "research.handoff",
            "research response cites evidence outside its context",
        ));
    }
    let mut notes = notes_json
        .map(|value| {
            serde_json::from_str::<serde_json::Value>(value).map_err(|_| {
                validation_error("research.handoff", "research notes are invalid JSON")
            })
        })
        .transpose()?
        .unwrap_or_else(|| serde_json::json!({}));
    if !notes.is_object() {
        return Err(validation_error(
            "research.handoff",
            "research notes must be a JSON object",
        ));
    }
    notes["saved_advice"] = serde_json::json!(bounded_redacted_text(&answer.notes));
    serde_json::to_string(&notes).map_err(|source| AppError::Serialization {
        operation: "serialize research handoff notes",
        source,
    })
}

fn validate_existing_terminal_event_in_transaction(
    transaction: &Transaction<'_>,
    expected: &NewEvent,
    cycle_id: &str,
) -> Result<(), AppError> {
    let existing: Option<(EventKind, Option<String>, Option<String>, String)> = transaction
        .query_row(
            "SELECT kind, campaign_id, experiment_id, payload_json
             FROM events
             WHERE project_id = ?1 AND dedup_key = ?2",
            params![expected.project_id, expected.dedup_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(database_error("read deferred terminal decision event"))?;
    let Some((kind, campaign_id, experiment_id, payload_json)) = existing else {
        return Ok(());
    };
    let payload: serde_json::Value = serde_json::from_str(&payload_json).map_err(|source| {
        AppError::Serialization {
            operation: "parse deferred terminal decision event",
            source,
        }
    })?;
    if kind != EventKind::CampaignDecision
        || campaign_id.as_deref() != expected.campaign_id.as_deref()
        || experiment_id.as_deref() != expected.experiment_id.as_deref()
        || payload != expected.payload
        || payload.get("cycle_id").and_then(serde_json::Value::as_str)
            != Some(cycle_id)
    {
        return Err(validation_error(
            "campaign_decision_event",
            "conflicts with the existing terminal decision projection",
        ));
    }
    Ok(())
}

fn validate_authority_lineage(authority: &DecisionAuthority) -> Result<(), AppError> {
    if authority.experiment_campaign_id != authority.cycle.campaign_id
        || !is_terminal(authority.experiment_status)
    {
        Err(validation_error(
            "source_experiment_id",
            "must identify a terminal experiment in the decision campaign",
        ))
    } else {
        Ok(())
    }
}

fn validate_active_authority(
    authority: &DecisionAuthority,
    expected_project_id: Option<&str>,
) -> Result<(), AppError> {
    validate_authority_lineage(authority)?;
    if expected_project_id.is_some_and(|project_id| project_id != authority.project_id) {
        return Err(validation_error(
            "project_id",
            "must own the decision campaign",
        ));
    }
    if !authority.project_enabled || authority.project_paused || authority.project_halted {
        return Err(validation_error(
            "project",
            "must be enabled, unpaused, and unhalted for decision analysis",
        ));
    }
    if authority.campaign_state != CampaignState::Active {
        return Err(validation_error(
            "campaign",
            "must be active for decision analysis",
        ));
    }
    Ok(())
}

fn validate_reservation_lineage(
    authority: &DecisionAuthority,
    reservation: &DecisionReservation,
) -> Result<(), AppError> {
    if reservation.campaign_id != authority.cycle.campaign_id
        || reservation.source_experiment_id != authority.cycle.source_experiment_id
    {
        Err(validation_error(
            "decision_reservation",
            "does not match the persisted decision lineage",
        ))
    } else {
        Ok(())
    }
}

fn validate_research_decision_ownership(
    transaction: &Transaction<'_>,
    authority: &DecisionAuthority,
    cycle_id: &str,
) -> Result<(), AppError> {
    if research_decision_work_allowed(
        transaction,
        &authority.project_id,
        &authority.cycle.campaign_id,
        &authority.cycle.source_experiment_id,
        cycle_id,
    )? {
        Ok(())
    } else {
        Err(validation_error(
            "research_ownership",
            "must be attached to the exact terminal decision cycle before decision work",
        ))
    }
}

fn research_decision_work_allowed(
    transaction: &Transaction<'_>,
    project_id: &str,
    campaign_id: &str,
    source_experiment_id: &str,
    cycle_id: &str,
) -> Result<bool, AppError> {
    let ownership = research_ownership_in_transaction(
        transaction,
        project_id,
        campaign_id,
        source_experiment_id,
    )?;
    Ok(match ownership {
        ResearchOwnership::None => true,
        ResearchOwnership::Attached(snapshot) => {
            snapshot.decision_cycle_id.as_deref() == Some(cycle_id)
        }
        ResearchOwnership::Open(_) => false,
    })
}

fn read_cycle(connection: &Connection, cycle_id: &str) -> Result<DecisionCycle, AppError> {
    connection
        .query_row(
            &format!("{DECISION_CYCLE_SELECT} WHERE cycle_id = ?1"),
            [cycle_id],
            decision_cycle_from_row,
        )
        .optional()
        .map_err(database_error("read decision cycle"))?
        .ok_or_else(|| validation_error("decision_cycle", "does not exist"))
}

fn read_cycle_for_source(
    connection: &Connection,
    campaign_id: &str,
    experiment_id: &str,
) -> Result<DecisionCycle, AppError> {
    connection
        .query_row(
            &format!(
                "{DECISION_CYCLE_SELECT} WHERE campaign_id = ?1 AND source_experiment_id = ?2"
            ),
            params![campaign_id, experiment_id],
            decision_cycle_from_row,
        )
        .map_err(database_error("read terminal decision cycle"))
}

fn read_attempt(
    connection: &Connection,
    cycle_id: &str,
    attempt_number: i64,
) -> Result<DecisionAttempt, AppError> {
    connection
        .query_row(
            &format!(
                "{DECISION_ATTEMPT_SELECT} WHERE cycle_id = ?1 AND attempt_number = ?2"
            ),
            params![cycle_id, attempt_number],
            decision_attempt_from_row,
        )
        .optional()
        .map_err(database_error("read decision attempt"))?
        .ok_or_else(|| validation_error("decision_attempt", "does not exist"))
}

fn read_attempt_for_run(
    connection: &Connection,
    run_id: i64,
) -> Result<DecisionAttempt, AppError> {
    connection
        .query_row(
            &format!("{DECISION_ATTEMPT_SELECT} WHERE agent_run_id = ?1"),
            [run_id],
            decision_attempt_from_row,
        )
        .optional()
        .map_err(database_error("read decision attempt by agent run"))?
        .ok_or_else(|| validation_error("agent_run_id", "is not bound to a decision attempt"))
}

fn decision_cycle_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DecisionCycle> {
    Ok(DecisionCycle {
        cycle_id: row.get(0)?,
        campaign_id: row.get(1)?,
        source_experiment_id: row.get(2)?,
        source_terminal_at: row.get(3)?,
        state: row.get(4)?,
        next_wake_at: row.get(5)?,
        consecutive_failed_attempts: row.get(6)?,
        last_decision_kind: row.get(7)?,
        last_failure_code: row.get(8)?,
        last_failure_summary: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}

fn decision_attempt_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DecisionAttempt> {
    Ok(DecisionAttempt {
        cycle_id: row.get(0)?,
        attempt_number: row.get(1)?,
        state: row.get(2)?,
        context_schema_version: row.get(3)?,
        context_json: row.get(4)?,
        context_digest: row.get(5)?,
        agent_run_id: row.get(6)?,
        decision_json: row.get(7)?,
        decision_digest: row.get(8)?,
        decision_kind: row.get(9)?,
        failure_code: row.get(10)?,
        failure_summary: row.get(11)?,
        created_at: row.get(12)?,
        started_at: row.get(13)?,
        finished_at: row.get(14)?,
    })
}

fn decision_cycle_id(campaign_id: &str, experiment_id: &str) -> String {
    let digest = Sha256::digest(format!("{campaign_id}\0{experiment_id}").as_bytes());
    format!("decision-cycle:{digest:x}")
}

fn stored_terminal_exit_code(result: &serde_json::Value) -> Option<i32> {
    match result {
        serde_json::Value::Object(object) => object
            .get("Failed")
            .or_else(|| object.get("Success"))
            .and_then(serde_json::Value::as_i64)
            .and_then(|code| i32::try_from(code).ok()),
        _ => None,
    }
}

fn terminal_observation_matches_experiment(
    status: ExperimentStatus,
    state: &str,
    result: Option<&serde_json::Value>,
) -> bool {
    let observed_status = if state.eq_ignore_ascii_case("killed") {
        ExperimentStatus::Cancelled
    } else if state.eq_ignore_ascii_case("failed")
        || result.is_some_and(crate::events::result_is_failure)
    {
        ExperimentStatus::Failed
    } else {
        ExperimentStatus::Succeeded
    };
    status == observed_status
}

fn validate_payload(field: &'static str, value: &str) -> Result<(), AppError> {
    if value.is_empty() || value.len() > MAX_DECISION_PAYLOAD_BYTES {
        Err(validation_error(
            field,
            "must be non-empty and no larger than 128 KiB",
        ))
    } else {
        Ok(())
    }
}

fn validate_token(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), AppError> {
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        Err(validation_error(
            field,
            "must be non-empty, bounded, and contain no control characters",
        ))
    } else {
        Ok(())
    }
}

fn validate_bounded_text(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), AppError> {
    if value.len() > max_bytes {
        Err(validation_error(field, "exceeds the persisted size limit"))
    } else {
        Ok(())
    }
}

fn is_terminal(status: ExperimentStatus) -> bool {
    matches!(
        status,
        ExperimentStatus::Succeeded | ExperimentStatus::Failed | ExperimentStatus::Cancelled
    )
}

fn validation_error(
    field: &'static str,
    message: &'static str,
) -> AppError {
    AppError::Validation { field, message }
}

#[cfg(test)]
mod research_attachment_tests {
    use super::*;
    use crate::db::EventRepository;
    use tempfile::TempDir;

    fn snapshot() -> ResearchOwnershipSnapshot {
        ResearchOwnershipSnapshot {
            review_id: "review-1".to_owned(),
            project_id: "project-1".to_owned(),
            campaign_id: "campaign-1".to_owned(),
            source_experiment_id: "experiment-1".to_owned(),
            managed_task_signature: "pueue-managed-run:v1:signature".to_owned(),
            source_task_id: Some(41),
            attempt: 1,
            session_generation: 0,
            event_id: Some(1),
            operation_stage: Some("stop_confirmed".to_owned()),
            agent_run_id: Some(7),
            termination_request_id: Some(9),
            decision_cycle_id: None,
            successor_experiment_id: None,
            recovery_required: false,
        }
    }

    #[test]
    fn attachment_rejects_missing_exact_owner_without_mutating_cycle_or_event() {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        let mut connection = db.connect().unwrap();
        let transaction = connection.transaction().unwrap();
        let owner = snapshot();
        let event = NewEvent::new(
            owner.project_id.clone(),
            EventKind::CampaignDecision,
            campaign_decision_dedup_key("decision-cycle:missing"),
            serde_json::json!({
                "source": "terminal_experiment",
                "cycle_id": "decision-cycle:missing",
                "source_experiment_id": owner.source_experiment_id,
            }),
            100,
            100,
        )
        .with_campaign_lineage(owner.campaign_id.clone(), Some(owner.source_experiment_id.clone()));

        assert!(DecisionRepository::attach_research_terminal_cycle_in_transaction(
            &transaction,
            &owner,
            &event,
            100,
        )
        .is_err());
        transaction.commit().unwrap();
        assert_eq!(
            db.connect()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM decision_cycles", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            db.connect()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM events", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn handoff_notes_preserve_the_session_envelope_and_save_bounded_advice() {
        let owner = snapshot();
        let context_json = serde_json::json!({
            "schema_version": 1,
            "facts": {
                "review": {
                    "review_id": owner.review_id.clone(),
                    "experiment_id": owner.source_experiment_id.clone(),
                    "task_signature": owner.managed_task_signature.clone(),
                },
                "campaign": {"campaign_id": owner.campaign_id.clone()},
                "project": {"project_id": owner.project_id.clone()},
                "objective": {"digest": "objective-digest"},
                "target": {
                    "experiment_id": owner.source_experiment_id.clone(),
                    "pueue_task_id": owner.source_task_id,
                    "task_signature": owner.managed_task_signature.clone(),
                },
                "evidence": [{"evidence_ref": "research:review-1"}],
            },
        })
        .to_string();
        let context_digest = format!("{:x}", Sha256::digest(context_json.as_bytes()));
        let response_json = serde_json::json!({
            "schema_version": 1,
            "review_id": owner.review_id.clone(),
            "experiment_id": owner.source_experiment_id.clone(),
            "context_digest": context_digest.clone(),
            "action": "stop_and_next",
            "reason": "confirmed",
            "evidence_refs": ["research:review-1"],
            "notes": "bounded advice",
            "next_direction": "continue",
            "checkpoint": null,
        })
        .to_string();
        let notes = research_handoff_notes(
            &owner,
            Some(&context_json),
            Some(&context_digest),
            Some(&response_json),
            Some(r#"{"session_binding":"confirmed"}"#),
            "objective-digest",
        )
        .unwrap();
        let notes: serde_json::Value = serde_json::from_str(&notes).unwrap();
        assert_eq!(
            notes.get("session_binding").and_then(serde_json::Value::as_str),
            Some("confirmed")
        );
        assert_eq!(
            notes.get("saved_advice").and_then(serde_json::Value::as_str),
            Some("bounded advice")
        );
    }

    #[test]
    fn handoff_notes_reject_context_without_exact_owner_identity() {
        let owner = snapshot();
        let context_json = r#"{"schema_version":1}"#;
        let context_digest = format!("{:x}", Sha256::digest(context_json.as_bytes()));
        let response_json = serde_json::json!({
            "schema_version": 1,
            "review_id": owner.review_id.clone(),
            "experiment_id": owner.source_experiment_id.clone(),
            "context_digest": context_digest.clone(),
            "action": "stop_and_next",
            "reason": "confirmed",
            "evidence_refs": ["unrelated"],
            "notes": "bounded advice",
            "next_direction": "continue",
            "checkpoint": null,
        })
        .to_string();

        assert!(research_handoff_notes(
            &owner,
            Some(context_json),
            Some(&context_digest),
            Some(&response_json),
            Some(r#"{"session_binding":"confirmed"}"#),
            "objective-digest",
        )
        .is_err());
    }

    #[test]
    fn existing_pending_event_is_eligible_for_unclaimed_adoption() {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        db.connect()
            .unwrap()
            .execute(
                "INSERT INTO projects (
                     project_id, root_path, pueue_group, config_path,
                     enabled, paused, created_at, updated_at
                 ) VALUES ('project-1', '/tmp/project-1', 'group-1',
                           '/tmp/project-1/config.toml', 1, 0, 1, 1)",
                [],
            )
            .unwrap();
        let expected = NewEvent::new(
            "project-1",
            EventKind::CampaignDecision,
            "campaign-decision:v1:cycle-1",
            serde_json::json!({"source": "terminal_experiment"}),
            100,
            100,
        );
        let stored = EventRepository::new(&db)
            .insert_idempotent(&expected)
            .unwrap();
        let mut connection = db.connect().unwrap();
        let transaction = connection.transaction().unwrap();
        let event = read_research_terminal_event_in_transaction(
            &transaction,
            "project-1",
            "cycle-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(event.event_id, stored.event_id);
        validate_unclaimed_research_terminal_event(&transaction, &event, "cycle-1").unwrap();
        transaction.commit().unwrap();

        let connection = db.connect().unwrap();
        connection
            .execute_batch("PRAGMA foreign_keys = OFF;")
            .unwrap();
        connection
            .execute(
                "INSERT INTO decision_attempts
                     (cycle_id, attempt_number, state, created_at)
                 VALUES ('cycle-1', 1, 'reserved', 100)",
                [],
            )
            .unwrap();
        drop(connection);

        let mut connection = db.connect().unwrap();
        let transaction = connection.transaction().unwrap();
        let event = read_research_terminal_event_in_transaction(
            &transaction,
            "project-1",
            "cycle-1",
        )
        .unwrap()
        .unwrap();
        assert!(validate_unclaimed_research_terminal_event(&transaction, &event, "cycle-1").is_err());
    }

    #[test]
    fn terminal_decision_event_uses_the_exact_managed_observation_projection() {
        let event = DecisionRepository::terminal_decision_event(
            "project-1",
            "campaign-1",
            "experiment-1",
            TerminalDecisionEventProjection {
                task_id: 41,
                managed_task_signature: "pueue-managed-run:v1:managed",
                group: "research",
                state: "Killed",
                enqueued_at: Some(10),
                started_at: Some(20),
                ended_at: Some(30),
                exit_code: Some(0),
            },
            100,
        );

        let cycle_id = DecisionRepository::terminal_cycle_id("campaign-1", "experiment-1");
        assert_eq!(event.kind, EventKind::CampaignDecision);
        assert_eq!(
            event.dedup_key,
            campaign_decision_dedup_key(&cycle_id)
        );
        assert_eq!(event.campaign_id.as_deref(), Some("campaign-1"));
        assert_eq!(event.experiment_id.as_deref(), Some("experiment-1"));
        assert_eq!(
            event.payload,
            serde_json::json!({
                "source": "terminal_experiment",
                "cycle_id": cycle_id,
                "source_experiment_id": "experiment-1",
                "terminal_observation": {
                    "task_id": 41,
                    "task_signature": "pueue-managed-run:v1:managed",
                    "group": "research",
                    "state": "Killed",
                    "enqueued_at": 10,
                    "started_at": 20,
                    "ended_at": 30,
                    "exit_code": 0,
                },
            })
        );
    }
}

#[cfg(test)]
mod due_query_plan_tests {
    use rusqlite::{params, StatementStatus};
    use tempfile::TempDir;

    use super::{
        CAMPAIGN_PENDING_DECISION_SQL, GLOBAL_DUE_WAIT_DECISION_SQL,
    };
    use crate::db::Db;

    #[test]
    fn every_due_state_probe_is_direct_bounded_and_index_ordered() {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        for (name, sql, parameters, expected_index) in [
            (
                "global due waiting",
                GLOBAL_DUE_WAIT_DECISION_SQL,
                vec![
                    &100_i64 as &dyn rusqlite::ToSql,
                    &4_i64 as &dyn rusqlite::ToSql,
                ],
                "decision_cycles_state_wake_source_order_idx",
            ),
            (
                "campaign pending",
                CAMPAIGN_PENDING_DECISION_SQL,
                vec![&"campaign-a" as &dyn rusqlite::ToSql],
                "decision_cycles_campaign_state_source_order_idx",
            ),
        ] {
            assert!(!sql.contains(" OR "), "{name}: {sql}");
            assert!(!sql.contains("experiments"), "{name}: {sql}");
            let connection = db.connect().unwrap();
            let mut statement = connection
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap();
            let details = statement
                .query_map(rusqlite::params_from_iter(parameters), |row| {
                    row.get::<_, String>(3)
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(
                details.iter().any(|detail| detail.contains(expected_index)),
                "{name}: {details:?}"
            );
            assert!(
                details.iter().all(|detail| {
                    !detail.starts_with("SCAN decision_cycles")
                        && !detail.contains("TEMP B-TREE")
                }),
                "{name}: {details:?}"
            );
        }
    }

    #[test]
    fn due_wake_probe_has_a_constant_vm_step_bound_before_one_thousand_future_waits() {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        let mut connection = db.connect().unwrap();
        connection
            .execute(
                "INSERT INTO projects (
                    project_id, root_path, pueue_group, config_path,
                    enabled, paused, created_at, updated_at
                 ) VALUES ('project-a', '/tmp/project-a', 'pa-project-a',
                           '/tmp/project-a/config.toml', 1, 0, 1, 1)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO campaigns (
                    campaign_id, project_id, objective_text, objective_digest,
                    initial_argv_json, state, created_at, updated_at
                 ) VALUES ('campaign-a', 'project-a', 'objective', 'digest',
                           '[]', 'active', 1, 1)",
                [],
            )
            .unwrap();
        connection.execute_batch("PRAGMA foreign_keys = OFF;").unwrap();
        let transaction = connection.transaction().unwrap();
        for ordinal in 1..=1_000_i64 {
            transaction
                .execute(
                    "INSERT INTO decision_cycles (
                        cycle_id, campaign_id, source_experiment_id,
                        source_terminal_at, state, next_wake_at,
                        consecutive_failed_attempts, created_at, updated_at
                     ) VALUES (?1, 'campaign-a', ?2, ?3, 'waiting', 10000, 0, 1, 1)",
                    params![
                        format!("future-cycle-{ordinal:04}"),
                        format!("future-experiment-{ordinal:04}"),
                        ordinal,
                    ],
                )
                .unwrap();
        }
        transaction
            .execute(
                "INSERT INTO decision_cycles (
                    cycle_id, campaign_id, source_experiment_id,
                    source_terminal_at, state, next_wake_at,
                    consecutive_failed_attempts, created_at, updated_at
                 ) VALUES ('due-cycle', 'campaign-a', 'due-experiment',
                           2000, 'waiting', 99, 0, 1, 1)",
                [],
            )
            .unwrap();
        transaction.commit().unwrap();

        let mut statement = connection.prepare(GLOBAL_DUE_WAIT_DECISION_SQL).unwrap();
        let cycle_ids = statement
            .query_map(params![100_i64, 1_i64], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let vm_steps = statement.get_status(StatementStatus::VmStep);

        assert_eq!(cycle_ids, ["due-cycle"]);
        assert!(vm_steps < 200, "due wake used {vm_steps} VM steps");
    }

}
