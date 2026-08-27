use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use sha2::{Digest, Sha256};

use crate::{
    decision_evidence::{validate_stored_decision_context, DECISION_CONTEXT_SCHEMA_VERSION},
    diagnostics::MAX_EVENT_LIST_LIMIT,
    execution_policy::CampaignLimits,
    models::{
        AgentRunStatus, CampaignState, DecisionAttempt, DecisionAttemptState, DecisionCycle,
        DecisionCycleState, Event, EventKind, ExperimentStatus, NewEvent,
    },
    AppError,
};

use super::{database_error, repositories::insert_event_idempotent_in_transaction, Db};

const MAX_DECISION_PAYLOAD_BYTES: usize = 128 * 1024;
const MAX_DECISION_DIGEST_BYTES: usize = 256;
const MAX_DECISION_CODE_BYTES: usize = 128;
const MAX_DECISION_SUMMARY_BYTES: usize = 2_048;
const MAX_TERMINAL_DECISION_BACKFILL: i64 = 128;

pub(crate) fn campaign_decision_dedup_key(cycle_id: &str) -> String {
    format!("campaign-decision:v1:{cycle_id}")
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
        if attempt.state != DecisionAttemptState::EvidenceReady
            || attempt.context_schema_version
                != Some(i64::from(DECISION_CONTEXT_SCHEMA_VERSION))
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

    pub(crate) fn terminal_cycle_id(campaign_id: &str, experiment_id: &str) -> String {
        decision_cycle_id(campaign_id, experiment_id)
    }

    pub fn publish_terminal_cycle_event(
        &self,
        campaign_id: &str,
        experiment_id: &str,
        event: &NewEvent,
        now: i64,
    ) -> Result<(DecisionCycle, Event), AppError> {
        let expected_cycle_id = decision_cycle_id(campaign_id, experiment_id);
        if event.kind != EventKind::CampaignDecision
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
        let (event, _) = insert_event_idempotent_in_transaction(&transaction, event)?;
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
        Ok((cycle, event))
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
            let cycle_id = decision_cycle_id(&backfill.campaign_id, &backfill.experiment_id);
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
            let event = NewEvent::new(
                &backfill.project_id,
                EventKind::CampaignDecision,
                campaign_decision_dedup_key(&cycle_id),
                serde_json::json!({
                    "source": "terminal_experiment",
                    "cycle_id": cycle_id,
                    "source_experiment_id": backfill.experiment_id,
                    "terminal_observation": {
                        "task_id": backfill.pueue_task_id,
                        "task_signature": backfill.managed_task_signature,
                        "group": backfill.pueue_group,
                        "state": backfill.state,
                        "enqueued_at": backfill.enqueued_at,
                        "started_at": backfill.started_at,
                        "ended_at": backfill.ended_at,
                        "exit_code": exit_code,
                    },
                }),
                now,
                now,
            )
            .with_campaign_lineage(
                backfill.campaign_id.clone(),
                Some(backfill.experiment_id.clone()),
            );
            self.publish_terminal_cycle_event(
                &backfill.campaign_id,
                &backfill.experiment_id,
                &event,
                now,
            )?;
            count += 1;
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
        validate_payload("context_json", context_json)?;
        validate_token("context_digest", context_digest, MAX_DECISION_DIGEST_BYTES)?;
        let attempt = read_attempt(
            &transaction,
            &reservation.cycle_id,
            reservation.attempt_number,
        )?;
        if attempt.state == DecisionAttemptState::EvidenceReady
            && attempt.context_schema_version == Some(1)
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
                 SET state = 'evidence_ready', context_schema_version = 1,
                     context_json = ?1, context_digest = ?2
                 WHERE cycle_id = ?3 AND attempt_number = ?4 AND state = 'reserved'",
                params![
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
            let project_id: String = transaction
                .query_row(
                    "SELECT project_id FROM campaigns WHERE campaign_id = ?1",
                    [&cycle.campaign_id],
                    |row| row.get(0),
                )
                .map_err(database_error("read due decision event project"))?;
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
