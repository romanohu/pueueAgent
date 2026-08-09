use rusqlite::{params, OptionalExtension};

use crate::{
    db::{database_error, Db, IncidentRepository},
    detect::{Observation, ObservationState, TASK_TERMINAL_RECOVERY_KIND},
    models::{IncidentTransition, NewIncident},
    termination::{TerminationManager, TerminationPolicy},
    AppError,
};

pub struct IncidentStore<'db> {
    db: &'db Db,
}

impl<'db> IncidentStore<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn observe(&self, observation: Observation) -> Result<IncidentTransition, AppError> {
        match observation.state() {
            ObservationState::Active => {
                let update = IncidentRepository::new(self.db).upsert_active(&NewIncident::new(
                    observation.project_id(),
                    observation.kind(),
                    observation.task_key(),
                    observation.fingerprint(),
                    observation.seen_at(),
                ))?;
                if TerminationPolicy.should_kill(&observation) {
                    if let Some(task_signature) = observation.task_signature() {
                        TerminationManager::new_without_pueue(self.db).request_with_reason(
                            update.incident.incident_id,
                            task_signature,
                            termination_reason(&observation),
                            observation.seen_at(),
                            None,
                        )?;
                    }
                }
                Ok(update.transition)
            }
            ObservationState::Recovered if observation.kind() == TASK_TERMINAL_RECOVERY_KIND => {
                self.resolve_task_incidents(&observation)
            }
            ObservationState::Recovered => self.resolve_active(&observation),
        }
    }

    fn resolve_task_incidents(
        &self,
        observation: &Observation,
    ) -> Result<IncidentTransition, AppError> {
        let task_key = observation.task_key().ok_or(AppError::Runtime {
            operation: "resolve terminal task incidents",
        })?;
        let connection = self.db.connect()?;
        let changed = connection
            .execute(
                "UPDATE incidents
                 SET status = 'resolved',
                     resolved_at = ?1,
                     last_seen_at = MAX(last_seen_at, ?1)
                 WHERE project_id = ?2
                   AND task_key = ?3
                   AND status IN ('open', 'acknowledged')",
                params![observation.seen_at(), observation.project_id(), task_key],
            )
            .map_err(database_error("resolve terminal task incidents"))?;
        if changed == 0 {
            Ok(IncidentTransition::Unchanged)
        } else {
            Ok(IncidentTransition::Resolved)
        }
    }

    fn resolve_active(&self, observation: &Observation) -> Result<IncidentTransition, AppError> {
        let connection = self.db.connect()?;
        let incident_id = if let Some(task_key) = observation.task_key() {
            connection.query_row(
                "SELECT incident_id FROM incidents
                 WHERE project_id = ?1 AND kind = ?2
                   AND task_key = ?3
                   AND status IN ('open', 'acknowledged')
                 ORDER BY last_seen_at DESC, incident_id DESC
                 LIMIT 1",
                params![observation.project_id(), observation.kind(), task_key],
                |row| row.get::<_, i64>(0),
            )
        } else {
            connection.query_row(
                "SELECT incident_id FROM incidents
                 WHERE project_id = ?1 AND kind = ?2
                   AND task_key IS NULL
                   AND fingerprint = ?3
                   AND status IN ('open', 'acknowledged')
                 ORDER BY last_seen_at DESC, incident_id DESC
                 LIMIT 1",
                params![
                    observation.project_id(),
                    observation.kind(),
                    observation.fingerprint(),
                ],
                |row| row.get::<_, i64>(0),
            )
        }
        .optional()
        .map_err(database_error("find active incident for recovery"))?;
        if let Some(incident_id) = incident_id {
            IncidentRepository::new(self.db).resolve(incident_id, observation.seen_at())
        } else {
            Ok(IncidentTransition::Unchanged)
        }
    }
}

fn termination_reason(observation: &Observation) -> String {
    let evidence = observation.evidence();
    let bounded_evidence = evidence.chars().take(1024).collect::<String>();
    serde_json::json!({
        "kind": observation.kind(),
        "pattern_name": observation.pattern_name(),
        "action": observation.action().as_str(),
        "confirmation_count": observation.confirmation_count(),
        "evidence": bounded_evidence,
        "source_path": observation.source_path().map(|path| path.display().to_string()),
    })
    .to_string()
}
