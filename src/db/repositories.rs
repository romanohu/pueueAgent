use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
};

use rusqlite::{
    params,
    params_from_iter,
    types::{Type, Value},
    Connection, OptionalExtension, Row, Transaction, TransactionBehavior,
};
use serde_json::json;
use uuid::Uuid;

use crate::{
    batches::{
        derive_request_status, validate_accepted_result, validate_error, validate_new_batch,
        BatchJobResult, MAX_BATCH_ARGV_JSON_BYTES, MAX_BATCH_METADATA_JSON_BYTES,
    },
    diagnostics::{EventFilter, MAX_EVENT_LIST_LIMIT},
    environment::{
        MAX_PRIVATE_TEMP_RUN_ID, PrivateRunTemp, ProjectAdmissionLock, RunIdAdmissionGuard,
    },
    execution_policy::{
        PolicyViolation, PolicyViolationCode, PolicyViolationStage, ProjectRootAnchor,
        VerifiedProjectRoot,
    },
    interventions::{
        validate_message, Intervention, InterventionCounts, InterventionReservation,
        MAX_INTERVENTIONS_PER_RUN, MAX_INTERVENTION_BYTES_PER_RUN,
    },
    models::{
        path_text, AgentContextMode, AgentRun, AgentRunEvent, AgentRunStatus, BatchJob,
        BatchJobStatus, BatchRequest, BatchStatus, CampaignState, CodeChangeState,
        DecisionCycleState, Event,
        EventKind, EventStatus, ExecutionProjection, Incident, IncidentTransition, IncidentUpdate,
        IntegrationEvent, InterventionStatus, NewAgentRun, NewBatchRequest, NewEvent, NewIncident,
        NewIntegrationEvent, NewProject, NewSubmission, NewTaskObservation, NewTerminationRequest,
        Project, Submission, SubmissionKind, SubmissionStatus, TaskObservation, TerminationRequest,
        TerminationRequestStatus,
    },
    output::bounded_redacted_text,
    retry::{retry_backoff_seconds, retry_decision, EventResolution, RetryDecision, RetryPolicy},
    AppError,
};

use super::{
    database_error,
    decisions::{campaign_decision_dedup_key, DecisionRepository, DecisionReservation},
    Db,
};

const EVENT_CLAIM_WORK_LIMIT: usize = MAX_EVENT_LIST_LIMIT;
const EVENT_CLAIM_PROBE_PASSES: usize = 2;

fn event_claim_candidate_sql(status: EventStatus) -> String {
    let status = match status {
        EventStatus::Pending => "pending",
        EventStatus::RetryWait => "retry_wait",
        _ => unreachable!("event claim probes only pending and retry-wait states"),
    };
    format!(
        "SELECT event_id, project_id, not_before, created_at
         FROM events INDEXED BY events_claimable_idx
         WHERE status IN ('pending', 'retry_wait')
           AND status = '{status}' AND not_before <= ?1
           AND kind NOT IN ('health_diagnosis', 'code_change')
         ORDER BY not_before, created_at, event_id
         LIMIT ?2"
    )
}

pub struct ProjectRepository<'db> {
    db: &'db Db,
}

fn acquire_run_id_admission_guard(db: &Db) -> Result<RunIdAdmissionGuard, AppError> {
    match RunIdAdmissionGuard::try_acquire(db.run_id_lock_parent())? {
        Some(guard) => Ok(guard),
        None => Err(AppError::Runtime {
            operation: "acquire agent run ID admission guard",
        }),
    }
}

pub(super) struct ProjectLifecycleAdmission {
    _verified_root: VerifiedProjectRoot,
    _lock: ProjectAdmissionLock,
}

pub(super) fn acquire_project_lifecycle_admission(
    db: &Db,
    project_id: &str,
    operation: &'static str,
) -> Result<ProjectLifecycleAdmission, AppError> {
    let project = ProjectRepository::new(db)
        .find_by_id(project_id)?
        .ok_or(AppError::Runtime {
            operation: "read project for lifecycle admission",
        })?;
    let root_anchor = ProjectRootAnchor::resolve(&project.root_path).map_err(AppError::from)?;
    let verified_root = root_anchor.verify_identity().map_err(AppError::from)?;
    let project_lock = ProjectAdmissionLock::try_acquire(&verified_root)
        .map_err(AppError::from)?
        .ok_or(AppError::Runtime { operation })?;
    let verified_root = root_anchor.verify_identity().map_err(AppError::from)?;
    Ok(ProjectLifecycleAdmission {
        _verified_root: verified_root,
        _lock: project_lock,
    })
}

impl<'db> ProjectRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn register(&self, project: &NewProject) -> Result<Project, AppError> {
        let run_id_guard = acquire_run_id_admission_guard(self.db)?;
        let canonical_root =
            fs::canonicalize(&project.root_path).map_err(|source| AppError::Io {
                operation: "canonicalize project root",
                source,
            })?;
        let root_anchor = ProjectRootAnchor::resolve(&canonical_root).map_err(AppError::from)?;
        let verified_root = root_anchor.verify_identity().map_err(AppError::from)?;
        let project_lock = match ProjectAdmissionLock::try_acquire(&verified_root)? {
            Some(lock) => lock,
            None => {
                return Err(AppError::Runtime {
                    operation: "acquire project registration admission lock",
                });
            }
        };
        let retained_generations = PrivateRunTemp::inspect_generation_floor(&verified_root)?;
        let durable_run_id_high_water =
            AgentRunRepository::new(self.db).durable_run_id_high_water(&run_id_guard)?;
        retained_generations.validate_durable_high_water(durable_run_id_high_water)?;
        let root_path = path_text(&canonical_root, "root_path")?;
        let config_path = path_text(&project.config_path, "config_path")?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin project registration"))?;

        if exists(
            &transaction,
            "SELECT 1 FROM projects WHERE root_path = ?1",
            root_path,
        )? {
            return Err(AppError::DatabaseConflict { field: "root_path" });
        }
        if exists(
            &transaction,
            "SELECT 1 FROM projects WHERE pueue_group = ?1",
            &project.pueue_group,
        )? {
            return Err(AppError::DatabaseConflict {
                field: "pueue_group",
            });
        }
        if exists(
            &transaction,
            "SELECT 1 FROM projects WHERE project_id = ?1",
            &project.project_id,
        )? {
            return Err(AppError::DatabaseConflict {
                field: "project_id",
            });
        }

        transaction
            .execute(
                "INSERT INTO projects (
                    project_id, root_path, pueue_group, config_path, enabled, paused,
                    halted_reason, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?7)",
                params![
                    project.project_id,
                    root_path,
                    project.pueue_group,
                    config_path,
                    project.enabled,
                    project.paused,
                    project.created_at,
                ],
            )
            .map_err(project_constraint_error)?;
        let registered = transaction
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE project_id = ?1",
                [&project.project_id],
                project_from_row,
            )
            .map_err(database_error("read registered project"))?;
        #[cfg(test)]
        invoke_register_before_commit_hook(&canonical_root);
        root_anchor.verify_identity().map_err(AppError::from)?;
        transaction
            .commit()
            .map_err(database_error("commit project registration"))?;
        drop(project_lock);
        drop(run_id_guard);
        Ok(registered)
    }

    pub fn find_by_group(&self, pueue_group: &str) -> Result<Option<Project>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE pueue_group = ?1",
                [pueue_group],
                project_from_row,
            )
            .optional()
            .map_err(database_error("find project by Pueue group"))
    }

    pub fn find_by_id(&self, project_id: &str) -> Result<Option<Project>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE project_id = ?1",
                [project_id],
                project_from_row,
            )
            .optional()
            .map_err(database_error("find project by ID"))
    }

    pub fn list_enabled(&self) -> Result<Vec<Project>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE enabled = 1 ORDER BY project_id",
            )
            .map_err(database_error("prepare enabled project query"))?;
        let projects = statement
            .query_map([], project_from_row)
            .map_err(database_error("list enabled projects"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read enabled projects"))?;
        Ok(projects)
    }

    pub fn list_all(&self) -> Result<Vec<Project>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects ORDER BY project_id",
            )
            .map_err(database_error("prepare all project query"))?;
        let projects = statement
            .query_map([], project_from_row)
            .map_err(database_error("list all projects"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read all projects"))?;
        Ok(projects)
    }

    pub fn list_active(&self) -> Result<Vec<Project>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects
                 WHERE enabled = 1 AND paused = 0 AND halted_reason IS NULL
                 ORDER BY project_id",
            )
            .map_err(database_error("prepare active project query"))?;
        let projects = statement
            .query_map([], project_from_row)
            .map_err(database_error("list active projects"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read active projects"))?;
        Ok(projects)
    }

    pub fn refresh_admission_authority(
        &self,
        expected: &Project,
    ) -> Result<Option<Project>, AppError> {
        let current = self
            .find_by_id(&expected.project_id)?
            .ok_or(AppError::Runtime {
                operation: "read project during admission revalidation",
            })?;
        if current.root_path != expected.root_path || current.pueue_group != expected.pueue_group {
            return Err(AppError::Validation {
                field: "project",
                message: "root and group identity must remain stable during admission",
            });
        }
        if !current.enabled || current.paused || current.halted_reason.is_some() {
            return Ok(None);
        }
        Ok(Some(current))
    }

    pub fn pause(&self, project_id: &str, now: i64) -> Result<Project, AppError> {
        let _admission = acquire_project_lifecycle_admission(
            self.db,
            project_id,
            "acquire project pause admission lock",
        )?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin project pause"))?;
        transaction
            .execute(
                "UPDATE projects SET paused = 1, updated_at = ?1 WHERE project_id = ?2",
                params![now, project_id],
            )
            .map_err(database_error("pause project"))?;
        let project = transaction
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE project_id = ?1",
                [project_id],
                project_from_row,
            )
            .map_err(database_error("read paused project"))?;
        insert_operator_log(
            &transaction,
            &project,
            "pause",
            &json!({
                "paused": true,
            }),
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit project pause"))?;
        Ok(project)
    }

    pub fn resume(&self, project_id: &str, now: i64) -> Result<Project, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin project resume"))?;
        let was_halted: Option<String> = transaction
            .query_row(
                "SELECT halted_reason FROM projects WHERE project_id = ?1",
                [project_id],
                |row| row.get(0),
            )
            .map_err(database_error("read project halt state before resume"))?;
        transaction
            .execute(
                "UPDATE projects
                 SET paused = 0, halted_reason = NULL, updated_at = ?1
                 WHERE project_id = ?2",
                params![now, project_id],
            )
            .map_err(database_error("resume project"))?;
        let project = transaction
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE project_id = ?1",
                [project_id],
                project_from_row,
            )
            .map_err(database_error("read resumed project"))?;
        insert_operator_log(
            &transaction,
            &project,
            "resume",
            &json!({
                "paused": false,
                "cleared_halt": was_halted.is_some(),
                "previous_halted_reason": was_halted,
            }),
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit project resume"))?;
        Ok(project)
    }

    pub fn halt(&self, project_id: &str, reason: &str, now: i64) -> Result<Project, AppError> {
        let _admission = acquire_project_lifecycle_admission(
            self.db,
            project_id,
            "acquire project halt admission lock",
        )?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin project halt"))?;
        transaction
            .execute(
                "UPDATE projects
                 SET paused = 1, halted_reason = ?1, updated_at = ?2
                 WHERE project_id = ?3",
                params![reason, now, project_id],
            )
            .map_err(database_error("halt project"))?;
        let project = transaction
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE project_id = ?1",
                [project_id],
                project_from_row,
            )
            .map_err(database_error("read halted project"))?;
        insert_operator_log(
            &transaction,
            &project,
            "halt",
            &json!({
                "paused": true,
                "halted_reason": reason,
            }),
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit project halt"))?;
        Ok(project)
    }

    pub fn disable(
        &self,
        project_id: &str,
        now: i64,
        unresolved_task_ids: &[i64],
    ) -> Result<Project, AppError> {
        let _admission = acquire_project_lifecycle_admission(
            self.db,
            project_id,
            "acquire project disable admission lock",
        )?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin project disable"))?;
        transaction
            .execute(
                "UPDATE projects
                 SET enabled = 0, paused = 1, updated_at = ?1
                 WHERE project_id = ?2",
                params![now, project_id],
            )
            .map_err(database_error("disable project"))?;
        let project = transaction
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE project_id = ?1",
                [project_id],
                project_from_row,
            )
            .map_err(database_error("read disabled project"))?;
        insert_operator_log(
            &transaction,
            &project,
            "disable",
            &json!({
                "group_released": false,
                "mode": "keep_reservation",
                "unresolved_task_count": unresolved_task_ids.len(),
                "unresolved_task_ids": unresolved_task_ids,
            }),
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit project disable"))?;
        Ok(project)
    }

    pub fn remove(
        &self,
        project_id: &str,
        now: i64,
        unresolved_task_ids: &[i64],
    ) -> Result<Project, AppError> {
        let _run_id_guard = acquire_run_id_admission_guard(self.db)?;
        let project = self.find_by_id(project_id)?.ok_or(AppError::Runtime {
            operation: "read project before removal",
        })?;
        let root_anchor = ProjectRootAnchor::resolve(&project.root_path).map_err(AppError::from)?;
        let verified_root = root_anchor.verify_identity().map_err(AppError::from)?;
        let _project_lock = match ProjectAdmissionLock::try_acquire(&verified_root)? {
            Some(lock) => lock,
            None => {
                return Err(AppError::Runtime {
                    operation: "acquire project removal admission lock",
                });
            }
        };
        let retained_generations = PrivateRunTemp::inspect_generation_floor(&verified_root)?;
        let durable_run_id_high_water =
            AgentRunRepository::new(self.db).durable_run_id_high_water(&_run_id_guard)?;
        retained_generations.validate_durable_high_water(durable_run_id_high_water)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin project removal"))?;
        let transaction_project = transaction
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE project_id = ?1",
                [project_id],
                project_from_row,
            )
            .map_err(database_error("read project before removal"))?;
        if transaction_project.root_path != project.root_path {
            return Err(AppError::Runtime {
                operation: "verify project root before removal",
            });
        }
        insert_operator_log(
            &transaction,
            &transaction_project,
            "remove",
            &json!({
                "group_released": true,
                "mode": "remove",
                "unresolved_task_count": unresolved_task_ids.len(),
                "unresolved_task_ids": unresolved_task_ids,
            }),
            now,
        )?;
        transaction
            .execute("DELETE FROM projects WHERE project_id = ?1", [project_id])
            .map_err(database_error("remove project"))?;
        #[cfg(test)]
        invoke_remove_before_commit_hook(&project.root_path);
        root_anchor.verify_identity().map_err(AppError::from)?;
        transaction
            .commit()
            .map_err(database_error("commit project removal"))?;
        Ok(transaction_project)
    }

    pub fn record_task_cancellation(
        &self,
        project: &Project,
        task_id: i64,
        task_signature: &str,
        requested_state: &str,
        action: &str,
        final_state: &str,
        reason: &str,
        now: i64,
    ) -> Result<(), AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin task cancellation operator log"))?;
        insert_operator_log(
            &transaction,
            project,
            "cancel",
            &json!({
                "task_id": task_id,
                "task_signature": bounded_redacted_text(task_signature),
                "requested_state": bounded_redacted_text(requested_state),
                "action": bounded_redacted_text(action),
                "final_state": bounded_redacted_text(final_state),
                "reason": bounded_redacted_text(reason),
            }),
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit task cancellation operator log"))
    }

    pub fn find_by_root(&self, root: &std::path::Path) -> Result<Option<Project>, AppError> {
        let canonical_root = match fs::canonicalize(root) {
            Ok(path) => path,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(AppError::Io {
                    operation: "canonicalize project root for lookup",
                    source,
                })
            }
        };
        let root_path = path_text(&canonical_root, "root_path")?;
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE root_path = ?1",
                [root_path],
                project_from_row,
            )
            .optional()
            .map_err(database_error("find project by canonical root"))
    }
}

fn insert_operator_log(
    transaction: &Transaction<'_>,
    project: &Project,
    action: &'static str,
    details: &serde_json::Value,
    now: i64,
) -> Result<(), AppError> {
    let details_json =
        serde_json::to_string(details).map_err(|source| AppError::Serialization {
            operation: "serialize operator log details",
            source,
        })?;
    transaction
        .execute(
            "INSERT INTO operator_logs (
                project_id, pueue_group, action, details_json, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                project.project_id,
                project.pueue_group,
                action,
                details_json,
                now,
            ],
        )
        .map_err(database_error("insert operator log"))?;
    Ok(())
}

pub struct EventRepository<'db> {
    db: &'db Db,
}

/// The bounded execution facts attached to an event's most recent project-owned run.
///
/// This deliberately mirrors only persisted execution projection columns. It
/// never selects run input such as argv, environment, prompts, transcripts, or
/// log output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventExecutionProjection {
    pub run_id: i64,
    pub execution_kind: Option<String>,
    pub executable_path: Option<String>,
    pub executable_identity: Option<String>,
    pub policy_code: Option<String>,
    pub failure_stage: Option<String>,
}

/// Infer the only policy stage possible for a terminal policy-blocked event
/// that is proven not to have an agent-run link: scheduler admission rejected
/// it before binding.
pub fn inferred_pre_binding_policy_code(event: &Event, has_run_link: bool) -> Option<String> {
    if event.status != EventStatus::DeadLetter || has_run_link {
        return None;
    }
    let code = event.last_error.as_deref()?.strip_prefix("policy_blocked:")?;
    is_policy_violation_code(code).then(|| code.to_owned())
}

impl<'db> EventRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn insert_idempotent(&self, event: &NewEvent) -> Result<Event, AppError> {
        self.insert_idempotent_with_inserted(event)
            .map(|(event, _)| event)
    }

    /// Claim one known event directly (used by the health-diagnosis
    /// coordinator, which owns its events outside the scheduler claim loop).
    pub fn claim_by_id(
        &self,
        project_id: &str,
        event_id: i64,
        lease_until: i64,
    ) -> Result<Option<Event>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin direct event claim"))?;
        let changed = transaction
            .execute(
                "UPDATE events
                 SET status = 'claimed', lease_until = ?3, attempts = attempts + 1
                 WHERE project_id = ?1 AND event_id = ?2 AND status IN ('pending', 'retry_wait')",
                params![project_id, event_id, lease_until],
            )
            .map_err(database_error("claim event by id"))?;
        if changed != 1 {
            transaction
                .commit()
                .map_err(database_error("commit empty direct event claim"))?;
            return Ok(None);
        }
        let event = transaction
            .query_row(
                &format!("{EVENT_SELECT} WHERE project_id = ?1 AND event_id = ?2"),
                params![project_id, event_id],
                event_from_row,
            )
            .optional()
            .map_err(database_error("read claimed event"))?;
        transaction
            .commit()
            .map_err(database_error("commit direct event claim"))?;
        Ok(event)
    }

    pub fn insert_idempotent_with_inserted(
        &self,
        event: &NewEvent,
    ) -> Result<(Event, bool), AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin idempotent event insert"))?;
        let stored = insert_event_idempotent_in_transaction(&transaction, event)?;
        transaction
            .commit()
            .map_err(database_error("commit idempotent event insert"))?;
        Ok(stored)
    }

    pub fn insert_periodic_deep_check_if_due(
        &self,
        event: &NewEvent,
        oldest_running_task_started_at: Option<i64>,
        interval_seconds: i64,
    ) -> Result<bool, AppError> {
        let payload_json =
            serde_json::to_string(&event.payload).map_err(|source| AppError::Serialization {
                operation: "serialize periodic deep check payload",
                source,
            })?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin periodic deep check scheduling"))?;
        validate_event_lineage(&transaction, event)?;

        let has_open_event = transaction
            .query_row(
                "SELECT 1 FROM events
                 WHERE project_id = ?1
                   AND kind = 'deep_check'
                   AND dedup_key LIKE 'periodic-deep-check:v1:%'
                   AND status IN ('pending', 'claimed', 'retry_wait')
                 LIMIT 1",
                [event.project_id.as_str()],
                |_| Ok(()),
            )
            .optional()
            .map_err(database_error("recheck open periodic deep check"))?
            .is_some();
        if has_open_event {
            transaction
                .commit()
                .map_err(database_error("commit skipped periodic deep check"))?;
            return Ok(false);
        }

        let project_active = transaction
            .query_row(
                "SELECT enabled, paused, halted_reason
                 FROM projects
                 WHERE project_id = ?1",
                [event.project_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, bool>(0)?,
                        row.get::<_, bool>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(database_error("recheck project lifecycle for periodic deep check"))?;
        if !project_active.is_some_and(|(enabled, paused, halted_reason)| {
            enabled && !paused && halted_reason.is_none()
        }) {
            transaction
                .commit()
                .map_err(database_error("commit skipped inactive periodic deep check"))?;
            return Ok(false);
        }

        let has_active_agent = transaction
            .query_row(
                "SELECT 1 FROM agent_runs
                 WHERE project_id = ?1
                   AND status IN ('starting', 'running')
                 LIMIT 1",
                [event.project_id.as_str()],
                |_| Ok(()),
            )
            .optional()
            .map_err(database_error("recheck active agent run for periodic deep check"))?
            .is_some();
        if has_active_agent {
            transaction
                .commit()
                .map_err(database_error("commit skipped periodic deep check"))?;
            return Ok(false);
        }

        let last_scheduled_at = transaction
            .query_row(
                "SELECT created_at FROM events
                 WHERE project_id = ?1
                   AND kind = 'deep_check'
                   AND dedup_key LIKE 'periodic-deep-check:v1:%'
                 ORDER BY created_at DESC, event_id DESC
                 LIMIT 1",
                [event.project_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("recheck latest periodic deep check"))?;
        let anchor = last_scheduled_at
            .max(oldest_running_task_started_at)
            .unwrap_or(event.created_at);
        if event.created_at.saturating_sub(anchor) < interval_seconds {
            transaction
                .commit()
                .map_err(database_error("commit deferred periodic deep check"))?;
            return Ok(false);
        }

        let inserted = transaction
            .execute(
                "INSERT INTO events (
                    project_id, campaign_id, experiment_id, kind, dedup_key, payload_json, status, attempts,
                    not_before, lease_until, created_at, completed_at, last_error
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', 0, ?7, NULL, ?8, NULL, NULL)
                 ON CONFLICT(project_id, dedup_key) DO NOTHING",
                params![
                    event.project_id,
                    event.campaign_id,
                    event.experiment_id,
                    event.kind,
                    event.dedup_key,
                    payload_json,
                    event.not_before,
                    event.created_at,
                ],
            )
            .map_err(database_error("insert periodic deep check event"))?;
        transaction
            .commit()
            .map_err(database_error("commit periodic deep check scheduling"))?;
        Ok(inserted == 1)
    }

    pub fn find_by_id(&self, event_id: i64) -> Result<Option<Event>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{} WHERE event_id = ?1", EVENT_SELECT),
                [event_id],
                event_from_row,
            )
            .optional()
            .map_err(database_error("find event by ID"))
    }

    pub fn find_by_dedup_key(
        &self,
        project_id: &str,
        dedup_key: &str,
    ) -> Result<Option<Event>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{} WHERE project_id = ?1 AND dedup_key = ?2", EVENT_SELECT),
                params![project_id, dedup_key],
                event_from_row,
            )
            .optional()
            .map_err(database_error("find event by deduplication key"))
    }

    pub fn latest_periodic_deep_check_at(
        &self,
        project_id: &str,
    ) -> Result<Option<i64>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT created_at FROM events
                 WHERE project_id = ?1
                   AND kind = 'deep_check'
                   AND dedup_key LIKE 'periodic-deep-check:v1:%'
                 ORDER BY created_at DESC, event_id DESC
                 LIMIT 1",
                [project_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("find latest periodic deep check"))
    }

    pub fn has_open_periodic_deep_check(&self, project_id: &str) -> Result<bool, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT 1 FROM events
                 WHERE project_id = ?1
                   AND kind = 'deep_check'
                   AND dedup_key LIKE 'periodic-deep-check:v1:%'
                   AND status IN ('pending', 'claimed', 'retry_wait')
                 LIMIT 1",
                [project_id],
                |_| Ok(()),
            )
            .optional()
            .map(|row| row.is_some())
            .map_err(database_error("check open periodic deep check"))
    }

    pub fn find_terminal_by_pueue_task(
        &self,
        project_id: &str,
        pueue_group: &str,
        pueue_task_id: i64,
    ) -> Result<Option<Event>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1
                        AND kind IN ('task_finished', 'task_failed')
                        AND json_extract(payload_json, '$.group') = ?2
                        AND json_extract(payload_json, '$.task_id') = ?3
                     ORDER BY event_id DESC
                     LIMIT 1",
                    EVENT_SELECT
                ),
                params![project_id, pueue_group, pueue_task_id],
                event_from_row,
            )
            .optional()
            .map_err(database_error("find terminal event by Pueue task"))
    }

    pub fn replace_pending(
        &self,
        event_id: i64,
        event: &NewEvent,
    ) -> Result<Option<Event>, AppError> {
        let payload_json =
            serde_json::to_string(&event.payload).map_err(|source| AppError::Serialization {
                operation: "serialize replacement event payload",
                source,
            })?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin pending event replacement"))?;
        validate_event_lineage(&transaction, event)?;
        let changed = transaction
            .execute(
                "UPDATE events
                 SET campaign_id = ?1, experiment_id = ?2,
                     kind = ?3, dedup_key = ?4, payload_json = ?5,
                     not_before = ?6, created_at = ?7
                 WHERE event_id = ?8 AND status = 'pending'",
                params![
                    event.campaign_id,
                    event.experiment_id,
                    event.kind,
                    event.dedup_key,
                    payload_json,
                    event.not_before,
                    event.created_at,
                    event_id,
                ],
            )
            .map_err(database_error("replace pending event"))?;
        let stored = if changed == 0 {
            None
        } else {
            Some(
                transaction
                    .query_row(
                        &format!("{} WHERE event_id = ?1", EVENT_SELECT),
                        [event_id],
                        event_from_row,
                    )
                    .map_err(database_error("read replaced event"))?,
            )
        };
        transaction
            .commit()
            .map_err(database_error("commit pending event replacement"))?;
        Ok(stored)
    }

    pub fn replace_callback_with_terminal(
        &self,
        event_id: i64,
        event: &NewEvent,
    ) -> Result<Event, AppError> {
        let payload_json =
            serde_json::to_string(&event.payload).map_err(|source| AppError::Serialization {
                operation: "serialize terminal callback replacement payload",
                source,
            })?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin callback terminal replacement"))?;
        validate_event_lineage(&transaction, event)?;
        let changed = transaction
            .execute(
                "UPDATE events
                 SET campaign_id = ?1, experiment_id = ?2,
                     kind = ?3, dedup_key = ?4, payload_json = ?5,
                     not_before = ?6, created_at = ?7
                 WHERE event_id = ?8",
                params![
                    event.campaign_id,
                    event.experiment_id,
                    event.kind,
                    event.dedup_key,
                    payload_json,
                    event.not_before,
                    event.created_at,
                    event_id,
                ],
            )
            .map_err(database_error("replace callback with terminal event"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "replace callback with terminal event",
            });
        }
        transaction
            .execute(
                "UPDATE events
                 SET status = 'pending', attempts = 0, lease_until = NULL,
                     completed_at = NULL, last_error = NULL
                 WHERE event_id = ?1
                   AND status = 'completed'
                   AND last_error = 'campaign_lineage_missing'
                   AND campaign_id IS NOT NULL
                   AND experiment_id IS NOT NULL
                   AND NOT EXISTS (
                       SELECT 1 FROM agent_run_events
                       WHERE agent_run_events.project_id = events.project_id
                         AND agent_run_events.event_id = events.event_id
                   )",
                [event_id],
            )
            .map_err(database_error("requeue trusted terminal event"))?;
        let stored = transaction
            .query_row(
                &format!("{} WHERE event_id = ?1", EVENT_SELECT),
                [event_id],
                event_from_row,
            )
            .map_err(database_error("read terminal callback replacement"))?;
        transaction
            .commit()
            .map_err(database_error("commit callback terminal replacement"))?;
        Ok(stored)
    }

    pub fn discard_pending(&self, event_id: i64) -> Result<bool, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin pending callback cleanup"))?;
        let removed = transaction
            .execute(
                "DELETE FROM events WHERE event_id = ?1 AND status = 'pending'",
                [event_id],
            )
            .map_err(database_error("discard duplicate pending callback"))?;
        transaction
            .commit()
            .map_err(database_error("commit pending callback cleanup"))?;
        Ok(removed == 1)
    }

    pub fn claim_batch(
        &self,
        now: i64,
        lease_until: i64,
        limit: usize,
    ) -> Result<Vec<Event>, AppError> {
        self.claim_batch_excluding_projects(now, lease_until, limit, &BTreeSet::new())
    }

    pub fn claim_batch_excluding_projects(
        &self,
        now: i64,
        lease_until: i64,
        limit: usize,
        blocked_project_ids: &BTreeSet<String>,
    ) -> Result<Vec<Event>, AppError> {
        if lease_until <= now {
            return Err(AppError::Configuration {
                field: "event_lease",
            });
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        if limit > MAX_EVENT_LIST_LIMIT {
            return Err(AppError::Configuration {
                field: "event_claim_limit",
            });
        }
        let mut connection = self.db.connect()?;
        Self::claim_batch_excluding_projects_in_connection(
            &mut connection,
            now,
            lease_until,
            limit,
            blocked_project_ids,
        )
    }

    fn claim_batch_excluding_projects_in_connection(
        connection: &mut Connection,
        now: i64,
        lease_until: i64,
        limit: usize,
        blocked_project_ids: &BTreeSet<String>,
    ) -> Result<Vec<Event>, AppError> {
        let claim_limit = i64::try_from(limit).map_err(|_| AppError::Configuration {
            field: "event_claim_limit",
        })?;
        let work_limit = i64::try_from(EVENT_CLAIM_WORK_LIMIT).unwrap_or(i64::MAX);
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin immediate event claim"))?;
        let mut event_ids = Vec::with_capacity(limit);
        for _ in 0..EVENT_CLAIM_PROBE_PASSES {
            if i64::try_from(event_ids.len()).unwrap_or(i64::MAX) >= claim_limit {
                break;
            }
            let mut candidates = Vec::with_capacity(EVENT_CLAIM_WORK_LIMIT * 2);
            for status in [EventStatus::Pending, EventStatus::RetryWait] {
                let mut statement = transaction
                    .prepare(&event_claim_candidate_sql(status))
                    .map_err(database_error("prepare event claim"))?;
                candidates.extend(
                    statement
                        .query_map(params![now, work_limit], |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, i64>(2)?,
                                row.get::<_, i64>(3)?,
                                status,
                            ))
                        })
                        .map_err(database_error("query claimable events"))?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(database_error("read claimable events"))?,
                );
            }
            candidates.sort_by_key(|(event_id, _, not_before, created_at, _)| {
                (*not_before, *created_at, *event_id)
            });
            candidates.truncate(EVENT_CLAIM_WORK_LIMIT);
            if candidates.is_empty() {
                break;
            }
            for (event_id, project_id, _, _, expected_status) in candidates {
                if blocked_project_ids.contains(&project_id) {
                    let rotated = transaction
                        .execute(
                            "UPDATE events
                             SET status = 'retry_wait', not_before = ?1, lease_until = NULL
                             WHERE project_id = ?2 AND event_id = ?3 AND status = ?4
                               AND not_before <= ?5",
                            params![lease_until, project_id, event_id, expected_status, now],
                        )
                        .map_err(database_error("rotate blocked event claim candidate"))?;
                    if rotated != 1 {
                        return Err(AppError::Runtime {
                            operation: "rotate blocked event claim candidate",
                        });
                    }
                } else if event_ids.len() < limit {
                    let claimed = transaction
                        .execute(
                            "UPDATE events
                             SET status = 'claimed', lease_until = ?1,
                                 attempts = attempts + 1
                             WHERE project_id = ?2 AND event_id = ?3 AND status = ?4
                               AND not_before <= ?5",
                            params![lease_until, project_id, event_id, expected_status, now],
                        )
                        .map_err(database_error("claim event"))?;
                    if claimed != 1 {
                        return Err(AppError::Runtime {
                            operation: "claim event",
                        });
                    }
                    event_ids.push(event_id);
                }
            }
        }

        let mut events = Vec::with_capacity(event_ids.len());
        for event_id in event_ids {
            events.push(
                transaction
                    .query_row(
                        &format!("{} WHERE event_id = ?1", EVENT_SELECT),
                        [event_id],
                        event_from_row,
                    )
                    .map_err(database_error("read claimed event"))?,
            );
        }
        transaction
            .commit()
            .map_err(database_error("commit event claim"))?;
        Ok(events)
    }

    pub fn recover_expired_claims(&self, now: i64) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin expired claim recovery"))?;
        let recovered = transaction
            .execute(
                "UPDATE events
                 SET status = 'pending', lease_until = NULL,
                     attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END
                 WHERE status = 'claimed' AND lease_until <= ?1
                   AND NOT EXISTS (
                       SELECT 1 FROM agent_run_events
                       WHERE agent_run_events.project_id = events.project_id
                         AND agent_run_events.event_id = events.event_id
                   )",
                [now],
            )
            .map_err(database_error("recover expired event claims"))?;
        transaction
            .commit()
            .map_err(database_error("commit expired claim recovery"))?;
        Ok(recovered)
    }

    pub fn resolve_claimed_without_run(
        &self,
        project_id: &str,
        event_ids: &[i64],
        now: i64,
        reason: &str,
        policy: RetryPolicy,
    ) -> Result<usize, AppError> {
        if event_ids.is_empty() {
            return Ok(0);
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin unbound claimed event resolution"))?;
        let bounded_reason = bounded_redacted_text(reason);
        let mut seen_event_ids = BTreeSet::new();
        let mut claimed_events = Vec::with_capacity(event_ids.len());
        for event_id in event_ids {
            if !seen_event_ids.insert(*event_id) {
                return Err(AppError::Validation {
                    field: "event_ids",
                    message: "event IDs must be unique",
                });
            }
            let Some((event_project_id, status, attempts)) = transaction
                .query_row(
                    "SELECT project_id, status, attempts FROM events WHERE event_id = ?1",
                    [event_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, EventStatus>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(database_error("validate unbound claimed event"))?
            else {
                return Err(AppError::Validation {
                    field: "event_id",
                    message: "event does not exist",
                });
            };
            if event_project_id != project_id {
                return Err(AppError::Validation {
                    field: "project_id",
                    message: "event belongs to another project",
                });
            }
            if status != EventStatus::Claimed {
                return Err(AppError::Validation {
                    field: "event_status",
                    message: "event must be claimed without a run",
                });
            }
            let linked_to_run: bool = transaction
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM agent_run_events
                         WHERE project_id = ?1 AND event_id = ?2
                     )",
                    params![project_id, event_id],
                    |row| row.get(0),
                )
                .map_err(database_error("validate unbound claimed event link"))?;
            if linked_to_run {
                return Err(AppError::Validation {
                    field: "event_id",
                    message: "event must not already be linked to an agent run",
                });
            }
            claimed_events.push((*event_id, attempts));
        }

        let mut changed = 0;
        for (event_id, attempts) in claimed_events {
            let (status, not_before, completed_at) = match retry_decision(attempts, now, policy) {
                RetryDecision::Retry { not_before } => {
                    (EventStatus::RetryWait, Some(not_before), None)
                }
                RetryDecision::DeadLetter => (EventStatus::DeadLetter, None, Some(now)),
            };
            let event_changed = transaction
                .execute(
                    "UPDATE events
                     SET status = ?1, lease_until = NULL,
                         not_before = COALESCE(?2, not_before),
                         completed_at = ?3, last_error = ?4
                     WHERE project_id = ?5 AND event_id = ?6 AND status = 'claimed'",
                    params![
                        status,
                        not_before,
                        completed_at,
                        bounded_reason,
                        project_id,
                        event_id,
                    ],
                )
                .map_err(database_error("resolve unbound claimed event"))?;
            if event_changed != 1 {
                return Err(AppError::Runtime {
                    operation: "resolve unbound claimed event",
                });
            }
            changed += event_changed;
        }
        transaction
            .commit()
            .map_err(database_error("commit unbound claimed event resolution"))?;
        Ok(changed)
    }

    /// Atomically dead-letter a claimed batch when dispatch is blocked by a
    /// policy violation.  This path intentionally does not consult attempts
    /// or retry policy and cannot create an agent run.  Validate every event
    /// before changing any row so grouped claims never partially transition.
    pub fn dead_letter_claimed_without_run(
        &self,
        project_id: &str,
        event_ids: &[i64],
        now: i64,
        violation: &PolicyViolation,
    ) -> Result<usize, AppError> {
        if event_ids.is_empty() {
            return Ok(0);
        }
        if event_ids.len() > MAX_EVENT_LIST_LIMIT {
            return Err(AppError::Validation {
                field: "event_ids",
                message: "event ID list exceeds bounded limit",
            });
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin policy-blocked event resolution"))?;
        let mut seen_event_ids = BTreeSet::new();
        for event_id in event_ids {
            if !seen_event_ids.insert(*event_id) {
                return Err(AppError::Validation {
                    field: "event_ids",
                    message: "event IDs must be unique",
                });
            }
            let Some((event_project_id, status, lease_until)) = transaction
                .query_row(
                    "SELECT project_id, status, lease_until FROM events WHERE event_id = ?1",
                    [event_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, EventStatus>(1)?,
                            row.get::<_, Option<i64>>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(database_error("validate policy-blocked event"))?
            else {
                return Err(AppError::Validation {
                    field: "event_id",
                    message: "event does not exist",
                });
            };
            if event_project_id != project_id {
                return Err(AppError::Validation {
                    field: "project_id",
                    message: "event belongs to another project",
                });
            }
            if status != EventStatus::Claimed {
                return Err(AppError::Validation {
                    field: "event_status",
                    message: "event must be claimed without a run",
                });
            }
            if lease_until.is_none_or(|lease_until| lease_until <= now) {
                return Err(AppError::Validation {
                    field: "lease_until",
                    message: "event claim lease is expired or missing",
                });
            }
            let linked_to_run: bool = transaction
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM agent_run_events
                         WHERE project_id = ?1 AND event_id = ?2
                     )",
                    params![project_id, event_id],
                    |row| row.get(0),
                )
                .map_err(database_error("validate policy-blocked event link"))?;
            if linked_to_run {
                return Err(AppError::Validation {
                    field: "event_id",
                    message: "event must not already be linked to an agent run",
                });
            }
        }

        let bounded_error = format!("policy_blocked:{}", violation.code.as_str());
        let mut changed = 0;
        for event_id in event_ids {
            let event_changed = transaction
                .execute(
                    "UPDATE events
                     SET status = 'dead_letter', lease_until = NULL,
                         completed_at = ?1, last_error = ?2
                     WHERE project_id = ?3 AND event_id = ?4 AND status = 'claimed'
                       AND lease_until > ?5",
                    params![now, bounded_error, project_id, event_id, now],
                )
                .map_err(database_error("dead-letter policy-blocked event"))?;
            if event_changed != 1 {
                return Err(AppError::Runtime {
                    operation: "dead-letter policy-blocked event",
                });
            }
            changed += event_changed;
        }
        transaction
            .commit()
            .map_err(database_error("commit policy-blocked event resolution"))?;
        Ok(changed)
    }

    pub fn defer_claimed(&self, event_ids: &[i64]) -> Result<usize, AppError> {
        if event_ids.is_empty() {
            return Ok(0);
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin deferred event release"))?;
        let mut changed = 0;
        for event_id in event_ids {
            changed += transaction
                .execute(
                    "UPDATE events
                     SET status = 'retry_wait',
                         not_before = lease_until,
                         lease_until = NULL,
                         attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END
                     WHERE event_id = ?1 AND status = 'claimed'",
                    [event_id],
                )
                .map_err(database_error("defer claimed event"))?;
        }
        transaction
            .commit()
            .map_err(database_error("commit deferred event release"))?;
        Ok(changed)
    }

    pub fn transition_many(
        &self,
        event_ids: &[i64],
        status: EventStatus,
        now: i64,
        not_before: Option<i64>,
        last_error: Option<&str>,
    ) -> Result<usize, AppError> {
        if matches!(
            status,
            EventStatus::InFlight | EventStatus::Dispatched | EventStatus::DeadLetter
        ) {
            return Err(AppError::Validation {
                field: "event_status",
                message: "acknowledgement-owned event states require their repository finalizer",
            });
        }
        if event_ids.is_empty() {
            return Ok(0);
        }
        let bounded_last_error = last_error.map(bounded_redacted_text);
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin event status transition"))?;
        let mut changed = 0;
        for event_id in event_ids {
            changed += transaction
                .execute(
                    "UPDATE events
                     SET status = ?1,
                         lease_until = NULL,
                         not_before = COALESCE(?2, not_before),
                         attempts = CASE
                             WHEN status = 'claimed' AND ?1 = 'retry_wait'
                                  AND attempts > 0
                                 THEN attempts - 1
                             ELSE attempts
                         END,
                         completed_at = CASE WHEN ?1 IN ('completed', 'failed') THEN ?3 ELSE completed_at END,
                         last_error = ?4
                     WHERE event_id = ?5",
                    params![
                        status,
                        not_before,
                        now,
                        bounded_last_error.as_deref(),
                        event_id
                    ],
                )
                .map_err(database_error("transition event status"))?;
        }
        transaction
            .commit()
            .map_err(database_error("commit event status transition"))?;
        Ok(changed)
    }

    pub fn recent_events(&self, project_id: &str, limit: usize) -> Result<Vec<Event>, AppError> {
        let limit = i64::try_from(limit).map_err(|_| AppError::Configuration {
            field: "event_query_limit",
        })?;
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                 ORDER BY created_at DESC, event_id DESC
                 LIMIT ?2",
                EVENT_SELECT
            ))
            .map_err(database_error("prepare recent event query"))?;
        let rows = statement
            .query_map(params![project_id, limit], event_from_row)
            .map_err(database_error("query recent events"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read recent events"))
    }

    pub fn list_filtered(
        &self,
        project_id: &str,
        filter: &EventFilter,
    ) -> Result<Vec<Event>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                 AND (?2 IS NULL OR kind = ?2)
                 AND (?3 IS NULL OR status = ?3)
                 ORDER BY created_at DESC, event_id DESC
                 LIMIT ?4",
                EVENT_SELECT
            ))
            .map_err(database_error("prepare filtered event query"))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    filter.kind,
                    filter.status,
                    bounded_diagnostic_limit(filter.limit),
                ],
                event_from_row,
            )
            .map_err(database_error("query filtered events"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read filtered events"))
    }

    /// Return one deterministic latest run per requested event in one bounded query.
    pub fn latest_run_ids(
        &self,
        project_id: &str,
        event_ids: &[i64],
    ) -> Result<BTreeMap<i64, i64>, AppError> {
        Ok(self
            .latest_execution_projections(project_id, event_ids)?
            .into_iter()
            .map(|(event_id, projection)| (event_id, projection.run_id))
            .collect())
    }

    /// Return the newest execution projection for each supplied event.
    ///
    /// All joined relations carry the requested project predicate so a
    /// colliding foreign event or run cannot contribute diagnostics.
    pub fn latest_execution_projections(
        &self,
        project_id: &str,
        event_ids: &[i64],
    ) -> Result<BTreeMap<i64, EventExecutionProjection>, AppError> {
        let mut unique_event_ids = BTreeSet::new();
        for event_id in event_ids {
            if unique_event_ids.len() >= MAX_EVENT_LIST_LIMIT {
                break;
            }
            unique_event_ids.insert(*event_id);
        }
        let event_ids = unique_event_ids;
        if event_ids.is_empty() {
            return Ok(BTreeMap::new());
        }

        let placeholders = std::iter::repeat_n("?", event_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT event_id, run_id, execution_kind, executable_path,
                    executable_identity, policy_code, failure_stage
             FROM (
                 SELECT agent_run_events.event_id, agent_runs.run_id,
                        agent_runs.execution_kind, agent_runs.executable_path,
                        agent_runs.executable_identity, agent_runs.policy_code,
                        agent_runs.failure_stage,
                        ROW_NUMBER() OVER (
                            PARTITION BY agent_run_events.event_id
                            ORDER BY agent_runs.started_at DESC, agent_runs.run_id DESC
                        ) AS latest_rank
                 FROM agent_runs
                 JOIN agent_run_events
                   ON agent_run_events.project_id = agent_runs.project_id
                  AND agent_run_events.run_id = agent_runs.run_id
                 JOIN events
                   ON events.project_id = agent_run_events.project_id
                  AND events.event_id = agent_run_events.event_id
                 WHERE agent_runs.project_id = ?1
                   AND agent_run_events.project_id = ?1
                   AND events.project_id = ?1
                   AND agent_run_events.event_id IN ({placeholders})
             )
             WHERE latest_rank = 1
             ORDER BY event_id",
        );
        let mut query_values = Vec::with_capacity(event_ids.len() + 1);
        query_values.push(Value::Text(project_id.to_owned()));
        query_values.extend(event_ids.iter().copied().map(Value::Integer));

        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&query)
            .map_err(database_error("prepare latest event execution projection query"))?;
        let rows = statement
            .query_map(params_from_iter(query_values), |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    EventExecutionProjection {
                        run_id: row.get(1)?,
                        execution_kind: row.get(2)?,
                        executable_path: row.get(3)?,
                        executable_identity: row.get(4)?,
                        policy_code: row.get(5)?,
                        failure_stage: row.get(6)?,
                    },
                ))
            })
            .map_err(database_error("query latest event execution projections"))?;
        rows.collect::<Result<BTreeMap<_, _>, _>>()
            .map_err(database_error("read latest event execution projections"))
    }

    /// Count terminal policy blocks for this project by their bounded policy
    /// code. The scheduler writes these errors itself; arbitrary event errors
    /// do not enter the diagnostic projection.
    pub fn policy_blocked_counts(
        &self,
        project_id: &str,
    ) -> Result<BTreeMap<String, i64>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT substr(last_error, length('policy_blocked:') + 1), COUNT(*)
                 FROM events
                 WHERE project_id = ?1
                   AND status = 'dead_letter'
                   AND last_error IN (
                       'policy_blocked:policy_missing', 'policy_blocked:policy_unreadable',
                       'policy_blocked:policy_weak_permissions', 'policy_blocked:policy_unknown_field',
                       'policy_blocked:trusted_path_unsafe', 'policy_blocked:anchor_missing',
                       'policy_blocked:anchor_replaced', 'policy_blocked:custom_agent_not_enrolled',
                       'policy_blocked:project_root_executable', 'policy_blocked:unsafe_codex_argument',
                       'policy_blocked:network_override', 'policy_blocked:environment_name',
                       'policy_blocked:session_missing', 'policy_blocked:session_not_owned',
                       'policy_blocked:root_changed', 'policy_blocked:agent_log_unsafe',
                       'policy_blocked:temp_unsafe', 'policy_blocked:setsid_failed',
                       'policy_blocked:native_gate_failed', 'policy_blocked:unsupported_platform'
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM agent_run_events
                       WHERE agent_run_events.project_id = events.project_id
                         AND agent_run_events.event_id = events.event_id
                   )
                 GROUP BY substr(last_error, length('policy_blocked:') + 1)
                 ORDER BY substr(last_error, length('policy_blocked:') + 1)",
            )
            .map_err(database_error("prepare policy-blocked event count query"))?;
        let rows = statement
            .query_map([project_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(database_error("query policy-blocked event counts"))?;
        rows.collect::<Result<BTreeMap<_, _>, _>>()
            .map_err(database_error("read policy-blocked event counts"))
    }

    pub fn latest_run_id(
        &self,
        project_id: &str,
        event_id: i64,
    ) -> Result<Option<i64>, AppError> {
        Ok(self
            .latest_run_ids(project_id, &[event_id])?
            .get(&event_id)
            .copied())
    }

    pub fn find_by_task_signature(
        &self,
        project_id: &str,
        task_signature: &str,
        limit: usize,
    ) -> Result<Vec<Event>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                 AND json_extract(payload_json, '$.task_signature') = ?2
                 ORDER BY created_at DESC, event_id DESC
                 LIMIT ?3",
                EVENT_SELECT
            ))
            .map_err(database_error("prepare task event query"))?;
        let rows = statement
            .query_map(
                params![project_id, task_signature, bounded_diagnostic_limit(limit)],
                event_from_row,
            )
            .map_err(database_error("query task events"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read task events"))
    }

    pub fn count_consecutive_failures(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<u32, AppError> {
        let mut count = 0;
        for event in self.recent_events(project_id, limit)? {
            match event.kind {
                EventKind::Crash
                | EventKind::TaskFailed
                | EventKind::Stalled
                | EventKind::AutoKilled
                | EventKind::TerminationFailed => count += 1,
                EventKind::TaskFinished | EventKind::DeepCheck | EventKind::OperatorWake => break,
                EventKind::CampaignDecision | EventKind::HealthDiagnosis | EventKind::CodeChange => {}
            }
        }
        Ok(count)
    }
}

pub struct IntegrationEventRepository<'db> {
    db: &'db Db,
}

impl<'db> IntegrationEventRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn insert_idempotent(
        &self,
        event: &NewIntegrationEvent,
    ) -> Result<IntegrationEvent, AppError> {
        let payload_json =
            serde_json::to_string(&event.payload).map_err(|source| AppError::Serialization {
                operation: "serialize integration event payload",
                source,
            })?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin idempotent integration event insert"))?;
        transaction
            .execute(
                "INSERT INTO integration_events (
                    kind, dedup_key, payload_json, created_at
                 ) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(dedup_key) DO NOTHING",
                params![event.kind, event.dedup_key, payload_json, event.created_at],
            )
            .map_err(database_error("insert integration event"))?;
        let stored = transaction
            .query_row(
                &format!("{} WHERE dedup_key = ?1", INTEGRATION_EVENT_SELECT),
                [&event.dedup_key],
                integration_event_from_row,
            )
            .map_err(database_error("read idempotent integration event"))?;
        transaction
            .commit()
            .map_err(database_error("commit idempotent integration event insert"))?;
        Ok(stored)
    }
}

pub struct IncidentRepository<'db> {
    db: &'db Db,
}

impl<'db> IncidentRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn upsert_active(&self, incident: &NewIncident) -> Result<IncidentUpdate, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin active incident upsert"))?;
        let latest_resolved = transaction
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1 AND kind = ?2 AND fingerprint = ?3
                     AND status = 'resolved' AND resolved_at IS NOT NULL
                     ORDER BY resolved_at DESC, incident_id DESC LIMIT 1",
                    INCIDENT_SELECT
                ),
                params![incident.project_id, incident.kind, incident.fingerprint],
                incident_from_row,
            )
            .optional()
            .map_err(database_error("read latest resolved incident"))?;

        let update = if let Some(resolved) = latest_resolved.filter(|resolved| {
            resolved
                .resolved_at
                .is_some_and(|at| incident.seen_at <= at)
        }) {
            IncidentUpdate {
                incident: resolved,
                transition: IncidentTransition::Unchanged,
            }
        } else {
            let active = transaction
                .query_row(
                    &format!(
                        "{} WHERE project_id = ?1 AND kind = ?2 AND fingerprint = ?3
                         AND status IN ('open', 'acknowledged')",
                        INCIDENT_SELECT
                    ),
                    params![incident.project_id, incident.kind, incident.fingerprint],
                    incident_from_row,
                )
                .optional()
                .map_err(database_error("read active incident"))?;
            if let Some(active) = active {
                let task_changed = incident
                    .task_key
                    .as_ref()
                    .is_some_and(|task_key| Some(task_key) != active.task_key.as_ref());
                transaction
                    .execute(
                        "UPDATE incidents
                         SET task_key = COALESCE(?1, task_key),
                             last_seen_at = MAX(last_seen_at, ?2)
                         WHERE incident_id = ?3",
                        params![incident.task_key, incident.seen_at, active.incident_id],
                    )
                    .map_err(database_error("update active incident"))?;
                let stored = read_incident(&transaction, active.incident_id)?;
                IncidentUpdate {
                    incident: stored,
                    transition: if task_changed {
                        IncidentTransition::Updated
                    } else {
                        IncidentTransition::Unchanged
                    },
                }
            } else {
                transaction
                    .execute(
                        "INSERT INTO incidents (
                            project_id, kind, task_key, fingerprint, status,
                            first_seen_at, last_seen_at, acknowledged_at, resolved_at
                         ) VALUES (?1, ?2, ?3, ?4, 'open', ?5, ?5, NULL, NULL)",
                        params![
                            incident.project_id,
                            incident.kind,
                            incident.task_key,
                            incident.fingerprint,
                            incident.seen_at,
                        ],
                    )
                    .map_err(database_error("insert active incident"))?;
                let stored = read_incident(&transaction, transaction.last_insert_rowid())?;
                IncidentUpdate {
                    incident: stored,
                    transition: IncidentTransition::Opened,
                }
            }
        };

        transaction
            .commit()
            .map_err(database_error("commit active incident upsert"))?;
        Ok(update)
    }

    pub fn resolve(
        &self,
        incident_id: i64,
        resolved_at: i64,
    ) -> Result<IncidentTransition, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin incident resolution"))?;
        let changed = transaction
            .execute(
                "UPDATE incidents
                 SET status = 'resolved', resolved_at = ?1,
                     last_seen_at = MAX(last_seen_at, ?1)
                 WHERE incident_id = ?2 AND status IN ('open', 'acknowledged')",
                params![resolved_at, incident_id],
            )
            .map_err(database_error("resolve incident"))?;
        if changed == 0 {
            return Err(AppError::Runtime {
                operation: "resolve an active incident",
            });
        }
        transaction
            .commit()
            .map_err(database_error("commit incident resolution"))?;
        Ok(IncidentTransition::Resolved)
    }

    pub fn find_by_id(&self, incident_id: i64) -> Result<Option<Incident>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{} WHERE incident_id = ?1", INCIDENT_SELECT),
                [incident_id],
                incident_from_row,
            )
            .optional()
            .map_err(database_error("find incident by ID"))
    }

    pub fn find_by_project_and_id(
        &self,
        project_id: &str,
        incident_id: i64,
    ) -> Result<Option<Incident>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1 AND incident_id = ?2",
                    INCIDENT_SELECT
                ),
                params![project_id, incident_id],
                incident_from_row,
            )
            .optional()
            .map_err(database_error("find project incident by ID"))
    }

    pub fn list_by_project(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<Vec<Incident>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                 ORDER BY last_seen_at DESC, incident_id DESC
                 LIMIT ?2",
                INCIDENT_SELECT
            ))
            .map_err(database_error("prepare project incident query"))?;
        let rows = statement
            .query_map(
                params![project_id, bounded_diagnostic_limit(limit)],
                incident_from_row,
            )
            .map_err(database_error("query project incidents"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read project incidents"))
    }

    pub fn find_by_task_key(
        &self,
        project_id: &str,
        task_key: &str,
        limit: usize,
    ) -> Result<Vec<Incident>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND task_key = ?2
                 ORDER BY last_seen_at DESC, incident_id DESC
                 LIMIT ?3",
                INCIDENT_SELECT
            ))
            .map_err(database_error("prepare task incident query"))?;
        let rows = statement
            .query_map(
                params![project_id, task_key, bounded_diagnostic_limit(limit)],
                incident_from_row,
            )
            .map_err(database_error("query task incidents"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read task incidents"))
    }
}

pub struct SubmissionRepository<'db> {
    db: &'db Db,
}

pub struct BatchRepository<'db> {
    db: &'db Db,
}

impl<'db> BatchRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn create_or_get(&self, request: &NewBatchRequest) -> Result<BatchRequest, AppError> {
        validate_new_batch(request)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin batch create or get"))?;
        let existing = transaction
            .query_row(
                "SELECT project_id, manifest_hash
                 FROM batch_requests WHERE request_id = ?1",
                [&request.request_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(database_error("find existing batch request"))?;

        if let Some((project_id, manifest_hash)) = existing {
            if project_id != request.project_id {
                return Err(AppError::Validation {
                    field: "project_id",
                    message: "request_id belongs to another project",
                });
            }
            if manifest_hash != request.manifest_hash {
                return Err(AppError::Validation {
                    field: "manifest_hash",
                    message: "request_id already has a different manifest",
                });
            }
            let stored = read_batch(&transaction, &request.project_id, &request.request_id)?
                .ok_or(AppError::Runtime {
                    operation: "read existing batch request",
                })?;
            transaction
                .commit()
                .map_err(database_error("commit existing batch request"))?;
            return Ok(stored);
        }

        transaction
            .execute(
                "INSERT INTO batch_requests (
                    request_id, project_id, manifest_hash, status,
                    lease_until, lease_token, created_at, updated_at, last_error
                 ) VALUES (?1, ?2, ?3, 'pending', NULL, NULL, ?4, ?4, NULL)",
                params![
                    request.request_id,
                    request.project_id,
                    request.manifest_hash,
                    request.created_at,
                ],
            )
            .map_err(database_error("insert batch request intent"))?;

        for job in &request.jobs {
            let argv_json =
                serde_json::to_string(&job.argv).map_err(|source| AppError::Serialization {
                    operation: "serialize batch job arguments",
                    source,
                })?;
            let metadata_json =
                serde_json::to_string(&job.metadata).map_err(|source| AppError::Serialization {
                    operation: "serialize batch job metadata",
                    source,
                })?;
            debug_assert!(argv_json.len() <= MAX_BATCH_ARGV_JSON_BYTES);
            debug_assert!(metadata_json.len() <= MAX_BATCH_METADATA_JSON_BYTES);
            transaction
                .execute(
                    "INSERT INTO batch_jobs (
                        request_id, job_id, ordinal, kind, argv_json, metadata_json,
                        status, pueue_task_id, submission_id, last_error
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', NULL, NULL, NULL)",
                    params![
                        request.request_id,
                        job.job_id,
                        job.ordinal,
                        job.kind,
                        argv_json,
                        metadata_json,
                    ],
                )
                .map_err(database_error("insert batch job intent"))?;
        }

        let stored = read_batch(&transaction, &request.project_id, &request.request_id)?.ok_or(
            AppError::Runtime {
                operation: "read created batch request",
            },
        )?;
        transaction
            .commit()
            .map_err(database_error("commit batch request intent"))?;
        Ok(stored)
    }

    pub fn find(
        &self,
        project_id: &str,
        request_id: &str,
    ) -> Result<Option<BatchRequest>, AppError> {
        let connection = self.db.connect()?;
        read_batch(&connection, project_id, request_id)
    }

    pub fn claim(
        &self,
        project_id: &str,
        request_id: &str,
        now: i64,
        lease_until: i64,
    ) -> Result<Option<BatchRequest>, AppError> {
        if lease_until <= now {
            return Err(AppError::Validation {
                field: "lease_until",
                message: "must be later than now",
            });
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin batch claim"))?;
        let lease_token = Uuid::new_v4().to_string();
        let changed = transaction
            .execute(
                "UPDATE batch_requests
                 SET status = 'dispatching', lease_until = ?1,
                     lease_token = ?2, updated_at = ?3
                 WHERE request_id = ?4 AND project_id = ?5
                   AND lease_until IS NULL
                   AND status IN ('pending', 'accepted', 'partial')
                   AND EXISTS(
                       SELECT 1 FROM batch_jobs
                       WHERE batch_jobs.request_id = batch_requests.request_id
                         AND batch_jobs.status = 'pending'
                   )",
                params![lease_until, lease_token, now, request_id, project_id],
            )
            .map_err(database_error("claim batch request"))?;
        if changed == 0 {
            transaction
                .commit()
                .map_err(database_error("commit empty batch claim"))?;
            return Ok(None);
        }
        transaction
            .execute(
                "UPDATE batch_jobs SET status = 'dispatching'
                 WHERE request_id = ?1 AND status = 'pending'",
                [request_id],
            )
            .map_err(database_error("claim batch jobs"))?;
        let claimed =
            read_batch(&transaction, project_id, request_id)?.ok_or(AppError::Runtime {
                operation: "read claimed batch request",
            })?;
        transaction
            .commit()
            .map_err(database_error("commit batch claim"))?;
        Ok(Some(claimed))
    }

    pub fn record_job_result(
        &self,
        project_id: &str,
        request_id: &str,
        job_id: &str,
        lease_token: &str,
        result: BatchJobResult,
        now: i64,
    ) -> Result<BatchRequest, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin batch job result"))?;
        let parent = transaction
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1 AND request_id = ?2",
                    BATCH_REQUEST_SELECT
                ),
                params![project_id, request_id],
                batch_request_from_row,
            )
            .optional()
            .map_err(database_error("find batch for job result"))?
            .ok_or(AppError::Validation {
                field: "project_id",
                message: "batch request is not owned by this project",
            })?;
        let current = transaction
            .query_row(
                "SELECT status, pueue_task_id, submission_id, last_error
                 FROM batch_jobs WHERE request_id = ?1 AND job_id = ?2",
                params![request_id, job_id],
                |row| {
                    Ok((
                        row.get::<_, BatchJobStatus>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(database_error("find batch job for result"))?
            .ok_or(AppError::Validation {
                field: "job_id",
                message: "job does not belong to this batch",
            })?;
        let active_lease = matches!(
            parent.status,
            BatchStatus::Dispatching | BatchStatus::Accepted
        ) && parent.lease_until.is_some_and(|lease| lease > now)
            && parent.lease_token.as_deref() == Some(lease_token);

        match result {
            BatchJobResult::Accepted {
                pueue_task_id,
                submission_id,
            } => {
                validate_accepted_result(pueue_task_id, &submission_id)?;
                if current.0 == BatchJobStatus::Accepted {
                    if current.1 == Some(pueue_task_id)
                        && current.2.as_deref() == Some(submission_id.as_str())
                    {
                        let stored = read_batch(&transaction, project_id, request_id)?.ok_or(
                            AppError::Runtime {
                                operation: "read idempotent batch job result",
                            },
                        )?;
                        transaction
                            .commit()
                            .map_err(database_error("commit idempotent batch job result"))?;
                        return Ok(stored);
                    }
                    return Err(AppError::Validation {
                        field: "pueue_task_id",
                        message: "accepted job already has a different external result",
                    });
                }
                if current.0 != BatchJobStatus::Dispatching {
                    return Err(AppError::Validation {
                        field: "job_id",
                        message: "job is not dispatching",
                    });
                }
                if !active_lease {
                    return Err(AppError::Validation {
                        field: "lease_token",
                        message: "batch lease is stale or not owned by this worker",
                    });
                }
                transaction
                    .execute(
                        "UPDATE batch_jobs
                         SET status = 'accepted', pueue_task_id = ?1,
                             submission_id = ?2, last_error = NULL
                         WHERE request_id = ?3 AND job_id = ?4
                           AND EXISTS(
                               SELECT 1 FROM batch_requests
                               WHERE batch_requests.request_id = batch_jobs.request_id
                                 AND batch_requests.project_id = ?5
                                 AND batch_requests.lease_token = ?6
                                 AND batch_requests.lease_until > ?7
                                 AND batch_requests.status IN ('dispatching', 'accepted')
                           )",
                        params![
                            pueue_task_id,
                            submission_id,
                            request_id,
                            job_id,
                            project_id,
                            lease_token,
                            now,
                        ],
                    )
                    .map_err(database_error("record accepted batch job"))?;
                if transaction.changes() != 1 {
                    return Err(AppError::Validation {
                        field: "lease_token",
                        message: "batch lease is stale or not owned by this worker",
                    });
                }
                let jobs = read_batch(&transaction, project_id, request_id)?
                    .ok_or(AppError::Runtime {
                        operation: "read batch jobs after acceptance",
                    })?
                    .jobs;
                let mut status = derive_request_status(&jobs);
                if status == BatchStatus::Partial
                    && jobs.iter().any(|job| {
                        matches!(
                            job.status,
                            BatchJobStatus::Pending | BatchJobStatus::Dispatching
                        )
                    })
                {
                    status = BatchStatus::Accepted;
                }
                let lease = (matches!(status, BatchStatus::Accepted | BatchStatus::Dispatching))
                    .then_some(parent.lease_until.unwrap());
                let token = lease.is_some().then_some(lease_token);
                transaction
                    .execute(
                        "UPDATE batch_requests SET status = ?1, lease_until = ?2,
                         lease_token = ?3, updated_at = ?4, last_error = NULL
                         WHERE request_id = ?5 AND project_id = ?6
                           AND lease_token = ?7 AND lease_until > ?8",
                        params![
                            status,
                            lease,
                            token,
                            now,
                            request_id,
                            project_id,
                            lease_token,
                            now
                        ],
                    )
                    .map_err(database_error("update batch status after acceptance"))?;
            }
            BatchJobResult::Failed { error } => {
                validate_error(&error)?;
                if current.0 == BatchJobStatus::Accepted {
                    return Err(AppError::Validation {
                        field: "job_id",
                        message: "accepted job cannot be marked failed",
                    });
                }
                if current.0 == BatchJobStatus::Failed {
                    if current.3.as_deref() == Some(error.as_str()) {
                        let stored = read_batch(&transaction, project_id, request_id)?.ok_or(
                            AppError::Runtime {
                                operation: "read idempotent failed batch job result",
                            },
                        )?;
                        transaction
                            .commit()
                            .map_err(database_error("commit idempotent failed batch result"))?;
                        return Ok(stored);
                    }
                    return Err(AppError::Validation {
                        field: "last_error",
                        message: "failed job already has a different error",
                    });
                }
                if current.0 != BatchJobStatus::Dispatching {
                    return Err(AppError::Validation {
                        field: "job_id",
                        message: "job is not dispatching",
                    });
                }
                if !active_lease {
                    return Err(AppError::Validation {
                        field: "lease_token",
                        message: "batch lease is stale or not owned by this worker",
                    });
                }
                transaction
                    .execute(
                        "UPDATE batch_jobs
                         SET status = 'failed', last_error = ?1
                         WHERE request_id = ?2 AND job_id = ?3
                           AND EXISTS(
                               SELECT 1 FROM batch_requests
                               WHERE batch_requests.request_id = batch_jobs.request_id
                                 AND batch_requests.project_id = ?4
                                 AND batch_requests.lease_token = ?5
                                 AND batch_requests.lease_until > ?6
                                 AND batch_requests.status IN ('dispatching', 'accepted')
                           )",
                        params![error, request_id, job_id, project_id, lease_token, now],
                    )
                    .map_err(database_error("record failed batch job"))?;
                if transaction.changes() != 1 {
                    return Err(AppError::Validation {
                        field: "lease_token",
                        message: "batch lease is stale or not owned by this worker",
                    });
                }
                transaction
                    .execute(
                        "UPDATE batch_jobs
                         SET status = 'pending', pueue_task_id = NULL,
                             submission_id = NULL, last_error = NULL
                         WHERE request_id = ?1 AND status = 'dispatching'",
                        [request_id],
                    )
                    .map_err(database_error("reset unsubmitted batch jobs"))?;
                let jobs = read_batch(&transaction, project_id, request_id)?
                    .ok_or(AppError::Runtime {
                        operation: "read batch jobs after failure",
                    })?
                    .jobs;
                let status = derive_request_status(&jobs);
                transaction
                    .execute(
                        "UPDATE batch_requests SET status = ?1, lease_until = NULL,
                         lease_token = NULL, updated_at = ?2, last_error = ?3
                         WHERE request_id = ?4 AND project_id = ?5",
                        params![status, now, error, request_id, project_id],
                    )
                    .map_err(database_error("update batch status after failure"))?;
            }
        }

        let stored =
            read_batch(&transaction, project_id, request_id)?.ok_or(AppError::Runtime {
                operation: "read updated batch request",
            })?;
        transaction
            .commit()
            .map_err(database_error("commit batch job result"))?;
        Ok(stored)
    }

    pub fn recover_expired(
        &self,
        project_id: &str,
        now: i64,
    ) -> Result<Vec<BatchRequest>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin batch lease recovery"))?;
        let mut statement = transaction
            .prepare(
                "SELECT request_id FROM batch_requests
                 WHERE project_id = ?1 AND lease_until IS NOT NULL AND lease_until <= ?2
                   AND status IN ('dispatching', 'accepted')
                 ORDER BY request_id",
            )
            .map_err(database_error("prepare expired batch query"))?;
        let request_ids = statement
            .query_map(params![project_id, now], |row| row.get::<_, String>(0))
            .map_err(database_error("query expired batch requests"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read expired batch requests"))?;
        drop(statement);

        let mut recovered = Vec::with_capacity(request_ids.len());
        for request_id in request_ids {
            transaction
                .execute(
                    "UPDATE batch_jobs SET status = 'pending'
                     WHERE request_id = ?1 AND status = 'dispatching'",
                    [&request_id],
                )
                .map_err(database_error("recover unaccepted batch jobs"))?;
            let jobs = read_batch(&transaction, project_id, &request_id)?
                .ok_or(AppError::Runtime {
                    operation: "read recovered batch jobs",
                })?
                .jobs;
            let status = derive_request_status(&jobs);
            transaction
                .execute(
                    "UPDATE batch_requests SET status = ?1, lease_until = NULL,
                     lease_token = NULL, updated_at = ?2
                     WHERE request_id = ?3 AND project_id = ?4",
                    params![status, now, request_id, project_id],
                )
                .map_err(database_error("clear expired batch lease"))?;
            recovered.push(read_batch(&transaction, project_id, &request_id)?.ok_or(
                AppError::Runtime {
                    operation: "read recovered batch request",
                },
            )?);
        }
        transaction
            .commit()
            .map_err(database_error("commit batch lease recovery"))?;
        Ok(recovered)
    }
}

impl<'db> SubmissionRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn insert_idempotent(&self, submission: &NewSubmission) -> Result<Submission, AppError> {
        let argv_json =
            serde_json::to_string(&submission.argv).map_err(|source| AppError::Serialization {
                operation: "serialize submission arguments",
                source,
            })?;
        if !submission.metadata.is_object() {
            return Err(AppError::Message {
                message: "submission metadata must be a JSON object".to_owned(),
            });
        }
        let metadata_json = serde_json::to_string(&submission.metadata).map_err(|source| {
            AppError::Serialization {
                operation: "serialize submission metadata",
                source,
            }
        })?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin idempotent submission insert"))?;
        validate_submission_origin_agent_run(&transaction, submission)?;
        transaction
            .execute(
                "INSERT INTO submissions (
                    submission_id, project_id, argv_json, created_at,
                    pueue_task_id, task_signature, status, kind, metadata_json, origin_agent_run_id
                 ) VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5, ?6, ?7, ?8)
                 ON CONFLICT(submission_id) DO NOTHING",
                params![
                    submission.submission_id,
                    submission.project_id,
                    argv_json,
                    submission.created_at,
                    submission.status,
                    submission.kind,
                    metadata_json,
                    submission.origin_agent_run_id,
                ],
            )
            .map_err(database_error("insert submission"))?;
        let stored = read_submission(&transaction, &submission.submission_id)?;
        transaction
            .commit()
            .map_err(database_error("commit idempotent submission insert"))?;
        Ok(stored)
    }

    pub fn find_by_id(&self, submission_id: &str) -> Result<Option<Submission>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{} WHERE submission_id = ?1", SUBMISSION_SELECT),
                [submission_id],
                submission_from_row,
            )
            .optional()
            .map_err(database_error("find submission by ID"))
    }

    pub fn list_by_project(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                 ORDER BY created_at DESC, submission_id DESC
                 LIMIT ?2",
                SUBMISSION_SELECT
            ))
            .map_err(database_error("prepare project submission query"))?;
        let rows = statement
            .query_map(
                params![project_id, bounded_diagnostic_limit(limit)],
                submission_from_row,
            )
            .map_err(database_error("query project submissions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read project submissions"))
    }

    pub fn list_originless_by_project_page_after(
        &self,
        project_id: &str,
        limit: usize,
        after: Option<&SubmissionPageCursor>,
    ) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        if let Some(after) = after {
            let mut statement = connection
                .prepare(&format!(
                    "{} WHERE project_id = ?1 AND origin_agent_run_id IS NULL
                     AND (created_at < ?2
                          OR (created_at = ?2 AND submission_id < ?3))
                     ORDER BY created_at DESC, submission_id DESC
                     LIMIT ?4",
                    SUBMISSION_SELECT
                ))
                .map_err(database_error("prepare paged originless submission query"))?;
            let rows = statement
                .query_map(
                    params![
                        project_id,
                        after.created_at,
                        after.submission_id,
                        bounded_diagnostic_limit(limit),
                    ],
                    submission_from_row,
                )
                .map_err(database_error("query paged originless submissions"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read paged originless submissions"))
        } else {
            let mut statement = connection
                .prepare(&format!(
                    "{} WHERE project_id = ?1 AND origin_agent_run_id IS NULL
                     ORDER BY created_at DESC, submission_id DESC
                     LIMIT ?2",
                    SUBMISSION_SELECT
                ))
                .map_err(database_error("prepare originless submission query"))?;
            let rows = statement
                .query_map(
                    params![project_id, bounded_diagnostic_limit(limit)],
                    submission_from_row,
                )
                .map_err(database_error("query originless submissions"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read originless submissions"))
        }
    }

    pub fn list_originless_by_project_page_since(
        &self,
        project_id: &str,
        limit: usize,
        since: &SubmissionPageCursor,
    ) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND origin_agent_run_id IS NULL
                 AND (created_at > ?2
                      OR (created_at = ?2 AND submission_id > ?3))
                 ORDER BY created_at ASC, submission_id ASC
                 LIMIT ?4",
                SUBMISSION_SELECT
            ))
            .map_err(database_error("prepare new originless submission query"))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    since.created_at,
                    since.submission_id,
                    bounded_diagnostic_limit(limit),
                ],
                submission_from_row,
            )
            .map_err(database_error("query new originless submissions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read new originless submissions"))
    }

    pub fn list_originless_by_project_page_at(
        &self,
        project_id: &str,
        at: &SubmissionPageCursor,
    ) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND origin_agent_run_id IS NULL
                 AND created_at = ?2 AND submission_id = ?3
                 LIMIT 1",
                SUBMISSION_SELECT
            ))
            .map_err(database_error(
                "prepare boundary originless submission query",
            ))?;
        let rows = statement
            .query_map(
                params![project_id, at.created_at, at.submission_id],
                submission_from_row,
            )
            .map_err(database_error("query boundary originless submissions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read boundary originless submissions"))
    }

    pub fn find_by_task_signature(
        &self,
        project_id: &str,
        task_signature: &str,
        limit: usize,
    ) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND task_signature = ?2
                 ORDER BY created_at DESC, submission_id DESC
                 LIMIT ?3",
                SUBMISSION_SELECT
            ))
            .map_err(database_error("prepare task submission query"))?;
        let rows = statement
            .query_map(
                params![project_id, task_signature, bounded_diagnostic_limit(limit)],
                submission_from_row,
            )
            .map_err(database_error("query task submissions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read task submissions"))
    }

    pub fn list_by_origin_agent_run(
        &self,
        project_id: &str,
        origin_agent_run_id: i64,
        limit: usize,
    ) -> Result<Vec<Submission>, AppError> {
        self.list_by_origin_agent_run_page_after(project_id, origin_agent_run_id, limit, None)
    }

    pub fn list_by_origin_agent_run_page_after(
        &self,
        project_id: &str,
        origin_agent_run_id: i64,
        limit: usize,
        after: Option<&SubmissionPageCursor>,
    ) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        if let Some(after) = after {
            let mut statement = connection
                .prepare(&format!(
                    "{} WHERE project_id = ?1 AND origin_agent_run_id = ?2
                     AND (created_at < ?3
                          OR (created_at = ?3 AND submission_id < ?4))
                     ORDER BY created_at DESC, submission_id DESC
                     LIMIT ?5",
                    SUBMISSION_SELECT
                ))
                .map_err(database_error(
                    "prepare paged origin agent run submission query",
                ))?;
            let rows = statement
                .query_map(
                    params![
                        project_id,
                        origin_agent_run_id,
                        after.created_at,
                        after.submission_id,
                        bounded_diagnostic_limit(limit),
                    ],
                    submission_from_row,
                )
                .map_err(database_error("query paged origin agent run submissions"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read paged origin agent run submissions"))
        } else {
            let mut statement = connection
                .prepare(&format!(
                    "{} WHERE project_id = ?1 AND origin_agent_run_id = ?2
                     ORDER BY created_at DESC, submission_id DESC
                     LIMIT ?3",
                    SUBMISSION_SELECT
                ))
                .map_err(database_error("prepare origin agent run submission query"))?;
            let rows = statement
                .query_map(
                    params![
                        project_id,
                        origin_agent_run_id,
                        bounded_diagnostic_limit(limit),
                    ],
                    submission_from_row,
                )
                .map_err(database_error("query origin agent run submissions"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read origin agent run submissions"))
        }
    }

    pub fn list_by_origin_agent_run_page_since(
        &self,
        project_id: &str,
        origin_agent_run_id: i64,
        limit: usize,
        since: Option<&SubmissionPageCursor>,
    ) -> Result<Vec<Submission>, AppError> {
        let Some(since) = since else {
            return Ok(Vec::new());
        };
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND origin_agent_run_id = ?2
                 AND (created_at > ?3
                      OR (created_at = ?3 AND submission_id > ?4))
                 ORDER BY created_at ASC, submission_id ASC
                 LIMIT ?5",
                SUBMISSION_SELECT
            ))
            .map_err(database_error(
                "prepare new origin agent run submission query",
            ))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    origin_agent_run_id,
                    since.created_at,
                    since.submission_id,
                    bounded_diagnostic_limit(limit),
                ],
                submission_from_row,
            )
            .map_err(database_error("query new origin agent run submissions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read new origin agent run submissions"))
    }

    pub fn list_by_origin_agent_run_page_at(
        &self,
        project_id: &str,
        origin_agent_run_id: i64,
        at: &SubmissionPageCursor,
    ) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND origin_agent_run_id = ?2
                 AND created_at = ?3 AND submission_id = ?4
                 LIMIT 1",
                SUBMISSION_SELECT
            ))
            .map_err(database_error(
                "prepare boundary origin agent run submission query",
            ))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    origin_agent_run_id,
                    at.created_at,
                    at.submission_id,
                ],
                submission_from_row,
            )
            .map_err(database_error(
                "query boundary origin agent run submissions",
            ))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read boundary origin agent run submissions"))
    }

    pub fn mark_accepted(
        &self,
        submission_id: &str,
        pueue_task_id: i64,
        task_signature: &str,
    ) -> Result<Submission, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin submission acceptance"))?;
        transaction
            .execute(
                "UPDATE submissions
                 SET pueue_task_id = ?1, task_signature = ?2, status = ?3
                 WHERE submission_id = ?4",
                params![
                    pueue_task_id,
                    task_signature,
                    SubmissionStatus::Accepted,
                    submission_id,
                ],
            )
            .map_err(database_error("mark submission accepted"))?;
        let stored = read_submission(&transaction, submission_id)?;
        transaction
            .commit()
            .map_err(database_error("commit submission acceptance"))?;
        Ok(stored)
    }

    pub fn adopt(
        &self,
        submission_id: &str,
        pueue_task_id: i64,
        task_signature: &str,
    ) -> Result<Submission, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin submission adoption"))?;
        transaction
            .execute(
                "UPDATE submissions
                 SET pueue_task_id = ?1, task_signature = ?2, status = ?3
                 WHERE submission_id = ?4",
                params![
                    pueue_task_id,
                    task_signature,
                    SubmissionStatus::Adopted,
                    submission_id,
                ],
            )
            .map_err(database_error("adopt Pueue task for submission"))?;
        let stored = read_submission(&transaction, submission_id)?;
        transaction
            .commit()
            .map_err(database_error("commit submission adoption"))?;
        Ok(stored)
    }

    pub fn transition_status(
        &self,
        submission_id: &str,
        status: SubmissionStatus,
    ) -> Result<Submission, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin submission status transition"))?;
        transaction
            .execute(
                "UPDATE submissions SET status = ?1 WHERE submission_id = ?2",
                params![status, submission_id],
            )
            .map_err(database_error("transition submission status"))?;
        let stored = read_submission(&transaction, submission_id)?;
        transaction
            .commit()
            .map_err(database_error("commit submission status transition"))?;
        Ok(stored)
    }

    pub fn find_unreconciled(&self, project_id: &str) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                    AND (
                        status IN (?2, ?3)
                        OR (
                            pueue_task_id IS NULL
                            AND status IN (?4, ?5)
                        )
                    )
                 ORDER BY created_at, submission_id",
                SUBMISSION_SELECT
            ))
            .map_err(database_error("prepare unreconciled submission query"))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    SubmissionStatus::Pending,
                    SubmissionStatus::Unreconciled,
                    SubmissionStatus::Accepted,
                    SubmissionStatus::Adopted,
                ],
                submission_from_row,
            )
            .map_err(database_error("find unreconciled submissions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read unreconciled submissions"))
    }

    pub fn count_started_or_accepted(&self, project_id: &str) -> Result<u32, AppError> {
        let connection = self.db.connect()?;
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM submissions
                 WHERE project_id = ?1
                   AND kind = 'experiment'
                   AND (
                       pueue_task_id IS NOT NULL
                       OR status IN ('accepted', 'adopted', 'unreconciled')
                   )",
                [project_id],
                |row| row.get(0),
            )
            .map_err(database_error("count accepted submissions"))?;
        u32::try_from(count).map_err(|_| AppError::Runtime {
            operation: "count accepted submissions",
        })
    }
}

fn validate_submission_origin_agent_run(
    transaction: &Transaction<'_>,
    submission: &NewSubmission,
) -> Result<(), AppError> {
    let Some(origin_agent_run_id) = submission.origin_agent_run_id else {
        return Ok(());
    };
    let belongs_to_project: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM agent_runs WHERE run_id = ?1 AND project_id = ?2
             )",
            params![origin_agent_run_id, submission.project_id],
            |row| row.get(0),
        )
        .map_err(database_error("validate submission origin agent run"))?;
    if belongs_to_project {
        Ok(())
    } else {
        Err(AppError::Validation {
            field: "origin_agent_run_id",
            message: "must identify an agent run in the submission project",
        })
    }
}

pub struct AgentRunRepository<'db> {
    db: &'db Db,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionLineage {
    pub submission_id: String,
    pub created_at: i64,
    pub kind: SubmissionKind,
    pub status: SubmissionStatus,
    pub pueue_task_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SubmissionPageCursor {
    pub created_at: i64,
    pub submission_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLineage {
    pub event_id: Option<i64>,
    pub event_kind: Option<EventKind>,
    pub event_status: Option<EventStatus>,
    pub run_id: Option<i64>,
    pub mode: Option<String>,
    pub run_status: Option<AgentRunStatus>,
    pub started_at: i64,
    pub submissions: Vec<SubmissionLineage>,
    pub execution_kind: Option<String>,
    pub executable_path: Option<String>,
    pub executable_identity: Option<String>,
    pub policy_code: Option<String>,
    pub failure_stage: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunLineageCursor {
    pub started_at: i64,
    pub run_id: i64,
    pub event_id: Option<i64>,
    pub submission_created_at: Option<i64>,
    pub submission_id: Option<String>,
    pub task_id: Option<i64>,
}

impl RunLineageCursor {
    pub fn new(
        started_at: i64,
        run_id: i64,
        submission_id: Option<String>,
        task_id: Option<i64>,
    ) -> Self {
        Self {
            started_at,
            run_id,
            event_id: None,
            submission_id,
            submission_created_at: None,
            task_id,
        }
    }

    pub fn submission(
        started_at: i64,
        run_id: i64,
        submission_id: String,
        submission_created_at: i64,
        task_id: Option<i64>,
    ) -> Self {
        Self {
            started_at,
            run_id,
            event_id: None,
            submission_id: Some(submission_id),
            submission_created_at: Some(submission_created_at),
            task_id,
        }
    }

    pub fn event_only(started_at: i64, event_id: i64) -> Self {
        Self {
            started_at,
            run_id: 0,
            event_id: Some(event_id),
            submission_id: None,
            submission_created_at: None,
            task_id: None,
        }
    }
}

impl RunLineage {
    pub fn root_cursor(&self) -> Option<RunLineageCursor> {
        match (self.run_id, self.event_id) {
            (Some(run_id), _) => Some(RunLineageCursor::new(self.started_at, run_id, None, None)),
            (None, Some(event_id)) => Some(RunLineageCursor::event_only(self.started_at, event_id)),
            (None, None) => None,
        }
    }

    pub fn cursors(&self) -> Vec<RunLineageCursor> {
        let run_id = self.run_id.unwrap_or_default();
        let mut cursors = Vec::new();
        if self.submissions.is_empty() {
            if let Some(cursor) = self.root_cursor() {
                cursors.push(cursor);
            }
        }
        cursors.extend(self.submissions.iter().map(|submission| {
            RunLineageCursor::submission(
                self.started_at,
                run_id,
                submission.submission_id.clone(),
                submission.created_at,
                submission.pueue_task_id,
            )
        }));
        cursors
    }
}

pub struct RunLineageRepository<'db> {
    db: &'db Db,
}

const FOLLOW_SUBMISSION_PAGE_LIMIT: usize = 1;
pub const MAX_FOLLOW_LINEAGE_SUBMISSIONS: usize = FOLLOW_SUBMISSION_PAGE_LIMIT * 4;

impl<'db> RunLineageRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn list_by_project(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<Vec<RunLineage>, AppError> {
        let limit = bounded_diagnostic_limit(limit) as usize;
        self.list_by_project_with_submission_page(
            project_id,
            limit,
            limit,
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
        )
    }

    pub fn list_by_project_follow(
        &self,
        project_id: &str,
        limit: usize,
        after: &BTreeMap<i64, SubmissionPageCursor>,
        head: &BTreeMap<i64, SubmissionPageCursor>,
    ) -> Result<Vec<RunLineage>, AppError> {
        let limit = bounded_diagnostic_limit(limit) as usize;
        self.list_by_project_with_submission_page(
            project_id,
            limit,
            FOLLOW_SUBMISSION_PAGE_LIMIT,
            after,
            head,
            true,
        )
    }

    fn list_by_project_with_submission_page(
        &self,
        project_id: &str,
        limit: usize,
        submission_page_limit: usize,
        after: &BTreeMap<i64, SubmissionPageCursor>,
        head: &BTreeMap<i64, SubmissionPageCursor>,
        follow: bool,
    ) -> Result<Vec<RunLineage>, AppError> {
        let limit = bounded_diagnostic_limit(limit) as usize;
        let runs = AgentRunRepository::new(self.db).list_by_project(project_id, limit)?;
        let event_repository = EventRepository::new(self.db);
        let submission_repository = SubmissionRepository::new(self.db);
        let mut lineages = Vec::new();
        for run in runs {
            let event = event_repository
                .find_by_id(run.primary_event_id)?
                .filter(|event| event.project_id == project_id);
            let mut run_submissions = if let Some(after) = after.get(&run.run_id) {
                let mut page = submission_repository.list_by_origin_agent_run_page_after(
                    project_id,
                    run.run_id,
                    submission_page_limit,
                    Some(after),
                )?;
                page.extend(
                    submission_repository
                        .list_by_origin_agent_run_page_at(project_id, run.run_id, after)?,
                );
                page
            } else if head.contains_key(&run.run_id) {
                Vec::new()
            } else {
                submission_repository.list_by_origin_agent_run_page_after(
                    project_id,
                    run.run_id,
                    submission_page_limit,
                    None,
                )?
            };
            if let Some(head) = head.get(&run.run_id) {
                run_submissions.extend(submission_repository.list_by_origin_agent_run_page_since(
                    project_id,
                    run.run_id,
                    submission_page_limit,
                    Some(head),
                )?);
                run_submissions.extend(
                    submission_repository
                        .list_by_origin_agent_run_page_at(project_id, run.run_id, head)?,
                );
            }
            run_submissions.sort_by(|left, right| {
                right
                    .created_at
                    .cmp(&left.created_at)
                    .then_with(|| right.submission_id.cmp(&left.submission_id))
            });
            run_submissions.dedup_by(|left, right| left.submission_id == right.submission_id);
            let run_submissions = run_submissions
                .iter()
                .map(SubmissionLineage::from)
                .collect();
            lineages.push(RunLineage {
                event_id: event
                    .as_ref()
                    .map(|event| event.event_id)
                    .or(Some(run.primary_event_id)),
                event_kind: event.as_ref().map(|event| event.kind),
                event_status: event.as_ref().map(|event| event.status),
                run_id: Some(run.run_id),
                mode: Some(run.context_mode.as_str().to_owned()),
                run_status: Some(run.status),
                started_at: run.started_at,
                submissions: run_submissions,
                execution_kind: run.execution_kind,
                executable_path: run.executable_path,
                executable_identity: run.executable_identity,
                policy_code: run.policy_code,
                failure_stage: run.failure_stage,
            });
        }
        let selected_primary_event_ids = lineages
            .iter()
            .filter_map(|lineage| lineage.event_id)
            .collect::<std::collections::BTreeSet<_>>();
        let remaining = limit.saturating_sub(lineages.len());
        let events = event_repository.recent_events(project_id, limit)?;
        let event_ids = events.iter().map(|event| event.event_id).collect::<Vec<_>>();
        let linked_events = event_repository.latest_execution_projections(project_id, &event_ids)?;
        let events = events.into_iter().filter(|event| {
            !selected_primary_event_ids.contains(&event.event_id)
                && !linked_events.contains_key(&event.event_id)
        });
        let mut submissions = if follow {
            let mut page = if let Some(after) = after.get(&0) {
                let mut page = submission_repository.list_originless_by_project_page_after(
                    project_id,
                    submission_page_limit,
                    Some(after),
                )?;
                page.extend(
                    submission_repository.list_originless_by_project_page_at(project_id, after)?,
                );
                page
            } else if head.contains_key(&0) {
                Vec::new()
            } else {
                submission_repository.list_originless_by_project_page_after(
                    project_id,
                    submission_page_limit,
                    None,
                )?
            };
            if let Some(head) = head.get(&0) {
                page.extend(submission_repository.list_originless_by_project_page_since(
                    project_id,
                    submission_page_limit,
                    head,
                )?);
                page.extend(
                    submission_repository.list_originless_by_project_page_at(project_id, head)?,
                );
            }
            page.sort_by(|left, right| {
                right
                    .created_at
                    .cmp(&left.created_at)
                    .then_with(|| right.submission_id.cmp(&left.submission_id))
            });
            page.dedup_by(|left, right| left.submission_id == right.submission_id);
            page
        } else {
            submission_repository
                .list_by_project(project_id, limit)?
                .into_iter()
                .filter(|submission| submission.origin_agent_run_id.is_none())
                .collect()
        };
        let mut incomplete = Vec::new();
        for event in events {
            let policy_code = inferred_pre_binding_policy_code(&event, false);
            incomplete.push(RunLineage {
                event_id: Some(event.event_id),
                event_kind: Some(event.kind),
                event_status: Some(event.status),
                run_id: None,
                mode: None,
                run_status: None,
                started_at: event.created_at,
                submissions: Vec::new(),
                execution_kind: None,
                executable_path: None,
                executable_identity: None,
                failure_stage: policy_code.as_ref().map(|_| "pre_binding".to_owned()),
                policy_code,
            });
        }
        if follow {
            if !submissions.is_empty() {
                let started_at = submissions[0].created_at;
                incomplete.push(RunLineage {
                    event_id: None,
                    event_kind: None,
                    event_status: None,
                    run_id: None,
                    mode: None,
                    run_status: None,
                    started_at,
                    submissions: submissions.iter().map(SubmissionLineage::from).collect(),
                    execution_kind: None,
                    executable_path: None,
                    executable_identity: None,
                    policy_code: None,
                    failure_stage: None,
                });
            }
        } else {
            for submission in submissions.drain(..) {
                incomplete.push(RunLineage {
                    event_id: None,
                    event_kind: None,
                    event_status: None,
                    run_id: submission.origin_agent_run_id,
                    mode: None,
                    run_status: None,
                    started_at: submission.created_at,
                    submissions: vec![SubmissionLineage::from(&submission)],
                    execution_kind: None,
                    executable_path: None,
                    executable_identity: None,
                    policy_code: None,
                    failure_stage: None,
                });
            }
        }
        incomplete.sort_by(|left, right| {
            right
                .started_at
                .cmp(&left.started_at)
                .then_with(|| right.run_id.cmp(&left.run_id))
        });
        if !follow {
            incomplete.truncate(remaining);
        }
        lineages.extend(incomplete);
        Ok(lineages)
    }
}

impl From<&Submission> for SubmissionLineage {
    fn from(submission: &Submission) -> Self {
        Self {
            submission_id: submission.submission_id.clone(),
            created_at: submission.created_at,
            kind: submission.kind,
            status: submission.status,
            pueue_task_id: submission.pueue_task_id,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentRunRecovery {
    pub failed_runs: usize,
    pub requeued_events: usize,
    pub dead_lettered_events: usize,
    /// Editor-owned runs are deliberately left untouched during generic
    /// startup recovery.  Their durable attempt binding lets the
    /// code-change coordinator decide whether to resume or reject them.
    pub preserved_code_change_editors: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentRunFinalizationPhase {
    Generic,
    MarkerFailure,
    PendingMarkerPolicy,
    PreRelease,
}

/// The two classes of failures that can occur while a run is still blocked
/// behind its launch gate.  The retry form is retained for transient setup
/// failures; policy failures bypass retry calculation entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateFailurePolicy {
    Retry(RetryPolicy),
    Policy(PolicyViolation),
}

/// Validate the complete durable identity of an active code-change editor.
/// The first boolean records whether any durable binding exists; the second
/// records whether the binding is the exact single-row shape.  Keeping those
/// states distinct lets generic recovery reject a binding attached to a
/// non-editor execution projection instead of silently treating it as an
/// ordinary run.
fn editor_recovery_binding(
    transaction: &Transaction<'_>,
    project_id: &str,
    agent_run_id: i64,
) -> Result<(bool, bool), AppError> {
    let mut statement = transaction
        .prepare(
            "SELECT a.code_change_run_id, a.attempt, a.agent_run_id,
                    a.editor_session_id, a.status,
                    r.state, r.editor_attempts, r.editor_session_id,
                    c.project_id
             FROM code_change_editor_attempts AS a
             JOIN code_change_runs AS r
               ON r.code_change_run_id = a.code_change_run_id
             JOIN campaigns AS c ON c.campaign_id = r.campaign_id
             WHERE a.agent_run_id = ?1",
        )
        .map_err(database_error("prepare code-change editor recovery binding"))?;
    let rows = statement
        .query_map([agent_run_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, CodeChangeState>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, String>(8)?,
            ))
        })
        .map_err(database_error("query code-change editor recovery binding"))?;
    let rows = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(database_error("read code-change editor recovery binding"))?;
    if rows.len() != 1 {
        return Ok((!rows.is_empty(), false));
    }
    let (
        _code_change_run_id,
        attempt,
        bound_agent_run_id,
        session_id,
        attempt_status,
        state,
        run_attempts,
        run_session_id,
        bound_project_id,
    ) = &rows[0];
    let valid = bound_project_id == project_id
            && *bound_agent_run_id == agent_run_id
            && matches!(*attempt, 1 | 2)
            && matches!(attempt_status.as_str(), "reserved" | "running")
            && *state == CodeChangeState::Editing
            && *run_attempts == *attempt
            && run_session_id.as_deref() == Some(session_id.as_str())
            && !session_id.is_empty();
    Ok((true, valid))
}

impl From<RetryPolicy> for GateFailurePolicy {
    fn from(policy: RetryPolicy) -> Self {
        Self::Retry(policy)
    }
}

impl From<PolicyViolation> for GateFailurePolicy {
    fn from(violation: PolicyViolation) -> Self {
        Self::Policy(violation)
    }
}

impl From<&PolicyViolation> for GateFailurePolicy {
    fn from(violation: &PolicyViolation) -> Self {
        Self::Policy(*violation)
    }
}

impl<'db> AgentRunRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub(crate) fn durable_run_id_high_water(
        &self,
        _run_id_guard: &RunIdAdmissionGuard,
    ) -> Result<i64, AppError> {
        let connection = self.db.connect()?;
        read_durable_agent_run_id_high_water(&connection)
    }

    fn acquire_run_id_guard(&self) -> Result<RunIdAdmissionGuard, AppError> {
        acquire_run_id_admission_guard(self.db)
    }

    pub fn insert(&self, run: &NewAgentRun) -> Result<AgentRun, AppError> {
        let run_id_guard = self.acquire_run_id_guard()?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin agent run insertion"))?;
        let run_id = insert_agent_run(&transaction, run, &run_id_guard)?;
        transaction
            .commit()
            .map_err(database_error("commit agent run insertion"))?;
        read_agent_run(&connection, run_id)
    }

    pub fn find_by_id(&self, run_id: i64) -> Result<Option<AgentRun>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{} WHERE run_id = ?1", AGENT_RUN_SELECT),
                [run_id],
                agent_run_from_row,
            )
            .optional()
            .map_err(database_error("find agent run"))
    }

    pub fn insert_with_events(
        &self,
        run: &NewAgentRun,
        event_ids: &[i64],
    ) -> Result<AgentRun, AppError> {
        self.insert_with_events_and_reservation(run, event_ids, None)
    }

    pub fn insert_with_events_and_reservation(
        &self,
        run: &NewAgentRun,
        event_ids: &[i64],
        reservation_token: Option<&str>,
    ) -> Result<AgentRun, AppError> {
        let run_id_guard = self.acquire_run_id_guard()?;
        self.insert_with_events_and_reservation_with_guard(
            run,
            event_ids,
            reservation_token,
            &run_id_guard,
        )
    }

    pub(crate) fn insert_with_events_and_reservation_with_guard(
        &self,
        run: &NewAgentRun,
        event_ids: &[i64],
        reservation_token: Option<&str>,
        _run_id_guard: &RunIdAdmissionGuard,
    ) -> Result<AgentRun, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin agent run, event, and intervention insertion",
            ))?;
        let reservation_count = if let Some(reservation_token) = reservation_token {
            let (total, bound): (i64, i64) = transaction
                .query_row(
                    "SELECT COUNT(*),
                            COALESCE(SUM(CASE WHEN agent_run_id IS NULL THEN 0 ELSE 1 END), 0)
                     FROM interventions
                     WHERE project_id = ?1 AND reservation_token = ?2
                       AND status = 'reserved'",
                    params![run.project_id, reservation_token],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(database_error("validate intervention reservation ownership"))?;
            if total == 0 {
                return Err(AppError::Runtime {
                    operation: "attach intervention reservation to agent run",
                });
            }
            if bound != 0 {
                return Err(AppError::Validation {
                    field: "reservation_token",
                    message: "intervention reservation token is already attached to an agent run",
                });
            }
            usize::try_from(total).map_err(|_| AppError::Runtime {
                operation: "count intervention reservation rows",
            })?
        } else {
            0
        };
        let run_id = insert_agent_run(&transaction, run, _run_id_guard)?;
        for event_id in event_ids {
            let status = transaction
                .query_row(
                    "SELECT status FROM events WHERE project_id = ?1 AND event_id = ?2",
                    params![run.project_id, event_id],
                    |row| row.get::<_, EventStatus>(0),
                )
                .optional()
                .map_err(database_error("validate event for agent run binding"))?;
            if status != Some(EventStatus::Claimed) {
                return Err(AppError::Validation {
                    field: "event_id",
                    message: "event must be a claimed project-owned event",
                });
            }
            attach_event_to_agent_run(&transaction, &run.project_id, run_id, *event_id)?;
        }
        for event_id in event_ids {
            let changed = transaction
                .execute(
                    "UPDATE events
                     SET status = 'in_flight', lease_until = NULL
                     WHERE project_id = ?1 AND event_id = ?2 AND status = 'claimed'",
                    params![run.project_id, event_id],
                )
                .map_err(database_error("bind claimed event to agent run"))?;
            if changed != 1 {
                return Err(AppError::Runtime {
                    operation: "bind claimed event to agent run",
                });
            }
        }
        if let Some(reservation_token) = reservation_token {
            let changed = transaction
                .execute(
                    "UPDATE interventions
                     SET agent_run_id = ?1
                     WHERE project_id = ?2 AND reservation_token = ?3 AND status = ?4
                       AND agent_run_id IS NULL",
                    params![
                        run_id,
                        run.project_id,
                        reservation_token,
                        InterventionStatus::Reserved,
                    ],
                )
                .map_err(database_error(
                    "attach intervention reservation to agent run",
                ))?;
            if changed != reservation_count {
                return Err(AppError::Runtime {
                    operation: "attach intervention reservation to agent run",
                });
            }
        }
        transaction.commit().map_err(database_error(
            "commit agent run, event, and intervention insertion",
        ))?;
        read_agent_run(&connection, run_id)
    }

    pub fn find_active_by_project(&self, project_id: &str) -> Result<Option<AgentRun>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1 AND status IN ('starting', 'running')
                     ORDER BY started_at DESC, run_id DESC LIMIT 1",
                    AGENT_RUN_SELECT
                ),
                [project_id],
                agent_run_from_row,
            )
            .optional()
            .map_err(database_error("find active agent run"))
    }

    pub fn list_startup_marker_candidates(
        &self,
        project_id: &str,
    ) -> Result<Vec<(i64, String, PathBuf)>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT run_id, launch_gate_state, log_path
                 FROM agent_runs
                 WHERE project_id = ?1 AND status IN ('starting', 'running')
                   AND (execution_kind IS NULL OR execution_kind <> 'code_change_editor')
                   AND (
                       launch_gate_state = 'release_requested'
                       OR (
                           launch_gate_state = 'pending'
                           AND policy_code IS NULL AND failure_stage IS NULL
                       )
                   )
                 ORDER BY run_id",
            )
            .map_err(database_error("prepare startup marker candidates"))?;
        let candidates = statement
            .query_map([project_id], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    PathBuf::from(row.get::<_, String>(2)?),
                ))
            })
            .map_err(database_error("query startup marker candidates"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read startup marker candidates"))?;
        Ok(candidates)
    }

    /// Persist the conservative evidence used when a launch marker predates a
    /// newly bound run. This does not finalize the run: it makes the exact
    /// pending-marker policy outcome durable so startup recovery cannot
    /// downgrade it to a retry if the finalization transaction is interrupted.
    pub fn record_pending_marker_policy_evidence(
        &self,
        project_id: &str,
        run_id: i64,
        violation: &PolicyViolation,
    ) -> Result<AgentRun, AppError> {
        if violation.code != PolicyViolationCode::NativeGateFailed
            || violation.stage != PolicyViolationStage::PostMarker
        {
            return Err(AppError::Validation {
                field: "policy_violation",
                message: "pending-marker evidence requires native_gate_failed/post_marker",
            });
        }

        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin pending-marker policy evidence"))?;
        let Some((stored_project_id, status, gate_state, policy_code, failure_stage)) = transaction
            .query_row(
                "SELECT project_id, status, launch_gate_state, policy_code, failure_stage
                 FROM agent_runs WHERE run_id = ?1",
                [run_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, AgentRunStatus>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .optional()
            .map_err(database_error("validate pending-marker policy evidence run"))?
        else {
            return Err(AppError::Validation {
                field: "run_id",
                message: "agent run does not exist",
            });
        };
        if stored_project_id != project_id {
            return Err(AppError::Validation {
                field: "project_id",
                message: "agent run belongs to another project",
            });
        }
        if status != AgentRunStatus::Starting {
            return Err(AppError::Validation {
                field: "status",
                message: "pending-marker evidence requires a starting run",
            });
        }
        if gate_state != "pending" {
            return Err(AppError::Validation {
                field: "launch_gate_state",
                message: "pending-marker evidence requires a pending launch gate",
            });
        }
        let exact_evidence = (
            Some(violation.code.as_str().to_owned()),
            Some(violation.stage.as_str().to_owned()),
        );
        if (policy_code.clone(), failure_stage.clone()) != (None, None)
            && (policy_code, failure_stage) != exact_evidence
        {
            return Err(AppError::Validation {
                field: "policy_violation",
                message: "agent run already carries different policy evidence",
            });
        }

        let invalid_events = transaction
            .query_row(
                "SELECT COUNT(*)
                 FROM agent_run_events
                 JOIN events
                   ON events.project_id = agent_run_events.project_id
                  AND events.event_id = agent_run_events.event_id
                 WHERE agent_run_events.project_id = ?1
                   AND agent_run_events.run_id = ?2
                   AND events.status <> 'in_flight'",
                params![project_id, run_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(database_error("validate pending-marker evidence events"))?;
        if invalid_events != 0 {
            return Err(AppError::Validation {
                field: "event_status",
                message: "pending-marker evidence requires in-flight linked events",
            });
        }
        let invalid_interventions = transaction
            .query_row(
                "SELECT COUNT(*) FROM interventions
                 WHERE project_id = ?1 AND agent_run_id = ?2 AND status <> 'reserved'",
                params![project_id, run_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(database_error(
                "validate pending-marker evidence interventions",
            ))?;
        if invalid_interventions != 0 {
            return Err(AppError::Validation {
                field: "intervention_status",
                message: "pending-marker evidence requires reserved interventions",
            });
        }

        transaction
            .execute(
                "UPDATE agent_runs
                 SET policy_code = ?1, failure_stage = ?2
                 WHERE project_id = ?3 AND run_id = ?4
                   AND status = 'starting' AND launch_gate_state = 'pending'
                   AND (policy_code IS NULL OR policy_code = ?1)
                   AND (failure_stage IS NULL OR failure_stage = ?2)",
                params![
                    violation.code.as_str(),
                    violation.stage.as_str(),
                    project_id,
                    run_id,
                ],
            )
            .map_err(database_error("record pending-marker policy evidence"))?;
        transaction
            .commit()
            .map_err(database_error("commit pending-marker policy evidence"))?;
        read_agent_run(&connection, run_id)
    }

    pub fn recover_interrupted(
        &self,
        finished_at: i64,
        reason: &str,
        policies: &BTreeMap<String, RetryPolicy>,
        confirmed_pending_marker_ids: &BTreeSet<i64>,
        confirmed_release_requested_ids: &BTreeSet<i64>,
    ) -> Result<AgentRunRecovery, AppError> {
        self.recover_interrupted_with_marker_evidence(
            finished_at,
            reason,
            policies,
            confirmed_pending_marker_ids,
            confirmed_release_requested_ids,
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
    }

    /// Recover durable startup gate evidence. Confirmed marker evidence and
    /// final-entry indeterminate evidence are deliberately distinct: the
    /// latter has a pinned secure parent but cannot establish marker validity.
    pub fn recover_interrupted_with_marker_evidence(
        &self,
        finished_at: i64,
        reason: &str,
        policies: &BTreeMap<String, RetryPolicy>,
        confirmed_pending_marker_ids: &BTreeSet<i64>,
        confirmed_release_requested_ids: &BTreeSet<i64>,
        indeterminate_pending_marker_ids: &BTreeSet<i64>,
        indeterminate_release_requested_ids: &BTreeSet<i64>,
    ) -> Result<AgentRunRecovery, AppError> {
        if confirmed_pending_marker_ids
            .iter()
            .any(|run_id| confirmed_release_requested_ids.contains(run_id))
        {
            return Err(AppError::Validation {
                field: "launch_gate_state",
                message: "startup marker evidence cannot belong to two gate phases",
            });
        }
        {
            let connection = self.db.connect()?;
            for (run_id, expected_status, expected_gate, allow_running_pending) in
                confirmed_pending_marker_ids
                .iter()
                .map(|run_id| (*run_id, AgentRunStatus::Starting, "pending", false))
                .chain(indeterminate_pending_marker_ids.iter().map(|run_id| {
                    (*run_id, AgentRunStatus::Starting, "pending", true)
                }))
                .chain(confirmed_release_requested_ids.iter().map(|run_id| {
                    (*run_id, AgentRunStatus::Running, "release_requested", false)
                }))
                .chain(indeterminate_release_requested_ids.iter().map(|run_id| {
                    (*run_id, AgentRunStatus::Running, "release_requested", false)
                }))
            {
                let Some((status, gate_state)) = connection
                    .query_row(
                        "SELECT status, launch_gate_state FROM agent_runs WHERE run_id = ?1",
                        [run_id],
                        |row| {
                            Ok((
                                row.get::<_, AgentRunStatus>(0)?,
                                row.get::<_, String>(1)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(database_error("validate startup marker evidence phase"))?
                else {
                    return Err(AppError::Validation {
                        field: "run_id",
                        message: "startup marker evidence run does not exist",
                    });
                };
                let status_matches = if expected_gate == "release_requested" {
                    matches!(status, AgentRunStatus::Starting | AgentRunStatus::Running)
                } else if allow_running_pending {
                    matches!(status, AgentRunStatus::Starting | AgentRunStatus::Running)
                } else {
                    status == expected_status
                };
                if !status_matches || gate_state != expected_gate {
                    return Err(AppError::Validation {
                        field: "launch_gate_state",
                        message: "startup marker evidence does not match its gate phase",
                    });
                }
            }
        }
        let projects = ProjectRepository::new(self.db).list_all()?;
        for project in &projects {
            if !policies.contains_key(&project.project_id) {
                return Err(AppError::Validation {
                    field: "retry_policy",
                    message: "startup recovery policy is missing for a project",
                });
            }
        }

        let pre_marker_reason = bounded_redacted_text(&format!(
            "restart_interruption: pre-marker execution not confirmed ({reason})"
        ));
        let execution_unknown_reason = bounded_redacted_text(&format!(
            "restart_interruption: execution outcome unknown ({reason})"
        ));
        let mut recovery = AgentRunRecovery::default();

        for project in projects {
            let policy = *policies
                .get(&project.project_id)
                .ok_or(AppError::Validation {
                    field: "retry_policy",
                    message: "startup recovery policy is missing for a project",
                })?;
            let mut connection = self.db.connect()?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(database_error("begin project interrupted agent run recovery"))?;

            let active_runs = {
                let mut statement = transaction
                    .prepare(
                        "SELECT run_id, status, launch_gate_state, policy_code, failure_stage,
                                execution_kind
                         FROM agent_runs
                         WHERE project_id = ?1 AND status IN ('starting', 'running')
                         ORDER BY run_id",
                    )
                    .map_err(database_error("prepare project interrupted agent runs"))?;
                let rows = statement
                    .query_map([&project.project_id], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, AgentRunStatus>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Option<String>>(3)?,
                                row.get::<_, Option<String>>(4)?,
                                row.get::<_, Option<String>>(5)?,
                        ))
                    })
                    .map_err(database_error("query project interrupted agent runs"))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(database_error("read project interrupted agent runs"))?;
                rows
            };

            // A code-change editor is recoverable only when its execution
            // projection and durable attempt binding agree exactly.  Do
            // this validation for every active row before mutating any of
            // them so a contradiction fails closed without a partial
            // generic-recovery pass.
            for (run_id, _run_status, _gate_state, _policy_code, _failure_stage, execution_kind)
                in &active_runs
            {
                let (has_binding, valid_binding) =
                    editor_recovery_binding(&transaction, &project.project_id, *run_id)?;
                if execution_kind.as_deref() == Some("code_change_editor") && !valid_binding {
                    return Err(AppError::Validation {
                        field: "code_change_editor_attempt",
                        message: "code-change editor execution has no exact durable attempt binding",
                    });
                }
                if execution_kind.as_deref() != Some("code_change_editor") && has_binding {
                    return Err(AppError::Validation {
                        field: "execution_kind",
                        message: "durable code-change editor binding requires code_change_editor execution kind",
                    });
                }
            }

            for (run_id, run_status, gate_state, policy_code, failure_stage, execution_kind) in active_runs {
                if execution_kind.as_deref() == Some("code_change_editor") {
                    // The preflight above established the exact binding.  Do
                    // not resolve its event, interventions, or agent row as
                    // generic interrupted work.
                    recovery.preserved_code_change_editors += 1;
                    continue;
                }
                if confirmed_pending_marker_ids.contains(&run_id)
                    && !(run_status == AgentRunStatus::Starting && gate_state == "pending")
                {
                    return Err(AppError::Validation {
                        field: "launch_gate_state",
                        message: "pending marker evidence changed phase during recovery",
                    });
                }
                if indeterminate_pending_marker_ids.contains(&run_id)
                    && !(matches!(run_status, AgentRunStatus::Starting | AgentRunStatus::Running)
                        && gate_state == "pending")
                {
                    return Err(AppError::Validation {
                        field: "launch_gate_state",
                        message: "indeterminate pending marker evidence changed phase during recovery",
                    });
                }
                if (confirmed_release_requested_ids.contains(&run_id)
                    || indeterminate_release_requested_ids.contains(&run_id))
                    && gate_state != "release_requested"
                {
                    return Err(AppError::Validation {
                        field: "launch_gate_state",
                        message: "release-requested marker evidence changed phase during recovery",
                    });
                }
                if run_status == AgentRunStatus::Starting && gate_state == "pending" {
                    let evidence_is_empty = policy_code.is_none() && failure_stage.is_none();
                    let evidence_is_exact = policy_code.as_deref()
                        == Some(PolicyViolationCode::NativeGateFailed.as_str())
                        && failure_stage.as_deref()
                            == Some(PolicyViolationStage::PostMarker.as_str());
                    if !evidence_is_empty && !evidence_is_exact {
                        return Err(AppError::Validation {
                            field: "policy_violation",
                            message: "pending run carries incomplete or conflicting policy evidence",
                        });
                    }
                }
                let linked_events = {
                    let mut statement = transaction
                        .prepare(
                            "SELECT events.event_id, events.status, events.attempts
                             FROM agent_run_events
                             JOIN events
                               ON events.project_id = agent_run_events.project_id
                              AND events.event_id = agent_run_events.event_id
                             WHERE agent_run_events.project_id = ?1
                               AND agent_run_events.run_id = ?2
                             ORDER BY events.event_id",
                        )
                        .map_err(database_error("prepare project recovery event query"))?;
                    let rows = statement
                        .query_map(params![&project.project_id, run_id], |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, EventStatus>(1)?,
                                row.get::<_, i64>(2)?,
                            ))
                        })
                        .map_err(database_error("query project recovery events"))?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(database_error("read project recovery events"))?;
                    rows
                };
                let pending_marker_policy_evidence = gate_state == "pending"
                    && ((run_status == AgentRunStatus::Starting
                        && confirmed_pending_marker_ids.contains(&run_id))
                        || (matches!(run_status, AgentRunStatus::Starting | AgentRunStatus::Running)
                            && indeterminate_pending_marker_ids.contains(&run_id))
                        || (run_status == AgentRunStatus::Starting
                            && policy_code.as_deref()
                            == Some(PolicyViolationCode::NativeGateFailed.as_str())
                            && failure_stage.as_deref()
                                == Some(PolicyViolationStage::PostMarker.as_str())));
                if pending_marker_policy_evidence && run_status == AgentRunStatus::Starting {
                    let invalid_interventions = transaction
                        .query_row(
                            "SELECT COUNT(*) FROM interventions
                             WHERE project_id = ?1 AND agent_run_id = ?2
                               AND status <> 'reserved'",
                            params![&project.project_id, run_id],
                            |row| row.get::<_, i64>(0),
                        )
                        .map_err(database_error(
                            "validate pending-marker recovery interventions",
                        ))?;
                    if invalid_interventions != 0 {
                        return Err(AppError::Validation {
                            field: "intervention_status",
                            message: "pending-marker recovery requires reserved interventions",
                        });
                    }
                }
                let execution_unknown = (gate_state == "release_requested"
                    && (confirmed_release_requested_ids.contains(&run_id)
                        || indeterminate_release_requested_ids.contains(&run_id)))
                    || (run_status == AgentRunStatus::Running
                        && gate_state == "pending"
                        && indeterminate_pending_marker_ids.contains(&run_id))
                    || gate_state == "released"
                    || linked_events
                        .iter()
                        .any(|(_, status, _)| *status == EventStatus::Dispatched);
                if linked_events.iter().any(|(_, status, _)| {
                    if execution_unknown {
                        !matches!(status, EventStatus::InFlight | EventStatus::Dispatched)
                    } else {
                        *status != EventStatus::InFlight
                    }
                }) {
                    return Err(AppError::Validation {
                        field: "event_status",
                        message: "linked event is not in the expected startup recovery state",
                    });
                }
                let pending_marker_reason = format!(
                    "policy_blocked:{}",
                    PolicyViolationCode::NativeGateFailed.as_str()
                );
                let recovery_reason = if pending_marker_policy_evidence {
                    &pending_marker_reason
                } else if execution_unknown {
                    &execution_unknown_reason
                } else {
                    &pre_marker_reason
                };

                for (event_id, event_status, attempts) in linked_events {
                    let (status, not_before, completed_at) = if pending_marker_policy_evidence
                        && event_status == EventStatus::InFlight
                    {
                        (EventStatus::DeadLetter, None, Some(finished_at))
                    } else if execution_unknown
                        && matches!(event_status, EventStatus::InFlight | EventStatus::Dispatched)
                    {
                        (
                            EventStatus::DeadLetter,
                            None,
                            Some(finished_at),
                        )
                    } else if !execution_unknown && event_status == EventStatus::InFlight {
                        match retry_decision(attempts, finished_at, policy) {
                            RetryDecision::Retry { not_before } => {
                                (EventStatus::RetryWait, Some(not_before), None)
                            }
                            RetryDecision::DeadLetter => {
                                (EventStatus::DeadLetter, None, Some(finished_at))
                            }
                        }
                    } else {
                        return Err(AppError::Validation {
                            field: "event_status",
                            message: "linked event is not recoverable during startup",
                        });
                    };
                    let changed = transaction
                        .execute(
                            "UPDATE events
                             SET status = ?1, lease_until = NULL,
                                 not_before = CASE
                                     WHEN ?1 = 'dead_letter' THEN not_before
                                     ELSE ?2
                                 END,
                                 completed_at = ?3, last_error = ?4
                             WHERE project_id = ?5 AND event_id = ?6 AND status = ?7",
                            params![
                                status,
                                not_before,
                                completed_at,
                                recovery_reason,
                                &project.project_id,
                                event_id,
                                event_status,
                            ],
                        )
                        .map_err(database_error("resolve project interrupted event"))?;
                    if changed != 1 {
                        return Err(AppError::Runtime {
                            operation: "resolve project interrupted event",
                        });
                    }
                    match status {
                        EventStatus::RetryWait => recovery.requeued_events += 1,
                        EventStatus::DeadLetter => recovery.dead_lettered_events += 1,
                        _ => {}
                    }
                }

                let intervention_statuses = if execution_unknown || pending_marker_policy_evidence {
                    "status = 'reserved'"
                } else {
                    "status IN ('reserved', 'applied')"
                };
                transaction
                    .execute(
                        &format!(
                            "UPDATE interventions
                             SET status = 'pending', reserved_at = NULL, applied_at = NULL,
                                 agent_run_id = NULL, lease_expires_at = NULL, reservation_token = NULL
                             WHERE project_id = ?1 AND agent_run_id = ?2 AND {intervention_statuses}"
                        ),
                        params![&project.project_id, run_id],
                    )
                    .map_err(database_error("release project interrupted interventions"))?;

                let final_gate = if execution_unknown { "released" } else { "failed" };
                let changed = transaction
                    .execute(
                        "UPDATE agent_runs
                         SET status = 'failed', finished_at = ?1, last_error = ?2,
                             launch_gate_state = ?3,
                             policy_code = CASE WHEN ?4 THEN 'native_gate_failed' ELSE policy_code END,
                             failure_stage = CASE WHEN ?4 THEN 'post_marker' ELSE failure_stage END
                         WHERE project_id = ?5 AND run_id = ?6
                           AND status IN ('starting', 'running')",
                        params![
                            finished_at,
                            recovery_reason,
                            final_gate,
                            pending_marker_policy_evidence,
                            &project.project_id,
                            run_id,
                        ],
                    )
                    .map_err(database_error("fail project interrupted agent run"))?;
                if changed != 1 {
                    return Err(AppError::Runtime {
                        operation: "fail project interrupted agent run",
                    });
                }
                recovery.failed_runs += 1;
            }

            transaction
                .execute(
                    "UPDATE interventions
                     SET status = 'pending', reserved_at = NULL, applied_at = NULL,
                         agent_run_id = NULL, lease_expires_at = NULL, reservation_token = NULL
                     WHERE project_id = ?1 AND status = 'reserved'
                       AND agent_run_id IS NULL AND lease_expires_at <= ?2",
                    params![&project.project_id, finished_at],
                )
                .map_err(database_error(
                    "recover expired project interventions",
                ))?;
            transaction
                .execute(
                    "UPDATE interventions
                     SET status = 'pending', reserved_at = NULL, applied_at = NULL,
                         agent_run_id = NULL, lease_expires_at = NULL, reservation_token = NULL
                     WHERE project_id = ?1 AND agent_run_id IS NOT NULL
                       AND status IN ('reserved', 'applied')
                       AND EXISTS (
                           SELECT 1 FROM agent_runs
                           WHERE agent_runs.project_id = interventions.project_id
                             AND agent_runs.run_id = interventions.agent_run_id
                             AND agent_runs.status = 'failed'
                             AND agent_runs.launch_gate_state IN ('pending', 'release_requested', 'failed')
                       )",
                    [&project.project_id],
                )
                .map_err(database_error(
                    "release stale pre-marker project interventions",
                ))?;
            transaction
                .commit()
                .map_err(database_error("commit project interrupted agent run recovery"))?;
        }

        Ok(recovery)
    }

    pub fn attach_event(&self, run_id: i64, event_id: i64) -> Result<AgentRunEvent, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin agent run event attachment"))?;
        let project_id = transaction
            .query_row(
                "SELECT project_id FROM agent_runs WHERE run_id = ?1",
                [run_id],
                |row| row.get::<_, String>(0),
            )
            .map_err(database_error("read agent run project"))?;
        attach_event_to_agent_run(&transaction, &project_id, run_id, event_id)?;
        transaction
            .commit()
            .map_err(database_error("commit agent run event attachment"))?;
        Ok(AgentRunEvent {
            project_id,
            run_id,
            event_id,
        })
    }

    pub fn mark_running(&self, run_id: i64, pid: i64) -> Result<AgentRun, AppError> {
        let connection = self.db.connect()?;
        connection
            .execute(
                "UPDATE agent_runs
                 SET pid = ?1, status = 'running', launch_gate_state = 'released'
                 WHERE run_id = ?2",
                params![pid, run_id],
            )
            .map_err(database_error("mark agent run running"))?;
        read_agent_run(&connection, run_id)
    }

    pub fn mark_running_and_apply_interventions(
        &self,
        project_id: &str,
        run_id: i64,
        pid: i64,
        applied_at: i64,
    ) -> Result<AgentRun, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin agent run start and intervention application",
            ))?;
        let changed = transaction
            .execute(
                "UPDATE agent_runs SET pid = ?1, status = 'running', launch_gate_state = 'pending'
                 WHERE project_id = ?2 AND run_id = ?3",
                params![pid, project_id, run_id],
            )
            .map_err(database_error("mark agent run running"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "mark project agent run running",
            });
        }
        transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, applied_at = ?2, lease_expires_at = NULL,
                     reservation_token = NULL
                 WHERE project_id = ?3 AND agent_run_id = ?4 AND status = ?5",
                params![
                    InterventionStatus::Applied,
                    applied_at,
                    project_id,
                    run_id,
                    InterventionStatus::Reserved,
                ],
            )
            .map_err(database_error(
                "mark interventions applied for running agent",
            ))?;
        transaction.commit().map_err(database_error(
            "commit agent run start and intervention application",
        ))?;
        read_agent_run(&connection, run_id)
    }

    pub fn mark_gate_release_requested(
        &self,
        project_id: &str,
        run_id: i64,
    ) -> Result<(), AppError> {
        let connection = self.db.connect()?;
        let changed = connection
            .execute(
                "UPDATE agent_runs
                 SET launch_gate_state = 'release_requested'
                 WHERE project_id = ?1 AND run_id = ?2 AND launch_gate_state = 'pending'",
                params![project_id, run_id],
            )
            .map_err(database_error("record agent launch gate release request"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "record agent launch gate release request",
            });
        }
        Ok(())
    }

    pub fn mark_gate_released(&self, project_id: &str, run_id: i64) -> Result<(), AppError> {
        let connection = self.db.connect()?;
        let changed = connection
            .execute(
                "UPDATE agent_runs
                 SET launch_gate_state = 'released'
                 WHERE project_id = ?1 AND run_id = ?2
                   AND launch_gate_state = 'release_requested'",
                params![project_id, run_id],
            )
            .map_err(database_error("record agent launch gate release"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "record agent launch gate release",
            });
        }
        Ok(())
    }

    pub fn acknowledge_dispatch(&self, project_id: &str, run_id: i64) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin agent dispatch acknowledgement"))?;

        let Some((stored_project_id, gate_state)) = transaction
            .query_row(
                "SELECT project_id, launch_gate_state FROM agent_runs WHERE run_id = ?1",
                [run_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(database_error("validate agent dispatch acknowledgement run"))?
        else {
            return Err(AppError::Validation {
                field: "run_id",
                message: "agent run does not exist",
            });
        };
        if stored_project_id != project_id {
            return Err(AppError::Validation {
                field: "project_id",
                message: "agent run belongs to another project",
            });
        }
        if gate_state != "release_requested" {
            return Err(AppError::Validation {
                field: "launch_gate_state",
                message: "agent launch gate is not awaiting dispatch acknowledgement",
            });
        }

        let event_count = transaction
            .query_row(
                "SELECT COUNT(*) FROM agent_run_events
                 WHERE project_id = ?1 AND run_id = ?2",
                params![project_id, run_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(database_error("count agent dispatch events"))?;
        let in_flight_count = transaction
            .query_row(
                "SELECT COUNT(*) FROM agent_run_events
                 JOIN events
                   ON events.project_id = agent_run_events.project_id
                  AND events.event_id = agent_run_events.event_id
                 WHERE agent_run_events.project_id = ?1
                   AND agent_run_events.run_id = ?2
                   AND events.status = 'in_flight'",
                params![project_id, run_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(database_error("validate agent dispatch event states"))?;
        if event_count != in_flight_count {
            return Err(AppError::Validation {
                field: "event_status",
                message: "all linked events must be in flight",
            });
        }

        let changed = transaction
            .execute(
                "UPDATE agent_runs
                 SET launch_gate_state = 'released'
                 WHERE project_id = ?1 AND run_id = ?2
                   AND launch_gate_state = 'release_requested'",
                params![project_id, run_id],
            )
            .map_err(database_error("acknowledge agent dispatch gate"))?;
        if changed != 1 {
            return Err(AppError::Validation {
                field: "launch_gate_state",
                message: "agent launch gate changed before dispatch acknowledgement",
            });
        }
        let dispatched = transaction
            .execute(
                "UPDATE events
                 SET status = 'dispatched', lease_until = NULL
                 WHERE project_id = ?1 AND status = 'in_flight'
                   AND event_id IN (
                       SELECT event_id FROM agent_run_events
                       WHERE project_id = ?1 AND run_id = ?2
                   )",
                params![project_id, run_id],
            )
            .map_err(database_error("acknowledge linked agent dispatch events"))?;
        if i64::try_from(dispatched).ok() != Some(event_count) {
            return Err(AppError::Runtime {
                operation: "acknowledge linked agent dispatch events",
            });
        }
        transaction
            .commit()
            .map_err(database_error("commit agent dispatch acknowledgement"))?;
        Ok(dispatched)
    }

    pub fn fail_before_gate_release_with_policy<P: Into<GateFailurePolicy>>(
        &self,
        project_id: &str,
        run_id: i64,
        finished_at: i64,
        reason: &str,
        policy: P,
    ) -> Result<AgentRun, AppError> {
        let resolution = match policy.into() {
            GateFailurePolicy::Retry(policy) => EventResolution::RetryPolicy(policy),
            GateFailurePolicy::Policy(violation) => EventResolution::PolicyBlocked {
                code: violation.code,
                stage: violation.stage,
            },
        };
        self.finish_and_resolve_events_inner(
            project_id,
            run_id,
            AgentRunStatus::Failed,
            finished_at,
            None,
            Some(reason),
            resolution,
            true,
            AgentRunFinalizationPhase::PreRelease,
        )
    }

    pub fn finish_and_resolve_events(
        &self,
        project_id: &str,
        run_id: i64,
        status: AgentRunStatus,
        finished_at: i64,
        exit_code: Option<i64>,
        last_error: Option<&str>,
        resolution: EventResolution,
    ) -> Result<AgentRun, AppError> {
        self.finish_and_resolve_events_inner(
            project_id,
            run_id,
            status,
            finished_at,
            exit_code,
            last_error,
            resolution,
            false,
            AgentRunFinalizationPhase::Generic,
        )
    }

    pub fn finish_after_marker_failure(
        &self,
        project_id: &str,
        run_id: i64,
        finished_at: i64,
        reason: &str,
    ) -> Result<AgentRun, AppError> {
        self.finish_and_resolve_events_inner(
            project_id,
            run_id,
            AgentRunStatus::Failed,
            finished_at,
            None,
            Some(reason),
            EventResolution::ExecutionUnknown {
                reason: reason.to_owned(),
            },
            false,
            AgentRunFinalizationPhase::MarkerFailure,
        )
    }

    /// Finalize a run after the durable launch marker when a policy violation
    /// is discovered.  Execution is already possible at this point, so linked
    /// events are dead-lettered and applied interventions remain applied.
    pub fn finish_after_marker_policy_failure(
        &self,
        project_id: &str,
        run_id: i64,
        finished_at: i64,
        violation: &PolicyViolation,
    ) -> Result<AgentRun, AppError> {
        self.finish_and_resolve_events_inner(
            project_id,
            run_id,
            AgentRunStatus::Failed,
            finished_at,
            None,
            Some("policy violation after launch marker"),
            EventResolution::PolicyBlocked {
                code: violation.code,
                stage: violation.stage,
            },
            false,
            AgentRunFinalizationPhase::MarkerFailure,
        )
    }

    /// Conservatively finalize a newly bound run when a durable marker is
    /// already present before any child is created. The marker is evidence,
    /// not proof that this run requested release, so this transition accepts
    /// only the original Starting/pending binding state.
    pub fn finish_pending_marker_policy_failure(
        &self,
        project_id: &str,
        run_id: i64,
        finished_at: i64,
        violation: &PolicyViolation,
    ) -> Result<AgentRun, AppError> {
        self.finish_and_resolve_events_inner(
            project_id,
            run_id,
            AgentRunStatus::Failed,
            finished_at,
            None,
            Some("pre-existing launch marker blocked a pending run"),
            EventResolution::PolicyBlocked {
                code: violation.code,
                stage: violation.stage,
            },
            false,
            AgentRunFinalizationPhase::PendingMarkerPolicy,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_and_resolve_events_inner(
        &self,
        project_id: &str,
        run_id: i64,
        status: AgentRunStatus,
        finished_at: i64,
        exit_code: Option<i64>,
        last_error: Option<&str>,
        resolution: EventResolution,
        reset_applied_interventions: bool,
        phase: AgentRunFinalizationPhase,
    ) -> Result<AgentRun, AppError> {
        if !matches!(
            status,
            AgentRunStatus::Completed
                | AgentRunStatus::Failed
                | AgentRunStatus::TimedOut
                | AgentRunStatus::Cancelled
        ) {
            return Err(AppError::Validation {
                field: "status",
                message: "agent run finalization requires a terminal status",
            });
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin agent run event finalization"))?;

        let Some((stored_project_id, current_status, gate_state)) = transaction
            .query_row(
                "SELECT project_id, status, launch_gate_state
                 FROM agent_runs WHERE run_id = ?1",
                [run_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, AgentRunStatus>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(database_error("validate agent run event finalization run"))?
        else {
            return Err(AppError::Validation {
                field: "run_id",
                message: "agent run does not exist",
            });
        };
        if stored_project_id != project_id {
            return Err(AppError::Validation {
                field: "project_id",
                message: "agent run belongs to another project",
            });
        }
        let status_matches = match phase {
            AgentRunFinalizationPhase::PendingMarkerPolicy => {
                current_status == AgentRunStatus::Starting
            }
            _ => matches!(
                current_status,
                AgentRunStatus::Starting | AgentRunStatus::Running
            ),
        };
        if !status_matches {
            return Err(AppError::Validation {
                field: "status",
                message: "agent run is not active",
            });
        }
        let expected_gate = match phase {
            AgentRunFinalizationPhase::Generic => "released",
            AgentRunFinalizationPhase::MarkerFailure => "release_requested",
            AgentRunFinalizationPhase::PendingMarkerPolicy => "pending",
            AgentRunFinalizationPhase::PreRelease => "pending_or_release_requested",
        };
        let gate_matches = match phase {
            AgentRunFinalizationPhase::Generic => gate_state == "released",
            AgentRunFinalizationPhase::MarkerFailure => gate_state == "release_requested",
            AgentRunFinalizationPhase::PendingMarkerPolicy => gate_state == "pending",
            AgentRunFinalizationPhase::PreRelease => {
                matches!(gate_state.as_str(), "pending" | "release_requested")
            }
        };
        if !gate_matches {
            return Err(AppError::Validation {
                field: "launch_gate_state",
                message: expected_gate,
            });
        }

        let linked_events = {
            let mut statement = transaction
                .prepare(
                    "SELECT events.event_id, events.status, events.attempts, events.kind,
                            events.dedup_key, events.campaign_id, events.experiment_id
                     FROM agent_run_events
                     JOIN events
                       ON events.project_id = agent_run_events.project_id
                      AND events.event_id = agent_run_events.event_id
                     WHERE agent_run_events.project_id = ?1
                       AND agent_run_events.run_id = ?2
                     ORDER BY events.event_id",
                )
                .map_err(database_error("prepare agent run event finalization"))?;
            let events = statement
                .query_map(params![project_id, run_id], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, EventStatus>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, EventKind>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                })
                .map_err(database_error("query agent run event finalization"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read agent run event finalization"))?;
            events
        };
        let events_match = match phase {
            AgentRunFinalizationPhase::Generic => linked_events
                .iter()
                .all(|(_, event_status, ..)| *event_status == EventStatus::Dispatched),
            AgentRunFinalizationPhase::MarkerFailure
            | AgentRunFinalizationPhase::PendingMarkerPolicy
            | AgentRunFinalizationPhase::PreRelease => {
                linked_events
                    .iter()
                    .all(|(_, event_status, ..)| *event_status == EventStatus::InFlight)
            }
        };
        if !events_match {
            return Err(AppError::Validation {
                field: "event_status",
                message: "linked events are not in the expected finalization phase",
            });
        }

        if phase == AgentRunFinalizationPhase::PendingMarkerPolicy {
            let non_reserved_interventions = transaction
                .query_row(
                    "SELECT COUNT(*) FROM interventions
                     WHERE project_id = ?1 AND agent_run_id = ?2 AND status <> 'reserved'",
                    params![project_id, run_id],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(database_error("validate pending-marker interventions"))?;
            if non_reserved_interventions != 0 {
                return Err(AppError::Validation {
                    field: "intervention_status",
                    message: "pending-marker interventions must remain reserved",
                });
            }
        }

        let bounded_run_error = match &resolution {
            EventResolution::ExecutionUnknown { reason } => Some(bounded_redacted_text(reason)),
            EventResolution::RetryPolicy(_) => last_error.map(bounded_redacted_text),
            EventResolution::PolicyBlocked { code, .. } => {
                Some(format!("policy_blocked:{}", code.as_str()))
            }
        };
        let bounded_event_error = bounded_run_error.clone();
        let (policy_code, failure_stage) = match &resolution {
            EventResolution::PolicyBlocked { code, stage } => {
                (Some(code.as_str()), Some(stage.as_str()))
            }
            EventResolution::ExecutionUnknown { .. } | EventResolution::RetryPolicy(_) => {
                (None, None)
            }
        };
        let expected_event_status = match phase {
            AgentRunFinalizationPhase::Generic => EventStatus::Dispatched,
            AgentRunFinalizationPhase::MarkerFailure
            | AgentRunFinalizationPhase::PendingMarkerPolicy
            | AgentRunFinalizationPhase::PreRelease => {
                EventStatus::InFlight
            }
        };
        for (
            event_id,
            _,
            attempts,
            event_kind,
            event_dedup_key,
            event_campaign_id,
            event_experiment_id,
        ) in linked_events
        {
            let (event_status, not_before, completed_at) = if matches!(
                &resolution,
                EventResolution::ExecutionUnknown { .. }
                    | EventResolution::PolicyBlocked { .. }
            ) {
                (EventStatus::DeadLetter, None, Some(finished_at))
            } else if status == AgentRunStatus::Completed {
                (EventStatus::Completed, None, Some(finished_at))
            } else {
                match retry_decision(
                    attempts,
                    finished_at,
                    match &resolution {
                        EventResolution::RetryPolicy(policy) => *policy,
                        EventResolution::ExecutionUnknown { .. }
                        | EventResolution::PolicyBlocked { .. } => unreachable!(),
                    },
                ) {
                    RetryDecision::Retry { not_before } => {
                        (EventStatus::RetryWait, Some(not_before), None)
                    }
                    RetryDecision::DeadLetter => {
                        (EventStatus::DeadLetter, None, Some(finished_at))
                    }
                }
            };
            let changed = transaction
                .execute(
                    "UPDATE events
                     SET status = ?1, lease_until = NULL,
                         not_before = COALESCE(?2, not_before),
                         completed_at = ?3, last_error = ?4
                     WHERE project_id = ?5 AND event_id = ?6 AND status = ?7",
                    params![
                        event_status,
                        not_before,
                        completed_at,
                        bounded_event_error.as_deref(),
                        project_id,
                        event_id,
                        expected_event_status,
                    ],
                )
                .map_err(database_error("resolve linked agent event"))?;
            if changed != 1 {
                return Err(AppError::Runtime {
                    operation: "resolve linked agent event",
                });
            }
            if event_kind == EventKind::CampaignDecision {
                let failed_lineage = transaction
                    .query_row(
                        "SELECT dc.cycle_id, dc.campaign_id, dc.source_experiment_id,
                                dc.state, c.state, p.enabled, p.paused, p.halted_reason
                         FROM decision_attempts da
                         JOIN decision_cycles dc ON dc.cycle_id = da.cycle_id
                         JOIN campaigns c ON c.campaign_id = dc.campaign_id
                         JOIN projects p ON p.project_id = c.project_id
                         WHERE da.agent_run_id = ?1 AND da.state = 'failed'
                           AND c.project_id = ?2",
                        params![run_id, project_id],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, DecisionCycleState>(3)?,
                                row.get::<_, CampaignState>(4)?,
                                row.get::<_, bool>(5)?,
                                row.get::<_, bool>(6)?,
                                row.get::<_, Option<String>>(7)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(database_error("read linked failed decision lineage"))?;
                if failed_lineage.is_none()
                    && status != AgentRunStatus::Completed
                    && matches!(&resolution, EventResolution::RetryPolicy(_))
                {
                    let unbound_lineage = match (
                        event_campaign_id.as_deref(),
                        event_experiment_id.as_deref(),
                    ) {
                        (Some(campaign_id), Some(source_experiment_id)) => transaction
                            .query_row(
                                "SELECT da.cycle_id, dc.campaign_id,
                                        dc.source_experiment_id, da.attempt_number,
                                        da.created_at, c.state
                                 FROM decision_cycles dc
                                 JOIN campaigns c ON c.campaign_id = dc.campaign_id
                                 JOIN decision_attempts da ON da.cycle_id = dc.cycle_id
                                 WHERE c.project_id = ?1 AND dc.campaign_id = ?2
                                   AND dc.source_experiment_id = ?3
                                   AND dc.state IN ('analyzing','pending')
                                   AND da.agent_run_id IS NULL
                                   AND da.state IN ('reserved','evidence_ready')
                                 ORDER BY da.attempt_number DESC
                                 LIMIT 1",
                                params![project_id, campaign_id, source_experiment_id],
                                |row| {
                                    Ok((
                                        DecisionReservation {
                                            cycle_id: row.get(0)?,
                                            campaign_id: row.get(1)?,
                                            source_experiment_id: row.get(2)?,
                                            attempt_number: row.get(3)?,
                                            created_at: row.get(4)?,
                                        },
                                        row.get::<_, CampaignState>(5)?,
                                    ))
                                },
                            )
                            .optional()
                            .map_err(database_error(
                                "read linked unbound decision lineage",
                            ))?,
                        _ => None,
                    };
                    if let Some((reservation, campaign_state)) = unbound_lineage {
                        let canonical_dedup_key =
                            campaign_decision_dedup_key(&reservation.cycle_id);
                        if event_dedup_key != canonical_dedup_key {
                            return Err(AppError::Validation {
                                field: "campaign_decision_event",
                                message: "linked decision event does not match the unbound attempt lineage",
                            });
                        }
                        if campaign_state == CampaignState::Retired {
                            let failed_attempt = transaction
                                .execute(
                                    "UPDATE decision_attempts
                                     SET state = 'failed', failure_code = 'campaign_retired',
                                         failure_summary = 'campaign retired before decision bind cleanup',
                                         finished_at = ?1
                                     WHERE cycle_id = ?2 AND attempt_number = ?3
                                       AND agent_run_id IS NULL
                                       AND state IN ('reserved','evidence_ready')",
                                    params![
                                        finished_at,
                                        reservation.cycle_id,
                                        reservation.attempt_number,
                                    ],
                                )
                                .map_err(database_error(
                                    "fail retired unbound decision attempt",
                                ))?;
                            let degraded_cycle = transaction
                                .execute(
                                    "UPDATE decision_cycles
                                     SET state = 'degraded', next_wake_at = NULL,
                                         last_failure_code = 'campaign_retired',
                                         last_failure_summary =
                                             'campaign retired before decision bind cleanup',
                                         updated_at = ?1
                                     WHERE cycle_id = ?2 AND campaign_id = ?3
                                       AND source_experiment_id = ?4
                                       AND state IN ('analyzing','pending')",
                                    params![
                                        finished_at,
                                        reservation.cycle_id,
                                        reservation.campaign_id,
                                        reservation.source_experiment_id,
                                    ],
                                )
                                .map_err(database_error(
                                    "degrade retired unbound decision cycle",
                                ))?;
                            let dead_lettered = transaction
                                .execute(
                                    "UPDATE events
                                     SET status = 'dead_letter', lease_until = NULL,
                                         completed_at = ?1,
                                         attempts = CASE
                                             WHEN attempts > 0 THEN attempts - 1 ELSE 0 END
                                     WHERE project_id = ?2 AND dedup_key = ?3
                                       AND campaign_id = ?4 AND experiment_id = ?5
                                       AND kind = 'campaign_decision' AND status = ?6",
                                    params![
                                        finished_at,
                                        project_id,
                                        canonical_dedup_key,
                                        reservation.campaign_id,
                                        reservation.source_experiment_id,
                                        event_status,
                                    ],
                                )
                                .map_err(database_error(
                                    "dead-letter retired unbound decision event",
                                ))?;
                            if failed_attempt != 1 || degraded_cycle != 1 || dead_lettered != 1 {
                                return Err(AppError::Validation {
                                    field: "campaign_decision_event",
                                    message: "retired unbound decision lineage changed during finalization",
                                });
                            }
                        } else {
                            let retry_at = finished_at
                                .saturating_add(retry_backoff_seconds(attempts.max(1)));
                            if DecisionRepository::recover_unbound_attempt_event_in_transaction(
                                &transaction,
                                &reservation,
                                event_id,
                                finished_at,
                                retry_at,
                            )?
                            .is_none()
                            {
                                return Err(AppError::Validation {
                                    field: "campaign_decision_event",
                                    message: "unbound decision lineage changed during finalization",
                                });
                            }
                        }
                        continue;
                    }
                }
                if let Some((
                    cycle_id,
                    campaign_id,
                    source_experiment_id,
                    cycle_state,
                    campaign_state,
                    project_enabled,
                    project_paused,
                    project_halted_reason,
                )) = failed_lineage
                {
                    let canonical_dedup_key = campaign_decision_dedup_key(&cycle_id);
                    if event_dedup_key != canonical_dedup_key
                        || event_campaign_id.as_deref() != Some(campaign_id.as_str())
                        || event_experiment_id.as_deref()
                            != Some(source_experiment_id.as_str())
                    {
                        return Err(AppError::Validation {
                            field: "campaign_decision_event",
                            message: "linked decision event does not match the failed attempt lineage",
                        });
                    }
                    match &resolution {
                        EventResolution::RetryPolicy(_) => {
                            if !matches!(
                                cycle_state,
                                DecisionCycleState::Pending | DecisionCycleState::Degraded
                            ) {
                                return Err(AppError::Validation {
                                    field: "decision_cycle",
                                    message: "retryable failed decision must be pending or degraded",
                                });
                            }
                            if campaign_state == CampaignState::Retired
                                && cycle_state == DecisionCycleState::Pending
                            {
                                let degraded = transaction
                                    .execute(
                                        "UPDATE decision_cycles
                                         SET state = 'degraded', next_wake_at = NULL,
                                             last_failure_code = 'campaign_retired',
                                             last_failure_summary =
                                                 'campaign retired before decision retry',
                                             updated_at = ?1
                                         WHERE cycle_id = ?2 AND campaign_id = ?3
                                           AND source_experiment_id = ?4
                                           AND state = 'pending'",
                                        params![
                                            finished_at,
                                            cycle_id,
                                            campaign_id,
                                            source_experiment_id,
                                        ],
                                    )
                                    .map_err(database_error(
                                        "degrade retired retryable decision cycle",
                                    ))?;
                                if degraded != 1 {
                                    return Err(AppError::Validation {
                                        field: "decision_cycle",
                                        message: "retired decision cycle changed during finalization",
                                    });
                                }
                            }
                            if cycle_state == DecisionCycleState::Degraded
                                || campaign_state == CampaignState::Retired
                            {
                                let dead_lettered = transaction
                                    .execute(
                                        "UPDATE events
                                         SET status = 'dead_letter', lease_until = NULL,
                                             completed_at = ?1
                                         WHERE project_id = ?2 AND dedup_key = ?3
                                           AND campaign_id = ?4 AND experiment_id = ?5
                                           AND kind = 'campaign_decision' AND status = ?6",
                                        params![
                                            finished_at,
                                            project_id,
                                            canonical_dedup_key,
                                            campaign_id,
                                            source_experiment_id,
                                            event_status,
                                        ],
                                    )
                                    .map_err(database_error(
                                        "dead-letter exhausted decision event",
                                    ))?;
                                if dead_lettered != 1 {
                                    return Err(AppError::Validation {
                                        field: "campaign_decision_event",
                                        message: "exhausted decision event changed during finalization",
                                    });
                                }
                                continue;
                            }
                            let immediately_eligible = campaign_state == CampaignState::Active
                                && project_enabled
                                && !project_paused
                                && project_halted_reason.is_none();
                            let retry_at = finished_at
                                .saturating_add(retry_backoff_seconds(attempts.max(1)));
                            let requeued = if immediately_eligible {
                                transaction
                                    .execute(
                                        "UPDATE events
                                         SET status = 'pending', not_before = ?1,
                                             lease_until = NULL, completed_at = NULL,
                                             attempts = 0, last_error = NULL
                                         WHERE project_id = ?2 AND dedup_key = ?3
                                           AND campaign_id = ?4 AND experiment_id = ?5
                                           AND kind = 'campaign_decision' AND status = ?6",
                                        params![
                                            finished_at,
                                            project_id,
                                            canonical_dedup_key,
                                            campaign_id,
                                            source_experiment_id,
                                            event_status,
                                        ],
                                    )
                                    .map_err(database_error(
                                        "requeue linked retryable decision event",
                                    ))?
                            } else {
                                transaction
                                    .execute(
                                        "UPDATE events
                                         SET status = 'retry_wait', not_before = ?1,
                                             lease_until = NULL, completed_at = NULL,
                                             attempts = 0, last_error = NULL
                                         WHERE project_id = ?2 AND dedup_key = ?3
                                           AND campaign_id = ?4 AND experiment_id = ?5
                                           AND kind = 'campaign_decision' AND status = ?6",
                                        params![
                                            retry_at,
                                            project_id,
                                            canonical_dedup_key,
                                            campaign_id,
                                            source_experiment_id,
                                            event_status,
                                        ],
                                    )
                                    .map_err(database_error(
                                        "defer authority-gated retryable decision event",
                                    ))?
                            };
                            if requeued != 1 {
                                return Err(AppError::Validation {
                                    field: "campaign_decision_event",
                                    message: "retryable decision event changed during finalization",
                                });
                            }
                        }
                        EventResolution::ExecutionUnknown { .. }
                        | EventResolution::PolicyBlocked { .. } => {
                            if cycle_state == crate::models::DecisionCycleState::Pending {
                                let (failure_code, campaign_reason) = match &resolution {
                                    EventResolution::ExecutionUnknown { .. } => (
                                        "execution_unknown".to_owned(),
                                        "decision_execution_unknown",
                                    ),
                                    EventResolution::PolicyBlocked { code, .. } => (
                                        format!("policy_blocked:{}", code.as_str()),
                                        "decision_policy_blocked",
                                    ),
                                    EventResolution::RetryPolicy(_) => unreachable!(),
                                };
                                let degraded = transaction
                                    .execute(
                                        "UPDATE decision_cycles
                                         SET state = 'degraded', next_wake_at = NULL,
                                             last_failure_code = ?1,
                                             last_failure_summary = ?2, updated_at = ?3
                                         WHERE cycle_id = ?4 AND campaign_id = ?5
                                           AND source_experiment_id = ?6
                                           AND state = 'pending'",
                                        params![
                                            failure_code,
                                            bounded_event_error.as_deref(),
                                            finished_at,
                                            cycle_id,
                                            campaign_id,
                                            source_experiment_id,
                                        ],
                                    )
                                    .map_err(database_error(
                                        "degrade terminally resolved decision cycle",
                                    ))?;
                                if degraded != 1 {
                                    return Err(AppError::Validation {
                                        field: "decision_cycle",
                                        message: "terminal decision cycle changed during finalization",
                                    });
                                }
                                transaction
                                    .execute(
                                        "UPDATE campaigns
                                         SET state = 'degraded', state_reason = ?1,
                                             next_eligible_at = NULL, updated_at = ?2
                                         WHERE campaign_id = ?3 AND project_id = ?4
                                           AND state IN ('active','budget_waiting',
                                                         'goal_reached_pending_review')",
                                        params![
                                            campaign_reason,
                                            finished_at,
                                            campaign_id,
                                            project_id,
                                        ],
                                    )
                                    .map_err(database_error(
                                        "degrade terminally resolved decision campaign",
                                    ))?;
                            }
                        }
                    }
                }
            }
        }

        let changed = transaction
            .execute(
                "UPDATE agent_runs
                 SET status = ?1, finished_at = ?2, exit_code = ?3, last_error = ?4,
                     policy_code = COALESCE(?5, policy_code),
                     failure_stage = COALESCE(?6, failure_stage),
                     launch_gate_state = CASE
                         WHEN launch_gate_state IN ('pending', 'release_requested')
                              AND ?1 = 'failed' THEN 'failed'
                         WHEN launch_gate_state IN ('pending', 'release_requested')
                              AND ?1 IN ('completed', 'timed_out', 'cancelled') THEN 'released'
                         ELSE launch_gate_state
                     END
                 WHERE project_id = ?7 AND run_id = ?8
                   AND status IN ('starting', 'running')",
                params![
                    status,
                    finished_at,
                    exit_code,
                    bounded_run_error.as_deref(),
                    policy_code,
                    failure_stage,
                    project_id,
                    run_id,
                ],
            )
            .map_err(database_error("finish agent run with event resolution"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "finish agent run with event resolution",
            });
        }
        let intervention_statuses = if reset_applied_interventions {
            "status IN ('reserved', 'applied')"
        } else {
            "status = 'reserved'"
        };
        transaction
            .execute(
                &format!(
                    "UPDATE interventions
                     SET status = 'pending', reserved_at = NULL, applied_at = NULL,
                         agent_run_id = NULL, lease_expires_at = NULL, reservation_token = NULL
                     WHERE project_id = ?1 AND agent_run_id = ?2 AND {intervention_statuses}"
                ),
                params![project_id, run_id],
            )
            .map_err(database_error("release interventions after agent run finalization"))?;
        let finalized_run = read_agent_run_in_transaction(&transaction, run_id)?;
        transaction
            .commit()
            .map_err(database_error("commit agent run event finalization"))?;
        Ok(finalized_run)
    }

    pub fn finish(
        &self,
        run_id: i64,
        status: AgentRunStatus,
        finished_at: i64,
        exit_code: Option<i64>,
        last_error: Option<&str>,
    ) -> Result<AgentRun, AppError> {
        let connection = self.db.connect()?;
        connection
            .execute(
                "UPDATE agent_runs
                 SET status = ?1, finished_at = ?2, exit_code = ?3, last_error = ?4
                 WHERE run_id = ?5",
                params![status, finished_at, exit_code, last_error, run_id],
            )
            .map_err(database_error("finish agent run"))?;
        read_agent_run(&connection, run_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn finish_and_release_interventions(
        &self,
        project_id: &str,
        run_id: i64,
        status: AgentRunStatus,
        finished_at: i64,
        exit_code: Option<i64>,
        last_error: Option<&str>,
    ) -> Result<AgentRun, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin agent run finish and intervention release",
            ))?;
        let changed = transaction
            .execute(
                "UPDATE agent_runs
                 SET status = ?1, finished_at = ?2, exit_code = ?3, last_error = ?4,
                     launch_gate_state = CASE
                         WHEN launch_gate_state IN ('pending', 'release_requested')
                              AND ?1 = 'failed' THEN 'failed'
                         WHEN launch_gate_state IN ('pending', 'release_requested')
                              AND ?1 IN ('completed', 'timed_out', 'cancelled') THEN 'released'
                         ELSE launch_gate_state
                     END
                 WHERE project_id = ?5 AND run_id = ?6",
                params![
                    status,
                    finished_at,
                    exit_code,
                    last_error,
                    project_id,
                    run_id,
                ],
            )
            .map_err(database_error("finish project agent run"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "finish project agent run",
            });
        }
        transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, reserved_at = NULL, applied_at = NULL,
                     agent_run_id = NULL, lease_expires_at = NULL, reservation_token = NULL
                 WHERE project_id = ?2 AND agent_run_id = ?3 AND status = ?4",
                params![
                    InterventionStatus::Pending,
                    project_id,
                    run_id,
                    InterventionStatus::Reserved,
                ],
            )
            .map_err(database_error(
                "release interventions for finished agent run",
            ))?;
        transaction.commit().map_err(database_error(
            "commit agent run finish and intervention release",
        ))?;
        read_agent_run(&connection, run_id)
    }

    pub fn count_by_project(&self, project_id: &str) -> Result<u32, AppError> {
        let connection = self.db.connect()?;
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE project_id = ?1",
                [project_id],
                |row| row.get(0),
            )
            .map_err(database_error("count agent runs"))?;
        u32::try_from(count).map_err(|_| AppError::Runtime {
            operation: "count agent runs",
        })
    }

    /// Count policy failure evidence by its bounded code and stage for one
    /// project. Rows with malformed legacy/corrupt text are excluded rather
    /// than rendered as operator diagnostics.
    pub fn policy_failure_stage_counts(
        &self,
        project_id: &str,
    ) -> Result<BTreeMap<(String, String), i64>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT policy_code, failure_stage, COUNT(*)
                 FROM agent_runs
                 WHERE project_id = ?1
                   AND policy_code IN (
                       'policy_missing', 'policy_unreadable', 'policy_weak_permissions',
                       'policy_unknown_field', 'trusted_path_unsafe', 'anchor_missing',
                       'anchor_replaced', 'custom_agent_not_enrolled', 'project_root_executable',
                       'unsafe_codex_argument', 'network_override', 'environment_name',
                       'session_missing', 'session_not_owned', 'root_changed', 'agent_log_unsafe',
                       'temp_unsafe', 'setsid_failed', 'native_gate_failed', 'unsupported_platform'
                   )
                   AND failure_stage IN (
                       'startup', 'pre_binding', 'run_bound_pre_marker', 'native_gate',
                       'post_marker', 'dispatched', 'finalized'
                   )
                 GROUP BY policy_code, failure_stage
                 ORDER BY policy_code, failure_stage",
            )
            .map_err(database_error("prepare policy failure stage count query"))?;
        let rows = statement
            .query_map([project_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(database_error("query policy failure stage counts"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read policy failure stage counts"))?;
        Ok(rows
            .into_iter()
            .map(|(code, stage, count)| ((code, stage), count))
            .collect())
    }

    pub fn list_by_project(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<Vec<AgentRun>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                 ORDER BY started_at DESC, run_id DESC
                 LIMIT ?2",
                AGENT_RUN_SELECT
            ))
            .map_err(database_error("prepare project agent run query"))?;
        let rows = statement
            .query_map(
                params![project_id, bounded_diagnostic_limit(limit)],
                agent_run_from_row,
            )
            .map_err(database_error("query project agent runs"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read project agent runs"))
    }

    pub fn find_by_event(
        &self,
        project_id: &str,
        event_id: i64,
        limit: usize,
    ) -> Result<Vec<AgentRun>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT agent_runs.run_id, agent_runs.project_id, agent_runs.primary_event_id,
                        agent_runs.pid, agent_runs.status, agent_runs.started_at,
                        agent_runs.finished_at, agent_runs.exit_code, agent_runs.log_path,
                        agent_runs.last_error, agent_runs.launch_gate_state,
                        agent_runs.context_mode, agent_runs.context_session_id,
                        agent_runs.context_lineage_json,
                        agent_runs.execution_kind, agent_runs.executable_path,
                        agent_runs.executable_identity, agent_runs.policy_code,
                        agent_runs.failure_stage
                 FROM agent_runs
                 JOIN agent_run_events
                   ON agent_run_events.project_id = agent_runs.project_id
                  AND agent_run_events.run_id = agent_runs.run_id
                 WHERE agent_runs.project_id = ?1 AND agent_run_events.event_id = ?2
                 ORDER BY agent_runs.started_at DESC, agent_runs.run_id DESC
                 LIMIT ?3",
            )
            .map_err(database_error("prepare event agent run query"))?;
        let rows = statement
            .query_map(
                params![project_id, event_id, bounded_diagnostic_limit(limit)],
                agent_run_from_row,
            )
            .map_err(database_error("query event agent runs"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read event agent runs"))
    }
}

pub struct TerminationRequestRepository<'db> {
    db: &'db Db,
}

impl<'db> TerminationRequestRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn insert_idempotent(
        &self,
        request: &NewTerminationRequest,
    ) -> Result<TerminationRequest, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin idempotent termination request insert",
            ))?;
        transaction
            .execute(
                "INSERT INTO termination_requests (
                    incident_id, project_id, task_signature, reason, status,
                    requested_at, grace_until, confirmed_at, last_error
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL)
                 ON CONFLICT(project_id, incident_id, task_signature) DO NOTHING",
                params![
                    request.incident_id,
                    request.project_id,
                    request.task_signature,
                    request.reason,
                    request.status,
                    request.requested_at,
                    request.grace_until,
                ],
            )
            .map_err(database_error("insert termination request"))?;
        let stored = transaction
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1 AND incident_id = ?2 AND task_signature = ?3",
                    TERMINATION_REQUEST_SELECT
                ),
                params![
                    request.project_id,
                    request.incident_id,
                    request.task_signature,
                ],
                termination_request_from_row,
            )
            .map_err(database_error("read idempotent termination request"))?;
        transaction.commit().map_err(database_error(
            "commit idempotent termination request insert",
        ))?;
        Ok(stored)
    }

    pub fn find_by_id(&self, request_id: i64) -> Result<Option<TerminationRequest>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{} WHERE request_id = ?1", TERMINATION_REQUEST_SELECT),
                [request_id],
                termination_request_from_row,
            )
            .optional()
            .map_err(database_error("find termination request by ID"))
    }

    pub fn transition_status(
        &self,
        request_id: i64,
        status: TerminationRequestStatus,
    ) -> Result<TerminationRequest, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin termination request status transition",
            ))?;
        transaction
            .execute(
                "UPDATE termination_requests SET status = ?1 WHERE request_id = ?2",
                params![status, request_id],
            )
            .map_err(database_error("transition termination request status"))?;
        let stored = read_termination_request(&transaction, request_id)?;
        transaction.commit().map_err(database_error(
            "commit termination request status transition",
        ))?;
        Ok(stored)
    }

    pub fn transition_status_if_current(
        &self,
        request_id: i64,
        current: TerminationRequestStatus,
        next: TerminationRequestStatus,
    ) -> Result<Option<TerminationRequest>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin guarded termination request status transition",
            ))?;
        let changed = transaction
            .execute(
                "UPDATE termination_requests
                 SET status = ?1, dispatch_lease_until = NULL
                 WHERE request_id = ?2 AND status = ?3",
                params![next, request_id, current],
            )
            .map_err(database_error(
                "guarded transition termination request status",
            ))?;
        let stored = if changed == 0 {
            None
        } else {
            Some(read_termination_request(&transaction, request_id)?)
        };
        transaction.commit().map_err(database_error(
            "commit guarded termination request status transition",
        ))?;
        Ok(stored)
    }

    pub fn claim_for_dispatch(
        &self,
        request_id: i64,
        now: i64,
        lease_until: i64,
    ) -> Result<Option<TerminationRequest>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin termination request dispatch claim"))?;
        let changed = transaction
            .execute(
                "UPDATE termination_requests
                 SET status = ?1, dispatch_lease_until = ?2
                 WHERE request_id = ?3
                   AND (
                       status = ?4
                       OR (
                           status = ?5
                           AND (
                               dispatch_lease_until IS NULL
                               OR dispatch_lease_until <= ?6
                           )
                       )
                   )",
                params![
                    TerminationRequestStatus::Dispatching,
                    lease_until,
                    request_id,
                    TerminationRequestStatus::Requested,
                    TerminationRequestStatus::Dispatching,
                    now,
                ],
            )
            .map_err(database_error("claim termination request dispatch"))?;
        let stored = if changed == 0 {
            None
        } else {
            Some(read_termination_request(&transaction, request_id)?)
        };
        transaction
            .commit()
            .map_err(database_error("commit termination request dispatch claim"))?;
        Ok(stored)
    }

    pub fn update_result(
        &self,
        request_id: i64,
        status: TerminationRequestStatus,
        confirmed_at: Option<i64>,
        last_error: Option<&str>,
    ) -> Result<TerminationRequest, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin termination request result update"))?;
        transaction
            .execute(
                "UPDATE termination_requests
                 SET status = ?1, dispatch_lease_until = NULL,
                     confirmed_at = ?2, last_error = ?3
                 WHERE request_id = ?4",
                params![status, confirmed_at, last_error, request_id],
            )
            .map_err(database_error("update termination request result"))?;
        let stored = read_termination_request(&transaction, request_id)?;
        transaction
            .commit()
            .map_err(database_error("commit termination request result update"))?;
        Ok(stored)
    }

    pub fn update_result_if_current(
        &self,
        request_id: i64,
        current: TerminationRequestStatus,
        next: TerminationRequestStatus,
        confirmed_at: Option<i64>,
        last_error: Option<&str>,
    ) -> Result<Option<TerminationRequest>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin guarded termination request result update",
            ))?;
        let changed = transaction
            .execute(
                "UPDATE termination_requests
                 SET status = ?1, dispatch_lease_until = NULL,
                     confirmed_at = ?2, last_error = ?3
                 WHERE request_id = ?4 AND status = ?5",
                params![next, confirmed_at, last_error, request_id, current],
            )
            .map_err(database_error("guarded update termination request result"))?;
        let stored = if changed == 0 {
            None
        } else {
            Some(read_termination_request(&transaction, request_id)?)
        };
        transaction.commit().map_err(database_error(
            "commit guarded termination request result update",
        ))?;
        Ok(stored)
    }

    pub fn set_grace_until_if_current(
        &self,
        request_id: i64,
        current: TerminationRequestStatus,
        grace_until: i64,
    ) -> Result<Option<TerminationRequest>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin guarded termination request grace update",
            ))?;
        let changed = transaction
            .execute(
                "UPDATE termination_requests
                 SET grace_until = ?1
                 WHERE request_id = ?2 AND status = ?3 AND grace_until IS NULL",
                params![grace_until, request_id, current],
            )
            .map_err(database_error("guarded update termination request grace"))?;
        let stored = if changed == 0 {
            None
        } else {
            Some(read_termination_request(&transaction, request_id)?)
        };
        transaction.commit().map_err(database_error(
            "commit guarded termination request grace update",
        ))?;
        Ok(stored)
    }

    pub fn mark_dispatched_if_current(
        &self,
        request_id: i64,
        dispatch_lease_until: i64,
        grace_until: i64,
    ) -> Result<Option<TerminationRequest>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin termination request dispatch confirmation",
            ))?;
        let changed = transaction
            .execute(
                "UPDATE termination_requests
                 SET status = ?1, dispatch_lease_until = NULL, grace_until = ?2
                 WHERE request_id = ?3 AND status = ?4 AND dispatch_lease_until = ?5",
                params![
                    TerminationRequestStatus::Sent,
                    grace_until,
                    request_id,
                    TerminationRequestStatus::Dispatching,
                    dispatch_lease_until,
                ],
            )
            .map_err(database_error("mark termination request as dispatched"))?;
        let stored = if changed == 0 {
            None
        } else {
            Some(read_termination_request(&transaction, request_id)?)
        };
        transaction.commit().map_err(database_error(
            "commit termination request dispatch confirmation",
        ))?;
        Ok(stored)
    }

    pub fn finish_dispatch_if_current(
        &self,
        request_id: i64,
        dispatch_lease_until: i64,
        status: TerminationRequestStatus,
        last_error: Option<&str>,
    ) -> Result<Option<TerminationRequest>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin termination dispatch result update"))?;
        let changed = transaction
            .execute(
                "UPDATE termination_requests
                 SET status = ?1, dispatch_lease_until = NULL,
                     confirmed_at = NULL, last_error = ?2
                 WHERE request_id = ?3 AND status = ?4 AND dispatch_lease_until = ?5",
                params![
                    status,
                    last_error,
                    request_id,
                    TerminationRequestStatus::Dispatching,
                    dispatch_lease_until,
                ],
            )
            .map_err(database_error("update termination dispatch result"))?;
        let stored = if changed == 0 {
            None
        } else {
            Some(read_termination_request(&transaction, request_id)?)
        };
        transaction
            .commit()
            .map_err(database_error("commit termination dispatch result update"))?;
        Ok(stored)
    }

    pub fn find_pending(&self, project_id: &str) -> Result<Vec<TerminationRequest>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND status IN (?2, ?3, ?4)
                 ORDER BY requested_at, request_id",
                TERMINATION_REQUEST_SELECT
            ))
            .map_err(database_error("prepare pending termination request query"))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    TerminationRequestStatus::Requested,
                    TerminationRequestStatus::Dispatching,
                    TerminationRequestStatus::Sent,
                ],
                termination_request_from_row,
            )
            .map_err(database_error("find pending termination requests"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read pending termination requests"))
    }

    pub fn find_by_project(&self, project_id: &str) -> Result<Vec<TerminationRequest>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 ORDER BY requested_at, request_id",
                TERMINATION_REQUEST_SELECT
            ))
            .map_err(database_error("prepare project termination request query"))?;
        let rows = statement
            .query_map([project_id], termination_request_from_row)
            .map_err(database_error("find project termination requests"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read project termination requests"))
    }

    pub fn list_by_project(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<Vec<TerminationRequest>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                 ORDER BY requested_at DESC, request_id DESC
                 LIMIT ?2",
                TERMINATION_REQUEST_SELECT
            ))
            .map_err(database_error(
                "prepare bounded project termination request query",
            ))?;
        let rows = statement
            .query_map(
                params![project_id, bounded_diagnostic_limit(limit)],
                termination_request_from_row,
            )
            .map_err(database_error("query bounded project termination requests"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read bounded project termination requests"))
    }

    pub fn find_by_task_signature(
        &self,
        project_id: &str,
        task_signature: &str,
        limit: usize,
    ) -> Result<Vec<TerminationRequest>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND task_signature = ?2
                 ORDER BY requested_at DESC, request_id DESC
                 LIMIT ?3",
                TERMINATION_REQUEST_SELECT
            ))
            .map_err(database_error("prepare task termination request query"))?;
        let rows = statement
            .query_map(
                params![project_id, task_signature, bounded_diagnostic_limit(limit)],
                termination_request_from_row,
            )
            .map_err(database_error("query task termination requests"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read task termination requests"))
    }
}

pub struct InterventionRepository<'db> {
    db: &'db Db,
}

impl<'db> InterventionRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn insert_pending(
        &self,
        project_id: &str,
        message: &str,
        created_at: i64,
    ) -> Result<Intervention, AppError> {
        validate_message(message)?;
        let intervention_id = Uuid::new_v4().to_string();
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin intervention insert"))?;
        let insertion_sequence: i64 = transaction
            .query_row(
                "SELECT COALESCE(MAX(insertion_sequence), 0) + 1
                 FROM interventions WHERE project_id = ?1",
                [project_id],
                |row| row.get(0),
            )
            .map_err(database_error("allocate intervention insertion sequence"))?;
        transaction
            .execute(
                "INSERT INTO interventions (
                    intervention_id, project_id, insertion_sequence, message, status, created_at, reserved_at,
                    applied_at, agent_run_id, attempts, lease_expires_at, reservation_token
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, NULL, 0, NULL, NULL)",
                params![
                    intervention_id,
                    project_id,
                    insertion_sequence,
                    message,
                    InterventionStatus::Pending,
                    created_at,
                ],
            )
            .map_err(database_error("insert intervention"))?;
        let intervention = transaction
            .query_row(
                &format!(
                    "{} WHERE intervention_id = ?1 AND project_id = ?2",
                    INTERVENTION_SELECT
                ),
                params![intervention_id, project_id],
                intervention_from_row,
            )
            .map_err(database_error("read inserted intervention"))?;
        transaction
            .commit()
            .map_err(database_error("commit intervention insert"))?;
        Ok(intervention)
    }

    pub fn list(
        &self,
        project_id: &str,
        status: InterventionStatus,
        limit: usize,
    ) -> Result<Vec<Intervention>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND status = ?2
                 ORDER BY insertion_sequence ASC, created_at ASC, intervention_id ASC
                 LIMIT ?3",
                INTERVENTION_SELECT
            ))
            .map_err(database_error("prepare intervention list"))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    status,
                    limit.min(MAX_INTERVENTIONS_PER_RUN) as i64,
                ],
                intervention_from_row,
            )
            .map_err(database_error("query interventions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read interventions"))
    }

    pub fn count_by_project(&self, project_id: &str) -> Result<InterventionCounts, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT
                    COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN status = 'reserved' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END), 0)
                 FROM interventions
                 WHERE project_id = ?1",
                [project_id],
                |row| {
                    Ok(InterventionCounts {
                        pending: row.get(0)?,
                        reserved: row.get(1)?,
                        applied: row.get(2)?,
                    })
                },
            )
            .map_err(database_error("count project interventions"))
    }

    pub fn reserve_pending(
        &self,
        project_id: &str,
        token: &str,
        now: i64,
        lease_until: i64,
        max_count: usize,
        max_bytes: usize,
    ) -> Result<InterventionReservation, AppError> {
        if lease_until <= now {
            return Err(AppError::Configuration {
                field: "intervention_lease",
            });
        }
        let max_count = max_count.min(MAX_INTERVENTIONS_PER_RUN);
        let max_bytes = max_bytes.min(MAX_INTERVENTION_BYTES_PER_RUN);
        if max_count == 0 || max_bytes == 0 {
            return Ok(InterventionReservation {
                token: token.to_owned(),
                items: Vec::new(),
            });
        }

        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin immediate intervention reservation"))?;
        let pending = {
            let mut statement = transaction
                .prepare(&format!(
                    "{} WHERE project_id = ?1 AND status = ?2
                     ORDER BY insertion_sequence ASC, created_at ASC, intervention_id ASC
                     LIMIT ?3",
                    INTERVENTION_SELECT
                ))
                .map_err(database_error("prepare pending intervention reservation"))?;
            let pending = statement
                .query_map(
                    params![project_id, InterventionStatus::Pending, max_count as i64],
                    intervention_from_row,
                )
                .map_err(database_error("query pending interventions"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read pending interventions"))?;
            pending
        };

        let mut total_bytes = 0;
        let mut items = Vec::with_capacity(pending.len());
        for intervention in pending {
            let next_total = total_bytes + intervention.message.len();
            if next_total > max_bytes {
                break;
            }
            let changed = transaction
                .execute(
                    "UPDATE interventions
                     SET status = ?1, reserved_at = ?2, attempts = attempts + 1,
                         lease_expires_at = ?3, reservation_token = ?4
                     WHERE intervention_id = ?5 AND project_id = ?6 AND status = ?7",
                    params![
                        InterventionStatus::Reserved,
                        now,
                        lease_until,
                        token,
                        intervention.intervention_id,
                        project_id,
                        InterventionStatus::Pending,
                    ],
                )
                .map_err(database_error("reserve intervention"))?;
            if changed == 1 {
                total_bytes = next_total;
                items.push(
                    transaction
                        .query_row(
                            &format!(
                                "{} WHERE intervention_id = ?1 AND project_id = ?2",
                                INTERVENTION_SELECT
                            ),
                            params![intervention.intervention_id, project_id],
                            intervention_from_row,
                        )
                        .map_err(database_error("read reserved intervention"))?,
                );
            }
        }
        transaction
            .commit()
            .map_err(database_error("commit intervention reservation"))?;
        Ok(InterventionReservation {
            token: token.to_owned(),
            items,
        })
    }

    pub fn mark_applied_for_run(
        &self,
        project_id: &str,
        run_id: i64,
        applied_at: i64,
    ) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin intervention application"))?;
        let changed = transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, applied_at = ?2, lease_expires_at = NULL,
                     reservation_token = NULL
                 WHERE project_id = ?3 AND agent_run_id = ?4 AND status = ?5",
                params![
                    InterventionStatus::Applied,
                    applied_at,
                    project_id,
                    run_id,
                    InterventionStatus::Reserved,
                ],
            )
            .map_err(database_error("mark interventions applied for run"))?;
        transaction
            .commit()
            .map_err(database_error("commit intervention application"))?;
        Ok(changed)
    }

    pub fn release_for_run(&self, project_id: &str, run_id: i64) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin intervention release"))?;
        let changed = transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, reserved_at = NULL, applied_at = NULL, agent_run_id = NULL,
                     lease_expires_at = NULL, reservation_token = NULL
                 WHERE project_id = ?2 AND agent_run_id = ?3 AND status = ?4",
                params![
                    InterventionStatus::Pending,
                    project_id,
                    run_id,
                    InterventionStatus::Reserved,
                ],
            )
            .map_err(database_error("release interventions for run"))?;
        transaction
            .commit()
            .map_err(database_error("commit intervention release"))?;
        Ok(changed)
    }

    pub fn release_reservation(
        &self,
        project_id: &str,
        reservation_token: &str,
    ) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin intervention reservation release"))?;
        let changed = transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, reserved_at = NULL, applied_at = NULL, agent_run_id = NULL,
                     lease_expires_at = NULL, reservation_token = NULL
                 WHERE project_id = ?2 AND reservation_token = ?3 AND status = ?4",
                params![
                    InterventionStatus::Pending,
                    project_id,
                    reservation_token,
                    InterventionStatus::Reserved,
                ],
            )
            .map_err(database_error("release intervention reservation"))?;
        transaction
            .commit()
            .map_err(database_error("commit intervention reservation release"))?;
        Ok(changed)
    }

    pub fn recover_expired(&self, now: i64) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin expired intervention recovery"))?;
        let changed = transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, reserved_at = NULL, applied_at = NULL, agent_run_id = NULL,
                     lease_expires_at = NULL, reservation_token = NULL
                 WHERE status = ?2 AND lease_expires_at <= ?3",
                params![
                    InterventionStatus::Pending,
                    InterventionStatus::Reserved,
                    now,
                ],
            )
            .map_err(database_error("recover expired interventions"))?;
        transaction
            .commit()
            .map_err(database_error("commit expired intervention recovery"))?;
        Ok(changed)
    }

    pub fn recover_expired_unattached(&self, now: i64) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin expired unattached intervention recovery",
            ))?;
        let changed = transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, reserved_at = NULL, applied_at = NULL, agent_run_id = NULL,
                     lease_expires_at = NULL, reservation_token = NULL
                 WHERE status = ?2 AND agent_run_id IS NULL AND lease_expires_at <= ?3",
                params![
                    InterventionStatus::Pending,
                    InterventionStatus::Reserved,
                    now,
                ],
            )
            .map_err(database_error("recover expired unattached interventions"))?;
        transaction.commit().map_err(database_error(
            "commit expired unattached intervention recovery",
        ))?;
        Ok(changed)
    }
}

pub struct TaskObservationRepository<'db> {
    db: &'db Db,
}

impl<'db> TaskObservationRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn upsert(&self, observation: &NewTaskObservation) -> Result<TaskObservation, AppError> {
        let command_json = serde_json::to_string(&observation.command).map_err(|source| {
            AppError::Serialization {
                operation: "serialize task observation command",
                source,
            }
        })?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin task observation upsert"))?;
        transaction
            .execute(
                "INSERT INTO task_observations (
                    project_id, task_signature, pueue_task_id, pueue_group, command_json,
                    state, enqueued_at, started_at, ended_at, result, first_observed_at,
                    observed_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                 ON CONFLICT(project_id, task_signature) DO UPDATE SET
                    pueue_task_id = excluded.pueue_task_id,
                    pueue_group = excluded.pueue_group,
                    command_json = excluded.command_json,
                    state = excluded.state,
                    enqueued_at = excluded.enqueued_at,
                    started_at = excluded.started_at,
                    ended_at = excluded.ended_at,
                    result = excluded.result,
                    observed_at = excluded.observed_at",
                params![
                    observation.project_id,
                    observation.task_signature,
                    observation.pueue_task_id,
                    observation.pueue_group,
                    command_json,
                    observation.state,
                    observation.enqueued_at,
                    observation.started_at,
                    observation.ended_at,
                    observation.result,
                    observation.observed_at,
                    observation.observed_at,
                ],
            )
            .map_err(database_error("upsert task observation"))?;
        let stored = transaction
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1 AND task_signature = ?2",
                    TASK_OBSERVATION_SELECT
                ),
                params![observation.project_id, observation.task_signature],
                task_observation_from_row,
            )
            .map_err(database_error("read upserted task observation"))?;
        transaction
            .commit()
            .map_err(database_error("commit task observation upsert"))?;
        Ok(stored)
    }

    pub fn find(
        &self,
        project_id: &str,
        task_signature: &str,
    ) -> Result<Option<TaskObservation>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1 AND task_signature = ?2",
                    TASK_OBSERVATION_SELECT
                ),
                params![project_id, task_signature],
                task_observation_from_row,
            )
            .optional()
            .map_err(database_error("find task observation"))
    }

    pub fn first_observed_at(
        &self,
        project_id: &str,
        task_signature: &str,
    ) -> Result<Option<i64>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT first_observed_at FROM task_observations
                 WHERE project_id = ?1 AND task_signature = ?2",
                params![project_id, task_signature],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("find first task observation time"))
    }

    pub fn find_by_pueue_task(
        &self,
        project_id: &str,
        pueue_task_id: i64,
        limit: usize,
    ) -> Result<Vec<TaskObservation>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND pueue_task_id = ?2
                 ORDER BY observed_at DESC, task_signature DESC
                 LIMIT ?3",
                TASK_OBSERVATION_SELECT
            ))
            .map_err(database_error("prepare Pueue task observation query"))?;
        let rows = statement
            .query_map(
                params![project_id, pueue_task_id, bounded_diagnostic_limit(limit)],
                task_observation_from_row,
            )
            .map_err(database_error("query Pueue task observations"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read Pueue task observations"))
    }
}

pub(crate) fn insert_event_idempotent_in_transaction(
    transaction: &Transaction<'_>,
    event: &NewEvent,
) -> Result<(Event, bool), AppError> {
    let payload_json =
        serde_json::to_string(&event.payload).map_err(|source| AppError::Serialization {
            operation: "serialize event payload",
            source,
        })?;
    validate_event_lineage(transaction, event)?;
    let inserted = transaction
        .execute(
            "INSERT INTO events (
                project_id, campaign_id, experiment_id, kind, dedup_key, payload_json, status, attempts,
                not_before, lease_until, created_at, completed_at, last_error
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', 0, ?7, NULL, ?8, NULL, NULL)
             ON CONFLICT(project_id, dedup_key) DO NOTHING",
            params![
                event.project_id,
                event.campaign_id,
                event.experiment_id,
                event.kind,
                event.dedup_key,
                payload_json,
                event.not_before,
                event.created_at,
            ],
        )
        .map_err(database_error("insert event"))?;
    let stored = transaction
        .query_row(
            &format!("{} WHERE project_id = ?1 AND dedup_key = ?2", EVENT_SELECT),
            params![event.project_id, event.dedup_key],
            event_from_row,
        )
        .map_err(database_error("read idempotent event"))?;
    if stored.campaign_id != event.campaign_id || stored.experiment_id != event.experiment_id {
        return Err(AppError::Validation {
            field: "event.lineage",
            message: "conflicts with the existing event lineage",
        });
    }
    Ok((stored, inserted == 1))
}

/// Insert an event directly as 'completed' in the same transaction (passive audit).
/// This is idempotent via the dedup_key and does not create a pending event
/// that could be dispatched by a scheduler.
pub(crate) fn insert_event_completed_in_transaction(
    transaction: &Transaction<'_>,
    event: &NewEvent,
) -> Result<(Event, bool), AppError> {
    let payload_json =
        serde_json::to_string(&event.payload).map_err(|source| AppError::Serialization {
            operation: "serialize event payload",
            source,
        })?;
    validate_event_lineage(transaction, event)?;
    let inserted = transaction
        .execute(
            "INSERT INTO events (
                project_id, campaign_id, experiment_id, kind, dedup_key, payload_json, status, attempts,
                not_before, lease_until, created_at, completed_at, last_error
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'completed', 0, ?7, NULL, ?8, ?8, NULL)
             ON CONFLICT(project_id, dedup_key) DO NOTHING",
            params![
                event.project_id,
                event.campaign_id,
                event.experiment_id,
                event.kind,
                event.dedup_key,
                payload_json,
                event.not_before,
                event.created_at,
            ],
        )
        .map_err(database_error("insert completed event"))?;
    let mut stored = transaction
        .query_row(
            &format!("{} WHERE project_id = ?1 AND dedup_key = ?2", EVENT_SELECT),
            params![event.project_id, event.dedup_key],
            event_from_row,
        )
        .map_err(database_error("read idempotent completed event"))?;
    if stored.kind != event.kind {
        return Err(AppError::Validation {
            field: "event.kind",
            message: "conflicts with the existing event kind",
        });
    }
    if stored.campaign_id != event.campaign_id || stored.experiment_id != event.experiment_id {
        return Err(AppError::Validation {
            field: "event.lineage",
            message: "conflicts with the existing event lineage",
        });
    }
    if stored.payload != event.payload {
        return Err(AppError::Validation {
            field: "event.payload",
            message: "conflicts with the existing event payload",
        });
    }
    match stored.status {
        EventStatus::Completed => {}
        EventStatus::Pending => {
            let changed = transaction
                .execute(
                    "UPDATE events
                     SET status = 'completed', lease_until = NULL,
                         completed_at = ?1, last_error = NULL
                     WHERE event_id = ?2 AND status = 'pending'",
                    params![event.created_at, stored.event_id],
                )
                .map_err(database_error("complete legacy promotion event"))?;
            if changed != 1 {
                return Err(AppError::Runtime {
                    operation: "complete legacy promotion event",
                });
            }
            stored = transaction
                .query_row(
                    &format!("{} WHERE project_id = ?1 AND dedup_key = ?2", EVENT_SELECT),
                    params![event.project_id, event.dedup_key],
                    event_from_row,
                )
                .map_err(database_error("read completed legacy promotion event"))?;
        }
        _ => {
            return Err(AppError::Validation {
                field: "event.status",
                message: "conflicts with the existing event status",
            });
        }
    }
    Ok((stored, inserted == 1))
}

const EVENT_SELECT: &str =
    "SELECT event_id, project_id, campaign_id, experiment_id, kind, dedup_key, payload_json, status, attempts,
            not_before, lease_until, created_at, completed_at, last_error
     FROM events";

const INTEGRATION_EVENT_SELECT: &str =
    "SELECT integration_event_id, kind, dedup_key, payload_json, created_at
     FROM integration_events";

const INCIDENT_SELECT: &str = "SELECT incident_id, project_id, kind, task_key, fingerprint, status,
            first_seen_at, last_seen_at, acknowledged_at, resolved_at
     FROM incidents";

const SUBMISSION_SELECT: &str = "SELECT submission_id, project_id, argv_json, created_at,
            pueue_task_id, task_signature, status, kind, metadata_json, origin_agent_run_id
     FROM submissions";

const AGENT_RUN_SELECT: &str = "SELECT run_id, project_id, primary_event_id, pid, status,
            started_at, finished_at, exit_code, log_path, last_error,
            launch_gate_state, context_mode, context_session_id, context_lineage_json,
            execution_kind, executable_path, executable_identity, policy_code, failure_stage
     FROM agent_runs";

const TERMINATION_REQUEST_SELECT: &str = "SELECT request_id, incident_id, project_id,
            task_signature, reason, status, requested_at, dispatch_lease_until,
            grace_until, confirmed_at, last_error
     FROM termination_requests";

const TASK_OBSERVATION_SELECT: &str = "SELECT project_id, task_signature, pueue_task_id,
            pueue_group, command_json, state, enqueued_at, started_at, ended_at, result, observed_at
     FROM task_observations";

const INTERVENTION_SELECT: &str = "SELECT intervention_id, project_id, insertion_sequence,
            message, status, created_at, reserved_at, applied_at, agent_run_id, attempts,
            lease_expires_at, reservation_token
     FROM interventions";

const BATCH_REQUEST_SELECT: &str = "SELECT request_id, project_id, manifest_hash, status,
            lease_until, lease_token, created_at, updated_at, last_error
     FROM batch_requests";

const BATCH_JOB_SELECT: &str = "SELECT request_id, job_id, ordinal, kind, argv_json,
            metadata_json, status, pueue_task_id, submission_id, last_error
     FROM batch_jobs";

fn bounded_diagnostic_limit(limit: usize) -> i64 {
    limit.min(MAX_EVENT_LIST_LIMIT) as i64
}

fn is_policy_violation_code(value: &str) -> bool {
    matches!(
        value,
        "policy_missing"
            | "policy_unreadable"
            | "policy_weak_permissions"
            | "policy_unknown_field"
            | "trusted_path_unsafe"
            | "anchor_missing"
            | "anchor_replaced"
            | "custom_agent_not_enrolled"
            | "project_root_executable"
            | "unsafe_codex_argument"
            | "network_override"
            | "environment_name"
            | "session_missing"
            | "session_not_owned"
            | "root_changed"
            | "agent_log_unsafe"
            | "temp_unsafe"
            | "setsid_failed"
            | "native_gate_failed"
            | "unsupported_platform"
    )
}

fn exists(
    transaction: &Transaction<'_>,
    query: &str,
    value: impl rusqlite::ToSql,
) -> Result<bool, AppError> {
    transaction
        .query_row(query, [value], |_| Ok(()))
        .optional()
        .map(|row| row.is_some())
        .map_err(database_error("check database uniqueness"))
}

pub fn validate_event_lineage(
    transaction: &Transaction<'_>,
    event: &NewEvent,
) -> Result<(), AppError> {
    if event.experiment_id.is_some() && event.campaign_id.is_none() {
        return Err(AppError::Validation {
            field: "event.experiment_id",
            message: "requires campaign lineage",
        });
    }
    let Some(campaign_id) = event.campaign_id.as_deref() else {
        return Ok(());
    };
    if campaign_id.is_empty() {
        return Err(AppError::Validation {
            field: "event.campaign_id",
            message: "must be non-empty",
        });
    }
    let campaign_project_id = transaction
        .query_row(
            "SELECT project_id FROM campaigns WHERE campaign_id = ?1",
            [campaign_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(database_error("validate event campaign lineage"))?
        .ok_or(AppError::Validation {
            field: "event.campaign_id",
            message: "does not identify a campaign",
        })?;
    if campaign_project_id != event.project_id {
        return Err(AppError::Validation {
            field: "event.campaign_id",
            message: "must belong to the event project",
        });
    }
    if let Some(experiment_id) = event.experiment_id.as_deref() {
        if experiment_id.is_empty() {
            return Err(AppError::Validation {
                field: "event.experiment_id",
                message: "must be non-empty",
            });
        }
        let experiment_campaign_id = transaction
            .query_row(
                "SELECT campaign_id FROM experiments WHERE experiment_id = ?1",
                [experiment_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(database_error("validate event experiment lineage"))?
            .ok_or(AppError::Validation {
                field: "event.experiment_id",
                message: "does not identify an experiment",
            })?;
        if experiment_campaign_id != campaign_id {
            return Err(AppError::Validation {
                field: "event.experiment_id",
                message: "must belong to the event campaign",
            });
        }
    }
    Ok(())
}

fn project_from_row(row: &Row<'_>) -> rusqlite::Result<Project> {
    Ok(Project {
        project_id: row.get(0)?,
        root_path: PathBuf::from(row.get::<_, String>(1)?),
        pueue_group: row.get(2)?,
        config_path: PathBuf::from(row.get::<_, String>(3)?),
        enabled: row.get(4)?,
        paused: row.get(5)?,
        halted_reason: row.get(6)?,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
    })
}

fn event_from_row(row: &Row<'_>) -> rusqlite::Result<Event> {
    let payload_json: String = row.get(6)?;
    let payload = serde_json::from_str(&payload_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(6, Type::Text, Box::new(source))
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
}

fn intervention_from_row(row: &Row<'_>) -> rusqlite::Result<Intervention> {
    Ok(Intervention {
        intervention_id: row.get(0)?,
        project_id: row.get(1)?,
        insertion_sequence: row.get(2)?,
        message: row.get(3)?,
        status: row.get(4)?,
        created_at: row.get(5)?,
        reserved_at: row.get(6)?,
        applied_at: row.get(7)?,
        agent_run_id: row.get(8)?,
        attempts: row.get(9)?,
        lease_expires_at: row.get(10)?,
        reservation_token: row.get(11)?,
    })
}

fn integration_event_from_row(row: &Row<'_>) -> rusqlite::Result<IntegrationEvent> {
    let payload_json: String = row.get(3)?;
    let payload = serde_json::from_str(&payload_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(3, Type::Text, Box::new(source))
    })?;
    Ok(IntegrationEvent {
        integration_event_id: row.get(0)?,
        kind: row.get(1)?,
        dedup_key: row.get(2)?,
        payload,
        created_at: row.get(4)?,
    })
}

fn incident_from_row(row: &Row<'_>) -> rusqlite::Result<Incident> {
    Ok(Incident {
        incident_id: row.get(0)?,
        project_id: row.get(1)?,
        kind: row.get(2)?,
        task_key: row.get(3)?,
        fingerprint: row.get(4)?,
        status: row.get(5)?,
        first_seen_at: row.get(6)?,
        last_seen_at: row.get(7)?,
        acknowledged_at: row.get(8)?,
        resolved_at: row.get(9)?,
    })
}

fn submission_from_row(row: &Row<'_>) -> rusqlite::Result<Submission> {
    let argv_json: String = row.get(2)?;
    let argv = serde_json::from_str(&argv_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(2, Type::Text, Box::new(source))
    })?;
    let metadata_json: String = row.get(8)?;
    let metadata: serde_json::Value = serde_json::from_str(&metadata_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(8, Type::Text, Box::new(source))
    })?;
    if !metadata.is_object() {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            8,
            Type::Text,
            "submission metadata must be a JSON object".into(),
        ));
    }
    Ok(Submission {
        submission_id: row.get(0)?,
        project_id: row.get(1)?,
        argv,
        created_at: row.get(3)?,
        pueue_task_id: row.get(4)?,
        task_signature: row.get(5)?,
        status: row.get(6)?,
        kind: row.get(7)?,
        metadata,
        origin_agent_run_id: row.get(9)?,
    })
}

fn batch_request_from_row(row: &Row<'_>) -> rusqlite::Result<BatchRequest> {
    Ok(BatchRequest {
        request_id: row.get(0)?,
        project_id: row.get(1)?,
        manifest_hash: row.get(2)?,
        status: row.get(3)?,
        lease_until: row.get(4)?,
        lease_token: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
        last_error: row.get(8)?,
        jobs: Vec::new(),
    })
}

fn batch_job_from_row(row: &Row<'_>) -> rusqlite::Result<BatchJob> {
    let argv_json: String = row.get(4)?;
    let argv = serde_json::from_str(&argv_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(4, Type::Text, Box::new(source))
    })?;
    let metadata_json: String = row.get(5)?;
    let metadata: serde_json::Value = serde_json::from_str(&metadata_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(5, Type::Text, Box::new(source))
    })?;
    if !metadata.is_object() {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            5,
            Type::Text,
            "batch metadata must be a JSON object".into(),
        ));
    }
    Ok(BatchJob {
        request_id: row.get(0)?,
        job_id: row.get(1)?,
        ordinal: row.get(2)?,
        kind: row.get(3)?,
        argv,
        metadata,
        status: row.get(6)?,
        pueue_task_id: row.get(7)?,
        submission_id: row.get(8)?,
        last_error: row.get(9)?,
    })
}

fn read_batch(
    connection: &Connection,
    project_id: &str,
    request_id: &str,
) -> Result<Option<BatchRequest>, AppError> {
    let Some(mut batch) = connection
        .query_row(
            &format!(
                "{} WHERE project_id = ?1 AND request_id = ?2",
                BATCH_REQUEST_SELECT
            ),
            params![project_id, request_id],
            batch_request_from_row,
        )
        .optional()
        .map_err(database_error("read batch request"))?
    else {
        return Ok(None);
    };
    let mut statement = connection
        .prepare(&format!(
            "{} WHERE request_id = ?1 ORDER BY ordinal, job_id",
            BATCH_JOB_SELECT
        ))
        .map_err(database_error("prepare batch job query"))?;
    batch.jobs = statement
        .query_map([request_id], batch_job_from_row)
        .map_err(database_error("query batch jobs"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(database_error("read batch jobs"))?;
    Ok(Some(batch))
}

fn read_submission(connection: &Connection, submission_id: &str) -> Result<Submission, AppError> {
    connection
        .query_row(
            &format!("{} WHERE submission_id = ?1", SUBMISSION_SELECT),
            [submission_id],
            submission_from_row,
        )
        .map_err(database_error("read submission"))
}

fn agent_run_from_row(row: &Row<'_>) -> rusqlite::Result<AgentRun> {
    let launch_gate_state: String = row.get(10)?;
    let context_mode_value: String = row.get(11)?;
    let context_session_id: Option<String> = row.get(12)?;
    let context_mode =
        AgentContextMode::from_db_parts(&context_mode_value, context_session_id.clone()).map_err(
            |source| rusqlite::Error::FromSqlConversionFailure(11, Type::Text, Box::new(source)),
        )?;
    let context_lineage_json: String = row.get(13)?;
    let context_lineage = serde_json::from_str(&context_lineage_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(13, Type::Text, Box::new(source))
    })?;
    Ok(AgentRun {
        run_id: row.get(0)?,
        project_id: row.get(1)?,
        primary_event_id: row.get(2)?,
        pid: row.get(3)?,
        status: row.get(4)?,
        started_at: row.get(5)?,
        finished_at: row.get(6)?,
        exit_code: row.get(7)?,
        log_path: PathBuf::from(row.get::<_, String>(8)?),
        last_error: row.get(9)?,
        launch_gate_state,
        context_mode,
        context_session_id,
        context_lineage,
        execution_kind: row.get(14)?,
        executable_path: row.get(15)?,
        executable_identity: row.get(16)?,
        policy_code: row.get(17)?,
        failure_stage: row.get(18)?,
    })
}

fn reserve_next_agent_run_id(transaction: &Transaction<'_>) -> Result<i64, AppError> {
    let stored_floor: i64 = transaction
        .query_row(
            "SELECT last_run_id FROM agent_run_id_sequence WHERE sequence_id = 1",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("read agent run ID sequence"))?;
    let existing_floor: i64 = transaction
        .query_row(
            "SELECT COALESCE(MAX(run_id), 0) FROM agent_runs",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("read maximum agent run ID"))?;
    let floor = stored_floor.max(existing_floor);
    if floor >= MAX_PRIVATE_TEMP_RUN_ID {
        return Err(AppError::Runtime {
            operation: "allocate agent run ID",
        });
    }
    let next_run_id = floor + 1;
    transaction
        .execute(
            "UPDATE agent_run_id_sequence
             SET last_run_id = ?1
             WHERE sequence_id = 1",
            [next_run_id],
        )
        .map_err(database_error("advance agent run ID sequence"))?;
    Ok(next_run_id)
}

fn read_durable_agent_run_id_high_water(connection: &Connection) -> Result<i64, AppError> {
    let stored_floor: i64 = connection
        .query_row(
            "SELECT last_run_id FROM agent_run_id_sequence WHERE sequence_id = 1",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("read agent run ID sequence floor"))?;
    let existing_floor: i64 = connection
        .query_row(
            "SELECT COALESCE(MAX(run_id), 0) FROM agent_runs",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("read maximum agent run ID for floor"))?;
    if stored_floor < 0
        || stored_floor > MAX_PRIVATE_TEMP_RUN_ID
        || existing_floor > MAX_PRIVATE_TEMP_RUN_ID
        || stored_floor < existing_floor
    {
        return Err(AppError::Runtime {
            operation: "validate agent run ID sequence floor",
        });
    }
    Ok(stored_floor)
}

fn insert_agent_run(
    transaction: &Transaction<'_>,
    run: &NewAgentRun,
    _run_id_guard: &RunIdAdmissionGuard,
) -> Result<i64, AppError> {
    let run_id = reserve_next_agent_run_id(transaction)?;
    let log_path = path_text(&run.log_path, "log_path")?;
    let context_lineage_json =
        serde_json::to_string(&run.context_lineage).map_err(|source| AppError::Serialization {
            operation: "serialize agent context lineage",
            source,
        })?;
    let launch_gate_state = if run.status == AgentRunStatus::Starting {
        "pending"
    } else {
        "released"
    };
    let execution = run.execution.as_ref();
    transaction
        .execute(
            "INSERT INTO agent_runs (
                run_id, project_id, primary_event_id, pid, status, started_at,
                finished_at, exit_code, log_path, last_error, launch_gate_state,
                context_mode,
                context_session_id, context_lineage_json,
                execution_kind, executable_path, executable_identity,
                policy_code, failure_stage
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, ?7, NULL, ?8, ?9, ?10, ?11,
                       ?12, ?13, ?14, NULL, NULL)",
            params![
                run_id,
                run.project_id,
                run.primary_event_id,
                run.pid,
                run.status,
                run.started_at,
                log_path,
                launch_gate_state,
                run.context_mode.as_str(),
                run.context_session_id.as_deref(),
                context_lineage_json,
                execution.map(ExecutionProjection::execution_kind),
                execution.map(ExecutionProjection::executable_path),
                execution.map(ExecutionProjection::executable_identity),
            ],
        )
        .map_err(database_error("insert agent run"))?;
    Ok(run_id)
}

fn attach_event_to_agent_run(
    transaction: &Transaction<'_>,
    project_id: &str,
    run_id: i64,
    event_id: i64,
) -> Result<(), AppError> {
    transaction
        .execute(
            "INSERT INTO agent_run_events (project_id, run_id, event_id)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(run_id, event_id) DO NOTHING",
            params![project_id, run_id, event_id],
        )
        .map_err(database_error("attach event to agent run"))?;
    Ok(())
}

fn read_agent_run(connection: &Connection, run_id: i64) -> Result<AgentRun, AppError> {
    connection
        .query_row(
            &format!("{} WHERE run_id = ?1", AGENT_RUN_SELECT),
            [run_id],
            agent_run_from_row,
        )
        .map_err(database_error("read agent run"))
}

#[cfg(test)]
std::thread_local! {
    static FAIL_FINALIZER_RESULT_READ_ONCE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static REPLACE_REGISTERED_ROOT_BEFORE_COMMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static REPLACE_REMOVED_ROOT_BEFORE_COMMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn inject_finalizer_result_read_failure_once() {
    FAIL_FINALIZER_RESULT_READ_ONCE.with(|fail| fail.set(true));
}

#[cfg(test)]
fn inject_registered_root_replacement_before_commit_once() {
    REPLACE_REGISTERED_ROOT_BEFORE_COMMIT.with(|replace| replace.set(true));
}

#[cfg(test)]
fn inject_removed_root_replacement_before_commit_once() {
    REPLACE_REMOVED_ROOT_BEFORE_COMMIT.with(|replace| replace.set(true));
}

#[cfg(test)]
fn invoke_register_before_commit_hook(root: &std::path::Path) {
    if REPLACE_REGISTERED_ROOT_BEFORE_COMMIT.with(|replace| replace.replace(false)) {
        let retired = root.with_file_name(format!(
            "{}-retired",
            root.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("project")
        ));
        std::fs::rename(root, &retired).expect("retire registered root in test hook");
        std::fs::create_dir(root).expect("replace registered root in test hook");
    }
}

#[cfg(test)]
fn invoke_remove_before_commit_hook(root: &std::path::Path) {
    if REPLACE_REMOVED_ROOT_BEFORE_COMMIT.with(|replace| replace.replace(false)) {
        let retired = root.with_file_name(format!(
            "{}-removal-retired",
            root.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("project")
        ));
        std::fs::rename(root, &retired).expect("retire removed root in test hook");
        std::fs::create_dir(root).expect("replace removed root in test hook");
    }
}

fn read_agent_run_in_transaction(
    transaction: &Transaction<'_>,
    run_id: i64,
) -> Result<AgentRun, AppError> {
    #[cfg(test)]
    if FAIL_FINALIZER_RESULT_READ_ONCE.with(|fail| fail.replace(false)) {
        return Err(AppError::Database {
            operation: "read finalized agent run",
            source: rusqlite::Error::QueryReturnedNoRows,
        });
    }
    transaction
        .query_row(
            &format!("{} WHERE run_id = ?1", AGENT_RUN_SELECT),
            [run_id],
            agent_run_from_row,
        )
        .map_err(database_error("read finalized agent run"))
}

fn read_termination_request(
    connection: &Connection,
    request_id: i64,
) -> Result<TerminationRequest, AppError> {
    connection
        .query_row(
            &format!("{} WHERE request_id = ?1", TERMINATION_REQUEST_SELECT),
            [request_id],
            termination_request_from_row,
        )
        .map_err(database_error("read termination request"))
}

fn termination_request_from_row(row: &Row<'_>) -> rusqlite::Result<TerminationRequest> {
    Ok(TerminationRequest {
        request_id: row.get(0)?,
        incident_id: row.get(1)?,
        project_id: row.get(2)?,
        task_signature: row.get(3)?,
        reason: row.get(4)?,
        status: row.get(5)?,
        requested_at: row.get(6)?,
        dispatch_lease_until: row.get(7)?,
        grace_until: row.get(8)?,
        confirmed_at: row.get(9)?,
        last_error: row.get(10)?,
    })
}

fn task_observation_from_row(row: &Row<'_>) -> rusqlite::Result<TaskObservation> {
    let command_json: String = row.get(4)?;
    let command = serde_json::from_str(&command_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(4, Type::Text, Box::new(source))
    })?;
    Ok(TaskObservation {
        project_id: row.get(0)?,
        task_signature: row.get(1)?,
        pueue_task_id: row.get(2)?,
        pueue_group: row.get(3)?,
        command,
        state: row.get(5)?,
        enqueued_at: row.get(6)?,
        started_at: row.get(7)?,
        ended_at: row.get(8)?,
        result: row.get(9)?,
        observed_at: row.get(10)?,
    })
}

fn read_incident(transaction: &Transaction<'_>, incident_id: i64) -> Result<Incident, AppError> {
    transaction
        .query_row(
            &format!("{} WHERE incident_id = ?1", INCIDENT_SELECT),
            [incident_id],
            incident_from_row,
        )
        .map_err(database_error("read incident"))
}

fn project_constraint_error(source: rusqlite::Error) -> AppError {
    let message = source.to_string();
    for (column, field) in [
        ("projects.root_path", "root_path"),
        ("projects.pueue_group", "pueue_group"),
        ("projects.project_id", "project_id"),
    ] {
        if message.contains(column) {
            return AppError::DatabaseConflict { field };
        }
    }
    AppError::Database {
        operation: "register project",
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use rusqlite::{params, StatementStatus};
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn event_claim_candidate_probe_is_bounded_before_one_thousand_ineligible_projects() {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        let mut connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO projects (
                     project_id, root_path, pueue_group, config_path,
                     enabled, paused, created_at, updated_at
                 ) VALUES
                     ('disabled-project', '/tmp/disabled-project', 'disabled-group',
                      '/tmp/disabled-project/config.toml', 0, 0, 1, 1),
                     ('eligible-project', '/tmp/eligible-project', 'eligible-group',
                      '/tmp/eligible-project/config.toml', 1, 0, 1, 1);",
            )
            .unwrap();
        let transaction = connection.transaction().unwrap();
        for ordinal in 1..=1_000_i64 {
            transaction
                .execute(
                    "INSERT INTO events (
                         project_id, kind, dedup_key, payload_json, status,
                         attempts, not_before, created_at
                     ) VALUES ('disabled-project', 'campaign_decision', ?1, '{}',
                               'pending', 0, 1, ?2)",
                    params![format!("disabled-decision-{ordinal}"), ordinal],
                )
                .unwrap();
            transaction
                .execute(
                    "INSERT INTO events (
                         project_id, kind, dedup_key, payload_json, status,
                         attempts, not_before, created_at
                     ) VALUES ('disabled-project', 'campaign_decision', ?1, '{}',
                               'retry_wait', 0, 1, ?2)",
                    params![format!("disabled-retry-{ordinal}"), ordinal],
                )
                .unwrap();
        }
        transaction
            .execute(
                "INSERT INTO events (
                     project_id, kind, dedup_key, payload_json, status,
                     attempts, not_before, created_at
                 ) VALUES ('eligible-project', 'deep_check', 'eligible', '{}',
                           'pending', 0, 1, 2000)",
                [],
            )
            .unwrap();
        transaction.commit().unwrap();

        for (status, expected_event_id) in [
            (EventStatus::Pending, 1_i64),
            (EventStatus::RetryWait, 2_i64),
        ] {
            let sql = event_claim_candidate_sql(status);
            let mut statement = connection.prepare(&sql).unwrap();
            let event_ids = statement
                .query_map(params![100_i64, 1_i64], |row| row.get::<_, i64>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let vm_steps = statement.get_status(StatementStatus::VmStep);

            assert!(!sql.contains("projects"), "{sql}");
            assert!(!sql.contains("campaigns"), "{sql}");
            assert_eq!(event_ids, [expected_event_id]);
            assert!(vm_steps < 100, "event claim used {vm_steps} VM steps");
        }
    }

    #[test]
    fn blocked_project_claim_probe_is_a_pure_index_prefix_without_json_filtering() {
        for status in [EventStatus::Pending, EventStatus::RetryWait] {
            let sql = event_claim_candidate_sql(status);
            assert!(sql.contains("INDEXED BY events_claimable_idx"), "{sql}");
            assert!(!sql.contains("json_each"), "{sql}");
            assert!(!sql.contains("projects"), "{sql}");
            assert!(!sql.contains("campaigns"), "{sql}");
        }
    }

    #[test]
    fn blocked_project_claim_full_transaction_has_a_fixed_vm_work_bound() {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        let mut connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO projects (
                     project_id, root_path, pueue_group, config_path,
                     enabled, paused, created_at, updated_at
                 ) VALUES
                     ('blocked-project', '/tmp/blocked-project', 'blocked-group',
                      '/tmp/blocked-project/config.toml', 1, 0, 1, 1),
                     ('eligible-project', '/tmp/eligible-project', 'eligible-group',
                      '/tmp/eligible-project/config.toml', 1, 0, 1, 1);",
            )
            .unwrap();
        let transaction = connection.transaction().unwrap();
        for ordinal in 1..=EVENT_CLAIM_WORK_LIMIT {
            transaction
                .execute(
                    "INSERT INTO events (
                         project_id, kind, dedup_key, payload_json, status,
                         attempts, not_before, created_at
                     ) VALUES ('blocked-project', 'deep_check', ?1, '{}',
                               'pending', 0, 1, ?2)",
                    params![format!("blocked-{ordinal:04}"), ordinal as i64],
                )
                .unwrap();
        }
        transaction
            .execute(
                "INSERT INTO events (
                     project_id, kind, dedup_key, payload_json, status,
                     attempts, not_before, created_at
                 ) VALUES ('eligible-project', 'deep_check', 'eligible', '{}',
                           'pending', 0, 1, 2000)",
                [],
            )
            .unwrap();
        transaction.commit().unwrap();

        let vm_steps = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&vm_steps);
        connection.progress_handler(
            1,
            Some(move || {
                observed.fetch_add(1, Ordering::Relaxed);
                false
            }),
        );
        let claimed = EventRepository::claim_batch_excluding_projects_in_connection(
            &mut connection,
            100,
            160,
            1,
            &BTreeSet::from(["blocked-project".to_owned()]),
        )
        .unwrap();
        connection.progress_handler(0, None::<fn() -> bool>);

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].project_id, "eligible-project");
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM events
                     WHERE project_id = 'blocked-project' AND status = 'retry_wait'
                       AND not_before = 160 AND attempts = 0",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            EVENT_CLAIM_WORK_LIMIT as i64
        );
        let vm_steps = vm_steps.load(Ordering::Relaxed);
        assert!(
            vm_steps < 150_000,
            "full blocked claim transaction used {vm_steps} VM steps"
        );
    }

    #[test]
    fn terminal_finalizer_result_read_failure_rolls_back_and_retries() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &root,
                "pa-project",
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();

        let event_id = EventRepository::new(&db)
            .insert_idempotent(&NewEvent::new(
                "project-a",
                EventKind::TaskFinished,
                "finalizer-read-failure",
                json!({"task_id": 41}),
                100,
                100,
            ))
            .unwrap()
            .event_id;
        EventRepository::new(&db).claim_batch(100, 200, 1).unwrap();
        let intervention = InterventionRepository::new(&db)
            .insert_pending("project-a", "retain until finalization", 100)
            .unwrap();
        InterventionRepository::new(&db)
            .reserve_pending("project-a", "finalizer-read-token", 101, 201, 1, 1024)
            .unwrap();
        let run = AgentRunRepository::new(&db)
            .insert_with_events_and_reservation(
                &NewAgentRun::new(
                    "project-a",
                    event_id,
                    None,
                    AgentRunStatus::Starting,
                    110,
                    PathBuf::from("/tmp/finalizer-read-failure.log"),
                ),
                &[event_id],
                Some("finalizer-read-token"),
            )
            .unwrap();
        let runs = AgentRunRepository::new(&db);
        runs.mark_running_and_apply_interventions("project-a", run.run_id, 4242, 120)
            .unwrap();

        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE finalizer_event_mutations (count INTEGER NOT NULL);
                 INSERT INTO finalizer_event_mutations VALUES (0);
                 CREATE TRIGGER count_finalizer_event_mutations
                 AFTER UPDATE OF status ON events
                 WHEN NEW.status = 'dead_letter'
                 BEGIN
                     UPDATE finalizer_event_mutations SET count = count + 1;
                 END;",
            )
            .unwrap();
        drop(connection);

        inject_finalizer_result_read_failure_once();
        let first = runs.fail_before_gate_release_with_policy(
            "project-a",
            run.run_id,
            200,
            "terminal finalizer read fault",
            RetryPolicy { max_retries: 0 },
        );
        assert!(first.is_err());

        let before_retry: (AgentRunStatus, EventStatus) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT agent_runs.status, events.status
                 FROM agent_runs
                 JOIN agent_run_events USING (project_id, run_id)
                 JOIN events USING (project_id, event_id)
                 WHERE agent_runs.run_id = ?1",
                [run.run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            before_retry,
            (AgentRunStatus::Running, EventStatus::InFlight)
        );
        let intervention_before_retry: InterventionStatus = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status FROM interventions WHERE intervention_id = ?1",
                [&intervention.intervention_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(intervention_before_retry, InterventionStatus::Applied);

        let finalized = runs
            .fail_before_gate_release_with_policy(
                "project-a",
                run.run_id,
                200,
                "terminal finalizer read fault",
                RetryPolicy { max_retries: 0 },
            )
            .unwrap();
        assert_eq!(finalized.status, AgentRunStatus::Failed);
        let count: i64 = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT count FROM finalizer_event_mutations",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        let after_retry: (AgentRunStatus, EventStatus) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT agent_runs.status, events.status
                 FROM agent_runs
                 JOIN agent_run_events USING (project_id, run_id)
                 JOIN events USING (project_id, event_id)
                 WHERE agent_runs.run_id = ?1",
                params![run.run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            after_retry,
            (AgentRunStatus::Failed, EventStatus::DeadLetter)
        );
        let intervention_after_retry: (InterventionStatus, Option<i64>) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status, agent_run_id FROM interventions WHERE intervention_id = ?1",
                [&intervention.intervention_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            intervention_after_retry,
            (InterventionStatus::Pending, None)
        );
        assert!(runs
            .fail_before_gate_release_with_policy(
                "project-a",
                run.run_id,
                201,
                "must not finalize twice",
                RetryPolicy { max_retries: 0 },
            )
            .is_err());
        let count_after_rejection: i64 = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT count FROM finalizer_event_mutations",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count_after_rejection, 1);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn durable_run_id_high_water_survives_project_removal_and_reenrollment() {
        let temp = TempDir::new().unwrap();
        let first_root = temp.path().join("first-project");
        fs::create_dir_all(&first_root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &first_root,
                "pa-run-id-floor",
                first_root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        let first_event = EventRepository::new(&db)
            .insert_idempotent(&NewEvent::new(
                "project-a",
                EventKind::TaskFinished,
                "first-run-id",
                json!({"task_id": 41}),
                100,
                100,
            ))
            .unwrap()
            .event_id;
        EventRepository::new(&db).claim_batch(100, 200, 1).unwrap();
        let first_run = AgentRunRepository::new(&db)
            .insert_with_events(
                &NewAgentRun::new(
                    "project-a",
                    first_event,
                    None,
                    AgentRunStatus::Starting,
                    110,
                    "/tmp/first-run-id.log",
                ),
                &[first_event],
            )
            .unwrap();
        assert_eq!(first_run.run_id, 1);
        let first_anchor = crate::execution_policy::ProjectRootAnchor::resolve(
            &fs::canonicalize(&first_root).unwrap(),
        )
        .unwrap();
        let retained = crate::environment::PrivateRunTemp::create(
            &first_anchor.verify_identity().unwrap(),
            first_run.run_id,
        )
        .unwrap();
        let retained_path = retained.path().to_path_buf();
        drop(retained);
        ProjectRepository::new(&db)
            .remove("project-a", 120, &[])
            .unwrap();
        assert!(retained_path.is_dir());

        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &first_root,
                "pa-run-id-reenrolled",
                first_root.join(".pueue-agent/config.toml"),
                130,
            ))
            .unwrap();
        let second_event = EventRepository::new(&db)
            .insert_idempotent(&NewEvent::new(
                "project-a",
                EventKind::TaskFinished,
                "second-run-id",
                json!({"task_id": 42}),
                130,
                130,
            ))
            .unwrap()
            .event_id;
        EventRepository::new(&db).claim_batch(130, 230, 1).unwrap();
        let second_run = AgentRunRepository::new(&db)
            .insert_with_events(
                &NewAgentRun::new(
                    "project-a",
                    second_event,
                    None,
                    AgentRunStatus::Starting,
                    140,
                    "/tmp/second-run-id.log",
                ),
                &[second_event],
            )
            .unwrap();
        assert_eq!(second_run.run_id, first_run.run_id + 1);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn committed_run_id_before_temp_create_failure_is_not_reused() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let first_root = temp.path().join("first-project");
        let second_root = temp.path().join("second-project");
        fs::create_dir(&first_root).unwrap();
        fs::create_dir(&second_root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        for (project_id, root, group) in [
            ("project-a", &first_root, "pa-temp-gap"),
            ("project-b", &second_root, "pb-temp-gap"),
        ] {
            ProjectRepository::new(&db)
                .register(&NewProject::new(
                    project_id,
                    root,
                    group,
                    root.join(".pueue-agent/config.toml"),
                    100,
                ))
                .unwrap();
        }
        let first_event = EventRepository::new(&db)
            .insert_idempotent(&NewEvent::new(
                "project-a",
                EventKind::TaskFinished,
                "temp-gap-first",
                json!({}),
                100,
                100,
            ))
            .unwrap()
            .event_id;
        let first_run = AgentRunRepository::new(&db)
            .insert(&NewAgentRun::new(
                "project-a",
                first_event,
                None,
                AgentRunStatus::Starting,
                110,
                "/tmp/temp-gap-first.log",
            ))
            .unwrap();
        assert_eq!(first_run.run_id, 1);

        let tmp = first_root.join(".pueue-agent/tmp");
        fs::create_dir_all(&tmp).unwrap();
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o700)).unwrap();
        let occupied = tmp.join(first_run.run_id.to_string());
        fs::create_dir(&occupied).unwrap();
        fs::set_permissions(&occupied, fs::Permissions::from_mode(0o700)).unwrap();
        let anchor = ProjectRootAnchor::resolve(&fs::canonicalize(&first_root).unwrap()).unwrap();
        assert!(PrivateRunTemp::create(&anchor.verify_identity().unwrap(), first_run.run_id)
            .is_err());

        let second_event = EventRepository::new(&db)
            .insert_idempotent(&NewEvent::new(
                "project-b",
                EventKind::TaskFinished,
                "temp-gap-second",
                json!({}),
                120,
                120,
            ))
            .unwrap()
            .event_id;
        let second_run = AgentRunRepository::new(&db)
            .insert(&NewAgentRun::new(
                "project-b",
                second_event,
                None,
                AgentRunStatus::Starting,
                120,
                "/tmp/temp-gap-second.log",
            ))
            .unwrap();
        assert_eq!(second_run.run_id, 2);
        let sequence: i64 = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT last_run_id FROM agent_run_id_sequence WHERE sequence_id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sequence, 2);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn project_removal_rejects_future_generation_without_advancing_sequence() {
        let temp = TempDir::new().unwrap();
        let first_root = temp.path().join("first-project");
        fs::create_dir(&first_root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &first_root,
                "pa-removal-floor",
                first_root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        let first_anchor = ProjectRootAnchor::resolve(&fs::canonicalize(&first_root).unwrap())
            .unwrap();
        let retained = PrivateRunTemp::create(&first_anchor.verify_identity().unwrap(), 41)
            .unwrap();
        fs::write(retained.path().join("preserved"), b"retained evidence").unwrap();
        let retained_path = retained.path().to_path_buf();
        drop(retained);

        let result = ProjectRepository::new(&db).remove("project-a", 110, &[]);

        let sequence: i64 = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT last_run_id FROM agent_run_id_sequence WHERE sequence_id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sequence, 0);
        assert!(matches!(
            result,
            Err(AppError::PolicyViolation { violation })
                if violation.code == PolicyViolationCode::TempUnsafe
        ));
        assert!(ProjectRepository::new(&db)
            .find_by_id("project-a")
            .unwrap()
            .is_some());
        assert_eq!(
            fs::read(retained_path.join("preserved")).unwrap(),
            b"retained evidence"
        );

    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn project_removal_inventory_failure_preserves_row_and_sequence() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project");
        fs::create_dir(&root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &root,
                "pa-removal-unsafe",
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        let anchor = ProjectRootAnchor::resolve(&fs::canonicalize(&root).unwrap()).unwrap();
        let retained = PrivateRunTemp::create(&anchor.verify_identity().unwrap(), 17).unwrap();
        let tmp = retained.path().parent().unwrap().to_path_buf();
        drop(retained);
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o777)).unwrap();

        let result = ProjectRepository::new(&db).remove("project-a", 110, &[]);

        assert!(matches!(
            result,
            Err(AppError::PolicyViolation { violation })
                if violation.code == PolicyViolationCode::TempUnsafe
        ));
        assert!(ProjectRepository::new(&db)
            .find_by_id("project-a")
            .unwrap()
            .is_some());
        let connection = db.connect().unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT last_run_id FROM agent_run_id_sequence WHERE sequence_id = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM operator_logs", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn project_removal_lock_contention_preserves_row_and_sequence() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project");
        fs::create_dir(&root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &root,
                "pa-removal-lock",
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        let anchor = ProjectRootAnchor::resolve(&fs::canonicalize(&root).unwrap()).unwrap();
        let verified = anchor.verify_identity().unwrap();
        let lock = ProjectAdmissionLock::try_acquire(&verified)
            .unwrap()
            .expect("project lock");

        let result = ProjectRepository::new(&db).remove("project-a", 110, &[]);

        assert!(matches!(
            result,
            Err(AppError::Runtime {
                operation: "acquire project removal admission lock"
            })
        ));
        assert!(ProjectRepository::new(&db)
            .find_by_id("project-a")
            .unwrap()
            .is_some());
        assert_eq!(
            db.connect()
                .unwrap()
                .query_row(
                    "SELECT last_run_id FROM agent_run_id_sequence WHERE sequence_id = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        drop(lock);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn project_removal_root_replacement_rolls_back_floor_and_row() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project");
        fs::create_dir(&root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &root,
                "pa-removal-replaced",
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        inject_removed_root_replacement_before_commit_once();
        let result = ProjectRepository::new(&db).remove("project-a", 110, &[]);

        assert!(matches!(
            result,
            Err(AppError::PolicyViolation { violation })
                if violation.code == PolicyViolationCode::RootChanged
        ));
        assert!(ProjectRepository::new(&db)
            .find_by_id("project-a")
            .unwrap()
            .is_some());
        let connection = db.connect().unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT last_run_id FROM agent_run_id_sequence WHERE sequence_id = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM operator_logs", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert!(root.with_file_name("project-removal-retired").is_dir());
    }

    #[test]
    fn project_admission_lock_is_scoped_and_nonblocking() {
        use std::{sync::mpsc, thread, time::Duration};

        let temp = TempDir::new().unwrap();
        let first_root = temp.path().join("first-project");
        let second_root = temp.path().join("second-project");
        fs::create_dir_all(&first_root).unwrap();
        fs::create_dir_all(&second_root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &first_root,
                "pa-lock-test",
                first_root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-b",
                &second_root,
                "pb-lock-test",
                second_root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        let unrelated_event = EventRepository::new(&db)
            .insert_idempotent(&NewEvent::new(
                "project-b",
                EventKind::TaskFinished,
                "unrelated-lock-write",
                json!({"task_id": 43}),
                100,
                100,
            ))
            .unwrap()
            .event_id;
        let first_anchor = crate::execution_policy::ProjectRootAnchor::resolve(
            &fs::canonicalize(&first_root).unwrap(),
        )
        .unwrap();
        let first_root = first_anchor.verify_identity().unwrap();
        let first_lock = crate::environment::ProjectAdmissionLock::try_acquire(&first_root)
            .unwrap()
            .expect("first project lock");

        let (same_result_tx, same_result_rx) = mpsc::channel();
        let same_anchor = first_anchor.clone();
        thread::spawn(move || {
            let root = same_anchor.verify_identity().unwrap();
            let result = crate::environment::ProjectAdmissionLock::try_acquire(&root)
                .unwrap()
                .is_none();
            same_result_tx.send(result).unwrap();
        });
        assert!(same_result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("same-project contention result"));

        let (other_result_tx, other_result_rx) = mpsc::channel();
        let unrelated_db = db.clone();
        thread::spawn(move || {
            let result = AgentRunRepository::new(&unrelated_db)
                .insert(&NewAgentRun::new(
                    "project-b",
                    unrelated_event,
                    None,
                    AgentRunStatus::Starting,
                    110,
                    "/tmp/unrelated-lock-write.log",
                ))
                .map(|_| ());
            other_result_tx.send(result).unwrap();
        });
        assert!(other_result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("unrelated-project bind result")
            .is_ok());
        drop(first_lock);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn registration_rejects_future_generation_without_advancing_run_id_sequence() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let root = temp.path().join("registered-project");
        let tmp = root.join(".pueue-agent/tmp");
        fs::create_dir_all(&tmp).unwrap();
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o700)).unwrap();
        let future = tmp.join(MAX_PRIVATE_TEMP_RUN_ID.to_string());
        fs::create_dir(&future).unwrap();
        fs::set_permissions(&future, fs::Permissions::from_mode(0o700)).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();

        let result = ProjectRepository::new(&db).register(&NewProject::new(
            "project-future",
            &root,
            "group-future",
            root.join(".pueue-agent/config.toml"),
            100,
        ));

        assert!(matches!(
            result,
            Err(AppError::PolicyViolation { violation })
                if violation.code == PolicyViolationCode::TempUnsafe
        ));
        let connection = db.connect().unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT last_run_id FROM agent_run_id_sequence WHERE sequence_id = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM projects WHERE project_id = 'project-future'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn registration_rejects_root_replacement_before_commit() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("registered-project");
        fs::create_dir(&root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();

        inject_registered_root_replacement_before_commit_once();
        let result = ProjectRepository::new(&db).register(&NewProject::new(
            "project-replaced",
            &root,
            "group-replaced",
            root.join(".pueue-agent/config.toml"),
            100,
        ));

        assert!(matches!(
            result,
            Err(AppError::PolicyViolation { violation })
                if violation.code == PolicyViolationCode::RootChanged
        ));
        let connection = db.connect().unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM projects WHERE project_id = 'project-replaced'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT last_run_id FROM agent_run_id_sequence WHERE sequence_id = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn run_id_guard_survives_visible_database_parent_swap() {
        let temp = TempDir::new().unwrap();
        let parent = temp.path().join("state");
        fs::create_dir(&parent).unwrap();
        let db = Db::open(&parent.join("state.sqlite3")).unwrap();
        let first = RunIdAdmissionGuard::try_acquire(db.run_id_lock_parent())
            .unwrap()
            .expect("first global run-ID guard");

        let moved = temp.path().join("state-moved");
        fs::rename(&parent, &moved).unwrap();
        fs::create_dir(&parent).unwrap();

        let second = RunIdAdmissionGuard::try_acquire(db.run_id_lock_parent()).unwrap();
        assert!(second.is_none(), "visible parent replacement bypassed guard");
        drop(first);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn global_run_id_guard_serializes_durable_allocation() {
        use std::sync::mpsc;

        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &root,
                "pa-bootstrap-race",
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        let event_id = EventRepository::new(&db)
            .insert_idempotent(&NewEvent::new(
                "project-a",
                EventKind::TaskFailed,
                "bootstrap-race-event",
                json!({}),
                100,
                100,
            ))
            .unwrap()
            .event_id;
        let first = RunIdAdmissionGuard::try_acquire(db.run_id_lock_parent())
            .unwrap()
            .expect("allocator guard");

        let (sender, receiver) = mpsc::channel();
        let competing_db = db.clone();
        std::thread::spawn(move || {
            let result = AgentRunRepository::new(&competing_db).insert(&NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                110,
                "/tmp/bootstrap-race.log",
            ));
            sender.send(result.is_err()).unwrap();
        });
        assert!(receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("competing allocator result"));
        assert_eq!(AgentRunRepository::new(&db).count_by_project("project-a").unwrap(), 0);
        drop(first);

        let run = AgentRunRepository::new(&db)
            .insert(&NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                120,
                "/tmp/bootstrap-race-second.log",
            ))
            .unwrap();
        assert_eq!(run.run_id, 1);
    }

    #[test]
    fn run_id_allocator_rejects_a_floor_that_would_emit_maximum_generation_id() {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute(
                "UPDATE agent_run_id_sequence SET last_run_id = ?1 WHERE sequence_id = 1",
                [i64::MAX - 1],
            )
            .unwrap();
        drop(connection);
        let mut connection = db.connect().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert!(reserve_next_agent_run_id(&transaction).is_err());
        transaction.rollback().unwrap();
        let floor: i64 = connection
            .query_row(
                "SELECT last_run_id FROM agent_run_id_sequence WHERE sequence_id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(floor, i64::MAX - 1);
    }
}
