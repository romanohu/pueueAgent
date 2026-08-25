//! Bounded read-only diagnosis agents for suspicious running experiments.
//!
//! A `Suspicious` running-health row consumes exactly one pinned, read-only
//! Codex run per pass.  The schema-validated output is persisted on
//! `running_health.diagnosis_json`; malformed output increments a bounded
//! attempt counter stored inside the same column and reverts the row to
//! `Suspicious`.  After [`MAX_DIAGNOSIS_ATTEMPTS`] the cap stops further
//! spawns and every failed run dead-letters with reason
//! `health_diagnosis_missing`.

use sha2::{Digest, Sha256};

use crate::{
    agent::{AgentHandle, AgentRunner, AgentSpawnError, AgentSpawnStage},
    config,
    db::{CampaignRepository, Db, EventRepository, HealthRepository, ProjectRepository},
    execution_policy::preflight_decision_runtime,
    health::{campaign_defers, read_task_tail},
    models::{EventKind, EventStatus, HealthState, NewEvent, RunningHealthRow},
    retry::RetryPolicy,
    AppError,
};

pub const HEALTH_DIAGNOSIS_SCHEMA: &[u8] = br#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["root_cause_class", "confidence", "recommended_action", "summary"],
  "properties": {
    "root_cause_class": {"type": "string", "minLength": 1, "maxLength": 128},
    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    "recommended_action": {"enum": ["continue", "kill_and_resume", "kill_and_escalate"]},
    "summary": {"type": "string", "minLength": 1, "maxLength": 512}
  }
}"#;

pub const MAX_DIAGNOSIS_TAIL_BYTES: usize = 4 * 1024;
pub const MAX_DIAGNOSIS_EVIDENCE_BYTES: usize = 128 * 1024;

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ValidatedDiagnosis {
    pub root_cause_class: String,
    pub confidence: f64,
    pub recommended_action: RecommendedAction,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendedAction {
    Continue,
    KillAndResume,
    KillAndEscalate,
}

/// Parse and strictly validate one Codex diagnosis output document.
pub fn parse_and_validate_diagnosis(bytes: &[u8]) -> Result<ValidatedDiagnosis, AppError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|source| AppError::Serialization {
            operation: "parse health diagnosis output",
            source,
        })?;
    let object = value.as_object().ok_or_else(diagnosis_invalid)?;
    const REQUIRED_FIELDS: [&str; 4] = [
        "root_cause_class",
        "confidence",
        "recommended_action",
        "summary",
    ];
    if object.len() != REQUIRED_FIELDS.len()
        || REQUIRED_FIELDS.iter().any(|field| !object.contains_key(*field))
    {
        return Err(diagnosis_invalid());
    }
    let root_cause_class = bounded_text(
        object
            .get("root_cause_class")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(diagnosis_invalid)?,
        128,
    )?;
    let confidence = object
        .get("confidence")
        .and_then(serde_json::Value::as_f64)
        .ok_or_else(diagnosis_invalid)?;
    if !(0.0..=1.0).contains(&confidence) || !confidence.is_finite() {
        return Err(diagnosis_invalid());
    }
    let recommended_action = match object
        .get("recommended_action")
        .and_then(serde_json::Value::as_str)
    {
        Some("continue") => RecommendedAction::Continue,
        Some("kill_and_resume") => RecommendedAction::KillAndResume,
        Some("kill_and_escalate") => RecommendedAction::KillAndEscalate,
        _ => return Err(diagnosis_invalid()),
    };
    let summary =
        bounded_text(object.get("summary").and_then(serde_json::Value::as_str).ok_or_else(diagnosis_invalid)?, 512)?;
    Ok(ValidatedDiagnosis {
        root_cause_class,
        confidence,
        recommended_action,
        summary,
    })
}

fn diagnosis_invalid() -> AppError {
    AppError::Validation {
        field: "diagnosis_output",
        message: "must match the health diagnosis output schema",
    }
}

fn bounded_text(value: &str, max_bytes: usize) -> Result<String, AppError> {
    if value.is_empty()
        || value.len() > max_bytes
        || value.chars().any(char::is_control)
    {
        return Err(diagnosis_invalid());
    }
    Ok(value.to_owned())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosisEvidenceBundle {
    pub json: String,
    pub digest: String,
}

/// Build the bounded evidence prompt section for one diagnosis run.  The log
/// tail excerpt is capped at [`MAX_DIAGNOSIS_TAIL_BYTES`]; only digest lines
/// ever reach durable storage (`signal_summary_json` evidence digests plus
/// this bundle's digest), never the raw excerpt itself.
pub fn build_diagnosis_evidence(
    row: &RunningHealthRow,
    objective_digest: &str,
    log_tail_excerpt: Option<&str>,
) -> Result<DiagnosisEvidenceBundle, AppError> {
    let signal_summary: serde_json::Value = serde_json::from_str(&row.signal_summary_json)
        .map_err(|source| AppError::Serialization {
            operation: "parse stored running health signal summary",
            source,
        })?;
    let excerpt = log_tail_excerpt.map(|excerpt| {
        let mut end = excerpt.len().min(MAX_DIAGNOSIS_TAIL_BYTES);
        while end > 0 && !excerpt.is_char_boundary(end) {
            end -= 1;
        }
        &excerpt[..end]
    });
    let evidence = serde_json::json!({
        "schema_version": 1,
        "experiment_id": row.experiment_id,
        "campaign_id": row.campaign_id,
        "project_id": row.project_id,
        "pueue_task_id": row.pueue_task_id,
        "objective_digest": objective_digest,
        "signal_summary": signal_summary,
        "log_tail": excerpt.map(|excerpt| {
            serde_json::json!({
                "byte_limit": MAX_DIAGNOSIS_TAIL_BYTES,
                "digest": format!("{:x}", Sha256::digest(excerpt.as_bytes())),
                "excerpt": excerpt,
            })
        }),
    });
    let bytes = serde_json::to_vec(&evidence).map_err(|source| AppError::Serialization {
        operation: "serialize health diagnosis evidence",
        source,
    })?;
    if bytes.len() > MAX_DIAGNOSIS_EVIDENCE_BYTES {
        return Err(AppError::Validation {
            field: "diagnosis_evidence",
            message: "exceeds the serialized evidence limit",
        });
    }
    Ok(DiagnosisEvidenceBundle {
        digest: format!("{:x}", Sha256::digest(&bytes)),
        json: String::from_utf8(bytes).expect("JSON serialization always emits UTF-8"),
    })
}

pub struct StartedDiagnosis {
    pub run_id: i64,
    pub primary_event_id: i64,
    pub experiment_id: String,
    pub event_ids: Vec<i64>,
    pub handle: AgentHandle,
}

#[derive(Default)]
pub struct DiagnosisPassReport {
    pub started: Vec<StartedDiagnosis>,
    pub deferred: usize,
    pub failed_spawns: usize,
    pub cleanups: Vec<crate::agent::BoundCleanupHandle>,
}

/// Spawn at most `claim_limit` bounded diagnosis agents for suspicious rows.
pub async fn run_due_diagnoses(
    db: &Db,
    runner: &AgentRunner,
    claim_limit: usize,
    now: i64,
) -> Result<DiagnosisPassReport, AppError> {
    let mut report = DiagnosisPassReport::default();
    if preflight_decision_runtime().is_err() {
        return Ok(report);
    }
    HealthRepository::requeue_interrupted_diagnoses(db, now)?;
    let candidates = HealthRepository::due_diagnoses(db, claim_limit)?;
    for row in candidates {
        diagnose_row(db, runner, &row, now, &mut report).await?;
    }
    Ok(report)
}

async fn diagnose_row(
    db: &Db,
    runner: &AgentRunner,
    row: &RunningHealthRow,
    now: i64,
    report: &mut DiagnosisPassReport,
) -> Result<(), AppError> {
    let Some(project) = ProjectRepository::new(db).find_by_id(&row.project_id)? else {
        report.deferred += 1;
        return Ok(());
    };
    if !project.enabled || project.paused || project.halted_reason.is_some() {
        report.deferred += 1;
        return Ok(());
    }
    if campaign_defers(db, &row.campaign_id)? {
        report.deferred += 1;
        return Ok(());
    }
    let project_config = config::load(&project.config_path)?;
    let project_policy = match runner.resolve_project_policy(&project, &project_config) {
        Ok(policy) => policy,
        Err(_) => {
            report.deferred += 1;
            return Ok(());
        }
    };

    let objective_digest = CampaignRepository::new(db)
        .find_by_id(&row.campaign_id)?
        .ok_or_else(|| AppError::Validation {
            field: "campaign_id",
            message: "running health row references a missing campaign",
        })?
        .objective_digest;
    let snapshot = read_task_tail(
        &project.root_path.join(".pueue-agent/logs"),
        row.pueue_task_id,
        project_config.check.log_tail_bytes,
    )?;
    let evidence = build_diagnosis_evidence(
        row,
        &objective_digest,
        snapshot.as_ref().map(|snapshot| snapshot.evidence.as_str()),
    )?;

    let attempt_number = row.diagnosis_attempt_count();
    let event = EventRepository::new(db)
        .insert_idempotent(
            &NewEvent::new(
                &row.project_id,
                EventKind::HealthDiagnosis,
                format!(
                    "health-diagnosis:v1:{}:attempt-{attempt_number}",
                    row.experiment_id
                ),
                serde_json::json!({
                    "experiment_id": row.experiment_id,
                    "attempt": attempt_number,
                }),
                now,
                now,
            )
            .with_campaign_lineage(row.campaign_id.as_str(), Some(row.experiment_id.as_str())),
        )?;
    let events = EventRepository::new(db);
    if events.claim_by_id(&row.project_id, event.event_id)?.is_none() {
        report.deferred += 1;
        return Ok(());
    }

    let run_id_guard = match runner.try_acquire_run_id_admission_guard(db).map_err(AppError::from)?
    {
        Some(guard) => guard,
        None => {
            release_claimed_event(db, &[event.event_id], now, now + 60)?;
            report.deferred += 1;
            return Ok(());
        }
    };
    let project_lock = match runner
        .try_acquire_project_admission_lock(&project_policy)
        .map_err(AppError::from)?
    {
        Some(lock) => lock,
        None => {
            release_claimed_event(db, &[event.event_id], now, now + 60)?;
            report.deferred += 1;
            return Ok(());
        }
    };

    HealthRepository::set_state(db, &row.experiment_id, HealthState::Diagnosing, now)?;

    match runner
        .spawn_diagnosis(
            db,
            &project,
            &project_policy,
            &project_config.agent,
            RetryPolicy { max_retries: 0 },
            event.event_id,
            &[event.event_id],
            &row.experiment_id,
            &evidence.json,
            now,
            run_id_guard,
            project_lock,
        )
        .await
    {
        Ok(handle) => {
            report.started.push(StartedDiagnosis {
                run_id: handle.run_id,
                primary_event_id: event.event_id,
                experiment_id: row.experiment_id.clone(),
                event_ids: vec![event.event_id],
                handle,
            });
        }
        Err(AgentSpawnError {
            stage,
            cleanup,
            ..
        }) => {
            HealthRepository::set_state(db, &row.experiment_id, HealthState::Suspicious, now)?;
            release_claimed_event(db, &[event.event_id], now, now + 60)?;
            match (cleanup, stage) {
                (Some(cleanup), _) => {
                    report.cleanups.push(cleanup);
                    report.failed_spawns += 1;
                }
                (None, AgentSpawnStage::PreBinding) => report.deferred += 1,
                (None, _) => report.failed_spawns += 1,
            }
        }
    }
    Ok(())
}

fn release_claimed_event(
    db: &Db,
    event_ids: &[i64],
    now: i64,
    not_before: i64,
) -> Result<(), AppError> {
    EventRepository::new(db)
        .transition_many(
            event_ids,
            EventStatus::RetryWait,
            now,
            Some(not_before),
            None,
        )
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::parse_diagnosis_attempt_count;

    fn diagnosis_bytes(value: &serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(value).unwrap()
    }

    #[test]
    fn valid_diagnosis_outputs_parse_with_all_actions() {
        for action in ["continue", "kill_and_resume", "kill_and_escalate"] {
            let parsed = parse_and_validate_diagnosis(&diagnosis_bytes(&serde_json::json!({
                "root_cause_class": "oom",
                "confidence": 0.9,
                "recommended_action": action,
                "summary": "gpu exhausted",
            })))
            .unwrap();
            assert_eq!(parsed.root_cause_class, "oom");
            assert_eq!(parsed.confidence, 0.9);
            assert_eq!(parsed.summary, "gpu exhausted");
        }
    }

    #[test]
    fn validated_diagnosis_persists_recommended_actions_in_snake_case() {
        let parsed = parse_and_validate_diagnosis(&diagnosis_bytes(&serde_json::json!({
            "root_cause_class": "oom",
            "confidence": 0.9,
            "recommended_action": "kill_and_resume",
            "summary": "gpu exhausted",
        })))
        .unwrap();
        let persisted = serde_json::to_value(&parsed).unwrap();
        assert_eq!(
            persisted["recommended_action"],
            serde_json::Value::String("kill_and_resume".to_owned())
        );
        for (action, expected) in [
            (RecommendedAction::Continue, "continue"),
            (RecommendedAction::KillAndResume, "kill_and_resume"),
            (RecommendedAction::KillAndEscalate, "kill_and_escalate"),
        ] {
            assert_eq!(
                serde_json::to_value(action).unwrap(),
                serde_json::Value::String(expected.to_owned())
            );
        }
    }

    #[test]
    fn diagnosis_outputs_outside_the_schema_are_rejected() {
        let malformed = [
            serde_json::json!({"root_cause_class":"oom","confidence":0.9,"recommended_action":"kill_and_resume"}),
            serde_json::json!({"root_cause_class":"","confidence":0.9,"recommended_action":"kill_and_resume","summary":"s"}),
            serde_json::json!({"root_cause_class":"oom","confidence":1.5,"recommended_action":"kill_and_resume","summary":"s"}),
            serde_json::json!({"root_cause_class":"oom","confidence":0.9,"recommended_action":"restart","summary":"s"}),
            serde_json::json!({"root_cause_class":"oom","confidence":0.9,"recommended_action":"kill_and_resume","summary":""}),
            serde_json::json!({"root_cause_class":"oom","confidence":0.9,"recommended_action":"kill_and_resume","summary":"x".repeat(513)}),
            serde_json::json!({"extra":true}),
        ];
        for value in malformed {
            assert!(parse_and_validate_diagnosis(&diagnosis_bytes(&value)).is_err());
        }
        assert!(parse_and_validate_diagnosis(b"{malformed").is_err());
    }

    #[test]
    fn attempt_counts_survive_wrapper_storage_round_trip() {
        assert_eq!(parse_diagnosis_attempt_count(None), 0);
        assert_eq!(parse_diagnosis_attempt_count(Some("null")), 0);
        assert_eq!(parse_diagnosis_attempt_count(Some(r#"{"attempt":2}"#)), 2);
    }

    #[test]
    fn evidence_bundles_are_bounded_digest_only_persistence_inputs() {
        let row = RunningHealthRow {
            experiment_id: "experiment".to_owned(),
            campaign_id: "campaign".to_owned(),
            project_id: "project".to_owned(),
            pueue_task_id: 41,
            state: HealthState::Suspicious,
            observation_count: 2,
            last_observed_at: 100,
            signal_summary_json: r#"[{"class":"oom","source":"builtin_probe","evidence_digest":"d1","observed_at":90}]"#
                .to_owned(),
            diagnosis_json: None,
            created_at: 50,
            updated_at: 100,
        };
        let bundle = build_diagnosis_evidence(&row, "objective-digest", Some("tail line\n"))
            .unwrap();
        let value: serde_json::Value = serde_json::from_str(&bundle.json).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["experiment_id"], "experiment");
        assert_eq!(value["objective_digest"], "objective-digest");
        assert_eq!(value["log_tail"]["excerpt"], "tail line\n");
        assert_eq!(value["log_tail"]["byte_limit"], MAX_DIAGNOSIS_TAIL_BYTES);
        assert!(bundle.json.len() <= MAX_DIAGNOSIS_EVIDENCE_BYTES);

        let long_tail = "x".repeat(MAX_DIAGNOSIS_TAIL_BYTES * 2);
        let bounded =
            build_diagnosis_evidence(&row, "objective-digest", Some(&long_tail)).unwrap();
        let value: serde_json::Value = serde_json::from_str(&bounded.json).unwrap();
        assert_eq!(
            value["log_tail"]["excerpt"].as_str().unwrap().len(),
            MAX_DIAGNOSIS_TAIL_BYTES
        );
    }
}
