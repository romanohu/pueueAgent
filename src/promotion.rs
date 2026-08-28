//! Promotion engine for declared objective metrics.
//!
//! A completed experiment's primary metric value is compared against the
//! campaign's current best (the baseline experiment's metric before any best
//! exists) inside a single IMMEDIATE transaction that also owns the
//! `current_best_experiment_id` update and the plateau counter transition.

use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::json;

use crate::{
    db::{database_error, insert_event_completed_in_transaction, insert_event_idempotent_in_transaction, Db},
    execution_policy::CampaignLimits,
    models::{CampaignState, EventKind, ExperimentStatus, MetricDirection, NewEvent, ObjectiveMetric},
    AppError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotionOutcome {
    /// The experiment beat the current best by at least `min_delta`:
    /// `current_best_experiment_id` now references it and `plateau_count`
    /// was reset. Re-evaluating the recorded best is an idempotent no-op.
    Improved,
    /// The completing baseline experiment became the first current best
    /// without a prior comparison target: this is bookkeeping, not an
    /// improvement event, so no plateau transition accompanies it.
    BaselineEstablished,
    /// The experiment compared but did not beat the current best beyond
    /// `min_delta`, or completed successfully without a primary metric
    /// value (spec §5-1): `plateau_count` was incremented.
    NotImproved,
    /// The experiment did not complete successfully, carries no primary
    /// metric value with no comparison anchor, or no comparison anchor
    /// exists yet: nothing was evaluated or changed.
    SkippedNoMetric,
    /// The campaign declares no objective metric or is not active:
    /// evaluation is skipped entirely.
    SkippedNoObjective,
}

pub fn evaluate(
    db: &Db,
    campaign_id: &str,
    experiment_id: &str,
    terminal_status: ExperimentStatus,
    limits: &CampaignLimits,
    now: i64,
) -> Result<PromotionOutcome, AppError> {
    let mut connection = db.connect()?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(database_error("begin promotion evaluation"))?;
    let outcome = evaluate_in_transaction(
        &transaction,
        campaign_id,
        experiment_id,
        terminal_status,
        limits,
        now,
    )?;
    transaction
        .commit()
        .map_err(database_error("commit promotion evaluation"))?;
    Ok(outcome)
}

struct CampaignPromotionState {
    project_id: String,
    state: CampaignState,
    objective: Option<ObjectiveMetric>,
    current_best_experiment_id: Option<String>,
    baseline_experiment_id: Option<String>,
}

fn evaluate_in_transaction(
    connection: &Transaction<'_>,
    campaign_id: &str,
    experiment_id: &str,
    terminal_status: ExperimentStatus,
    limits: &CampaignLimits,
    now: i64,
) -> Result<PromotionOutcome, AppError> {
    let campaign = read_campaign_promotion_state(connection, campaign_id)?
        .ok_or(AppError::Validation {
            field: "campaign_id",
            message: "does not identify a campaign",
        })?;

    // Fail-closed: require metrics row before any state mutations.
    // If metrics row is missing, settle the evaluated marker but change no state/plateau.
    let metrics_exists = metrics_row_exists(connection, experiment_id)?;
    if !metrics_exists {
        mark_evaluated(connection, experiment_id, now)?;
        return Ok(PromotionOutcome::SkippedNoMetric);
    }

    // Check if already evaluated - idempotent re-evaluation.
    let is_evaluated = is_already_evaluated(connection, experiment_id)?;
    if is_evaluated {
        return Ok(deduce_already_evaluated_outcome(
            &campaign,
            experiment_id,
            terminal_status,
            connection,
        )?);
    }

    // Inactive campaign or no objective: still settle marker, no state/plateau changes.
    if campaign.state != CampaignState::Active || campaign.objective.is_none() {
        mark_evaluated(connection, experiment_id, now)?;
        return Ok(PromotionOutcome::SkippedNoObjective);
    }

    let Some(ref objective) = campaign.objective else {
        // Should not reach here due to check above, but defensive.
        mark_evaluated(connection, experiment_id, now)?;
        return Ok(PromotionOutcome::SkippedNoObjective);
    };

    // Non-successful terminal: settle marker, no comparison, no plateau change.
    if terminal_status != ExperimentStatus::Succeeded {
        mark_evaluated(connection, experiment_id, now)?;
        return Ok(PromotionOutcome::SkippedNoMetric);
    }

    // Successful terminal with no primary metric value: increment plateau, settle marker.
    let Some(candidate_value) = primary_metric_value(connection, experiment_id)? else {
        increment_plateau(
            connection,
            campaign_id,
            &campaign.project_id,
            experiment_id,
            limits,
            now,
        )?;
        mark_evaluated(connection, experiment_id, now)?;
        return Ok(PromotionOutcome::NotImproved);
    };

    match campaign.current_best_experiment_id.as_deref() {
        Some(best_id) if best_id == experiment_id => {
            mark_evaluated(connection, experiment_id, now)?;
            if campaign.baseline_experiment_id.as_deref() == Some(experiment_id) {
                Ok(PromotionOutcome::BaselineEstablished)
            } else {
                Ok(PromotionOutcome::Improved)
            }
        }
        Some(best_id) => {
            let outcome = compare_and_settle(
                connection,
                campaign_id,
                &campaign.project_id,
                experiment_id,
                candidate_value,
                best_id,
                &objective,
                limits,
                now,
            )?;
            mark_evaluated(connection, experiment_id, now)?;
            Ok(outcome)
        }
        None => match campaign.baseline_experiment_id.as_deref() {
            Some(baseline_id) if baseline_id == experiment_id => {
                promote_baseline(connection, campaign_id, experiment_id, now)?;
                mark_evaluated(connection, experiment_id, now)?;
                Ok(PromotionOutcome::BaselineEstablished)
            }
            Some(baseline_id) => {
                let outcome = compare_and_settle(
                    connection,
                    campaign_id,
                    &campaign.project_id,
                    experiment_id,
                    candidate_value,
                    baseline_id,
                    &objective,
                    limits,
                    now,
                )?;
                mark_evaluated(connection, experiment_id, now)?;
                Ok(outcome)
            }
            None => {
                mark_evaluated(connection, experiment_id, now)?;
                Ok(PromotionOutcome::SkippedNoMetric)
            }
        },
    }
}

fn compare_and_settle(
    connection: &Transaction<'_>,
    campaign_id: &str,
    project_id: &str,
    experiment_id: &str,
    candidate_value: f64,
    best_experiment_id: &str,
    objective: &ObjectiveMetric,
    limits: &CampaignLimits,
    now: i64,
) -> Result<PromotionOutcome, AppError> {
    let Some(best_value) = primary_metric_value(connection, best_experiment_id)? else {
        return Ok(PromotionOutcome::SkippedNoMetric);
    };
    let delta = objective.min_delta.unwrap_or(0.0);
    let improved = match objective.direction {
        MetricDirection::Minimize => candidate_value < best_value - delta,
        MetricDirection::Maximize => candidate_value > best_value + delta,
    };
    if improved {
        promote_challenger(connection, campaign_id, project_id, experiment_id, now)?;
        Ok(PromotionOutcome::Improved)
    } else {
        increment_plateau(connection, campaign_id, project_id, experiment_id, limits, now)?;
        Ok(PromotionOutcome::NotImproved)
    }
}

fn increment_plateau(
    connection: &Transaction<'_>,
    campaign_id: &str,
    project_id: &str,
    experiment_id: &str,
    limits: &CampaignLimits,
    now: i64,
) -> Result<(), AppError> {
    connection
        .execute(
            "UPDATE campaigns
             SET plateau_count = plateau_count + 1, updated_at = ?1
             WHERE campaign_id = ?2",
            rusqlite::params![now, campaign_id],
        )
        .map_err(database_error("increment campaign plateau count"))?;
    let plateau_count: i64 = connection
        .query_row(
            "SELECT plateau_count FROM campaigns WHERE campaign_id = ?1",
            [campaign_id],
            |row| row.get(0),
        )
        .map_err(database_error("read incremented campaign plateau count"))?;
    if plateau_count != i64::from(limits.plateau_threshold) {
        return Ok(());
    }
    emit_strategy_refresh_wake(
        connection,
        campaign_id,
        project_id,
        experiment_id,
        plateau_count,
        now,
    )
}

/// Emit the deduplicated operator wake that escalates a reached plateau to a
/// strategy refresh. The round number is the count of previously emitted
/// refresh wakes plus one, so an improvement reset starts a fresh dedup
/// namespace instead of colliding with the previous round's key. Runs inside
/// the caller's transaction so the wake is atomic with the plateau increment.
fn emit_strategy_refresh_wake(
    connection: &Transaction<'_>,
    campaign_id: &str,
    project_id: &str,
    experiment_id: &str,
    plateau_count: i64,
    now: i64,
) -> Result<(), AppError> {
    let past_rounds: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND campaign_id = ?2
               AND kind = 'operator_wake'
               AND dedup_key LIKE 'strategy-refresh:v1:%'",
            rusqlite::params![project_id, campaign_id],
            |row| row.get(0),
        )
        .map_err(database_error("count prior strategy refresh wakes"))?;
    let round = past_rounds + 1;
    let event = NewEvent::new(
        project_id,
        EventKind::OperatorWake,
        format!("strategy-refresh:v1:{campaign_id}:{round}"),
        json!({
            "source": "promotion",
            "reason": "plateau_threshold_reached",
            "campaign_id": campaign_id,
            "source_experiment_id": experiment_id,
            "plateau_count": plateau_count,
            "round": round,
        }),
        now,
        now,
    )
    .with_campaign_lineage(campaign_id, None::<String>);
    insert_event_idempotent_in_transaction(connection, &event)?;
    Ok(())
}

fn promote_baseline(
    connection: &Connection,
    campaign_id: &str,
    experiment_id: &str,
    now: i64,
) -> Result<(), AppError> {
    connection
        .execute(
            "UPDATE campaigns
             SET current_best_experiment_id = ?1, plateau_count = 0, updated_at = ?2
             WHERE campaign_id = ?3",
            rusqlite::params![experiment_id, now, campaign_id],
        )
        .map_err(database_error("update campaign current best experiment"))?;
    Ok(())
}

fn promote_challenger(
    connection: &Transaction<'_>,
    campaign_id: &str,
    project_id: &str,
    experiment_id: &str,
    now: i64,
) -> Result<(), AppError> {
    connection
        .execute(
            "UPDATE campaigns
             SET current_best_experiment_id = ?1, plateau_count = 0, updated_at = ?2
             WHERE campaign_id = ?3",
            rusqlite::params![experiment_id, now, campaign_id],
        )
        .map_err(database_error("update campaign current best experiment"))?;
    emit_promotion_marker(connection, campaign_id, project_id, experiment_id, now)?;
    Ok(())
}

fn emit_promotion_marker(
    connection: &Transaction<'_>,
    campaign_id: &str,
    project_id: &str,
    experiment_id: &str,
    now: i64,
) -> Result<(), AppError> {
    let dedup_key = format!("promotion:v1:{campaign_id}:{experiment_id}");
    let event = NewEvent::new(
        project_id,
        EventKind::OperatorWake,
        dedup_key,
        json!({
            "source": "promotion",
            "reason": "improved",
            "campaign_id": campaign_id,
            "experiment_id": experiment_id,
        }),
        now,
        now,
    )
    .with_campaign_lineage(campaign_id.to_owned(), Some(experiment_id.to_owned()));
    // Insert as 'completed' to make the audit passive - never pending or scheduler-dispatchable.
    // Retry idempotent via dedup_key.
    insert_event_completed_in_transaction(connection, &event)?;
    Ok(())
}

fn is_already_evaluated(
    connection: &Connection,
    experiment_id: &str,
) -> Result<bool, AppError> {
    let evaluated: Option<Option<String>> = connection
        .query_row(
            "SELECT evaluated_at FROM experiment_metrics WHERE experiment_id = ?1",
            [experiment_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(database_error("read experiment evaluated marker"))?;
    Ok(matches!(evaluated, Some(Some(_))))
}

fn metrics_row_exists(
    connection: &Connection,
    experiment_id: &str,
) -> Result<bool, AppError> {
    let exists: Option<i64> = connection
        .query_row(
            "SELECT 1 FROM experiment_metrics WHERE experiment_id = ?1",
            [experiment_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(database_error("check experiment metrics row exists"))?;
    Ok(exists.is_some())
}

fn mark_evaluated(
    connection: &Connection,
    experiment_id: &str,
    now: i64,
) -> Result<(), AppError> {
    let now_text = now.to_string();
    connection
        .execute(
            "UPDATE experiment_metrics SET evaluated_at = ?1, updated_at = ?1
              WHERE experiment_id = ?2 AND evaluated_at IS NULL",
            rusqlite::params![now_text, experiment_id],
        )
        .map_err(database_error("mark experiment evaluated"))?;
    Ok(())
}

fn deduce_already_evaluated_outcome(
    campaign: &CampaignPromotionState,
    experiment_id: &str,
    terminal_status: ExperimentStatus,
    connection: &Connection,
) -> Result<PromotionOutcome, AppError> {
    if campaign.state != CampaignState::Active || campaign.objective.is_none() {
        return Ok(PromotionOutcome::SkippedNoObjective);
    }
    if terminal_status != ExperimentStatus::Succeeded {
        return Ok(PromotionOutcome::SkippedNoMetric);
    }
    if primary_metric_value(connection, experiment_id)?.is_none() {
        return Ok(PromotionOutcome::NotImproved);
    }
    match campaign.current_best_experiment_id.as_deref() {
        Some(best_id) if best_id == experiment_id => {
            if campaign.baseline_experiment_id.as_deref() == Some(experiment_id) {
                Ok(PromotionOutcome::BaselineEstablished)
            } else {
                Ok(PromotionOutcome::Improved)
            }
        }
        Some(_) => {
            // Without recomputing delta, return NotImproved as idempotent default
            // for already-evaluated challengers that are not the best.
            // This avoids double-counting plateau.
            Ok(PromotionOutcome::NotImproved)
        }
        None => {
            if campaign.baseline_experiment_id.as_deref() == Some(experiment_id) {
                Ok(PromotionOutcome::BaselineEstablished)
            } else {
                Ok(PromotionOutcome::SkippedNoMetric)
            }
        }
    }
}

fn read_campaign_promotion_state(
    connection: &Connection,
    campaign_id: &str,
) -> Result<Option<CampaignPromotionState>, AppError> {
    connection
        .query_row(
            "SELECT project_id, state, objective_metric_json, current_best_experiment_id,
                    baseline_experiment_id
             FROM campaigns WHERE campaign_id = ?1",
            [campaign_id],
            |row| {
                Ok(CampaignPromotionState {
                    project_id: row.get(0)?,
                    state: row.get(1)?,
                    objective: match row.get::<_, Option<String>>(2)? {
                        None => None,
                        Some(text) => Some(serde_json::from_str(&text).map_err(|source| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(source),
                            )
                        })?),
                    },
                    current_best_experiment_id: row.get(3)?,
                    baseline_experiment_id: row.get(4)?,
                })
            },
        )
        .optional()
        .map_err(database_error("read campaign promotion state"))
}

fn primary_metric_value(
    connection: &Connection,
    experiment_id: &str,
) -> Result<Option<f64>, AppError> {
    Ok(connection
        .query_row(
            "SELECT primary_metric_value FROM experiment_metrics WHERE experiment_id = ?1",
            [experiment_id],
            |row| row.get::<_, Option<f64>>(0),
        )
        .optional()
        .map_err(database_error("read experiment primary metric"))?
        .flatten())
}

fn ensure_metrics_row_with_evaluated(
    connection: &Connection,
    experiment_id: &str,
    now: i64,
) -> Result<(), AppError> {
    // Insert a minimal metrics row with evaluated_at set to settle the marker.
    // This handles the fail-closed case where evaluation runs but no metrics row exists.
    let now_text = now.to_string();
    connection
        .execute(
            "INSERT INTO experiment_metrics (
                experiment_id, source, primary_metric_name, primary_metric_value,
                metrics_json, artifact_defect, created_at, updated_at, evaluated_at
             ) VALUES (?1, 'manifest', NULL, NULL, '{}', 'evaluation_missing_row', ?2, ?2, ?3)
             ON CONFLICT(experiment_id) DO UPDATE SET
                evaluated_at = COALESCE(experiment_metrics.evaluated_at, excluded.evaluated_at),
                updated_at = ?2
             WHERE experiment_metrics.evaluated_at IS NULL",
            rusqlite::params![experiment_id, now, now_text],
        )
        .map_err(database_error("ensure metrics row with evaluated marker"))?;
    Ok(())
}

