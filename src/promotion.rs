//! Promotion engine for declared objective metrics.
//!
//! A completed experiment's primary metric value is compared against the
//! campaign's current best (the baseline experiment's metric before any best
//! exists) inside a single IMMEDIATE transaction that also owns the
//! `current_best_experiment_id` update and the plateau counter transition.

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

use crate::{
    db::{database_error, Db},
    models::{CampaignState, ExperimentStatus, MetricDirection, ObjectiveMetric},
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
        now,
    )?;
    transaction
        .commit()
        .map_err(database_error("commit promotion evaluation"))?;
    Ok(outcome)
}

struct CampaignPromotionState {
    state: CampaignState,
    objective: Option<ObjectiveMetric>,
    current_best_experiment_id: Option<String>,
    baseline_experiment_id: Option<String>,
}

fn evaluate_in_transaction(
    connection: &Connection,
    campaign_id: &str,
    experiment_id: &str,
    terminal_status: ExperimentStatus,
    now: i64,
) -> Result<PromotionOutcome, AppError> {
    let campaign = read_campaign_promotion_state(connection, campaign_id)?
        .ok_or(AppError::Validation {
            field: "campaign_id",
            message: "does not identify a campaign",
        })?;
    if campaign.state != CampaignState::Active {
        return Ok(PromotionOutcome::SkippedNoObjective);
    }
    let Some(objective) = campaign.objective else {
        return Ok(PromotionOutcome::SkippedNoObjective);
    };
    if terminal_status != ExperimentStatus::Succeeded {
        return Ok(PromotionOutcome::SkippedNoMetric);
    }
    let Some(candidate_value) = primary_metric_value(connection, experiment_id)? else {
        increment_plateau(connection, campaign_id, now)?;
        return Ok(PromotionOutcome::NotImproved);
    };

    match campaign.current_best_experiment_id.as_deref() {
        Some(best_id) if best_id == experiment_id => {
            if campaign.baseline_experiment_id.as_deref() == Some(experiment_id) {
                Ok(PromotionOutcome::BaselineEstablished)
            } else {
                Ok(PromotionOutcome::Improved)
            }
        }
        Some(best_id) => compare_and_settle(
            connection,
            campaign_id,
            experiment_id,
            candidate_value,
            best_id,
            &objective,
            now,
        ),
        None => match campaign.baseline_experiment_id.as_deref() {
            Some(baseline_id) if baseline_id == experiment_id => {
                promote(connection, campaign_id, experiment_id, now)?;
                Ok(PromotionOutcome::BaselineEstablished)
            }
            Some(baseline_id) => compare_and_settle(
                connection,
                campaign_id,
                experiment_id,
                candidate_value,
                baseline_id,
                &objective,
                now,
            ),
            None => Ok(PromotionOutcome::SkippedNoMetric),
        },
    }
}

fn compare_and_settle(
    connection: &Connection,
    campaign_id: &str,
    experiment_id: &str,
    candidate_value: f64,
    best_experiment_id: &str,
    objective: &ObjectiveMetric,
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
        promote(connection, campaign_id, experiment_id, now)?;
        Ok(PromotionOutcome::Improved)
    } else {
        increment_plateau(connection, campaign_id, now)?;
        Ok(PromotionOutcome::NotImproved)
    }
}

fn increment_plateau(
    connection: &Connection,
    campaign_id: &str,
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
    Ok(())
}

fn promote(
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

fn read_campaign_promotion_state(
    connection: &Connection,
    campaign_id: &str,
) -> Result<Option<CampaignPromotionState>, AppError> {
    connection
        .query_row(
            "SELECT state, objective_metric_json, current_best_experiment_id,
                    baseline_experiment_id
             FROM campaigns WHERE campaign_id = ?1",
            [campaign_id],
            |row| {
                Ok(CampaignPromotionState {
                    state: row.get(0)?,
                    objective: match row.get::<_, Option<String>>(1)? {
                        None => None,
                        Some(text) => Some(serde_json::from_str(&text).map_err(|source| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                rusqlite::types::Type::Text,
                                Box::new(source),
                            )
                        })?),
                    },
                    current_best_experiment_id: row.get(2)?,
                    baseline_experiment_id: row.get(3)?,
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

