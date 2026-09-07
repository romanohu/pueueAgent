//! Promotion engine for declared objective metrics.
//!
//! A completed experiment's primary metric value is compared against the
//! campaign's current best (the baseline experiment's metric before any best
//! exists) inside a single IMMEDIATE transaction that also owns the
//! `current_best_experiment_id` update and the plateau counter transition.

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior};
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

impl PromotionOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Improved => "improved",
            Self::BaselineEstablished => "baseline_established",
            Self::NotImproved => "not_improved",
            Self::SkippedNoMetric => "skipped_no_metric",
            Self::SkippedNoObjective => "skipped_no_objective",
        }
    }

    pub fn from_str(value: &str) -> Result<Self, AppError> {
        match value {
            "improved" => Ok(Self::Improved),
            "baseline_established" => Ok(Self::BaselineEstablished),
            "not_improved" => Ok(Self::NotImproved),
            "skipped_no_metric" => Ok(Self::SkippedNoMetric),
            "skipped_no_objective" => Ok(Self::SkippedNoObjective),
            _ => Err(AppError::Validation {
                field: "promotion_outcome",
                message: "is not a recognized code-change promotion outcome",
            }),
        }
    }
}

/// The immutable comparison decision persisted around the Git ref boundary.
/// The target is present only for an improvement; all other outcomes retain
/// the candidate run's revision in the run row while leaving the best ref
/// untouched.  Campaign and experiment IDs are retained privately so the
/// finalize API cannot be redirected to another lineage by a caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodePromotionPlan {
    pub outcome: PromotionOutcome,
    pub expected_current_best_experiment_id: Option<String>,
    pub expected_old_sha: Option<String>,
    pub candidate_sha: Option<String>,
    pub(crate) campaign_id: String,
    pub(crate) experiment_id: String,
}

impl CodePromotionPlan {
    /// Reconstruct the exact plan persisted by a code-change promotion intent.
    /// The candidate revision is supplied by the immutable run lineage; the
    /// four remaining values are the only durable promotion inputs.
    pub(crate) fn from_persisted(
        campaign_id: String,
        experiment_id: String,
        candidate_sha: &str,
        promotion_outcome: Option<&str>,
        expected_current_best_experiment_id: Option<String>,
        expected_old_sha: Option<String>,
        promotion_target_sha: Option<String>,
    ) -> Result<Self, AppError> {
        let outcome = promotion_outcome.ok_or(AppError::Validation {
            field: "promotion_outcome",
            message: "is required before code-change promotion finalization",
        })?;
        let outcome = PromotionOutcome::from_str(outcome)?;
        validate_code_candidate_sha(candidate_sha)?;
        validate_optional_code_sha(expected_old_sha.as_deref())?;
        match outcome {
            PromotionOutcome::Improved => {
                let target = promotion_target_sha.as_deref().ok_or(AppError::Validation {
                    field: "promotion_target_sha",
                    message: "improved code-change plan requires a candidate target SHA",
                })?;
                validate_code_candidate_sha(target)?;
                if target != candidate_sha {
                    return Err(AppError::Validation {
                        field: "promotion_target_sha",
                        message: "must match the immutable candidate SHA",
                    });
                }
            }
            _ if promotion_target_sha.is_some() => {
                return Err(AppError::Validation {
                    field: "promotion_target_sha",
                    message: "non-improved code-change plan cannot carry a target SHA",
                });
            }
            _ => {}
        }
        Ok(Self {
            outcome,
            expected_current_best_experiment_id,
            expected_old_sha,
            candidate_sha: promotion_target_sha,
            campaign_id,
            experiment_id,
        })
    }
}

fn validate_code_candidate_sha(value: &str) -> Result<(), AppError> {
    if (value.len() != 40 && value.len() != 64)
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(AppError::Validation {
            field: "code_change.candidate_sha",
            message: "must be a lowercase full object ID",
        });
    }
    Ok(())
}

fn validate_optional_code_sha(value: Option<&str>) -> Result<(), AppError> {
    if let Some(value) = value {
        validate_code_candidate_sha(value)?;
    }
    Ok(())
}

/// Compare a terminal code-change experiment without changing campaign,
/// metric, or audit state.  The caller holds the same IMMEDIATE transaction
/// that will persist the returned intent, so the comparison snapshot and
/// durable promotion fields share one SQLite boundary.
pub fn preview_code_candidate(
    connection: &Transaction<'_>,
    campaign_id: &str,
    experiment_id: &str,
    terminal_status: ExperimentStatus,
    limits: &CampaignLimits,
    expected_old_sha: Option<&str>,
    candidate_sha: &str,
) -> Result<CodePromotionPlan, AppError> {
    validate_code_candidate_sha(candidate_sha)?;
    validate_optional_code_sha(expected_old_sha)?;
    let campaign = read_campaign_promotion_state(connection, campaign_id)?
        .ok_or(AppError::Validation {
            field: "campaign_id",
            message: "does not identify a campaign",
        })?;
    if let Some(objective) = campaign.objective.as_ref() {
        objective.validate()?;
    }
    validate_experiment_campaign(connection, campaign_id, experiment_id, "experiment_id")?;
    for (field, comparison_id) in [
        (
            "current_best_experiment_id",
            campaign.current_best_experiment_id.as_deref(),
        ),
        (
            "baseline_experiment_id",
            campaign.baseline_experiment_id.as_deref(),
        ),
    ] {
        if let Some(comparison_id) = comparison_id {
            validate_experiment_campaign(connection, campaign_id, comparison_id, field)?;
        }
    }
    if !metrics_row_exists(connection, experiment_id)? {
        return Err(AppError::Validation {
            field: "experiment_id",
            message: "missing experiment_metrics row; evaluation aborted",
        });
    }

    let outcome = if campaign.state != CampaignState::Active || campaign.objective.is_none() {
        PromotionOutcome::SkippedNoObjective
    } else if terminal_status != ExperimentStatus::Succeeded {
        PromotionOutcome::SkippedNoMetric
    } else {
        match primary_metric_value(connection, campaign_id, experiment_id)? {
            None => PromotionOutcome::NotImproved,
            Some(candidate_value) => match campaign.current_best_experiment_id.as_deref() {
                Some(best_id) if best_id == experiment_id => {
                    if campaign.baseline_experiment_id.as_deref() == Some(experiment_id) {
                        PromotionOutcome::BaselineEstablished
                    } else {
                        PromotionOutcome::Improved
                    }
                }
                Some(best_id) => compare_preview(
                    connection,
                    campaign_id,
                    candidate_value,
                    best_id,
                    campaign.objective.as_ref().expect("objective checked above"),
                    limits,
                )?,
                None => match campaign.baseline_experiment_id.as_deref() {
                    Some(baseline_id) if baseline_id == experiment_id => {
                        PromotionOutcome::BaselineEstablished
                    }
                    Some(baseline_id) => compare_preview(
                        connection,
                        campaign_id,
                        candidate_value,
                        baseline_id,
                        campaign.objective.as_ref().expect("objective checked above"),
                        limits,
                    )?,
                    None => PromotionOutcome::SkippedNoMetric,
                },
            },
        }
    };
    Ok(CodePromotionPlan {
        outcome,
        expected_current_best_experiment_id: campaign.current_best_experiment_id,
        expected_old_sha: expected_old_sha.map(str::to_owned),
        candidate_sha: (outcome == PromotionOutcome::Improved).then(|| candidate_sha.to_owned()),
        campaign_id: campaign_id.to_owned(),
        experiment_id: experiment_id.to_owned(),
    })
}

fn compare_preview(
    connection: &Transaction<'_>,
    campaign_id: &str,
    candidate_value: f64,
    best_experiment_id: &str,
    objective: &ObjectiveMetric,
    _limits: &CampaignLimits,
) -> Result<PromotionOutcome, AppError> {
    let Some(best_value) = primary_metric_value(connection, campaign_id, best_experiment_id)? else {
        return Ok(PromotionOutcome::SkippedNoMetric);
    };
    let delta = objective.min_delta.unwrap_or(0.0);
    let improved = match objective.direction {
        MetricDirection::Minimize => candidate_value < best_value - delta,
        MetricDirection::Maximize => candidate_value > best_value + delta,
    };
    Ok(if improved {
        PromotionOutcome::Improved
    } else {
        PromotionOutcome::NotImproved
    })
}

/// Apply a previously persisted code-candidate comparison inside the caller's
/// IMMEDIATE transaction.  The expected current-best predicate is checked
/// before any campaign, plateau, or audit mutation; callers persist the
/// metric evaluated marker in the same transaction after this returns.
pub fn finalize_code_candidate(
    connection: &Transaction<'_>,
    plan: &CodePromotionPlan,
    limits: &CampaignLimits,
    now: i64,
) -> Result<PromotionOutcome, AppError> {
    match plan.outcome {
        PromotionOutcome::Improved => {
            let target = plan.candidate_sha.as_deref().ok_or(AppError::Validation {
                field: "promotion_target_sha",
                message: "improved code-change plan requires a candidate target SHA",
            })?;
            validate_code_candidate_sha(target)?;
        }
        _ if plan.candidate_sha.is_some() => {
            return Err(AppError::Validation {
                field: "promotion_target_sha",
                message: "non-improved code-change plan cannot carry a target SHA",
            });
        }
        _ => {}
    }
    validate_optional_code_sha(plan.expected_old_sha.as_deref())?;
    let campaign = read_campaign_promotion_state(connection, &plan.campaign_id)?
        .ok_or(AppError::Validation {
            field: "campaign_id",
            message: "does not identify a campaign",
        })?;
    validate_experiment_campaign(
        connection,
        &plan.campaign_id,
        &plan.experiment_id,
        "experiment_id",
    )?;
    if let Some(expected_best) = plan.expected_current_best_experiment_id.as_deref() {
        validate_experiment_campaign(
            connection,
            &plan.campaign_id,
            expected_best,
            "current_best_experiment_id",
        )?;
    }
    if campaign.current_best_experiment_id != plan.expected_current_best_experiment_id {
        return Err(AppError::Validation {
            field: "current_best_experiment_id",
            message: "changed since the code-change promotion preview",
        });
    }
    if !metrics_row_exists(connection, &plan.experiment_id)? {
        return Err(AppError::Validation {
            field: "experiment_id",
            message: "missing experiment_metrics row; finalization aborted",
        });
    }
    if is_already_evaluated(connection, &plan.experiment_id)? {
        return Ok(plan.outcome);
    }
    match plan.outcome {
        PromotionOutcome::Improved => {
            let campaign = read_campaign_promotion_state(connection, &plan.campaign_id)?
                .ok_or(AppError::Validation {
                    field: "campaign_id",
                    message: "does not identify a campaign",
                })?;
            promote_challenger(
                connection,
                &plan.campaign_id,
                &campaign.project_id,
                &plan.experiment_id,
                now,
            )?;
        }
        PromotionOutcome::BaselineEstablished => {
            promote_baseline(connection, &plan.campaign_id, &plan.experiment_id, now)?;
        }
        PromotionOutcome::NotImproved => {
            campaign.objective.as_ref().ok_or(AppError::Validation {
                field: "objective_metric",
                message: "not-improved code-change plan requires an objective",
            })?;
            increment_plateau(
                connection,
                &plan.campaign_id,
                &campaign.project_id,
                &plan.experiment_id,
                limits,
                now,
            )?;
        }
        PromotionOutcome::SkippedNoMetric | PromotionOutcome::SkippedNoObjective => {}
    }
    Ok(plan.outcome)
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
    if let Some(objective) = campaign.objective.as_ref() {
        objective.validate()?;
    }

    validate_experiment_campaign(connection, campaign_id, experiment_id, "experiment_id")?;
    for (field, comparison_id) in [
        (
            "current_best_experiment_id",
            campaign.current_best_experiment_id.as_deref(),
        ),
        (
            "baseline_experiment_id",
            campaign.baseline_experiment_id.as_deref(),
        ),
    ] {
        if let Some(comparison_id) = comparison_id {
            validate_experiment_campaign(connection, campaign_id, comparison_id, field)?;
        }
    }

    // Fail-closed: require metrics row before any state mutations.
    // If metrics row is missing, return error without any mutation or evaluated_at marking.
    let metrics_exists = metrics_row_exists(connection, experiment_id)?;
    if !metrics_exists {
        return Err(AppError::Validation {
            field: "experiment_id",
            message: "missing experiment_metrics row; evaluation aborted",
        });
    }

    // Check if already evaluated - idempotent re-evaluation.
    let is_evaluated = is_already_evaluated(connection, experiment_id)?;
    if is_evaluated {
        return Ok(deduce_already_evaluated_outcome(
            &campaign,
            campaign_id,
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
    let Some(candidate_value) = primary_metric_value(connection, campaign_id, experiment_id)? else {
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
    let Some(best_value) = primary_metric_value(connection, campaign_id, best_experiment_id)? else {
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

fn validate_experiment_campaign(
    connection: &Connection,
    campaign_id: &str,
    experiment_id: &str,
    field: &'static str,
) -> Result<(), AppError> {
    let experiment_campaign = connection
        .query_row(
            "SELECT campaign_id FROM experiments WHERE experiment_id = ?1",
            [experiment_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(database_error("validate experiment campaign lineage"))?;
    match experiment_campaign {
        Some(experiment_campaign) if experiment_campaign == campaign_id => Ok(()),
        Some(_) => Err(AppError::Validation {
            field,
            message: "must belong to the same campaign",
        }),
        None => Err(AppError::Validation {
            field,
            message: "does not identify an experiment",
        }),
    }
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

pub(crate) fn mark_evaluated(
    connection: &Connection,
    experiment_id: &str,
    now: i64,
) -> Result<(), AppError> {
    let now_text = now.to_string();
    let affected = connection
        .execute(
            "UPDATE experiment_metrics SET evaluated_at = ?1, updated_at = ?1
              WHERE experiment_id = ?2 AND evaluated_at IS NULL",
            rusqlite::params![now_text, experiment_id],
        )
        .map_err(database_error("mark experiment evaluated"))?;
    if affected != 1 {
        return Err(AppError::Validation {
            field: "experiment_id",
            message: "mark_evaluated expected exactly one affected row",
        });
    }
    Ok(())
}

fn deduce_already_evaluated_outcome(
    campaign: &CampaignPromotionState,
    campaign_id: &str,
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
    if primary_metric_value(connection, campaign_id, experiment_id)?.is_none() {
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
    campaign_id: &str,
    experiment_id: &str,
) -> Result<Option<f64>, AppError> {
    Ok(connection
        .query_row(
            "SELECT em.primary_metric_value
             FROM experiment_metrics em
             JOIN experiments e ON e.experiment_id = em.experiment_id
             WHERE em.experiment_id = ?1
               AND e.campaign_id = ?2
               AND em.artifact_defect IS NULL",
            rusqlite::params![experiment_id, campaign_id],
            |row| row.get::<_, Option<f64>>(0),
        )
        .optional()
        .map_err(database_error("read experiment primary metric"))?
        .flatten())
}
