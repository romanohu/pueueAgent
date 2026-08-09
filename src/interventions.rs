pub use crate::models::InterventionStatus;

use crate::AppError;

pub const MAX_INTERVENTION_BYTES: usize = 4 * 1024;
pub const MAX_INTERVENTIONS_PER_RUN: usize = 16;
pub const MAX_INTERVENTION_BYTES_PER_RUN: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intervention {
    pub intervention_id: String,
    pub project_id: String,
    pub message: String,
    pub status: InterventionStatus,
    pub created_at: i64,
    pub reserved_at: Option<i64>,
    pub applied_at: Option<i64>,
    pub agent_run_id: Option<i64>,
    pub attempts: i64,
    pub lease_expires_at: Option<i64>,
    pub reservation_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterventionCounts {
    pub pending: i64,
    pub reserved: i64,
    pub applied: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterventionReservation {
    pub token: String,
    pub items: Vec<Intervention>,
}

pub fn validate_message(message: &str) -> Result<(), AppError> {
    if message.trim().is_empty() || message.len() > MAX_INTERVENTION_BYTES {
        return Err(AppError::Configuration {
            field: "intervention_message",
        });
    }
    Ok(())
}
