#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_retries: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    Retry { not_before: i64 },
    DeadLetter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventResolution {
    RetryPolicy(RetryPolicy),
    ExecutionUnknown { reason: String },
}

pub fn retry_decision(attempts: i64, now: i64, policy: RetryPolicy) -> RetryDecision {
    if attempts <= i64::from(policy.max_retries) {
        RetryDecision::Retry {
            not_before: now.saturating_add(retry_backoff_seconds(attempts)),
        }
    } else {
        RetryDecision::DeadLetter
    }
}

pub fn retry_backoff_seconds(attempts: i64) -> i64 {
    let exponent = attempts.saturating_sub(1).clamp(0, 6) as u32;
    60_i64.saturating_mul(2_i64.saturating_pow(exponent))
}
