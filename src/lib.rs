pub mod agent;
pub mod batches;
pub mod campaign;
pub mod cancel;
pub mod cli;
pub mod codex_command;
pub mod codex_session;
pub mod config;
pub mod daemon;
pub mod decision;
pub mod decision_evidence;
pub mod decision_protocol;
pub mod db;
pub mod detect;
pub mod diagnostics;
pub mod environment;
pub mod error;
pub mod execution_policy;
pub mod events;
pub mod guardrails;
pub mod health;
pub mod health_diagnosis;
pub mod incidents;
pub mod init;
pub mod interventions;
pub mod logs;
pub mod models;
pub mod native_launcher;
pub mod output;
pub mod paths;
pub mod periodic;
pub mod process;
pub mod project;
pub mod project_logs;
pub mod promotion;
pub mod proposals;
pub mod pueue;
pub mod pueue_process;
pub mod pueue_security;
pub mod reconcile;
pub mod result_manifest;
pub mod retry;
pub mod runs;
pub mod scheduler;
pub mod service;
pub mod signals;
pub mod state;
pub mod status;
pub mod submit;
pub mod termination;
pub mod upgrade;
pub mod version;

pub use error::AppError;

#[cfg(test)]
mod retry_contract_tests {
    use super::retry::{retry_backoff_seconds, retry_decision, RetryDecision, RetryPolicy};

    #[test]
    fn retry_policy_uses_attempt_number_and_zero_retry_is_dead_letter() {
        assert_eq!(
            retry_decision(1, 1_000, RetryPolicy { max_retries: 0 }),
            RetryDecision::DeadLetter
        );
        assert_eq!(
            retry_decision(1, 1_000, RetryPolicy { max_retries: 2 }),
            RetryDecision::Retry { not_before: 1_060 }
        );
        assert_eq!(
            retry_decision(2, 1_000, RetryPolicy { max_retries: 2 }),
            RetryDecision::Retry { not_before: 1_120 }
        );
        assert_eq!(
            retry_decision(3, 1_000, RetryPolicy { max_retries: 2 }),
            RetryDecision::DeadLetter
        );
        assert_eq!(retry_backoff_seconds(20), 3_840);
    }
}
