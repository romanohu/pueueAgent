use crate::{
    code_change,
    events::new_code_change_transition_event,
    models::{
        CodeChangeCheck, CodeChangeCheckStatus, CodeChangeEditorAttempt, CodeChangeRun,
        CodeChangeState, NewCodeChangeRun,
    },
    output::bounded_redacted_text,
    AppError,
};
use rusqlite::{params, Connection, OptionalExtension, Row, Transaction, TransactionBehavior};

use super::{database_error, insert_event_completed_in_transaction, Db};

const MAX_CODE_CHANGE_ID_BYTES: usize = 256;
const MAX_CODE_CHANGE_SUMMARY_BYTES: usize = 240;
const MAX_CODE_CHANGE_DIGEST_BYTES: usize = 128;
const MAX_CODE_CHANGE_REJECTION_CODE_BYTES: usize = 128;
const MAX_CODE_CHANGE_CHECKS: usize = 8;
const MAX_CODE_CHANGE_ARGV_JSON_BYTES: usize = 16 * 1024;
const MAX_CODE_CHANGE_LIMIT: usize = 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCodeChangeCheck {
    pub attempt: i64,
    pub ordinal: i64,
    pub source: String,
    pub argv: Vec<String>,
    pub working_directory: String,
    pub status: CodeChangeCheckStatus,
    pub output_digest: Option<String>,
    pub summary: Option<String>,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
}

impl NewCodeChangeCheck {
    pub fn new(
        attempt: i64,
        ordinal: i64,
        source: impl Into<String>,
        argv: Vec<String>,
        working_directory: impl Into<String>,
    ) -> Self {
        Self {
            attempt,
            ordinal,
            source: source.into(),
            argv,
            working_directory: working_directory.into(),
            status: CodeChangeCheckStatus::Reserved,
            output_digest: None,
            summary: None,
            started_at: None,
            finished_at: None,
        }
    }
}

pub struct CodeChangeRepository<'db> {
    db: &'db Db,
}

impl<'db> CodeChangeRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn create_pending(&self, run: &NewCodeChangeRun) -> Result<CodeChangeRun, AppError> {
        validate_new_run(run)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin code-change run insertion"))?;
        let proposal_campaign: Option<String> = transaction
            .query_row(
                "SELECT campaign_id FROM proposals
                 WHERE proposal_id = ?1 AND campaign_id = ?2 AND kind = 'code_change'",
                params![run.proposal_id, run.campaign_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("validate code-change proposal"))?;
        if proposal_campaign.is_none() {
            return Err(AppError::Validation {
                field: "code_change.proposal_id",
                message: "must identify a code-change proposal in the campaign",
            });
        }
        transaction
            .execute(
                "INSERT INTO code_change_runs (
                    code_change_run_id, proposal_id, campaign_id, state, base_sha,
                    candidate_sha, candidate_ref, best_ref, worktree_id,
                    worktree_relative_path, editor_session_id, editor_attempts,
                    diff_digest, changed_file_count, diff_bytes, experiment_id,
                    rejection_code, rejection_summary, promotion_outcome,
                 promotion_expected_best_experiment_id, promotion_expected_old_sha,
                    promotion_target_sha, cleanup_completed_at,
                    state_root_identity, worktrees_identity, campaign_identity,
                    candidate_root_identity, candidate_admin_identity,
                    candidate_common_identity, candidate_admin_path,
                    candidate_common_path, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8, ?9, ?10,
                           0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL, NULL, NULL, ?11, ?11)",
                params![
                    run.code_change_run_id,
                    run.proposal_id,
                    run.campaign_id,
                    CodeChangeState::Reserved,
                    run.base_sha,
                    run.candidate_ref,
                    run.best_ref,
                    run.worktree_id,
                    run.worktree_relative_path,
                    run.editor_session_id,
                    run.created_at,
                ],
            )
            .map_err(database_error("insert code-change run"))?;
        let stored = read_run(&transaction, &run.code_change_run_id)?;
        insert_lifecycle_event(&transaction, &stored, 0, None, run.created_at)?;
        transaction
            .commit()
            .map_err(database_error("commit code-change run insertion"))?;
        read_by_id(&connection, &run.code_change_run_id)
    }

    pub fn find_by_id(&self, run_id: &str) -> Result<Option<CodeChangeRun>, AppError> {
        let connection = self.db.connect()?;
        find_by_id_connection(&connection, run_id)
    }

    pub fn find_by_proposal(&self, proposal_id: &str) -> Result<Option<CodeChangeRun>, AppError> {
        validate_identifier("proposal_id", proposal_id)?;
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{CODE_CHANGE_SELECT} WHERE proposal_id = ?1"),
                [proposal_id],
                code_change_run_from_row,
            )
            .optional()
            .map_err(database_error("find code-change run by proposal"))
    }

    pub fn find_editor_attempt(
        &self,
        run_id: &str,
        attempt: i64,
    ) -> Result<Option<CodeChangeEditorAttempt>, AppError> {
        validate_identifier("code_change_run_id", run_id)?;
        if !(1..=2).contains(&attempt) {
            return Err(AppError::Validation {
                field: "code_change.attempt",
                message: "must be one of the two bounded editor attempts",
            });
        }
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT code_change_run_id, attempt, agent_run_id, editor_session_id, status,
                        result_digest, failure_code, failure_summary, started_at, finished_at
                 FROM code_change_editor_attempts
                 WHERE code_change_run_id = ?1 AND attempt = ?2",
                params![run_id, attempt],
                code_change_editor_attempt_from_row,
            )
            .optional()
            .map_err(database_error("find code-change editor attempt"))
    }

    pub fn list_editor_attempts(
        &self,
        run_id: &str,
    ) -> Result<Vec<CodeChangeEditorAttempt>, AppError> {
        validate_identifier("code_change_run_id", run_id)?;
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT code_change_run_id, attempt, agent_run_id, editor_session_id, status,
                        result_digest, failure_code, failure_summary, started_at, finished_at
                 FROM code_change_editor_attempts
                 WHERE code_change_run_id = ?1
                 ORDER BY attempt",
            )
            .map_err(database_error("prepare code-change editor attempt list"))?;
        let rows = statement
            .query_map([run_id], code_change_editor_attempt_from_row)
            .map_err(database_error("query code-change editor attempts"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read code-change editor attempts"))
    }

    pub fn list_recoverable(&self, limit: usize) -> Result<Vec<CodeChangeRun>, AppError> {
        let limit = limit.min(MAX_CODE_CHANGE_LIMIT) as i64;
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{CODE_CHANGE_SELECT}
                 WHERE state <> 'completed'
                   AND cleanup_completed_at IS NULL
                 ORDER BY updated_at, code_change_run_id
                 LIMIT ?1"
            ))
            .map_err(database_error("prepare recoverable code-change runs"))?;
        let rows = statement
            .query_map([limit], code_change_run_from_row)
            .map_err(database_error("query recoverable code-change runs"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read recoverable code-change runs"))
    }

    pub fn transition(
        &self,
        run_id: &str,
        expected: CodeChangeState,
        next: CodeChangeState,
        now: i64,
    ) -> Result<CodeChangeRun, AppError> {
        validate_identifier("code_change_run_id", run_id)?;
        validate_transition(expected, next)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin code-change transition"))?;
        let current = read_run(&transaction, run_id)?;
        if current.state == next && current.state == expected {
            transaction
                .commit()
                .map_err(database_error("commit idempotent code-change transition"))?;
            return Ok(current);
        }
        let changed = transaction
            .execute(
                "UPDATE code_change_runs
                 SET state = ?1, updated_at = ?2
                 WHERE code_change_run_id = ?3 AND state = ?4",
                params![next, now, run_id, expected],
            )
            .map_err(database_error("transition code-change run"))?;
        if changed != 1 {
            return Err(AppError::Validation {
                field: "code_change.state",
                message: "changed concurrently or does not match the expected state",
            });
        }
        let stored = read_run(&transaction, run_id)?;
        insert_lifecycle_event(&transaction, &stored, stored.editor_attempts, None, now)?;
        transaction
            .commit()
            .map_err(database_error("commit code-change transition"))?;
        read_by_id(&connection, run_id)
    }

    pub fn reserve_editor_attempt(
        &self,
        run_id: &str,
        attempt: i64,
        agent_run_id: i64,
        editor_session_id: &str,
        now: i64,
    ) -> Result<CodeChangeEditorAttempt, AppError> {
        validate_identifier("code_change_run_id", run_id)?;
        validate_identifier("editor_session_id", editor_session_id)?;
        if !(1..=2).contains(&attempt) || agent_run_id <= 0 {
            return Err(AppError::Validation {
                field: "code_change.attempt",
                message: "must be one of the two bounded editor attempts",
            });
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin editor attempt reservation"))?;
        let run = read_run(&transaction, run_id)?;
        if run.state != CodeChangeState::Editing {
            return Err(AppError::Validation {
                field: "code_change.state",
                message: "editor attempts require editing state",
            });
        }
        let project_matches: bool = transaction
            .query_row(
                "SELECT EXISTS(
                     SELECT 1
                     FROM campaigns AS c
                     JOIN agent_runs AS a ON a.project_id = c.project_id
                     WHERE c.campaign_id = ?1 AND a.run_id = ?2
                       AND a.execution_kind = 'code_change_editor'
                       AND a.status IN ('starting', 'running')
                 )",
                params![&run.campaign_id, agent_run_id],
                |row| row.get(0),
            )
            .map_err(database_error("validate editor attempt project"))?;
        if !project_matches {
            return Err(AppError::Validation {
                field: "code_change.agent_run_id",
                message: "must identify an agent run in the code-change project",
            });
        }
        if attempt != run.editor_attempts + 1 {
            return Err(AppError::Validation {
                field: "code_change.attempt",
                message: "must advance the editor attempt exactly once",
            });
        }
        if run
            .editor_session_id
            .as_deref()
            .is_some_and(|expected| expected != editor_session_id)
        {
            return Err(AppError::Validation {
                field: "code_change.editor_session_id",
                message: "all editor attempts must use the same session",
            });
        }
        transaction
            .execute(
                "INSERT INTO code_change_editor_attempts (
                    code_change_run_id, attempt, agent_run_id, editor_session_id, status,
                    result_digest, failure_code, failure_summary, started_at, finished_at
                 ) VALUES (?1, ?2, ?3, ?4, 'reserved', NULL, NULL, NULL, NULL, NULL)",
                params![run_id, attempt, agent_run_id, editor_session_id],
            )
            .map_err(database_error("insert editor attempt reservation"))?;
        let changed = transaction
            .execute(
                "UPDATE code_change_runs
                 SET editor_attempts = ?1, editor_session_id = ?2, updated_at = ?3
                 WHERE code_change_run_id = ?4 AND editor_attempts = ?5",
                params![attempt, editor_session_id, now, run_id, run.editor_attempts],
            )
            .map_err(database_error("advance code-change editor attempt"))?;
        if changed != 1 {
            return Err(AppError::Validation {
                field: "code_change.attempt",
                message: "changed concurrently or does not match the expected attempt",
            });
        }
        let stored = read_editor_attempt(&transaction, run_id, attempt)?;
        let updated_run = read_run(&transaction, run_id)?;
        insert_lifecycle_event(&transaction, &updated_run, attempt, None, now)?;
        transaction
            .commit()
            .map_err(database_error("commit editor attempt reservation"))?;
        Ok(stored)
    }

    /// Replace the supervisor placeholder with the exact session identity
    /// proven at editor terminal persistence.  The attempt/run pair is
    /// updated transactionally and cannot be rebound after it is finished.
    pub fn bind_editor_session(
        &self,
        run_id: &str,
        attempt: i64,
        editor_session_id: &str,
        now: i64,
    ) -> Result<CodeChangeEditorAttempt, AppError> {
        validate_identifier("code_change_run_id", run_id)?;
        validate_identifier("editor_session_id", editor_session_id)?;
        if !(1..=2).contains(&attempt) {
            return Err(AppError::Validation {
                field: "code_change.attempt",
                message: "must be one of the two bounded editor attempts",
            });
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin editor session binding"))?;
        let run = read_run(&transaction, run_id)?;
        let current = read_editor_attempt(&transaction, run_id, attempt)?;
        if current.editor_session_id == editor_session_id
            && run.editor_session_id.as_deref() == Some(editor_session_id)
        {
            transaction
                .commit()
                .map_err(database_error("commit idempotent editor session binding"))?;
            return Ok(current);
        }
        if !matches!(current.status.as_str(), "reserved" | "running") {
            return Err(AppError::Validation {
                field: "code_change.editor_session_id",
                message: "finished editor attempts cannot be rebound",
            });
        }
        if run.editor_session_id.as_deref() != Some(current.editor_session_id.as_str()) {
            return Err(AppError::Validation {
                field: "code_change.editor_session_id",
                message: "editor attempt session disagrees with its run",
            });
        }
        let changed = transaction
            .execute(
                "UPDATE code_change_editor_attempts
                 SET editor_session_id = ?1
                 WHERE code_change_run_id = ?2 AND attempt = ?3
                   AND editor_session_id = ?4
                   AND status IN ('reserved', 'running')",
                params![editor_session_id, run_id, attempt, current.editor_session_id],
            )
            .map_err(database_error("bind editor session on attempt"))?;
        if changed != 1 {
            return Err(AppError::Validation {
                field: "code_change.editor_session_id",
                message: "editor session changed concurrently",
            });
        }
        let changed = transaction
            .execute(
                "UPDATE code_change_runs
                 SET editor_session_id = ?1, updated_at = ?2
                 WHERE code_change_run_id = ?3 AND editor_session_id = ?4",
                params![editor_session_id, now, run_id, current.editor_session_id],
            )
            .map_err(database_error("bind editor session on run"))?;
        if changed != 1 {
            return Err(AppError::Validation {
                field: "code_change.editor_session_id",
                message: "editor session changed concurrently",
            });
        }
        let stored = read_editor_attempt(&transaction, run_id, attempt)?;
        let run = read_run(&transaction, run_id)?;
        insert_lifecycle_event(&transaction, &run, attempt, None, now)?;
        transaction
            .commit()
            .map_err(database_error("commit editor session binding"))?;
        Ok(stored)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn finish_editor_attempt(
        &self,
        run_id: &str,
        attempt: i64,
        status: &str,
        result_digest: Option<&str>,
        failure_code: Option<&str>,
        failure_summary: Option<&str>,
        started_at: Option<i64>,
        finished_at: i64,
        now: i64,
    ) -> Result<CodeChangeEditorAttempt, AppError> {
        self.finish_editor_attempt_with_checks(
            run_id,
            attempt,
            status,
            result_digest,
            failure_code,
            failure_summary,
            started_at,
            finished_at,
            &[],
            now,
        )
    }

    /// Finish an editor attempt and persist its validated check proposals in
    /// one transaction.  Proposed checks remain reserved until the checking
    /// stage independently selects and executes them.
    #[allow(clippy::too_many_arguments)]
    pub fn finish_editor_attempt_with_checks(
        &self,
        run_id: &str,
        attempt: i64,
        status: &str,
        result_digest: Option<&str>,
        failure_code: Option<&str>,
        failure_summary: Option<&str>,
        started_at: Option<i64>,
        finished_at: i64,
        checks: &[NewCodeChangeCheck],
        now: i64,
    ) -> Result<CodeChangeEditorAttempt, AppError> {
        validate_attempt_status(status)?;
        validate_optional_digest(result_digest)?;
        validate_optional_text(
            "failure_code",
            failure_code,
            MAX_CODE_CHANGE_REJECTION_CODE_BYTES,
        )?;
        validate_optional_text(
            "failure_summary",
            failure_summary,
            MAX_CODE_CHANGE_SUMMARY_BYTES,
        )?;
        if checks.len() > MAX_CODE_CHANGE_CHECKS {
            return Err(AppError::Validation {
                field: "code_change_check",
                message: "exceeds the bounded check count",
            });
        }
        for check in checks {
            validate_check(check, attempt)?;
            if check.source != "editor" || check.status != CodeChangeCheckStatus::Reserved {
                return Err(AppError::Validation {
                    field: "code_change_check.source",
                    message: "editor proposals must be reserved checks with editor source",
                });
            }
        }
        let persisted_failure_summary = failure_summary.map(bounded_redacted_text);
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin editor attempt completion"))?;
        let current = read_editor_attempt(&transaction, run_id, attempt)?;
        let existing_checks = list_checks(&transaction, run_id, attempt)?;
        if current.status == status
            && current.result_digest.as_deref() == result_digest
            && current.failure_code.as_deref() == failure_code
            && current.failure_summary.as_deref() == persisted_failure_summary.as_deref()
            && current.finished_at == Some(finished_at)
            && editor_checks_match(&existing_checks, checks)
        {
            transaction.commit().map_err(database_error(
                "commit idempotent editor attempt completion",
            ))?;
            return Ok(current);
        }
        let run = read_run(&transaction, run_id)?;
        if run.state != CodeChangeState::Editing {
            return Err(AppError::Validation {
                field: "code_change.state",
                message: "editor attempt completion requires editing state",
            });
        }
        let changed = transaction
            .execute(
                "UPDATE code_change_editor_attempts
                 SET status = ?1, result_digest = ?2, failure_code = ?3,
                     failure_summary = ?4, started_at = COALESCE(started_at, ?5),
                     finished_at = ?6
                 WHERE code_change_run_id = ?7 AND attempt = ?8
                   AND status IN ('reserved', 'running')",
                params![
                    status,
                    result_digest,
                    failure_code,
                    persisted_failure_summary.as_deref(),
                    started_at,
                    finished_at,
                    run_id,
                    attempt,
                ],
            )
            .map_err(database_error("finish editor attempt"))?;
        if changed != 1 {
            return Err(AppError::Validation {
                field: "code_change_editor_attempt.status",
                message: "attempt is already finished or changed concurrently",
            });
        }
        if !existing_checks.is_empty() {
            return Err(AppError::Validation {
                field: "code_change_check",
                message: "editor attempt already has different proposed checks",
            });
        }
        for check in checks {
            let argv_json = serde_json::to_string(&check.argv).map_err(|source| {
                AppError::Serialization {
                    operation: "serialize editor-proposed check argv",
                    source,
                }
            })?;
            if argv_json.len() > MAX_CODE_CHANGE_ARGV_JSON_BYTES {
                return Err(AppError::Validation {
                    field: "code_change_check.argv",
                    message: "serialized argv exceeds the bounded size",
                });
            }
            transaction
                .execute(
                    "INSERT INTO code_change_checks (
                        code_change_run_id, attempt, ordinal, source, argv_json,
                        working_directory, status, output_digest, summary,
                        started_at, finished_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, NULL, NULL)",
                    params![
                        run_id,
                        check.attempt,
                        check.ordinal,
                        check.source,
                        argv_json,
                        check.working_directory,
                        check.status,
                    ],
                )
                .map_err(database_error("insert editor-proposed check"))?;
        }
        transaction
            .execute(
                "UPDATE code_change_runs SET updated_at = ?1
                 WHERE code_change_run_id = ?2",
                params![now, run_id],
            )
            .map_err(database_error("update code-change editor attempt time"))?;
        let stored = read_editor_attempt(&transaction, run_id, attempt)?;
        let run = read_run(&transaction, run_id)?;
        insert_lifecycle_event(&transaction, &run, attempt, None, now)?;
        transaction
            .commit()
            .map_err(database_error("commit editor attempt completion"))?;
        Ok(stored)
    }

    pub fn replace_attempt_checks(
        &self,
        run_id: &str,
        attempt: i64,
        checks: &[NewCodeChangeCheck],
        now: i64,
    ) -> Result<Vec<CodeChangeCheck>, AppError> {
        validate_identifier("code_change_run_id", run_id)?;
        if !(1..=2).contains(&attempt) {
            return Err(AppError::Validation {
                field: "code_change.attempt",
                message: "must be one of the two bounded editor attempts",
            });
        }
        if checks.len() > MAX_CODE_CHANGE_CHECKS {
            return Err(AppError::Validation {
                field: "code_change_check",
                message: "exceeds the bounded check count",
            });
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin code-change check replacement"))?;
        let run = read_run(&transaction, run_id)?;
        if run.state != CodeChangeState::Checking {
            return Err(AppError::Validation {
                field: "code_change.state",
                message: "checks require checking state",
            });
        }
        if transaction
            .query_row(
                "SELECT 1 FROM code_change_editor_attempts
                 WHERE code_change_run_id = ?1 AND attempt = ?2",
                params![run_id, attempt],
                |_| Ok(()),
            )
            .optional()
            .map_err(database_error("validate code-change editor attempt"))?
            .is_none()
        {
            return Err(AppError::Validation {
                field: "code_change.attempt",
                message: "must identify a reserved editor attempt",
            });
        }
        transaction
            .execute(
                "DELETE FROM code_change_checks WHERE code_change_run_id = ?1 AND attempt = ?2",
                params![run_id, attempt],
            )
            .map_err(database_error("replace code-change checks"))?;
        for check in checks {
            validate_check(check, attempt)?;
            let argv_json =
                serde_json::to_string(&check.argv).map_err(|source| AppError::Serialization {
                    operation: "serialize code-change check argv",
                    source,
                })?;
            if argv_json.len() > MAX_CODE_CHANGE_ARGV_JSON_BYTES {
                return Err(AppError::Validation {
                    field: "code_change_check.argv",
                    message: "serialized argv exceeds the bounded size",
                });
            }
            transaction
                .execute(
                    "INSERT INTO code_change_checks (
                        code_change_run_id, attempt, ordinal, source, argv_json,
                        working_directory, status, output_digest, summary,
                        started_at, finished_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
                        run_id,
                        check.attempt,
                        check.ordinal,
                        check.source,
                        argv_json,
                        check.working_directory,
                        check.status,
                        check.output_digest,
                        check.summary.as_deref().map(bounded_redacted_text),
                        check.started_at,
                        check.finished_at,
                    ],
                )
                .map_err(database_error("insert code-change check"))?;
        }
        transaction
            .execute(
                "UPDATE code_change_runs SET updated_at = ?1
                 WHERE code_change_run_id = ?2",
                params![now, run_id],
            )
            .map_err(database_error("update code-change check time"))?;
        let run = read_run(&transaction, run_id)?;
        insert_lifecycle_event(&transaction, &run, attempt, None, now)?;
        let stored = list_checks(&transaction, run_id, attempt)?;
        transaction
            .commit()
            .map_err(database_error("commit code-change check replacement"))?;
        Ok(stored)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn finish_check(
        &self,
        run_id: &str,
        attempt: i64,
        ordinal: i64,
        status: CodeChangeCheckStatus,
        output_digest: Option<&str>,
        summary: Option<&str>,
        started_at: Option<i64>,
        finished_at: i64,
        now: i64,
    ) -> Result<CodeChangeCheck, AppError> {
        if !matches!(
            status,
            CodeChangeCheckStatus::Passed
                | CodeChangeCheckStatus::Failed
                | CodeChangeCheckStatus::TimedOut
        ) {
            return Err(AppError::Validation {
                field: "code_change_check.status",
                message: "must be passed, failed, or timed_out when finished",
            });
        }
        validate_optional_digest(output_digest)?;
        validate_optional_text("summary", summary, MAX_CODE_CHANGE_SUMMARY_BYTES)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin code-change check completion"))?;
        let persisted_summary = summary.map(bounded_redacted_text);
        let current = transaction
            .query_row(
                &format!(
                    "{CODE_CHANGE_CHECK_SELECT}
                     WHERE code_change_run_id = ?1 AND attempt = ?2 AND ordinal = ?3"
                ),
                params![run_id, attempt, ordinal],
                code_change_check_from_row,
            )
            .optional()
            .map_err(database_error("read code-change check before completion"))?;
        if let Some(current) = current {
            let started_at_matches =
                started_at.is_none_or(|started_at| current.started_at == Some(started_at));
            if current.status == status
                && current.output_digest.as_deref() == output_digest
                && current.summary.as_deref() == persisted_summary.as_deref()
                && started_at_matches
                && current.finished_at == Some(finished_at)
            {
                transaction.commit().map_err(database_error(
                    "commit idempotent code-change check completion",
                ))?;
                return Ok(current);
            }
            if current.status != CodeChangeCheckStatus::Reserved {
                return Err(AppError::Validation {
                    field: "code_change_check.status",
                    message: "conflicts with the existing finished check",
                });
            }
        }
        let run = read_run(&transaction, run_id)?;
        if run.state != CodeChangeState::Checking {
            return Err(AppError::Validation {
                field: "code_change.state",
                message: "check completion requires checking state",
            });
        }
        let changed = transaction
            .execute(
                "UPDATE code_change_checks
                 SET status = ?1, output_digest = ?2, summary = ?3,
                     started_at = COALESCE(started_at, ?4), finished_at = ?5
                 WHERE code_change_run_id = ?6 AND attempt = ?7 AND ordinal = ?8
                   AND status = 'reserved'",
                params![
                    status,
                    output_digest,
                    persisted_summary.as_deref(),
                    started_at,
                    finished_at,
                    run_id,
                    attempt,
                    ordinal,
                ],
            )
            .map_err(database_error("finish code-change check"))?;
        if changed != 1 {
            return Err(AppError::Validation {
                field: "code_change_check.status",
                message: "check is missing, already finished, or changed concurrently",
            });
        }
        transaction
            .execute(
                "UPDATE code_change_runs SET updated_at = ?1
                 WHERE code_change_run_id = ?2",
                params![now, run_id],
            )
            .map_err(database_error("update code-change check completion time"))?;
        insert_lifecycle_event(&transaction, &run, attempt, None, now)?;
        let stored = transaction
            .query_row(
                &format!(
                    "{CODE_CHANGE_CHECK_SELECT}
                     WHERE code_change_run_id = ?1 AND attempt = ?2 AND ordinal = ?3"
                ),
                params![run_id, attempt, ordinal],
                code_change_check_from_row,
            )
            .map_err(database_error("read finished code-change check"))?;
        transaction
            .commit()
            .map_err(database_error("commit code-change check completion"))?;
        Ok(stored)
    }

    pub fn record_candidate(
        &self,
        run_id: &str,
        candidate_sha: &str,
        diff_digest: &str,
        changed_file_count: i64,
        diff_bytes: i64,
        now: i64,
    ) -> Result<CodeChangeRun, AppError> {
        validate_sha("candidate_sha", candidate_sha)?;
        validate_digest("diff_digest", diff_digest)?;
        if changed_file_count < 0 || diff_bytes < 0 {
            return Err(AppError::Validation {
                field: "code_change.diff",
                message: "counts must be non-negative",
            });
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin code-change candidate recording"))?;
        let run = read_run(&transaction, run_id)?;
        if let Some(existing_candidate_sha) = run.candidate_sha.as_deref() {
            if existing_candidate_sha == candidate_sha
                && run.diff_digest.as_deref() == Some(diff_digest)
                && run.changed_file_count == Some(changed_file_count)
                && run.diff_bytes == Some(diff_bytes)
            {
                transaction
                    .commit()
                    .map_err(database_error("commit idempotent code-change candidate"))?;
                return Ok(run);
            }
            return Err(AppError::Validation {
                field: "code_change.candidate_sha",
                message: "conflicts with the existing candidate",
            });
        }
        if run.state != CodeChangeState::Committing {
            return Err(AppError::Validation {
                field: "code_change.state",
                message: "candidate requires committing state",
            });
        }
        let changed = transaction
            .execute(
                "UPDATE code_change_runs
                 SET candidate_sha = ?1, diff_digest = ?2,
                     changed_file_count = ?3, diff_bytes = ?4, updated_at = ?5
                 WHERE code_change_run_id = ?6 AND state = 'committing'
                   AND candidate_sha IS NULL",
                params![
                    candidate_sha,
                    diff_digest,
                    changed_file_count,
                    diff_bytes,
                    now,
                    run_id
                ],
            )
            .map_err(database_error("record code-change candidate"))?;
        if changed != 1 {
            return Err(AppError::Validation {
                field: "code_change.candidate_sha",
                message: "candidate is already recorded or changed concurrently",
            });
        }
        let stored = read_run(&transaction, run_id)?;
        insert_lifecycle_event(&transaction, &stored, stored.editor_attempts, None, now)?;
        transaction
            .commit()
            .map_err(database_error("commit code-change candidate recording"))?;
        read_by_id(&connection, run_id)
    }

    pub fn bind_experiment(
        &self,
        run_id: &str,
        experiment_id: &str,
        code_revision_sha: &str,
        now: i64,
    ) -> Result<CodeChangeRun, AppError> {
        validate_identifier("experiment_id", experiment_id)?;
        validate_sha("code_revision_sha", code_revision_sha)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin code-change experiment binding"))?;
        let run = read_run(&transaction, run_id)?;
        if !matches!(
            run.state,
            CodeChangeState::CandidateReady | CodeChangeState::ExperimentSubmitted
        ) {
            return Err(AppError::Validation {
                field: "code_change.state",
                message: "experiment binding requires candidate-ready state",
            });
        }
        if run.candidate_sha.as_deref() != Some(code_revision_sha) {
            return Err(AppError::Validation {
                field: "code_revision_sha",
                message: "must match the persisted candidate SHA",
            });
        }
        let linked: Option<(String, String)> = transaction
            .query_row(
                "SELECT campaign_id, proposal_id FROM experiments WHERE experiment_id = ?1",
                [experiment_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(database_error("validate code-change experiment"))?;
        if linked.as_ref() != Some(&(run.campaign_id.clone(), run.proposal_id.clone())) {
            return Err(AppError::Validation {
                field: "experiment_id",
                message: "must belong to the code-change campaign and proposal",
            });
        }
        if run.experiment_id.as_deref() == Some(experiment_id) {
            transaction.commit().map_err(database_error(
                "commit idempotent code-change experiment binding",
            ))?;
            return Ok(run);
        }
        if run.experiment_id.is_some() {
            return Err(AppError::Validation {
                field: "code_change.experiment_id",
                message: "already binds a different experiment",
            });
        }
        let experiment_changed = transaction
            .execute(
                "UPDATE experiments
                 SET code_change_run_id = ?1, code_revision_sha = ?2
                 WHERE experiment_id = ?3 AND code_change_run_id IS NULL
                   AND code_revision_sha IS NULL",
                params![run.code_change_run_id, code_revision_sha, experiment_id],
            )
            .map_err(database_error("bind experiment to code-change run"))?;
        if experiment_changed != 1 {
            return Err(AppError::Validation {
                field: "experiment_id",
                message: "is already bound to a different code-change run",
            });
        }
        let changed = transaction
            .execute(
                "UPDATE code_change_runs SET experiment_id = ?1, updated_at = ?2
                 WHERE code_change_run_id = ?3 AND experiment_id IS NULL",
                params![experiment_id, now, run_id],
            )
            .map_err(database_error("record code-change experiment"))?;
        if changed != 1 {
            return Err(AppError::Validation {
                field: "code_change.experiment_id",
                message: "changed concurrently or is already bound",
            });
        }
        let stored = read_run(&transaction, run_id)?;
        insert_lifecycle_event(&transaction, &stored, stored.editor_attempts, None, now)?;
        transaction
            .commit()
            .map_err(database_error("commit code-change experiment binding"))?;
        read_by_id(&connection, run_id)
    }

    pub fn record_evaluation(
        &self,
        run_id: &str,
        promotion_outcome: &str,
        expected_best_experiment_id: Option<&str>,
        expected_old_sha: Option<&str>,
        target_sha: Option<&str>,
        now: i64,
    ) -> Result<CodeChangeRun, AppError> {
        validate_optional_text(
            "promotion_outcome",
            Some(promotion_outcome),
            MAX_CODE_CHANGE_REJECTION_CODE_BYTES,
        )?;
        validate_optional_text(
            "promotion_expected_best_experiment_id",
            expected_best_experiment_id,
            MAX_CODE_CHANGE_ID_BYTES,
        )?;
        validate_optional_sha("promotion_expected_old_sha", expected_old_sha)?;
        validate_optional_sha("promotion_target_sha", target_sha)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin code-change evaluation recording"))?;
        let run = read_run(&transaction, run_id)?;
        if run.state == CodeChangeState::Evaluated
            && run.promotion_outcome.as_deref() == Some(promotion_outcome)
            && run.promotion_expected_best_experiment_id.as_deref() == expected_best_experiment_id
            && run.promotion_expected_old_sha.as_deref() == expected_old_sha
            && run.promotion_target_sha.as_deref() == target_sha
        {
            transaction
                .commit()
                .map_err(database_error("commit idempotent code-change evaluation"))?;
            return Ok(run);
        }
        if run.state != CodeChangeState::ExperimentSubmitted {
            return Err(AppError::Validation {
                field: "code_change.state",
                message: "evaluation requires experiment_submitted state",
            });
        }
        let changed = transaction
            .execute(
                "UPDATE code_change_runs
                 SET state = 'evaluated', promotion_outcome = ?1,
                     promotion_expected_best_experiment_id = ?2,
                     promotion_expected_old_sha = ?3, promotion_target_sha = ?4,
                     updated_at = ?5
                 WHERE code_change_run_id = ?6 AND state = 'experiment_submitted'",
                params![
                    promotion_outcome,
                    expected_best_experiment_id,
                    expected_old_sha,
                    target_sha,
                    now,
                    run_id,
                ],
            )
            .map_err(database_error("record code-change evaluation"))?;
        if changed != 1 {
            return Err(AppError::Validation {
                field: "code_change.state",
                message: "changed concurrently or does not match the expected state",
            });
        }
        let stored = read_run(&transaction, run_id)?;
        insert_lifecycle_event(&transaction, &stored, stored.editor_attempts, None, now)?;
        transaction
            .commit()
            .map_err(database_error("commit code-change evaluation recording"))?;
        read_by_id(&connection, run_id)
    }

    pub fn reject(
        &self,
        run_id: &str,
        reason_code: &str,
        reason_summary: &str,
        now: i64,
    ) -> Result<CodeChangeRun, AppError> {
        validate_optional_text(
            "rejection_code",
            Some(reason_code),
            MAX_CODE_CHANGE_REJECTION_CODE_BYTES,
        )?;
        validate_optional_text(
            "rejection_summary",
            Some(reason_summary),
            MAX_CODE_CHANGE_SUMMARY_BYTES,
        )?;
        self.finish_terminal(
            run_id,
            CodeChangeState::Rejected,
            reason_code,
            reason_summary,
            now,
        )
    }

    pub fn require_recovery(
        &self,
        run_id: &str,
        reason_code: &str,
        reason_summary: &str,
        now: i64,
    ) -> Result<CodeChangeRun, AppError> {
        validate_optional_text(
            "recovery_code",
            Some(reason_code),
            MAX_CODE_CHANGE_REJECTION_CODE_BYTES,
        )?;
        validate_optional_text(
            "recovery_summary",
            Some(reason_summary),
            MAX_CODE_CHANGE_SUMMARY_BYTES,
        )?;
        self.finish_terminal(
            run_id,
            CodeChangeState::RecoveryRequired,
            reason_code,
            reason_summary,
            now,
        )
    }

    pub fn finish_cleanup(&self, run_id: &str, now: i64) -> Result<CodeChangeRun, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin code-change cleanup completion"))?;
        let run = read_run(&transaction, run_id)?;
        let next = match run.state {
            CodeChangeState::CleanupPending => CodeChangeState::Completed,
            // A Task 3 candidate can be cleaned before Task 4 creates an
            // experiment.  Preserve the candidate-ready state while
            // recording the durable cleanup completion marker; the schema
            // intentionally permits that marker independently of state.
            CodeChangeState::CandidateReady => CodeChangeState::CandidateReady,
            CodeChangeState::Completed if run.cleanup_completed_at.is_some() => {
                transaction
                    .commit()
                    .map_err(database_error("commit idempotent code-change cleanup"))?;
                return Ok(run);
            }
            CodeChangeState::Rejected if run.cleanup_completed_at.is_some() => {
                transaction
                    .commit()
                    .map_err(database_error("commit idempotent code-change cleanup"))?;
                return Ok(run);
            }
            CodeChangeState::Rejected => CodeChangeState::Rejected,
            _ => {
                return Err(AppError::Validation {
                    field: "code_change.state",
                    message: "cleanup requires cleanup_pending or rejected state",
                });
            }
        };
        let changed = transaction
            .execute(
                "UPDATE code_change_runs
                 SET state = ?1, cleanup_completed_at = ?2, updated_at = ?2
                 WHERE code_change_run_id = ?3 AND state = ?4
                   AND cleanup_completed_at IS NULL",
                params![next, now, run_id, run.state],
            )
            .map_err(database_error("finish code-change cleanup"))?;
        if changed != 1 {
            return Err(AppError::Validation {
                field: "code_change.cleanup_completed_at",
                message: "changed concurrently or cleanup is already complete",
            });
        }
        let stored = read_run(&transaction, run_id)?;
        insert_lifecycle_event(
            &transaction,
            &stored,
            stored.editor_attempts,
            stored.rejection_code.as_deref(),
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit code-change cleanup completion"))?;
        read_by_id(&connection, run_id)
    }

    /// Persist the exact descriptor proof produced by the Task 3 manager.
    /// The proof is write-once: a second call is idempotent only when every
    /// field is byte-for-byte identical, so a later caller cannot replace the
    /// ownership boundary with a same-path capability.
    #[allow(clippy::too_many_arguments)]
    pub fn record_worktree_ownership(
        &self,
        run_id: &str,
        state_root_identity: &str,
        worktrees_identity: &str,
        campaign_identity: &str,
        candidate_root_identity: &str,
        candidate_admin_identity: &str,
        candidate_common_identity: &str,
        candidate_admin_path: &str,
        candidate_common_path: &str,
        now: i64,
    ) -> Result<CodeChangeRun, AppError> {
        validate_identifier("code_change_run_id", run_id)?;
        for (field, value) in [
            ("state_root_identity", state_root_identity),
            ("worktrees_identity", worktrees_identity),
            ("campaign_identity", campaign_identity),
            ("candidate_root_identity", candidate_root_identity),
            ("candidate_admin_identity", candidate_admin_identity),
            ("candidate_common_identity", candidate_common_identity),
            ("candidate_admin_path", candidate_admin_path),
            ("candidate_common_path", candidate_common_path),
        ] {
            if value.is_empty() || value.len() > MAX_CODE_CHANGE_ID_BYTES || value.contains('\0') {
                return Err(AppError::Validation {
                    field,
                    message: "must be a bounded non-empty ownership proof",
                });
            }
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin code-change ownership proof"))?;
        let run = read_run(&transaction, run_id)?;
        let values = [
            state_root_identity,
            worktrees_identity,
            campaign_identity,
            candidate_root_identity,
            candidate_admin_identity,
            candidate_common_identity,
            candidate_admin_path,
            candidate_common_path,
        ];
        let existing = [
            run.state_root_identity.as_deref(),
            run.worktrees_identity.as_deref(),
            run.campaign_identity.as_deref(),
            run.candidate_root_identity.as_deref(),
            run.candidate_admin_identity.as_deref(),
            run.candidate_common_identity.as_deref(),
            run.candidate_admin_path.as_deref(),
            run.candidate_common_path.as_deref(),
        ];
        if existing.iter().any(Option::is_some) {
            if existing
                .iter()
                .zip(values.iter())
                .all(|(stored, expected)| *stored == Some(*expected))
            {
                transaction
                    .commit()
                    .map_err(database_error("commit idempotent code-change ownership proof"))?;
                return Ok(run);
            }
            return Err(AppError::Validation {
                field: "code_change.ownership",
                message: "durable ownership proof cannot be replaced",
            });
        }
        let changed = transaction
            .execute(
                "UPDATE code_change_runs
                 SET state_root_identity = ?1, worktrees_identity = ?2,
                     campaign_identity = ?3, candidate_root_identity = ?4,
                     candidate_admin_identity = ?5, candidate_common_identity = ?6,
                     candidate_admin_path = ?7, candidate_common_path = ?8,
                     updated_at = ?9
                 WHERE code_change_run_id = ?10
                   AND state_root_identity IS NULL
                   AND worktrees_identity IS NULL
                   AND campaign_identity IS NULL
                   AND candidate_root_identity IS NULL
                   AND candidate_admin_identity IS NULL
                   AND candidate_common_identity IS NULL
                   AND candidate_admin_path IS NULL
                   AND candidate_common_path IS NULL",
                params![
                    state_root_identity,
                    worktrees_identity,
                    campaign_identity,
                    candidate_root_identity,
                    candidate_admin_identity,
                    candidate_common_identity,
                    candidate_admin_path,
                    candidate_common_path,
                    now,
                    run_id,
                ],
            )
            .map_err(database_error("persist code-change ownership proof"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "persist code-change ownership proof changed concurrently",
            });
        }
        let stored = read_run(&transaction, run_id)?;
        transaction
            .commit()
            .map_err(database_error("commit code-change ownership proof"))?;
        Ok(stored)
    }

    fn finish_terminal(
        &self,
        run_id: &str,
        state: CodeChangeState,
        reason_code: &str,
        reason_summary: &str,
        now: i64,
    ) -> Result<CodeChangeRun, AppError> {
        let persisted_reason_summary = bounded_redacted_text(reason_summary);
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin code-change terminal transition"))?;
        let run = read_run(&transaction, run_id)?;
        if run.state == state
            && run.rejection_code.as_deref() == Some(reason_code)
            && run.rejection_summary.as_deref() == Some(persisted_reason_summary.as_str())
        {
            transaction.commit().map_err(database_error(
                "commit idempotent code-change terminal transition",
            ))?;
            return Ok(run);
        }
        if matches!(
            run.state,
            CodeChangeState::Completed
                | CodeChangeState::Rejected
                | CodeChangeState::RecoveryRequired
        ) || (state == CodeChangeState::Rejected
            && matches!(
                run.state,
                CodeChangeState::CandidateReady
                    | CodeChangeState::ExperimentSubmitted
                    | CodeChangeState::Evaluated
                    | CodeChangeState::CleanupPending
                    | CodeChangeState::Completed
            ))
        {
            return Err(AppError::Validation {
                field: "code_change.state",
                message: "terminal code-change state cannot be changed",
            });
        }
        let changed = transaction
            .execute(
                "UPDATE code_change_runs
                 SET state = ?1, rejection_code = ?2, rejection_summary = ?3,
                     updated_at = ?4
                 WHERE code_change_run_id = ?5 AND state = ?6",
                params![
                    state,
                    reason_code,
                    persisted_reason_summary,
                    now,
                    run_id,
                    run.state,
                ],
            )
            .map_err(database_error("record code-change terminal transition"))?;
        if changed != 1 {
            return Err(AppError::Validation {
                field: "code_change.state",
                message: "changed concurrently or does not match the expected state",
            });
        }
        let stored = read_run(&transaction, run_id)?;
        insert_lifecycle_event(
            &transaction,
            &stored,
            stored.editor_attempts,
            Some(reason_code),
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit code-change terminal transition"))?;
        read_by_id(&connection, run_id)
    }
}

const CODE_CHANGE_SELECT: &str = "SELECT code_change_run_id, proposal_id, campaign_id, state,
        base_sha, candidate_sha, candidate_ref, best_ref, worktree_id,
        worktree_relative_path, editor_session_id, editor_attempts, diff_digest,
        changed_file_count, diff_bytes, experiment_id, rejection_code,
        rejection_summary, promotion_outcome, promotion_expected_best_experiment_id,
        promotion_expected_old_sha, promotion_target_sha, cleanup_completed_at,
        state_root_identity, worktrees_identity, campaign_identity,
        candidate_root_identity, candidate_admin_identity, candidate_common_identity,
        candidate_admin_path, candidate_common_path, created_at, updated_at
    FROM code_change_runs";

const CODE_CHANGE_CHECK_SELECT: &str = "SELECT code_change_run_id, attempt, ordinal, source,
        argv_json, working_directory, status, output_digest, summary, started_at, finished_at
    FROM code_change_checks";

fn read_by_id(connection: &Connection, run_id: &str) -> Result<CodeChangeRun, AppError> {
    find_by_id_connection(connection, run_id)?.ok_or(AppError::Runtime {
        operation: "read code-change run after mutation",
    })
}

fn find_by_id_connection(
    connection: &Connection,
    run_id: &str,
) -> Result<Option<CodeChangeRun>, AppError> {
    validate_identifier("code_change_run_id", run_id)?;
    connection
        .query_row(
            &format!("{CODE_CHANGE_SELECT} WHERE code_change_run_id = ?1"),
            [run_id],
            code_change_run_from_row,
        )
        .optional()
        .map_err(database_error("find code-change run"))
}

fn read_run(transaction: &Transaction<'_>, run_id: &str) -> Result<CodeChangeRun, AppError> {
    transaction
        .query_row(
            &format!("{CODE_CHANGE_SELECT} WHERE code_change_run_id = ?1"),
            [run_id],
            code_change_run_from_row,
        )
        .map_err(database_error("read code-change run"))
}

fn read_editor_attempt(
    transaction: &Transaction<'_>,
    run_id: &str,
    attempt: i64,
) -> Result<CodeChangeEditorAttempt, AppError> {
    transaction
        .query_row(
            "SELECT code_change_run_id, attempt, agent_run_id, editor_session_id, status,
                    result_digest, failure_code, failure_summary, started_at, finished_at
             FROM code_change_editor_attempts
             WHERE code_change_run_id = ?1 AND attempt = ?2",
            params![run_id, attempt],
            code_change_editor_attempt_from_row,
        )
        .map_err(database_error("read code-change editor attempt"))
}

fn list_checks(
    transaction: &Transaction<'_>,
    run_id: &str,
    attempt: i64,
) -> Result<Vec<CodeChangeCheck>, AppError> {
    let mut statement = transaction
        .prepare(&format!(
            "{CODE_CHANGE_CHECK_SELECT}
             WHERE code_change_run_id = ?1 AND attempt = ?2
             ORDER BY ordinal"
        ))
        .map_err(database_error("prepare code-change check list"))?;
    let rows = statement
        .query_map(params![run_id, attempt], code_change_check_from_row)
        .map_err(database_error("query code-change check list"))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(database_error("read code-change check list"))
}

fn editor_checks_match(
    existing: &[CodeChangeCheck],
    expected: &[NewCodeChangeCheck],
) -> bool {
    existing.len() == expected.len()
        && existing.iter().zip(expected).all(|(existing, expected)| {
            existing.attempt == expected.attempt
                && existing.ordinal == expected.ordinal
                && existing.source == expected.source
                && existing.argv == expected.argv
                && existing.working_directory == expected.working_directory
                && existing.status == expected.status
                && existing.output_digest == expected.output_digest
                && existing.summary == expected.summary
                && existing.started_at == expected.started_at
                && existing.finished_at == expected.finished_at
        })
}

fn insert_lifecycle_event(
    transaction: &Transaction<'_>,
    run: &CodeChangeRun,
    attempt: i64,
    reason_code: Option<&str>,
    event_time: i64,
) -> Result<(), AppError> {
    let project_id: String = transaction
        .query_row(
            "SELECT project_id FROM campaigns WHERE campaign_id = ?1",
            [&run.campaign_id],
            |row| row.get(0),
        )
        .map_err(database_error("read code-change event project"))?;
    let persisted_event_time: Option<i64> = transaction
        .query_row(
            "SELECT created_at FROM events
             WHERE project_id = ?1 AND dedup_key = ?2",
            params![
                project_id.as_str(),
                format!(
                    "code-change:v1:{}:{}:{}",
                    run.code_change_run_id, run.state, attempt
                )
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(database_error("read existing code-change audit event"))?;
    let event = new_code_change_transition_event(
        project_id,
        run,
        attempt,
        reason_code,
        persisted_event_time.unwrap_or(event_time),
    );
    insert_event_completed_in_transaction(transaction, &event).map(|_| ())
}

fn code_change_run_from_row(row: &Row<'_>) -> rusqlite::Result<CodeChangeRun> {
    Ok(CodeChangeRun {
        code_change_run_id: row.get(0)?,
        proposal_id: row.get(1)?,
        campaign_id: row.get(2)?,
        state: row.get(3)?,
        base_sha: row.get(4)?,
        candidate_sha: row.get(5)?,
        candidate_ref: row.get(6)?,
        best_ref: row.get(7)?,
        worktree_id: row.get(8)?,
        worktree_relative_path: row.get(9)?,
        editor_session_id: row.get(10)?,
        editor_attempts: row.get(11)?,
        diff_digest: row.get(12)?,
        changed_file_count: row.get(13)?,
        diff_bytes: row.get(14)?,
        experiment_id: row.get(15)?,
        rejection_code: row.get(16)?,
        rejection_summary: row.get(17)?,
        promotion_outcome: row.get(18)?,
        promotion_expected_best_experiment_id: row.get(19)?,
        promotion_expected_old_sha: row.get(20)?,
        promotion_target_sha: row.get(21)?,
        cleanup_completed_at: row.get(22)?,
        state_root_identity: row.get(23)?,
        worktrees_identity: row.get(24)?,
        campaign_identity: row.get(25)?,
        candidate_root_identity: row.get(26)?,
        candidate_admin_identity: row.get(27)?,
        candidate_common_identity: row.get(28)?,
        candidate_admin_path: row.get(29)?,
        candidate_common_path: row.get(30)?,
        created_at: row.get(31)?,
        updated_at: row.get(32)?,
    })
}

fn code_change_editor_attempt_from_row(row: &Row<'_>) -> rusqlite::Result<CodeChangeEditorAttempt> {
    Ok(CodeChangeEditorAttempt {
        code_change_run_id: row.get(0)?,
        attempt: row.get(1)?,
        agent_run_id: row.get(2)?,
        editor_session_id: row.get(3)?,
        status: row.get(4)?,
        result_digest: row.get(5)?,
        failure_code: row.get(6)?,
        failure_summary: row.get(7)?,
        started_at: row.get(8)?,
        finished_at: row.get(9)?,
    })
}

fn code_change_check_from_row(row: &Row<'_>) -> rusqlite::Result<CodeChangeCheck> {
    let argv_json: String = row.get(4)?;
    let argv = serde_json::from_str(&argv_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(source))
    })?;
    Ok(CodeChangeCheck {
        code_change_run_id: row.get(0)?,
        attempt: row.get(1)?,
        ordinal: row.get(2)?,
        source: row.get(3)?,
        argv,
        working_directory: row.get(5)?,
        status: row.get(6)?,
        output_digest: row.get(7)?,
        summary: row.get(8)?,
        started_at: row.get(9)?,
        finished_at: row.get(10)?,
    })
}

fn validate_new_run(run: &NewCodeChangeRun) -> Result<(), AppError> {
    validate_identifier("code_change_run_id", &run.code_change_run_id)?;
    validate_identifier("proposal_id", &run.proposal_id)?;
    validate_identifier("campaign_id", &run.campaign_id)?;
    validate_identifier("worktree_id", &run.worktree_id)?;
    validate_identifier("worktree_relative_path", &run.worktree_relative_path)?;
    validate_identifier("candidate_ref", &run.candidate_ref)?;
    validate_identifier("best_ref", &run.best_ref)?;
    if let Some(editor_session_id) = run.editor_session_id.as_deref() {
        validate_identifier("editor_session_id", editor_session_id)?;
    }
    validate_owned_path("worktree_relative_path", &run.worktree_relative_path)?;
    validate_owned_ref("candidate_ref", &run.candidate_ref)?;
    validate_owned_ref("best_ref", &run.best_ref)?;
    if run.candidate_ref != code_change::candidate_ref(&run.campaign_id, &run.proposal_id)?
        || run.best_ref != code_change::best_ref(&run.campaign_id)?
    {
        return Err(AppError::Validation {
            field: "code_change.ref",
            message: "must be the campaign-owned candidate and best ref",
        });
    }
    validate_sha("base_sha", &run.base_sha)
}

fn validate_owned_ref(field: &'static str, value: &str) -> Result<(), AppError> {
    if value.starts_with('/')
        || value.contains("..")
        || value.contains('\\')
        || !value
            .split('/')
            .all(|segment| !segment.is_empty() && segment != ".")
    {
        return Err(AppError::Validation {
            field,
            message: "must be an owned internal ref without traversal",
        });
    }
    Ok(())
}

fn validate_owned_path(field: &'static str, value: &str) -> Result<(), AppError> {
    if value.starts_with('/')
        || value.contains('\\')
        || value
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(AppError::Validation {
            field,
            message: "must be an owned relative path without traversal",
        });
    }
    Ok(())
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), AppError> {
    if value.is_empty()
        || value.len() > MAX_CODE_CHANGE_ID_BYTES
        || value.chars().any(|character| character.is_control())
    {
        return Err(AppError::Validation {
            field,
            message: "must be non-empty bounded text without control characters",
        });
    }
    Ok(())
}

pub(crate) fn validate_sha(field: &'static str, value: &str) -> Result<(), AppError> {
    if (value.len() != 40 && value.len() != 64)
        || value
            .chars()
            .any(|character| !character.is_ascii_hexdigit())
        || value
            .chars()
            .any(|character| character.is_ascii_uppercase())
    {
        return Err(AppError::Validation {
            field,
            message: "must be a canonical lowercase full object ID",
        });
    }
    Ok(())
}

fn validate_optional_sha(field: &'static str, value: Option<&str>) -> Result<(), AppError> {
    if let Some(value) = value {
        validate_sha(field, value)?;
    }
    Ok(())
}

fn validate_digest(field: &'static str, value: &str) -> Result<(), AppError> {
    if value.is_empty() || value.len() > MAX_CODE_CHANGE_DIGEST_BYTES {
        return Err(AppError::Validation {
            field,
            message: "must be non-empty bounded text",
        });
    }
    Ok(())
}

fn validate_optional_digest(value: Option<&str>) -> Result<(), AppError> {
    if let Some(value) = value {
        validate_digest("digest", value)?;
    }
    Ok(())
}

fn validate_optional_text(
    field: &'static str,
    value: Option<&str>,
    max_bytes: usize,
) -> Result<(), AppError> {
    if let Some(value) = value {
        if value.len() > max_bytes || value.chars().any(|character| character.is_control()) {
            return Err(AppError::Validation {
                field,
                message: "must be bounded UTF-8 text without control characters",
            });
        }
    }
    Ok(())
}

fn validate_attempt_status(value: &str) -> Result<(), AppError> {
    if matches!(value, "ready" | "failed") {
        Ok(())
    } else {
        Err(AppError::Validation {
            field: "code_change_editor_attempt.status",
            message: "must be a recognized editor attempt status",
        })
    }
}

fn validate_check(check: &NewCodeChangeCheck, expected_attempt: i64) -> Result<(), AppError> {
    if check.attempt != expected_attempt || check.ordinal < 0 {
        return Err(AppError::Validation {
            field: "code_change_check.ordinal",
            message: "must belong to the requested bounded attempt",
        });
    }
    if !matches!(
        check.source.as_str(),
        "supervisor" | "discovered" | "editor"
    ) {
        return Err(AppError::Validation {
            field: "code_change_check.source",
            message: "must be supervisor, discovered, or editor",
        });
    }
    if check.argv.is_empty() || check.argv.iter().any(|argument| argument.is_empty()) {
        return Err(AppError::Validation {
            field: "code_change_check.argv",
            message: "must contain a non-empty argv",
        });
    }
    if check
        .argv
        .iter()
        .any(|argument| argument.chars().any(|character| character.is_control()))
    {
        return Err(AppError::Validation {
            field: "code_change_check.argv",
            message: "must not contain control characters",
        });
    }
    validate_identifier(
        "code_change_check.working_directory",
        &check.working_directory,
    )?;
    validate_optional_digest(check.output_digest.as_deref())?;
    validate_optional_text(
        "code_change_check.summary",
        check.summary.as_deref(),
        MAX_CODE_CHANGE_SUMMARY_BYTES,
    )
}

fn validate_transition(expected: CodeChangeState, next: CodeChangeState) -> Result<(), AppError> {
    let valid = matches!(
        (expected, next),
        (
            CodeChangeState::Reserved,
            CodeChangeState::PreparingWorktree
        ) | (CodeChangeState::PreparingWorktree, CodeChangeState::Editing)
            | (CodeChangeState::Editing, CodeChangeState::Checking)
            | (CodeChangeState::Checking, CodeChangeState::Committing)
            | (CodeChangeState::Committing, CodeChangeState::CandidateReady)
            | (
                CodeChangeState::CandidateReady,
                CodeChangeState::ExperimentSubmitted
            )
            | (
                CodeChangeState::ExperimentSubmitted,
                CodeChangeState::Evaluated
            )
            | (CodeChangeState::Evaluated, CodeChangeState::CleanupPending)
            | (CodeChangeState::CleanupPending, CodeChangeState::Completed)
    );
    if valid {
        Ok(())
    } else {
        Err(AppError::Validation {
            field: "code_change.state",
            message: "is not a permitted lifecycle transition",
        })
    }
}
