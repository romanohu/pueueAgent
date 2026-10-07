use std::{io::IsTerminal, path::Path};

use serde::Serialize;

use crate::{
    db::DecisionDoctorProjection,
    models::{Campaign, Experiment, Proposal},
    research::{ResearchReviewSummary, ResearchStatusProjection},
    AppError,
};

const MAX_OUTPUT_TEXT_BYTES: usize = 240;
const MAX_EXECUTION_PATH_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Human,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputTarget {
    Terminal,
    Pipe,
}

impl OutputTarget {
    pub fn detect() -> Self {
        if std::io::stdout().is_terminal() {
            Self::Terminal
        } else {
            Self::Pipe
        }
    }
}

impl OutputMode {
    pub fn uses_ansi(self, target: OutputTarget) -> bool {
        self == Self::Human
            && target == OutputTarget::Terminal
            && std::env::var_os("NO_COLOR").is_none()
    }
}

pub fn render_id(kind: &str, id: impl std::fmt::Display) -> String {
    format!("{kind}={id}")
}

pub fn human_header(command: &str, project_id: &str) -> String {
    format!(
        "pueue-agent {command} project={}",
        bounded_redacted_text(project_id)
    )
}

pub fn human_summary(summary: impl AsRef<str>) -> String {
    format!("summary: {}", bounded_redacted_text(summary.as_ref()))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct DecisionStatusProjection {
    cycle_id: String,
    source_experiment_id: String,
    state: crate::models::DecisionCycleState,
    attempt_count: i64,
    last_decision_kind: Option<String>,
    next_wake_at: Option<i64>,
    failure_code: Option<String>,
    failure_summary: Option<String>,
}

impl From<&DecisionDoctorProjection> for DecisionStatusProjection {
    fn from(decision: &DecisionDoctorProjection) -> Self {
        Self {
            cycle_id: bounded_redacted_text(&decision.cycle.cycle_id),
            source_experiment_id: bounded_redacted_text(&decision.cycle.source_experiment_id),
            state: decision.cycle.state,
            attempt_count: decision.attempt_count,
            last_decision_kind: safe_optional_text(decision.cycle.last_decision_kind.as_deref()),
            next_wake_at: decision.cycle.next_wake_at,
            failure_code: safe_optional_text(decision.cycle.last_failure_code.as_deref()),
            failure_summary: safe_optional_text(decision.cycle.last_failure_summary.as_deref()),
        }
    }
}

pub(crate) fn render_decision_status_line(decision: &DecisionStatusProjection) -> String {
    format!(
        "decision: cycle_id={} source_experiment_id={} state={} attempt_count={} last_decision_kind={} next_wake_at={} failure_code={} failure_summary={}",
        decision.cycle_id,
        decision.source_experiment_id,
        decision.state,
        decision.attempt_count,
        decision.last_decision_kind.as_deref().unwrap_or("none"),
        decision
            .next_wake_at
            .map_or_else(|| "none".to_owned(), |value| value.to_string()),
        decision.failure_code.as_deref().unwrap_or("none"),
        decision.failure_summary.as_deref().unwrap_or("none"),
    )
}

pub fn render_campaign_status(
    campaign: &Campaign,
    proposal_count: i64,
    experiment_counts: &std::collections::BTreeMap<String, i64>,
    budget_usage: &std::collections::BTreeMap<String, i64>,
    task_ids: &[i64],
    json: bool,
) -> Result<String, AppError> {
    render_campaign_status_with_decision(
        campaign,
        proposal_count,
        experiment_counts,
        budget_usage,
        task_ids,
        None,
        None,
        &[],
        json,
    )
}

pub(crate) fn render_campaign_status_with_decision(
    campaign: &Campaign,
    proposal_count: i64,
    experiment_counts: &std::collections::BTreeMap<String, i64>,
    budget_usage: &std::collections::BTreeMap<String, i64>,
    task_ids: &[i64],
    decision: Option<&DecisionStatusProjection>,
    research: Option<&ResearchStatusProjection>,
    research_history: &[ResearchReviewSummary],
    json: bool,
) -> Result<String, AppError> {
    let experiment_count = experiment_counts.values().sum::<i64>();
    if json {
        return serde_json::to_string(&serde_json::json!({
            "schema_version": 1,
            "project_id": bounded_redacted_text(&campaign.project_id),
            "campaign_id": bounded_redacted_text(&campaign.campaign_id),
            "state": campaign.state.as_str(),
            "state_reason": safe_optional_text(campaign.state_reason.as_deref()),
            "objective_digest": bounded_redacted_text(&campaign.objective_digest),
            "baseline_experiment_id": safe_optional_text(campaign.baseline_experiment_id.as_deref()),
            "next_eligible_at": campaign.next_eligible_at,
            "created_at": campaign.created_at,
            "updated_at": campaign.updated_at,
            "counts": {
                "proposals": proposal_count,
                "experiments": experiment_count,
                "experiment_states": experiment_counts,
            },
            "budget_usage": budget_usage,
            "task_ids": task_ids,
            "decision": decision,
            "research": research,
            "research_history": research_history,
        }))
        .map_err(|source| AppError::Serialization {
            operation: "serialize campaign status",
            source,
        });
    }

    let mut lines = vec![
        human_header("campaign status", &campaign.project_id),
        format!("campaign: {}", bounded_redacted_text(&campaign.campaign_id)),
        format!("state: {}", format_state(campaign.state.as_str())),
    ];
    if let Some(decision) = decision {
        lines.push(render_decision_status_line(decision));
    }
    if let Some(research) = research {
        lines.push(render_research_status_line(research));
        lines.extend(render_research_history_lines(research_history));
    }
    lines.extend([
        format!(
            "state_reason: {}",
            safe_optional_text(campaign.state_reason.as_deref())
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "objective_digest: {}",
            bounded_redacted_text(&campaign.objective_digest)
        ),
        format!(
            "baseline_experiment: {}",
            safe_optional_text(campaign.baseline_experiment_id.as_deref())
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "counts: proposals={} experiments={} states={}",
            proposal_count,
            experiment_count,
            render_counts(experiment_counts)
        ),
        format!("budget_usage: {}", render_counts(budget_usage)),
        format!(
            "task_ids: {}",
            if task_ids.is_empty() {
                "none".to_owned()
            } else {
                task_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            }
        ),
        format!(
            "timestamps: created_at={} updated_at={} next_eligible_at={}",
            campaign.created_at,
            campaign.updated_at,
            campaign
                .next_eligible_at
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned())
        ),
        human_summary("campaign state inspected"),
    ]);
    Ok(lines.join("\n"))
}

pub(crate) fn render_research_status_line(research: &ResearchStatusProjection) -> String {
    format!(
        "research: state={} next_due_at={} last_review_id={} experiment_id={} last_action={} checkpoint_confirmation={} blocked_reason={} discarded_reason={} session_generation={} session_rebuilt={}",
        bounded_redacted_text(&research.state),
        research
            .next_due_at
            .map_or_else(|| "none".to_owned(), |value| value.to_string()),
        safe_optional_text(research.last_review_id.as_deref()).unwrap_or_else(|| "none".to_owned()),
        safe_optional_text(research.experiment_id.as_deref()).unwrap_or_else(|| "none".to_owned()),
        safe_optional_text(research.last_action.as_deref()).unwrap_or_else(|| "none".to_owned()),
        safe_optional_text(research.checkpoint_confirmation.as_deref())
            .unwrap_or_else(|| "none".to_owned()),
        safe_optional_text(research.blocked_reason.as_deref()).unwrap_or_else(|| "none".to_owned()),
        safe_optional_text(research.discarded_reason.as_deref()).unwrap_or_else(|| "none".to_owned()),
        research.session_generation,
        session_rebuilt_label(research.session_rebuilt, "直近レビューでセッションを再構成"),
    )
}

pub(crate) fn render_research_history_lines(
    history: &[ResearchReviewSummary],
) -> Vec<String> {
    if history.is_empty() {
        return vec!["research_history: none".to_owned()];
    }
    history
        .iter()
        .map(|review| {
            format!(
                "research_history: review_id={} experiment_id={} state={} last_action={} checkpoint_confirmation={} blocked_reason={} discarded_reason={} session_generation={} session_rebuilt={}",
                bounded_redacted_text(&review.review_id),
                bounded_redacted_text(&review.experiment_id),
                bounded_redacted_text(&review.state),
                safe_optional_text(review.last_action.as_deref())
                    .unwrap_or_else(|| "none".to_owned()),
                safe_optional_text(review.checkpoint_confirmation.as_deref())
                    .unwrap_or_else(|| "none".to_owned()),
                safe_optional_text(review.blocked_reason.as_deref())
                    .unwrap_or_else(|| "none".to_owned()),
                safe_optional_text(review.discarded_reason.as_deref())
                    .unwrap_or_else(|| "none".to_owned()),
                review.session_generation,
                session_rebuilt_label(review.session_rebuilt, "このレビューでセッションを再構成"),
            )
        })
        .collect()
}

fn session_rebuilt_label(value: bool, explanation: &str) -> String {
    if value {
        format!("true({explanation})")
    } else {
        "false".to_owned()
    }
}

pub fn render_campaign_mutation(
    campaign: &Campaign,
    operation: &'static str,
    json: bool,
) -> Result<String, AppError> {
    if json {
        return serde_json::to_string(&serde_json::json!({
            "schema_version": 1,
            "operation": operation,
            "project_id": bounded_redacted_text(&campaign.project_id),
            "campaign_id": bounded_redacted_text(&campaign.campaign_id),
            "state": campaign.state.as_str(),
            "state_reason": safe_optional_text(campaign.state_reason.as_deref()),
            "updated_at": campaign.updated_at,
        }))
        .map_err(|source| AppError::Serialization {
            operation: "serialize campaign mutation",
            source,
        });
    }
    Ok([
        human_header(&format!("campaign {operation}"), &campaign.project_id),
        format!("campaign: {}", bounded_redacted_text(&campaign.campaign_id)),
        format!("state: {}", format_state(campaign.state.as_str())),
        format!(
            "state_reason: {}",
            safe_optional_text(campaign.state_reason.as_deref())
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!("updated_at: {}", campaign.updated_at),
        human_summary(format!("campaign {operation} complete")),
    ]
    .join("\n"))
}

pub fn render_proposal_list(
    campaign: &Campaign,
    proposals: &[Proposal],
    json: bool,
) -> Result<String, AppError> {
    if json {
        let proposals = proposals
            .iter()
            .map(proposal_list_value)
            .collect::<Vec<_>>();
        return serde_json::to_string(&serde_json::json!({
            "schema_version": 1,
            "project_id": bounded_redacted_text(&campaign.project_id),
            "campaign_id": bounded_redacted_text(&campaign.campaign_id),
            "proposals": proposals,
        }))
        .map_err(|source| AppError::Serialization {
            operation: "serialize campaign proposal list",
            source,
        });
    }
    let mut lines = vec![human_header("proposal list", &campaign.project_id)];
    lines.push(format!(
        "campaign: {}",
        bounded_redacted_text(&campaign.campaign_id)
    ));
    lines.extend(proposals.iter().map(|proposal| {
        format!(
            "proposal={} kind={} state={} source_experiment={} created_at={} updated_at={}",
            bounded_redacted_text(&proposal.proposal_id),
            proposal.kind.as_str(),
            format_state(proposal.status.as_str()),
            safe_optional_text(proposal.source_experiment_id.as_deref())
                .unwrap_or_else(|| "none".to_owned()),
            proposal.created_at,
            proposal.updated_at,
        )
    }));
    lines.push(human_summary(format!("{} proposal(s)", proposals.len())));
    Ok(lines.join("\n"))
}

pub fn render_proposal_inspection(
    campaign: &Campaign,
    proposal: &Proposal,
    json: bool,
) -> Result<String, AppError> {
    let evidence = proposal
        .expected_evidence
        .iter()
        .map(|value| bounded_redacted_text(value))
        .collect::<Vec<_>>();
    if json {
        let mut value = proposal_list_value(proposal);
        let object = value.as_object_mut().expect("proposal projection is an object");
        object.insert(
            "hypothesis".to_owned(),
            serde_json::Value::String(bounded_redacted_text(&proposal.hypothesis)),
        );
        object.insert("expected_evidence".to_owned(), serde_json::json!(evidence));
        object.insert(
            "canonical_digest".to_owned(),
            serde_json::Value::String(bounded_redacted_text(&proposal.canonical_digest)),
        );
        object.insert(
            "project_id".to_owned(),
            serde_json::Value::String(bounded_redacted_text(&campaign.project_id)),
        );
        object.insert("schema_version".to_owned(), serde_json::json!(1));
        return serde_json::to_string(&value).map_err(|source| AppError::Serialization {
            operation: "serialize campaign proposal inspection",
            source,
        });
    }
    Ok([
        human_header("proposal inspect", &campaign.project_id),
        format!("proposal: {}", bounded_redacted_text(&proposal.proposal_id)),
        format!("campaign: {}", bounded_redacted_text(&proposal.campaign_id)),
        format!("kind: {}", proposal.kind.as_str()),
        format!("state: {}", format_state(proposal.status.as_str())),
        format!("hypothesis: {}", bounded_redacted_text(&proposal.hypothesis)),
        format!(
            "expected_evidence: {}",
            if evidence.is_empty() {
                "none".to_owned()
            } else {
                evidence.join(", ")
            }
        ),
        format!(
            "canonical_digest: {}",
            bounded_redacted_text(&proposal.canonical_digest)
        ),
        format!("timestamps: created_at={} updated_at={}", proposal.created_at, proposal.updated_at),
        human_summary("proposal inspected"),
    ]
    .join("\n"))
}

pub fn render_experiment_list(
    campaign: &Campaign,
    experiments: &[Experiment],
    json: bool,
) -> Result<String, AppError> {
    if json {
        let experiments = experiments
            .iter()
            .map(experiment_list_value)
            .collect::<Vec<_>>();
        return serde_json::to_string(&serde_json::json!({
            "schema_version": 1,
            "project_id": bounded_redacted_text(&campaign.project_id),
            "campaign_id": bounded_redacted_text(&campaign.campaign_id),
            "experiments": experiments,
        }))
        .map_err(|source| AppError::Serialization {
            operation: "serialize campaign experiment list",
            source,
        });
    }
    let mut lines = vec![human_header("experiment list", &campaign.project_id)];
    lines.push(format!(
        "campaign: {}",
        bounded_redacted_text(&campaign.campaign_id)
    ));
    lines.extend(experiments.iter().map(|experiment| {
        format!(
            "experiment={} state={} proposal={} submission={} task_id={} created_at={} updated_at={}",
            bounded_redacted_text(&experiment.experiment_id),
            format_state(experiment.status.as_str()),
            bounded_redacted_text(&experiment.proposal_id),
            bounded_redacted_text(&experiment.submission_id),
            experiment
                .pueue_task_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            experiment.created_at,
            experiment.updated_at,
        )
    }));
    lines.push(human_summary(format!("{} experiment(s)", experiments.len())));
    Ok(lines.join("\n"))
}

pub fn render_experiment_inspection(
    campaign: &Campaign,
    experiment: &Experiment,
    argv_digest: &str,
    research_history: &[ResearchReviewSummary],
    json: bool,
) -> Result<String, AppError> {
    if json {
        let mut value = experiment_list_value(experiment);
        let object = value
            .as_object_mut()
            .expect("experiment projection is an object");
        object.insert("schema_version".to_owned(), serde_json::json!(1));
        object.insert(
            "project_id".to_owned(),
            serde_json::Value::String(bounded_redacted_text(&campaign.project_id)),
        );
        object.insert(
            "argv_digest".to_owned(),
            serde_json::Value::String(argv_digest.to_owned()),
        );
        object.insert(
            "task_signature".to_owned(),
            safe_optional_text(experiment.task_signature.as_deref())
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
        );
        object.insert(
            "research_history".to_owned(),
            serde_json::to_value(research_history).map_err(|source| AppError::Serialization {
                operation: "serialize experiment research history",
                source,
            })?,
        );
        return serde_json::to_string(&value).map_err(|source| AppError::Serialization {
            operation: "serialize campaign experiment inspection",
            source,
        });
    }
    let mut lines = vec![
        human_header("experiment inspect", &campaign.project_id),
        format!("experiment: {}", bounded_redacted_text(&experiment.experiment_id)),
        format!("campaign: {}", bounded_redacted_text(&experiment.campaign_id)),
        format!("proposal: {}", bounded_redacted_text(&experiment.proposal_id)),
        format!("submission: {}", bounded_redacted_text(&experiment.submission_id)),
        format!(
            "parent_experiment: {}",
            safe_optional_text(experiment.parent_experiment_id.as_deref())
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!("attempt: {}", experiment.attempt),
        format!("state: {}", format_state(experiment.status.as_str())),
        format!(
            "task_id: {}",
            experiment
                .pueue_task_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "task_signature: {}",
            safe_optional_text(experiment.task_signature.as_deref())
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!("argv_digest: {argv_digest}"),
        format!(
            "failure_code: {}",
            safe_optional_text(experiment.failure_code.as_deref())
                .unwrap_or_else(|| "none".to_owned())
        ),
    ];
    lines.extend(render_research_history_lines(research_history));
    lines.extend([
        format!(
            "timestamps: created_at={} updated_at={} finished_at={}",
            experiment.created_at,
            experiment.updated_at,
            experiment
                .finished_at
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned())
        ),
        human_summary("experiment inspected"),
    ]);
    Ok(lines.join("\n"))
}

fn proposal_list_value(proposal: &Proposal) -> serde_json::Value {
    serde_json::json!({
        "proposal_id": bounded_redacted_text(&proposal.proposal_id),
        "campaign_id": bounded_redacted_text(&proposal.campaign_id),
        "kind": proposal.kind.as_str(),
        "status": proposal.status.as_str(),
        "source_experiment_id": safe_optional_text(proposal.source_experiment_id.as_deref()),
        "reject_reason": safe_optional_text(proposal.reject_reason.as_deref()),
        "created_at": proposal.created_at,
        "updated_at": proposal.updated_at,
    })
}

fn experiment_list_value(experiment: &Experiment) -> serde_json::Value {
    serde_json::json!({
        "experiment_id": bounded_redacted_text(&experiment.experiment_id),
        "campaign_id": bounded_redacted_text(&experiment.campaign_id),
        "proposal_id": bounded_redacted_text(&experiment.proposal_id),
        "submission_id": bounded_redacted_text(&experiment.submission_id),
        "parent_experiment_id": safe_optional_text(experiment.parent_experiment_id.as_deref()),
        "attempt": experiment.attempt,
        "status": experiment.status.as_str(),
        "pueue_task_id": experiment.pueue_task_id,
        "failure_code": safe_optional_text(experiment.failure_code.as_deref()),
        "created_at": experiment.created_at,
        "updated_at": experiment.updated_at,
        "finished_at": experiment.finished_at,
    })
}

fn safe_optional_text(value: Option<&str>) -> Option<String> {
    value.map(bounded_redacted_text)
}

fn render_counts(counts: &std::collections::BTreeMap<String, i64>) -> String {
    if counts.is_empty() {
        return "none".to_owned();
    }
    counts
        .iter()
        .map(|(key, value)| format!("{}={value}", bounded_redacted_text(key)))
        .collect::<Vec<_>>()
        .join(",")
}

pub fn format_state(state: &str) -> String {
    let state = state.to_ascii_lowercase();
    if !OutputMode::Human.uses_ansi(OutputTarget::detect()) {
        return state;
    }

    let color = match state.as_str() {
        "running" | "completed" | "confirmed" => "32",
        "queued" | "pending" | "claimed" | "starting" => "33",
        "failed" | "timed_out" | "cancelled" | "halted" => "31",
        _ => return state,
    };
    format!("\x1b[{color}m{state}\x1b[0m")
}

pub fn redact_sensitive_text(value: &str) -> String {
    let sanitized = strip_control_and_ansi(value);
    let tokens = lex_tokens(&sanitized);
    redact_sensitive_tokens(&tokens, false, false).0.join(" ")
}

/// Return whether source text can be published byte-for-byte under the
/// existing sensitivity policy. This is an eligibility predicate only: it
/// deliberately does not bound, normalize, or emit the source text.
pub(crate) fn permits_lossless_evidence_text(value: &str) -> bool {
    let mut characters = value.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '\r' if characters.peek() != Some(&'\n') => return false,
            '\n' | '\r' | '\t' => {}
            character if character.is_control() => return false,
            _ => {}
        }
    }
    let tokens = lex_tokens(value);
    !redact_sensitive_tokens(&tokens, true, true).1
}

fn redact_sensitive_tokens(
    tokens: &[LexToken],
    allow_unquoted_slash_operators: bool,
    reject_raw_backslashes: bool,
) -> (Vec<String>, bool) {
    let mut redacted = Vec::with_capacity(tokens.len());
    let mut redacted_any = false;
    let mut redact_next = false;
    let mut redact_assignment_value = false;
    let mut assignment_redaction_emitted = false;
    let mut redact_structured_value = false;

    let mut index = 0;
    while index < tokens.len() {
        let token = &tokens[index];

        if redact_assignment_value {
            if is_assignment_boundary(tokens, index) {
                redact_assignment_value = false;
                assignment_redaction_emitted = false;
                continue;
            }
            if !assignment_redaction_emitted {
                redacted.push("[REDACTED]".to_owned());
                redacted_any = true;
                assignment_redaction_emitted = true;
            }
            index += 1;
            continue;
        }

        if redact_structured_value {
            if is_assignment_boundary(tokens, index) {
                redact_structured_value = false;
                continue;
            }
            if token.value.eq_ignore_ascii_case("bearer") {
                redacted.push(token.value.to_owned());
                redact_next = true;
            } else if token.value == "=" {
                redacted.push(token.value.to_owned());
                redact_assignment_value = true;
                assignment_redaction_emitted = false;
            } else {
                redacted.push("[REDACTED]".to_owned());
                redacted_any = true;
                redact_assignment_value = true;
                assignment_redaction_emitted = true;
            }
            redact_structured_value = false;
            index += 1;
            continue;
        }

        if redact_next {
            if token.value == "=" {
                redacted.push(token.value.to_owned());
                redact_assignment_value = true;
                assignment_redaction_emitted = false;
            } else {
                redacted.push("[REDACTED]".to_owned());
                redacted_any = true;
            }
            redact_next = false;
            index += 1;
            continue;
        }

        let is_unquoted_slash_operator = allow_unquoted_slash_operators
            && !token.quoted
            && !token.had_backslash
            && matches!(token.value.as_str(), "/" | "//" | "/=");
        let is_raw_backslash_path = reject_raw_backslashes && token.had_backslash;
        if (is_path_token(&token.value) || is_raw_backslash_path) && !is_unquoted_slash_operator {
            redacted.push("[path]".to_owned());
            redacted_any = true;
            index += 1;
            continue;
        }

        // Check provider credentials before preserving any assignment label:
        // a credential-shaped string can itself appear as a key or flag.
        if is_bare_secret_token(&token.value) {
            redacted.push("[REDACTED]".to_owned());
            redacted_any = true;
            index += 1;
            continue;
        }

        if let Some((key, _)) = token.value.split_once('=') {
            if is_sensitive_key(key) {
                redacted.push(format!("{key}=[REDACTED]"));
                redacted_any = true;
                redact_assignment_value = true;
                assignment_redaction_emitted = true;
                index += 1;
                continue;
            }
        }

        if let Some((key, _)) = token.value.split_once(':') {
            if is_sensitive_key(key) {
                redacted.push(format!("{key}:"));
                redact_structured_value = true;
                index += 1;
                continue;
            }
        }

        if let Some((flag, _)) = token.value.split_once('=') {
            if is_sensitive_flag(flag) {
                redacted.push(format!("{flag}=[REDACTED]"));
                redacted_any = true;
                index += 1;
                continue;
            }
        }

        if let Some(session_id_token) = classify_session_id_token(&token.value) {
            match session_id_token {
                SessionIdToken::Label { raw } => {
                    redacted.push(raw.to_owned());
                    if let Some(separator) = tokens
                        .get(index + 1)
                        .filter(|next| !next.quoted)
                        .and_then(|next| match next.value.as_str() {
                            "=" | ":" | ":=" => Some(next.value.as_str()),
                            _ => None,
                        })
                    {
                        redacted.push(separator.to_owned());
                        redact_assignment_value = true;
                        assignment_redaction_emitted = false;
                        index += 2;
                    } else {
                        redact_assignment_value = true;
                        assignment_redaction_emitted = false;
                        index += 1;
                    }
                }
                SessionIdToken::Assignment {
                    label,
                    separator,
                    inline_value,
                } => {
                    redact_assignment_value = true;
                    assignment_redaction_emitted = inline_value;
                    if inline_value {
                        redacted.push(format!("{label}{separator}[REDACTED]"));
                        redacted_any = true;
                    } else {
                        redacted.push(format!("{label}{separator}"));
                    }
                    index += 1;
                }
            }
            continue;
        }

        if is_sensitive_flag(&token.value) || token.value.eq_ignore_ascii_case("bearer") {
            redacted.push(token.value.to_owned());
            if tokens
                .get(index + 1)
                .is_some_and(|next| !next.quoted && next.value == "=")
            {
                redacted.push("=".to_owned());
                redact_assignment_value = true;
                assignment_redaction_emitted = false;
                index += 2;
            } else {
                redact_next = true;
                index += 1;
            }
            continue;
        }

        if is_sensitive_marker(&token.value) {
            if tokens
                .get(index + 1)
                .is_some_and(|next| !next.quoted && next.value == "=")
            {
                redacted.push(token.value.to_owned());
                redacted.push("=".to_owned());
                redact_assignment_value = true;
                assignment_redaction_emitted = false;
                index += 2;
            } else {
                redacted.push("[REDACTED]".to_owned());
                redacted_any = true;
                redact_next = true;
                index += 1;
            }
            continue;
        }

        redacted.push(token.value.to_owned());
        index += 1;
    }

    (redacted, redacted_any)
}

fn is_assignment_boundary(tokens: &[LexToken], index: usize) -> bool {
    let token = &tokens[index];
    (!token.quoted && token.value.starts_with('-') && token.value.len() > 1)
        || (!token.quoted
            && token
                .value
                .split_once('=')
                .is_some_and(|(key, _)| is_sensitive_key(key) || is_sensitive_flag(key)))
        || tokens
            .get(index + 1)
            .is_some_and(|next| !next.quoted && next.value == "=")
}

pub fn bounded_redacted_text(value: &str) -> String {
    bounded_text(redact_sensitive_text(value))
}

/// Bound text assembled exclusively from static or typed, already-validated
/// diagnostic fields. Unlike generic redaction, this preserves deliberate
/// separators and verified absolute paths while still removing terminal
/// control sequences.
pub(crate) fn bounded_typed_text(value: &str) -> String {
    bounded_text(strip_control_and_ansi(value))
}

fn bounded_text(value: String) -> String {
    if value.len() <= MAX_OUTPUT_TEXT_BYTES {
        return value;
    }

    let mut prefix = String::new();
    for character in value.chars() {
        if prefix.len() + character.len_utf8() > MAX_OUTPUT_TEXT_BYTES - 3 {
            break;
        }
        prefix.push(character);
    }
    format!("{prefix}...")
}

/// Render only a persisted executable or registered-root path, never a command argument.
///
/// Unlike generic redaction this preserves a verified absolute executable
/// path. Callers must use it only for a typed persisted path projection;
/// malformed or oversized database text is omitted rather than rendered.
pub fn bounded_execution_path(value: &str) -> Option<String> {
    if value.is_empty()
        || value.len() > MAX_EXECUTION_PATH_BYTES
        || value.chars().any(char::is_control)
        || !Path::new(value).is_absolute()
    {
        return None;
    }
    Some(value.to_owned())
}

fn is_sensitive_flag(value: &str) -> bool {
    let normalized = value
        .trim_start_matches('-')
        .to_ascii_lowercase()
        .replace('_', "-");
    matches!(
        normalized.as_str(),
        "token" | "api-key" | "password" | "secret" | "prompt" | "transcript"
    ) || normalized.ends_with("-token")
        || normalized.ends_with("-secret")
        || normalized.ends_with("-password")
        || normalized.ends_with("-key")
}

fn is_sensitive_key(value: &str) -> bool {
    let key = value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
        .collect::<String>()
        .to_ascii_uppercase()
        .replace('-', "_");
    key.contains("TOKEN")
        || key.contains("SECRET")
        || key.contains("PASSWORD")
        || key.contains("PASSWD")
        || key.contains("ACCESS_KEY")
        || key.contains("API_KEY")
        || key.contains("AUTHORIZATION")
        || key.contains("CREDENTIAL")
        || key.contains("PRIVATE_KEY")
}

fn is_sensitive_marker(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    let normalized = lower.trim_matches(|character| {
        matches!(character, ':' | '=' | '`' | ',' | ';' | '"' | '\'')
    });
    [
        "token",
        "secret",
        "password",
        "passwd",
        "prompt",
        "transcript",
        "log_path",
        "session_id",
        "cookie",
        "apikey",
        "api_key",
        "authorization",
        "bearer",
        "credential",
        "private_key",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
}

enum SessionIdToken<'a> {
    Label {
        raw: &'a str,
    },
    Assignment {
        label: &'a str,
        separator: &'a str,
        inline_value: bool,
    },
}

fn classify_session_id_token(value: &str) -> Option<SessionIdToken<'_>> {
    const LABEL: &str = "agent.context.session_id";
    let leading_trimmed = value.trim_start_matches(is_session_id_punctuation);
    let leading_len = value.len() - leading_trimmed.len();
    let lower = leading_trimmed.to_ascii_lowercase();
    if !lower.starts_with(LABEL) {
        return None;
    }
    let label_end = leading_len + LABEL.len();
    let label = &value[..label_end];
    let suffix = &value[label_end..];
    for separator in [":=", "=", ":"] {
        if let Some(rest) = suffix.strip_prefix(separator) {
            let inline_value = !rest.is_empty()
                && !rest
                    .chars()
                    .all(is_session_id_punctuation);
            return Some(SessionIdToken::Assignment {
                label,
                separator,
                inline_value,
            });
        }
    }
    if suffix.chars().all(is_session_id_punctuation) {
        return Some(SessionIdToken::Label { raw: value });
    }
    None
}

fn is_session_id_punctuation(character: char) -> bool {
    matches!(
        character,
        ':' | '`' | ',' | ';' | '"' | '\'' | '.' | '(' | ')' | '[' | ']' | '{' | '}'
    )
}

fn is_bare_secret_token(value: &str) -> bool {
    // Logs and structured diagnostics commonly wrap credentials in punctuation
    // or attach them to otherwise innocuous labels. Check each credential-shaped
    // component so `value=<credential>`, `(<credential>)`, and JSON arrays cannot
    // bypass the same policy used for standalone provider tokens. Redact the
    // containing lexical token as a whole rather than publishing a partial value.
    value
        .split(|character: char| {
            !character.is_ascii_alphanumeric() && !matches!(character, '_' | '-')
        })
        .any(|component| {
            component.len() >= 20
                && ["ghp_", "github_pat_", "sk-", "xoxb-", "xoxp-", "akia"]
                    .iter()
                    .any(|prefix| {
                        component
                            .get(..prefix.len())
                            .is_some_and(|start| start.eq_ignore_ascii_case(prefix))
                    })
        })
}

fn is_path_token(token: &str) -> bool {
    token.starts_with('/')
        || token.starts_with("~/")
        || token.starts_with("./")
        || token.starts_with("../")
        || token.contains('/')
        || token.contains('\\')
        || token
            .as_bytes()
            .get(1)
            .is_some_and(|character| *character == b':')
}

fn strip_control_and_ansi(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut characters = value.chars().peekable();

    while let Some(character) = characters.next() {
        if character != '\x1b' {
            if character.is_control() {
                output.push(' ');
            } else {
                output.push(character);
            }
            continue;
        }

        match characters.peek().copied() {
            Some('[') => {
                characters.next();
                for sequence_character in characters.by_ref() {
                    if ('@'..='~').contains(&sequence_character) {
                        break;
                    }
                }
            }
            Some(']') => {
                characters.next();
                let mut previous = None;
                for sequence_character in characters.by_ref() {
                    if sequence_character == '\x07'
                        || (previous == Some('\x1b') && sequence_character == '\\')
                    {
                        break;
                    }
                    previous = Some(sequence_character);
                }
            }
            _ => {}
        }
    }

    output
}

#[derive(Debug)]
struct LexToken {
    value: String,
    quoted: bool,
    had_backslash: bool,
}

fn lex_tokens(value: &str) -> Vec<LexToken> {
    let mut characters = value.chars().peekable();
    let mut tokens = Vec::new();

    loop {
        while characters
            .peek()
            .is_some_and(|character| character.is_whitespace())
        {
            characters.next();
        }

        if characters.peek().is_none() {
            break;
        }

        let mut token = String::new();
        let mut quote = None;
        let mut quoted = false;
        let mut had_backslash = false;
        while let Some(character) = characters.next() {
            if let Some(quote_character) = quote {
                if character == quote_character {
                    quote = None;
                } else if character == '\\' {
                    had_backslash = true;
                    if let Some(escaped) = characters.next() {
                        token.push(escaped);
                    }
                } else {
                    token.push(character);
                }
                continue;
            }

            if character == '\'' || character == '"' {
                quote = Some(character);
                quoted = true;
            } else if character == '\\' {
                had_backslash = true;
                if let Some(escaped) = characters.next() {
                    token.push(escaped);
                }
            } else if character.is_whitespace() {
                break;
            } else {
                token.push(character);
            }
        }
        tokens.push(LexToken {
            value: token,
            quoted,
            had_backslash,
        });
    }

    tokens
}

#[cfg(test)]
mod tests {
    use super::{permits_lossless_evidence_text, redact_sensitive_text};

    #[test]
    fn provider_credentials_are_redacted_inside_punctuation_and_plain_labels() {
        for prefix in ["ghp_", "github_pat_", "sk-", "xoxb-", "xoxp-", "AKIA"] {
            let credential = format!("{prefix}abcdefghijklmnopqrstuvwxyz123456");
            for value in [
                credential.clone(),
                format!("({credential})"),
                format!("[{credential}],"),
                format!("value={credential}"),
                format!("detail:{credential}."),
                format!(r#"{{"values":["{credential}"]}}"#),
                format!("ordinary,{credential},text"),
                format!("「{credential}」"),
                format!("{credential}=value"),
                format!("{credential}:value"),
            ] {
                let source = format!("inspect {value} now");
                assert_eq!(redact_sensitive_text(&source), "inspect [REDACTED] now");
                assert!(!permits_lossless_evidence_text(&source));
            }
        }
    }

    #[test]
    fn provider_credential_detection_preserves_short_and_embedded_lookalikes() {
        for source in [
            "inspect ghp_short now",
            "inspect (ghp_short) now",
            "inspect notghp_abcdefghijklmnopqrstuvwxyz123456 now",
            "inspect github_project now",
            "inspect value=ordinary now",
        ] {
            assert_eq!(redact_sensitive_text(source), source);
            assert!(permits_lossless_evidence_text(source));
        }
    }

    #[test]
    fn lossless_source_allows_multiline_indentation_and_cpu_fixture_syntax() {
        let source = format!(
            "# CPU trainer\n\ndef train(value):\n\treturn value / 2\n{}\n",
            "# ordinary source ".repeat(20)
        );

        assert!(source.len() > 240);
        assert!(permits_lossless_evidence_text(&source));
        assert!(permits_lossless_evidence_text(
            "def train():\r\n\treturn 1 / 2\r\n"
        ));
    }

    #[test]
    fn lossless_source_rejects_relative_backslash_paths_and_preserves_display_redaction() {
        for source in [
            "model\\step.pt\n",
            "\"model\\step.pt\"\n",
            "model\\\\step.pt\n",
            concat!(r"\/", "\n"),
            concat!(r"\//", "\n"),
            concat!(r"\/=", "\n"),
        ] {
            assert!(!permits_lossless_evidence_text(source), "source={source:?}");
        }
        assert_eq!(redact_sensitive_text("model\\step.pt"), "modelstep.pt");
        assert_eq!(redact_sensitive_text("C:\\model\\step.json"), "[path]");
    }

    #[test]
    fn lossless_source_rejects_lone_carriage_return() {
        assert!(!permits_lossless_evidence_text("before\rafter\n"));
        assert!(!permits_lossless_evidence_text("before\r"));
    }

    #[test]
    fn real_research_cpu_trainer_source_is_lossless_eligible() {
        let source = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/e2e/research_experiment/train.py"
        ))
        .unwrap();

        // The fixture uses syntax-only spacing and chr(10) to preserve its
        // trainer behavior while satisfying the bounded source policy.
        assert!(permits_lossless_evidence_text(&source));
    }

    #[test]
    fn lossless_source_allows_only_exact_unquoted_slash_operators() {
        assert!(permits_lossless_evidence_text(
            "value / other\nratio // count\nvalue /= 2\n"
        ));
        assert!(!permits_lossless_evidence_text("path = /tmp/model\n"));
        assert!(!permits_lossless_evidence_text("compact = a/b\n"));
        assert!(!permits_lossless_evidence_text("quoted = \"/\"\n"));
        assert!(!permits_lossless_evidence_text("quoted = '//'\n"));
        assert!(!permits_lossless_evidence_text(
            "url = https://example.test/model\n"
        ));
        assert!(!permits_lossless_evidence_text(
            "windows = C:\\model\\step.json\n"
        ));
    }

    #[test]
    fn lossless_source_rejects_existing_sensitive_classifiers_and_controls() {
        for source in [
            "API_KEY = \"value\"\n",
            "--token secret-value\n",
            "agent.context.session_id: abc\n",
            "before\x1b[31mafter\n",
            "before\0after\n",
            "before\x0bafter\n",
        ] {
            assert!(!permits_lossless_evidence_text(source), "source={source:?}");
        }
    }
}
