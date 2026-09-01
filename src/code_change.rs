//! Pure protocol and repository-shape validation for code-change proposals.
//!
//! The stateful worktree, editor, and check runners are added in a later
//! phase.  This module intentionally keeps the admission-facing data small and
//! deterministic so it can be validated before any child process is started.

use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    execution_policy::{CampaignLimits, CodeChangeTool},
    AppError,
};

pub const RUST_CHECK: &[&str] = &["cargo", "test", "--all-targets", "--", "--test-threads=1"];
pub const UV_PYTEST_CHECK: &[&str] = &["uv", "run", "pytest"];
pub const PYTHON_PYTEST_CHECK: &[&str] = &["python", "-m", "pytest"];

const MAX_EDITOR_OUTPUT_BYTES: usize = 64 * 1024;
const MAX_EDITOR_SUMMARY_BYTES: usize = 64 * 1024;
const MAX_CHECK_SOURCE_BYTES: usize = 64;
const MAX_CHECK_ARG_BYTES: usize = 4 * 1024;
const MAX_CHECK_ARG_COUNT: usize = 32;
const MAX_INTERNAL_ID_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EditorOutput {
    pub schema_version: u32,
    pub status: String,
    pub summary: String,
    pub proposed_checks: Vec<ProposedCheck>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposedCheck {
    pub source: String,
    pub argv: Vec<String>,
    pub working_directory: String,
}

pub fn canonical_full_sha(value: &str) -> Result<String, AppError> {
    if (value.len() != 40 && value.len() != 64)
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(validation(
            "code_change.sha",
            "must be a lowercase full object ID",
        ));
    }
    Ok(value.to_owned())
}

pub fn validate_full_sha(value: &str) -> Result<(), AppError> {
    canonical_full_sha(value).map(|_| ())
}

pub fn candidate_ref(campaign_id: &str, proposal_id: &str) -> Result<String, AppError> {
    validate_internal_id("campaign_id", campaign_id)?;
    validate_internal_id("proposal_id", proposal_id)?;
    Ok(format!(
        "campaign/{}/candidate/{}",
        campaign_id, proposal_id,
    ))
}

pub fn best_ref(campaign_id: &str) -> Result<String, AppError> {
    validate_internal_id("campaign_id", campaign_id)?;
    Ok(format!("campaign/{}/best", campaign_id))
}

pub fn owned_worktree_relative_path(
    campaign_id: &str,
    proposal_id: &str,
) -> Result<PathBuf, AppError> {
    validate_internal_id("campaign_id", campaign_id)?;
    validate_internal_id("proposal_id", proposal_id)?;
    Ok(PathBuf::from(".pueue-agent")
        .join("worktrees")
        .join(campaign_id)
        .join(proposal_id))
}

pub fn parse_editor_output(
    input: &[u8],
    root: &Path,
    limits: &CampaignLimits,
    available_tools: &BTreeSet<CodeChangeTool>,
) -> Result<EditorOutput, AppError> {
    if input.len() > MAX_EDITOR_OUTPUT_BYTES {
        return Err(validation(
            "code_change.editor_output",
            "exceeds the bounded editor output size",
        ));
    }
    let output: EditorOutput = serde_json::from_slice(input)
        .map_err(|_| validation("code_change.editor_output", "must be strict editor JSON"))?;
    validate_editor_output(&output, root, limits, available_tools)?;
    Ok(output)
}

pub fn parse_editor_json(
    input: &[u8],
    root: &Path,
    limits: &CampaignLimits,
    available_tools: &BTreeSet<CodeChangeTool>,
) -> Result<EditorOutput, AppError> {
    parse_editor_output(input, root, limits, available_tools)
}

pub fn validate_editor_output(
    output: &EditorOutput,
    root: &Path,
    limits: &CampaignLimits,
    available_tools: &BTreeSet<CodeChangeTool>,
) -> Result<(), AppError> {
    if output.schema_version != 1 {
        return Err(validation(
            "code_change.schema_version",
            "must be version 1",
        ));
    }
    if output.status != "ready" && output.status != "cannot_apply" {
        return Err(validation(
            "code_change.status",
            "must be ready or cannot_apply",
        ));
    }
    if output.summary.len() > MAX_EDITOR_SUMMARY_BYTES {
        return Err(validation(
            "code_change.summary",
            "exceeds the bounded summary size",
        ));
    }
    validate_proposed_checks(&output.proposed_checks, root, limits, available_tools)?;
    if output.status == "ready" && output.proposed_checks.is_empty() {
        return Err(validation(
            "code_change.proposed_checks",
            "ready output must include a project check",
        ));
    }
    Ok(())
}

pub fn validate_proposed_checks(
    checks: &[ProposedCheck],
    root: &Path,
    limits: &CampaignLimits,
    available_tools: &BTreeSet<CodeChangeTool>,
) -> Result<(), AppError> {
    if checks.len() > limits.max_code_change_checks as usize {
        return Err(validation(
            "code_change.proposed_checks",
            "exceeds the bounded check count",
        ));
    }
    if !root.is_absolute() {
        return Err(validation(
            "code_change.root",
            "must be an absolute project root",
        ));
    }
    for check in checks {
        if check.source.is_empty() || check.source.len() > MAX_CHECK_SOURCE_BYTES {
            return Err(validation(
                "code_change_check.source",
                "must be bounded non-empty text",
            ));
        }
        validate_relative_working_directory(&check.working_directory)?;
        if check.argv.is_empty() || check.argv.len() > MAX_CHECK_ARG_COUNT {
            return Err(validation(
                "code_change_check.argv",
                "must be a bounded non-empty argv",
            ));
        }
        if check.argv.iter().any(|arg| {
            arg.is_empty() || arg.len() > MAX_CHECK_ARG_BYTES || arg.chars().any(char::is_control)
        }) {
            return Err(validation(
                "code_change_check.argv",
                "contains an invalid argument",
            ));
        }
        if shell_argv(&check.argv) {
            return Err(validation(
                "code_change_check.argv",
                "shell command execution is not permitted",
            ));
        }
        let tool = tool_for_program(&check.argv[0]).ok_or_else(|| {
            validation(
                "code_change_check.argv",
                "must start with a pinned code-change tool",
            )
        })?;
        if !available_tools.contains(&tool) {
            return Err(validation(
                "code_change_check.argv",
                "requested tool is not available in the startup policy",
            ));
        }
        if check.source
            != match tool {
                CodeChangeTool::Git => "git",
                CodeChangeTool::Cargo => "cargo",
                CodeChangeTool::Uv => "uv",
                CodeChangeTool::Python => "python",
            }
        {
            return Err(validation(
                "code_change_check.source",
                "does not match the pinned check tool",
            ));
        }
        let expected = match tool {
            CodeChangeTool::Cargo => RUST_CHECK,
            CodeChangeTool::Uv => UV_PYTEST_CHECK,
            CodeChangeTool::Python => PYTHON_PYTEST_CHECK,
            CodeChangeTool::Git => &[],
        };
        if tool == CodeChangeTool::Git {
            return Err(validation(
                "code_change_check.argv",
                "Git is reserved for the coordinator diff check",
            ));
        }
        if !check
            .argv
            .iter()
            .map(String::as_str)
            .eq(expected.iter().copied())
        {
            return Err(validation(
                "code_change_check.argv",
                "must use the fixed project-check argv",
            ));
        }
    }
    Ok(())
}

pub fn discover_project_checks(
    root: &Path,
    available_tools: &BTreeSet<CodeChangeTool>,
) -> Result<Vec<ProposedCheck>, AppError> {
    if !root.is_absolute() {
        return Err(validation("code_change.root", "must be absolute"));
    }
    let mut checks = Vec::new();
    if root.join("Cargo.toml").is_file() && available_tools.contains(&CodeChangeTool::Cargo) {
        checks.push(ProposedCheck {
            source: "cargo".to_owned(),
            argv: RUST_CHECK.iter().map(|arg| (*arg).to_owned()).collect(),
            working_directory: ".".to_owned(),
        });
    }
    let pytest = root.join("pytest.ini").is_file() || pyproject_has_pytest(root);
    if pytest {
        let (tool, argv) = if root.join("uv.lock").is_file() {
            if available_tools.contains(&CodeChangeTool::Uv) {
                ("uv", UV_PYTEST_CHECK)
            } else {
                ("", &[] as &[&str])
            }
        } else if available_tools.contains(&CodeChangeTool::Python) {
            ("python", PYTHON_PYTEST_CHECK)
        } else {
            ("", &[] as &[&str])
        };
        if !tool.is_empty() {
            checks.push(ProposedCheck {
                source: tool.to_owned(),
                argv: argv.iter().map(|arg| (*arg).to_owned()).collect(),
                working_directory: ".".to_owned(),
            });
        }
    }
    Ok(checks)
}

pub fn discover_checks(
    root: &Path,
    available_tools: &BTreeSet<CodeChangeTool>,
) -> Result<Vec<ProposedCheck>, AppError> {
    discover_project_checks(root, available_tools)
}

pub fn is_protected_path(path: &Path) -> bool {
    path.components().any(|component| {
        let Component::Normal(name) = component else {
            return false;
        };
        name == ".git" || name == ".pueue-agent" || name == "execution-policy.toml"
    })
}

pub fn validate_protected_paths(paths: &[PathBuf]) -> Result<(), AppError> {
    if paths.iter().any(|path| is_protected_path(path)) {
        Err(validation(
            "code_change.diff_paths",
            "contains a protected service path",
        ))
    } else {
        Ok(())
    }
}

pub fn validate_diff_limits(
    changed_files: usize,
    diff_bytes: usize,
    limits: &CampaignLimits,
) -> Result<(), AppError> {
    if changed_files > limits.max_code_change_changed_files as usize {
        return Err(validation(
            "code_change.changed_files",
            "exceeds the configured changed-file cap",
        ));
    }
    if diff_bytes > limits.max_code_change_diff_bytes as usize {
        return Err(validation(
            "code_change.diff_bytes",
            "exceeds the configured diff-byte cap",
        ));
    }
    Ok(())
}

pub fn has_project_check(checks: &[ProposedCheck]) -> bool {
    checks.iter().any(|check| {
        check.argv == RUST_CHECK
            || check.argv == UV_PYTEST_CHECK
            || check.argv == PYTHON_PYTEST_CHECK
    })
}

fn validate_internal_id(field: &'static str, value: &str) -> Result<(), AppError> {
    if value.is_empty()
        || value.len() > MAX_INTERNAL_ID_BYTES
        || value == "."
        || value == ".."
        || value.bytes().any(|byte| {
            !matches!(
                byte,
                b'a'..=b'z'
                    | b'A'..=b'Z'
                    | b'0'..=b'9'
                    | b'_'
                    | b'-'
                    | b'.'
                    | b':'
            )
        })
    {
        return Err(validation(field, "must be a safe internal identifier"));
    }
    Ok(())
}

fn validate_relative_working_directory(value: &str) -> Result<(), AppError> {
    let path = Path::new(value);
    if value.is_empty()
        || value.chars().any(char::is_control)
        || path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
    {
        return Err(validation(
            "code_change_check.working_directory",
            "must be a relative non-traversing path",
        ));
    }
    Ok(())
}

fn shell_argv(argv: &[String]) -> bool {
    let basename = Path::new(&argv[0])
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    matches!(
        basename,
        "sh" | "bash" | "dash" | "zsh" | "fish" | "cmd" | "powershell" | "pwsh"
    ) || argv.iter().any(|arg| {
        matches!(arg.as_str(), "-c" | "-Command" | "/c" | "/C")
            || arg
                .chars()
                .any(|character| matches!(character, ';' | '&' | '|' | '$' | '`' | '<' | '>'))
    })
}

fn tool_for_program(program: &str) -> Option<CodeChangeTool> {
    match Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
    {
        Some("git") => Some(CodeChangeTool::Git),
        Some("cargo") => Some(CodeChangeTool::Cargo),
        Some("uv") => Some(CodeChangeTool::Uv),
        Some("python") | Some("python3") => Some(CodeChangeTool::Python),
        _ => None,
    }
}

fn pyproject_has_pytest(root: &Path) -> bool {
    let path = root.join("pyproject.toml");
    let Ok(contents) = fs::read_to_string(path) else {
        return false;
    };
    toml::from_str::<toml::Value>(&contents)
        .ok()
        .is_some_and(|value| {
            value
                .get("tool")
                .and_then(|tool| tool.get("pytest"))
                .and_then(|pytest| pytest.get("ini_options"))
                .is_some()
        })
}

fn validation(field: &'static str, message: &'static str) -> AppError {
    AppError::Validation { field, message }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn tools(tools: &[CodeChangeTool]) -> BTreeSet<CodeChangeTool> {
        tools.iter().copied().collect()
    }

    #[test]
    fn validates_canonical_sha_and_internal_refs() {
        let sha = "a".repeat(40);
        assert_eq!(canonical_full_sha(&sha).unwrap(), sha);
        assert!(canonical_full_sha(&"A".repeat(40)).is_err());
        assert!(canonical_full_sha(&"a".repeat(39)).is_err());
        assert_eq!(
            candidate_ref("campaign-1", "proposal-1").unwrap(),
            "campaign/campaign-1/candidate/proposal-1"
        );
        assert_eq!(best_ref("campaign-1").unwrap(), "campaign/campaign-1/best");
    }

    #[test]
    fn owned_path_and_editor_json_reject_unsafe_inputs() {
        assert_eq!(
            owned_worktree_relative_path("campaign-1", "proposal-1").unwrap(),
            PathBuf::from(".pueue-agent/worktrees/campaign-1/proposal-1")
        );
        let root = tempdir().unwrap();
        let limits = CampaignLimits::default();
        let available = tools(&[CodeChangeTool::Cargo]);
        let valid = format!(
            r#"{{"schema_version":1,"status":"ready","summary":"ok","proposed_checks":[{{"source":"cargo","argv":["cargo","test","--all-targets","--","--test-threads=1"],"working_directory":"."}}]}}"#
        );
        assert!(parse_editor_output(valid.as_bytes(), root.path(), &limits, &available).is_ok());
        let absolute = format!(
            r#"{{"schema_version":1,"status":"ready","summary":"ok","proposed_checks":[{{"source":"cargo","argv":["cargo","test","--all-targets","--","--test-threads=1"],"working_directory":"/tmp"}}]}}"#
        );
        assert!(
            parse_editor_output(absolute.as_bytes(), root.path(), &limits, &available).is_err()
        );
        let unknown = valid.replace("\"summary\":\"ok\"", "\"summary\":\"ok\",\"extra\":true");
        assert!(parse_editor_output(unknown.as_bytes(), root.path(), &limits, &available).is_err());
        assert!(
            parse_editor_output(&vec![b'x'; 65 * 1024], root.path(), &limits, &available).is_err()
        );
    }

    #[test]
    fn checks_reject_shell_and_too_many_entries() {
        let root = tempdir().unwrap();
        let limits = CampaignLimits::default();
        let available = tools(&[CodeChangeTool::Cargo]);
        let shell = ProposedCheck {
            source: "shell".to_owned(),
            argv: vec!["sh".to_owned(), "-c".to_owned(), "cargo test".to_owned()],
            working_directory: ".".to_owned(),
        };
        assert!(validate_proposed_checks(&[shell], root.path(), &limits, &available).is_err());
        let checks = (0..=limits.max_code_change_checks)
            .map(|_| ProposedCheck {
                source: "cargo".to_owned(),
                argv: RUST_CHECK.iter().map(|arg| (*arg).to_owned()).collect(),
                working_directory: ".".to_owned(),
            })
            .collect::<Vec<_>>();
        assert!(validate_proposed_checks(&checks, root.path(), &limits, &available).is_err());
    }

    #[test]
    fn discovery_is_deterministic_and_uv_lock_selects_uv() {
        let root = tempdir().unwrap();
        fs::write(root.path().join("Cargo.toml"), "[package]\nname='x'\n").unwrap();
        fs::write(
            root.path().join("pyproject.toml"),
            "[tool.pytest.ini_options]\naddopts='-q'\n",
        )
        .unwrap();
        fs::write(root.path().join("uv.lock"), "version = 1\n").unwrap();
        let checks = discover_project_checks(
            root.path(),
            &tools(&[
                CodeChangeTool::Cargo,
                CodeChangeTool::Uv,
                CodeChangeTool::Python,
            ]),
        )
        .unwrap();
        assert_eq!(checks.len(), 2);
        assert_eq!(
            checks[0].argv,
            RUST_CHECK
                .iter()
                .map(|arg| (*arg).to_owned())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            checks[1].argv,
            UV_PYTEST_CHECK
                .iter()
                .map(|arg| (*arg).to_owned())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn protected_paths_and_diff_caps_are_rejected() {
        assert!(is_protected_path(Path::new(".git/index")));
        assert!(is_protected_path(Path::new(".pueue-agent/state")));
        assert!(!is_protected_path(Path::new("src/lib.rs")));
        assert!(validate_protected_paths(&[PathBuf::from(".git/index")]).is_err());
        let limits = CampaignLimits::default();
        assert!(validate_diff_limits(51, 0, &limits).is_err());
        assert!(validate_diff_limits(0, 500_001, &limits).is_err());
    }
}
