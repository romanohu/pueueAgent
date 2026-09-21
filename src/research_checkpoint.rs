use std::{
    ffi::OsString,
    path::{Component, Path},
};

use serde::{Deserialize, Serialize};

use crate::{
    models::ProposalKind,
    proposals::{self, ProposalInput},
    pueue::validate_add_argv,
    research_protocol::CheckpointRequest,
    AppError,
};

const SUPPORTED_INTERPRETERS: &[&str] = &["python", "python3"];
const MAX_CHECKPOINT_PATH_BYTES: usize = 4 * 1024;

/// The only command delta admitted for a checkpoint successor.
///
/// `index` is the inserted option index for a pair delta and the complete
/// `--flag=path` token index for an equals delta.  Keeping the exact index and
/// form makes restart validation deterministic rather than rediscovering a
/// potentially different matching token.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CheckpointArgvDeltaForm {
    Pair,
    Equals,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointArgvDelta {
    form: CheckpointArgvDeltaForm,
    index: usize,
    flag: String,
}

impl CheckpointArgvDelta {
    pub(crate) const fn form(&self) -> CheckpointArgvDeltaForm {
        self.form
    }

    pub(crate) const fn index(&self) -> usize {
        self.index
    }

    pub(crate) fn flag(&self) -> &str {
        &self.flag
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedCheckpointCommand {
    source_argv: Vec<String>,
    request_argv: Vec<String>,
    source_working_directory: String,
    request_working_directory: String,
    checkpoint_path: String,
    delta: CheckpointArgvDelta,
}

impl ValidatedCheckpointCommand {
    pub(crate) fn source_argv(&self) -> &[String] {
        &self.source_argv
    }

    pub(crate) fn request_argv(&self) -> &[String] {
        &self.request_argv
    }

    pub(crate) fn source_working_directory(&self) -> &str {
        &self.source_working_directory
    }

    pub(crate) fn request_working_directory(&self) -> &str {
        &self.request_working_directory
    }

    pub(crate) fn checkpoint_path(&self) -> &str {
        &self.checkpoint_path
    }

    pub(crate) fn delta(&self) -> &CheckpointArgvDelta {
        &self.delta
    }

    /// Revalidate the exact source/request pair and replace only the retained
    /// checkpoint path value.  The retained path is an already-verified
    /// supervisor path; this helper only guarantees whole-token substitution.
    pub(crate) fn reapply_retained_path(
        &self,
        retained_path: &str,
    ) -> Result<Vec<String>, AppError> {
        let request = CheckpointRequest {
            path: self.checkpoint_path.clone(),
            argv: self.request_argv.clone(),
            working_directory: self.request_working_directory.clone(),
            support_evidence_refs: Vec::new(),
        };
        let revalidated = validate_checkpoint_command(
            &self.source_argv,
            &self.source_working_directory,
            &request,
        )?;
        if revalidated.delta != self.delta {
            return Err(checkpoint_validation_error(
                "checkpoint argv delta changed during revalidation",
            ));
        }
        validate_retained_path(retained_path)?;

        let mut argv = revalidated.request_argv;
        match revalidated.delta.form {
            CheckpointArgvDeltaForm::Pair => {
                let path_index =
                    revalidated.delta.index.checked_add(1).ok_or_else(|| {
                        checkpoint_validation_error("checkpoint argv index overflow")
                    })?;
                if argv.get(revalidated.delta.index).map(String::as_str)
                    != Some(revalidated.delta.flag.as_str())
                    || argv.get(path_index).map(String::as_str)
                        != Some(revalidated.checkpoint_path.as_str())
                {
                    return Err(checkpoint_validation_error(
                        "checkpoint argv delta no longer identifies the original path",
                    ));
                }
                argv[path_index] = retained_path.to_owned();
            }
            CheckpointArgvDeltaForm::Equals => {
                let expected =
                    format!("{}={}", revalidated.delta.flag, revalidated.checkpoint_path);
                if argv.get(revalidated.delta.index).map(String::as_str) != Some(expected.as_str())
                {
                    return Err(checkpoint_validation_error(
                        "checkpoint argv delta no longer identifies the original path",
                    ));
                }
                argv[revalidated.delta.index] =
                    format!("{}={retained_path}", revalidated.delta.flag);
            }
        }
        validate_command_argv(&argv)?;
        Ok(argv)
    }
}

/// Validate the authoritative source command and the requested successor
/// command.  The request may differ only by one checkpoint-loading option and
/// its exact path value; source learning arguments retain byte/order identity.
pub(crate) fn validate_checkpoint_command(
    source_argv: &[String],
    source_working_directory: &str,
    request: &CheckpointRequest,
) -> Result<ValidatedCheckpointCommand, AppError> {
    validate_checkpoint_path(&request.path)?;

    let source = validate_command_proposal(source_argv, source_working_directory)?;
    let requested = validate_command_proposal(&request.argv, &request.working_directory)?;
    if source.working_directory() != requested.working_directory() {
        return Err(checkpoint_validation_error(
            "source and requested working directories must match",
        ));
    }

    validate_trainer_command_shape(source_argv)?;
    validate_trainer_command_shape(&request.argv)?;
    validate_command_argv(source_argv)?;
    validate_command_argv(&request.argv)?;
    let delta = validate_checkpoint_argv_delta(source_argv, &request.argv, &request.path)?;

    Ok(ValidatedCheckpointCommand {
        source_argv: source.argv().to_owned(),
        request_argv: requested.argv().to_owned(),
        source_working_directory: source.working_directory().to_owned(),
        request_working_directory: requested.working_directory().to_owned(),
        checkpoint_path: request.path.clone(),
        delta,
    })
}

/// Validate only the exact argv delta.  This is useful when the surrounding
/// proposal/cwd policy has already been checked by the caller.
pub(crate) fn validate_checkpoint_argv_delta(
    source_argv: &[String],
    request_argv: &[String],
    checkpoint_path: &str,
) -> Result<CheckpointArgvDelta, AppError> {
    validate_checkpoint_path(checkpoint_path)?;
    if source_argv.is_empty() || request_argv.is_empty() {
        return Err(checkpoint_validation_error("argv must be non-empty"));
    }

    let mut candidates = Vec::new();
    let mut standalone_path_count = 0usize;
    let mut equals_path_count = 0usize;
    for (index, token) in request_argv.iter().enumerate() {
        if token == checkpoint_path {
            standalone_path_count = standalone_path_count.saturating_add(1);
            if index > 0 && is_checkpoint_flag(&request_argv[index - 1]) {
                candidates.push(CheckpointArgvDelta {
                    form: CheckpointArgvDeltaForm::Pair,
                    index: index - 1,
                    flag: request_argv[index - 1].clone(),
                });
            }
        }
        if let Some((flag, value)) = token.split_once('=') {
            if value == checkpoint_path && is_checkpoint_flag(flag) {
                equals_path_count = equals_path_count.saturating_add(1);
                candidates.push(CheckpointArgvDelta {
                    form: CheckpointArgvDeltaForm::Equals,
                    index,
                    flag: flag.to_owned(),
                });
            }
        }
    }

    if candidates.len() != 1 {
        return Err(checkpoint_validation_error(
            "argv must contain exactly one checkpoint delta",
        ));
    }
    let delta = candidates.remove(0);
    match delta.form {
        CheckpointArgvDeltaForm::Pair if standalone_path_count != 1 || equals_path_count != 0 => {
            return Err(checkpoint_validation_error(
                "checkpoint path must occur exactly once as a pair value",
            ));
        }
        CheckpointArgvDeltaForm::Equals if standalone_path_count != 0 || equals_path_count != 1 => {
            return Err(checkpoint_validation_error(
                "checkpoint path must occur exactly once as an equals value",
            ));
        }
        _ => {}
    }

    if source_argv
        .iter()
        .any(|token| token == delta.flag() || token.starts_with(&format!("{}=", delta.flag())))
    {
        return Err(checkpoint_validation_error(
            "checkpoint flag already exists in source argv",
        ));
    }

    let mut stripped = request_argv.to_vec();
    match delta.form {
        CheckpointArgvDeltaForm::Pair => {
            let end = delta
                .index
                .checked_add(2)
                .ok_or_else(|| checkpoint_validation_error("checkpoint argv index overflow"))?;
            if end > stripped.len() {
                return Err(checkpoint_validation_error(
                    "checkpoint argv delta index is invalid",
                ));
            }
            stripped.drain(delta.index..end);
        }
        CheckpointArgvDeltaForm::Equals => {
            if delta.index >= stripped.len() {
                return Err(checkpoint_validation_error(
                    "checkpoint argv delta index is invalid",
                ));
            }
            stripped.remove(delta.index);
        }
    }
    if stripped != source_argv {
        return Err(checkpoint_validation_error(
            "request argv changes source tokens outside the checkpoint delta",
        ));
    }
    Ok(delta)
}

fn validate_command_proposal(
    argv: &[String],
    working_directory: &str,
) -> Result<proposals::ValidatedProposal, AppError> {
    proposals::validate(
        ProposalInput {
            kind: ProposalKind::Experiment,
            hypothesis: String::new(),
            source_experiment_id: None,
            argv: argv.to_vec(),
            working_directory: working_directory.to_owned(),
            expected_evidence: Vec::new(),
        },
        "research-checkpoint",
    )
}

fn validate_command_argv(argv: &[String]) -> Result<(), AppError> {
    let mut add_args = Vec::with_capacity(argv.len() + 1);
    add_args.push(OsString::from("--"));
    add_args.extend(argv.iter().map(OsString::from));
    validate_add_argv(&add_args)
}

fn validate_trainer_command_shape(argv: &[String]) -> Result<(), AppError> {
    if argv.is_empty() {
        return Err(checkpoint_validation_error(
            "trainer argv must be non-empty",
        ));
    }
    if argv.iter().any(|token| has_shell_payload(token)) {
        return Err(checkpoint_validation_error(
            "shell payloads are unsupported for checkpoint continuation",
        ));
    }

    let program = argv[0].as_str();
    if SUPPORTED_INTERPRETERS.contains(&program) {
        if argv.len() < 2 || !is_interpreter_entrypoint(&argv[1]) {
            return Err(checkpoint_validation_error(
                "interpreter command must have a direct relative trainer entrypoint",
            ));
        }
        return Ok(());
    }
    if is_shell_or_wrapper(program)
        || Path::new(program)
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(is_shell_or_wrapper)
        || !is_direct_project_entrypoint(program)
    {
        return Err(checkpoint_validation_error(
            "trainer command must use a direct project-relative entrypoint",
        ));
    }
    Ok(())
}

fn validate_checkpoint_path(path: &str) -> Result<(), AppError> {
    if path.is_empty()
        || path.len() > MAX_CHECKPOINT_PATH_BYTES
        || path.chars().any(char::is_control)
        || has_shell_payload(path)
    {
        return Err(checkpoint_validation_error(
            "checkpoint path must be a bounded relative path",
        ));
    }
    let path = Path::new(path);
    if path.is_absolute() {
        return Err(checkpoint_validation_error(
            "checkpoint path must be relative",
        ));
    }
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(component) => {
                components.push(component.to_string_lossy().into_owned())
            }
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(checkpoint_validation_error(
                    "checkpoint path must use normal relative components",
                ));
            }
        }
    }
    if components.is_empty() || components.join("/") != path.to_string_lossy() {
        return Err(checkpoint_validation_error(
            "checkpoint path must use normal relative components",
        ));
    }
    Ok(())
}

fn validate_retained_path(path: &str) -> Result<(), AppError> {
    if path.is_empty() || path.chars().any(char::is_control) || has_shell_payload(path) {
        return Err(checkpoint_validation_error(
            "retained checkpoint path is invalid",
        ));
    }
    Ok(())
}

fn is_checkpoint_flag(value: &str) -> bool {
    value.starts_with("--")
        && !value.starts_with("---")
        && value.len() > 2
        && !value.contains('=')
        && !has_shell_payload(value)
        && value[2..]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn is_interpreter_entrypoint(value: &str) -> bool {
    if value.is_empty() || value.starts_with('-') || has_shell_payload(value) {
        return false;
    }
    let path = Path::new(value);
    if path.is_absolute() {
        return false;
    }
    path.components()
        .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

fn is_direct_project_entrypoint(value: &str) -> bool {
    if !value.starts_with("./") && !value.contains('/') {
        return false;
    }
    if !is_interpreter_entrypoint(value) {
        return false;
    }
    let mut component_index = 0;
    for component in Path::new(value).components() {
        if matches!(component, Component::CurDir) && component_index != 0 {
            return false;
        }
        component_index += 1;
    }
    true
}

fn is_shell_or_wrapper(value: &str) -> bool {
    matches!(
        value,
        "sh" | "bash"
            | "dash"
            | "zsh"
            | "fish"
            | "csh"
            | "ksh"
            | "tcsh"
            | "cmd"
            | "cmd.exe"
            | "powershell"
            | "pwsh"
            | "env"
            | "sudo"
            | "doas"
            | "nice"
            | "timeout"
            | "xargs"
            | "exec"
            | "time"
            | "poetry"
            | "uv"
            | "pipenv"
            | "conda"
            | "docker"
            | "podman"
            | "singularity"
            | "srun"
            | "mpirun"
            | "mpiexec"
            | "torchrun"
    )
}

fn has_shell_payload(value: &str) -> bool {
    value.bytes().any(|byte| {
        matches!(
            byte,
            b'\\' | b'\'' | b'"' | b'`' | b'$' | b';' | b'|' | b'&' | b'<' | b'>'
        )
    })
}

fn checkpoint_validation_error(message: &'static str) -> AppError {
    AppError::Validation {
        field: "checkpoint.argv",
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(path: &str, argv: &[&str], working_directory: &str) -> CheckpointRequest {
        CheckpointRequest {
            path: path.to_owned(),
            argv: argv.iter().map(|value| (*value).to_owned()).collect(),
            working_directory: working_directory.to_owned(),
            support_evidence_refs: vec!["loader:entrypoint".to_owned()],
        }
    }

    #[test]
    fn accepts_pair_delta_and_replaces_only_exact_path_value() {
        let source = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "--lr".to_owned(),
            "0.001".to_owned(),
        ];
        let request = request(
            "checkpoints/step-20.json",
            &[
                "python",
                "train.py",
                "--resume",
                "checkpoints/step-20.json",
                "--lr",
                "0.001",
            ],
            ".",
        );

        let command = validate_checkpoint_command(&source, ".", &request).unwrap();
        assert_eq!(command.delta().form(), CheckpointArgvDeltaForm::Pair);
        assert_eq!(command.delta().index(), 2);
        assert_eq!(
            command
                .reapply_retained_path("/private/checkpoint")
                .unwrap(),
            vec![
                "python",
                "train.py",
                "--resume",
                "/private/checkpoint",
                "--lr",
                "0.001",
            ]
        );
    }

    #[test]
    fn accepts_equals_delta_and_preserves_learning_arguments() {
        let source = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "--lr".to_owned(),
            "0.001".to_owned(),
            "--steps".to_owned(),
            "20".to_owned(),
        ];
        let request = request(
            "checkpoints/step-20.json",
            &[
                "python",
                "train.py",
                "--lr",
                "0.001",
                "--steps",
                "20",
                "--resume=checkpoints/step-20.json",
            ],
            ".",
        );

        let command = validate_checkpoint_command(&source, ".", &request).unwrap();
        assert_eq!(command.delta().form(), CheckpointArgvDeltaForm::Equals);
        assert_eq!(command.delta().index(), 6);
        assert_eq!(
            command
                .reapply_retained_path("/private/checkpoint")
                .unwrap(),
            vec![
                "python",
                "train.py",
                "--lr",
                "0.001",
                "--steps",
                "20",
                "--resume=/private/checkpoint",
            ]
        );
        assert_eq!(command.source_argv(), source.as_slice());
    }

    #[test]
    fn rejects_changed_learning_specification_or_order() {
        let source = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "--lr".to_owned(),
            "0.001".to_owned(),
            "--steps".to_owned(),
            "20".to_owned(),
        ];
        for argv in [
            vec![
                "python",
                "train.py",
                "--lr",
                "0.01",
                "--steps",
                "20",
                "--resume",
                "checkpoint.json",
            ],
            vec![
                "python",
                "train.py",
                "--steps",
                "20",
                "--lr",
                "0.001",
                "--resume",
                "checkpoint.json",
            ],
            vec![
                "python",
                "other.py",
                "--lr",
                "0.001",
                "--steps",
                "20",
                "--resume",
                "checkpoint.json",
            ],
        ] {
            let request = request("checkpoint.json", &argv, ".");
            assert!(validate_checkpoint_command(&source, ".", &request).is_err());
        }
    }

    #[test]
    fn rejects_ambiguous_or_existing_checkpoint_delta() {
        let source = vec!["python".to_owned(), "train.py".to_owned()];
        for argv in [
            vec![
                "python",
                "train.py",
                "--resume",
                "checkpoint.json",
                "--resume",
                "checkpoint.json",
            ],
            vec![
                "python",
                "train.py",
                "--resume=checkpoint.json",
                "--resume",
                "checkpoint.json",
            ],
        ] {
            let request = request("checkpoint.json", &argv, ".");
            assert!(validate_checkpoint_command(&source, ".", &request).is_err());
        }

        let source_with_flag = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "--resume".to_owned(),
            "old.json".to_owned(),
        ];
        let request = request(
            "checkpoint.json",
            &[
                "python",
                "train.py",
                "--resume",
                "old.json",
                "--resume",
                "checkpoint.json",
            ],
            ".",
        );
        assert!(validate_checkpoint_command(&source_with_flag, ".", &request).is_err());
    }

    #[test]
    fn rejects_substrings_shell_payloads_and_non_long_flags() {
        let source = vec!["python".to_owned(), "train.py".to_owned()];
        for (path, argv) in [
            (
                "checkpoint.json",
                vec!["python", "train.py", "--resume", "checkpoint.json.bak"],
            ),
            (
                "checkpoint.json",
                vec!["python", "train.py", "-r", "checkpoint.json"],
            ),
            (
                "checkpoint.json",
                vec!["python", "train.py", "--resume/path", "checkpoint.json"],
            ),
            (
                "checkpoint.json",
                vec!["python", "train.py", "--resume=prefix-checkpoint.json"],
            ),
            (
                "checkpoint.json",
                vec![
                    "python",
                    "train.py; rm -rf /",
                    "--resume",
                    "checkpoint.json",
                ],
            ),
        ] {
            let request = request(path, &argv, ".");
            assert!(validate_checkpoint_command(&source, ".", &request).is_err());
        }
    }

    #[test]
    fn rejects_changed_or_non_normalized_working_directory() {
        let source = vec!["python".to_owned(), "train.py".to_owned()];
        let changed = request(
            "checkpoint.json",
            &["python", "train.py", "--resume", "checkpoint.json"],
            "nested",
        );
        assert!(validate_checkpoint_command(&source, ".", &changed).is_err());

        for working_directory in ["../other", "/tmp", "nested/./other", ""] {
            let request = request(
                "checkpoint.json",
                &["python", "train.py", "--resume", "checkpoint.json"],
                working_directory,
            );
            assert!(validate_checkpoint_command(&source, ".", &request).is_err());
        }
    }

    #[test]
    fn supports_only_direct_entrypoint_or_small_interpreter_shape() {
        let direct = vec![
            "./train.py".to_owned(),
            "--steps".to_owned(),
            "20".to_owned(),
        ];
        let direct_request = request(
            "checkpoint.json",
            &["./train.py", "--steps", "20", "--resume", "checkpoint.json"],
            ".",
        );
        assert!(validate_checkpoint_command(&direct, ".", &direct_request).is_ok());

        let nested_direct = vec!["trainer/train.py".to_owned()];
        let nested_request = request(
            "checkpoint.json",
            &["trainer/train.py", "--resume=checkpoint.json"],
            ".",
        );
        assert!(validate_checkpoint_command(&nested_direct, ".", &nested_request).is_ok());

        let interpreter = vec!["python3".to_owned(), "train.py".to_owned()];
        let interpreter_request = request(
            "checkpoint.json",
            &["python3", "train.py", "--resume=checkpoint.json"],
            ".",
        );
        assert!(validate_checkpoint_command(&interpreter, ".", &interpreter_request).is_ok());

        for source_argv in [
            vec!["sh".to_owned(), "-c".to_owned(), "train".to_owned()],
            vec!["./sh".to_owned(), "-c".to_owned(), "train".to_owned()],
            vec!["env".to_owned(), "python".to_owned(), "train.py".to_owned()],
            vec!["python".to_owned(), "-m".to_owned(), "trainer".to_owned()],
            vec!["python".to_owned(), "-u".to_owned(), "train.py".to_owned()],
            vec!["/usr/bin/python".to_owned(), "train.py".to_owned()],
            vec!["train.py".to_owned()],
            vec!["../train.py".to_owned()],
        ] {
            let request = request(
                "checkpoint.json",
                &source_argv
                    .iter()
                    .map(String::as_str)
                    .chain(["--resume", "checkpoint.json"])
                    .collect::<Vec<_>>(),
                ".",
            );
            assert!(validate_checkpoint_command(&source_argv, ".", &request).is_err());
        }
    }
}
