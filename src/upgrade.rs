use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    process::Command,
};

use crate::{cli::UpgradeArgs, AppError};

pub const SOURCE_ROOT_ENV: &str = "PUEUE_AGENT_SOURCE_ROOT";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeOptions {
    pub source: Option<PathBuf>,
    pub json: bool,
    pub branch: String,
    pub remote: String,
}

impl UpgradeOptions {
    pub fn from_args(args: UpgradeArgs) -> Self {
        Self {
            source: args.source,
            json: args.json,
            branch: "main".to_owned(),
            remote: "origin".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeReport {
    pub source: PathBuf,
    pub checkout: CheckoutState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckoutState {
    pub branch: String,
    pub upstream: String,
    pub head: String,
    pub upstream_head: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCommandOutput {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

pub trait GitCommandRunner {
    fn run(&self, source: &Path, args: &[&str]) -> Result<GitCommandOutput, AppError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessGitCommandRunner;

impl GitCommandRunner for ProcessGitCommandRunner {
    fn run(&self, source: &Path, args: &[&str]) -> Result<GitCommandOutput, AppError> {
        let output = Command::new("git")
            .current_dir(source)
            .args(args)
            .output()
            .map_err(|source| AppError::Io {
                operation: "run git for upgrade",
                source,
            })?;

        Ok(GitCommandOutput {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

pub fn resolve_source_root(
    explicit: Option<&Path>,
    current_exe: &Path,
    env_source: Option<&Path>,
) -> Result<PathBuf, AppError> {
    if let Some(source) = explicit {
        return validate_source_root(source);
    }

    if let Some(source) = source_root_from_release_binary(current_exe) {
        return validate_source_root(&source);
    }

    if let Some(source) = env_source {
        return validate_source_root(source);
    }

    Err(AppError::Message {
        message: format!(
            "upgrade source was not found; pass --source, run from target/release/pueue-agent, or set {SOURCE_ROOT_ENV}"
        ),
    })
}

pub fn validate_checkout(
    source: &Path,
    branch: &str,
    remote: &str,
) -> Result<CheckoutState, AppError> {
    validate_checkout_with(source, branch, remote, &ProcessGitCommandRunner)
}

pub fn validate_checkout_with<R: GitCommandRunner>(
    source: &Path,
    branch: &str,
    remote: &str,
    runner: &R,
) -> Result<CheckoutState, AppError> {
    let source = validate_source_root(source)?;
    let expected_upstream = format!("{remote}/{branch}");

    let status = run_git(runner, &source, &["status", "--porcelain"])?;
    if !status.stdout.trim().is_empty() {
        return Err(AppError::Message {
            message: "upgrade source checkout must be clean".to_owned(),
        });
    }

    let current_branch = git_stdout(runner, &source, &["branch", "--show-current"])?;
    if current_branch != branch {
        return Err(AppError::Message {
            message: format!(
                "upgrade source checkout must be on branch `{branch}`, found `{current_branch}`"
            ),
        });
    }

    let upstream_output = runner.run(
        &source,
        &["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"],
    )?;
    if !upstream_output.success {
        return Err(AppError::Message {
            message: format!("upgrade source checkout must track `{expected_upstream}`"),
        });
    }
    let upstream = upstream_output.stdout.trim().to_owned();
    if upstream != expected_upstream {
        return Err(AppError::Message {
            message: format!(
                "upgrade source checkout must track `{expected_upstream}`, found `{upstream}`"
            ),
        });
    }

    run_git(runner, &source, &["fetch", remote, branch])?;

    let head = git_stdout(runner, &source, &["rev-parse", "HEAD"])?;
    let upstream_head = git_stdout(runner, &source, &["rev-parse", &expected_upstream])?;
    let ancestry = runner.run(
        &source,
        &["merge-base", "--is-ancestor", "HEAD", &expected_upstream],
    )?;
    if !ancestry.success {
        return Err(AppError::Message {
            message: format!(
                "upgrade source checkout has diverged from `{expected_upstream}` and cannot fast-forward"
            ),
        });
    }

    Ok(CheckoutState {
        branch: current_branch,
        upstream,
        head,
        upstream_head,
    })
}

fn source_root_from_release_binary(current_exe: &Path) -> Option<PathBuf> {
    let executable = current_exe.canonicalize().ok()?;
    if executable.file_name()? != OsStr::new("pueue-agent") {
        return None;
    }

    let release = executable.parent()?;
    if release.file_name()? != OsStr::new("release") {
        return None;
    }

    let target = release.parent()?;
    if target.file_name()? != OsStr::new("target") {
        return None;
    }

    target.parent().map(Path::to_path_buf)
}

fn validate_source_root(source: &Path) -> Result<PathBuf, AppError> {
    let source = source.canonicalize().map_err(|source_error| AppError::Message {
        message: format!(
            "invalid upgrade source `{}`: {source_error}",
            source.display()
        ),
    })?;

    if !source.join(".git").exists() {
        return Err(invalid_source_root(&source));
    }

    let manifest_path = source.join("Cargo.toml");
    let manifest = std::fs::read_to_string(&manifest_path).map_err(|source_error| {
        AppError::Message {
            message: format!(
                "invalid upgrade source `{}`: {source_error}",
                source.display()
            ),
        }
    })?;
    let manifest = manifest.parse::<toml::Value>().map_err(|source_error| AppError::Message {
        message: format!(
            "invalid upgrade source `{}`: {source_error}",
            source.display()
        ),
    })?;

    if manifest
        .get("package")
        .and_then(toml::Value::as_table)
        .and_then(|package| package.get("name"))
        .and_then(toml::Value::as_str)
        != Some("pueue-agent")
    {
        return Err(invalid_source_root(&source));
    }

    Ok(source)
}

fn invalid_source_root(source: &Path) -> AppError {
    AppError::Message {
        message: format!(
            "invalid upgrade source `{}`: expected a pueue-agent Cargo checkout",
            source.display()
        ),
    }
}

fn run_git<R: GitCommandRunner>(
    runner: &R,
    source: &Path,
    args: &[&str],
) -> Result<GitCommandOutput, AppError> {
    let output = runner.run(source, args)?;
    if output.success {
        Ok(output)
    } else {
        Err(AppError::Message {
            message: format!(
                "upgrade git command failed (`git {}`): {}",
                args.join(" "),
                output.stderr.trim()
            ),
        })
    }
}

fn git_stdout<R: GitCommandRunner>(
    runner: &R,
    source: &Path,
    args: &[&str],
) -> Result<String, AppError> {
    Ok(run_git(runner, source, args)?.stdout.trim().to_owned())
}
