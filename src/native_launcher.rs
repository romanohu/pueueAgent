//! Agent-specific authorization for the marker-free native process launcher.
//!
//! `process` owns the executable descriptor and the release/exec/ack pipes.
//! This module owns the higher-level agent gate: the durable marker is created
//! from a pinned project-root descriptor before the child is released.  A
//! marker is never removed or replaced by this layer.

use std::{ffi::OsString, path::PathBuf};

use crate::{
    environment::SanitizedEnvironment,
    execution_policy::{
        ExecutableAnchor, PolicyViolation, PolicyViolationCode, PolicyViolationStage,
        VerifiedProjectRoot,
    },
    project_logs::{
        create_gate_marker, ensure_agent_log_dir, inspect_agent_log_dir,
        inspect_existing_agent_log, inspect_gate_marker, open_agent_log, LogFileIdentity,
        ProjectRootLogReader,
    },
    AppError,
};

/// All paths in this specification are already service-resolved except for
/// the two project-relative log paths.  Keeping those paths relative is
/// important: the log reader is the authority that validates and opens them.
///
/// This type deliberately has no `Debug` implementation.  The environment
/// and argv can contain task data and must not accidentally enter diagnostics.
#[cfg(unix)]
pub struct NativeLaunchSpec {
    pub launcher: ExecutableAnchor,
    pub executable: ExecutableAnchor,
    pub argv: Vec<OsString>,
    pub cwd: Option<PathBuf>,
    pub environment: SanitizedEnvironment,
    pub project_root: VerifiedProjectRoot,
    pub relative_log_path: PathBuf,
    pub relative_marker_path: PathBuf,
}

/// Non-Unix builds retain the public shape so callers can compile uniformly,
/// while `spawn` below fails closed before touching any filesystem path.
#[cfg(not(unix))]
pub struct NativeLaunchSpec {
    pub launcher: ExecutableAnchor,
    pub executable: ExecutableAnchor,
    pub argv: Vec<OsString>,
    pub cwd: Option<PathBuf>,
    pub environment: SanitizedEnvironment,
    pub project_root: VerifiedProjectRoot,
    pub relative_log_path: PathBuf,
    pub relative_marker_path: PathBuf,
}

/// Marker/release adapter for an agent process.
pub struct NativeLauncher;

impl NativeLauncher {
    pub fn new() -> Self {
        Self
    }

    /// Spawn a target in the marker-free blocked state.
    ///
    /// No marker is created here.  The caller must invoke
    /// [`NativeAgentChild::authorize_marker`] after all run admission work has
    /// succeeded.
    #[cfg(unix)]
    pub fn spawn(spec: NativeLaunchSpec) -> Result<NativeAgentChild, AppError> {
        let command_root = spec.project_root.try_clone()?;
        let reader = ProjectRootLogReader::from_verified(spec.project_root);

        // Ensure the fixed project-owned log directory before validating the
        // marker.  Both operations remain descriptor-relative; no ambient
        // path is joined or inspected here.
        let log_directory_identity = ensure_agent_log_dir(&reader)?;
        if inspect_gate_marker(&reader, &spec.relative_marker_path)?.is_some() {
            return Err(native_gate_error(PolicyViolationStage::PostMarker));
        }
        // This is intentionally the sole open of the agent log.  Clone the
        // resulting descriptor for stdout/stderr rather than reopening by
        // path, so both streams remain pinned to the same checked inode.
        let agent_log = open_agent_log(&reader, &spec.relative_log_path)?;
        let identity = *agent_log.identity();
        let stdout = agent_log.try_clone()?;
        let stderr = agent_log.into_file();

        let executable_anchor = spec.executable.clone();
        let verified = crate::process::spawn_verified_command(
            crate::process::VerifiedCommandSpec {
                launcher: spec.launcher,
                executable: spec.executable,
                argv: spec.argv,
                cwd: spec.cwd,
                environment: spec.environment,
                process_group: crate::process::ProcessGroupRequirement::Required,
                start_suspended: true,
                project_root: Some(command_root),
                pueue_config: None,
                child_io: crate::process::VerifiedChildIo::AgentLog {
                    stdout,
                    stderr,
                    identity,
                },
            },
        )?;

        Ok(NativeAgentChild {
            verified,
            reader,
            executable_anchor,
            relative_log_path: spec.relative_log_path,
            log_directory_identity,
            log_identity: identity,
            relative_marker_path: spec.relative_marker_path,
            authorization_attempted: false,
            authorization_complete: false,
        })
    }

    #[cfg(not(unix))]
    pub fn spawn(_spec: NativeLaunchSpec) -> Result<NativeAgentChild, AppError> {
        Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::NativeGate,
        )
        .into())
    }
}

impl Default for NativeLauncher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(unix)]
pub struct NativeAgentChild {
    verified: crate::process::VerifiedChild,
    reader: ProjectRootLogReader,
    executable_anchor: ExecutableAnchor,
    relative_log_path: PathBuf,
    log_directory_identity: LogFileIdentity,
    log_identity: LogFileIdentity,
    relative_marker_path: PathBuf,
    authorization_attempted: bool,
    authorization_complete: bool,
}

#[cfg(unix)]
impl NativeAgentChild {
    /// Publish the durable marker, release the blocked child, and wait for
    /// both the close-on-exec proof and exact release acknowledgement.
    ///
    /// This operation is one-shot.  In particular, a directory-sync error
    /// from `create_gate_marker` can occur after publication, so errors are
    /// classified by re-inspecting the marker before terminating the group.
    pub async fn authorize_marker(&mut self) -> Result<(), AppError> {
        if self.authorization_attempted {
            crate::process::terminate_process_group(&mut self.verified).await;
            return Err(native_gate_error(PolicyViolationStage::PostMarker));
        }
        self.authorization_attempted = true;

        let revalidation = self
            .reader
            .revalidate_root_path_identity()
            .and_then(|_| {
                self.executable_anchor
                    .verify_identity()
                    .map(|_| ())
                    .map_err(AppError::from)
            })
            .and_then(|_| {
                let current = inspect_agent_log_dir(&self.reader)?;
                if current != self.log_directory_identity {
                    return Err(log_replaced_error());
                }
                Ok(())
            })
            .and_then(|_| {
                let current = inspect_existing_agent_log(&self.reader, &self.relative_log_path)?;
                if current != self.log_identity {
                    return Err(log_replaced_error());
                }
                Ok(())
            });
        if let Err(error) = revalidation {
            crate::process::terminate_process_group(&mut self.verified).await;
            return Err(pre_marker_revalidation_error(error));
        }

        let created = create_gate_marker(&self.reader, &self.relative_marker_path);
        match created {
            Ok(_) => {}
            Err(error) => {
                let classified = marker_failure_error(
                    error,
                    inspect_gate_marker(&self.reader, &self.relative_marker_path),
                );
                crate::process::terminate_process_group(&mut self.verified).await;
                return Err(classified);
            }
        }

        if let Err(error) = self.verified.release() {
            crate::process::terminate_process_group(&mut self.verified).await;
            return Err(native_gate_error_with_fallback(error, PolicyViolationStage::PostMarker));
        }
        if let Err(error) = self.verified.confirm_exec().await {
            crate::process::terminate_process_group(&mut self.verified).await;
            return Err(native_gate_error_with_fallback(error, PolicyViolationStage::PostMarker));
        }
        if let Err(error) = self.verified.wait_for_release_ack().await {
            crate::process::terminate_process_group(&mut self.verified).await;
            return Err(native_gate_error_with_fallback(error, PolicyViolationStage::PostMarker));
        }
        self.authorization_complete = true;
        Ok(())
    }

    pub async fn wait(&mut self) -> Result<std::process::ExitStatus, AppError> {
        self.verified.wait().await
    }

    pub async fn terminate(&mut self) {
        crate::process::terminate_process_group(&mut self.verified).await;
    }

    pub fn is_authorized(&self) -> bool {
        self.authorization_complete
    }
}

#[cfg(unix)]
fn native_gate_error(stage: PolicyViolationStage) -> AppError {
    PolicyViolation::new(PolicyViolationCode::NativeGateFailed, stage).into()
}

#[cfg(unix)]
fn log_replaced_error() -> AppError {
    use crate::execution_policy::{LogUnsafeReason, PolicyViolationDetail};

    PolicyViolation::with_detail(
        PolicyViolationCode::LogUnsafe,
        PolicyViolationStage::RunBoundPreMarker,
        PolicyViolationDetail::LogUnsafe(LogUnsafeReason::RootChanged),
    )
    .into()
}

#[cfg(unix)]
fn pre_marker_revalidation_error(error: AppError) -> AppError {
    match error {
        AppError::PolicyViolation { violation } => PolicyViolation::with_detail(
            violation.code,
            PolicyViolationStage::RunBoundPreMarker,
            violation.detail,
        )
        .into(),
        error => error,
    }
}

#[cfg(unix)]
fn native_gate_error_with_fallback(error: AppError, stage: PolicyViolationStage) -> AppError {
    match error {
        AppError::PolicyViolation { violation }
            if violation.stage == PolicyViolationStage::PostMarker => {
                AppError::PolicyViolation { violation }
            }
        _ => native_gate_error(stage),
    }
}

#[cfg(unix)]
fn marker_failure_error(
    creation_error: AppError,
    inspection: Result<Option<crate::project_logs::GateMarkerIdentity>, AppError>,
) -> AppError {
    match inspection {
        Ok(Some(_)) | Err(_) => {
            // Publication is proven or cannot be disproven. Keep any marker
            // untouched and report only the bounded uncertainty stage.
            native_gate_error(PolicyViolationStage::PostMarker)
        }
        Ok(None) => match creation_error {
            // A descriptor/path policy failure is safe to preserve only when
            // re-inspection proves that no marker was published.
            error @ AppError::PolicyViolation { .. } => error,
            _ => native_gate_error(PolicyViolationStage::RunBoundPreMarker),
        },
    }
}

#[cfg(not(unix))]
pub struct NativeAgentChild;

#[cfg(all(test, unix))]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::{Mutex, OnceLock},
    };

    use tempfile::tempdir;

    use super::*;
    use crate::{
        execution_policy::ProjectRootAnchor,
        project_logs::{
            create_gate_marker, ensure_agent_log_dir, inspect_gate_marker,
            set_test_marker_failure, MarkerIoFailure,
        },
    };

    static MARKER_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn reader(root: &Path) -> ProjectRootLogReader {
        let root = std::fs::canonicalize(root).expect("canonical fixture root");
        let anchor = ProjectRootAnchor::resolve(&root).expect("fixture root anchor");
        ProjectRootLogReader::from_verified(anchor.verify_identity().expect("fixture root"))
    }

    fn marker_path() -> PathBuf {
        PathBuf::from(".pueue-agent/logs/fixture.authorized")
    }

    fn prepare() -> (tempfile::TempDir, ProjectRootLogReader) {
        let temporary = tempdir().expect("temporary root");
        let log_reader = reader(temporary.path());
        ensure_agent_log_dir(&log_reader).expect("agent log directory");
        (temporary, log_reader)
    }

    fn test_guard() -> std::sync::MutexGuard<'static, ()> {
        let guard = MARKER_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        set_test_marker_failure(None);
        guard
    }

    fn policy_stage(error: AppError) -> Option<PolicyViolationStage> {
        match error {
            AppError::PolicyViolation { violation } => Some(violation.stage),
            _ => None,
        }
    }

    #[test]
    fn marker_creation_is_exclusive_and_durable() {
        let _guard = test_guard();
        let (_temporary, log_reader) = prepare();
        let marker = marker_path();
        create_gate_marker(&log_reader, &marker).expect("first marker");
        assert!(inspect_gate_marker(&log_reader, &marker)
            .expect("inspect marker")
            .is_some());
        assert!(create_gate_marker(&log_reader, &marker).is_err());
    }

    #[test]
    fn pre_publish_marker_failure_removes_private_inode() {
        let _guard = test_guard();
        let (temporary, log_reader) = prepare();
        let marker = marker_path();
        set_test_marker_failure(Some(MarkerIoFailure::Write));
        assert!(create_gate_marker(&log_reader, &marker).is_err());
        set_test_marker_failure(None);
        let inspection = inspect_gate_marker(&log_reader, &marker);
        assert!(inspection.as_ref().expect("inspect marker").is_none());
        assert_eq!(
            policy_stage(marker_failure_error(
                AppError::Runtime {
                    operation: "create gate marker fixture",
                },
                inspection,
            )),
            Some(PolicyViolationStage::RunBoundPreMarker)
        );
        let log_directory = temporary.path().join(".pueue-agent/logs");
        assert_eq!(fs::read_dir(log_directory).expect("read log directory").count(), 0);
    }

    #[test]
    fn directory_sync_failure_is_post_publish_uncertainty() {
        let _guard = test_guard();
        let (_temporary, log_reader) = prepare();
        let marker = marker_path();
        set_test_marker_failure(Some(MarkerIoFailure::DirectorySync));
        assert!(create_gate_marker(&log_reader, &marker).is_err());
        set_test_marker_failure(None);
        let inspection = inspect_gate_marker(&log_reader, &marker);
        assert!(inspection.as_ref().expect("inspect marker").is_some());
        assert_eq!(
            policy_stage(marker_failure_error(
                AppError::Runtime {
                    operation: "create gate marker fixture",
                },
                inspection,
            )),
            Some(PolicyViolationStage::PostMarker)
        );
    }

    #[test]
    fn marker_inspection_failure_is_conservatively_post_marker() {
        let inspection = Err(AppError::Io {
            operation: "inspect marker fixture",
            source: std::io::Error::new(std::io::ErrorKind::Other, "fixture failure"),
        });
        assert_eq!(
            policy_stage(marker_failure_error(
                AppError::Runtime {
                    operation: "create gate marker fixture",
                },
                inspection,
            )),
            Some(PolicyViolationStage::PostMarker)
        );
    }

    #[test]
    fn absent_marker_preserves_the_original_unsafe_policy_failure() {
        let creation_error: AppError = PolicyViolation::new(
            PolicyViolationCode::LogUnsafe,
            PolicyViolationStage::NativeGate,
        )
        .into();
        let classified = marker_failure_error(creation_error, Ok(None));
        match classified {
            AppError::PolicyViolation { violation } => {
                assert_eq!(violation.code, PolicyViolationCode::LogUnsafe);
                assert_eq!(violation.stage, PolicyViolationStage::NativeGate);
            }
            _ => panic!("unsafe marker failure lost its policy classification"),
        }
    }
}
