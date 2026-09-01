//! Agent-specific authorization for the marker-free native process launcher.
//!
//! `process` owns the executable descriptor and the release/exec/ack pipes.
//! This module owns the higher-level agent gate: the durable marker is created
//! from a pinned project-root descriptor before the child is released.  A
//! marker is never removed or replaced by this layer.

use std::{ffi::OsString, path::PathBuf};

use crate::{
    environment::{PrivateRunTemp, SanitizedEnvironment, VerifiedPrivateTemp},
    execution_policy::{
        ExecutableAnchor, PolicyViolation, PolicyViolationCode, PolicyViolationStage,
        VerifiedProjectRoot, VerifiedWorkingDirectory,
    },
    project_logs::{ensure_agent_log_dir, LogFileIdentity, ProjectRootLogReader},
    process::TerminalObservation,
    AppError,
};

#[cfg(unix)]
use crate::project_logs::{open_agent_log_gate, AgentGateDirectory};

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
    pub fn spawn(
        spec: NativeLaunchSpec,
        private_temp: &PrivateRunTemp,
    ) -> Result<NativeAgentChild, AppError> {
        Self::spawn_verified(spec, private_temp.verified_target()?)
    }

    #[cfg(unix)]
    pub(crate) fn spawn_verified(
        spec: NativeLaunchSpec,
        private_temp: VerifiedPrivateTemp,
    ) -> Result<NativeAgentChild, AppError> {
        let working_directory = verified_working_directory(&spec.project_root, spec.cwd.as_deref())?;
        let command_root = spec.project_root.try_clone()?;
        let reader = ProjectRootLogReader::from_verified(spec.project_root);

        // Ensure the fixed project-owned log directory before validating the
        // marker.  Both operations remain descriptor-relative; no ambient
        // path is joined or inspected here.
        ensure_agent_log_dir(&reader)?;
        // This is intentionally the sole open of the agent log.  Clone the
        // resulting descriptor for stdout/stderr rather than reopening by
        // path, so both streams remain pinned to the same checked inode.
        let (agent_log, gate_directory) = open_agent_log_gate(
            &reader,
            &spec.relative_log_path,
            &spec.relative_marker_path,
        )?;
        if gate_directory.inspect_marker()?.is_some() {
            return Err(native_gate_error(PolicyViolationStage::PostMarker));
        }
        let identity = *agent_log.identity();
        let stdout = agent_log.try_clone()?;
        let stderr = agent_log.into_file();

        let executable_anchor = spec.executable.clone();
        let verified = crate::process::spawn_verified_agent_command(
            crate::process::VerifiedCommandSpec {
                launcher: spec.launcher,
                executable: spec.executable,
                argv: spec.argv,
                working_directory: Some(working_directory),
                environment: spec.environment,
                process_group: crate::process::ProcessGroupRequirement::Required,
                start_suspended: true,
                project_root: Some(command_root),
                pueue_config: None,
                git_directories: None,
                child_io: crate::process::VerifiedChildIo::AgentLog {
                    stdout,
                    stderr,
                    identity,
                },
            },
            private_temp,
        );
        let verified = verified?;

        Ok(NativeAgentChild {
            verified,
            reader,
            executable_anchor,
            gate_directory,
            log_identity: identity,
            authorization_attempted: false,
            authorization_complete: false,
            termination_completed: false,
            termination_uncertain: false,
            #[cfg(test)]
            injected_termination_error: false,
        })
    }

    #[cfg(not(unix))]
    pub fn spawn(
        _spec: NativeLaunchSpec,
        _private_temp: &PrivateRunTemp,
    ) -> Result<NativeAgentChild, AppError> {
        Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::NativeGate,
        )
        .into())
    }

    #[cfg(not(unix))]
    pub(crate) fn spawn_verified(
        _spec: NativeLaunchSpec,
        _private_temp: VerifiedPrivateTemp,
    ) -> Result<NativeAgentChild, AppError> {
        Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::NativeGate,
        )
        .into())
    }
}

#[cfg(unix)]
fn verified_working_directory(
    root: &VerifiedProjectRoot,
    cwd: Option<&std::path::Path>,
) -> Result<VerifiedWorkingDirectory, AppError> {
    let cwd = cwd.ok_or_else(|| native_gate_error(PolicyViolationStage::NativeGate))?;
    if !cwd.is_absolute() {
        return Err(native_gate_error(PolicyViolationStage::NativeGate));
    }
    let relative = cwd
        .strip_prefix(&root.anchor.canonical_path)
        .map_err(|_| native_gate_error(PolicyViolationStage::NativeGate))?;
    VerifiedWorkingDirectory::open_descendant(root, relative).map_err(AppError::from)
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
    gate_directory: AgentGateDirectory,
    log_identity: LogFileIdentity,
    authorization_attempted: bool,
    authorization_complete: bool,
    termination_completed: bool,
    termination_uncertain: bool,
    #[cfg(test)]
    injected_termination_error: bool,
}

#[cfg(unix)]
impl NativeAgentChild {
    pub fn id(&self) -> i64 {
        self.verified.id()
    }

    pub(crate) fn terminal_observed(&mut self) -> Result<TerminalObservation, AppError> {
        self.verified.terminal_observed()
    }

    pub(crate) fn ownership_lost(&mut self) -> Result<bool, AppError> {
        Ok(matches!(
            self.verified.terminal_observed()?,
            TerminalObservation::OwnershipLost
        ))
    }

    pub(crate) async fn reap_observed_terminal(
        &mut self,
    ) -> Result<std::process::ExitStatus, AppError> {
        self.verified.reap_observed_terminal().await
    }

    /// Publish the durable marker, release the blocked child, and wait for
    /// both the close-on-exec proof and exact release acknowledgement.
    ///
    /// This operation is one-shot.  In particular, a directory-sync error
    /// from `create_gate_marker` can occur after publication, so errors are
    /// classified by re-inspecting the marker before terminating the group.
    pub async fn authorize_marker(&mut self) -> Result<(), AppError> {
        if self.authorization_attempted {
            self.terminate_after_authorization_error().await;
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
                self.gate_directory
                    .revalidate_current(&self.reader, self.log_identity)
            });
        if let Err(error) = revalidation {
            let classified = match self.gate_directory.inspect_marker() {
                Ok(None) => pre_marker_revalidation_error(error),
                Ok(Some(_)) | Err(_) => native_gate_error(PolicyViolationStage::PostMarker),
            };
            self.terminate_after_authorization_error().await;
            return Err(classified);
        }
        let marker_identity = match self.gate_directory.publish_marker() {
            Ok(identity) => identity,
            Err(error) => {
                let classified = marker_failure_error(
                    error,
                    self.gate_directory.inspect_marker(),
                );
                self.terminate_after_authorization_error().await;
                return Err(classified);
            }
        };

        let post_publish = self
            .reader
            .revalidate_root_path_identity()
            .and_then(|_| {
                self.executable_anchor
                    .verify_identity()
                    .map(|_| ())
                    .map_err(AppError::from)
            })
            .and_then(|_| {
                self.gate_directory.revalidate_published(
                    &self.reader,
                    self.log_identity,
                    marker_identity,
                )
            });
        if post_publish.is_err() {
            self.terminate_after_authorization_error().await;
            return Err(native_gate_error(PolicyViolationStage::PostMarker));
        }

        if let Err(error) = self.verified.release() {
            self.terminate_after_authorization_error().await;
            return Err(native_gate_error_with_fallback(error, PolicyViolationStage::PostMarker));
        }
        if let Err(error) = self.verified.confirm_exec().await {
            self.terminate_after_authorization_error().await;
            return Err(native_gate_error_with_fallback(error, PolicyViolationStage::PostMarker));
        }
        if let Err(error) = self.verified.wait_for_release_ack().await {
            self.terminate_after_authorization_error().await;
            return Err(native_gate_error_with_fallback(error, PolicyViolationStage::PostMarker));
        }
        self.authorization_complete = true;
        Ok(())
    }

    pub async fn wait(&mut self) -> Result<std::process::ExitStatus, AppError> {
        self.verified.wait().await
    }

    pub(crate) async fn wait_retaining_unknown(
        &mut self,
    ) -> Result<Option<std::process::ExitStatus>, AppError> {
        loop {
            match self.terminal_observed()? {
                TerminalObservation::Running => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                TerminalObservation::Terminal => {
                    return self.reap_observed_terminal().await.map(Some);
                }
                TerminalObservation::OwnershipLost => return Ok(None),
            }
        }
    }

    pub async fn terminate(&mut self) -> Result<(), AppError> {
        #[cfg(test)]
        if std::mem::take(&mut self.injected_termination_error) {
            self.termination_completed = false;
            self.termination_uncertain = true;
            return Err(AppError::Io {
                operation: "observe verified child without reaping",
                source: std::io::Error::from_raw_os_error(libc::EIO),
            });
        }
        let result = crate::process::terminate_process_group(&mut self.verified).await;
        match &result {
            Ok(()) => {
                self.termination_completed = true;
                self.termination_uncertain = false;
            }
            Err(_) => {
                self.termination_completed = false;
                self.termination_uncertain = true;
            }
        }
        result
    }

    async fn terminate_after_authorization_error(&mut self) {
        if self.terminate().await.is_err() {
            self.termination_uncertain = true;
        }
    }

    pub(crate) fn termination_uncertain(&self) -> bool {
        self.termination_uncertain
    }

    pub(crate) fn termination_completed(&self) -> bool {
        self.termination_completed
    }

    #[cfg(test)]
    pub(crate) fn inject_termination_observation_error(&mut self) {
        self.injected_termination_error = true;
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
        // When absence is proven, preserve the original failure. In
        // particular, I/O/runtime failures remain eligible for retry while
        // descriptor/path policy failures retain their policy classification.
        Ok(None) => creation_error,
    }
}

#[cfg(all(test, unix))]
fn test_native_child(
    test_name: &str,
) -> (tempfile::TempDir, NativeAgentChild) {
    use crate::{
        execution_policy::ProjectRootAnchor,
        project_logs::open_agent_log_gate,
    };

    let temporary = tempfile::tempdir().expect("temporary native-child root");
    let root = std::fs::canonicalize(temporary.path()).expect("canonical native-child root");
    let anchor = ProjectRootAnchor::resolve(&root).expect("native-child root anchor");
    let reader = ProjectRootLogReader::from_verified(
        anchor.verify_identity().expect("verified native-child root"),
    );
    ensure_agent_log_dir(&reader).expect("native-child log directory");
    let log_path = PathBuf::from(".pueue-agent/logs/agent-handle.log");
    let marker_path = PathBuf::from(".pueue-agent/logs/agent-handle.authorized");
    let (agent_log, gate_directory) =
        open_agent_log_gate(&reader, &log_path, &marker_path).expect("native-child gate");
    let log_identity = *agent_log.identity();
    let executable_path = std::fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
    let executable_anchor = ExecutableAnchor::from_absolute(&executable_path, &[])
        .expect("native-child executable anchor");
    let verified = crate::process::test_running_verified_child(test_name)
        .expect("running verified child");
    let child = NativeAgentChild {
        verified,
        reader,
        executable_anchor,
        gate_directory,
        log_identity,
        authorization_attempted: false,
        authorization_complete: true,
        termination_completed: false,
        termination_uncertain: false,
        injected_termination_error: false,
    };
    (temporary, child)
}

#[cfg(all(test, unix))]
pub(crate) fn test_native_child_with_termination_error(
    test_name: &str,
) -> (tempfile::TempDir, NativeAgentChild) {
    let (temporary, mut child) = test_native_child(test_name);
    child.inject_termination_observation_error();
    (temporary, child)
}

#[cfg(all(test, unix))]
pub(crate) fn test_native_child_with_group_signal_error(
    test_name: &str,
) -> (tempfile::TempDir, NativeAgentChild) {
    let (temporary, mut child) = test_native_child(test_name);
    child.verified.inject_group_signal_error();
    (temporary, child)
}

#[cfg(all(test, unix))]
pub(crate) fn test_native_child_with_ownership_loss_before_reap(
    test_name: &str,
) -> (tempfile::TempDir, NativeAgentChild) {
    let (temporary, mut child) = test_native_child(test_name);
    child.verified.inject_ownership_loss_before_reap();
    (temporary, child)
}

#[cfg(not(unix))]
pub struct NativeAgentChild;

#[cfg(not(unix))]
impl NativeAgentChild {
    pub fn id(&self) -> i64 {
        0
    }

    pub(crate) fn terminal_observed(&mut self) -> Result<TerminalObservation, AppError> {
        Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::Dispatched,
        )
        .into())
    }

    pub(crate) fn ownership_lost(&mut self) -> Result<bool, AppError> {
        Ok(false)
    }

    pub(crate) async fn reap_observed_terminal(
        &mut self,
    ) -> Result<std::process::ExitStatus, AppError> {
        Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::Dispatched,
        )
        .into())
    }

    pub async fn wait(&mut self) -> Result<std::process::ExitStatus, AppError> {
        Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::Dispatched,
        )
        .into())
    }

    pub(crate) async fn wait_retaining_unknown(
        &mut self,
    ) -> Result<Option<std::process::ExitStatus>, AppError> {
        self.wait().await.map(Some)
    }

    pub async fn terminate(&mut self) -> Result<(), AppError> {
        Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::Dispatched,
        )
        .into())
    }

    pub(crate) fn termination_uncertain(&self) -> bool {
        false
    }

    pub(crate) fn termination_completed(&self) -> bool {
        false
    }
}

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
            open_agent_log_gate, set_test_marker_failure, MarkerIoFailure,
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
        assert!(matches!(
            marker_failure_error(
                AppError::Runtime {
                    operation: "create gate marker fixture",
                },
                inspection,
            ),
            AppError::Runtime {
                operation: "create gate marker fixture"
            }
        ));
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

    #[test]
    fn absent_marker_preserves_the_original_retryable_creation_failure() {
        let creation_error = AppError::Io {
            operation: "write gate marker",
            source: std::io::Error::new(std::io::ErrorKind::Other, "fixture failure"),
        };
        assert!(matches!(
            marker_failure_error(creation_error, Ok(None)),
            AppError::Io {
                operation: "write gate marker",
                ..
            }
        ));
    }

    #[test]
    fn retained_gate_directory_publishes_only_in_the_opened_log_generation() {
        use std::os::unix::fs::PermissionsExt;

        let _guard = test_guard();
        let (temporary, log_reader) = prepare();
        let log = Path::new(".pueue-agent/logs/fixture.log");
        let marker = marker_path();
        let (opened_log, gate) =
            open_agent_log_gate(&log_reader, log, &marker).expect("open bound gate");
        let log_identity = *opened_log.identity();
        gate.revalidate_current(&log_reader, log_identity)
            .expect("initial gate generation");

        let current = temporary.path().join(".pueue-agent/logs");
        let retired = temporary.path().join(".pueue-agent/retired-logs");
        fs::rename(&current, &retired).expect("retire bound log directory");
        fs::create_dir(&current).expect("create replacement log directory");
        fs::set_permissions(&current, fs::Permissions::from_mode(0o700))
            .expect("set replacement mode");
        fs::hard_link(retired.join("fixture.log"), current.join("fixture.log"))
            .expect("preserve log identity in replacement directory");

        let marker_identity = gate.publish_marker().expect("publish through retained parent");
        assert!(retired.join("fixture.authorized").exists());
        assert!(!current.join("fixture.authorized").exists());
        assert!(gate
            .revalidate_published(&log_reader, log_identity, marker_identity)
            .is_err());
    }

    #[test]
    fn gate_log_and_marker_must_share_the_fixed_secure_parent() {
        let _guard = test_guard();
        let (_temporary, log_reader) = prepare();
        assert!(open_agent_log_gate(
            &log_reader,
            Path::new(".pueue-agent/logs/fixture.log"),
            Path::new(".pueue-agent/other/fixture.authorized"),
        )
        .is_err());
        assert!(open_agent_log_gate(
            &log_reader,
            Path::new(".pueue-agent/logs/fixture.log"),
            Path::new(".pueue-agent/logs/fixture.log"),
        )
        .is_err());
    }

    #[test]
    #[ignore = "internal native launcher child fixture"]
    fn native_child_hold_subprocess() {
        std::thread::sleep(std::time::Duration::from_secs(30));
    }

    async fn marker_failure_child(
        failure: MarkerIoFailure,
    ) -> (tempfile::TempDir, ProjectRootLogReader, NativeAgentChild) {
        let (temporary, log_reader) = prepare();
        let log = Path::new(".pueue-agent/logs/fixture.log");
        let marker = marker_path();
        let (agent_log, gate_directory) =
            open_agent_log_gate(&log_reader, log, &marker).expect("open bound gate");
        let log_identity = *agent_log.identity();
        let executable_path = std::fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        let executable_anchor = ExecutableAnchor::from_absolute(&executable_path, &[])
            .expect("unit-test executable anchor");
        let verified = crate::process::test_running_verified_child(
            "native_launcher::tests::native_child_hold_subprocess",
        )
        .expect("running group fixture");
        let child_reader = reader(temporary.path());
        set_test_marker_failure(Some(failure));
        (
            temporary,
            log_reader,
            NativeAgentChild {
                verified,
                reader: child_reader,
                executable_anchor,
                gate_directory,
                log_identity,
                authorization_attempted: false,
                authorization_complete: false,
                termination_completed: false,
                termination_uncertain: false,
                injected_termination_error: false,
            },
        )
    }

    #[tokio::test]
    async fn write_and_fsync_failures_remain_retryable_and_reap_the_group() {
        let _guard = test_guard();
        for failure in [MarkerIoFailure::Write, MarkerIoFailure::FileSync] {
            let (temporary, log_reader, mut child) = marker_failure_child(failure).await;
            assert!(matches!(child.authorize_marker().await, Err(AppError::Io { .. })));
            assert!(inspect_gate_marker(&log_reader, &marker_path())
                .expect("inspect absent marker")
                .is_none());
            assert!(
                tokio::time::timeout(std::time::Duration::from_secs(2), child.wait())
                    .await
                    .is_ok(),
                "prepublish failure did not reap the group"
            );
            assert!(!temporary.path().join("target-started").exists());
        }
    }

    #[tokio::test]
    async fn directory_sync_uncertainty_is_post_marker_and_reaps_the_group() {
        let _guard = test_guard();
        let (_temporary, log_reader, mut child) =
            marker_failure_child(MarkerIoFailure::DirectorySync).await;
        assert_eq!(
            policy_stage(child.authorize_marker().await.unwrap_err()),
            Some(PolicyViolationStage::PostMarker)
        );
        assert!(inspect_gate_marker(&log_reader, &marker_path())
            .expect("inspect retained marker")
            .is_some());
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), child.wait())
                .await
                .is_ok(),
            "post-marker uncertainty did not reap the group"
        );
        assert!(child.termination_completed());
    }

    #[tokio::test]
    async fn post_publish_failure_retains_classification_when_termination_is_uncertain() {
        let _guard = test_guard();
        let (_temporary, log_reader, mut child) =
            marker_failure_child(MarkerIoFailure::DirectorySync).await;
        child.inject_termination_observation_error();
        assert_eq!(
            policy_stage(child.authorize_marker().await.unwrap_err()),
            Some(PolicyViolationStage::PostMarker)
        );
        assert!(child.termination_uncertain());
        assert!(!child.termination_completed());
        assert!(inspect_gate_marker(&log_reader, &marker_path())
            .expect("inspect retained marker")
            .is_some());
        child.injected_termination_error = false;
        child.terminate().await.unwrap();
        assert!(child.termination_completed());
    }
}
