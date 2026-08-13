//! The bounded control codec used by the native launch helper.
//!
//! This module intentionally contains no process creation.  The supervisor
//! and the hidden helper exchange one complete, length-delimited frame over a
//! pipe.  Keeping the codec separate from launch code makes it possible to
//! validate the untrusted byte stream before any target descriptor is used.

use std::{
    ffi::{OsStr, OsString},
    fmt,
    io::{Read, Write},
    ops::BitOr,
    sync::{mpsc, Mutex, MutexGuard, OnceLock},
    time::{Duration, Instant},
};

use crate::execution_policy::ExecutableIdentity;

#[cfg(unix)]
use std::{
    io,
    mem,
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd},
        unix::ffi::{OsStrExt, OsStringExt},
    },
    ptr,
};

#[cfg(unix)]
use std::process::{Child, Command as StdCommand, ExitStatus, Stdio};

#[cfg(unix)]
pub type RawFd = std::os::fd::RawFd;
#[cfg(not(unix))]
pub type RawFd = i32;

/// Descriptor numbers are part of the helper ABI.  They are deliberately
/// fixed so a helper cannot select an arbitrary inherited descriptor.
pub const CONTROL_FD: RawFd = 3;
pub const RELEASE_FD: RawFd = 4;
pub const EXEC_STATUS_FD: RawFd = 5;
pub const TARGET_FD: RawFd = 6;
pub const PROJECT_ROOT_FD: RawFd = 7;
pub const AGENT_LOG_FD: RawFd = 8;
pub const PUEUE_CONFIG_FD: RawFd = 9;
pub const RELEASE_ACK_FD: RawFd = 10;
/// Compatibility alias for the final acknowledgement descriptor.
pub const ACK_FD: RawFd = RELEASE_ACK_FD;

pub const RELEASE_ACK: &[u8] = b"released\n";
pub const MAX_FRAME_SIZE: usize = 1024 * 1024;
pub const MAX_ARGV: usize = 256;
pub const MAX_ENV: usize = 128;
pub const MAX_FIELD_SIZE: usize = 64 * 1024;
const BOOTSTRAP_IO_TIMEOUT: Duration = Duration::from_secs(5);

const HEADER_SIZE: usize = 12;
const KNOWN_FLAGS: u16 = FLAG_PROJECT_ROOT | FLAG_AGENT_LOG | FLAG_PUEUE_CONFIG | FLAG_PROCESS_GROUP;
const FLAG_PROJECT_ROOT: u16 = 1 << 0;
const FLAG_AGENT_LOG: u16 = 1 << 1;
const FLAG_PUEUE_CONFIG: u16 = 1 << 2;
const FLAG_PROCESS_GROUP: u16 = 1 << 3;

const FIELD_ARGV: u8 = 1;
const FIELD_ENV: u8 = 2;
const FIELD_TARGET_IDENTITY: u8 = 3;
const FIELD_CWD: u8 = 4;
const FIELD_PROJECT_ROOT_IDENTITY: u8 = 5;
const FIELD_AGENT_LOG_IDENTITY: u8 = 6;
const FIELD_PUEUE_CONFIG_IDENTITY: u8 = 7;
const IDENTITY_SIZE: usize = 8 + 8 + 4 + 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FixedFdContract {
    pub control: RawFd,
    pub release: RawFd,
    pub exec_status: RawFd,
    pub target: RawFd,
    pub project_root: RawFd,
    pub agent_log: RawFd,
    pub pueue_config: RawFd,
    pub release_ack: RawFd,
}

impl Default for FixedFdContract {
    fn default() -> Self {
        Self {
            control: CONTROL_FD,
            release: RELEASE_FD,
            exec_status: EXEC_STATUS_FD,
            target: TARGET_FD,
            project_root: PROJECT_ROOT_FD,
            agent_log: AGENT_LOG_FD,
            pueue_config: PUEUE_CONFIG_FD,
            release_ack: RELEASE_ACK_FD,
        }
    }
}

impl FixedFdContract {
    pub const fn standard() -> Self {
        Self {
            control: CONTROL_FD,
            release: RELEASE_FD,
            exec_status: EXEC_STATUS_FD,
            target: TARGET_FD,
            project_root: PROJECT_ROOT_FD,
            agent_log: AGENT_LOG_FD,
            pueue_config: PUEUE_CONFIG_FD,
            release_ack: RELEASE_ACK_FD,
        }
    }

    pub const fn is_standard(self) -> bool {
        self.control == CONTROL_FD
            && self.release == RELEASE_FD
            && self.exec_status == EXEC_STATUS_FD
            && self.target == TARGET_FD
            && self.project_root == PROJECT_ROOT_FD
            && self.agent_log == AGENT_LOG_FD
            && self.pueue_config == PUEUE_CONFIG_FD
            && self.release_ack == RELEASE_ACK_FD
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum LaunchMode {
    Agent = 1,
    Pueue = 2,
}

impl TryFrom<u8> for LaunchMode {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Agent),
            2 => Ok(Self::Pueue),
            _ => Err(CodecError::UnknownMode),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LaunchFlags(u16);

impl LaunchFlags {
    pub const NONE: Self = Self(0);
    pub const PROJECT_ROOT: Self = Self(FLAG_PROJECT_ROOT);
    pub const AGENT_LOG: Self = Self(FLAG_AGENT_LOG);
    pub const PUEUE_CONFIG: Self = Self(FLAG_PUEUE_CONFIG);
    pub const PROCESS_GROUP: Self = Self(FLAG_PROCESS_GROUP);

    pub const fn bits(self) -> u16 { self.0 }
    pub const fn contains(self, other: Self) -> bool { self.0 & other.0 == other.0 }
    pub const fn union(self, other: Self) -> Self { Self(self.0 | other.0) }
    fn from_bits(bits: u16) -> Result<Self, CodecError> {
        if bits & !KNOWN_FLAGS != 0 { Err(CodecError::UnknownFlags) } else { Ok(Self(bits)) }
    }
}

impl BitOr for LaunchFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ControlFrame {
    pub mode: LaunchMode,
    pub flags: LaunchFlags,
    pub argv: Vec<OsString>,
    pub environment: Vec<(OsString, OsString)>,
    pub cwd: Option<OsString>,
    pub target_identity: ExecutableIdentity,
    pub project_root_identity: Option<ExecutableIdentity>,
    pub agent_log_identity: Option<ExecutableIdentity>,
    pub pueue_config_identity: Option<ExecutableIdentity>,
}

impl fmt::Debug for ControlFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlFrame")
            .field("mode", &self.mode)
            .field("flags", &self.flags)
            .field("argv_count", &self.argv.len())
            .field("environment_count", &self.environment.len())
            .field("cwd_present", &self.cwd.is_some())
            .field("target_identity_present", &true)
            .field("project_root_identity_present", &self.project_root_identity.is_some())
            .field("agent_log_identity_present", &self.agent_log_identity.is_some())
            .field("pueue_config_identity_present", &self.pueue_config_identity.is_some())
            .finish()
    }
}

impl ControlFrame {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> { encode_control_frame(self) }
    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> { decode_control_frame(bytes) }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CodecError {
    Truncated,
    InvalidMagic,
    UnsupportedVersion,
    UnknownMode,
    UnknownFlags,
    FrameTooLarge,
    LengthOverflow,
    FieldTooLarge,
    TooManyArguments,
    TooManyEnvironmentEntries,
    DuplicateEnvironmentName,
    InvalidEnvironmentName,
    NulByte,
    InvalidField,
    UnknownField(u8),
    DuplicateField(u8),
    MissingField(u8),
    UnexpectedField(u8),
    NonIncreasingFieldOrder,
    TrailingBytes,
    InvalidIdentity,
    MissingProcessGroup,
    TooManyFields,
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        use CodecError::*;
        let text = match self {
            Truncated => "truncated control frame",
            InvalidMagic => "invalid control frame magic",
            UnsupportedVersion => "unsupported control frame version",
            UnknownMode => "unknown launch mode",
            UnknownFlags => "unknown launch flags",
            FrameTooLarge => "control frame is too large",
            LengthOverflow => "control frame length overflows",
            FieldTooLarge => "control frame field is too large",
            TooManyArguments => "too many argv entries",
            TooManyEnvironmentEntries => "too many environment entries",
            DuplicateEnvironmentName => "duplicate environment name",
            InvalidEnvironmentName => "invalid environment name",
            NulByte => "NUL byte in control frame field",
            InvalidField => "invalid control frame field",
            UnknownField(_) => "unknown control frame field",
            DuplicateField(_) => "duplicate control frame field",
            MissingField(_) => "missing control frame field",
            UnexpectedField(_) => "unexpected control frame field",
            NonIncreasingFieldOrder => "control frame fields are not in canonical order",
            TrailingBytes => "trailing control frame bytes",
            InvalidIdentity => "invalid identity field",
            MissingProcessGroup => "process-group flag is required",
            TooManyFields => "too many control frame fields",
        };
        formatter.write_str(text)
    }
}

impl std::error::Error for CodecError {}

#[cfg(unix)]
#[derive(Debug)]
pub enum BootstrapError {
    Io(io::Error),
    Codec(CodecError),
    EmptyPacket,
    TruncatedPacket,
    MissingRights,
    UnexpectedAncillary,
    WrongRightCount,
    IdentityMismatch,
    GateClosed,
    DescriptorNotCloseOnExec,
    BootstrapCorrupt,
    AliasedPipeRoles,
}

#[cfg(unix)]
impl fmt::Display for BootstrapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(_) => formatter.write_str("bootstrap I/O failed"),
            Self::Codec(error) => write!(formatter, "bootstrap codec rejected input: {error}"),
            Self::EmptyPacket => formatter.write_str("bootstrap packet was empty"),
            Self::TruncatedPacket => formatter.write_str("bootstrap packet was truncated"),
            Self::MissingRights => formatter.write_str("bootstrap packet had no descriptor rights"),
            Self::UnexpectedAncillary => formatter.write_str("bootstrap packet had unexpected ancillary data"),
            Self::WrongRightCount => formatter.write_str("bootstrap packet had the wrong descriptor count"),
            Self::IdentityMismatch => formatter.write_str("bootstrap descriptor identity mismatch"),
            Self::GateClosed => formatter.write_str("bootstrap release gate was already closed"),
            Self::DescriptorNotCloseOnExec => formatter.write_str("bootstrap descriptor was not close-on-exec"),
            Self::BootstrapCorrupt => formatter.write_str("bootstrap fixed descriptor map was rejected"),
            Self::AliasedPipeRoles => formatter.write_str("bootstrap pipe roles were aliased"),
        }
    }
}

#[cfg(unix)]
impl std::error::Error for BootstrapError {}

/// Errors exposed by the parent-side hidden-helper bootstrap adapter. Their
/// text is bounded and never contains paths, argv, environment, or OS error
/// strings.
#[cfg(unix)]
#[derive(Debug)]
pub enum ProcessLaunchError {
    LauncherRejected,
    Spawn,
    Bootstrap(BootstrapError),
    ReadinessRejected,
    Io,
}

#[cfg(unix)]
impl fmt::Display for ProcessLaunchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::LauncherRejected => "trusted launcher identity was rejected",
            Self::Spawn => "trusted launcher could not be spawned",
            Self::Bootstrap(_) => "bootstrap protocol was rejected",
            Self::ReadinessRejected => "helper readiness record was rejected",
            Self::Io => "helper readiness I/O failed",
        })
    }
}

#[cfg(unix)]
impl std::error::Error for ProcessLaunchError {}

/// The helper's fixed-size readiness record. It contains no target or
/// credential data and is sent only after fixed-FD validation succeeds.
#[cfg(unix)]
pub const HELPER_READY_RECORD: [u8; 8] = *b"PAER\x01\x00\x00\x00";
#[cfg(unix)]
const HELPER_FAILED_RECORD: [u8; 8] = *b"PAER\x01\x01\x00\x00";
#[cfg(unix)]
const HELPER_READY_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(unix)]
const HELPER_CLEANUP_INLINE_TIMEOUT: Duration = Duration::from_millis(250);
#[cfg(unix)]
const HELPER_REAPER_POLL_INTERVAL: Duration = Duration::from_millis(5);

#[cfg(unix)]
static HELPER_REAPER: OnceLock<mpsc::Sender<Child>> = OnceLock::new();

/// A successfully bootstrapped hidden helper. The helper currently exits
/// after readiness; retaining this handle lets callers reap that lifecycle.
#[cfg(unix)]
pub struct ValidatedHelper {
    child: Option<Child>,
    reaper: mpsc::Sender<Child>,
}

#[cfg(unix)]
impl ValidatedHelper {
    pub fn id(&self) -> u32 {
        self.child.as_ref().expect("validated helper child is present").id()
    }

    pub fn try_wait(&mut self) -> Result<Option<ExitStatus>, ProcessLaunchError> {
        self.child
            .as_mut()
            .ok_or(ProcessLaunchError::Io)?
            .try_wait()
            .map_err(|_| ProcessLaunchError::Io)
    }

    pub fn wait(&mut self) -> Result<ExitStatus, ProcessLaunchError> {
        self.child
            .as_mut()
            .ok_or(ProcessLaunchError::Io)?
            .wait()
            .map_err(|_| ProcessLaunchError::Io)
    }
}

#[cfg(unix)]
impl Drop for ValidatedHelper {
    fn drop(&mut self) {
        if let Some(child) = self.child.take() {
            cleanup_helper(child, &self.reaper);
        }
    }
}

#[cfg(unix)]
impl From<io::Error> for BootstrapError {
    fn from(value: io::Error) -> Self { Self::Io(value) }
}

#[cfg(unix)]
impl From<CodecError> for BootstrapError {
    fn from(value: CodecError) -> Self { Self::Codec(value) }
}

#[cfg(unix)]
pub(crate) struct BootstrapPacket {
    pub frame: ControlFrame,
    /// Rights are ordered by `bootstrap_right_slots(frame)`.  They remain
    /// owned here until the helper has moved every descriptor above the fixed
    /// ABI range.
    pub rights: Vec<OwnedFd>,
}

#[cfg(unix)]
pub(crate) struct InstalledBootstrap {
    pub frame: ControlFrame,
}

#[cfg(unix)]
static PROCESS_LAUNCH_LOCK: Mutex<()> = Mutex::new(());

#[cfg(unix)]
pub(crate) struct ProcessLaunchGuard(MutexGuard<'static, ()>);

/// Serialize descriptor creation that cannot atomically request CLOEXEC with
/// every supervisor spawn adapter. Trusted supervisor code must hold this
/// guard from before it creates inheritable process resources until after
/// spawn returns. Third-party code running inside the trusted supervisor is
/// outside the threat model.
#[cfg(unix)]
pub(crate) fn process_launch_guard() -> Result<ProcessLaunchGuard, BootstrapError> {
    PROCESS_LAUNCH_LOCK
        .lock()
        .map(ProcessLaunchGuard)
        .map_err(|_| BootstrapError::BootstrapCorrupt)
}

/// Consume the fixed bootstrap protocol in the hidden helper. This function
/// is intentionally the only operation performed by `internal-launch`: no
/// project paths, configuration, or ordinary command state are opened here.
#[cfg(unix)]
pub fn run_internal_launch() -> Result<(), BootstrapError> {
    // Keep a duplicate of stdin solely for the bounded failure record. The
    // fixed-map installer owns and may close fd 0 on an error path.
    let failure_channel = unsafe { libc::fcntl(0, libc::F_DUPFD_CLOEXEC, RELEASE_ACK_FD + 1) };
    let result = receive_and_install_bootstrap(libc::STDIN_FILENO);
    match result {
        Ok(installed) => {
            // Reading the validated mode here makes the readiness dependency
            // explicit: this branch is reachable only after the full frame
            // and its mode-specific descriptor map have been installed.
            let _validated_mode = installed.frame.mode;
            // Control fd 3 is the bootstrap socket peer. A readiness record is
            // sent only after every fixed descriptor was mapped and validated.
            write_fixed_record(CONTROL_FD, &HELPER_READY_RECORD)?;
            Ok(())
        }
        Err(error) => {
            if failure_channel >= 0 {
                let _ = write_fixed_record(failure_channel, &HELPER_FAILED_RECORD);
                unsafe { libc::close(failure_channel); }
            }
            Err(error)
        }
    }
}

#[cfg(not(unix))]
pub fn run_internal_launch() -> Result<(), ()> { Err(()) }

#[cfg(unix)]
fn write_fixed_record(raw: RawFd, record: &[u8; 8]) -> Result<(), BootstrapError> {
    let mut stream = std::mem::ManuallyDrop::new(unsafe {
        std::os::unix::net::UnixStream::from_raw_fd(raw)
    });
    let deadline = ProtocolDeadline::new(HELPER_READY_TIMEOUT)?;
    write_all_before(&mut stream, record, &deadline)?;
    // Close only the write half after the bounded record. This gives the
    // parent an exact EOF delimiter without making readiness depend on the
    // helper process scheduler reaching exit first.
    if unsafe { libc::shutdown(raw, libc::SHUT_WR) } < 0 {
        return Err(BootstrapError::Io(io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(unix)]
fn build_helper_command(
    launcher: &std::path::Path,
    child_input: Stdio,
) -> StdCommand {
    let mut command = StdCommand::new(launcher);
    command
        .arg("internal-launch")
        .env_clear()
        .stdin(child_input)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

#[cfg(unix)]
fn revalidate_launcher_before_spawn(
    launcher: &crate::execution_policy::ExecutableAnchor,
) -> Result<(), ProcessLaunchError> {
    launcher
        .verify_identity()
        .map(|_| ())
        .map_err(|_| ProcessLaunchError::LauncherRejected)
}

#[cfg(unix)]
fn helper_reaper() -> Result<mpsc::Sender<Child>, ProcessLaunchError> {
    if let Some(sender) = HELPER_REAPER.get() {
        return Ok(sender.clone());
    }

    let (sender, receiver) = mpsc::channel();
    std::thread::Builder::new()
        .name("pueue-agent-helper-reaper".to_owned())
        .spawn(move || helper_reaper_loop(receiver))
        .map_err(|_| ProcessLaunchError::Spawn)?;
    if HELPER_REAPER.set(sender.clone()).is_err() {
        return HELPER_REAPER
            .get()
            .cloned()
            .ok_or(ProcessLaunchError::Spawn);
    }
    Ok(sender)
}

#[cfg(unix)]
fn helper_reaper_loop(receiver: mpsc::Receiver<Child>) {
    let mut children: Vec<Child> = Vec::new();
    loop {
        if children.is_empty() {
            match receiver.recv() {
                Ok(child) => children.push(child),
                Err(_) => return,
            }
        } else {
            match receiver.recv_timeout(HELPER_REAPER_POLL_INTERVAL) {
                Ok(child) => children.push(child),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // Keep ownership until all already-submitted children have
                    // been reaped, even during process teardown.
                }
            }
        }
        while let Ok(child) = receiver.try_recv() {
            children.push(child);
        }

        let mut index = 0;
        while index < children.len() {
            // A failed status probe must not skip termination. Both operations
            // are nonblocking syscalls on supported Unix platforms.
            let _ = children[index].kill();
            match children[index].try_wait() {
                Ok(Some(_)) => {
                    children.swap_remove(index);
                }
                Ok(None) | Err(_) => index += 1,
            }
        }
    }
}

#[cfg(unix)]
fn cleanup_helper(mut child: Child, reaper: &mpsc::Sender<Child>) {
    let deadline = Instant::now()
        .checked_add(HELPER_CLEANUP_INLINE_TIMEOUT)
        .unwrap_or_else(Instant::now);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) | Err(_) => {
                // Always attempt termination, including after a failed probe.
                let _ = child.kill();
            }
        }
        if Instant::now() >= deadline {
            // The receiver is owned by a process-lifetime thread initialized
            // before any helper spawn. Sending is unbounded and nonblocking;
            // the thread retains Child ownership and polls waitpid until reap.
            let _ = reaper.send(child);
            return;
        }
        std::thread::sleep(HELPER_REAPER_POLL_INTERVAL);
    }
}

#[cfg(unix)]
fn prepare_bootstrap_rights(
    guard: &ProcessLaunchGuard,
    rights: &[OwnedFd],
) -> Result<(), BootstrapError> {
    let _ = &guard.0;
    for right in rights {
        set_close_on_exec(right.as_raw_fd())?;
        prove_close_on_exec(right.as_raw_fd())?;
    }
    Ok(())
}

/// Start the verified absolute-path helper with only the hidden subcommand in
/// argv, send its bounded descriptor bootstrap frame, and await its exact
/// readiness record. This is deliberately a bootstrap-only adapter: it does
/// not create or execute the target. Descriptor-bound supervisor execution
/// and OS-specific launch are added by the later native-launch layer.
#[cfg(unix)]
pub fn spawn_validated_helper(
    launcher: &crate::execution_policy::ExecutableAnchor,
    frame: ControlFrame,
    rights: Vec<OwnedFd>,
) -> Result<ValidatedHelper, ProcessLaunchError> {
    // This check is immediately before command construction/spawn. The
    // current platform adapter launches the canonical path, so the verified
    // descriptor is retained only for the identity check until the future
    // descriptor-bound supervisor is introduced.
    let verified = launcher
        .verify_identity()
        .map_err(|_| ProcessLaunchError::LauncherRejected)?;
    let reaper = helper_reaper()?;

    let launch_guard = process_launch_guard().map_err(ProcessLaunchError::Bootstrap)?;
    prepare_bootstrap_rights(&launch_guard, &rights).map_err(ProcessLaunchError::Bootstrap)?;
    let (parent_socket, child_socket) =
        bootstrap_socket_pair(&launch_guard).map_err(ProcessLaunchError::Bootstrap)?;

    let child_input = unsafe { std::fs::File::from_raw_fd(child_socket.into_raw_fd()) };
    let mut command = build_helper_command(
        &verified.anchor.canonical_path,
        Stdio::from(child_input),
    );

    // Revalidate as the final operation before spawn. This closes the normal
    // replacement-before-spawn case; a same-UID swap in the tiny path lookup
    // window is the documented boundary until descriptor-bound launch exists.
    revalidate_launcher_before_spawn(launcher)?;
    let child = match command.spawn() {
        Ok(child) => child,
        Err(_) => return Err(ProcessLaunchError::Spawn),
    };

    // No inheritable process resources are created after this point. Keep the
    // guard through spawn return, then use the parent endpoint for protocol I/O.
    drop(launch_guard);
    let parent_raw = parent_socket.into_raw_fd();
    let mut parent_stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(parent_raw) };
    if let Err(error) = send_bootstrap_packet(parent_stream.as_raw_fd(), &frame, &rights.iter().map(AsRawFd::as_raw_fd).collect::<Vec<_>>()) {
        cleanup_helper(child, &reaper);
        return Err(ProcessLaunchError::Bootstrap(error));
    }
    if let Err(error) = read_helper_readiness(&mut parent_stream) {
        cleanup_helper(child, &reaper);
        return Err(error);
    }

    Ok(ValidatedHelper { child: Some(child), reaper })
}

#[cfg(not(unix))]
pub fn spawn_validated_helper(
    _launcher: &crate::execution_policy::ExecutableAnchor,
    _frame: ControlFrame,
    _rights: Vec<()>,
) -> Result<(), ()> { Err(()) }

#[cfg(unix)]
fn read_helper_readiness(
    stream: &mut std::os::unix::net::UnixStream,
) -> Result<(), ProcessLaunchError> {
    let deadline = Instant::now()
        .checked_add(HELPER_READY_TIMEOUT)
        .ok_or(ProcessLaunchError::Io)?;
    let mut record = [0u8; 8];
    let mut offset = 0usize;
    while offset < record.len() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
            .ok_or(ProcessLaunchError::ReadinessRejected)?;
        stream
            .set_read_timeout(Some(remaining))
            .map_err(|_| ProcessLaunchError::Io)?;
        match stream.read(&mut record[offset..]) {
            Ok(0) => return Err(ProcessLaunchError::ReadinessRejected),
            Ok(count) => offset += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::TimedOut || error.kind() == io::ErrorKind::WouldBlock => {
                return Err(ProcessLaunchError::ReadinessRejected)
            }
            Err(_) => return Err(ProcessLaunchError::Io),
        }
    }
    if record != HELPER_READY_RECORD {
        return Err(ProcessLaunchError::ReadinessRejected);
    }

    // A successful helper closes the channel immediately after its fixed
    // record. Reject any trailing byte and bound the wait by the same
    // absolute deadline.
    // The final exact-read iteration already installed a timeout bounded by
    // this same deadline. Do not reset SO_RCVTIMEO after peer half-close: on
    // some Unix implementations that transition rejects a second timeout
    // update with EINVAL. The existing timeout still bounds this read.
    let mut trailing = [0u8; 1];
    match stream.read(&mut trailing) {
        Ok(0) => Ok(()),
        Ok(_) => Err(ProcessLaunchError::ReadinessRejected),
        Err(error) if error.kind() == io::ErrorKind::Interrupted => Err(ProcessLaunchError::Io),
        Err(error) if error.kind() == io::ErrorKind::TimedOut || error.kind() == io::ErrorKind::WouldBlock => {
            Err(ProcessLaunchError::ReadinessRejected)
        }
        Err(_) => Err(ProcessLaunchError::Io),
    }
}

#[cfg(unix)]
pub(crate) fn bootstrap_socket_pair(
    guard: &ProcessLaunchGuard,
) -> Result<(OwnedFd, OwnedFd), BootstrapError> {
    let _ = &guard.0;
    let mut sockets = [-1; 2];
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let socket_type = libc::SOCK_STREAM | libc::SOCK_CLOEXEC;
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let socket_type = libc::SOCK_STREAM;
    if unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            socket_type,
            0,
            sockets.as_mut_ptr(),
        )
    } < 0 {
        return Err(BootstrapError::Io(io::Error::last_os_error()));
    }
    // SAFETY: socketpair initialized both descriptors and ownership is
    // transferred exactly once.
    let pair = unsafe {
        (OwnedFd::from_raw_fd(sockets[0]), OwnedFd::from_raw_fd(sockets[1]))
    };
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        set_close_on_exec(pair.0.as_raw_fd())?;
        set_close_on_exec(pair.1.as_raw_fd())?;
    }
    prove_close_on_exec(pair.0.as_raw_fd())?;
    prove_close_on_exec(pair.1.as_raw_fd())?;
    Ok(pair)
}

#[cfg(unix)]
pub(crate) fn bootstrap_slots(frame: &ControlFrame) -> Result<Vec<RawFd>, CodecError> {
    validate_frame_shape(frame)?;
    let mut slots = vec![CONTROL_FD, RELEASE_FD, EXEC_STATUS_FD, TARGET_FD];
    if frame.flags.contains(LaunchFlags::PROJECT_ROOT) { slots.push(PROJECT_ROOT_FD); }
    if frame.flags.contains(LaunchFlags::AGENT_LOG) { slots.push(AGENT_LOG_FD); }
    if frame.flags.contains(LaunchFlags::PUEUE_CONFIG) { slots.push(PUEUE_CONFIG_FD); }
    slots.push(RELEASE_ACK_FD);
    Ok(slots)
}

#[cfg(unix)]
pub(crate) fn bootstrap_right_slots(frame: &ControlFrame) -> Result<Vec<RawFd>, CodecError> {
    let mut slots = bootstrap_slots(frame)?;
    debug_assert_eq!(slots.first(), Some(&CONTROL_FD));
    slots.remove(0);
    Ok(slots)
}

#[cfg(unix)]
pub(crate) fn send_bootstrap_packet(
    socket: RawFd,
    frame: &ControlFrame,
    rights: &[RawFd],
) -> Result<(), BootstrapError> {
    send_bootstrap_packet_with_timeout(socket, frame, rights, BOOTSTRAP_IO_TIMEOUT)
}

#[cfg(unix)]
fn send_bootstrap_packet_with_timeout(
    socket: RawFd,
    frame: &ControlFrame,
    rights: &[RawFd],
    timeout: Duration,
) -> Result<(), BootstrapError> {
    let deadline = ProtocolDeadline::new(timeout)?;
    let bytes = frame.encode()?;
    if rights.len() != bootstrap_right_slots(frame)?.len() {
        return Err(BootstrapError::WrongRightCount);
    }
    let mut stream = mem::ManuallyDrop::new(unsafe { std::os::unix::net::UnixStream::from_raw_fd(socket) });
    deadline.set_write_timeout(&stream)?;
    let mut iov = libc::iovec {
        iov_base: bytes.as_ptr().cast_mut().cast(),
        iov_len: 1,
    };
    let rights_bytes = rights
        .len()
        .checked_mul(mem::size_of::<RawFd>())
        .ok_or(BootstrapError::WrongRightCount)?;
    // SAFETY: CMSG_SPACE only performs checked platform size arithmetic for
    // this small, protocol-bounded descriptor array.
    let control_len = unsafe { libc::CMSG_SPACE(rights_bytes as _) } as usize;
    let mut control = vec![0u8; control_len];
    let mut message: libc::msghdr = unsafe { mem::zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = control.len() as _;
    // SAFETY: message owns a correctly sized ancillary buffer.  The first
    // header and its data region fit because the buffer was sized by
    // CMSG_SPACE for exactly `rights_bytes`.
    unsafe {
        let header = libc::CMSG_FIRSTHDR(&message);
        if header.is_null() { return Err(BootstrapError::UnexpectedAncillary); }
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = libc::CMSG_LEN(rights_bytes as _) as _;
        ptr::copy_nonoverlapping(
            rights.as_ptr().cast::<u8>(),
            libc::CMSG_DATA(header),
            rights_bytes,
        );
        loop {
            deadline.set_write_timeout(&stream)?;
            let sent = libc::sendmsg(socket, &message, libc::MSG_NOSIGNAL);
            if sent < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted { continue; }
                return Err(BootstrapError::Io(error));
            }
            if sent != 1 { return Err(BootstrapError::TruncatedPacket); }
            break;
        }
    }
    // SAFETY: the caller owns `socket` for this operation; ManuallyDrop keeps
    // this borrowed wrapper from closing it.
    write_all_before(&mut stream, &bytes[1..], &deadline)?;
    if unsafe { libc::shutdown(socket, libc::SHUT_WR) } < 0 {
        return Err(BootstrapError::Io(io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn receive_bootstrap_packet(socket: RawFd) -> Result<BootstrapPacket, BootstrapError> {
    receive_bootstrap_packet_with_timeout(socket, BOOTSTRAP_IO_TIMEOUT)
}

#[cfg(unix)]
fn receive_bootstrap_packet_with_timeout(
    socket: RawFd,
    timeout: Duration,
) -> Result<BootstrapPacket, BootstrapError> {
    let deadline = ProtocolDeadline::new(timeout)?;
    let mut stream = mem::ManuallyDrop::new(unsafe { std::os::unix::net::UnixStream::from_raw_fd(socket) });
    deadline.set_read_timeout(&stream)?;
    let mut first = [0u8; 1];
    // One extra slot ensures an over-cardinality sender is observed rather
    // than silently accepted at the protocol maximum.
    let max_rights = 8usize;
    let ancillary_bytes = max_rights * mem::size_of::<RawFd>();
    let control_len = unsafe { libc::CMSG_SPACE(ancillary_bytes as _) } as usize;
    let mut control = vec![0u8; control_len];
    let mut iov = libc::iovec {
        iov_base: first.as_mut_ptr().cast(),
        iov_len: first.len(),
    };
    let mut message: libc::msghdr = unsafe { mem::zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = control.len() as _;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let recv_flags = libc::MSG_CMSG_CLOEXEC;
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let recv_flags = 0;
    let received = loop {
        deadline.set_read_timeout(&stream)?;
        let result = unsafe { libc::recvmsg(socket, &mut message, recv_flags) };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted { continue; }
            return Err(BootstrapError::Io(error));
        }
        break result;
    };
    if received == 0 { return Err(BootstrapError::EmptyPacket); }

    let mut rights = Vec::new();
    let mut ancillary_count = 0usize;
    let mut invalid_ancillary = false;
    unsafe {
        let mut header = libc::CMSG_FIRSTHDR(&message);
        while !header.is_null() {
            let base_len = libc::CMSG_LEN(0) as usize;
            let header_len = (*header).cmsg_len as usize;
            if (*header).cmsg_level == libc::SOL_SOCKET
                && (*header).cmsg_type == libc::SCM_RIGHTS
                && header_len >= base_len
            {
                ancillary_count += 1;
                let data_len = header_len - base_len;
                if data_len % mem::size_of::<RawFd>() == 0 {
                    for index in 0..(data_len / mem::size_of::<RawFd>()) {
                        let raw = ptr::read_unaligned(
                            libc::CMSG_DATA(header).cast::<RawFd>().add(index),
                        );
                        rights.push(OwnedFd::from_raw_fd(raw));
                    }
                } else {
                    invalid_ancillary = true;
                }
            } else {
                invalid_ancillary = true;
            }
            header = libc::CMSG_NXTHDR(&message, header);
        }
    }
    // Every complete received right is now RAII-owned. Any error below closes
    // all of them, including ancillary truncation and over-cardinality.
    if message.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
        return Err(BootstrapError::TruncatedPacket);
    }
    if invalid_ancillary || ancillary_count != 1 {
        return Err(BootstrapError::UnexpectedAncillary);
    }
    if rights.is_empty() { return Err(BootstrapError::MissingRights); }
    if rights.len() > 7 { return Err(BootstrapError::WrongRightCount); }
    for right in &rights {
        let flags = unsafe { libc::fcntl(right.as_raw_fd(), libc::F_GETFD) };
        if flags < 0 { return Err(BootstrapError::Io(io::Error::last_os_error())); }
        if flags & libc::FD_CLOEXEC == 0 {
            if unsafe { libc::fcntl(right.as_raw_fd(), libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
                return Err(BootstrapError::Io(io::Error::last_os_error()));
            }
        }
        let proof = unsafe { libc::fcntl(right.as_raw_fd(), libc::F_GETFD) };
        if proof < 0 || proof & libc::FD_CLOEXEC == 0 {
            return Err(BootstrapError::DescriptorNotCloseOnExec);
        }
    }
    let mut header = [0u8; HEADER_SIZE];
    header[0] = first[0];
    read_exact_before(&mut stream, &mut header[1..], &deadline)?;
    if header[..4] != *b"PAEX" { return Err(BootstrapError::Codec(CodecError::InvalidMagic)); }
    let payload_len = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
    let total = HEADER_SIZE.checked_add(payload_len).ok_or(CodecError::LengthOverflow)?;
    if total > MAX_FRAME_SIZE { return Err(BootstrapError::Codec(CodecError::FrameTooLarge)); }
    let mut bytes = vec![0u8; total];
    bytes[..HEADER_SIZE].copy_from_slice(&header);
    read_exact_before(&mut stream, &mut bytes[HEADER_SIZE..], &deadline)?;
    let mut trailing = [0u8; 1];
    deadline.set_read_timeout(&stream)?;
    if stream.read(&mut trailing)? != 0 {
        return Err(BootstrapError::Codec(CodecError::TrailingBytes));
    }
    let frame = ControlFrame::decode(&bytes)?;
    if rights.len() != bootstrap_right_slots(&frame)?.len() {
        return Err(BootstrapError::WrongRightCount);
    }
    Ok(BootstrapPacket { frame, rights })
}

#[cfg(unix)]
struct ProtocolDeadline { at: Instant }

#[cfg(unix)]
impl ProtocolDeadline {
    fn new(timeout: Duration) -> Result<Self, BootstrapError> {
        Instant::now().checked_add(timeout).map(|at| Self { at })
            .ok_or(BootstrapError::BootstrapCorrupt)
    }
    fn remaining(&self) -> Result<Duration, BootstrapError> {
        self.at.checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| BootstrapError::Io(io::Error::new(io::ErrorKind::TimedOut, "bootstrap deadline elapsed")))
    }
    fn set_read_timeout(&self, stream: &std::os::unix::net::UnixStream) -> Result<(), BootstrapError> {
        stream.set_read_timeout(Some(self.remaining()?)).map_err(BootstrapError::Io)
    }
    fn set_write_timeout(&self, stream: &std::os::unix::net::UnixStream) -> Result<(), BootstrapError> {
        stream.set_write_timeout(Some(self.remaining()?)).map_err(BootstrapError::Io)
    }
}

#[cfg(unix)]
fn read_exact_before(
    stream: &mut std::os::unix::net::UnixStream,
    mut buffer: &mut [u8],
    deadline: &ProtocolDeadline,
) -> Result<(), BootstrapError> {
    while !buffer.is_empty() {
        deadline.set_read_timeout(stream)?;
        match stream.read(buffer) {
            Ok(0) => return Err(BootstrapError::TruncatedPacket),
            Ok(count) => buffer = &mut buffer[count..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(BootstrapError::Io(error)),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn write_all_before(
    stream: &mut std::os::unix::net::UnixStream,
    mut buffer: &[u8],
    deadline: &ProtocolDeadline,
) -> Result<(), BootstrapError> {
    while !buffer.is_empty() {
        deadline.set_write_timeout(stream)?;
        match stream.write(buffer) {
            Ok(0) => return Err(BootstrapError::TruncatedPacket),
            Ok(count) => buffer = &buffer[count..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(BootstrapError::Io(error)),
        }
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn receive_and_install_bootstrap(
    socket: RawFd,
) -> Result<InstalledBootstrap, BootstrapError> {
    let packet = receive_bootstrap_packet(socket)?;
    // The Command-owned bootstrap endpoint is normally stdin.  Duplicate it
    // before touching any fixed ABI descriptor so every dup2 source is above
    // the complete 3..10 destination range.
    // SAFETY: after the receive finishes, the helper transfers exclusive
    // ownership of its bootstrap descriptor into fixed-map installation.
    let control = unsafe { OwnedFd::from_raw_fd(socket) };
    install_bootstrap_fixed_map(control, packet)
}

#[cfg(unix)]
fn install_bootstrap_fixed_map(
    control: OwnedFd,
    packet: BootstrapPacket,
) -> Result<InstalledBootstrap, BootstrapError> {
    install_bootstrap_fixed_map_with_failure(control, packet, None)
}

#[cfg(unix)]
fn install_bootstrap_fixed_map_with_failure(
    control: OwnedFd,
    packet: BootstrapPacket,
    fail_before_slot: Option<RawFd>,
) -> Result<InstalledBootstrap, BootstrapError> {
    let original_fixed_slots = original_fixed_slots(Some(&control), &packet.rights);
    let expected_hint = bootstrap_slots(&packet.frame).unwrap_or_default();
    let mut slots = FixedSlotTracker::new(expected_hint, original_fixed_slots);
    let prepared = match preflight_bootstrap_fixed_map(control, packet) {
        Ok(prepared) => prepared,
        Err(error) => {
            slots.finish_failure();
            return Err(error);
        }
    };
    let PreparedBootstrap { frame, expected_slots, mut sources } = prepared;
    slots.expected = expected_slots.clone();
    slots.drop_unused_originals(&mut sources);
    slots.close_unused();
    for (index, slot) in expected_slots.iter().copied().enumerate() {
        let source = sources[index].source.as_raw_fd();
        release_original_at(&mut sources, slot);
        slots.mark_original_released(slot);
        if fail_before_slot == Some(slot)
            || dup2_retry(source, slot).is_err()
            || set_close_on_exec(slot).is_err()
        {
            drop(sources);
            slots.finish_failure();
            return Err(BootstrapError::BootstrapCorrupt);
        }
        slots.mark_installed(slot);
    }
    drop(sources);
    for slot in &expected_slots {
        if validate_role(*slot, *slot, &frame).is_err() {
            slots.finish_failure();
            return Err(BootstrapError::BootstrapCorrupt);
        }
        let flags = unsafe { libc::fcntl(*slot, libc::F_GETFD) };
        if flags < 0 || flags & libc::FD_CLOEXEC == 0 {
            slots.finish_failure();
            return Err(BootstrapError::BootstrapCorrupt);
        }
    }
    Ok(InstalledBootstrap { frame })
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FixedSlotState { OwnedOriginal, Installed, Closed, NeverOwned }

#[cfg(unix)]
struct FixedSlotTracker {
    base: RawFd,
    expected: Vec<RawFd>,
    states: Vec<FixedSlotState>,
}

#[cfg(unix)]
impl FixedSlotTracker {
    fn new(expected: Vec<RawFd>, owned: Vec<RawFd>) -> Self {
        Self::with_range(CONTROL_FD, RELEASE_ACK_FD, expected, owned)
    }
    fn with_range(base: RawFd, end: RawFd, expected: Vec<RawFd>, owned: Vec<RawFd>) -> Self {
        let states = (base..=end).map(|fd| {
            if owned.contains(&fd) { FixedSlotState::OwnedOriginal }
            else { FixedSlotState::NeverOwned }
        }).collect();
        Self { base, expected, states }
    }
    fn state_mut(&mut self, fd: RawFd) -> &mut FixedSlotState {
        &mut self.states[(fd - self.base) as usize]
    }
    fn mark_original_released(&mut self, fd: RawFd) {
        if *self.state_mut(fd) == FixedSlotState::OwnedOriginal {
            *self.state_mut(fd) = FixedSlotState::NeverOwned;
        }
    }
    fn mark_installed(&mut self, fd: RawFd) { *self.state_mut(fd) = FixedSlotState::Installed; }
    fn drop_unused_originals(&mut self, sources: &mut [TrackedSource]) {
        for tracked in sources {
            let fd = tracked.original.as_ref().map(AsRawFd::as_raw_fd);
            if let Some(fd) = fd
                .filter(|fd| (*fd >= self.base) && (*fd < self.base + self.states.len() as RawFd))
                .filter(|fd| !self.expected.contains(fd))
            {
                drop(tracked.original.take());
                *self.state_mut(fd) = FixedSlotState::Closed;
            }
        }
    }
    fn close_unused(&mut self) {
        self.close_unused_with(&mut close_raw);
    }
    fn close_unused_with(&mut self, close: &mut impl FnMut(RawFd)) {
        for fd in self.base..self.base + self.states.len() as RawFd {
            if !self.expected.contains(&fd) { self.close_once_with(fd, close); }
        }
    }
    fn finish_failure(&mut self) {
        self.finish_failure_with(&mut close_raw);
    }
    fn finish_failure_with(&mut self, close: &mut impl FnMut(RawFd)) {
        for fd in self.base..self.base + self.states.len() as RawFd {
            self.close_once_with(fd, close);
        }
    }
    fn close_once_with(&mut self, fd: RawFd, close: &mut impl FnMut(RawFd)) {
        match *self.state_mut(fd) {
            FixedSlotState::Installed | FixedSlotState::NeverOwned => {
                close(fd);
                *self.state_mut(fd) = FixedSlotState::Closed;
            }
            FixedSlotState::OwnedOriginal | FixedSlotState::Closed => {}
        }
    }
}

#[cfg(unix)]
struct PreparedBootstrap {
    frame: ControlFrame,
    expected_slots: Vec<RawFd>,
    sources: Vec<TrackedSource>,
}

#[cfg(unix)]
struct TrackedSource {
    source: OwnedFd,
    original: Option<OwnedFd>,
}

#[cfg(unix)]
fn original_fixed_slots(control: Option<&OwnedFd>, rights: &[OwnedFd]) -> Vec<RawFd> {
    control.into_iter().chain(rights.iter())
        .map(AsRawFd::as_raw_fd)
        .filter(|fd| (CONTROL_FD..=RELEASE_ACK_FD).contains(fd))
        .collect()
}

#[cfg(unix)]
fn release_original_at(sources: &mut [TrackedSource], destination: RawFd) {
    use std::os::fd::IntoRawFd;
    for tracked in sources {
        if tracked.original.as_ref().map(AsRawFd::as_raw_fd) == Some(destination) {
            if let Some(original) = tracked.original.take() {
                let raw = original.into_raw_fd();
                debug_assert_eq!(raw, destination);
            }
            return;
        }
    }
}

#[cfg(unix)]
impl TrackedSource {
    fn move_above_fixed(descriptor: OwnedFd) -> Result<Self, BootstrapError> {
        Self::move_above_ceiling(descriptor, RELEASE_ACK_FD)
    }

    fn move_above_ceiling(
        descriptor: OwnedFd,
        ceiling: RawFd,
    ) -> Result<Self, BootstrapError> {
        if descriptor.as_raw_fd() > ceiling {
            return Ok(Self { source: descriptor, original: None });
        }
        let duplicated = unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_DUPFD_CLOEXEC, ceiling + 1) };
        if duplicated < 0 { return Err(BootstrapError::Io(io::Error::last_os_error())); }
        let source = unsafe { OwnedFd::from_raw_fd(duplicated) };
        Ok(Self { source, original: Some(descriptor) })
    }

}

#[cfg(unix)]
fn preflight_bootstrap_fixed_map(
    control: OwnedFd,
    packet: BootstrapPacket,
) -> Result<PreparedBootstrap, BootstrapError> {
    validate_distinct_pipe_roles(&packet)?;
    let expected_slots = bootstrap_slots(&packet.frame)?;
    let frame = packet.frame;
    let mut sources = Vec::with_capacity(expected_slots.len());
    sources.push(TrackedSource::move_above_fixed(control)?);
    for right in packet.rights {
        sources.push(TrackedSource::move_above_fixed(right)?);
    }
    if sources.len() != expected_slots.len() {
        return Err(BootstrapError::WrongRightCount);
    }
    for (slot, tracked) in expected_slots.iter().copied().zip(&sources) {
        validate_role(slot, tracked.source.as_raw_fd(), &frame)?;
    }
    Ok(PreparedBootstrap { frame, expected_slots, sources })
}

#[cfg(unix)]
fn dup2_retry(source: RawFd, destination: RawFd) -> Result<(), io::Error> {
    loop {
        if unsafe { libc::dup2(source, destination) } >= 0 { return Ok(()); }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted { return Err(error); }
    }
}

#[cfg(unix)]
fn validate_distinct_pipe_roles(packet: &BootstrapPacket) -> Result<(), BootstrapError> {
    let slots = bootstrap_right_slots(&packet.frame)?;
    let mut identities = [(0u64, 0u64); 3];
    for (identity_index, role) in [RELEASE_FD, EXEC_STATUS_FD, RELEASE_ACK_FD]
        .into_iter()
        .enumerate()
    {
        let position = slots.iter().position(|slot| *slot == role)
            .ok_or(BootstrapError::WrongRightCount)?;
        let descriptor = packet.rights.get(position).ok_or(BootstrapError::WrongRightCount)?;
        let mut stat: libc::stat = unsafe { mem::zeroed() };
        if unsafe { libc::fstat(descriptor.as_raw_fd(), &mut stat) } < 0 {
            return Err(BootstrapError::Io(io::Error::last_os_error()));
        }
        identities[identity_index] = (stat.st_dev as u64, stat.st_ino as u64);
    }
    if identities[0] == identities[1]
        || identities[0] == identities[2]
        || identities[1] == identities[2]
    {
        return Err(BootstrapError::AliasedPipeRoles);
    }
    Ok(())
}

#[cfg(unix)]
fn set_close_on_exec(raw: RawFd) -> Result<(), BootstrapError> {
    let flags = unsafe { libc::fcntl(raw, libc::F_GETFD) };
    if flags < 0 { return Err(BootstrapError::Io(io::Error::last_os_error())); }
    if unsafe { libc::fcntl(raw, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(BootstrapError::Io(io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(unix)]
fn prove_close_on_exec(raw: RawFd) -> Result<(), BootstrapError> {
    let flags = unsafe { libc::fcntl(raw, libc::F_GETFD) };
    if flags < 0 { return Err(BootstrapError::Io(io::Error::last_os_error())); }
    if flags & libc::FD_CLOEXEC == 0 {
        return Err(BootstrapError::DescriptorNotCloseOnExec);
    }
    Ok(())
}

#[cfg(unix)]
fn close_raw(raw: RawFd) {
    // Closing an already absent optional slot is harmless.  All live sources
    // were moved above the fixed range before this can run.
    unsafe { libc::close(raw); }
}

#[cfg(unix)]
fn validate_role(
    slot: RawFd,
    raw: RawFd,
    frame: &ControlFrame,
) -> Result<(), BootstrapError> {
    let mut stat: libc::stat = unsafe { mem::zeroed() };
    if unsafe { libc::fstat(raw, &mut stat) } < 0 {
        return Err(BootstrapError::Io(io::Error::last_os_error()));
    }
    let file_type = stat.st_mode & libc::S_IFMT;
    let access = unsafe { libc::fcntl(raw, libc::F_GETFL) };
    if access < 0 { return Err(BootstrapError::Io(io::Error::last_os_error())); }
    let access = access & libc::O_ACCMODE;
    let expected_identity = match slot {
        CONTROL_FD if file_type == libc::S_IFSOCK && access == libc::O_RDWR => None,
        RELEASE_FD if file_type == libc::S_IFIFO && access == libc::O_RDONLY => {
            let mut descriptor = libc::pollfd { fd: raw, events: libc::POLLIN, revents: 0 };
            let ready = unsafe { libc::poll(&mut descriptor, 1, 0) };
            if ready < 0 { return Err(BootstrapError::Io(io::Error::last_os_error())); }
            if ready != 0 { return Err(BootstrapError::GateClosed); }
            None
        }
        EXEC_STATUS_FD if file_type == libc::S_IFIFO && access == libc::O_WRONLY => None,
        TARGET_FD if file_type == libc::S_IFREG && access == libc::O_RDONLY => {
            Some(frame.target_identity)
        }
        PROJECT_ROOT_FD if file_type == libc::S_IFDIR && access == libc::O_RDONLY => {
            frame.project_root_identity
        }
        AGENT_LOG_FD
            if file_type == libc::S_IFREG
                && (access == libc::O_WRONLY || access == libc::O_RDWR) =>
        {
            frame.agent_log_identity
        }
        PUEUE_CONFIG_FD if file_type == libc::S_IFREG && access == libc::O_RDONLY => {
            frame.pueue_config_identity
        }
        RELEASE_ACK_FD if file_type == libc::S_IFIFO && access == libc::O_WRONLY => None,
        _ => return Err(BootstrapError::WrongRightCount),
    };
    if slot == TARGET_FD || matches!(slot, PROJECT_ROOT_FD | AGENT_LOG_FD | PUEUE_CONFIG_FD) {
        let expected = expected_identity.ok_or(BootstrapError::WrongRightCount)?;
        let actual = ExecutableIdentity {
            device: stat.st_dev as u64,
            inode: stat.st_ino as u64,
            owner: stat.st_uid as u32,
            mode: (stat.st_mode as u32) & 0o7777,
        };
        if actual != expected { return Err(BootstrapError::IdentityMismatch); }
    }
    Ok(())
}

pub fn encode_control_frame(frame: &ControlFrame) -> Result<Vec<u8>, CodecError> {
    validate_frame_shape(frame)?;
    let argv_len = argv_encoded_len(&frame.argv)?;
    let environment_len = environment_encoded_len(&frame.environment)?;
    let cwd_len = frame.cwd.as_ref().map(|cwd| leaf_encoded_len(cwd)).transpose()?.unwrap_or(0);
    let field_count = 3
        + usize::from(frame.cwd.is_some())
        + usize::from(frame.project_root_identity.is_some())
        + usize::from(frame.agent_log_identity.is_some())
        + usize::from(frame.pueue_config_identity.is_some());
    let mut payload_len = 4usize;
    payload_len = payload_len.checked_add(encoded_field_size(argv_len))
        .and_then(|value| value.checked_add(encoded_field_size(environment_len)))
        .and_then(|value| value.checked_add(encoded_field_size(IDENTITY_SIZE)))
        .ok_or(CodecError::LengthOverflow)?;
    if frame.cwd.is_some() { payload_len = payload_len.checked_add(encoded_field_size(cwd_len)).ok_or(CodecError::LengthOverflow)?; }
    for present in [frame.project_root_identity.is_some(), frame.agent_log_identity.is_some(), frame.pueue_config_identity.is_some()] {
        if present { payload_len = payload_len.checked_add(encoded_field_size(IDENTITY_SIZE)).ok_or(CodecError::LengthOverflow)?; }
    }
    let total = HEADER_SIZE.checked_add(payload_len).ok_or(CodecError::LengthOverflow)?;
    if total > MAX_FRAME_SIZE { return Err(CodecError::FrameTooLarge); }
    let mut output = Vec::with_capacity(total);
    output.extend_from_slice(b"PAEX");
    output.push(1);
    output.push(frame.mode as u8);
    push_u16(&mut output, frame.flags.bits());
    push_u32(&mut output, payload_len as u32);
    push_u32(&mut output, field_count as u32);
    append_argv_field(&mut output, &frame.argv, argv_len)?;
    append_environment_field(&mut output, &frame.environment, environment_len)?;
    append_identity_field(&mut output, FIELD_TARGET_IDENTITY, &frame.target_identity);
    if let Some(cwd) = &frame.cwd {
        append_os_field(&mut output, FIELD_CWD, cwd)?;
    }
    if let Some(identity) = frame.project_root_identity {
        append_identity_field(&mut output, FIELD_PROJECT_ROOT_IDENTITY, &identity);
    }
    if let Some(identity) = frame.agent_log_identity {
        append_identity_field(&mut output, FIELD_AGENT_LOG_IDENTITY, &identity);
    }
    if let Some(identity) = frame.pueue_config_identity {
        append_identity_field(&mut output, FIELD_PUEUE_CONFIG_IDENTITY, &identity);
    }
    Ok(output)
}

pub fn decode_control_frame(bytes: &[u8]) -> Result<ControlFrame, CodecError> {
    if bytes.len() < HEADER_SIZE { return Err(CodecError::Truncated); }
    if bytes[..4] != *b"PAEX" { return Err(CodecError::InvalidMagic); }
    if bytes[4] != 1 { return Err(CodecError::UnsupportedVersion); }
    let mode = LaunchMode::try_from(bytes[5])?;
    let flags = LaunchFlags::from_bits(u16::from_be_bytes([bytes[6], bytes[7]]))?;
    let payload_len = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
    let total = HEADER_SIZE.checked_add(payload_len).ok_or(CodecError::LengthOverflow)?;
    if total > MAX_FRAME_SIZE { return Err(CodecError::FrameTooLarge); }
    if bytes.len() < total { return Err(CodecError::Truncated); }
    if bytes.len() > total { return Err(CodecError::TrailingBytes); }
    let mut cursor = Cursor::new(&bytes[HEADER_SIZE..total]);
    let field_count = cursor.u32()? as usize;
    if field_count > 7 { return Err(CodecError::TooManyFields); }
    let mut argv = None;
    let mut environment = None;
    let mut target_identity = None;
    let mut cwd = None;
    let mut project_root_identity = None;
    let mut agent_log_identity = None;
    let mut pueue_config_identity = None;
    let mut previous_kind = 0;
    for _ in 0..field_count {
        let kind = cursor.u8()?;
        if kind <= previous_kind { return Err(CodecError::NonIncreasingFieldOrder); }
        previous_kind = kind;
        let length = cursor.u32()? as usize;
        if length > cursor.remaining() { return Err(CodecError::Truncated); }
        let body = cursor.bytes(length)?;
        match kind {
            FIELD_ARGV => set_once(&mut argv, decode_argv(body), kind)?,
            FIELD_ENV => set_once(&mut environment, decode_environment(body), kind)?,
            FIELD_TARGET_IDENTITY => set_once(&mut target_identity, decode_identity(body), kind)?,
            FIELD_CWD => set_once(&mut cwd, decode_os_field(body), kind)?,
            FIELD_PROJECT_ROOT_IDENTITY => set_once(&mut project_root_identity, decode_identity(body), kind)?,
            FIELD_AGENT_LOG_IDENTITY => set_once(&mut agent_log_identity, decode_identity(body), kind)?,
            FIELD_PUEUE_CONFIG_IDENTITY => set_once(&mut pueue_config_identity, decode_identity(body), kind)?,
            other => return Err(CodecError::UnknownField(other)),
        }
    }
    if cursor.remaining() != 0 { return Err(CodecError::TrailingBytes); }
    let frame = ControlFrame {
        mode,
        flags,
        argv: argv.ok_or(CodecError::MissingField(FIELD_ARGV))??,
        environment: environment.ok_or(CodecError::MissingField(FIELD_ENV))??,
        target_identity: target_identity.ok_or(CodecError::MissingField(FIELD_TARGET_IDENTITY))??,
        cwd: cwd.transpose()?,
        project_root_identity: project_root_identity.transpose()?,
        agent_log_identity: agent_log_identity.transpose()?,
        pueue_config_identity: pueue_config_identity.transpose()?,
    };
    validate_frame_shape(&frame)?;
    Ok(frame)
}

fn validate_frame_shape(frame: &ControlFrame) -> Result<(), CodecError> {
    if frame.flags.bits() & !KNOWN_FLAGS != 0 { return Err(CodecError::UnknownFlags); }
    if frame.argv.len() > MAX_ARGV { return Err(CodecError::TooManyArguments); }
    if frame.environment.len() > MAX_ENV { return Err(CodecError::TooManyEnvironmentEntries); }
    let root = frame.flags.contains(LaunchFlags::PROJECT_ROOT);
    let log = frame.flags.contains(LaunchFlags::AGENT_LOG);
    let pueue = frame.flags.contains(LaunchFlags::PUEUE_CONFIG);
    if !frame.flags.contains(LaunchFlags::PROCESS_GROUP) { return Err(CodecError::MissingProcessGroup); }
    match frame.mode {
        LaunchMode::Agent => {
            if !root { return Err(CodecError::MissingField(FIELD_PROJECT_ROOT_IDENTITY)); }
            if !log { return Err(CodecError::MissingField(FIELD_AGENT_LOG_IDENTITY)); }
            if pueue { return Err(CodecError::UnexpectedField(FIELD_PUEUE_CONFIG_IDENTITY)); }
        }
        LaunchMode::Pueue => {
            if !pueue { return Err(CodecError::MissingField(FIELD_PUEUE_CONFIG_IDENTITY)); }
            if root { return Err(CodecError::UnexpectedField(FIELD_PROJECT_ROOT_IDENTITY)); }
            if log { return Err(CodecError::UnexpectedField(FIELD_AGENT_LOG_IDENTITY)); }
        }
    }
    if root != frame.project_root_identity.is_some() { return Err(if root { CodecError::MissingField(FIELD_PROJECT_ROOT_IDENTITY) } else { CodecError::UnexpectedField(FIELD_PROJECT_ROOT_IDENTITY) }); }
    if log != frame.agent_log_identity.is_some() { return Err(if log { CodecError::MissingField(FIELD_AGENT_LOG_IDENTITY) } else { CodecError::UnexpectedField(FIELD_AGENT_LOG_IDENTITY) }); }
    if pueue != frame.pueue_config_identity.is_some() { return Err(if pueue { CodecError::MissingField(FIELD_PUEUE_CONFIG_IDENTITY) } else { CodecError::UnexpectedField(FIELD_PUEUE_CONFIG_IDENTITY) }); }
    validate_environment_without_allocation(&frame.environment)?;
    for arg in &frame.argv { validate_field_bytes(arg)?; }
    if let Some(cwd) = &frame.cwd { validate_field_bytes(cwd)?; }
    Ok(())
}

/// Validate the caller-owned environment without building a set or cloning
/// any names.  The wire contract bounds this list to 128 entries, making the
/// borrowed O(n²) duplicate check both bounded and preferable to allocating
/// before the exact frame-size preflight has completed.
fn validate_environment_without_allocation(
    environment: &[(OsString, OsString)],
) -> Result<(), CodecError> {
    for (index, (name, value)) in environment.iter().enumerate() {
        validate_field_bytes(name)?;
        validate_field_bytes(value)?;
        let name_bytes = os_bytes(name)?;
        if name_bytes.is_empty() || name_bytes.contains(&b'=') {
            return Err(CodecError::InvalidEnvironmentName);
        }
        for (previous, _) in &environment[..index] {
            if os_bytes(previous)? == name_bytes {
                return Err(CodecError::DuplicateEnvironmentName);
            }
        }
    }
    Ok(())
}

fn leaf_encoded_len(value: &OsStr) -> Result<usize, CodecError> {
    validate_field_bytes(value)?;
    4usize.checked_add(os_bytes(value)?.len()).ok_or(CodecError::LengthOverflow)
}

fn argv_encoded_len(argv: &[OsString]) -> Result<usize, CodecError> {
    let mut length = 4usize;
    for value in argv { length = length.checked_add(leaf_encoded_len(value)?).ok_or(CodecError::LengthOverflow)?; }
    Ok(length)
}

fn environment_encoded_len(environment: &[(OsString, OsString)]) -> Result<usize, CodecError> {
    let mut length = 4usize;
    for (name, value) in environment {
        let name_len = leaf_encoded_len(name)?;
        let value_len = leaf_encoded_len(value)?;
        length = length.checked_add(name_len).and_then(|length| length.checked_add(value_len)).ok_or(CodecError::LengthOverflow)?;
    }
    Ok(length)
}

fn encoded_field_size(body_len: usize) -> usize { 1 + 4 + body_len }

fn append_argv_field(output: &mut Vec<u8>, argv: &[OsString], body_len: usize) -> Result<(), CodecError> {
    output.push(FIELD_ARGV);
    push_u32(output, body_len as u32);
    push_u32(output, argv.len() as u32);
    for value in argv { push_os_field(output, value)?; }
    Ok(())
}

fn append_environment_field(output: &mut Vec<u8>, environment: &[(OsString, OsString)], body_len: usize) -> Result<(), CodecError> {
    output.push(FIELD_ENV);
    push_u32(output, body_len as u32);
    push_u32(output, environment.len() as u32);
    for (name, value) in environment {
        push_os_field(output, name)?;
        push_os_field(output, value)?;
    }
    Ok(())
}

fn append_os_field(output: &mut Vec<u8>, kind: u8, value: &OsStr) -> Result<(), CodecError> {
    output.push(kind);
    push_u32(output, leaf_encoded_len(value)? as u32);
    push_os_field(output, value)
}

fn append_identity_field(output: &mut Vec<u8>, kind: u8, identity: &ExecutableIdentity) {
    output.push(kind);
    push_u32(output, IDENTITY_SIZE as u32);
    output.extend_from_slice(&identity.device.to_be_bytes());
    output.extend_from_slice(&identity.inode.to_be_bytes());
    output.extend_from_slice(&identity.owner.to_be_bytes());
    output.extend_from_slice(&identity.mode.to_be_bytes());
}

fn decode_argv(bytes: &[u8]) -> Result<Vec<OsString>, CodecError> {
    let mut cursor = Cursor::new(bytes);
    let count = cursor.u32()? as usize;
    if count > MAX_ARGV { return Err(CodecError::TooManyArguments); }
    let mut values = Vec::with_capacity(count);
    for _ in 0..count { values.push(cursor.os_field()?); }
    cursor.finish()?;
    Ok(values)
}

fn decode_os_field(bytes: &[u8]) -> Result<OsString, CodecError> {
    let mut cursor = Cursor::new(bytes);
    let value = cursor.os_field()?;
    cursor.finish()?;
    Ok(value)
}

fn decode_environment(bytes: &[u8]) -> Result<Vec<(OsString, OsString)>, CodecError> {
    let mut cursor = Cursor::new(bytes);
    let count = cursor.u32()? as usize;
    if count > MAX_ENV { return Err(CodecError::TooManyEnvironmentEntries); }
    let mut values = Vec::with_capacity(count);
    let mut names = std::collections::HashSet::with_capacity(count);
    for _ in 0..count {
        let name = cursor.os_field()?;
        let value = cursor.os_field()?;
        let name_bytes = os_bytes(&name)?;
        if name_bytes.is_empty() || name_bytes.contains(&b'=') {
            return Err(CodecError::InvalidEnvironmentName);
        }
        let key = name_bytes.to_vec();
        if !names.insert(key) { return Err(CodecError::DuplicateEnvironmentName); }
        values.push((name, value));
    }
    cursor.finish()?;
    Ok(values)
}

fn decode_identity(bytes: &[u8]) -> Result<ExecutableIdentity, CodecError> {
    if bytes.len() != IDENTITY_SIZE { return Err(CodecError::InvalidIdentity); }
    Ok(ExecutableIdentity {
        device: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
        inode: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
        owner: u32::from_be_bytes(bytes[16..20].try_into().unwrap()),
        mode: u32::from_be_bytes(bytes[20..24].try_into().unwrap()),
    })
}

fn validate_field_bytes(value: &OsStr) -> Result<(), CodecError> {
    let bytes = os_bytes(value)?;
    if bytes.len() > MAX_FIELD_SIZE { return Err(CodecError::FieldTooLarge); }
    if bytes.contains(&0) { return Err(CodecError::NulByte); }
    Ok(())
}

fn os_bytes(value: &OsStr) -> Result<&[u8], CodecError> {
    #[cfg(unix)] { Ok(value.as_bytes()) }
    #[cfg(not(unix))] { value.to_str().map(str::as_bytes).ok_or(CodecError::InvalidField) }
}

fn os_string(bytes: Vec<u8>) -> Result<OsString, CodecError> {
    if bytes.contains(&0) { return Err(CodecError::NulByte); }
    #[cfg(unix)] { Ok(OsString::from_vec(bytes)) }
    #[cfg(not(unix))] { String::from_utf8(bytes).map(OsString::from).map_err(|_| CodecError::InvalidField) }
}

fn push_os_field(output: &mut Vec<u8>, value: &OsStr) -> Result<(), CodecError> {
    validate_field_bytes(value)?;
    let bytes = os_bytes(value)?;
    push_u32(output, bytes.len() as u32);
    output.extend_from_slice(bytes);
    Ok(())
}

fn push_u16(output: &mut Vec<u8>, value: u16) { output.extend_from_slice(&value.to_be_bytes()); }
fn push_u32(output: &mut Vec<u8>, value: u32) { output.extend_from_slice(&value.to_be_bytes()); }

fn set_once<T>(slot: &mut Option<Result<T, CodecError>>, value: Result<T, CodecError>, kind: u8) -> Result<(), CodecError> {
    if slot.is_some() { return Err(CodecError::DuplicateField(kind)); }
    *slot = Some(value);
    Ok(())
}

struct Cursor<'a> { bytes: &'a [u8], position: usize }
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self { Self { bytes, position: 0 } }
    fn remaining(&self) -> usize { self.bytes.len().saturating_sub(self.position) }
    fn take(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let end = self.position.checked_add(length).ok_or(CodecError::LengthOverflow)?;
        if end > self.bytes.len() { return Err(CodecError::Truncated); }
        let result = &self.bytes[self.position..end];
        self.position = end;
        Ok(result)
    }
    fn bytes(&mut self, length: usize) -> Result<&'a [u8], CodecError> { self.take(length) }
    fn u8(&mut self) -> Result<u8, CodecError> { Ok(*self.take(1)?.first().unwrap()) }
    fn u32(&mut self) -> Result<u32, CodecError> { Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap())) }
    fn os_field(&mut self) -> Result<OsString, CodecError> {
        let length = self.u32()? as usize;
        if length > MAX_FIELD_SIZE { return Err(CodecError::FieldTooLarge); }
        os_string(self.take(length)?.to_vec())
    }
    fn finish(&self) -> Result<(), CodecError> { if self.remaining() == 0 { Ok(()) } else { Err(CodecError::TrailingBytes) } }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        ffi::OsString,
        fs::{self, File, OpenOptions},
        os::fd::{AsRawFd, FromRawFd, OwnedFd},
        os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        process::{Child, Command, Stdio},
        time::{Duration, Instant},
    };

    fn identity() -> ExecutableIdentity { ExecutableIdentity { device: 1, inode: 2, owner: 3, mode: 0o755 } }
    fn frame() -> ControlFrame {
        ControlFrame {
            mode: LaunchMode::Agent,
            flags: LaunchFlags::PROJECT_ROOT | LaunchFlags::AGENT_LOG | LaunchFlags::PROCESS_GROUP,
            argv: vec![OsString::from("codex"), OsString::from("prompt")],
            environment: vec![(OsString::from("LANG"), OsString::from("C"))],
            cwd: Some(OsString::from("/trusted/project")),
            target_identity: identity(),
            project_root_identity: Some(identity()),
            agent_log_identity: Some(identity()),
            pueue_config_identity: None,
        }
    }

    #[test]
    fn round_trip_preserves_frame() {
        let original = frame();
        let encoded = original.encode().unwrap();
        assert_eq!(ControlFrame::decode(&encoded).unwrap(), original);
    }

    #[cfg(unix)]
    #[test]
    fn round_trip_preserves_non_utf8_os_strings() {
        let mut original = frame();
        original.argv = vec![OsString::from_vec(vec![b'a', 0xff, b'b'])];
        original.environment = vec![(OsString::from_vec(vec![b'K', 0xfe]), OsString::from_vec(vec![0xfd]))];
        original.cwd = Some(OsString::from_vec(vec![b'/', 0xfc, b'c']));
        assert_eq!(ControlFrame::decode(&original.encode().unwrap()).unwrap(), original);
    }

    #[test]
    fn rejects_unknown_mode_flags_fields_and_trailing_bytes() {
        let encoded = frame().encode().unwrap();
        let mut mode = encoded.clone(); mode[5] = 9;
        assert_eq!(ControlFrame::decode(&mode), Err(CodecError::UnknownMode));
        let mut flags = encoded.clone(); flags[6] = 0x80;
        assert_eq!(ControlFrame::decode(&flags), Err(CodecError::UnknownFlags));
        let mut trailing = encoded.clone(); trailing.push(0);
        assert_eq!(ControlFrame::decode(&trailing), Err(CodecError::TrailingBytes));
        let mut unknown = encoded.clone();
        // Replace the first canonical field id with an unknown value.
        let field_count_offset = HEADER_SIZE;
        assert_eq!(u32::from_be_bytes(unknown[field_count_offset..field_count_offset + 4].try_into().unwrap()), 6);
        let offset = HEADER_SIZE + 4;
        unknown[offset] = 99;
        assert!(matches!(ControlFrame::decode(&unknown), Err(CodecError::UnknownField(99))));
    }

    #[test]
    fn rejects_nul_duplicates_and_boundaries() {
        let mut nul = frame(); nul.argv[0] = OsString::from("a\0b");
        assert_eq!(nul.encode(), Err(CodecError::NulByte));
        let mut duplicate = frame(); duplicate.environment.push((OsString::from("LANG"), OsString::from("en")));
        assert_eq!(duplicate.encode(), Err(CodecError::DuplicateEnvironmentName));
        let mut invalid_name = frame(); invalid_name.environment[0].0 = OsString::from("BAD=NAME");
        assert_eq!(invalid_name.encode(), Err(CodecError::InvalidEnvironmentName));
        let mut too_many = frame(); too_many.argv = (0..=MAX_ARGV).map(|_| OsString::from("x")).collect();
        assert_eq!(too_many.encode(), Err(CodecError::TooManyArguments));
        let mut too_many_env = frame(); too_many_env.environment = (0..=MAX_ENV).map(|index| (OsString::from(format!("K{index}")), OsString::from("v"))).collect();
        assert_eq!(too_many_env.encode(), Err(CodecError::TooManyEnvironmentEntries));
        let mut too_long = frame(); too_long.argv = vec![OsString::from("x".repeat(MAX_FIELD_SIZE + 1))];
        assert_eq!(too_long.encode(), Err(CodecError::FieldTooLarge));
        let mut bad_cwd = frame(); bad_cwd.cwd = Some(OsString::from("bad\0cwd"));
        assert_eq!(bad_cwd.encode(), Err(CodecError::NulByte));
    }

    #[test]
    fn decoder_enforces_u32_count_and_frame_limits() {
        let encoded = frame().encode().unwrap();
        let mut argv_count = encoded.clone();
        // Header (12), field count (4), first field id/length (5), then the
        // argv body's u32 count.
        let argv_count_offset = HEADER_SIZE + 4 + 5;
        argv_count[argv_count_offset..argv_count_offset + 4]
            .copy_from_slice(&((MAX_ARGV as u32) + 1).to_be_bytes());
        assert_eq!(ControlFrame::decode(&argv_count), Err(CodecError::TooManyArguments));

        let mut oversized = vec![0; HEADER_SIZE];
        oversized[..4].copy_from_slice(b"PAEX");
        oversized[4] = 1;
        oversized[5] = LaunchMode::Agent as u8;
        oversized[8..12].copy_from_slice(&(MAX_FRAME_SIZE as u32).to_be_bytes());
        assert_eq!(ControlFrame::decode(&oversized), Err(CodecError::FrameTooLarge));
    }

    #[test]
    fn container_may_exceed_leaf_limit_but_frame_still_has_a_one_mib_cap() {
        let mut aggregate = frame();
        aggregate.argv = vec![OsString::from("x".repeat(40 * 1024)), OsString::from("y".repeat(40 * 1024))];
        let encoded = aggregate.encode().unwrap();
        assert!(encoded.len() > MAX_FIELD_SIZE);
        assert_eq!(ControlFrame::decode(&encoded).unwrap(), aggregate);

        let mut oversized = frame();
        oversized.argv = (0..20).map(|_| OsString::from("z".repeat(MAX_FIELD_SIZE))).collect();
        assert_eq!(oversized.encode(), Err(CodecError::FrameTooLarge));
    }

    #[test]
    fn oversized_environment_rejects_after_borrowed_preflight() {
        let mut oversized = frame();
        oversized.environment = (0..MAX_ENV)
            .map(|index| {
                (
                    OsString::from(format!("KEY_{index}")),
                    OsString::from("v".repeat(MAX_FIELD_SIZE)),
                )
            })
            .collect();
        assert_eq!(oversized.encode(), Err(CodecError::FrameTooLarge));
    }

    #[test]
    fn optional_descriptor_flags_must_match_identity_fields() {
        let mut missing = frame(); missing.project_root_identity = None;
        assert_eq!(missing.encode(), Err(CodecError::MissingField(FIELD_PROJECT_ROOT_IDENTITY)));
        let mut unexpected = frame(); unexpected.flags = LaunchFlags::AGENT_LOG | LaunchFlags::PROCESS_GROUP;
        assert_eq!(unexpected.encode(), Err(CodecError::MissingField(FIELD_PROJECT_ROOT_IDENTITY)));
    }

    #[test]
    fn mode_and_descriptor_matrix_is_closed() {
        let mut agent = frame();
        assert!(agent.encode().is_ok());
        agent.flags = LaunchFlags::PROJECT_ROOT | LaunchFlags::PROCESS_GROUP;
        assert_eq!(agent.encode(), Err(CodecError::MissingField(FIELD_AGENT_LOG_IDENTITY)));
        agent.flags = LaunchFlags::PROJECT_ROOT | LaunchFlags::AGENT_LOG | LaunchFlags::PUEUE_CONFIG | LaunchFlags::PROCESS_GROUP;
        assert_eq!(agent.encode(), Err(CodecError::UnexpectedField(FIELD_PUEUE_CONFIG_IDENTITY)));
        agent.mode = LaunchMode::Pueue;
        agent.flags = LaunchFlags::PUEUE_CONFIG | LaunchFlags::PROCESS_GROUP;
        agent.project_root_identity = None;
        agent.agent_log_identity = None;
        agent.pueue_config_identity = Some(identity());
        assert!(agent.encode().is_ok());
        agent.flags = LaunchFlags::PROCESS_GROUP;
        assert_eq!(agent.encode(), Err(CodecError::MissingField(FIELD_PUEUE_CONFIG_IDENTITY)));
        agent.flags = LaunchFlags::PUEUE_CONFIG | LaunchFlags::PROJECT_ROOT | LaunchFlags::PROCESS_GROUP;
        assert_eq!(agent.encode(), Err(CodecError::UnexpectedField(FIELD_PROJECT_ROOT_IDENTITY)));
        agent.flags = LaunchFlags::PUEUE_CONFIG | LaunchFlags::AGENT_LOG | LaunchFlags::PROCESS_GROUP;
        assert_eq!(agent.encode(), Err(CodecError::UnexpectedField(FIELD_AGENT_LOG_IDENTITY)));
        agent.flags = LaunchFlags::PROJECT_ROOT | LaunchFlags::AGENT_LOG;
        agent.mode = LaunchMode::Agent;
        agent.project_root_identity = Some(identity());
        agent.agent_log_identity = Some(identity());
        agent.pueue_config_identity = None;
        assert_eq!(agent.encode(), Err(CodecError::MissingProcessGroup));
    }

    #[test]
    fn debug_is_redacted_to_shape_only() {
        let mut value = frame();
        value.argv = vec![OsString::from("secret-argv")];
        value.environment = vec![(OsString::from("FIXTURE_NAME"), OsString::from("fixture-value"))];
        value.cwd = Some(OsString::from("secret-cwd"));
        let debug = format!("{value:?}");
        assert!(debug.contains("argv_count"));
        assert!(debug.contains("environment_count"));
        assert!(!debug.contains("secret-argv"));
        assert!(!debug.contains("FIXTURE_NAME"));
        assert!(!debug.contains("fixture-value"));
        assert!(!debug.contains("secret-cwd"));
    }

    #[test]
    fn decoder_rejects_nul_and_duplicate_environment_names() {
        let mut nul = Vec::new();
        push_u32(&mut nul, 1);
        push_u32(&mut nul, 1);
        nul.push(b'A');
        let nul_value = b"bad\0value";
        push_u32(&mut nul, nul_value.len() as u32);
        nul.extend_from_slice(nul_value);
        assert_eq!(decode_environment(&nul), Err(CodecError::NulByte));
        let mut duplicate = Vec::new();
        push_u32(&mut duplicate, 2);
        for value in [b"one".as_slice(), b"two".as_slice()] {
            push_u32(&mut duplicate, 1);
            duplicate.push(b'A');
            push_u32(&mut duplicate, value.len() as u32);
            duplicate.extend_from_slice(value);
        }
        assert_eq!(decode_environment(&duplicate), Err(CodecError::DuplicateEnvironmentName));
    }

    #[test]
    fn fixed_descriptor_contract_is_immutable() {
        let fds = FixedFdContract::standard();
        assert!(fds.is_standard());
        assert_eq!(fds.control, 3);
        assert_eq!(fds.release, 4);
        assert_eq!(fds.exec_status, 5);
        assert_eq!(fds.target, 6);
        assert_eq!(fds.project_root, 7);
        assert_eq!(fds.agent_log, 8);
        assert_eq!(fds.pueue_config, 9);
        assert_eq!(fds.release_ack, 10);
    }

    #[cfg(unix)]
    #[test]
    fn helper_command_argv_contains_only_the_hidden_subcommand() {
        let command = build_helper_command(
            std::path::Path::new("/trusted/pueue-agent"),
            Stdio::null(),
        );
        assert_eq!(command.get_program(), std::ffi::OsStr::new("/trusted/pueue-agent"));
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            vec![std::ffi::OsStr::new("internal-launch")],
        );
    }

    #[cfg(unix)]
    #[test]
    fn parent_rights_are_made_close_on_exec_before_spawn() {
        let (right, _peer) = pipe_pair();
        let flags = unsafe { libc::fcntl(right.as_raw_fd(), libc::F_GETFD) };
        assert!(flags >= 0);
        assert_eq!(
            unsafe { libc::fcntl(right.as_raw_fd(), libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
            0,
        );
        assert_eq!(unsafe { libc::fcntl(right.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC, 0);

        let guard = process_launch_guard().unwrap();
        prepare_bootstrap_rights(&guard, std::slice::from_ref(&right)).unwrap();

        assert_ne!(
            unsafe { libc::fcntl(right.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0,
        );
    }

    #[cfg(unix)]
    #[test]
    fn final_revalidation_rejects_replacement_after_command_construction() {
        let temporary = tempfile::tempdir().unwrap();
        let launcher = temporary.path().join("trusted-launcher");
        fs::copy(std::env::current_exe().unwrap(), &launcher).unwrap();
        fs::set_permissions(&launcher, fs::Permissions::from_mode(0o700)).unwrap();
        let launcher = fs::canonicalize(launcher).unwrap();
        let anchor = crate::execution_policy::ExecutableAnchor::from_absolute(&launcher, &[])
            .unwrap();
        let replacement = temporary.path().join("replacement-launcher");
        fs::copy(std::env::current_exe().unwrap(), &replacement).unwrap();
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o700)).unwrap();

        let _command = build_helper_command(&launcher, Stdio::null());
        fs::rename(replacement, launcher).unwrap();
        let result = revalidate_launcher_before_spawn(&anchor);
        assert!(matches!(result, Err(ProcessLaunchError::LauncherRejected)));
    }

    #[cfg(unix)]
    #[test]
    fn bootstrap_role_matrix_has_exact_optional_slots() {
        let frame = frame();
        assert_eq!(bootstrap_slots(&frame).unwrap(), vec![3, 4, 5, 6, 7, 8, 10]);

        let mut pueue = frame;
        pueue.mode = LaunchMode::Pueue;
        pueue.flags = LaunchFlags::PUEUE_CONFIG | LaunchFlags::PROCESS_GROUP;
        pueue.project_root_identity = None;
        pueue.agent_log_identity = None;
        pueue.pueue_config_identity = Some(identity());
        assert_eq!(bootstrap_slots(&pueue).unwrap(), vec![3, 4, 5, 6, 9, 10]);
    }

    #[cfg(unix)]
    fn socket_pair() -> (OwnedFd, OwnedFd) {
        let guard = process_launch_guard().unwrap();
        bootstrap_socket_pair(&guard).unwrap()
    }

    #[cfg(unix)]
    fn pipe_pair() -> (OwnedFd, OwnedFd) {
        let mut descriptors = [-1; 2];
        assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
        for descriptor in descriptors { set_close_on_exec(descriptor).unwrap(); }
        unsafe { (OwnedFd::from_raw_fd(descriptors[0]), OwnedFd::from_raw_fd(descriptors[1])) }
    }

    #[cfg(unix)]
    #[test]
    fn scm_rights_round_trip_is_exact_and_close_on_exec() {
        let (sender, receiver) = socket_pair();
        let mut owned = Vec::new();
        for _ in 0..6 {
            let (read, _write) = pipe_pair();
            owned.push(read);
        }
        let raw: Vec<_> = owned.iter().map(AsRawFd::as_raw_fd).collect();
        send_bootstrap_packet(sender.as_raw_fd(), &frame(), &raw).unwrap();
        let packet = receive_bootstrap_packet(receiver.as_raw_fd()).unwrap();
        assert_eq!(packet.frame, frame());
        assert_eq!(packet.rights.len(), 6);
        for descriptor in packet.rights {
            let flags = unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_GETFD) };
            assert_ne!(flags & libc::FD_CLOEXEC, 0);
        }
    }

    #[cfg(unix)]
    #[test]
    fn stream_receiver_rejects_trailing_bytes_after_half_close() {
        let (sender, receiver) = socket_pair();
        let mut owned = Vec::new();
        for _ in 0..6 {
            let (read, _write) = pipe_pair();
            owned.push(read);
        }
        let raw: Vec<_> = owned.iter().map(AsRawFd::as_raw_fd).collect();
        let bytes = frame().encode().unwrap();
        let mut iov = libc::iovec { iov_base: bytes.as_ptr().cast_mut().cast(), iov_len: bytes.len() };
        let rights_bytes = raw.len() * mem::size_of::<RawFd>();
        let mut control = vec![0u8; unsafe { libc::CMSG_SPACE(rights_bytes as _) } as usize];
        let mut message: libc::msghdr = unsafe { mem::zeroed() };
        message.msg_iov = &mut iov;
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = control.len() as _;
        unsafe {
            let ancillary = libc::CMSG_FIRSTHDR(&message);
            (*ancillary).cmsg_level = libc::SOL_SOCKET;
            (*ancillary).cmsg_type = libc::SCM_RIGHTS;
            (*ancillary).cmsg_len = libc::CMSG_LEN(rights_bytes as _) as _;
            ptr::copy_nonoverlapping(raw.as_ptr().cast::<u8>(), libc::CMSG_DATA(ancillary), rights_bytes);
            assert_eq!(libc::sendmsg(sender.as_raw_fd(), &message, libc::MSG_NOSIGNAL), bytes.len() as isize);
        }
        let mut borrowed = mem::ManuallyDrop::new(unsafe { std::os::unix::net::UnixStream::from_raw_fd(sender.as_raw_fd()) });
        borrowed.write_all(b"x").unwrap();
        assert_eq!(unsafe { libc::shutdown(sender.as_raw_fd(), libc::SHUT_WR) }, 0);
        assert!(matches!(
            receive_bootstrap_packet(receiver.as_raw_fd()),
            Err(BootstrapError::Codec(CodecError::TrailingBytes))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn stream_receiver_uses_one_absolute_deadline_during_trickle() {
        let guard = process_launch_guard().unwrap();
        let (sender, receiver) = bootstrap_socket_pair(&guard).unwrap();
        drop(guard);
        let mut owned = Vec::new();
        for _ in 0..6 {
            let (read, _write) = pipe_pair();
            owned.push(read);
        }
        let raw: Vec<_> = owned.iter().map(AsRawFd::as_raw_fd).collect();
        let bytes = frame().encode().unwrap();
        let mut iov = libc::iovec { iov_base: bytes.as_ptr().cast_mut().cast(), iov_len: 1 };
        let rights_bytes = raw.len() * mem::size_of::<RawFd>();
        let mut control = vec![0u8; unsafe { libc::CMSG_SPACE(rights_bytes as _) } as usize];
        let mut message: libc::msghdr = unsafe { mem::zeroed() };
        message.msg_iov = &mut iov;
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = control.len() as _;
        unsafe {
            let ancillary = libc::CMSG_FIRSTHDR(&message);
            (*ancillary).cmsg_level = libc::SOL_SOCKET;
            (*ancillary).cmsg_type = libc::SCM_RIGHTS;
            (*ancillary).cmsg_len = libc::CMSG_LEN(rights_bytes as _) as _;
            ptr::copy_nonoverlapping(raw.as_ptr().cast::<u8>(), libc::CMSG_DATA(ancillary), rights_bytes);
            assert_eq!(libc::sendmsg(sender.as_raw_fd(), &message, libc::MSG_NOSIGNAL), 1);
        }
        let trickle = bytes[1..HEADER_SIZE].to_vec();
        let writer = std::thread::spawn(move || {
            let mut stream = std::os::unix::net::UnixStream::from(sender);
            for byte in trickle {
                std::thread::sleep(Duration::from_millis(15));
                if stream.write_all(&[byte]).is_err() { break; }
            }
        });
        let started = Instant::now();
        assert!(receive_bootstrap_packet_with_timeout(
            receiver.as_raw_fd(),
            Duration::from_millis(40),
        ).is_err());
        assert!(started.elapsed() < Duration::from_millis(150));
        drop(receiver);
        writer.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn bootstrap_socket_is_stream_and_close_on_exec() {
        let guard = process_launch_guard().unwrap();
        let (left, right) = bootstrap_socket_pair(&guard).unwrap();
        for descriptor in [left.as_raw_fd(), right.as_raw_fd()] {
            prove_close_on_exec(descriptor).unwrap();
            let mut socket_type = 0;
            let mut length = mem::size_of_val(&socket_type) as libc::socklen_t;
            assert_eq!(unsafe {
                libc::getsockopt(
                    descriptor,
                    libc::SOL_SOCKET,
                    libc::SO_TYPE,
                    (&mut socket_type as *mut libc::c_int).cast(),
                    &mut length,
                )
            }, 0);
            assert_eq!(socket_type, libc::SOCK_STREAM);
        }
        drop(guard);
    }

    #[cfg(unix)]
    #[test]
    fn scm_rights_sender_rejects_missing_or_extra_descriptors() {
        let (sender, _receiver) = socket_pair();
        let mut owned = Vec::new();
        for _ in 0..7 {
            let (read, _write) = pipe_pair();
            owned.push(read);
        }
        let raw: Vec<_> = owned.iter().map(AsRawFd::as_raw_fd).collect();
        assert!(matches!(
            send_bootstrap_packet(sender.as_raw_fd(), &frame(), &raw[..5]),
            Err(BootstrapError::WrongRightCount)
        ));
        assert!(matches!(
            send_bootstrap_packet(sender.as_raw_fd(), &frame(), &raw),
            Err(BootstrapError::WrongRightCount)
        ));
    }

    #[cfg(unix)]
    fn metadata_identity(metadata: &fs::Metadata) -> ExecutableIdentity {
        ExecutableIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
            owner: metadata.uid(),
            mode: metadata.mode() & 0o7777,
        }
    }

    #[cfg(unix)]
    fn bounded_wait(child: &mut Child) -> std::process::ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(status) = child.try_wait().unwrap() { return status; }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                let _ = child.wait();
                panic!("bootstrap child timed out");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "internal bootstrap subprocess entry"]
    fn bootstrap_subprocess_helper() {
        let installed = receive_and_install_bootstrap(libc::STDIN_FILENO).unwrap();
        let expected = bootstrap_slots(&installed.frame).unwrap();
        for slot in expected {
            validate_role(slot, slot, &installed.frame).unwrap();
            prove_close_on_exec(slot).unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "internal failing bootstrap subprocess entry"]
    fn bootstrap_failure_subprocess_helper() {
        assert!(receive_and_install_bootstrap(libc::STDIN_FILENO).is_err());
        for descriptor in CONTROL_FD..=RELEASE_ACK_FD {
            assert_eq!(unsafe { libc::fcntl(descriptor, libc::F_GETFD) }, -1);
        }
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "internal validated-helper lifecycle subprocess entry"]
    fn validated_helper_lifecycle_subprocess() {
        std::thread::sleep(Duration::from_secs(30));
    }

    #[cfg(unix)]
    #[test]
    fn dropping_validated_helper_terminates_and_reaps_its_child() {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args([
            "--ignored", "--exact",
            "process::tests::validated_helper_lifecycle_subprocess", "--nocapture",
        ]);
        let child = command.spawn().unwrap();
        let pid = child.id() as libc::pid_t;
        let reaper = helper_reaper().unwrap();
        let started = Instant::now();
        drop(ValidatedHelper { child: Some(child), reaper });
        assert!(started.elapsed() <= HELPER_CLEANUP_INLINE_TIMEOUT + Duration::from_secs(1));

        let mut status = 0;
        assert_eq!(unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ECHILD));
    }

    #[cfg(unix)]
    #[test]
    fn background_helper_reaper_owns_and_reaps_transferred_child() {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args([
            "--ignored", "--exact",
            "process::tests::validated_helper_lifecycle_subprocess", "--nocapture",
        ]);
        let child = command.spawn().unwrap();
        let pid = child.id() as libc::pid_t;
        helper_reaper().unwrap().send(child).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let probe = unsafe { libc::kill(pid, 0) };
            if probe == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                let mut status = 0;
                assert_eq!(unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) }, -1);
                assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ECHILD));
                break;
            }
            if Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            } else {
                panic!("helper reaper did not reap before deadline");
            }
        }
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "internal mutation-failure bootstrap subprocess entry"]
    fn bootstrap_mutation_failure_subprocess_helper() {
        let packet = receive_bootstrap_packet(libc::STDIN_FILENO).unwrap();
        let control = unsafe { OwnedFd::from_raw_fd(libc::STDIN_FILENO) };
        assert!(matches!(
            install_bootstrap_fixed_map_with_failure(control, packet, Some(EXEC_STATUS_FD)),
            Err(BootstrapError::BootstrapCorrupt)
        ));
        for descriptor in CONTROL_FD..=RELEASE_ACK_FD {
            assert_eq!(unsafe { libc::fcntl(descriptor, libc::F_GETFD) }, -1);
        }
    }

    #[cfg(unix)]
    #[test]
    fn tracked_fixed_sources_keep_original_ownership_until_cleanup() {
        let (read, _write) = pipe_pair();
        let owned_raw = unsafe { libc::fcntl(read.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 100) };
        assert!(owned_raw >= 100);
        let owner = unsafe { OwnedFd::from_raw_fd(owned_raw) };
        let tracked = TrackedSource::move_above_ceiling(owner, owned_raw).unwrap();
        assert_eq!(unsafe { libc::fcntl(owned_raw, libc::F_GETFD) }, libc::FD_CLOEXEC);
        assert!(tracked.source.as_raw_fd() > owned_raw);
        drop(tracked);
        assert_eq!(unsafe { libc::fcntl(owned_raw, libc::F_GETFD) }, -1);
    }

    #[cfg(unix)]
    #[test]
    fn fixed_slot_tracker_closes_each_synthetic_slot_once() {
        let mut tracker = FixedSlotTracker::with_range(100, 102, vec![100, 101], vec![100]);
        let mut closed = Vec::new();
        tracker.close_unused_with(&mut |fd| closed.push(fd));
        assert_eq!(closed, [102]);
        assert_eq!(tracker.states, [
            FixedSlotState::OwnedOriginal,
            FixedSlotState::NeverOwned,
            FixedSlotState::Closed,
        ]);
        tracker.mark_original_released(100);
        tracker.mark_installed(100);
        tracker.mark_installed(101);
        tracker.finish_failure_with(&mut |fd| closed.push(fd));
        assert_eq!(tracker.states, [FixedSlotState::Closed; 3]);
        assert_eq!(closed, [102, 100, 101]);
        tracker.finish_failure_with(&mut |fd| closed.push(fd));
        assert_eq!(tracker.states, [FixedSlotState::Closed; 3]);
        assert_eq!(closed, [102, 100, 101]);
    }

    #[cfg(unix)]
    fn assert_install_failure_closes_map(frame: &ControlFrame, rights: &[RawFd]) {
        let launch_guard = process_launch_guard().unwrap();
        let (parent_socket, child_socket) = bootstrap_socket_pair(&launch_guard).unwrap();
        let child_input = File::from(child_socket);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored", "--exact",
                "process::tests::bootstrap_failure_subprocess_helper", "--nocapture",
            ])
            .stdin(Stdio::from(child_input))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = command.spawn().unwrap();
        drop(launch_guard);
        send_bootstrap_packet(parent_socket.as_raw_fd(), frame, rights).unwrap();
        assert!(bounded_wait(&mut child).success());
    }

    #[cfg(unix)]
    #[test]
    fn helper_installs_exact_fixed_map_without_changing_parent_fds() {
        let temporary = tempfile::tempdir().unwrap();
        let target_path = temporary.path().join("fixture-target");
        fs::write(&target_path, b"generated fixture bytes").unwrap();
        fs::set_permissions(&target_path, fs::Permissions::from_mode(0o700)).unwrap();
        let target = File::open(&target_path).unwrap();
        let root = File::open(temporary.path()).unwrap();
        let log_path = temporary.path().join("agent.log");
        let log = OpenOptions::new().create_new(true).write(true).mode(0o600).open(&log_path).unwrap();
        let (release_read, release_write) = pipe_pair();
        let (exec_read, exec_write) = pipe_pair();
        let (ack_read, ack_write) = pipe_pair();
        let launch_guard = process_launch_guard().unwrap();
        let (parent_socket, child_socket) = bootstrap_socket_pair(&launch_guard).unwrap();
        let mut launch = frame();
        launch.target_identity = metadata_identity(&target.metadata().unwrap());
        launch.project_root_identity = Some(metadata_identity(&root.metadata().unwrap()));
        launch.agent_log_identity = Some(metadata_identity(&log.metadata().unwrap()));
        let rights = [
            release_read.as_raw_fd(),
            exec_write.as_raw_fd(),
            target.as_raw_fd(),
            root.as_raw_fd(),
            log.as_raw_fd(),
            ack_write.as_raw_fd(),
        ];
        let sentinel_raw = unsafe { libc::fcntl(root.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 200) };
        assert!(sentinel_raw >= 200);
        let sentinel = unsafe { File::from_raw_fd(sentinel_raw) };
        let sentinel_before = metadata_identity(&sentinel.metadata().unwrap());
        let executable = std::env::current_exe().unwrap();
        let child_input = File::from(child_socket);
        let mut command = Command::new(executable);
        command
            .args([
                "--ignored",
                "--exact",
                "process::tests::bootstrap_subprocess_helper",
                "--nocapture",
            ])
            .stdin(Stdio::from(child_input))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = command.spawn().unwrap();
        drop(launch_guard);
        send_bootstrap_packet(parent_socket.as_raw_fd(), &launch, &rights).unwrap();
        let status = bounded_wait(&mut child);
        assert!(status.success());
        assert_eq!(unsafe { libc::fcntl(sentinel.as_raw_fd(), libc::F_GETFD) }, libc::FD_CLOEXEC);
        assert_eq!(metadata_identity(&sentinel.metadata().unwrap()), sentinel_before);
        drop((release_write, exec_read, ack_read));
    }

    #[cfg(unix)]
    #[test]
    fn helper_mutation_failure_closes_the_fixed_map() {
        let temporary = tempfile::tempdir().unwrap();
        let target_path = temporary.path().join("fixture-target");
        fs::write(&target_path, b"generated fixture bytes").unwrap();
        fs::set_permissions(&target_path, fs::Permissions::from_mode(0o700)).unwrap();
        let target = File::open(&target_path).unwrap();
        let root = File::open(temporary.path()).unwrap();
        let log = OpenOptions::new().create_new(true).write(true).mode(0o600)
            .open(temporary.path().join("agent.log")).unwrap();
        let (release_read, _release_write) = pipe_pair();
        let (_exec_read, exec_write) = pipe_pair();
        let (_ack_read, ack_write) = pipe_pair();
        let mut launch = frame();
        launch.target_identity = metadata_identity(&target.metadata().unwrap());
        launch.project_root_identity = Some(metadata_identity(&root.metadata().unwrap()));
        launch.agent_log_identity = Some(metadata_identity(&log.metadata().unwrap()));
        let rights = [
            release_read.as_raw_fd(), exec_write.as_raw_fd(), target.as_raw_fd(),
            root.as_raw_fd(), log.as_raw_fd(), ack_write.as_raw_fd(),
        ];
        let launch_guard = process_launch_guard().unwrap();
        let (parent_socket, child_socket) = bootstrap_socket_pair(&launch_guard).unwrap();
        let child_input = File::from(child_socket);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored", "--exact",
                "process::tests::bootstrap_mutation_failure_subprocess_helper", "--nocapture",
            ])
            .stdin(Stdio::from(child_input))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = command.spawn().unwrap();
        drop(launch_guard);
        send_bootstrap_packet(parent_socket.as_raw_fd(), &launch, &rights).unwrap();
        assert!(bounded_wait(&mut child).success());
    }

    #[cfg(unix)]
    #[test]
    fn helper_rejects_identity_mismatch_before_install() {
        let temporary = tempfile::tempdir().unwrap();
        let target_path = temporary.path().join("fixture-target");
        fs::write(&target_path, b"generated fixture bytes").unwrap();
        fs::set_permissions(&target_path, fs::Permissions::from_mode(0o700)).unwrap();
        let target = File::open(&target_path).unwrap();
        let root = File::open(temporary.path()).unwrap();
        let log = OpenOptions::new().create_new(true).write(true).mode(0o600)
            .open(temporary.path().join("agent.log")).unwrap();
        let (release_read, _release_write) = pipe_pair();
        let (_exec_read, exec_write) = pipe_pair();
        let (_ack_read, ack_write) = pipe_pair();
        let mut launch = frame();
        launch.target_identity = metadata_identity(&target.metadata().unwrap());
        launch.target_identity.inode = launch.target_identity.inode.wrapping_add(1);
        launch.project_root_identity = Some(metadata_identity(&root.metadata().unwrap()));
        launch.agent_log_identity = Some(metadata_identity(&log.metadata().unwrap()));
        let rights = [release_read.as_raw_fd(), exec_write.as_raw_fd(), target.as_raw_fd(), root.as_raw_fd(), log.as_raw_fd(), ack_write.as_raw_fd()];
        assert_install_failure_closes_map(&launch, &rights);
    }

    #[cfg(unix)]
    #[test]
    fn helper_rejects_release_gate_eof_before_fixed_map_install() {
        let temporary = tempfile::tempdir().unwrap();
        let target_path = temporary.path().join("fixture-target");
        fs::write(&target_path, b"generated fixture bytes").unwrap();
        fs::set_permissions(&target_path, fs::Permissions::from_mode(0o700)).unwrap();
        let target = File::open(&target_path).unwrap();
        let root = File::open(temporary.path()).unwrap();
        let log = OpenOptions::new().create_new(true).write(true).mode(0o600)
            .open(temporary.path().join("agent.log")).unwrap();
        let (release_read, release_write) = pipe_pair();
        drop(release_write);
        let (_exec_read, exec_write) = pipe_pair();
        let (_ack_read, ack_write) = pipe_pair();
        let mut launch = frame();
        launch.target_identity = metadata_identity(&target.metadata().unwrap());
        launch.project_root_identity = Some(metadata_identity(&root.metadata().unwrap()));
        launch.agent_log_identity = Some(metadata_identity(&log.metadata().unwrap()));
        let rights = [release_read.as_raw_fd(), exec_write.as_raw_fd(), target.as_raw_fd(), root.as_raw_fd(), log.as_raw_fd(), ack_write.as_raw_fd()];
        assert_install_failure_closes_map(&launch, &rights);
    }

    #[cfg(unix)]
    #[test]
    fn helper_rejects_aliased_pipe_roles_before_install() {
        let temporary = tempfile::tempdir().unwrap();
        let target_path = temporary.path().join("fixture-target");
        fs::write(&target_path, b"generated fixture bytes").unwrap();
        fs::set_permissions(&target_path, fs::Permissions::from_mode(0o700)).unwrap();
        let target = File::open(&target_path).unwrap();
        let root = File::open(temporary.path()).unwrap();
        let log = OpenOptions::new().create_new(true).write(true).mode(0o600)
            .open(temporary.path().join("agent.log")).unwrap();
        let (release_read, release_write) = pipe_pair();
        let (_exec_read, exec_write) = pipe_pair();
        let mut launch = frame();
        launch.target_identity = metadata_identity(&target.metadata().unwrap());
        launch.project_root_identity = Some(metadata_identity(&root.metadata().unwrap()));
        launch.agent_log_identity = Some(metadata_identity(&log.metadata().unwrap()));
        let rights = [
            release_read.as_raw_fd(), exec_write.as_raw_fd(), target.as_raw_fd(),
            root.as_raw_fd(), log.as_raw_fd(), exec_write.as_raw_fd(),
        ];
        assert_install_failure_closes_map(&launch, &rights);
        drop(release_write);
    }

}
