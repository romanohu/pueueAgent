//! The bounded control codec used by the native launch helper.
//!
//! This module intentionally contains no process creation.  The supervisor
//! and the hidden helper exchange one complete, length-delimited frame over a
//! pipe.  Keeping the codec separate from launch code makes it possible to
//! validate the untrusted byte stream before any target descriptor is used.

use std::{
    ffi::{OsStr, OsString},
    fmt,
    ops::BitOr,
};

use crate::execution_policy::ExecutableIdentity;

#[cfg(unix)]
use std::{
    io,
    mem,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::ffi::{OsStrExt, OsStringExt},
    },
    ptr,
};

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
pub(crate) enum BootstrapError {
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
const fn bootstrap_socket_type() -> libc::c_int {
    // Darwin does not implement AF_UNIX SOCK_SEQPACKET (EPROTONOSUPPORT).
    // A connected datagram socketpair retains the same one-record receive and
    // ancillary truncation guarantees used by this protocol.
    #[cfg(target_vendor = "apple")]
    { libc::SOCK_DGRAM }
    #[cfg(not(target_vendor = "apple"))]
    { libc::SOCK_SEQPACKET }
}

#[cfg(unix)]
pub(crate) fn bootstrap_socket_pair() -> Result<(OwnedFd, OwnedFd), BootstrapError> {
    let mut sockets = [-1; 2];
    if unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            bootstrap_socket_type(),
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
    set_close_on_exec(pair.0.as_raw_fd())?;
    set_close_on_exec(pair.1.as_raw_fd())?;
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
    let bytes = frame.encode()?;
    if rights.len() != bootstrap_right_slots(frame)?.len() {
        return Err(BootstrapError::WrongRightCount);
    }
    let mut iov = libc::iovec {
        iov_base: bytes.as_ptr().cast_mut().cast(),
        iov_len: bytes.len(),
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
        let sent = libc::sendmsg(socket, &message, libc::MSG_NOSIGNAL);
        if sent < 0 { return Err(BootstrapError::Io(io::Error::last_os_error())); }
        if sent as usize != bytes.len() { return Err(BootstrapError::TruncatedPacket); }
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn receive_bootstrap_packet(socket: RawFd) -> Result<BootstrapPacket, BootstrapError> {
    let mut bytes = vec![0u8; MAX_FRAME_SIZE];
    // One extra slot ensures an over-cardinality sender is observed rather
    // than silently accepted at the protocol maximum.
    let max_rights = 8usize;
    let ancillary_bytes = max_rights * mem::size_of::<RawFd>();
    let control_len = unsafe { libc::CMSG_SPACE(ancillary_bytes as _) } as usize;
    let mut control = vec![0u8; control_len];
    let mut iov = libc::iovec {
        iov_base: bytes.as_mut_ptr().cast(),
        iov_len: bytes.len(),
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
    let received = unsafe { libc::recvmsg(socket, &mut message, recv_flags) };
    if received < 0 { return Err(BootstrapError::Io(io::Error::last_os_error())); }
    if received == 0 { return Err(BootstrapError::EmptyPacket); }
    if message.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
        return Err(BootstrapError::TruncatedPacket);
    }
    bytes.truncate(received as usize);

    let mut rights = Vec::new();
    unsafe {
        let mut header = libc::CMSG_FIRSTHDR(&message);
        while !header.is_null() {
            if (*header).cmsg_level != libc::SOL_SOCKET || (*header).cmsg_type != libc::SCM_RIGHTS {
                return Err(BootstrapError::UnexpectedAncillary);
            }
            let base_len = libc::CMSG_LEN(0) as usize;
            let header_len = (*header).cmsg_len as usize;
            if header_len < base_len {
                return Err(BootstrapError::TruncatedPacket);
            }
            let data_len = header_len - base_len;
            if data_len == 0 || data_len % mem::size_of::<RawFd>() != 0 {
                return Err(BootstrapError::TruncatedPacket);
            }
            for index in 0..(data_len / mem::size_of::<RawFd>()) {
                let raw = ptr::read_unaligned(
                    libc::CMSG_DATA(header).cast::<RawFd>().add(index),
                );
                rights.push(OwnedFd::from_raw_fd(raw));
            }
            header = libc::CMSG_NXTHDR(&message, header);
        }
    }
    if rights.is_empty() { return Err(BootstrapError::MissingRights); }
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
    let frame = ControlFrame::decode(&bytes)?;
    if rights.len() != bootstrap_right_slots(&frame)?.len() {
        return Err(BootstrapError::WrongRightCount);
    }
    Ok(BootstrapPacket { frame, rights })
}

#[cfg(unix)]
pub(crate) fn receive_and_install_bootstrap(
    socket: RawFd,
) -> Result<InstalledBootstrap, BootstrapError> {
    let packet = receive_bootstrap_packet(socket)?;
    // The Command-owned bootstrap endpoint is normally stdin.  Duplicate it
    // before touching any fixed ABI descriptor so every dup2 source is above
    // the complete 3..10 destination range.
    let control = duplicate_above_fixed(socket)?;
    close_raw(socket);
    install_bootstrap_fixed_map(control, packet)
}

#[cfg(unix)]
fn install_bootstrap_fixed_map(
    control: OwnedFd,
    packet: BootstrapPacket,
) -> Result<InstalledBootstrap, BootstrapError> {
    let expected_slots = bootstrap_slots(&packet.frame)?;
    let mut moved = Vec::with_capacity(expected_slots.len());
    moved.push(ensure_above_fixed(control)?);
    for right in packet.rights {
        moved.push(ensure_above_fixed(right)?);
    }
    if moved.len() != expected_slots.len() {
        return Err(BootstrapError::WrongRightCount);
    }
    for (slot, descriptor) in expected_slots.iter().copied().zip(&moved) {
        validate_role(slot, descriptor.as_raw_fd(), &packet.frame)?;
    }

    for slot in CONTROL_FD..=RELEASE_ACK_FD {
        if !expected_slots.contains(&slot) { close_raw(slot); }
    }
    for (slot, descriptor) in expected_slots.iter().copied().zip(&moved) {
        if unsafe { libc::dup2(descriptor.as_raw_fd(), slot) } < 0 {
            return Err(BootstrapError::Io(io::Error::last_os_error()));
        }
        set_close_on_exec(slot)?;
    }
    drop(moved);
    for slot in &expected_slots {
        validate_role(*slot, *slot, &packet.frame)?;
        let flags = unsafe { libc::fcntl(*slot, libc::F_GETFD) };
        if flags < 0 || flags & libc::FD_CLOEXEC == 0 {
            return Err(BootstrapError::DescriptorNotCloseOnExec);
        }
    }
    Ok(InstalledBootstrap { frame: packet.frame })
}

#[cfg(unix)]
fn ensure_above_fixed(descriptor: OwnedFd) -> Result<OwnedFd, BootstrapError> {
    if descriptor.as_raw_fd() > RELEASE_ACK_FD { return Ok(descriptor); }
    let moved = duplicate_above_fixed(descriptor.as_raw_fd())?;
    drop(descriptor);
    Ok(moved)
}

#[cfg(unix)]
fn duplicate_above_fixed(raw: RawFd) -> Result<OwnedFd, BootstrapError> {
    let duplicated = unsafe { libc::fcntl(raw, libc::F_DUPFD_CLOEXEC, RELEASE_ACK_FD + 1) };
    if duplicated < 0 { return Err(BootstrapError::Io(io::Error::last_os_error())); }
    // SAFETY: F_DUPFD_CLOEXEC returned a new descriptor owned by this call.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
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
        value.environment = vec![(OsString::from("SECRET_NAME"), OsString::from("secret-value"))];
        value.cwd = Some(OsString::from("secret-cwd"));
        let debug = format!("{value:?}");
        assert!(debug.contains("argv_count"));
        assert!(debug.contains("environment_count"));
        assert!(!debug.contains("secret-argv"));
        assert!(!debug.contains("SECRET_NAME"));
        assert!(!debug.contains("secret-value"));
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
        bootstrap_socket_pair().unwrap()
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
    fn bounded_wait(child: libc::pid_t) -> libc::c_int {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let mut status = 0;
            let result = unsafe { libc::waitpid(child, &mut status, libc::WNOHANG) };
            if result == child { return status; }
            assert!(result >= 0, "waitpid: {}", io::Error::last_os_error());
            if Instant::now() >= deadline {
                unsafe {
                    libc::kill(child, libc::SIGKILL);
                    libc::waitpid(child, &mut status, 0);
                }
                panic!("bootstrap child timed out");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
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
        let (parent_socket, child_socket) = socket_pair();
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
        let child = unsafe { libc::fork() };
        assert!(child >= 0, "fork: {}", io::Error::last_os_error());
        if child == 0 {
            close_raw(parent_socket.as_raw_fd());
            for descriptor in rights { close_raw(descriptor); }
            close_raw(release_write.as_raw_fd());
            close_raw(exec_read.as_raw_fd());
            close_raw(ack_read.as_raw_fd());
            if unsafe { libc::dup2(child_socket.as_raw_fd(), libc::STDIN_FILENO) } < 0 {
                unsafe { libc::_exit(90) };
            }
            if child_socket.as_raw_fd() != libc::STDIN_FILENO { close_raw(child_socket.as_raw_fd()); }
            match receive_and_install_bootstrap(libc::STDIN_FILENO) {
                Ok(installed) if installed.frame == launch => unsafe { libc::_exit(0) },
                _ => unsafe { libc::_exit(91) },
            }
        }
        drop(child_socket);
        send_bootstrap_packet(parent_socket.as_raw_fd(), &launch, &rights).unwrap();
        let status = bounded_wait(child);
        assert!(libc::WIFEXITED(status));
        assert_eq!(libc::WEXITSTATUS(status), 0);
        assert_eq!(unsafe { libc::fcntl(sentinel.as_raw_fd(), libc::F_GETFD) }, libc::FD_CLOEXEC);
        assert_eq!(metadata_identity(&sentinel.metadata().unwrap()), sentinel_before);
        drop((release_write, exec_read, ack_read));
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
        let (sender, receiver) = socket_pair();
        let mut launch = frame();
        launch.target_identity = metadata_identity(&target.metadata().unwrap());
        launch.target_identity.inode = launch.target_identity.inode.wrapping_add(1);
        launch.project_root_identity = Some(metadata_identity(&root.metadata().unwrap()));
        launch.agent_log_identity = Some(metadata_identity(&log.metadata().unwrap()));
        let rights = [release_read.as_raw_fd(), exec_write.as_raw_fd(), target.as_raw_fd(), root.as_raw_fd(), log.as_raw_fd(), ack_write.as_raw_fd()];
        send_bootstrap_packet(sender.as_raw_fd(), &launch, &rights).unwrap();
        let packet = receive_bootstrap_packet(receiver.as_raw_fd()).unwrap();
        let control = duplicate_above_fixed(receiver.as_raw_fd()).unwrap();
        assert!(matches!(
            install_bootstrap_fixed_map(control, packet),
            Err(BootstrapError::IdentityMismatch)
        ));
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
        let (sender, receiver) = socket_pair();
        let mut launch = frame();
        launch.target_identity = metadata_identity(&target.metadata().unwrap());
        launch.project_root_identity = Some(metadata_identity(&root.metadata().unwrap()));
        launch.agent_log_identity = Some(metadata_identity(&log.metadata().unwrap()));
        let rights = [release_read.as_raw_fd(), exec_write.as_raw_fd(), target.as_raw_fd(), root.as_raw_fd(), log.as_raw_fd(), ack_write.as_raw_fd()];
        send_bootstrap_packet(sender.as_raw_fd(), &launch, &rights).unwrap();
        let packet = receive_bootstrap_packet(receiver.as_raw_fd()).unwrap();
        let control = duplicate_above_fixed(receiver.as_raw_fd()).unwrap();
        assert!(matches!(
            install_bootstrap_fixed_map(control, packet),
            Err(BootstrapError::GateClosed)
        ));
    }

}
