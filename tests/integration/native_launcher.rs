#[cfg(unix)]
mod unix {
    use std::{
        ffi::OsString,
        fs::{self, File, OpenOptions},
        os::fd::{FromRawFd, IntoRawFd, OwnedFd},
        os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        path::Path,
    };

    use pueue_agent::{
        execution_policy::{ExecutableAnchor, ExecutableIdentity},
        process::{spawn_validated_helper, ControlFrame, LaunchFlags, LaunchMode},
    };
    use tempfile::tempdir;

    fn identity(metadata: &fs::Metadata) -> ExecutableIdentity {
        ExecutableIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
            owner: metadata.uid(),
            mode: metadata.mode() & 0o7777,
        }
    }

    fn pipe() -> (OwnedFd, OwnedFd) {
        let mut descriptors = [-1; 2];
        assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
        for descriptor in descriptors {
            let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
            assert!(flags >= 0);
            assert_eq!(unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags | libc::FD_CLOEXEC) }, 0);
        }
        unsafe {
            (
                OwnedFd::from_raw_fd(descriptors[0]),
                OwnedFd::from_raw_fd(descriptors[1]),
            )
        }
    }

    fn frame(target: &File, root: &File, log: &File) -> ControlFrame {
        ControlFrame {
            mode: LaunchMode::Agent,
            flags: LaunchFlags::PROCESS_GROUP
                .union(LaunchFlags::PROJECT_ROOT)
                .union(LaunchFlags::AGENT_LOG),
            argv: vec![OsString::from("generated-fixture")],
            environment: Vec::new(),
            cwd: None,
            target_identity: identity(&target.metadata().unwrap()),
            project_root_identity: Some(identity(&root.metadata().unwrap())),
            agent_log_identity: Some(identity(&log.metadata().unwrap())),
            pueue_config_identity: None,
        }
    }

    #[test]
    fn helper_is_hidden_and_rejects_extra_arguments() {
        let binary = env!("CARGO_BIN_EXE_pueue-agent");
        let help = std::process::Command::new(binary).arg("--help").output().unwrap();
        assert!(help.status.success());
        assert!(!String::from_utf8_lossy(&help.stdout).contains("internal-launch"));

        let extra = std::process::Command::new(binary)
            .args(["internal-launch", "unexpected"])
            .output()
            .unwrap();
        assert_eq!(extra.status.code(), Some(2));
    }

    #[test]
    fn validated_helper_sends_readiness_and_exits_without_target_execution() {
        let temporary = tempdir().unwrap();
        let target_path = temporary.path().join("generated-target");
        fs::write(&target_path, b"fixture bytes").unwrap();
        fs::set_permissions(&target_path, fs::Permissions::from_mode(0o700)).unwrap();
        let target = File::open(&target_path).unwrap();
        let root = File::open(temporary.path()).unwrap();
        let log = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(temporary.path().join("agent.log"))
            .unwrap();
        let launch = frame(&target, &root, &log);

        let (release_read, _release_write) = pipe();
        let (_exec_read, exec_write) = pipe();
        let (_ack_read, ack_write) = pipe();
        let rights = vec![
            release_read,
            exec_write,
            unsafe { OwnedFd::from_raw_fd(target.try_clone().unwrap().into_raw_fd()) },
            unsafe { OwnedFd::from_raw_fd(root.try_clone().unwrap().into_raw_fd()) },
            unsafe { OwnedFd::from_raw_fd(log.try_clone().unwrap().into_raw_fd()) },
            ack_write,
        ];

        let launcher = ExecutableAnchor::from_absolute(Path::new(env!("CARGO_BIN_EXE_pueue-agent")), &[])
            .unwrap();
        let mut helper = spawn_validated_helper(&launcher, launch, rights).unwrap();
        assert!(helper.wait().unwrap().success());
    }

    #[test]
    fn launcher_replacement_before_spawn_fails_closed() {
        let temporary = tempdir().unwrap();
        let launcher_path = temporary.path().join("trusted-launcher");
        fs::copy(env!("CARGO_BIN_EXE_pueue-agent"), &launcher_path).unwrap();
        fs::set_permissions(&launcher_path, fs::Permissions::from_mode(0o700)).unwrap();
        let launcher_path = fs::canonicalize(launcher_path).unwrap();
        let anchor = ExecutableAnchor::from_absolute(&launcher_path, &[]).unwrap();

        let replacement = temporary.path().join("replacement");
        fs::copy(env!("CARGO_BIN_EXE_pueue-agent"), &replacement).unwrap();
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o700)).unwrap();
        fs::rename(replacement, launcher_path).unwrap();

        let result = spawn_validated_helper(
            &anchor,
            ControlFrame {
                mode: LaunchMode::Pueue,
                flags: LaunchFlags::PROCESS_GROUP | LaunchFlags::PUEUE_CONFIG,
                argv: Vec::new(),
                environment: Vec::new(),
                cwd: None,
                target_identity: ExecutableIdentity { device: 0, inode: 0, owner: 0, mode: 0 },
                project_root_identity: None,
                agent_log_identity: None,
                pueue_config_identity: Some(ExecutableIdentity { device: 0, inode: 0, owner: 0, mode: 0 }),
            },
            Vec::new(),
        );
        assert!(matches!(result, Err(pueue_agent::process::ProcessLaunchError::LauncherRejected)));
    }
}

#[cfg(not(unix))]
#[test]
fn native_helper_is_not_available_on_non_unix() {}
