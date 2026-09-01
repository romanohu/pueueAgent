#[cfg(unix)]
mod unix {
    use std::{
        ffi::OsString,
        fs::{self, File, OpenOptions},
        io::Write,
        os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd},
        os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        os::unix::net::UnixStream,
        path::{Path, PathBuf},
        process::{Child, Command, ExitStatus, Stdio},
        time::{Duration, Instant},
    };

    use pueue_agent::{
        environment::{PrivateRunTemp, SanitizedEnvironment},
        execution_policy::{ExecutableAnchor, ExecutableIdentity, PueueConfigAnchor, VerifiedWorkingDirectory},
        project_logs::LogFileIdentity,
        process::{
            spawn_validated_helper, spawn_verified_command, spawn_verified_command_in_private_temp,
            BootstrapError, ControlFrame,
            LaunchFlags, LaunchMode, ProcessGroupRequirement, ProcessLaunchError,
            VerifiedChildIo, VerifiedCommandSpec, terminate_process_group,
        },
    };
    use tempfile::tempdir;

    // The production protocol deadline is five seconds. Keep the integration
    // allowance slightly above it so concurrent filesystem-heavy test setup
    // does not turn a bounded failure assertion into a scheduling race.
    const MAX_HELPER_WAIT: Duration = Duration::from_secs(6);

    fn secure_directory(path: &Path) {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

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

    fn copy_launcher(directory: &Path) -> (ExecutableAnchor, PathBuf) {
        assert!(!directory.starts_with(env!("CARGO_MANIFEST_DIR")));
        secure_directory(directory);
        let launcher_path = directory.join("trusted-pueue-agent");
        fs::copy(env!("CARGO_BIN_EXE_pueue-agent"), &launcher_path).unwrap();
        fs::set_permissions(&launcher_path, fs::Permissions::from_mode(0o700)).unwrap();
        let launcher_path = fs::canonicalize(launcher_path).unwrap();
        let anchor = ExecutableAnchor::from_absolute(&launcher_path, &[]).unwrap();
        (anchor, launcher_path)
    }

    fn compile_generated_fixture(directory: &Path) -> ExecutableAnchor {
        let source = directory.join("generated-target.rs");
        let executable = directory.join("generated-target");
        fs::write(
            &source,
            r#"use std::{env, fs};
fn main() {
    let output = env::args_os().nth(1).expect("output argument");
    fs::write(output, b"started").expect("write started marker");
}"#,
        )
        .unwrap();
        let output = Command::new("rustc")
            .args(["--edition=2021", "-o"])
            .arg(&executable)
            .arg(&source)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "generated fixture compilation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        ExecutableAnchor::from_absolute(&fs::canonicalize(executable).unwrap(), &[]).unwrap()
    }

    fn compile_generated_private_temp_fixture(directory: &Path) -> ExecutableAnchor {
        let source = directory.join("generated-private-temp-target.rs");
        let executable = directory.join("generated-private-temp-target");
        fs::write(
            &source,
            r#"use std::{env, fs, os::unix::fs::{MetadataExt, PermissionsExt}, path::PathBuf};
extern "C" { fn umask(mask: u32) -> u32; }

fn main() {
    let private_temp = PathBuf::from(env::args_os().nth(1).expect("private temp argument"));
    let private_temp_metadata = fs::metadata(&private_temp).expect("private temp path");
    let descriptor = fs::File::open("/dev/fd/11").expect("private temp target descriptor");
    let descriptor_metadata = descriptor.metadata().expect("private temp target descriptor metadata");
    assert!(descriptor_metadata.is_dir(), "private temp target descriptor is not visible");
    assert_eq!(descriptor_metadata.dev(), private_temp_metadata.dev());
    assert_eq!(descriptor_metadata.ino(), private_temp_metadata.ino());
    for descriptor in 3..=10 {
        // macOS may reserve fd 3 for a runtime-owned directory after exec;
        // the remaining protocol descriptors are still required to be closed.
        #[cfg(target_os = "macos")]
        if descriptor == 3 {
            continue;
        }
        let descriptor_path = format!("/dev/fd/{descriptor}");
        let descriptor_file = fs::File::open(&descriptor_path);
        assert!(
            descriptor_file.is_err(),
            "protocol descriptor {descriptor} leaked into target"
        );
    }
    let created = private_temp.join("target-created");
    fs::create_dir(&created).expect("create private temp directory");
    let mode = fs::metadata(&created)
        .expect("stat private temp directory")
        .permissions()
        .mode()
        & 0o777;
    let inherited_umask = unsafe {
        let current = umask(0);
        umask(current);
        current
    };
    fs::write(
        private_temp.join("target-report"),
        format!("{mode:o},{inherited_umask:o}"),
    )
    .expect("write private temp report");
}"#,
        )
        .unwrap();
        let output = Command::new("rustc")
            .args(["--edition=2021", "-o"])
            .arg(&executable)
            .arg(&source)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "generated private-temp fixture compilation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        ExecutableAnchor::from_absolute(&fs::canonicalize(executable).unwrap(), &[]).unwrap()
    }

    fn compile_generated_pueue_fixture(directory: &Path) -> ExecutableAnchor {
        let source = directory.join("generated-pueue-target.rs");
        let executable = directory.join("generated-pueue-target");
        fs::write(
            &source,
            r#"use std::{env, fs};
fn main() {
    let output = env::args_os().nth(1).expect("output argument");
    let config = fs::read("/dev/fd/9").expect("verified pueue config fd");
    let other_protocol_fd_is_closed = fs::metadata("/dev/fd/4").is_err();
    assert!(other_protocol_fd_is_closed, "release fd leaked into target");
    assert_eq!(config, b"pueue-config");
    fs::write(output, b"pueue-fd9-ok").expect("write result");
}"#,
        )
        .unwrap();
        let output = Command::new("rustc")
            .args(["--edition=2021", "-o"])
            .arg(&executable)
            .arg(&source)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "generated Pueue fixture compilation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        ExecutableAnchor::from_absolute(&fs::canonicalize(executable).unwrap(), &[]).unwrap()
    }

    fn compile_generated_group_fixture(directory: &Path) -> ExecutableAnchor {
        let source = directory.join("generated-group-target.rs");
        let executable = directory.join("generated-group-target");
        fs::write(
            &source,
            r#"use std::{env, fs, process::Command, thread, time::Duration};
extern "C" { fn signal(signum: i32, handler: usize) -> usize; }
fn main() {
    let mut args = env::args();
    let _program = args.next();
    let mode = args.next().expect("mode");
    let pid_path = args.next().expect("pid path");
    if mode == "descendant" {
        unsafe { let _ = signal(15, 1); }
        fs::write(&pid_path, std::process::id().to_string()).expect("write descendant pid");
        loop { thread::sleep(Duration::from_millis(25)); }
    }
    let child = Command::new(env::current_exe().expect("current executable"))
        .args(["descendant", &pid_path])
        .spawn()
        .expect("spawn descendant");
    fs::write(&pid_path, child.id().to_string()).expect("write child pid");
    loop { thread::sleep(Duration::from_millis(25)); }
}"#,
        )
        .unwrap();
        let output = Command::new("rustc")
            .args(["--edition=2021", "-o"])
            .arg(&executable)
            .arg(&source)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "generated group fixture compilation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        ExecutableAnchor::from_absolute(&fs::canonicalize(executable).unwrap(), &[]).unwrap()
    }

    fn bounded_wait(child: &mut Child) -> ExitStatus {
        let deadline = Instant::now() + MAX_HELPER_WAIT;
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                return status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("native bootstrap helper did not exit before its test deadline");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn spawn_internal_with_socket(launcher: &Path) -> (Child, UnixStream) {
        let (parent, child) = UnixStream::pair().unwrap();
        for descriptor in [parent.as_raw_fd(), child.as_raw_fd()] {
            let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
            assert!(flags >= 0);
            assert_eq!(
                unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags | libc::FD_CLOEXEC) },
                0,
            );
        }
        let child = Command::new(launcher)
            .arg("internal-launch")
            .env_clear()
            .stdin(Stdio::from(OwnedFd::from(child)))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        (child, parent)
    }

    fn frame(target: &File, root: &File, log: &File, private_temp: &File) -> ControlFrame {
        ControlFrame {
            mode: LaunchMode::Agent,
            flags: LaunchFlags::PROCESS_GROUP
                .union(LaunchFlags::PROJECT_ROOT)
                .union(LaunchFlags::WORKING_DIRECTORY)
                .union(LaunchFlags::AGENT_LOG)
                .union(LaunchFlags::PRIVATE_TEMP),
            argv: vec![OsString::from("generated-fixture"), OsString::from("payload-sentinel")],
            environment: vec![(OsString::from("FIXTURE_NAME"), OsString::from("fixture-value"))],
            cwd: None,
            working_directory_identity: Some(identity(&root.metadata().unwrap())),
            target_identity: identity(&target.metadata().unwrap()),
            project_root_identity: Some(identity(&root.metadata().unwrap())),
            agent_log_identity: Some(identity(&log.metadata().unwrap())),
            pueue_config_identity: None,
            target_path: None,
            private_temp_identity: Some(identity(&private_temp.metadata().unwrap())),
            git_admin_identity: None,
            git_common_identity: None,
            git_worktree_parent_identity: None,
        }
    }

    fn rights(target: &File, root: &File, log: &File, private_temp: &File) -> (Vec<OwnedFd>, Vec<OwnedFd>) {
        let (release_read, release_write) = pipe();
        let (exec_read, exec_write) = pipe();
        let (ack_read, ack_write) = pipe();
        (
            vec![
                release_read,
                exec_write,
                unsafe { OwnedFd::from_raw_fd(target.try_clone().unwrap().into_raw_fd()) },
                unsafe { OwnedFd::from_raw_fd(root.try_clone().unwrap().into_raw_fd()) },
                unsafe { OwnedFd::from_raw_fd(root.try_clone().unwrap().into_raw_fd()) },
                unsafe { OwnedFd::from_raw_fd(log.try_clone().unwrap().into_raw_fd()) },
                ack_write,
                unsafe { OwnedFd::from_raw_fd(private_temp.try_clone().unwrap().into_raw_fd()) },
            ],
            vec![release_write, exec_read, ack_read],
        )
    }

    fn files(directory: &Path) -> (File, File, File, File) {
        secure_directory(directory);
        let target_path = directory.join("generated-target");
        fs::write(&target_path, b"generated fixture bytes").unwrap();
        fs::set_permissions(&target_path, fs::Permissions::from_mode(0o700)).unwrap();
        let target = File::open(&target_path).unwrap();
        let root = File::open(directory).unwrap();
        let log = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(directory.join("agent.log"))
            .unwrap();
        let private_temp_path = directory.join("private-temp");
        fs::create_dir(&private_temp_path).unwrap();
        fs::set_permissions(&private_temp_path, fs::Permissions::from_mode(0o700)).unwrap();
        let private_temp = File::open(private_temp_path).unwrap();
        (target, root, log, private_temp)
    }

    fn agent_log_io(directory: &Path) -> VerifiedChildIo {
        let path = directory.join("agent-test.log");
        let stdout = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        let stderr = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&path)
            .unwrap();
        let identity = LogFileIdentity::from_open_descriptor(&stdout).unwrap();
        VerifiedChildIo::AgentLog {
            stdout,
            stderr,
            identity,
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
    fn helper_eof_and_malformed_bootstrap_fail_within_deadline() {
        let temporary = tempdir().unwrap();
        let (_, launcher_path) = copy_launcher(temporary.path());

        let (mut eof_child, eof_parent) = spawn_internal_with_socket(&launcher_path);
        drop(eof_parent);
        assert!(!bounded_wait(&mut eof_child).success());

        let (mut malformed_child, mut malformed_parent) = spawn_internal_with_socket(&launcher_path);
        malformed_parent.write_all(b"not-a-control-frame").unwrap();
        malformed_parent
            .shutdown(std::net::Shutdown::Write)
            .unwrap();
        assert!(!bounded_wait(&mut malformed_child).success());
    }

    #[test]
    fn validated_helper_sends_readiness_and_exits_without_target_execution() {
        let temporary = tempdir().unwrap();
        let (target, root, log, private_temp) = files(temporary.path());
        let launch = frame(&target, &root, &log, &private_temp);
        let (rights, _keepers) = rights(&target, &root, &log, &private_temp);
        let (launcher, _) = copy_launcher(temporary.path());
        let mut helper = spawn_validated_helper(&launcher, launch, rights).unwrap();
        assert!(helper.wait().unwrap().success());
    }

    #[test]
    fn parent_right_without_cloexec_is_prepared_before_helper_spawn() {
        let temporary = tempdir().unwrap();
        let (target, root, log, private_temp) = files(temporary.path());
        let launch = frame(&target, &root, &log, &private_temp);
        let (rights, _keepers) = rights(&target, &root, &log, &private_temp);
        let raw = rights[2].as_raw_fd();
        let flags = unsafe { libc::fcntl(raw, libc::F_GETFD) };
        assert!(flags >= 0);
        assert_eq!(unsafe { libc::fcntl(raw, libc::F_SETFD, flags & !libc::FD_CLOEXEC) }, 0);
        assert_eq!(unsafe { libc::fcntl(raw, libc::F_GETFD) } & libc::FD_CLOEXEC, 0);

        let (launcher, _) = copy_launcher(temporary.path());
        let mut helper = spawn_validated_helper(&launcher, launch, rights).unwrap();
        assert!(helper.wait().unwrap().success());
    }

    #[test]
    fn identity_and_release_gate_failures_are_bounded_and_redacted() {
        let temporary = tempdir().unwrap();
        let (target, root, log, private_temp) = files(temporary.path());
        let (launcher, _) = copy_launcher(temporary.path());

        let mut identity_mismatch = frame(&target, &root, &log, &private_temp);
        identity_mismatch.target_identity.inode = identity_mismatch.target_identity.inode.wrapping_add(1);
        let (identity_rights, _identity_keepers) = rights(&target, &root, &log, &private_temp);
        let started = Instant::now();
        let identity_error = match spawn_validated_helper(&launcher, identity_mismatch, identity_rights) {
            Ok(_) => panic!("identity mismatch unexpectedly produced readiness"),
            Err(error) => error,
        };
        assert!(started.elapsed() < MAX_HELPER_WAIT);
        assert!(matches!(
            identity_error,
            ProcessLaunchError::HelperFailure(
                pueue_agent::process::HelperFailureKind::Security
            )
        ));
        let rendered = identity_error.to_string();
        assert!(!rendered.contains("payload-sentinel"));
        assert!(!rendered.contains("fixture-value"));
        assert!(!rendered.contains(temporary.path().to_string_lossy().as_ref()));

        let mut private_temp_mismatch = frame(&target, &root, &log, &private_temp);
        let mut wrong_private_temp_identity = private_temp_mismatch.private_temp_identity.unwrap();
        wrong_private_temp_identity.inode = wrong_private_temp_identity.inode.wrapping_add(1);
        private_temp_mismatch.private_temp_identity = Some(wrong_private_temp_identity);
        let (private_temp_rights, _private_temp_keepers) =
            rights(&target, &root, &log, &private_temp);
        assert!(matches!(
            spawn_validated_helper(&launcher, private_temp_mismatch, private_temp_rights),
            Err(ProcessLaunchError::HelperFailure(
                pueue_agent::process::HelperFailureKind::Security
            ))
        ));

        let (mut wrong_type_rights, _wrong_type_keepers) =
            rights(&target, &root, &log, &private_temp);
        wrong_type_rights.pop();
        wrong_type_rights.push(unsafe {
            OwnedFd::from_raw_fd(target.try_clone().unwrap().into_raw_fd())
        });
        assert!(matches!(
            spawn_validated_helper(
                &launcher,
                frame(&target, &root, &log, &private_temp),
                wrong_type_rights,
            ),
            Err(ProcessLaunchError::HelperFailure(
                pueue_agent::process::HelperFailureKind::Security
            ))
        ));

        let (mut duplicate_role_rights, _duplicate_role_keepers) =
            rights(&target, &root, &log, &private_temp);
        duplicate_role_rights[3] = unsafe {
            OwnedFd::from_raw_fd(private_temp.try_clone().unwrap().into_raw_fd())
        };
        assert!(matches!(
            spawn_validated_helper(
                &launcher,
                frame(&target, &root, &log, &private_temp),
                duplicate_role_rights,
            ),
            Err(ProcessLaunchError::HelperFailure(
                pueue_agent::process::HelperFailureKind::Security
            ))
        ));

        let (mut reordered_rights, _reordered_keepers) =
            rights(&target, &root, &log, &private_temp);
        reordered_rights.swap(3, 6);
        assert!(matches!(
            spawn_validated_helper(
                &launcher,
                frame(&target, &root, &log, &private_temp),
                reordered_rights,
            ),
            Err(ProcessLaunchError::HelperFailure(
                pueue_agent::process::HelperFailureKind::Security
            ))
        ));

        let launch = frame(&target, &root, &log, &private_temp);
        let (release_read, release_write) = pipe();
        drop(release_write);
        let (exec_read, exec_write) = pipe();
        let (ack_read, ack_write) = pipe();
        let gate_rights = vec![
            release_read,
            exec_write,
            unsafe { OwnedFd::from_raw_fd(target.try_clone().unwrap().into_raw_fd()) },
            unsafe { OwnedFd::from_raw_fd(root.try_clone().unwrap().into_raw_fd()) },
            unsafe { OwnedFd::from_raw_fd(root.try_clone().unwrap().into_raw_fd()) },
            unsafe { OwnedFd::from_raw_fd(log.try_clone().unwrap().into_raw_fd()) },
            ack_write,
            unsafe { OwnedFd::from_raw_fd(private_temp.try_clone().unwrap().into_raw_fd()) },
        ];
        let started = Instant::now();
        let gate_error = match spawn_validated_helper(&launcher, launch, gate_rights) {
            Ok(_) => panic!("closed release gate unexpectedly produced readiness"),
            Err(error) => error,
        };
        assert!(started.elapsed() < MAX_HELPER_WAIT);
        assert!(matches!(
            gate_error,
            ProcessLaunchError::HelperFailure(
                pueue_agent::process::HelperFailureKind::Security
            )
        ));
        drop((exec_read, ack_read));
    }

    #[test]
    fn malformed_parent_request_terminates_helper_and_preserves_parent_sentinel() {
        let temporary = tempdir().unwrap();
        let (target, root, log, private_temp) = files(temporary.path());
        let (launcher, _) = copy_launcher(temporary.path());
        let launch = frame(&target, &root, &log, &private_temp);

        let started = Instant::now();
        let malformed_error = match spawn_validated_helper(&launcher, launch.clone(), Vec::new()) {
            Ok(_) => panic!("missing descriptor rights unexpectedly produced readiness"),
            Err(error) => error,
        };
        assert!(started.elapsed() < MAX_HELPER_WAIT);
        assert!(matches!(
            malformed_error,
            ProcessLaunchError::Bootstrap(BootstrapError::WrongRightCount)
        ));

        let sentinel_raw = unsafe { libc::fcntl(root.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 200) };
        assert!(sentinel_raw >= 200);
        let sentinel = unsafe { File::from_raw_fd(sentinel_raw) };
        let sentinel_identity = identity(&sentinel.metadata().unwrap());
        let (valid_rights, _keepers) = rights(&target, &root, &log, &private_temp);
        let mut helper = spawn_validated_helper(&launcher, launch, valid_rights).unwrap();
        assert!(helper.wait().unwrap().success());
        assert_eq!(identity(&sentinel.metadata().unwrap()), sentinel_identity);
        assert_eq!(
            unsafe { libc::fcntl(sentinel.as_raw_fd(), libc::F_GETFD) },
            libc::FD_CLOEXEC,
        );
    }

    #[test]
    fn launcher_replacement_before_spawn_fails_closed() {
        let temporary = tempdir().unwrap();
        let (anchor, launcher_path) = copy_launcher(temporary.path());

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
                working_directory_identity: None,
                target_identity: ExecutableIdentity { device: 0, inode: 0, owner: 0, mode: 0 },
                project_root_identity: None,
                agent_log_identity: None,
                pueue_config_identity: Some(ExecutableIdentity { device: 0, inode: 0, owner: 0, mode: 0 }),
                target_path: None,
                private_temp_identity: None,
                git_admin_identity: None,
                git_common_identity: None,
                git_worktree_parent_identity: None,
            },
            Vec::new(),
        );
        assert!(matches!(result, Err(pueue_agent::process::ProcessLaunchError::LauncherRejected)));
    }

    #[tokio::test]
    async fn code_change_valid_nested_cwd_is_descriptor_verified() {
        let temporary = tempdir().unwrap();
        secure_directory(temporary.path());
        let base = fs::canonicalize(temporary.path()).unwrap();
        let nested = base.join("nested");
        fs::create_dir(&nested).unwrap();
        secure_directory(&nested);
        let source = base.join("cwd-target.rs");
        let executable = base.join("cwd-target");
        fs::write(
            &source,
            r#"use std::{env, fs};
fn main() {
    let output = env::args_os().nth(1).expect("output path");
    let cwd = env::current_dir().expect("current directory");
    fs::write(output, cwd.to_string_lossy().as_bytes()).expect("write cwd");
}"#,
        )
        .unwrap();
        let compile = Command::new("rustc")
            .args(["--edition=2021", "-o"])
            .arg(&executable)
            .arg(&source)
            .output()
            .unwrap();
        assert!(
            compile.status.success(),
            "cwd fixture compilation failed: {}",
            String::from_utf8_lossy(&compile.stderr)
        );
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        let root = pueue_agent::execution_policy::ProjectRootAnchor::resolve(&base).unwrap();
        let verified = root.verify_identity().unwrap();
        let working =
            VerifiedWorkingDirectory::open_descendant(&verified, Path::new("nested")).unwrap();
        assert_eq!(working.canonical_path(), fs::canonicalize(&nested).unwrap());
        let launcher = copy_launcher(&base).0;
        let target = ExecutableAnchor::from_absolute(&executable, &[]).unwrap();
        let private_temp = PrivateRunTemp::create(&verified, 91).unwrap();
        let output = base.join("cwd.txt");
        let mut child = pueue_agent::process::spawn_verified_command_in_private_temp(
            VerifiedCommandSpec {
                launcher,
                executable: target,
                argv: vec![OsString::from("cwd-target"), output.as_os_str().to_os_string()],
                working_directory: Some(working),
                environment: SanitizedEnvironment::default(),
                process_group: ProcessGroupRequirement::Required,
                start_suspended: true,
                project_root: Some(verified),
                pueue_config: None,
                git_directories: None,
                child_io: agent_log_io(&base),
            },
            &private_temp,
        )
        .unwrap();
        child.release().unwrap();
        child.confirm_exec().await.unwrap();
        child.wait_for_release_ack().await.unwrap();
        assert!(child.wait().await.unwrap().success());
        assert_eq!(fs::canonicalize(fs::read_to_string(output).unwrap()).unwrap(), nested);
    }

    #[test]
    fn code_change_sibling_cwd_is_rejected() {
        let temporary = tempdir().unwrap();
        secure_directory(temporary.path());
        let base = fs::canonicalize(temporary.path()).unwrap();
        let sibling_parent = tempdir().unwrap();
        let sibling = sibling_parent.path().join("sibling-cwd");
        fs::create_dir(&sibling).unwrap();
        let root = pueue_agent::execution_policy::ProjectRootAnchor::resolve(&base).unwrap();
        let verified = root.verify_identity().unwrap();
        assert!(VerifiedWorkingDirectory::open_descendant(&verified, &sibling).is_err());
    }

    #[test]
    fn code_change_symlink_descendant_is_rejected() {
        let temporary = tempdir().unwrap();
        secure_directory(temporary.path());
        let base = fs::canonicalize(temporary.path()).unwrap();
        let target = base.join("target");
        let link = base.join("link");
        fs::create_dir(&target).unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let root = pueue_agent::execution_policy::ProjectRootAnchor::resolve(&base).unwrap();
        let verified = root.verify_identity().unwrap();
        assert!(VerifiedWorkingDirectory::open_descendant(&verified, Path::new("link")).is_err());
    }

    #[test]
    fn code_change_replaced_candidate_root_is_rejected() {
        let temporary = tempdir().unwrap();
        secure_directory(temporary.path());
        let base = fs::canonicalize(temporary.path()).unwrap();
        let candidate = base.join("candidate");
        fs::create_dir(&candidate).unwrap();
        secure_directory(&candidate);
        let anchor = pueue_agent::execution_policy::ProjectRootAnchor::resolve(&candidate).unwrap();
        let replacement = base.join("replacement");
        fs::create_dir(&replacement).unwrap();
        fs::rename(&candidate, base.join("candidate-old")).unwrap();
        fs::rename(&replacement, &candidate).unwrap();
        assert!(anchor.verify_identity().is_err());
    }

    #[test]
    fn code_change_cwd_descriptor_matrix() {
        code_change_sibling_cwd_is_rejected();
        code_change_symlink_descendant_is_rejected();
        code_change_replaced_candidate_root_is_rejected();
    }

    #[tokio::test]
    async fn verified_target_stays_blocked_until_release_then_executes_and_is_acked() {
        let temporary = tempdir().unwrap();
        let (launcher, _) = copy_launcher(temporary.path());
        let target = compile_generated_fixture(temporary.path());
        let root_anchor = pueue_agent::execution_policy::ProjectRootAnchor::resolve(
            &fs::canonicalize(temporary.path()).unwrap(),
        )
        .unwrap();
        let started = temporary.path().join("target-started");
        let verified_root = root_anchor.verify_identity().unwrap();
        let private_temp = PrivateRunTemp::create(&verified_root, 1).unwrap();
        let working_directory = VerifiedWorkingDirectory::root(&verified_root).unwrap();
        let child_io = agent_log_io(temporary.path());
        let mut child = spawn_verified_command_in_private_temp(VerifiedCommandSpec {
            launcher,
            executable: target,
            argv: vec![
                OsString::from("generated-target"),
                started.as_os_str().to_os_string(),
            ],
            working_directory: Some(working_directory),
            environment: SanitizedEnvironment::default(),
            process_group: ProcessGroupRequirement::Required,
            start_suspended: true,
            project_root: Some(verified_root),
            pueue_config: None,
            git_directories: None,
            child_io,
        }, &private_temp)
        .unwrap();

        assert!(!started.exists(), "target executed before release");
        child.release().unwrap();
        assert!(child.wait_for_release_ack().await.is_err());
        child.confirm_exec().await.unwrap();
        child.wait_for_release_ack().await.unwrap();
        let status = child.wait().await.unwrap();
        assert!(status.success());
        assert_eq!(fs::read(&started).unwrap(), b"started");
    }

    #[tokio::test]
    async fn verified_target_private_temp_creation_inherits_private_umask() {
        let temporary = tempdir().unwrap();
        let (launcher, _) = copy_launcher(temporary.path());
        let target = compile_generated_private_temp_fixture(temporary.path());
        let root_anchor = pueue_agent::execution_policy::ProjectRootAnchor::resolve(
            &fs::canonicalize(temporary.path()).unwrap(),
        )
        .unwrap();

        let verified_root = root_anchor.verify_identity().unwrap();
        let private_temp = PrivateRunTemp::create(&verified_root, 2).unwrap();
        let working_directory = VerifiedWorkingDirectory::root(&verified_root).unwrap();
        let child_io = agent_log_io(temporary.path());
        let mut child = spawn_verified_command_in_private_temp(VerifiedCommandSpec {
            launcher,
            executable: target,
            argv: vec![
                OsString::from("generated-private-temp-target"),
                private_temp.path().as_os_str().to_os_string(),
            ],
            working_directory: Some(working_directory),
            environment: SanitizedEnvironment::default(),
            process_group: ProcessGroupRequirement::Required,
            start_suspended: true,
            project_root: Some(verified_root),
            pueue_config: None,
            git_directories: None,
            child_io,
        }, &private_temp)
        .unwrap();

        child.release().unwrap();
        child.confirm_exec().await.unwrap();
        child.wait_for_release_ack().await.unwrap();
        assert!(child.wait().await.unwrap().success());

        let created = private_temp.path().join("target-created");
        assert_eq!(
            fs::metadata(&created).unwrap().permissions().mode() & 0o777,
            0o700,
        );
        assert_eq!(
            fs::read_to_string(private_temp.path().join("target-report")).unwrap(),
            "700,77",
        );
    }

    #[tokio::test]
    async fn pueue_target_receives_verified_config_at_fd9_without_protocol_fd_leaks() {
        let temporary = tempdir().unwrap();
        let (launcher, _) = copy_launcher(temporary.path());
        let target = compile_generated_pueue_fixture(temporary.path());
        let config_path = temporary.path().join("pueue.yml");
        fs::write(&config_path, b"pueue-config").unwrap();
        fs::set_permissions(&config_path, fs::Permissions::from_mode(0o600)).unwrap();
        let config_anchor = PueueConfigAnchor::from_absolute(
            &fs::canonicalize(&config_path).unwrap(),
            &[],
        )
        .unwrap();
        let verified_config = config_anchor.verify_identity(&[]).unwrap();
        let result_path = temporary.path().join("pueue-result");

        let mut child = spawn_verified_command(VerifiedCommandSpec {
            launcher,
            executable: target,
            argv: vec![
                OsString::from("generated-pueue-target"),
                result_path.as_os_str().to_os_string(),
            ],
            working_directory: None,
            environment: SanitizedEnvironment::default(),
            process_group: ProcessGroupRequirement::Required,
            start_suspended: true,
            project_root: None,
            pueue_config: Some(verified_config),
            git_directories: None,
            child_io: VerifiedChildIo::Capture,
        })
        .unwrap();

        assert!(!result_path.exists(), "Pueue target executed before release");
        child.release().unwrap();
        child.confirm_exec().await.unwrap();
        child.wait_for_release_ack().await.unwrap();
        assert!(child.wait().await.unwrap().success());
        assert_eq!(fs::read(result_path).unwrap(), b"pueue-fd9-ok");
    }

    #[tokio::test]
    async fn terminate_kills_descendant_that_ignores_term_without_reaping_during_grace() {
        let temporary = tempdir().unwrap();
        let (launcher, _) = copy_launcher(temporary.path());
        let target = compile_generated_group_fixture(temporary.path());
        let root_anchor = pueue_agent::execution_policy::ProjectRootAnchor::resolve(
            &fs::canonicalize(temporary.path()).unwrap(),
        )
        .unwrap();
        let pid_path = temporary.path().join("descendant.pid");
        let verified_root = root_anchor.verify_identity().unwrap();
        let private_temp = PrivateRunTemp::create(&verified_root, 3).unwrap();
        let working_directory = VerifiedWorkingDirectory::root(&verified_root).unwrap();
        let child_io = agent_log_io(temporary.path());
        let mut child = spawn_verified_command_in_private_temp(VerifiedCommandSpec {
            launcher,
            executable: target,
            argv: vec![
                OsString::from("generated-group-target"),
                OsString::from("parent"),
                pid_path.as_os_str().to_os_string(),
            ],
            working_directory: Some(working_directory),
            environment: SanitizedEnvironment::default(),
            process_group: ProcessGroupRequirement::Required,
            start_suspended: true,
            project_root: Some(verified_root),
            pueue_config: None,
            git_directories: None,
            child_io,
        }, &private_temp)
        .unwrap();
        child.release().unwrap();
        child.confirm_exec().await.unwrap();
        child.wait_for_release_ack().await.unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut descendant_pid = None;
        while descendant_pid.is_none() && Instant::now() < deadline {
            descendant_pid = fs::read_to_string(&pid_path)
                .ok()
                .and_then(|contents| contents.trim().parse::<libc::pid_t>().ok());
            if descendant_pid.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let descendant_pid = descendant_pid.expect(
            "group fixture did not publish a parseable descendant pid before the bounded deadline",
        );
        terminate_process_group(&mut child).await.unwrap();
        let gone_deadline = Instant::now() + Duration::from_secs(2);
        while unsafe { libc::kill(descendant_pid, 0) } == 0 && Instant::now() < gone_deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(unsafe { libc::kill(descendant_pid, 0) }, -1);
        assert_eq!(std::io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
    }
}

#[cfg(not(unix))]
#[test]
fn native_helper_is_not_available_on_non_unix() {}
