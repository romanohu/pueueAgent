#![cfg(all(unix, debug_assertions))]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};

use pueue_agent::execution_policy::{
    load_or_create_policy, PolicyLoadInput, ResolvedExecutionPolicy, StartupEnvironment,
};
use tempfile::TempDir;

const FIXTURE_SOURCE: &str = r#"
use std::{
    env,
    ffi::OsStr,
    fs,
    io::Write,
    os::unix::ffi::OsStrExt,
    path::Path,
    process::{Command, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

static TERM_SEEN: AtomicBool = AtomicBool::new(false);

extern "C" fn observe_term(_: i32) {
    TERM_SEEN.store(true, Ordering::SeqCst);
}

extern "C" {
    fn signal(signal: i32, handler: usize) -> usize;
}

fn main() {
    if env::args_os().nth(1).as_deref() == Some(OsStr::new("--native-descendant")) {
        unsafe {
            let _ = signal(15, 1);
        }
        fs::write(__DESCENDANT_PID__, std::process::id().to_string())
            .expect("write descendant pid");
        loop {
            thread::sleep(Duration::from_millis(5));
        }
    }

    unsafe {
        let _ = signal(15, observe_term as usize);
    }

    let mut captured_args = Vec::new();
    for argument in env::args_os() {
        captured_args.extend_from_slice(argument.as_bytes());
        captured_args.push(0);
    }
    fs::write(__ARGV_CAPTURE__, captured_args).expect("capture argv");
    fs::write(
        __CONFIG_CAPTURE__,
        fs::read("/dev/fd/9").expect("read verified config fd 9"),
    )
    .expect("capture config fd 9");
    fs::write(__LEADER_PID__, std::process::id().to_string()).expect("write leader pid");

    let _descendant = Command::new(env::current_exe().expect("current fixture executable"))
        .arg("--native-descendant")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn TERM-ignoring descendant");
    let descendant_deadline = Instant::now() + Duration::from_secs(2);
    while !Path::new(__DESCENDANT_PID__).exists() {
        assert!(
            Instant::now() < descendant_deadline,
            "descendant did not publish readiness"
        );
        thread::sleep(Duration::from_millis(5));
    }
    fs::write(__READY__, b"ready").expect("write fixture readiness");

    let output = vec![b'x'; __OUTPUT_BYTES__];
    if __OUTPUT_STREAM__ == "stdout" {
        let mut stream = std::io::stdout();
        let _ = stream.write_all(&output);
        let _ = stream.flush();
    } else if __OUTPUT_STREAM__ == "stderr" {
        let mut stream = std::io::stderr();
        let _ = stream.write_all(&output);
        let _ = stream.flush();
    } else if __OUTPUT_STREAM__ == "both" {
        let stdout_output = output.clone();
        let stdout_writer = thread::spawn(move || {
            let mut stream = std::io::stdout();
            let _ = stream.write_all(&stdout_output);
            let _ = stream.flush();
        });
        let stderr_writer = thread::spawn(move || {
            let mut stream = std::io::stderr();
            let _ = stream.write_all(&output);
            let _ = stream.flush();
        });
        let _ = stdout_writer.join();
        let _ = stderr_writer.join();
    }

    if __EXIT_CODE__ >= 0 {
        std::process::exit(__EXIT_CODE__);
    }

    loop {
        if TERM_SEEN.swap(false, Ordering::SeqCst) {
            let observation = b"term-observed\n";
            let pipe_open = if __TERM_STREAM__ == "stdout" {
                let mut stream = std::io::stdout();
                stream.write_all(observation).and_then(|_| stream.flush()).is_ok()
            } else {
                let mut stream = std::io::stderr();
                stream.write_all(observation).and_then(|_| stream.flush()).is_ok()
            };
            fs::write(
                __TERM_OBSERVATION__,
                if pipe_open {
                    b"pipe-open".as_slice()
                } else {
                    b"pipe-closed".as_slice()
                },
            )
            .expect("write TERM observation");
        }
        thread::sleep(Duration::from_millis(5));
    }
}
"#;

#[derive(Clone, Copy, Debug)]
pub enum NativeBehavior {
    Hold,
    StdoutOverflow,
    StderrOverflow,
    BothStreamsOverflow,
    ExactLimitSuccess,
    NonzeroExit,
}

pub struct NativeFakePueue {
    _temp: TempDir,
    policy: Arc<ResolvedExecutionPolicy>,
    ready_path: PathBuf,
    leader_pid_path: PathBuf,
    descendant_pid_path: PathBuf,
    term_observation_path: PathBuf,
    argv_capture_path: PathBuf,
    config_capture_path: PathBuf,
}

impl NativeFakePueue {
    pub fn new(behavior: NativeBehavior) -> Self {
        let temp = TempDir::new().expect("create native Pueue fixture root");
        let base = fs::canonicalize(temp.path()).expect("canonicalize fixture root");
        let state_dir = base.join("state");
        let project_root = base.join("project");
        let trusted_dir = base.join("trusted");
        let codex_home = base.join("codex-home");
        for directory in [&state_dir, &project_root, &trusted_dir, &codex_home] {
            fs::create_dir(directory).expect("create secure fixture directory");
            secure_directory(directory);
        }

        let ready_path = base.join("ready");
        let leader_pid_path = base.join("leader-pid");
        let descendant_pid_path = base.join("descendant-pid");
        let term_observation_path = base.join("term-observation");
        let argv_capture_path = base.join("argv-capture");
        let config_capture_path = base.join("config-fd9-capture");

        let (output_stream, output_bytes, term_stream, exit_code) = match behavior {
            NativeBehavior::Hold => ("none", 0, "stdout", -1),
            NativeBehavior::StdoutOverflow => ("stdout", 64 * 1024 + 1, "stderr", -1),
            NativeBehavior::StderrOverflow => ("stderr", 64 * 1024 + 1, "stdout", -1),
            NativeBehavior::BothStreamsOverflow => {
                ("both", 64 * 1024 + 1, "stdout", -1)
            }
            NativeBehavior::ExactLimitSuccess => ("both", 64 * 1024, "stdout", 0),
            NativeBehavior::NonzeroExit => ("both", 17, "stdout", 7),
        };
        let source_path = base.join("native-process-fixture.rs");
        let target_path = trusted_dir.join("pueue");
        let source = FIXTURE_SOURCE
            .replace("__READY__", &rust_string(&ready_path))
            .replace("__LEADER_PID__", &rust_string(&leader_pid_path))
            .replace("__DESCENDANT_PID__", &rust_string(&descendant_pid_path))
            .replace("__TERM_OBSERVATION__", &rust_string(&term_observation_path))
            .replace("__ARGV_CAPTURE__", &rust_string(&argv_capture_path))
            .replace("__CONFIG_CAPTURE__", &rust_string(&config_capture_path))
            .replace("__OUTPUT_STREAM__", &format!("{output_stream:?}"))
            .replace("__OUTPUT_BYTES__", &output_bytes.to_string())
            .replace("__TERM_STREAM__", &format!("{term_stream:?}"))
            .replace("__EXIT_CODE__", &exit_code.to_string());
        fs::write(&source_path, source).expect("write generated native Pueue source");
        let output = Command::new("rustc")
            .args(["--edition=2021", "-O", "-o"])
            .arg(&target_path)
            .arg(&source_path)
            .output()
            .expect("compile generated native Pueue target");
        assert!(
            output.status.success(),
            "generated native Pueue fixture failed to compile: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        secure_executable(&target_path);

        let codex_path = trusted_dir.join("codex");
        fs::copy(&target_path, &codex_path).expect("create trusted codex fixture");
        secure_executable(&codex_path);
        let launcher_path = trusted_dir.join("launcher");
        fs::copy(env!("CARGO_BIN_EXE_pueue-agent"), &launcher_path)
            .expect("copy current compiled native launcher");
        secure_executable(&launcher_path);

        let config_path = base.join("pueue.yml");
        fs::write(&config_path, b"fixture-config-fd9\n").expect("write Pueue fixture config");
        secure_file(&config_path);
        let policy_path = state_dir.join("execution-policy.toml");
        fs::write(
            &policy_path,
            format!(
                "version = 1\ntrusted_path = {:?}\n\n[executables]\ncodex = {:?}\npueue = {:?}\n",
                trusted_dir.display().to_string(),
                codex_path.display().to_string(),
                target_path.display().to_string(),
            ),
        )
        .expect("write generated core policy");
        secure_file(&policy_path);

        let policy = load_or_create_policy(&PolicyLoadInput {
            state_dir,
            project_roots: vec![project_root],
            inherited_path: trusted_dir.clone().into_os_string(),
            startup_environment: StartupEnvironment::from_pairs([("FIXTURE", "native-pueue")]),
            codex_home,
            pueue_config: config_path,
            launcher_path,
        })
        .expect("load generated native Pueue policy");

        Self {
            _temp: temp,
            policy: Arc::new(policy),
            ready_path,
            leader_pid_path,
            descendant_pid_path,
            term_observation_path,
            argv_capture_path,
            config_capture_path,
        }
    }

    pub fn policy(&self) -> Arc<ResolvedExecutionPolicy> {
        Arc::clone(&self.policy)
    }

    pub async fn wait_until_ready(&self) {
        assert_eq!(read_file_bounded(&self.ready_path).await, b"ready");
    }

    pub async fn assert_started_processes_alive(&self) {
        let leader = read_pid_bounded(&self.leader_pid_path).await;
        let descendant = read_pid_bounded(&self.descendant_pid_path).await;
        assert!(
            probe_pid(leader).await,
            "fixture leader exited before adapter cleanup"
        );
        assert!(
            probe_pid(descendant).await,
            "fixture descendant exited before adapter cleanup"
        );
    }

    pub async fn term_observation(&self) -> Vec<u8> {
        read_file_bounded(&self.term_observation_path).await
    }

    pub async fn wait_for_processes_gone(&self) {
        let leader = read_pid_bounded(&self.leader_pid_path).await;
        let descendant = read_pid_bounded(&self.descendant_pid_path).await;
        wait_for_pid_gone(leader).await;
        wait_for_pid_gone(descendant).await;
    }

    pub async fn captured_argv(&self) -> Vec<Vec<u8>> {
        read_file_bounded(&self.argv_capture_path)
            .await
            .split(|byte| *byte == 0)
            .filter(|argument| !argument.is_empty())
            .map(<[u8]>::to_vec)
            .collect()
    }

    pub async fn captured_config(&self) -> Vec<u8> {
        read_file_bounded(&self.config_capture_path).await
    }
}

fn rust_string(path: &Path) -> String {
    format!("{:?}", path.to_string_lossy())
}

async fn read_file_bounded(path: &Path) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        let owned = path.to_path_buf();
        match tokio::task::spawn_blocking(move || fs::read(owned))
            .await
            .expect("fixture file read task")
        {
            Ok(bytes) => return bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("read fixture file {}: {error}", path.display()),
        }
        assert!(
            Instant::now() < deadline,
            "fixture file {} was not published",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn read_pid_bounded(path: &Path) -> libc::pid_t {
    String::from_utf8(read_file_bounded(path).await)
        .expect("fixture PID is UTF-8")
        .parse()
        .expect("fixture PID is numeric")
}

async fn probe_pid(pid: libc::pid_t) -> bool {
    tokio::task::spawn_blocking(move || {
        if unsafe { libc::kill(pid, 0) } == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    })
    .await
    .expect("PID probe task")
}

async fn wait_for_pid_gone(pid: libc::pid_t) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if !probe_pid(pid).await {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "fixture PID {pid} survived adapter cleanup"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn secure_directory(path: &Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("set fixture directory mode");
}

fn secure_file(path: &Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("set fixture file mode");
}

fn secure_executable(path: &Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("set fixture executable mode");
}
