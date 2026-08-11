use std::{
    cell::RefCell,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use pueue_agent::upgrade::{resolve_source_root, validate_checkout};
use tempfile::TempDir;

#[allow(dead_code)]
struct UpgradeFixture {
    temp: TempDir,
    source: PathBuf,
    installed_binary: PathBuf,
    commands: FakeCommandRunner,
    service: FakeServiceRecorder,
}

impl UpgradeFixture {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let remote = temp.path().join("origin.git");
        run_git(temp.path(), ["init", "--bare", remote.to_str().unwrap()]);

        let source = temp.path().join("source");
        run_git(temp.path(), ["init", "-b", "main", source.to_str().unwrap()]);
        fs::write(
            source.join("Cargo.toml"),
            "[package]\nname = \"pueue-agent\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        fs::write(source.join("README.md"), "fixture\n").unwrap();
        run_git(&source, ["add", "."]);
        run_git(
            &source,
            [
                "-c",
                "user.name=Upgrade Fixture",
                "-c",
                "user.email=upgrade-fixture@example.test",
                "commit",
                "-m",
                "initial fixture",
            ],
        );
        run_git(&source, ["remote", "add", "origin", remote.to_str().unwrap()]);
        run_git(&source, ["push", "-u", "origin", "main"]);

        let installed_binary = temp.path().join("bin/pueue-agent");
        fs::create_dir_all(installed_binary.parent().unwrap()).unwrap();
        fs::write(&installed_binary, "fixture binary\n").unwrap();

        Self {
            temp,
            source: source.canonicalize().unwrap(),
            installed_binary,
            commands: FakeCommandRunner::default(),
            service: FakeServiceRecorder::default(),
        }
    }

    fn dirty_main_checkout() -> Self {
        let fixture = Self::new();
        fs::write(fixture.source.join("README.md"), "dirty fixture\n").unwrap();
        fixture
    }

    fn source_root(&self) -> &Path {
        &self.source
    }

    fn release_binary(&self) -> PathBuf {
        let binary = self.source.join("target/release/pueue-agent");
        fs::create_dir_all(binary.parent().unwrap()).unwrap();
        fs::write(&binary, "fixture binary\n").unwrap();
        binary
    }
}

#[allow(dead_code)]
#[derive(Default)]
struct FakeCommandRunner {
    invocations: RefCell<Vec<Vec<String>>>,
}

#[allow(dead_code)]
#[derive(Default)]
struct FakeServiceRecorder {
    actions: RefCell<Vec<String>>,
}

fn run_git<const N: usize>(working_directory: &Path, args: [&str; N]) {
    let output = Command::new("git")
        .current_dir(working_directory)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn source_resolution_prefers_explicit_source_and_requires_cargo_manifest() {
    let fixture = UpgradeFixture::new();

    let source = resolve_source_root(
        Some(fixture.source_root()),
        Path::new("/unrelated/target/release/pueue-agent"),
        None,
    )
    .unwrap();

    assert_eq!(source, fixture.source_root());
}

#[test]
fn source_resolution_uses_release_binary_ancestor_before_environment_fallback() {
    let fixture = UpgradeFixture::new();
    let executable = fixture.release_binary();

    let source = resolve_source_root(None, &executable, None).unwrap();

    assert_eq!(source, fixture.source_root());
}

#[test]
fn source_resolution_falls_back_to_environment_source() {
    let fixture = UpgradeFixture::new();

    let source = resolve_source_root(
        None,
        Path::new("/unrelated/pueue-agent"),
        Some(fixture.source_root()),
    )
    .unwrap();

    assert_eq!(source, fixture.source_root());
}

#[test]
fn source_resolution_rejects_a_manifest_for_another_package() {
    let fixture = UpgradeFixture::new();
    let wrong_package = fixture.temp.path().join("wrong-package");
    fs::create_dir_all(wrong_package.join(".git")).unwrap();
    fs::write(
        wrong_package.join("Cargo.toml"),
        "[package]\nname = \"another-package\"\nversion = \"0.1.0\"\n",
    )
    .unwrap();

    let error = resolve_source_root(
        Some(&wrong_package),
        Path::new("/unrelated/pueue-agent"),
        None,
    )
    .unwrap_err();

    assert!(error.to_string().contains("pueue-agent"));
}

#[test]
fn clean_main_checkout_tracking_origin_main_is_accepted() {
    let fixture = UpgradeFixture::new();

    let state = validate_checkout(fixture.source_root(), "main", "origin").unwrap();

    assert_eq!(state.branch, "main");
    assert_eq!(state.upstream, "origin/main");
}

#[test]
fn dirty_or_diverged_checkout_is_rejected_without_mutation() {
    let fixture = UpgradeFixture::dirty_main_checkout();
    let head_before = git_stdout(fixture.source_root(), ["rev-parse", "HEAD"]);

    let error = validate_checkout(fixture.source_root(), "main", "origin").unwrap_err();

    assert!(error.to_string().contains("clean") || error.to_string().contains("dirty"));
    assert_eq!(git_stdout(fixture.source_root(), ["rev-parse", "HEAD"]), head_before);
}

#[test]
fn checkout_on_another_branch_is_rejected() {
    let fixture = UpgradeFixture::new();
    run_git(fixture.source_root(), ["checkout", "-b", "feature"]);

    let error = validate_checkout(fixture.source_root(), "main", "origin").unwrap_err();

    assert!(error.to_string().contains("main"));
}

#[test]
fn checkout_without_origin_main_upstream_is_rejected() {
    let fixture = UpgradeFixture::new();
    run_git(fixture.source_root(), ["branch", "--unset-upstream"]);

    let error = validate_checkout(fixture.source_root(), "main", "origin").unwrap_err();

    assert!(error.to_string().contains("origin/main"));
}

#[test]
fn checkout_ahead_of_origin_main_is_rejected_before_merge() {
    let fixture = UpgradeFixture::new();
    fs::write(fixture.source_root().join("ahead.txt"), "ahead\n").unwrap();
    run_git(fixture.source_root(), ["add", "ahead.txt"]);
    run_git(
        fixture.source_root(),
        [
            "-c",
            "user.name=Upgrade Fixture",
            "-c",
            "user.email=upgrade-fixture@example.test",
            "commit",
            "-m",
            "ahead fixture",
        ],
    );
    let head_before = git_stdout(fixture.source_root(), ["rev-parse", "HEAD"]);

    let error = validate_checkout(fixture.source_root(), "main", "origin").unwrap_err();

    assert!(error.to_string().contains("fast-forward") || error.to_string().contains("diverged"));
    assert_eq!(git_stdout(fixture.source_root(), ["rev-parse", "HEAD"]), head_before);
}

fn git_stdout<const N: usize>(working_directory: &Path, args: [&str; N]) -> String {
    let output = Command::new("git")
        .current_dir(working_directory)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}
