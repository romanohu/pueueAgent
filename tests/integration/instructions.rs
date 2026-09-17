use std::fs;

use assert_cmd::Command;
use tempfile::TempDir;

fn initialized_legacy_project() -> (TempDir, std::path::PathBuf, Vec<u8>) {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("project");
    fs::create_dir(&root).unwrap();

    let initialized = Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["init", root.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(initialized.status.success());

    let instruction_path = root.join(".pueue-agent/instructions.md");
    let original = b"custom prefix\r\n"
        .iter()
        .copied()
        .chain(
            include_bytes!("../../templates/legacy/instructions-v0.md")
                .iter()
                .copied(),
        )
        .chain(b"custom suffix\n".iter().copied())
        .collect::<Vec<_>>();
    fs::write(&instruction_path, &original).unwrap();
    (temporary, instruction_path, original)
}

fn update(root: &std::path::Path, apply: Option<&str>) -> std::process::Output {
    let mut command = Command::cargo_bin("pueue-agent").unwrap();
    command.timeout(std::time::Duration::from_secs(5));
    command.args(["instructions", "update"]);
    if let Some(token) = apply {
        command.args(["--apply", token]);
    }
    command.arg(root);
    command.output().unwrap()
}

#[cfg(unix)]
fn update_token(root: &std::path::Path, before: &[u8], after: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    use std::os::unix::ffi::OsStrExt;

    let mut digest = Sha256::new();
    digest.update(b"pueue-agent:instructions-update:v1\0");
    for part in [root.as_os_str().as_bytes(), before, after] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    format!("{:x}", digest.finalize())
}

#[test]
fn update_preview_reports_a_legacy_distribution_without_writing() {
    let (_temporary, instruction_path, original) = initialized_legacy_project();
    let state_dir = instruction_path.parent().unwrap();
    let mut before_entries = fs::read_dir(state_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    before_entries.sort_unstable();
    let root = state_dir.parent().unwrap();

    let preview = Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["instructions", "update"])
        .arg(instruction_path.parent().unwrap().parent().unwrap())
        .output()
        .unwrap();

    assert!(preview.status.success());
    assert_eq!(fs::read(&instruction_path).unwrap(), original);
    let mut after_entries = fs::read_dir(state_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    after_entries.sort_unstable();
    assert_eq!(after_entries, before_entries);
    assert!(!state_dir.join("instructions.backups").exists());
    assert!(!state_dir.join(".instructions.lock").exists());
    assert!(!state_dir.join("state.sqlite3").exists());
    assert!(!root.join(".pueue-agent.sqlite3").exists());
}

#[test]
fn preview_prints_substantive_managed_instruction_diff() {
    let (_temporary, instruction_path, _original) = initialized_legacy_project();
    let root = instruction_path.parent().unwrap().parent().unwrap();

    let preview = update(root, None);
    assert!(
        preview.status.success(),
        "{}",
        String::from_utf8_lossy(&preview.stderr)
    );
    let output = String::from_utf8(preview.stdout).unwrap();
    assert!(output.contains("+## Standard role"));
    assert!(output.contains("+## Diagnosis role"));
    assert!(output.contains("-## Phase 2 decision agent"));
}

#[cfg(unix)]
#[test]
fn preview_then_apply_preserves_custom_bytes_and_all_unrelated_state() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let (_temporary, instruction_path, original) = initialized_legacy_project();
    let root = instruction_path.parent().unwrap().parent().unwrap();
    let state_dir = root.join(".pueue-agent");
    let unrelated = [
        (
            "config.toml",
            fs::read(state_dir.join("config.toml")).unwrap(),
        ),
        ("STATE.md", fs::read(state_dir.join("STATE.md")).unwrap()),
        (
            "state.json",
            fs::read(state_dir.join("state.json")).unwrap(),
        ),
    ];

    let preview = update(root, None);
    assert!(
        preview.status.success(),
        "{}",
        String::from_utf8_lossy(&preview.stderr)
    );
    assert_eq!(fs::read(&instruction_path).unwrap(), original);
    let preview_text = String::from_utf8(preview.stdout).unwrap();
    let token = preview_text
        .lines()
        .find_map(|line| line.strip_prefix("preview_token: "))
        .unwrap()
        .to_owned();
    assert_eq!(token.len(), 64);
    let legacy = include_bytes!("../../templates/legacy/instructions-v0.md");
    let current = include_bytes!("../../templates/instructions.md");
    let legacy_start = original
        .windows(legacy.len())
        .position(|window| window == legacy)
        .unwrap();
    let candidate = [
        &original[..legacy_start],
        current.as_slice(),
        &original[legacy_start + legacy.len()..],
    ]
    .concat();
    let canonical_root = root.canonicalize().unwrap();
    assert_eq!(
        token,
        update_token(&canonical_root, &original, &candidate),
        "preview token must use the documented domain and length prefixes"
    );
    let preview_lines = preview_text.lines().collect::<Vec<_>>();
    assert_eq!(
        preview_lines
            .iter()
            .find_map(|line| line.strip_prefix("before_sha256: "))
            .unwrap(),
        sha256_hex(&original)
    );
    assert_eq!(
        preview_lines
            .iter()
            .find_map(|line| line.strip_prefix("after_sha256: "))
            .unwrap(),
        sha256_hex(&candidate)
    );
    assert!(!state_dir.join("instructions.backups").exists());
    assert!(!state_dir.join("state.sqlite3").exists());

    let apply = update(root, Some(&token));
    assert!(
        apply.status.success(),
        "{}",
        String::from_utf8_lossy(&apply.stderr)
    );
    let apply_text = String::from_utf8_lossy(&apply.stdout);
    assert!(apply_text.contains("status: updated"));
    assert!(apply_text.contains("backup_path: "));
    assert!(apply_text.contains("active agents are unchanged"));
    let updated = fs::read(&instruction_path).unwrap();
    assert!(updated.starts_with(b"custom prefix\r\n"));
    assert!(updated.ends_with(b"custom suffix\n"));
    assert!(updated
        .windows(include_bytes!("../../templates/instructions.md").len())
        .any(|window| window == include_bytes!("../../templates/instructions.md")));
    for (name, bytes) in unrelated {
        assert_eq!(fs::read(state_dir.join(name)).unwrap(), bytes);
    }
    assert!(!state_dir.join("state.sqlite3").exists());
    let backups = state_dir.join("instructions.backups");
    assert_eq!(
        fs::metadata(&backups).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let entries = fs::read_dir(&backups)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 1);
    let backup = entries[0].path();
    assert_eq!(
        fs::metadata(&backup).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(fs::read(backup).unwrap(), original);

    let repeated = update(root, Some(&token));
    assert!(
        repeated.status.success(),
        "{}",
        String::from_utf8_lossy(&repeated.stderr)
    );
    assert!(String::from_utf8_lossy(&repeated.stdout).contains("status: current"));
    assert_eq!(fs::read_dir(&backups).unwrap().count(), 1);

    let lock_path = state_dir.join(".instructions.lock");
    let lock_metadata = fs::metadata(&lock_path).unwrap();
    assert_eq!(lock_metadata.permissions().mode() & 0o777, 0o600);
    let lock_inode = lock_metadata.ino();
    fs::write(&instruction_path, &original).unwrap();
    let second_preview = update(root, None);
    let second_token = String::from_utf8_lossy(&second_preview.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("preview_token: "))
        .unwrap()
        .to_owned();
    let second_apply = update(root, Some(&second_token));
    assert!(
        second_apply.status.success(),
        "{}",
        String::from_utf8_lossy(&second_apply.stderr)
    );
    assert_eq!(fs::metadata(&lock_path).unwrap().ino(), lock_inode);
    assert_eq!(fs::read_dir(&backups).unwrap().count(), 1);
}

#[test]
fn preview_token_is_invalidated_by_custom_text_changes_and_root_binding() {
    let (_temporary, instruction_path, _original) = initialized_legacy_project();
    let root = instruction_path
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let preview = update(&root, None);
    assert!(preview.status.success());
    let token = String::from_utf8_lossy(&preview.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("preview_token: "))
        .unwrap()
        .to_owned();
    fs::write(
        &instruction_path,
        [fs::read(&instruction_path).unwrap(), b"edited\n".to_vec()].concat(),
    )
    .unwrap();
    let stale = update(&root, Some(&token));
    assert!(!stale.status.success());
    assert!(String::from_utf8_lossy(&stale.stderr).contains("instructions: conflict"));

    let (_second_temporary, second_instruction_path, _second_original) =
        initialized_legacy_project();
    let second_root = second_instruction_path.parent().unwrap().parent().unwrap();
    let second_preview = update(second_root, None);
    assert!(second_preview.status.success());
    let second_output = String::from_utf8_lossy(&second_preview.stdout);
    let second_token = second_output
        .lines()
        .find_map(|line| line.strip_prefix("preview_token: "))
        .unwrap();
    assert_ne!(
        token, second_token,
        "tokens must bind otherwise-identical project roots"
    );
}

#[test]
fn current_distribution_is_a_read_only_noop_even_with_an_apply_token() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("project");
    fs::create_dir(&root).unwrap();
    let initialized = Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["init", root.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(initialized.status.success());
    let path = root.join(".pueue-agent/instructions.md");
    let before = fs::read(&path).unwrap();

    let result = update(&root, Some("not-a-preview-token"));
    assert!(result.status.success());
    assert!(String::from_utf8_lossy(&result.stdout).contains("status: current"));
    assert_eq!(fs::read(path).unwrap(), before);
    assert!(!root.join(".pueue-agent/.instructions.lock").exists());
}

#[test]
fn unknown_custom_or_malformed_distributions_fail_closed_without_writes() {
    for contents in [
        b"operator-owned instructions\n".to_vec(),
        {
            let mut value = include_bytes!("../../templates/instructions.md").to_vec();
            value[50] = b'X';
            value
        },
        [
            include_bytes!("../../templates/legacy/instructions-v0.md").as_slice(),
            include_bytes!("../../templates/legacy/instructions-v0.md").as_slice(),
        ]
        .concat(),
        [
            include_bytes!("../../templates/legacy/instructions-v0.md").as_slice(),
            include_bytes!("../../templates/instructions.md").as_slice(),
        ]
        .concat(),
        b"<!-- pueue-agent:instructions v1 begin -->\ncustom\n<!-- pueue-agent:instructions v1 end -->\n".to_vec(),
    ] {
        let (_temporary, instruction_path, _original) = initialized_legacy_project();
        fs::write(&instruction_path, &contents).unwrap();
        let before = fs::read(&instruction_path).unwrap();
        let root = instruction_path.parent().unwrap().parent().unwrap();
        let output = update(root, None);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("instructions: conflict"));
        assert_eq!(fs::read(&instruction_path).unwrap(), before);
        assert!(!root.join(".pueue-agent/instructions.backups").exists());
    }
}

#[test]
fn reserved_marker_variants_fail_closed_through_the_cli() {
    for extra_marker in [
        b"<!-- pueue-agent:instructions v2 begin -->\n".as_slice(),
        b"<!-- pueue-agent:instructions v1 begin\n".as_slice(),
    ] {
        for distribution in [
            include_bytes!("../../templates/instructions.md").as_slice(),
            include_bytes!("../../templates/legacy/instructions-v0.md").as_slice(),
        ] {
            let (_temporary, instruction_path, _original) = initialized_legacy_project();
            let contents = [distribution, extra_marker].concat();
            fs::write(&instruction_path, &contents).unwrap();

            let root = instruction_path.parent().unwrap().parent().unwrap();
            let result = update(root, None);
            assert!(!result.status.success());
            assert!(String::from_utf8_lossy(&result.stderr).contains("instructions: conflict"));
            assert_eq!(fs::read(&instruction_path).unwrap(), contents);
            assert!(!root.join(".pueue-agent/instructions.backups").exists());
        }
    }
}

#[test]
fn missing_invalid_utf8_and_size_limited_inputs_are_not_created_or_replaced() {
    let (_temporary, instruction_path, _original) = initialized_legacy_project();
    let root = instruction_path.parent().unwrap().parent().unwrap();

    fs::remove_file(&instruction_path).unwrap();
    let missing = update(root, None);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("instructions: conflict"));

    fs::write(&instruction_path, [0xff, 0xfe]).unwrap();
    let invalid = update(root, None);
    assert!(!invalid.status.success());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("instructions: conflict"));

    let oversized = vec![b'x'; 64 * 1024 + 1];
    fs::write(&instruction_path, &oversized).unwrap();
    let too_large = update(root, None);
    assert!(!too_large.status.success());
    assert!(String::from_utf8_lossy(&too_large.stderr).contains("65536"));
    assert_eq!(fs::read(instruction_path).unwrap(), oversized);
}

#[test]
fn replacement_that_would_exceed_the_candidate_limit_is_rejected_before_backup() {
    let (_temporary, instruction_path, _original) = initialized_legacy_project();
    let root = instruction_path.parent().unwrap().parent().unwrap();
    let legacy = include_bytes!("../../templates/legacy/instructions-v0.md");
    let mut input = vec![b'p'; 64 * 1024 - legacy.len()];
    input.extend_from_slice(legacy);
    assert_eq!(input.len(), 64 * 1024);
    fs::write(&instruction_path, &input).unwrap();

    let output = update(root, None);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("65536"));
    assert_eq!(fs::read(&instruction_path).unwrap(), input);
    assert!(!root.join(".pueue-agent/instructions.backups").exists());
}

#[test]
fn apply_requires_a_value_for_the_preview_token() {
    let (_temporary, _instruction_path, _original) = initialized_legacy_project();
    let output = Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["instructions", "update", "--apply"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
}

#[cfg(unix)]
#[test]
fn unsafe_instruction_links_hardlinks_fifo_and_backup_are_rejected() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let (_temporary, instruction_path, original) = initialized_legacy_project();
    let root = instruction_path.parent().unwrap().parent().unwrap();
    let outside = root.parent().unwrap().join("outside-instructions");
    fs::write(&outside, &original).unwrap();
    fs::remove_file(&instruction_path).unwrap();
    symlink(&outside, &instruction_path).unwrap();
    let linked = update(root, None);
    assert!(!linked.status.success());
    assert!(String::from_utf8_lossy(&linked.stderr).contains("instructions: unsafe"));
    assert_eq!(fs::read(outside).unwrap(), original);

    fs::remove_file(&instruction_path).unwrap();
    fs::write(
        &instruction_path,
        include_bytes!("../../templates/legacy/instructions-v0.md"),
    )
    .unwrap();
    fs::hard_link(&instruction_path, root.join("legacy-hardlink")).unwrap();
    let hardlinked = update(root, None);
    assert!(!hardlinked.status.success());
    assert!(String::from_utf8_lossy(&hardlinked.stderr).contains("instructions: unsafe"));
    fs::remove_file(root.join("legacy-hardlink")).unwrap();

    fs::remove_file(&instruction_path).unwrap();
    let fifo = root.join("fifo");
    unsafe {
        assert_eq!(
            libc::mkfifo(
                std::ffi::CString::new(&fifo.to_string_lossy()[..])
                    .unwrap()
                    .as_ptr(),
                0o600
            ),
            0
        );
    }
    fs::rename(&fifo, &instruction_path).unwrap();
    let fifo_result = update(root, None);
    assert!(!fifo_result.status.success());
    assert!(String::from_utf8_lossy(&fifo_result.stderr).contains("instructions: unsafe"));
    fs::remove_file(&instruction_path).unwrap();

    fs::write(
        &instruction_path,
        include_bytes!("../../templates/legacy/instructions-v0.md"),
    )
    .unwrap();
    let preview = update(root, None);
    let token = String::from_utf8_lossy(&preview.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("preview_token: "))
        .unwrap()
        .to_owned();
    let backups = root.join(".pueue-agent/instructions.backups");
    fs::create_dir(&backups).unwrap();
    fs::set_permissions(&backups, fs::Permissions::from_mode(0o700)).unwrap();
    let backup_name = format!(
        "{}.md",
        sha256_hex(include_bytes!("../../templates/legacy/instructions-v0.md"))
    );
    let backup_path = backups.join(backup_name);
    fs::write(&backup_path, b"wrong backup\n").unwrap();
    fs::set_permissions(&backup_path, fs::Permissions::from_mode(0o600)).unwrap();
    let unsafe_backup = update(root, Some(&token));
    assert!(!unsafe_backup.status.success());
    assert!(
        String::from_utf8_lossy(&unsafe_backup.stderr).contains("instructions: conflict"),
        "{}",
        String::from_utf8_lossy(&unsafe_backup.stderr)
    );
    assert_eq!(
        fs::read(&instruction_path).unwrap(),
        include_bytes!("../../templates/legacy/instructions-v0.md")
    );
    assert_eq!(fs::read(&backup_path).unwrap(), b"wrong backup\n");
}

#[cfg(unix)]
#[test]
fn lock_state_and_backup_symlinks_are_rejected_without_publication() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let (_temporary, instruction_path, original) = initialized_legacy_project();
    let root = instruction_path.parent().unwrap().parent().unwrap();
    let state_dir = root.join(".pueue-agent");
    let preview = update(root, None);
    let token = String::from_utf8_lossy(&preview.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("preview_token: "))
        .unwrap()
        .to_owned();

    let outside_lock = root.parent().unwrap().join("outside-lock");
    fs::write(&outside_lock, b"outside lock").unwrap();
    symlink(&outside_lock, state_dir.join(".instructions.lock")).unwrap();
    let lock_result = update(root, Some(&token));
    assert!(!lock_result.status.success());
    assert!(String::from_utf8_lossy(&lock_result.stderr).contains("instructions: unsafe"));
    assert_eq!(fs::read(&outside_lock).unwrap(), b"outside lock");
    assert_eq!(fs::read(&instruction_path).unwrap(), original);
    fs::remove_file(state_dir.join(".instructions.lock")).unwrap();

    let outside_backups = root.parent().unwrap().join("outside-backups");
    fs::create_dir(&outside_backups).unwrap();
    symlink(&outside_backups, state_dir.join("instructions.backups")).unwrap();
    let backup_dir_result = update(root, Some(&token));
    assert!(!backup_dir_result.status.success());
    assert!(String::from_utf8_lossy(&backup_dir_result.stderr).contains("instructions: unsafe"));
    assert!(fs::read_dir(&outside_backups).unwrap().next().is_none());
    assert_eq!(fs::read(&instruction_path).unwrap(), original);
    fs::remove_file(state_dir.join("instructions.backups")).unwrap();

    let backups = state_dir.join("instructions.backups");
    fs::create_dir(&backups).unwrap();
    fs::set_permissions(&backups, fs::Permissions::from_mode(0o700)).unwrap();
    let outside_backup = root.parent().unwrap().join("outside-backup");
    fs::write(&outside_backup, &original).unwrap();
    let backup_name = format!("{}.md", sha256_hex(&original));
    symlink(&outside_backup, backups.join(backup_name)).unwrap();
    let backup_file_result = update(root, Some(&token));
    assert!(!backup_file_result.status.success());
    assert!(String::from_utf8_lossy(&backup_file_result.stderr)
        .contains("instructions: unsafe path: open instruction backup"));
    assert_eq!(fs::read(&outside_backup).unwrap(), original);
    assert_eq!(fs::read(&instruction_path).unwrap(), original);
}

#[cfg(unix)]
#[test]
fn state_directory_symlink_is_rejected_before_reading_instructions() {
    use std::os::unix::fs::symlink;

    let (_temporary, instruction_path, _original) = initialized_legacy_project();
    let root = instruction_path.parent().unwrap().parent().unwrap();
    let state_dir = root.join(".pueue-agent");
    let moved_state_dir = root.parent().unwrap().join("outside-state");
    fs::rename(&state_dir, &moved_state_dir).unwrap();
    symlink(&moved_state_dir, &state_dir).unwrap();

    let result = update(root, None);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("instructions: unsafe"));

    fs::remove_file(&state_dir).unwrap();
    fs::rename(moved_state_dir, state_dir).unwrap();
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(unix)]
#[test]
fn two_cooperating_applies_are_serialized_without_duplicate_backups() {
    use pueue_agent::instructions::UpdateStatus;
    use std::sync::{mpsc, Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    let (_temporary, instruction_path, _original) = initialized_legacy_project();
    let root = instruction_path
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let preview = pueue_agent::instructions::update(&root, None).unwrap();
    let token = Arc::new(preview.preview_token.unwrap());
    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = mpsc::channel();
    let first_root = root.clone();
    let first_token = Arc::clone(&token);
    let first_barrier = Arc::clone(&barrier);
    let first_sender = sender.clone();
    let first = thread::spawn(move || {
        first_barrier.wait();
        first_sender
            .send(pueue_agent::instructions::update(
                &first_root,
                Some(first_token.as_str()),
            ))
            .unwrap();
    });
    let second_root = root.clone();
    let second_token = Arc::clone(&token);
    let second_barrier = Arc::clone(&barrier);
    let second_sender = sender;
    let second = thread::spawn(move || {
        second_barrier.wait();
        second_sender
            .send(pueue_agent::instructions::update(
                &second_root,
                Some(second_token.as_str()),
            ))
            .unwrap();
    });
    let results = [
        receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("first concurrent apply timed out"),
        receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("second concurrent apply timed out"),
    ];
    first.join().unwrap();
    second.join().unwrap();
    assert!(results[0].is_ok(), "first apply failed: {results:?}");
    assert!(results[1].is_ok(), "second apply failed: {results:?}");
    let statuses = [
        results[0].as_ref().unwrap().status,
        results[1].as_ref().unwrap().status,
    ];
    assert!(statuses.contains(&UpdateStatus::Updated));
    assert!(statuses.contains(&UpdateStatus::Current));
    assert_eq!(
        fs::read_dir(root.join(".pueue-agent/instructions.backups"))
            .unwrap()
            .count(),
        1
    );
    let updated = fs::read(&instruction_path).unwrap();
    assert!(updated
        .windows(include_bytes!("../../templates/instructions.md").len())
        .any(|window| window == include_bytes!("../../templates/instructions.md")));
}
