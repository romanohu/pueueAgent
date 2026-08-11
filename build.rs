use std::{
    env, fs,
    path::{Component, Path, PathBuf},
    process::Command,
};

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap_or_default());
    for path in git_metadata_paths(&manifest_dir) {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    let revision = Command::new("git")
        .current_dir(&manifest_dir)
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|revision| revision.trim().to_owned())
        .filter(|revision| !revision.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=PUEUE_AGENT_GIT_REVISION={revision}");
}

fn git_metadata_paths(manifest_dir: &Path) -> Vec<PathBuf> {
    let Some(git_dir) = resolve_git_dir(manifest_dir) else {
        return Vec::new();
    };
    let head = git_dir.join("HEAD");
    let mut paths = vec![head.clone()];

    if let Ok(contents) = fs::read_to_string(&head) {
        if let Some(reference) = contents.trim().strip_prefix("ref: ") {
            let reference = Path::new(reference);
            if reference.components().all(|component| matches!(component, Component::Normal(_))) {
                paths.push(git_dir.join(reference));
            }
        }
    }

    paths
}

fn resolve_git_dir(manifest_dir: &Path) -> Option<PathBuf> {
    let marker = manifest_dir.join(".git");
    if marker.is_dir() {
        return Some(marker);
    }
    if !marker.is_file() {
        return None;
    }

    let contents = fs::read_to_string(marker).ok()?;
    let target = contents.trim().strip_prefix("gitdir: ")?.trim();
    if target.is_empty() {
        return None;
    }

    let target = PathBuf::from(target);
    Some(if target.is_absolute() {
        target
    } else {
        manifest_dir.join(target)
    })
}

#[cfg(test)]
mod tests {
    use std::{fs, time::SystemTime};

    use super::*;

    #[test]
    fn linked_worktree_tracks_head_and_symbolic_branch_ref() {
        let unique = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = env::temp_dir().join(format!("pueue-agent-build-{unique}"));
        let git_dir = root.join("metadata");
        fs::create_dir_all(git_dir.join("refs/heads")).unwrap();
        fs::write(root.join(".git"), format!("gitdir: {}", git_dir.display())).unwrap();
        fs::write(git_dir.join("HEAD"), "ref: refs/heads/main\n").unwrap();

        assert_eq!(
            git_metadata_paths(&root),
            vec![git_dir.join("HEAD"), git_dir.join("refs/heads/main")]
        );

        fs::remove_dir_all(root).unwrap();
    }
}
