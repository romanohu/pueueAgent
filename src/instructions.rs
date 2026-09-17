//! Explicit, bounded updates for the project instruction distribution.
//!
//! Classification and token calculation live here.  All filesystem mutation
//! is deliberately delegated to the private `instructions_file` module.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::{
    output::{bounded_redacted_text, bounded_typed_text},
    AppError,
};

pub(super) mod classification {
    pub(crate) const MAX_BYTES: usize = 64 * 1024;
    pub(super) const CURRENT_TEMPLATE: &[u8] = include_bytes!("../templates/instructions.md");
    pub(super) const LEGACY_TEMPLATE: &[u8] =
        include_bytes!("../templates/legacy/instructions-v0.md");
    pub(super) const RESERVED_MARKER_PREFIX: &[u8] = b"<!-- pueue-agent:instructions";
    pub(super) const BEGIN_MARKER: &[u8] = b"<!-- pueue-agent:instructions v1 begin -->";
    pub(super) const END_MARKER: &[u8] = b"<!-- pueue-agent:instructions v1 end -->";
}

use classification::{
    BEGIN_MARKER, CURRENT_TEMPLATE, END_MARKER, LEGACY_TEMPLATE, MAX_BYTES, RESERVED_MARKER_PREFIX,
};

const TOKEN_DOMAIN: &[u8] = b"pueue-agent:instructions-update:v1\0";
const MAX_DIFF_BYTES: usize = 8 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpdateStatus {
    Current,
    UpdateAvailable,
    Updated,
}

impl UpdateStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::UpdateAvailable => "update_available",
            Self::Updated => "updated",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstructionsUpdate {
    pub status: UpdateStatus,
    pub preview_token: Option<String>,
    pub before_sha256: String,
    pub after_sha256: String,
    pub backup_path: Option<PathBuf>,
    pub diff: Option<String>,
}

pub fn update(
    project_root: &Path,
    apply_token: Option<&str>,
) -> Result<InstructionsUpdate, AppError> {
    let inspected = crate::instructions_file::inspect(project_root)?;
    let before_sha256 = sha256_hex(&inspected.bytes);
    let classification = classify(&inspected.bytes)?;

    let Classification::UpdateAvailable { candidate } = classification else {
        return Ok(InstructionsUpdate {
            status: UpdateStatus::Current,
            preview_token: None,
            before_sha256: before_sha256.clone(),
            after_sha256: before_sha256,
            backup_path: None,
            diff: None,
        });
    };

    let after_sha256 = sha256_hex(&candidate);
    let root_bytes = path_bytes(&inspected.canonical_root)?;
    let preview_token = update_token(root_bytes, &inspected.bytes, &candidate);
    let diff = Some(render_bounded_diff(&inspected.bytes, &candidate));

    if let Some(apply_token) = apply_token {
        if apply_token != preview_token || !is_digest(apply_token) {
            return Err(conflict("preview token does not match the current project"));
        }
        let publish_result = crate::instructions_file::publish(
            &inspected.canonical_root,
            &inspected.bytes,
            &candidate,
            &before_sha256,
        )?;
        let backup_path = match publish_result {
            crate::instructions_file::PublishResult::Updated(path) => path,
            crate::instructions_file::PublishResult::Current(bytes) => {
                let digest = sha256_hex(&bytes);
                return Ok(InstructionsUpdate {
                    status: UpdateStatus::Current,
                    preview_token: None,
                    before_sha256: digest.clone(),
                    after_sha256: digest,
                    backup_path: None,
                    diff: None,
                });
            }
        };
        return Ok(InstructionsUpdate {
            status: UpdateStatus::Updated,
            preview_token: None,
            before_sha256,
            after_sha256,
            backup_path: Some(backup_path),
            diff,
        });
    }

    Ok(InstructionsUpdate {
        status: UpdateStatus::UpdateAvailable,
        preview_token: Some(preview_token),
        before_sha256,
        after_sha256,
        backup_path: None,
        diff,
    })
}

pub fn render_update(result: &InstructionsUpdate) -> String {
    let mut lines = vec![
        format!("status: {}", result.status.as_str()),
        format!(
            "before_sha256: {}",
            bounded_typed_text(&result.before_sha256)
        ),
        format!("after_sha256: {}", bounded_typed_text(&result.after_sha256)),
    ];
    if let Some(token) = &result.preview_token {
        // The machine token is intentionally not passed through the generic
        // secret redactor: operators must be able to copy it exactly.  The
        // typed sanitizer still bounds it and strips terminal controls from
        // manually constructed response values.
        lines.push(format!("preview_token: {}", bounded_typed_text(token)));
    }
    if let Some(diff) = &result.diff {
        lines.push(format!("diff:\n{}", render_bounded_diff_output(diff)));
    }
    if let Some(path) = &result.backup_path {
        lines.push(format!(
            "backup_path: {}",
            bounded_typed_text(&path.to_string_lossy())
        ));
    }
    lines.push(
        "output_note: diff and paths are bounded; terminal controls and sensitive text are redacted; preview_token is exact and not redacted"
            .to_owned(),
    );
    lines.push(match result.status {
        UpdateStatus::Current => "next: no instruction update is required".to_owned(),
        UpdateStatus::UpdateAvailable => {
            "next: review the diff, then run pueue-agent instructions update --apply <PREVIEW_TOKEN>".to_owned()
        }
        UpdateStatus::Updated => {
            "next: the updated instructions apply to the next agent run; active agents are unchanged".to_owned()
        }
    });
    lines.join("\n")
}

#[derive(Debug)]
enum Classification {
    Current,
    UpdateAvailable { candidate: Vec<u8> },
}

fn classify(input: &[u8]) -> Result<Classification, AppError> {
    if input.is_empty() {
        return Err(conflict("instruction file is empty"));
    }
    if input.len() > MAX_BYTES {
        return Err(conflict("instruction file exceeds 65536 bytes"));
    }
    std::str::from_utf8(input).map_err(|_| conflict("instruction file is not valid UTF-8"))?;

    let begin_count = count_occurrences(input, BEGIN_MARKER);
    let end_count = count_occurrences(input, END_MARKER);
    let reserved_marker_count = count_occurrences(input, RESERVED_MARKER_PREFIX);
    let current_count = count_occurrences(input, CURRENT_TEMPLATE);
    let legacy_count = count_occurrences(input, LEGACY_TEMPLATE);

    if current_count == 1
        && begin_count == 1
        && end_count == 1
        && reserved_marker_count == 2
        && legacy_count == 0
    {
        return Ok(Classification::Current);
    }

    // Marker-bearing files are recognized only when the complete, exact
    // current distribution is present.  This rejects duplicate, mixed, and
    // malformed marker layouts before they can be treated as Current.
    if reserved_marker_count != 0 || begin_count != 0 || end_count != 0 || current_count != 0 {
        return Err(conflict(
            "managed instruction markers are malformed or duplicated",
        ));
    }

    if legacy_count != 1 {
        return Err(conflict(
            "instruction distribution is unknown or customized",
        ));
    }

    let start = find_subslice(input, LEGACY_TEMPLATE).expect("legacy_count proves a match");
    let mut candidate =
        Vec::with_capacity(input.len() - LEGACY_TEMPLATE.len() + CURRENT_TEMPLATE.len());
    candidate.extend_from_slice(&input[..start]);
    candidate.extend_from_slice(CURRENT_TEMPLATE);
    candidate.extend_from_slice(&input[start + LEGACY_TEMPLATE.len()..]);
    if candidate.len() > MAX_BYTES {
        return Err(conflict("updated instruction file exceeds 65536 bytes"));
    }
    std::str::from_utf8(&candidate)
        .map_err(|_| conflict("updated instruction file is not valid UTF-8"))?;
    Ok(Classification::UpdateAvailable { candidate })
}

pub(crate) fn is_current_distribution(input: &[u8]) -> bool {
    matches!(classify(input), Ok(Classification::Current))
}

fn render_bounded_diff(before: &[u8], after: &[u8]) -> String {
    let mut diff = String::from("managed distribution: legacy 5d1a8e0 -> current v1\n");
    diff.push_str(&format!(
        "bytes {} -> {}; custom prefix/suffix preserved byte-for-byte\n",
        before.len(),
        after.len()
    ));
    diff.push_str("--- legacy managed template\n+++ current managed template\n");

    let before_lines = std::str::from_utf8(LEGACY_TEMPLATE)
        .expect("frozen legacy instructions template is UTF-8")
        .lines()
        .collect::<Vec<_>>();
    let after_lines = std::str::from_utf8(CURRENT_TEMPLATE)
        .expect("current instructions template is UTF-8")
        .lines()
        .collect::<Vec<_>>();
    let mut lcs = vec![vec![0usize; after_lines.len() + 1]; before_lines.len() + 1];
    for before_index in (0..before_lines.len()).rev() {
        for after_index in (0..after_lines.len()).rev() {
            lcs[before_index][after_index] =
                if before_lines[before_index] == after_lines[after_index] {
                    lcs[before_index + 1][after_index + 1] + 1
                } else {
                    lcs[before_index + 1][after_index].max(lcs[before_index][after_index + 1])
                };
        }
    }

    let mut before_index = 0;
    let mut after_index = 0;
    while before_index < before_lines.len() || after_index < after_lines.len() {
        if before_index < before_lines.len()
            && after_index < after_lines.len()
            && before_lines[before_index] == after_lines[after_index]
        {
            before_index += 1;
            after_index += 1;
        } else if after_index == after_lines.len()
            || (before_index < before_lines.len()
                && lcs[before_index + 1][after_index] >= lcs[before_index][after_index + 1])
        {
            if !push_bounded_diff_line(&mut diff, '-', before_lines[before_index]) {
                append_diff_truncation(&mut diff);
                break;
            }
            before_index += 1;
        } else {
            if !push_bounded_diff_line(&mut diff, '+', after_lines[after_index]) {
                append_diff_truncation(&mut diff);
                break;
            }
            after_index += 1;
        }
    }
    diff
}

fn push_bounded_diff_line(diff: &mut String, prefix: char, line: &str) -> bool {
    const TRUNCATION_NOTE: &str = "... [diff truncated]\n";
    if diff.len() + line.len() + 2 + TRUNCATION_NOTE.len() <= MAX_DIFF_BYTES {
        diff.push(prefix);
        diff.push_str(line);
        diff.push('\n');
        true
    } else {
        false
    }
}

fn append_diff_truncation(value: &mut String) {
    const TRUNCATION_NOTE: &str = "... [diff truncated]\n";
    let limit = MAX_DIFF_BYTES.saturating_sub(TRUNCATION_NOTE.len());
    truncate_utf8(value, limit);
    value.push_str(TRUNCATION_NOTE);
}

fn truncate_utf8(value: &mut String, limit: usize) {
    if value.len() <= limit {
        return;
    }
    let mut boundary = limit;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
}

fn render_bounded_diff_output(diff: &str) -> String {
    const TRUNCATION_NOTE: &str = "... [diff truncated]";
    let mut rendered = String::new();
    for line in diff.lines() {
        let safe_line = bounded_redacted_text(line);
        if rendered.len() + safe_line.len() + 1 > MAX_DIFF_BYTES {
            let limit = MAX_DIFF_BYTES.saturating_sub(TRUNCATION_NOTE.len() + 1);
            truncate_utf8(&mut rendered, limit);
            rendered.push_str(TRUNCATION_NOTE);
            break;
        }
        rendered.push_str(&safe_line);
        rendered.push('\n');
    }
    rendered.trim_end_matches('\n').to_owned()
}

fn count_occurrences(haystack: &[u8], needle: &[u8]) -> usize {
    if needle.is_empty() || needle.len() > haystack.len() {
        return 0;
    }
    haystack
        .windows(needle.len())
        .filter(|window| *window == needle)
        .count()
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn update_token(root_bytes: &[u8], before: &[u8], after: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(TOKEN_DOMAIN);
    for part in [root_bytes, before, after] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    format!("{:x}", digest.finalize())
}

fn is_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn path_bytes(path: &Path) -> Result<&[u8], AppError> {
    use std::os::unix::ffi::OsStrExt;
    Ok(path.as_os_str().as_bytes())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn path_bytes(_path: &Path) -> Result<&[u8], AppError> {
    Err(AppError::Message {
        message: "instructions: unsupported platform for lossless project paths".to_owned(),
    })
}

fn conflict(message: &'static str) -> AppError {
    AppError::Message {
        message: format!("instructions: conflict: {message}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        classification::*, classify, render_bounded_diff, render_update, Classification,
        InstructionsUpdate, UpdateStatus,
    };
    use std::path::PathBuf;

    #[test]
    fn classifies_exact_current_distribution_with_custom_bytes_as_current() {
        let mut bytes = b"prefix\r\n".to_vec();
        bytes.extend_from_slice(CURRENT_TEMPLATE);
        bytes.extend_from_slice(b"suffix\n");

        assert!(matches!(classify(&bytes).unwrap(), Classification::Current));
    }

    #[test]
    fn replaces_only_one_exact_legacy_distribution() {
        let mut bytes = b"prefix\r\n".to_vec();
        bytes.extend_from_slice(LEGACY_TEMPLATE);
        bytes.extend_from_slice(b"suffix\n");

        let Classification::UpdateAvailable { candidate } = classify(&bytes).unwrap() else {
            panic!("expected an available update");
        };
        assert!(candidate.starts_with(b"prefix\r\n"));
        assert!(candidate.ends_with(b"suffix\n"));
        assert!(candidate
            .windows(CURRENT_TEMPLATE.len())
            .any(|window| window == CURRENT_TEMPLATE));
    }

    #[test]
    fn rejects_duplicate_and_malformed_markers() {
        let mut duplicate = CURRENT_TEMPLATE.to_vec();
        duplicate.extend_from_slice(CURRENT_TEMPLATE);
        assert!(classify(&duplicate).is_err());

        let malformed = b"<!-- pueue-agent:instructions v1 begin -->\ncustom\n<!-- pueue-agent:instructions v1 end -->\n";
        assert!(classify(malformed).is_err());
    }

    #[test]
    fn rejects_reserved_marker_variants_around_current_and_legacy() {
        for extra_marker in [
            b"<!-- pueue-agent:instructions v2 begin -->\n".as_slice(),
            b"<!-- pueue-agent:instructions v1 begin\n".as_slice(),
        ] {
            assert!(classify(&[CURRENT_TEMPLATE, extra_marker].concat()).is_err());
            assert!(classify(&[LEGACY_TEMPLATE, extra_marker].concat()).is_err());
        }
    }

    #[test]
    fn renders_substantive_managed_instruction_diff() {
        let diff = render_bounded_diff(LEGACY_TEMPLATE, CURRENT_TEMPLATE);

        assert!(diff.contains("+## Standard role"));
        assert!(diff.contains("+## Diagnosis role"));
        assert!(diff.contains("-## Phase 2 decision agent"));
    }

    #[test]
    fn rejects_invalid_utf8_and_size_limits() {
        assert!(classify(&[0xff]).is_err());
        assert!(classify(&vec![b'x'; MAX_BYTES + 1]).is_err());
    }

    #[test]
    fn renders_bounded_safe_output_without_redacting_the_machine_token() {
        let token = "a".repeat(64);
        let result = InstructionsUpdate {
            status: UpdateStatus::UpdateAvailable,
            preview_token: Some(token.clone()),
            before_sha256: "b".repeat(64),
            after_sha256: "c".repeat(64),
            backup_path: Some(PathBuf::from("/tmp/instructions.backups/x.md")),
            diff: Some("\u{1b}[31m--token secret\u{1b}[0m".to_owned()),
        };

        let rendered = render_update(&result);
        assert!(rendered.contains(&format!("preview_token: {token}")));
        assert!(!rendered.contains('\u{1b}'));
        assert!(rendered.contains("output_note: diff and paths are bounded"));
        assert!(rendered.contains("[REDACTED]"));
    }
}
