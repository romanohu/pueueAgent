use std::io::IsTerminal;

const MAX_OUTPUT_TEXT_BYTES: usize = 240;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Human,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputTarget {
    Terminal,
    Pipe,
}

impl OutputTarget {
    pub fn detect() -> Self {
        if std::io::stdout().is_terminal() {
            Self::Terminal
        } else {
            Self::Pipe
        }
    }
}

impl OutputMode {
    pub fn uses_ansi(self, target: OutputTarget) -> bool {
        self == Self::Human
            && target == OutputTarget::Terminal
            && std::env::var_os("NO_COLOR").is_none()
    }
}

pub fn render_id(kind: &str, id: impl std::fmt::Display) -> String {
    format!("{kind} {id}")
}

pub fn format_state(state: &str) -> String {
    let state = state.to_ascii_lowercase();
    if !OutputMode::Human.uses_ansi(OutputTarget::detect()) {
        return state;
    }

    let color = match state.as_str() {
        "running" | "completed" | "confirmed" => "32",
        "queued" | "pending" | "claimed" | "starting" => "33",
        "failed" | "timed_out" | "cancelled" | "halted" => "31",
        _ => return state,
    };
    format!("\x1b[{color}m{state}\x1b[0m")
}

pub fn redact_sensitive_text(value: &str) -> String {
    let sanitized = strip_control_and_ansi(value);
    let tokens = sanitized.split_whitespace().collect::<Vec<_>>();
    let mut redacted = Vec::with_capacity(tokens.len());
    let mut redact_next = false;
    let mut redact_bearer_value = false;

    for token in tokens {
        if redact_bearer_value {
            if token.eq_ignore_ascii_case("bearer") {
                redacted.push(token.to_owned());
                redact_next = true;
            } else {
                redacted.push("[REDACTED]".to_owned());
            }
            redact_bearer_value = false;
            continue;
        }

        if redact_next {
            redacted.push("[REDACTED]".to_owned());
            redact_next = false;
            continue;
        }

        if let Some((key, _)) = token.split_once('=') {
            if is_sensitive_key(key) {
                redacted.push(format!("{key}=[REDACTED]"));
                continue;
            }
        }

        if let Some((key, _)) = token.split_once(':') {
            if is_sensitive_key(key) {
                redacted.push(format!("{key}:"));
                redact_bearer_value = true;
                continue;
            }
        }

        if let Some((flag, _)) = token.split_once('=') {
            if is_sensitive_flag(flag) {
                redacted.push(format!("{flag}=[REDACTED]"));
                continue;
            }
        }

        if is_sensitive_flag(token) || token.eq_ignore_ascii_case("bearer") {
            redacted.push(token.to_owned());
            redact_next = true;
            continue;
        }

        if is_path_token(token) {
            redacted.push("[path]".to_owned());
            continue;
        }

        if is_sensitive_marker(token) {
            redacted.push("[REDACTED]".to_owned());
            continue;
        }

        redacted.push(token.to_owned());
    }

    redacted.join(" ")
}

pub fn bounded_redacted_text(value: &str) -> String {
    let redacted = redact_sensitive_text(value);
    if redacted.len() <= MAX_OUTPUT_TEXT_BYTES {
        return redacted;
    }

    let mut prefix = String::new();
    for character in redacted.chars() {
        if prefix.len() + character.len_utf8() > MAX_OUTPUT_TEXT_BYTES - 3 {
            break;
        }
        prefix.push(character);
    }
    format!("{prefix}...")
}

fn is_sensitive_flag(value: &str) -> bool {
    let normalized = value
        .trim_start_matches('-')
        .to_ascii_lowercase()
        .replace('_', "-");
    matches!(
        normalized.as_str(),
        "token" | "api-key" | "password" | "secret" | "prompt" | "transcript"
    ) || normalized.ends_with("-token")
        || normalized.ends_with("-secret")
        || normalized.ends_with("-password")
        || normalized.ends_with("-key")
}

fn is_sensitive_key(value: &str) -> bool {
    let key = value.to_ascii_uppercase().replace('-', "_");
    key.contains("TOKEN")
        || key.contains("SECRET")
        || key.contains("PASSWORD")
        || key.contains("PASSWD")
        || key.contains("ACCESS_KEY")
        || key.contains("API_KEY")
        || key.contains("AUTHORIZATION")
        || key.contains("CREDENTIAL")
        || key.contains("PRIVATE_KEY")
}

fn is_sensitive_marker(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    [
        "token",
        "secret",
        "password",
        "passwd",
        "prompt",
        "transcript",
        "log_path",
        "session_id",
        "cookie",
        "apikey",
        "api_key",
        "authorization",
        "bearer",
        "credential",
        "private_key",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

fn is_path_token(token: &str) -> bool {
    token.starts_with('/')
        || token.starts_with("~/")
        || token.starts_with("./")
        || token.starts_with("../")
        || token.contains('/')
        || token.contains('\\')
        || token
            .as_bytes()
            .get(1)
            .is_some_and(|character| *character == b':')
}

fn strip_control_and_ansi(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut characters = value.chars().peekable();

    while let Some(character) = characters.next() {
        if character != '\x1b' {
            if character.is_control() {
                output.push(' ');
            } else {
                output.push(character);
            }
            continue;
        }

        match characters.peek().copied() {
            Some('[') => {
                characters.next();
                for sequence_character in characters.by_ref() {
                    if ('@'..='~').contains(&sequence_character) {
                        break;
                    }
                }
            }
            Some(']') => {
                characters.next();
                let mut previous = None;
                for sequence_character in characters.by_ref() {
                    if sequence_character == '\x07'
                        || (previous == Some('\x1b') && sequence_character == '\\')
                    {
                        break;
                    }
                    previous = Some(sequence_character);
                }
            }
            _ => {}
        }
    }

    output
}
