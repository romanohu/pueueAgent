use crate::AppError;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SignalClass {
    Oom,
    Numerical,
    WorkerLoss,
    Staleness,
    Exception,
    Configured(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalSource {
    BuiltinProbe,
    ConfigPattern,
    Stall,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalObservation {
    pub class: SignalClass,
    pub source: SignalSource,
    pub evidence_digest: String,
    pub observed_at: i64,
}

const CLASSIFIER_RULES: &[(SignalClass, &[&str])] = &[
    (
        SignalClass::Oom,
        &["cuda out of memory", "outofmemoryerror", "oomkilled"],
    ),
    (
        SignalClass::Numerical,
        &["nan", "inf", "divergence"],
    ),
    (
        SignalClass::WorkerLoss,
        &["dataloader worker", "worker process died", "rank process"],
    ),
    (SignalClass::Exception, &["traceback"]),
];

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

pub(crate) fn evidence_digest(evidence: &str) -> String {
    format!("fnv1a64:{:016x}", fnv1a64(evidence.as_bytes()))
}

pub(crate) fn validate_class_name(value: &str) -> Result<(), AppError> {
    let valid = (1..=32).contains(&value.chars().count())
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c == '_');
    if valid {
        Ok(())
    } else {
        Err(AppError::Configuration {
            field: "check.patterns.class",
        })
    }
}

fn contains_at_word_boundary(haystack: &str, marker: &str) -> bool {
    let mut start = 0;
    while let Some(offset) = haystack[start..].find(marker) {
        let idx = start + offset;
        let end = idx + marker.len();
        let prev_alnum = haystack[..idx]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_ascii_alphanumeric());
        let next_alnum = haystack[end..]
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric());
        if !prev_alnum && !next_alnum {
            return true;
        }
        start = end;
    }
    false
}

pub fn classify_log_tail(tail: &str) -> Vec<SignalClass> {
    let lowered = tail.to_lowercase();
    let mut classes = CLASSIFIER_RULES
        .iter()
        .filter(|(class, markers)| {
            markers.iter().any(|marker| match class {
                SignalClass::Numerical => contains_at_word_boundary(&lowered, marker),
                _ => lowered.contains(marker),
            })
        })
        .map(|(class, _)| class.clone())
        .collect::<Vec<_>>();
    classes.sort();
    classes.dedup();
    classes
}

pub fn builtin_signal_observations(tail: &str, observed_at: i64) -> Vec<SignalObservation> {
    classify_log_tail(tail)
        .into_iter()
        .map(|class| SignalObservation {
            class,
            source: SignalSource::BuiltinProbe,
            evidence_digest: evidence_digest(tail),
            observed_at,
        })
        .collect()
}

pub fn configured_pattern_signal(
    class_name: &str,
    tail: &str,
    observed_at: i64,
) -> SignalObservation {
    SignalObservation {
        class: SignalClass::Configured(class_name.to_owned()),
        source: SignalSource::ConfigPattern,
        evidence_digest: evidence_digest(tail),
        observed_at,
    }
}

pub fn staleness_signal(tail: &str, observed_at: i64) -> SignalObservation {
    SignalObservation {
        class: SignalClass::Staleness,
        source: SignalSource::Stall,
        evidence_digest: evidence_digest(tail),
        observed_at,
    }
}

#[cfg(test)]
mod tests {
    use super::{classify_log_tail, SignalClass};

    #[test]
    fn builtin_probes_map_canonical_markers() {
        assert!(classify_log_tail("torch.cuda.OutOfMemoryError: CUDA out of memory")
            .contains(&SignalClass::Oom));
        assert!(classify_log_tail("epoch 3 loss=NaN").contains(&SignalClass::Numerical));
        assert!(classify_log_tail("RuntimeError: DataLoader worker (pid 12) is killed")
            .contains(&SignalClass::WorkerLoss));
        assert!(classify_log_tail("Traceback (most recent call last):")
            .contains(&SignalClass::Exception));
        assert!(classify_log_tail("all good here").is_empty());
    }

    #[test]
    fn info_log_lines_do_not_trigger_numerical() {
        let out = classify_log_tail("- INFO - training started");
        assert!(!out.contains(&SignalClass::Numerical));
    }

    #[test]
    fn classification_is_case_insensitive_and_deduplicated() {
        let out = classify_log_tail("nan\nNAN\nCUDA OUT OF MEMORY");
        assert_eq!(
            out.iter().filter(|c| **c == SignalClass::Numerical).count(),
            1
        );
    }
}
