use serde::de::{DeserializeSeed, Deserializer};
use serde::{de, Deserialize, Serialize};
use serde_json::{Map, Number, Value};

use crate::{process::MAX_FIELD_SIZE, AppError};

pub const MAX_RESEARCH_ANSWER_BYTES: usize = 128 * 1024;
pub const MAX_RESEARCH_REASON_BYTES: usize = 4 * 1024;
pub const MAX_RESEARCH_NOTES_BYTES: usize = 4 * 1024;
pub const MAX_RESEARCH_NEXT_DIRECTION_BYTES: usize = 4 * 1024;
pub const MAX_RESEARCH_ID_BYTES: usize = 128;
pub const MAX_RESEARCH_PATH_BYTES: usize = 4 * 1024;
pub const MAX_RESEARCH_EVIDENCE_REFS: usize = 16;
pub const MAX_RESEARCH_EVIDENCE_REF_BYTES: usize = 512;
pub const MAX_RESEARCH_ARGV: usize = 256;
pub const MAX_RESEARCH_EVENT_BYTES: usize = 1024 * 1024;

pub const RESEARCH_OUTPUT_SCHEMA: &[u8] = br#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["schema_version", "review_id", "experiment_id", "context_digest", "action", "reason", "evidence_refs", "notes", "next_direction", "checkpoint"],
  "properties": {
    "schema_version": {"const": 1},
    "review_id": {"type": "string", "minLength": 1, "maxLength": 128},
    "experiment_id": {"type": "string", "minLength": 1, "maxLength": 128},
    "context_digest": {"type": "string", "pattern": "^[0-9A-Fa-f]{64}$"},
    "action": {"enum": ["continue", "stop_and_next", "resume_from_checkpoint"]},
    "reason": {"type": "string", "maxLength": 4096},
    "evidence_refs": {"type": "array", "maxItems": 16, "items": {"type": "string", "minLength": 1, "maxLength": 512}},
    "notes": {"type": "string", "maxLength": 4096},
    "next_direction": {"type": ["string", "null"], "maxLength": 4096},
    "checkpoint": {
      "type": ["object", "null"],
      "additionalProperties": false,
      "required": ["path", "argv", "working_directory", "support_evidence_refs"],
      "properties": {
        "path": {"type": "string", "minLength": 1, "maxLength": 4096},
        "argv": {"type": "array", "minItems": 1, "maxItems": 256, "items": {"type": "string", "maxLength": 65536}},
        "working_directory": {"type": "string", "minLength": 1, "maxLength": 4096},
        "support_evidence_refs": {"type": "array", "minItems": 1, "maxItems": 16, "items": {"type": "string", "minLength": 1, "maxLength": 512}}
      }
    }
  }
}"#;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchAnswer {
    pub schema_version: u8,
    pub review_id: String,
    pub experiment_id: String,
    pub context_digest: String,
    pub action: String,
    pub reason: String,
    pub evidence_refs: Vec<String>,
    pub notes: String,
    pub next_direction: Option<String>,
    pub checkpoint: Option<CheckpointRequest>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointRequest {
    pub path: String,
    pub argv: Vec<String>,
    pub working_directory: String,
    pub support_evidence_refs: Vec<String>,
}

pub fn parse_research_answer(bytes: &[u8]) -> Result<ResearchAnswer, AppError> {
    if bytes.is_empty() || bytes.len() > MAX_RESEARCH_ANSWER_BYTES {
        return Err(validation_error(
            "research_answer",
            "must be 1 to 131072 bytes",
        ));
    }

    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = StrictValue
        .deserialize(&mut deserializer)
        .map_err(|source| AppError::Serialization {
            operation: "parse research answer",
            source,
        })?;
    deserializer
        .end()
        .map_err(|source| AppError::Serialization {
            operation: "parse trailing research answer document",
            source,
        })?;
    let answer = serde_json::from_value(value).map_err(|source| AppError::Serialization {
        operation: "validate research answer schema",
        source,
    })?;
    validate_research_answer(answer)
}

/// Parse the bounded JSONL event stream produced by a research Codex run.
///
/// Only a top-level `thread.started` event is authoritative.  Text nested in
/// another event is never interpreted as session identity, and exactly one
/// started event is required.
pub fn parse_research_thread_id(bytes: &[u8]) -> Result<String, AppError> {
    if bytes.is_empty() || bytes.len() > MAX_RESEARCH_EVENT_BYTES {
        return Err(validation_error(
            "research.thread_events",
            "must be 1 to 1048576 bytes",
        ));
    }
    let mut thread_id = None;
    for line in bytes.split(|byte| *byte == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let value: Value = serde_json::from_slice(line).map_err(|source| AppError::Serialization {
            operation: "parse research event stream",
            source,
        })?;
        let object = value.as_object().ok_or_else(|| validation_error(
            "research.thread_events",
            "each event must be a JSON object",
        ))?;
        if object.get("type").and_then(Value::as_str) != Some("thread.started") {
            continue;
        }
        let value = object
            .get("thread_id")
            .and_then(Value::as_str)
            .ok_or_else(|| validation_error("research.thread_id", "thread.started must include thread_id"))?;
        let value = crate::codex_session::normalize_session_id(value).map_err(|_| {
            validation_error("research.thread_id", "must be a normalized UUID")
        })?;
        if thread_id.replace(value).is_some() {
            return Err(validation_error(
                "research.thread_id",
                "must contain exactly one thread.started event",
            ));
        }
    }
    thread_id.ok_or_else(|| {
        validation_error(
            "research.thread_id",
            "must contain exactly one thread.started event",
        )
    })
}

fn validate_research_answer(answer: ResearchAnswer) -> Result<ResearchAnswer, AppError> {
    if answer.schema_version != 1 {
        return Err(validation_error("schema_version", "must be 1"));
    }
    validate_identifier("review_id", &answer.review_id)?;
    validate_identifier("experiment_id", &answer.experiment_id)?;
    if answer.context_digest.len() != 64
        || !answer
            .context_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(validation_error(
            "context_digest",
            "must be exactly 64 hexadecimal bytes",
        ));
    }
    validate_text("reason", &answer.reason, MAX_RESEARCH_REASON_BYTES, false)?;
    validate_text("notes", &answer.notes, MAX_RESEARCH_NOTES_BYTES, false)?;
    validate_evidence_refs("evidence_refs", &answer.evidence_refs, false)?;

    match answer.action.as_str() {
        "continue" => {
            if answer.next_direction.is_some() || answer.checkpoint.is_some() {
                return Err(validation_error(
                    "action",
                    "continue must not include next_direction or checkpoint",
                ));
            }
        }
        "stop_and_next" => {
            let Some(next_direction) = answer.next_direction.as_deref() else {
                return Err(validation_error(
                    "next_direction",
                    "must be present for stop_and_next",
                ));
            };
            validate_text(
                "next_direction",
                next_direction,
                MAX_RESEARCH_NEXT_DIRECTION_BYTES,
                true,
            )?;
            if answer.checkpoint.is_some() {
                return Err(validation_error(
                    "checkpoint",
                    "must be absent for stop_and_next",
                ));
            }
            if answer.evidence_refs.is_empty() {
                return Err(validation_error(
                    "evidence_refs",
                    "must be non-empty for stop_and_next",
                ));
            }
        }
        "resume_from_checkpoint" => {
            if answer.next_direction.is_some() {
                return Err(validation_error(
                    "next_direction",
                    "must be absent for resume_from_checkpoint",
                ));
            }
            let Some(checkpoint) = answer.checkpoint.as_ref() else {
                return Err(validation_error(
                    "checkpoint",
                    "must be present for resume_from_checkpoint",
                ));
            };
            validate_checkpoint(checkpoint)?;
        }
        _ => {
            return Err(validation_error(
                "action",
                "must be continue, stop_and_next, or resume_from_checkpoint",
            ));
        }
    }
    Ok(answer)
}

fn validate_checkpoint(checkpoint: &CheckpointRequest) -> Result<(), AppError> {
    validate_text(
        "checkpoint.path",
        &checkpoint.path,
        MAX_RESEARCH_PATH_BYTES,
        true,
    )?;
    validate_text(
        "checkpoint.working_directory",
        &checkpoint.working_directory,
        MAX_RESEARCH_PATH_BYTES,
        true,
    )?;
    if checkpoint.argv.is_empty() {
        return Err(validation_error("checkpoint.argv", "must be non-empty"));
    }
    if checkpoint.argv.len() > MAX_RESEARCH_ARGV {
        return Err(validation_error(
            "checkpoint.argv",
            "contains too many items",
        ));
    }
    for argument in &checkpoint.argv {
        validate_text("checkpoint.argv", argument, MAX_FIELD_SIZE, false)?;
    }
    validate_evidence_refs(
        "checkpoint.support_evidence_refs",
        &checkpoint.support_evidence_refs,
        true,
    )
}

fn validate_evidence_refs(
    field: &'static str,
    refs: &[String],
    require_nonempty: bool,
) -> Result<(), AppError> {
    if refs.len() > MAX_RESEARCH_EVIDENCE_REFS {
        return Err(validation_error(field, "contains too many references"));
    }
    if require_nonempty && refs.is_empty() {
        return Err(validation_error(field, "must be non-empty"));
    }
    for reference in refs {
        validate_text(field, reference, MAX_RESEARCH_EVIDENCE_REF_BYTES, true)?;
    }
    Ok(())
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), AppError> {
    validate_text(field, value, MAX_RESEARCH_ID_BYTES, true)
}

fn validate_text(
    field: &'static str,
    value: &str,
    maximum_bytes: usize,
    require_nonempty: bool,
) -> Result<(), AppError> {
    if (require_nonempty && value.is_empty())
        || value.len() > maximum_bytes
        || value.chars().any(char::is_control)
    {
        return Err(validation_error(
            field,
            "is empty, oversized, or contains control characters",
        ));
    }
    Ok(())
}

struct StrictValue;

impl<'de> DeserializeSeed<'de> for StrictValue {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictValueVisitor)
    }
}

struct StrictValueVisitor;

impl<'de> de::Visitor<'de> for StrictValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Number(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("JSON number is not finite"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Null)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(StrictValue)? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut map_access: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut values = Map::new();
        while let Some(key) = map_access.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom("duplicate JSON object key"));
            }
            let value = map_access.next_value_seed(StrictValue)?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

fn validation_error(field: &'static str, message: &'static str) -> AppError {
    AppError::Validation { field, message }
}

#[cfg(test)]
mod tests {
    use super::{parse_research_answer, parse_research_thread_id};
    use serde_json::json;

    fn valid_answer() -> serde_json::Value {
        json!({
            "schema_version": 1,
            "review_id": "review-1",
            "experiment_id": "experiment-1",
            "context_digest": "a".repeat(64),
            "action": "continue",
            "reason": "progress remains measurable",
            "evidence_refs": ["task:41:tail"],
            "notes": "check the next epoch",
            "next_direction": null,
            "checkpoint": null
        })
    }

    #[test]
    fn continue_forbids_next_direction() {
        let value = serde_json::json!({
            "schema_version": 1, "review_id": "review-1",
            "experiment_id": "experiment-1", "context_digest": "a".repeat(64),
            "action": "continue", "reason": "progress remains measurable",
            "evidence_refs": ["task:41:tail"], "notes": "check the next epoch",
            "next_direction": "replace the model", "checkpoint": null
        });
        assert!(parse_research_answer(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn parser_accepts_each_supported_action_with_its_payload() {
        let mut continue_answer = valid_answer();
        assert!(parse_research_answer(&serde_json::to_vec(&continue_answer).unwrap()).is_ok());

        continue_answer["action"] = json!("stop_and_next");
        continue_answer["next_direction"] = json!("try a smaller learning rate");
        assert!(parse_research_answer(&serde_json::to_vec(&continue_answer).unwrap()).is_ok());

        continue_answer["action"] = json!("resume_from_checkpoint");
        continue_answer["next_direction"] = serde_json::Value::Null;
        continue_answer["checkpoint"] = json!({
            "path": "checkpoints/latest.pt",
            "argv": ["python", "train.py", "--resume"],
            "working_directory": ".",
            "support_evidence_refs": ["artifact:checkpoint"]
        });
        assert!(parse_research_answer(&serde_json::to_vec(&continue_answer).unwrap()).is_ok());
    }

    #[test]
    fn parser_rejects_unknown_top_level_and_nested_fields() {
        let mut top_level = valid_answer();
        top_level["extra"] = json!(true);
        assert!(parse_research_answer(&serde_json::to_vec(&top_level).unwrap()).is_err());

        let mut nested = valid_answer();
        nested["action"] = json!("resume_from_checkpoint");
        nested["checkpoint"] = json!({
            "path": "checkpoints/latest.pt",
            "argv": ["python"],
            "working_directory": ".",
            "support_evidence_refs": ["artifact:checkpoint"],
            "extra": "reject"
        });
        assert!(parse_research_answer(&serde_json::to_vec(&nested).unwrap()).is_err());
    }

    #[test]
    fn parser_rejects_mixed_action_payloads() {
        let mut value = valid_answer();
        value["action"] = json!("stop_and_next");
        value["next_direction"] = json!("try another direction");
        value["checkpoint"] = json!({
            "path": "checkpoints/latest.pt",
            "argv": ["python"],
            "working_directory": ".",
            "support_evidence_refs": ["artifact:checkpoint"]
        });
        assert!(parse_research_answer(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn parser_rejects_invalid_id_and_digest() {
        let mut invalid_id = valid_answer();
        invalid_id["review_id"] = json!("review\u{0000}id");
        assert!(parse_research_answer(&serde_json::to_vec(&invalid_id).unwrap()).is_err());

        let mut invalid_digest = valid_answer();
        invalid_digest["context_digest"] = json!("g".repeat(64));
        assert!(parse_research_answer(&serde_json::to_vec(&invalid_digest).unwrap()).is_err());
    }

    #[test]
    fn parser_rejects_absent_target() {
        let mut value = valid_answer();
        value.as_object_mut().unwrap().remove("experiment_id");
        assert!(parse_research_answer(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn parser_rejects_duplicate_json_keys() {
        let duplicate_top_level = br#"{"schema_version":1,"review_id":"review-1","review_id":"review-2","experiment_id":"experiment-1","context_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","action":"continue","reason":"ok","evidence_refs":[],"notes":"ok","next_direction":null,"checkpoint":null}"#;
        assert!(parse_research_answer(duplicate_top_level).is_err());

        let duplicate_nested = br#"{"schema_version":1,"review_id":"review-1","experiment_id":"experiment-1","context_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","action":"resume_from_checkpoint","reason":"ok","evidence_refs":[],"notes":"ok","next_direction":null,"checkpoint":{"path":"a","path":"b","argv":["python"],"working_directory":".","support_evidence_refs":["artifact:checkpoint"]}}"#;
        assert!(parse_research_answer(duplicate_nested).is_err());
    }

    #[test]
    fn parser_rejects_trailing_document() {
        let mut bytes = serde_json::to_vec(&valid_answer()).unwrap();
        bytes.extend_from_slice(br"{} ");
        assert!(parse_research_answer(&bytes).is_err());
    }

    #[test]
    fn parser_rejects_invalid_utf8() {
        let mut bytes = serde_json::to_vec(&valid_answer()).unwrap();
        bytes.extend_from_slice(&[0xff]);
        assert!(parse_research_answer(&bytes).is_err());
    }

    #[test]
    fn parser_counts_utf8_limits_by_bytes() {
        let mut value = valid_answer();
        value["reason"] = json!("é".repeat(2049));
        assert!(serde_json::to_vec(&value).unwrap().len() > 4 * 1024);
        assert!(parse_research_answer(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn parser_rejects_more_than_sixteen_evidence_references() {
        let mut value = valid_answer();
        value["evidence_refs"] = json!((0..17)
            .map(|index| format!("fact:{index}"))
            .collect::<Vec<_>>());
        value["action"] = json!("stop_and_next");
        value["next_direction"] = json!("try another direction");
        assert!(parse_research_answer(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn parser_rejects_invalid_argv_type() {
        let mut value = valid_answer();
        value["action"] = json!("resume_from_checkpoint");
        value["checkpoint"] = json!({
            "path": "checkpoints/latest.pt",
            "argv": "python train.py",
            "working_directory": ".",
            "support_evidence_refs": ["artifact:checkpoint"]
        });
        assert!(parse_research_answer(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn thread_parser_accepts_one_top_level_started_event() {
        let bytes = br#"{"type":"message","payload":{"text":"{\"type\":\"thread.started\"}"}}
{"type":"thread.started","thread_id":"11111111-1111-4111-8111-111111111111"}
{"type":"turn.completed"}
"#;
        assert_eq!(
            parse_research_thread_id(bytes).unwrap(),
            "11111111-1111-4111-8111-111111111111"
        );
    }

    #[test]
    fn thread_parser_rejects_malformed_or_duplicate_identity() {
        let malformed = br#"{"type":"thread.started","thread_id":"11111111-1111-4111-8111-111111111111"
"#;
        assert!(parse_research_thread_id(malformed).is_err());

        let duplicate = br#"{"type":"thread.started","thread_id":"11111111-1111-4111-8111-111111111111"}
{"type":"thread.started","thread_id":"22222222-2222-4222-8222-222222222222"}
"#;
        assert!(parse_research_thread_id(duplicate).is_err());
    }
}
