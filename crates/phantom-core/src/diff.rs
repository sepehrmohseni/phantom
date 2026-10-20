use std::collections::BTreeSet;

use serde_json::{Map, Number, Value};

use crate::error::{PhantomError, PhantomResult};
use crate::models::{DiffKind, JsonDiffEntry};

pub const ROOT_PATH: &str = "$";

pub fn compute_json_diff(expected: &Value, actual: &Value) -> Vec<JsonDiffEntry> {
    let mut entries = Vec::new();
    diff_node(ROOT_PATH, expected, actual, &mut entries);
    entries
}

pub fn compute_body_diff(expected: &[u8], actual: &[u8]) -> Vec<JsonDiffEntry> {
    if expected == actual {
        return Vec::new();
    }
    match (DecodedBody::decode(expected), DecodedBody::decode(actual)) {
        (DecodedBody::Json(left), DecodedBody::Json(right))
        | (DecodedBody::Form(left), DecodedBody::Form(right)) => compute_json_diff(&left, &right),
        (DecodedBody::Empty, DecodedBody::Empty) => Vec::new(),
        (DecodedBody::Empty, right) => vec![JsonDiffEntry::new(
            ROOT_PATH,
            DiffKind::Added,
            None,
            right.into_value(),
        )],
        (left, DecodedBody::Empty) => vec![JsonDiffEntry::new(
            ROOT_PATH,
            DiffKind::Removed,
            left.into_value(),
            None,
        )],
        (left, right) => {
            let kind = if left.is_structured() || right.is_structured() {
                DiffKind::TypeMismatch
            } else {
                DiffKind::Modified
            };
            vec![JsonDiffEntry::new(
                ROOT_PATH,
                kind,
                left.into_value(),
                right.into_value(),
            )]
        }
    }
}

pub fn decode_body(bytes: &[u8]) -> Option<Value> {
    DecodedBody::decode(bytes).into_value()
}

pub fn decode_json(bytes: &[u8]) -> Option<Value> {
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return None;
    }
    serde_json::from_slice(bytes).ok()
}

pub fn decode_form(bytes: &[u8]) -> Option<Value> {
    let text = std::str::from_utf8(bytes).ok()?.trim();
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_graphic()) {
        return None;
    }
    let mut fields = Map::new();
    for pair in text.split('&').filter(|pair| !pair.is_empty()) {
        let (raw_key, raw_value) = pair.split_once('=')?;
        let key = percent_decode(raw_key)?;
        if key.is_empty() {
            return None;
        }
        let value = Value::String(percent_decode(raw_value)?);
        match fields.get_mut(&key) {
            Some(Value::Array(values)) => values.push(value),
            Some(existing) => {
                let first = existing.take();
                *existing = Value::Array(vec![first, value]);
            }
            None => {
                fields.insert(key, value);
            }
        }
    }
    (!fields.is_empty()).then_some(Value::Object(fields))
}

fn percent_decode(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' => {
                let digits = bytes.get(index + 1..index + 3)?;
                if !digits.iter().all(u8::is_ascii_hexdigit) {
                    return None;
                }
                let text = std::str::from_utf8(digits).ok()?;
                decoded.push(u8::from_str_radix(text, 16).ok()?);
                index += 3;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded).ok()
}

pub fn canonical_json(value: &Value) -> String {
    let mut buffer = String::new();
    write_canonical(value, &mut buffer);
    buffer
}

pub fn child_key_path(parent: &str, key: &str) -> String {
    if is_identifier(key) {
        format!("{parent}.{key}")
    } else {
        format!("{parent}[{}]", Value::String(key.to_owned()))
    }
}

pub fn child_index_path(parent: &str, index: usize) -> String {
    format!("{parent}[{index}]")
}

enum DecodedBody {
    Empty,
    Json(Value),
    Form(Value),
    Text(String),
    Binary(Vec<u8>),
}

impl DecodedBody {
    fn decode(bytes: &[u8]) -> Self {
        if bytes.iter().all(u8::is_ascii_whitespace) {
            return Self::Empty;
        }
        if let Ok(json) = serde_json::from_slice::<Value>(bytes) {
            return Self::Json(json);
        }
        if let Some(form) = decode_form(bytes) {
            return Self::Form(form);
        }
        match std::str::from_utf8(bytes) {
            Ok(text) => Self::Text(text.to_owned()),
            Err(_) => Self::Binary(bytes.to_vec()),
        }
    }

    fn is_structured(&self) -> bool {
        matches!(self, Self::Json(_) | Self::Form(_))
    }

    fn into_value(self) -> Option<Value> {
        match self {
            Self::Empty => None,
            Self::Json(json) => Some(json),
            Self::Form(form) => Some(form),
            Self::Text(text) => Some(Value::String(text)),
            Self::Binary(bytes) => Some(Value::String(format!("0x{}", hex::encode(bytes)))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonType {
    Null,
    Bool,
    Number,
    String,
    Array,
    Object,
}

impl JsonType {
    fn of(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(_) => Self::Bool,
            Value::Number(_) => Self::Number,
            Value::String(_) => Self::String,
            Value::Array(_) => Self::Array,
            Value::Object(_) => Self::Object,
        }
    }
}

fn diff_node(path: &str, expected: &Value, actual: &Value, entries: &mut Vec<JsonDiffEntry>) {
    match (expected, actual) {
        (Value::Object(left), Value::Object(right)) => diff_objects(path, left, right, entries),
        (Value::Array(left), Value::Array(right)) => diff_arrays(path, left, right, entries),
        _ if JsonType::of(expected) != JsonType::of(actual) => entries.push(JsonDiffEntry::new(
            path,
            DiffKind::TypeMismatch,
            Some(expected.clone()),
            Some(actual.clone()),
        )),
        _ if !scalars_equal(expected, actual) => entries.push(JsonDiffEntry::new(
            path,
            DiffKind::Modified,
            Some(expected.clone()),
            Some(actual.clone()),
        )),
        _ => {}
    }
}

fn diff_objects(
    path: &str,
    expected: &Map<String, Value>,
    actual: &Map<String, Value>,
    entries: &mut Vec<JsonDiffEntry>,
) {
    let keys: BTreeSet<&str> = expected
        .keys()
        .chain(actual.keys())
        .map(String::as_str)
        .collect();
    for key in keys {
        match (expected.get(key), actual.get(key)) {
            (Some(left), Some(right)) => {
                diff_node(&child_key_path(path, key), left, right, entries)
            }
            (Some(left), None) => entries.push(JsonDiffEntry::new(
                child_key_path(path, key),
                DiffKind::Removed,
                Some(left.clone()),
                None,
            )),
            (None, Some(right)) => entries.push(JsonDiffEntry::new(
                child_key_path(path, key),
                DiffKind::Added,
                None,
                Some(right.clone()),
            )),
            (None, None) => {}
        }
    }
}

fn diff_arrays(path: &str, expected: &[Value], actual: &[Value], entries: &mut Vec<JsonDiffEntry>) {
    for index in 0..expected.len().max(actual.len()) {
        match (expected.get(index), actual.get(index)) {
            (Some(left), Some(right)) => {
                diff_node(&child_index_path(path, index), left, right, entries)
            }
            (Some(left), None) => entries.push(JsonDiffEntry::new(
                child_index_path(path, index),
                DiffKind::Removed,
                Some(left.clone()),
                None,
            )),
            (None, Some(right)) => entries.push(JsonDiffEntry::new(
                child_index_path(path, index),
                DiffKind::Added,
                None,
                Some(right.clone()),
            )),
            (None, None) => {}
        }
    }
}

fn scalars_equal(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Number(left), Value::Number(right)) => numbers_equal(left, right),
        _ => expected == actual,
    }
}

fn numbers_equal(left: &Number, right: &Number) -> bool {
    if left == right {
        return true;
    }
    if left.is_f64() || right.is_f64() {
        return matches!((left.as_f64(), right.as_f64()), (Some(a), Some(b)) if a == b);
    }
    false
}

fn is_identifier(key: &str) -> bool {
    let mut chars = key.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    }
}

fn write_canonical(value: &Value, buffer: &mut String) {
    match value {
        Value::Object(map) => {
            let mut fields: Vec<(&String, &Value)> = map.iter().collect();
            fields.sort_unstable_by(|left, right| left.0.cmp(right.0));
            buffer.push('{');
            for (position, (key, child)) in fields.into_iter().enumerate() {
                if position > 0 {
                    buffer.push(',');
                }
                buffer.push_str(&Value::String(key.clone()).to_string());
                buffer.push(':');
                write_canonical(child, buffer);
            }
            buffer.push('}');
        }
        Value::Array(items) => {
            buffer.push('[');
            for (position, item) in items.iter().enumerate() {
                if position > 0 {
                    buffer.push(',');
                }
                write_canonical(item, buffer);
            }
            buffer.push(']');
        }
        Value::Number(number) => buffer.push_str(&canonical_number(number)),
        scalar => buffer.push_str(&scalar.to_string()),
    }
}

fn canonical_number(number: &Number) -> String {
    const EXACT_INTEGER_LIMIT: f64 = 9_007_199_254_740_992.0;
    match number.as_f64() {
        Some(float)
            if number.is_f64() && float.fract() == 0.0 && float.abs() < EXACT_INTEGER_LIMIT =>
        {
            format!("{}", float as i64)
        }
        _ => number.to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Key(String),
    Index(usize),
    Wildcard,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IgnoreRules {
    sources: Vec<String>,
    patterns: Vec<Vec<Segment>>,
}

impl IgnoreRules {
    pub fn parse<S: AsRef<str>>(patterns: &[S]) -> PhantomResult<Self> {
        let mut rules = Self::default();
        for raw in patterns {
            let raw = raw.as_ref().trim();
            if raw.is_empty() {
                continue;
            }
            let segments =
                parse_path(raw).ok_or_else(|| PhantomError::InvalidPattern(raw.to_owned()))?;
            rules.sources.push(raw.to_owned());
            rules.patterns.push(segments);
        }
        Ok(rules)
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    pub fn sources(&self) -> &[String] {
        &self.sources
    }

    pub fn ignores(&self, path: &str) -> bool {
        let Some(target) = parse_path(path) else {
            return false;
        };
        self.patterns.iter().any(|pattern| {
            pattern.len() <= target.len()
                && pattern.iter().zip(&target).all(|(expected, actual)| {
                    matches!(expected, Segment::Wildcard) || expected == actual
                })
        })
    }

    pub fn retain(&self, entries: Vec<JsonDiffEntry>) -> Vec<JsonDiffEntry> {
        if self.is_empty() {
            return entries;
        }
        entries
            .into_iter()
            .filter(|entry| !self.ignores(&entry.path))
            .collect()
    }
}

fn parse_path(raw: &str) -> Option<Vec<Segment>> {
    let trimmed = raw.trim();
    let normalized = if let Some(rest) = trimmed.strip_prefix(ROOT_PATH) {
        rest.to_owned()
    } else if trimmed.starts_with('[') {
        trimmed.to_owned()
    } else {
        format!(".{trimmed}")
    };
    let mut segments = Vec::new();
    let mut cursor = normalized.as_str();
    while !cursor.is_empty() {
        if let Some(rest) = cursor.strip_prefix('.') {
            let end = rest.find(['.', '[']).unwrap_or(rest.len());
            let name = &rest[..end];
            if name.is_empty() {
                return None;
            }
            segments.push(if name == "*" {
                Segment::Wildcard
            } else {
                Segment::Key(name.to_owned())
            });
            cursor = &rest[end..];
        } else if let Some(rest) = cursor.strip_prefix('[') {
            if rest.starts_with('"') {
                let mut stream = serde_json::Deserializer::from_str(rest).into_iter::<String>();
                let key = stream.next()?.ok()?;
                let consumed = stream.byte_offset();
                cursor = rest.get(consumed..)?.strip_prefix(']')?;
                segments.push(Segment::Key(key));
            } else {
                let end = rest.find(']')?;
                let token = rest[..end].trim();
                segments.push(if token == "*" {
                    Segment::Wildcard
                } else {
                    Segment::Index(token.parse().ok()?)
                });
                cursor = &rest[end + 1..];
            }
        } else {
            return None;
        }
    }
    Some(segments)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn kinds(entries: &[JsonDiffEntry]) -> Vec<(&str, DiffKind)> {
        entries
            .iter()
            .map(|entry| (entry.path.as_str(), entry.kind))
            .collect()
    }

    #[test]
    fn identical_documents_produce_no_entries() {
        let document = json!({ "order": { "id": 7, "lines": [1, 2, 3] } });
        assert!(compute_json_diff(&document, &document.clone()).is_empty());
    }

    #[test]
    fn nested_drift_is_reported_with_json_paths() {
        let expected = json!({
            "payment": { "amount": 4200, "currency": "usd" },
            "items": [{ "status": "paid" }, { "status": "paid" }, { "status": "paid" }]
        });
        let actual = json!({
            "payment": { "amount": "4200", "currency": "usd", "captured": true },
            "items": [{ "status": "paid" }, { "status": "paid" }, { "status": "refunded" }]
        });
        assert_eq!(
            kinds(&compute_json_diff(&expected, &actual)),
            vec![
                ("$.items[2].status", DiffKind::Modified),
                ("$.payment.amount", DiffKind::TypeMismatch),
                ("$.payment.captured", DiffKind::Added),
            ]
        );
    }

    #[test]
    fn array_length_changes_surface_as_added_and_removed() {
        let shorter = compute_json_diff(&json!([1, 2, 3]), &json!([1, 2]));
        assert_eq!(kinds(&shorter), vec![("$[2]", DiffKind::Removed)]);
        let longer = compute_json_diff(&json!([1]), &json!([1, 9]));
        assert_eq!(kinds(&longer), vec![("$[1]", DiffKind::Added)]);
    }

    #[test]
    fn integer_and_float_encodings_of_the_same_number_match() {
        assert!(compute_json_diff(&json!({ "total": 100 }), &json!({ "total": 100.0 })).is_empty());
        assert_eq!(
            kinds(&compute_json_diff(
                &json!({ "total": 100 }),
                &json!({ "total": 100.5 })
            )),
            vec![("$.total", DiffKind::Modified)]
        );
    }

    #[test]
    fn non_identifier_keys_use_bracket_notation() {
        let entries = compute_json_diff(
            &json!({ "content-type": "a" }),
            &json!({ "content-type": "b" }),
        );
        assert_eq!(
            kinds(&entries),
            vec![("$[\"content-type\"]", DiffKind::Modified)]
        );
    }

    #[test]
    fn formatting_only_changes_are_not_drift() {
        assert!(compute_body_diff(
            br#"{"a":1,"b":[true]}"#,
            b"{ \"b\" : [ true ], \"a\" : 1 }\n"
        )
        .is_empty());
    }

    #[test]
    fn plain_text_bodies_fall_back_to_utf8_comparison() {
        let entries = compute_body_diff(b"pong", b"PONG");
        assert_eq!(kinds(&entries), vec![(ROOT_PATH, DiffKind::Modified)]);
        assert_eq!(entries[0].expected, Some(json!("pong")));
        assert_eq!(entries[0].actual, Some(json!("PONG")));
    }

    #[test]
    fn json_replaced_by_text_is_a_type_mismatch() {
        let entries = compute_body_diff(br#"{"ok":true}"#, b"internal error");
        assert_eq!(kinds(&entries), vec![(ROOT_PATH, DiffKind::TypeMismatch)]);
    }

    #[test]
    fn empty_bodies_are_reported_as_added_or_removed() {
        assert_eq!(
            kinds(&compute_body_diff(b"", b"{}")),
            vec![(ROOT_PATH, DiffKind::Added)]
        );
        assert_eq!(
            kinds(&compute_body_diff(b"{}", b"")),
            vec![(ROOT_PATH, DiffKind::Removed)]
        );
    }

    #[test]
    fn binary_bodies_are_compared_as_hex() {
        let entries = compute_body_diff(&[0xff, 0x00], &[0xff, 0x01]);
        assert_eq!(entries[0].expected, Some(json!("0xff00")));
        assert_eq!(entries[0].actual, Some(json!("0xff01")));
    }

    #[test]
    fn form_bodies_diff_field_by_field() {
        let entries = compute_body_diff(
            b"amount=4200&currency=usd&note=hello+world",
            b"amount=4300&currency=usd&note=hello%20world",
        );
        assert_eq!(kinds(&entries), vec![("$.amount", DiffKind::Modified)]);
    }

    #[test]
    fn switching_between_json_and_form_is_a_type_mismatch() {
        let entries = compute_body_diff(br#"{"a":"1"}"#, b"a=1");
        assert_eq!(kinds(&entries), vec![(ROOT_PATH, DiffKind::TypeMismatch)]);
    }

    #[test]
    fn canonical_numbers_match_the_diff_rules() {
        assert_eq!(
            canonical_json(&json!({ "total": 100.0 })),
            canonical_json(&json!({ "total": 100 }))
        );
        assert_eq!(canonical_json(&json!(-0.0)), "0");
        assert_eq!(canonical_json(&json!(12.5)), "12.5");
    }

    #[test]
    fn form_decoding_handles_repeats_and_rejects_prose() {
        assert_eq!(
            decode_form(b"tag=a&tag=b&x=%7E"),
            Some(json!({ "tag": ["a", "b"], "x": "~" }))
        );
        assert_eq!(decode_form(b"just some text"), None);
        assert_eq!(decode_form(b"broken=%zz"), None);
        assert_eq!(decode_form(b"=value"), None);
    }

    #[test]
    fn canonical_json_sorts_keys_recursively() {
        let value = json!({ "b": { "z": 1, "a": [{ "y": 2, "x": 1 }] }, "a": null });
        assert_eq!(
            canonical_json(&value),
            r#"{"a":null,"b":{"a":[{"x":1,"y":2}],"z":1}}"#
        );
    }

    #[test]
    fn ignore_rules_match_prefixes_wildcards_and_quoted_keys() {
        let rules = IgnoreRules::parse(&[
            "$.meta",
            "$.items[*].id",
            "created_at",
            "$[\"x-request-id\"]",
        ])
        .expect("patterns are valid");
        assert!(rules.ignores("$.meta.request_id"));
        assert!(rules.ignores("$.items[3].id"));
        assert!(rules.ignores("$.created_at"));
        assert!(rules.ignores("$[\"x-request-id\"]"));
        assert!(!rules.ignores("$.items[3].name"));
        assert!(!rules.ignores("$.metadata"));
    }

    #[test]
    fn ignore_rules_filter_diff_entries() {
        let rules = IgnoreRules::parse(&["$.generated_at"]).expect("pattern is valid");
        let entries = compute_json_diff(
            &json!({ "generated_at": 1, "total": 10 }),
            &json!({ "generated_at": 2, "total": 11 }),
        );
        assert_eq!(
            kinds(&rules.retain(entries)),
            vec![("$.total", DiffKind::Modified)]
        );
    }

    #[test]
    fn malformed_ignore_patterns_are_rejected() {
        assert!(IgnoreRules::parse(&["$.items[abc"]).is_err());
        assert!(IgnoreRules::parse(&["$..deep"]).is_err());
    }
}
