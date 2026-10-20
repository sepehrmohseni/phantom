use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::diff::{canonical_json, decode_form, decode_json};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpMessage {
    pub method: String,
    pub uri: String,
    #[serde(default)]
    pub status: Option<u16>,
    pub headers: Vec<(String, String)>,
    #[serde(with = "body_codec", default)]
    pub body: Vec<u8>,
}

impl HttpMessage {
    pub fn request(
        method: impl Into<String>,
        uri: impl Into<String>,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Self {
        Self {
            method: method.into(),
            uri: uri.into(),
            status: None,
            headers,
            body,
        }
    }

    pub fn response(status: u16, headers: Vec<(String, String)>, body: Vec<u8>) -> Self {
        Self {
            method: String::new(),
            uri: String::new(),
            status: Some(status),
            headers,
            body,
        }
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn path(&self) -> &str {
        UriParts::parse(&self.uri).path
    }

    pub fn json_body(&self) -> Option<Value> {
        decode_json(&self.body)
    }

    pub fn render_body(&self) -> String {
        if self.body.is_empty() {
            return String::new();
        }
        if let Some(json) = self.json_body() {
            if let Ok(pretty) = serde_json::to_string_pretty(&json) {
                return pretty;
            }
        }
        match std::str::from_utf8(&self.body) {
            Ok(text) => text.to_owned(),
            Err(_) => format!("0x{}", hex::encode(&self.body)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UriParts<'a> {
    pub authority: &'a str,
    pub path: &'a str,
    pub query: Option<&'a str>,
}

impl<'a> UriParts<'a> {
    pub fn parse(uri: &'a str) -> Self {
        let without_fragment = uri.split_once('#').map_or(uri, |(head, _)| head);
        let absolute = without_fragment
            .split_once("://")
            .filter(|(scheme, _)| is_scheme(scheme));
        let (authority, remainder) = match absolute {
            Some((_, rest)) => match rest.find(['/', '?']) {
                Some(index) => (&rest[..index], &rest[index..]),
                None => (rest, ""),
            },
            None => ("", without_fragment),
        };
        let authority = authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host);
        let (path, query) = match remainder.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (remainder, None),
        };
        Self {
            authority,
            path: if path.is_empty() { "/" } else { path },
            query,
        }
    }

    pub fn canonical_query(&self) -> String {
        let mut pairs: Vec<&str> = self
            .query
            .unwrap_or_default()
            .split('&')
            .filter(|pair| !pair.is_empty())
            .collect();
        pairs.sort_unstable();
        pairs.join("&")
    }
}

fn is_scheme(candidate: &str) -> bool {
    let mut characters = candidate.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic())
        && characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.')
        })
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RouteKey {
    pub method: String,
    pub authority: String,
    pub path: String,
}

impl RouteKey {
    pub fn new(method: &str, uri: &str) -> Self {
        let parts = UriParts::parse(uri);
        Self {
            method: method.to_ascii_uppercase(),
            authority: parts.authority.to_ascii_lowercase(),
            path: parts.path.to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutboundExchange {
    pub id: Uuid,
    pub signature_hash: String,
    pub target_host: String,
    pub request: HttpMessage,
    pub response: HttpMessage,
    pub latency_us: u64,
}

impl OutboundExchange {
    pub fn compute_signature(method: &str, uri: &str, body: &[u8]) -> String {
        let parts = UriParts::parse(uri);
        let mut hasher = Sha256::new();
        hasher.update(method.to_ascii_uppercase().as_bytes());
        hasher.update(b"\n");
        hasher.update(parts.authority.to_ascii_lowercase().as_bytes());
        hasher.update(b"\n");
        hasher.update(parts.path.as_bytes());
        hasher.update(b"\n");
        hasher.update(parts.canonical_query().as_bytes());
        hasher.update(b"\n");
        if let Some(json) = decode_json(body) {
            hasher.update(canonical_json(&json).as_bytes());
        } else if let Some(form) = decode_form(body) {
            hasher.update(b"form:");
            hasher.update(canonical_json(&form).as_bytes());
        } else {
            hasher.update(body);
        }
        hex::encode(hasher.finalize())
    }

    pub fn route_key(&self) -> RouteKey {
        RouteKey::new(&self.request.method, &self.request.uri)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrafficSession {
    pub id: Uuid,
    pub created_at_ms: i64,
    pub trace_id: String,
    pub inbound_request: HttpMessage,
    pub inbound_response: HttpMessage,
    pub outbound_calls: Vec<OutboundExchange>,
    pub total_duration_us: u64,
}

impl TrafficSession {
    pub fn status(&self) -> u16 {
        self.inbound_response.status.unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DiffKind {
    Match,
    Added,
    Removed,
    Modified,
    TypeMismatch,
}

impl DiffKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Match => "MATCH",
            Self::Added => "ADDED",
            Self::Removed => "REMOVED",
            Self::Modified => "MODIFIED",
            Self::TypeMismatch => "TYPE MISMATCH",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonDiffEntry {
    pub path: String,
    pub kind: DiffKind,
    #[serde(with = "json_value_codec", default)]
    pub expected: Option<Value>,
    #[serde(with = "json_value_codec", default)]
    pub actual: Option<Value>,
}

impl JsonDiffEntry {
    pub fn shown_expected(&self) -> Option<Value> {
        match self.kind {
            DiffKind::Added => None,
            _ => Some(self.expected.clone().unwrap_or(Value::Null)),
        }
    }

    pub fn shown_actual(&self) -> Option<Value> {
        match self.kind {
            DiffKind::Removed => None,
            _ => Some(self.actual.clone().unwrap_or(Value::Null)),
        }
    }

    pub fn new(
        path: impl Into<String>,
        kind: DiffKind,
        expected: Option<Value>,
        actual: Option<Value>,
    ) -> Self {
        Self {
            path: path.into(),
            kind,
            expected,
            actual,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayReport {
    pub id: Uuid,
    pub session_id: Uuid,
    pub executed_at_ms: i64,
    pub original_status: u16,
    pub replay_status: u16,
    pub original_latency_us: u64,
    pub replay_latency_us: u64,
    pub status_matched: bool,
    pub body_diffs: Vec<JsonDiffEntry>,
    #[serde(default)]
    pub outbound_diffs: Vec<JsonDiffEntry>,
    pub mocked_outbound_count: usize,
    pub passed: bool,
}

impl ReplayReport {
    pub fn latency_delta_us(&self) -> i64 {
        let replay = i64::try_from(self.replay_latency_us).unwrap_or(i64::MAX);
        let original = i64::try_from(self.original_latency_us).unwrap_or(i64::MAX);
        replay.saturating_sub(original)
    }

    pub fn drift_count(&self) -> usize {
        self.body_diffs.len() + self.outbound_diffs.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum OperatingMode {
    Record { upstream_url: String },
    Replay { staging_url: String },
}

impl OperatingMode {
    pub fn target_url(&self) -> &str {
        match self {
            Self::Record { upstream_url } => upstream_url,
            Self::Replay { staging_url } => staging_url,
        }
    }

    pub fn is_replay(&self) -> bool {
        matches!(self, Self::Replay { .. })
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Record { .. } => "RECORD",
            Self::Replay { .. } => "SANDBOX REPLAY",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", content = "data")]
pub enum WsEvent {
    SessionRecorded(TrafficSession),
    ReplayCompleted(ReplayReport),
    ModeChanged(OperatingMode),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayRequest {
    #[serde(default)]
    pub target_url: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayAllRequest {
    #[serde(default)]
    pub target_url: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayBatchSummary {
    pub target_url: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub errored: usize,
}

mod body_codec {
    use std::fmt;

    use serde::de::{self, SeqAccess, Visitor};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize)]
    #[serde(tag = "encoding", content = "data", rename_all = "lowercase")]
    enum Encoded<'a> {
        Utf8(&'a str),
        Hex(String),
    }

    #[derive(Deserialize)]
    #[serde(tag = "encoding", content = "data", rename_all = "lowercase")]
    enum Decoded {
        Utf8(String),
        Hex(String),
    }

    struct ByteBufVisitor;

    impl<'de> Visitor<'de> for ByteBufVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a byte buffer")
        }

        fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Vec<u8>, E> {
            Ok(bytes.to_vec())
        }

        fn visit_byte_buf<E: de::Error>(self, bytes: Vec<u8>) -> Result<Vec<u8>, E> {
            Ok(bytes)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Vec<u8>, A::Error> {
            let mut bytes = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(1 << 20));
            while let Some(byte) = sequence.next_element::<u8>()? {
                bytes.push(byte);
            }
            Ok(bytes)
        }
    }

    pub fn serialize<S: Serializer>(body: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        if !serializer.is_human_readable() {
            return serializer.serialize_bytes(body);
        }
        match std::str::from_utf8(body) {
            Ok(text) => Encoded::Utf8(text).serialize(serializer),
            Err(_) => Encoded::Hex(hex::encode(body)).serialize(serializer),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        if !deserializer.is_human_readable() {
            return deserializer.deserialize_byte_buf(ByteBufVisitor);
        }
        match Decoded::deserialize(deserializer)? {
            Decoded::Utf8(text) => Ok(text.into_bytes()),
            Decoded::Hex(encoded) => hex::decode(encoded).map_err(de::Error::custom),
        }
    }
}

mod json_value_codec {
    use serde::de;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde_json::Value;

    pub fn serialize<S: Serializer>(
        value: &Option<Value>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            value.serialize(serializer)
        } else {
            value.as_ref().map(Value::to_string).serialize(serializer)
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Value>, D::Error> {
        if deserializer.is_human_readable() {
            return Option::<Value>::deserialize(deserializer);
        }
        Option::<String>::deserialize(deserializer)?
            .map(|raw| serde_json::from_str(&raw).map_err(de::Error::custom))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn report() -> ReplayReport {
        ReplayReport {
            id: Uuid::now_v7(),
            session_id: Uuid::now_v7(),
            executed_at_ms: 1_760_000_000_000,
            original_status: 200,
            replay_status: 500,
            original_latency_us: 12_400,
            replay_latency_us: 43_400,
            status_matched: false,
            body_diffs: vec![JsonDiffEntry::new(
                "$.payment",
                DiffKind::Modified,
                Some(json!({ "amount": 4200, "tags": ["a", null] })),
                Some(json!("4200")),
            )],
            outbound_diffs: vec![JsonDiffEntry::new(
                "$.outbound[0]",
                DiffKind::Removed,
                None,
                None,
            )],
            mocked_outbound_count: 2,
            passed: false,
        }
    }

    #[test]
    fn signature_ignores_key_order_query_order_and_host_case() {
        let left = OutboundExchange::compute_signature(
            "post",
            "https://API.stripe.com/v1/charges?b=2&a=1",
            br#"{"amount":4200,"currency":"usd"}"#,
        );
        let right = OutboundExchange::compute_signature(
            "POST",
            "https://api.stripe.com/v1/charges?a=1&b=2",
            br#"{ "currency": "usd", "amount": 4200 }"#,
        );
        assert_eq!(left, right);
        assert_eq!(left.len(), 64);
    }

    #[test]
    fn form_bodies_are_canonicalised_like_json() {
        let left = OutboundExchange::compute_signature(
            "POST",
            "https://api.stripe.com/v1/charges",
            b"amount=4200&currency=usd&metadata%5Border%5D=7",
        );
        let right = OutboundExchange::compute_signature(
            "POST",
            "https://api.stripe.com/v1/charges",
            b"currency=usd&metadata%5Border%5D=7&amount=4200",
        );
        assert_eq!(left, right);
    }

    #[test]
    fn signature_changes_when_payload_values_change() {
        let base = OutboundExchange::compute_signature(
            "POST",
            "https://api.stripe.com/v1/charges",
            br#"{"amount":4200}"#,
        );
        let drifted = OutboundExchange::compute_signature(
            "POST",
            "https://api.stripe.com/v1/charges",
            br#"{"amount":4300}"#,
        );
        assert_ne!(base, drifted);
    }

    #[test]
    fn uri_parts_split_authority_path_and_query() {
        let absolute =
            UriParts::parse("https://user:secret@api.stripe.com:8443/v1/charges?limit=3#top");
        assert_eq!(absolute.authority, "api.stripe.com:8443");
        assert_eq!(absolute.path, "/v1/charges");
        assert_eq!(absolute.query, Some("limit=3"));
        let relative = UriParts::parse("/orders?id=1");
        assert_eq!(
            (relative.authority, relative.path, relative.query),
            ("", "/orders", Some("id=1"))
        );
        assert_eq!(UriParts::parse("https://example.com").path, "/");
        let relative = UriParts::parse("/login?next=https://app.example.com/home");
        assert_eq!(
            (relative.authority, relative.path, relative.query),
            ("", "/login", Some("next=https://app.example.com/home"))
        );
    }

    #[test]
    fn null_values_are_shown_as_null_not_as_missing() {
        let changed = JsonDiffEntry::new("$.a", DiffKind::TypeMismatch, None, Some(json!({})));
        assert_eq!(changed.shown_expected(), Some(Value::Null));
        let added = JsonDiffEntry::new("$.b", DiffKind::Added, None, None);
        assert_eq!(
            (added.shown_expected(), added.shown_actual()),
            (None, Some(Value::Null))
        );
        let removed = JsonDiffEntry::new("$.c", DiffKind::Removed, Some(json!(1)), None);
        assert_eq!(removed.shown_actual(), None);
    }

    #[test]
    fn json_and_form_bodies_never_share_a_signature() {
        let json = OutboundExchange::compute_signature("POST", "https://x.test/a", br#"{"a":"1"}"#);
        let form = OutboundExchange::compute_signature("POST", "https://x.test/a", b"a=1");
        assert_ne!(json, form);
    }

    #[test]
    fn replay_requests_reject_unknown_fields() {
        assert!(serde_json::from_str::<ReplayRequest>(r#"{"target":"http://x"}"#).is_err());
        assert!(serde_json::from_str::<ReplayRequest>(r#"{"target_url":"http://x"}"#).is_ok());
    }

    #[test]
    fn utf8_bodies_serialize_as_tagged_text() {
        let message = HttpMessage::request("POST", "/checkout", vec![], br#"{"a":1}"#.to_vec());
        let encoded = serde_json::to_value(&message).expect("message serializes");
        assert_eq!(
            encoded["body"],
            json!({ "encoding": "utf8", "data": "{\"a\":1}" })
        );
        let decoded: HttpMessage = serde_json::from_value(encoded).expect("message deserializes");
        assert_eq!(decoded, message);
    }

    #[test]
    fn binary_bodies_round_trip_through_hex() {
        let message = HttpMessage::response(200, vec![], vec![0x00, 0x9f, 0xff]);
        let encoded = serde_json::to_value(&message).expect("message serializes");
        assert_eq!(
            encoded["body"],
            json!({ "encoding": "hex", "data": "009fff" })
        );
        let decoded: HttpMessage = serde_json::from_value(encoded).expect("message deserializes");
        assert_eq!(decoded, message);
    }

    #[test]
    fn diff_values_stay_structured_in_json() {
        let encoded = serde_json::to_value(report()).expect("report serializes");
        assert_eq!(
            encoded["body_diffs"][0]["expected"]["tags"],
            json!(["a", null])
        );
        assert_eq!(encoded["outbound_diffs"][0]["expected"], json!(null));
    }

    #[cfg(feature = "storage")]
    #[test]
    fn reports_round_trip_through_bincode() {
        let original = report();
        let bytes = bincode::serialize(&original).expect("report encodes");
        let decoded: ReplayReport = bincode::deserialize(&bytes).expect("report decodes");
        assert_eq!(decoded, original);
    }

    #[test]
    fn ws_events_are_adjacently_tagged() {
        let event = WsEvent::ModeChanged(OperatingMode::Replay {
            staging_url: "http://staging:3000".to_owned(),
        });
        let encoded = serde_json::to_value(&event).expect("event serializes");
        assert_eq!(
            encoded,
            json!({ "event": "ModeChanged", "data": { "mode": "replay", "staging_url": "http://staging:3000" } })
        );
        let decoded: WsEvent = serde_json::from_value(encoded).expect("event deserializes");
        assert_eq!(decoded, event);
    }
}
