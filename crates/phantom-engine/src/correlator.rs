use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use parking_lot::RwLock;
use phantom_core::diff::ROOT_PATH;
use phantom_core::models::{RouteKey, UriParts};
use phantom_core::{
    compute_body_diff, DiffKind, IgnoreRules, JsonDiffEntry, OutboundExchange, TrafficSession,
};
use serde_json::{json, Value};
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

#[derive(Debug, Default)]
pub struct ActiveSessionContext {
    buckets: DashMap<String, Vec<OutboundExchange>>,
}

impl ActiveSessionContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(&self, preferred: Option<&str>) -> TraceBucket<'_> {
        let mut candidate = preferred.map(str::to_owned);
        loop {
            let trace_id = candidate
                .take()
                .unwrap_or_else(|| Uuid::new_v4().simple().to_string());
            if let Entry::Vacant(slot) = self.buckets.entry(trace_id.clone()) {
                slot.insert(Vec::new());
                return TraceBucket {
                    context: self,
                    trace_id,
                    finished: false,
                };
            }
        }
    }

    pub fn append(&self, trace_id: &str, exchange: OutboundExchange) -> bool {
        match self.buckets.get_mut(trace_id) {
            Some(mut bucket) => {
                bucket.push(exchange);
                true
            }
            None => false,
        }
    }

    pub fn resolve(&self, trace_id: Option<&str>) -> Option<String> {
        match trace_id {
            Some(trace_id) => self
                .buckets
                .contains_key(trace_id)
                .then(|| trace_id.to_owned()),
            None if self.buckets.len() == 1 => {
                self.buckets.iter().next().map(|entry| entry.key().clone())
            }
            None => None,
        }
    }

    pub fn in_flight(&self) -> usize {
        self.buckets.len()
    }

    pub fn contains(&self, trace_id: &str) -> bool {
        self.buckets.contains_key(trace_id)
    }

    fn drain(&self, trace_id: &str) -> Vec<OutboundExchange> {
        let mut calls = self
            .buckets
            .remove(trace_id)
            .map(|(_, calls)| calls)
            .unwrap_or_default();
        calls.sort_by_key(|call| call.id);
        calls
    }
}

#[derive(Debug)]
pub struct TraceBucket<'a> {
    context: &'a ActiveSessionContext,
    trace_id: String,
    finished: bool,
}

impl TraceBucket<'_> {
    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }

    pub fn finish(mut self) -> Vec<OutboundExchange> {
        self.finished = true;
        self.context.drain(&self.trace_id)
    }
}

impl Drop for TraceBucket<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.context.buckets.remove(&self.trace_id);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchKind {
    Exact,
    RouteFallback,
    Repeat,
}

impl MatchKind {
    pub fn header_value(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::RouteFallback => "route-fallback",
            Self::Repeat => "repeat",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SandboxHit {
    pub exchange: OutboundExchange,
    pub kind: MatchKind,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReplayOutcome {
    pub mocked: usize,
    pub misses: usize,
    pub drift: Vec<JsonDiffEntry>,
}

#[derive(Debug, Clone, Copy)]
enum Resolution {
    Served { recorded: usize, kind: MatchKind },
    Missed,
}

#[derive(Debug, Clone)]
struct ObservedCall {
    method: String,
    uri: String,
    signature: String,
    body: Option<Vec<u8>>,
    resolution: Resolution,
}

impl ObservedCall {
    fn summary(&self, reason: &str, duplicate_of: Option<usize>) -> Value {
        json!({
            "method": self.method,
            "url": self.uri,
            "signature_hash": self.signature,
            "reason": reason,
            "duplicate_of": duplicate_of,
        })
    }
}

#[derive(Debug)]
pub struct ActiveReplay {
    trace_id: String,
    session: TrafficSession,
    rules: IgnoreRules,
    consumed: Vec<bool>,
    observed: Vec<ObservedCall>,
}

impl ActiveReplay {
    fn new(trace_id: String, session: TrafficSession, rules: IgnoreRules) -> Self {
        let consumed = vec![false; session.outbound_calls.len()];
        Self {
            trace_id,
            session,
            rules,
            consumed,
            observed: Vec::new(),
        }
    }

    fn fallback_distance(&self, recorded: &OutboundExchange, uri: &str, body: &[u8]) -> usize {
        let query_changed = UriParts::parse(&recorded.request.uri).canonical_query()
            != UriParts::parse(uri).canonical_query();
        usize::from(query_changed)
            + self
                .rules
                .retain(compute_body_diff(&recorded.request.body, body))
                .len()
    }

    fn select(
        &self,
        route: &RouteKey,
        signature: &str,
        uri: &str,
        body: &[u8],
    ) -> Option<(usize, MatchKind)> {
        let calls = &self.session.outbound_calls;
        let fresh = |index: &usize| !self.consumed[*index];
        let exact = |index: &usize| calls[*index].signature_hash == signature;
        let same_route = |index: &usize| calls[*index].route_key() == *route;
        let indices = || 0..calls.len();
        indices()
            .filter(fresh)
            .find(exact)
            .map(|index| (index, MatchKind::Exact))
            .or_else(|| {
                indices()
                    .filter(fresh)
                    .filter(same_route)
                    .min_by_key(|index| self.fallback_distance(&calls[*index], uri, body))
                    .map(|index| (index, MatchKind::RouteFallback))
            })
            .or_else(|| {
                indices()
                    .rev()
                    .find(exact)
                    .map(|index| (index, MatchKind::Repeat))
            })
            .or_else(|| {
                indices()
                    .rev()
                    .find(same_route)
                    .map(|index| (index, MatchKind::Repeat))
            })
    }

    fn claim(
        &mut self,
        method: &str,
        uri: &str,
        signature: &str,
        body: &[u8],
    ) -> Option<SandboxHit> {
        let route = RouteKey::new(method, uri);
        let selection = self.select(&route, signature, uri, body);
        let resolution = match selection {
            Some((recorded, kind)) => {
                self.consumed[recorded] = true;
                Resolution::Served { recorded, kind }
            }
            None => Resolution::Missed,
        };
        let retains_body = matches!(
            resolution,
            Resolution::Served {
                kind: MatchKind::RouteFallback,
                ..
            }
        );
        self.observed.push(ObservedCall {
            method: method.to_ascii_uppercase(),
            uri: uri.to_owned(),
            signature: signature.to_owned(),
            body: retains_body.then(|| body.to_vec()),
            resolution,
        });
        selection.map(|(recorded, kind)| SandboxHit {
            exchange: self.session.outbound_calls[recorded].clone(),
            kind,
        })
    }

    fn outcome(&self, rules: &IgnoreRules) -> ReplayOutcome {
        let mut drift = Vec::new();
        let mut mocked = 0;
        let mut misses = 0;
        for (position, call) in self.observed.iter().enumerate() {
            match call.resolution {
                Resolution::Served {
                    kind: MatchKind::Exact,
                    ..
                } => mocked += 1,
                Resolution::Served {
                    recorded,
                    kind: MatchKind::RouteFallback,
                } => {
                    mocked += 1;
                    let recorded_request = &self.session.outbound_calls[recorded].request;
                    let expected_query = UriParts::parse(&recorded_request.uri).canonical_query();
                    let actual_query = UriParts::parse(&call.uri).canonical_query();
                    if expected_query != actual_query {
                        drift.push(JsonDiffEntry::new(
                            format!("$.outbound[{recorded}].query"),
                            DiffKind::Modified,
                            Some(Value::String(expected_query)),
                            Some(Value::String(actual_query)),
                        ));
                    }
                    let expected = &recorded_request.body;
                    let actual = call.body.as_deref().unwrap_or_default();
                    let prefix = format!("$.outbound[{recorded}].request");
                    for entry in rules.retain(compute_body_diff(expected, actual)) {
                        let suffix = entry.path.strip_prefix(ROOT_PATH).unwrap_or(&entry.path);
                        drift.push(JsonDiffEntry {
                            path: format!("{prefix}{suffix}"),
                            ..entry
                        });
                    }
                }
                Resolution::Served {
                    recorded,
                    kind: MatchKind::Repeat,
                } => {
                    mocked += 1;
                    drift.push(JsonDiffEntry::new(
                        format!("$.outbound_extra[{position}]"),
                        DiffKind::Added,
                        None,
                        Some(call.summary("duplicate", Some(recorded))),
                    ));
                }
                Resolution::Missed => {
                    misses += 1;
                    drift.push(JsonDiffEntry::new(
                        format!("$.outbound_extra[{position}]"),
                        DiffKind::Added,
                        None,
                        Some(call.summary("unrecorded", None)),
                    ));
                }
            }
        }
        for (index, consumed) in self.consumed.iter().enumerate() {
            if !consumed {
                drift.push(JsonDiffEntry::new(
                    format!("$.outbound[{index}]"),
                    DiffKind::Removed,
                    Some(exchange_summary(&self.session.outbound_calls[index])),
                    None,
                ));
            }
        }
        ReplayOutcome {
            mocked,
            misses,
            drift: rules.retain(drift),
        }
    }
}

fn exchange_summary(exchange: &OutboundExchange) -> Value {
    json!({
        "method": exchange.request.method,
        "url": exchange.request.uri,
        "signature_hash": exchange.signature_hash,
        "reason": "never_called",
    })
}

#[derive(Debug, Default)]
pub struct ReplaySandbox {
    active: Arc<RwLock<Option<ActiveReplay>>>,
    gate: Mutex<()>,
}

impl ReplaySandbox {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn engage(
        &self,
        trace_id: String,
        session: TrafficSession,
        rules: IgnoreRules,
    ) -> SandboxLease<'_> {
        let permit = self.gate.lock().await;
        *self.active.write() = Some(ActiveReplay::new(trace_id, session, rules));
        SandboxLease {
            sandbox: self,
            _permit: permit,
        }
    }

    pub fn active_trace(&self) -> Option<String> {
        self.active
            .read()
            .as_ref()
            .map(|replay| replay.trace_id.clone())
    }

    pub fn claim(
        &self,
        method: &str,
        uri: &str,
        signature: &str,
        body: &[u8],
    ) -> Option<SandboxHit> {
        self.active
            .write()
            .as_mut()
            .and_then(|replay| replay.claim(method, uri, signature, body))
    }

    fn release(&self) -> Option<ActiveReplay> {
        self.active.write().take()
    }
}

#[derive(Debug)]
pub struct SandboxLease<'a> {
    sandbox: &'a ReplaySandbox,
    _permit: MutexGuard<'a, ()>,
}

impl SandboxLease<'_> {
    pub fn finish(self) -> ReplayOutcome {
        self.sandbox
            .release()
            .map(|replay| replay.outcome(&replay.rules))
            .unwrap_or_default()
    }
}

impl Drop for SandboxLease<'_> {
    fn drop(&mut self) {
        self.sandbox.release();
    }
}

#[cfg(test)]
mod tests {
    use phantom_core::HttpMessage;

    use super::*;

    fn exchange(method: &str, uri: &str, body: &str) -> OutboundExchange {
        OutboundExchange {
            id: Uuid::now_v7(),
            signature_hash: OutboundExchange::compute_signature(method, uri, body.as_bytes()),
            target_host: "api.example.com".to_owned(),
            request: HttpMessage::request(method, uri, Vec::new(), body.as_bytes().to_vec()),
            response: HttpMessage::response(200, Vec::new(), br#"{"ok":true}"#.to_vec()),
            latency_us: 10,
        }
    }

    fn session(calls: Vec<OutboundExchange>) -> TrafficSession {
        TrafficSession {
            id: Uuid::now_v7(),
            created_at_ms: 0,
            trace_id: "recorded".to_owned(),
            inbound_request: HttpMessage::request("POST", "/checkout", Vec::new(), Vec::new()),
            inbound_response: HttpMessage::response(201, Vec::new(), Vec::new()),
            outbound_calls: calls,
            total_duration_us: 1,
        }
    }

    fn claim(replay: &mut ActiveReplay, method: &str, uri: &str, body: &str) -> Option<MatchKind> {
        let signature = OutboundExchange::compute_signature(method, uri, body.as_bytes());
        replay
            .claim(method, uri, &signature, body.as_bytes())
            .map(|hit| hit.kind)
    }

    fn drift_paths(outcome: &ReplayOutcome) -> Vec<(String, DiffKind)> {
        let mut paths: Vec<(String, DiffKind)> = outcome
            .drift
            .iter()
            .map(|entry| (entry.path.clone(), entry.kind))
            .collect();
        paths.sort_by(|left, right| left.0.cmp(&right.0));
        paths
    }

    const CHARGES: &str = "https://api.example.com/v1/charges";

    #[test]
    fn identical_calls_replay_without_drift() {
        let mut replay = ActiveReplay::new(
            "replay-a".to_owned(),
            session(vec![exchange("POST", CHARGES, r#"{"amount":4200}"#)]),
            IgnoreRules::default(),
        );
        assert_eq!(
            claim(&mut replay, "POST", CHARGES, r#"{ "amount": 4200 }"#),
            Some(MatchKind::Exact)
        );
        let outcome = replay.outcome(&IgnoreRules::default());
        assert_eq!((outcome.mocked, outcome.misses), (1, 0));
        assert!(outcome.drift.is_empty());
    }

    #[test]
    fn a_second_identical_call_is_answered_but_reported() {
        let mut replay = ActiveReplay::new(
            "replay-b".to_owned(),
            session(vec![exchange("POST", CHARGES, r#"{"amount":4200}"#)]),
            IgnoreRules::default(),
        );
        assert_eq!(
            claim(&mut replay, "POST", CHARGES, r#"{"amount":4200}"#),
            Some(MatchKind::Exact)
        );
        assert_eq!(
            claim(&mut replay, "POST", CHARGES, r#"{"amount":4200}"#),
            Some(MatchKind::Repeat)
        );
        let outcome = replay.outcome(&IgnoreRules::default());
        assert_eq!(outcome.mocked, 2);
        assert_eq!(
            drift_paths(&outcome),
            vec![("$.outbound_extra[1]".to_owned(), DiffKind::Added)]
        );
    }

    #[test]
    fn unrecorded_and_skipped_calls_are_both_drift() {
        let mut replay = ActiveReplay::new(
            "replay-c".to_owned(),
            session(vec![exchange("POST", CHARGES, r#"{"amount":4200}"#)]),
            IgnoreRules::default(),
        );
        assert_eq!(
            claim(
                &mut replay,
                "POST",
                "https://api.example.com/v1/refunds",
                "{}"
            ),
            None
        );
        let outcome = replay.outcome(&IgnoreRules::default());
        assert_eq!(outcome.misses, 1);
        assert_eq!(
            drift_paths(&outcome),
            vec![
                ("$.outbound[0]".to_owned(), DiffKind::Removed),
                ("$.outbound_extra[0]".to_owned(), DiffKind::Added),
            ]
        );
    }

    #[test]
    fn route_fallback_reports_query_and_body_changes_minus_ignored_fields() {
        let recorded = format!("{CHARGES}?expand=customer");
        let mut replay = ActiveReplay::new(
            "replay-d".to_owned(),
            session(vec![exchange(
                "POST",
                &recorded,
                r#"{"amount":4200,"nonce":"a1"}"#,
            )]),
            IgnoreRules::default(),
        );
        let replayed = format!("{CHARGES}?expand=balance");
        assert_eq!(
            claim(
                &mut replay,
                "POST",
                &replayed,
                r#"{"amount":4300,"nonce":"b2"}"#
            ),
            Some(MatchKind::RouteFallback)
        );
        let rules = IgnoreRules::parse(&["$.nonce"]).expect("valid pattern");
        let outcome = replay.outcome(&rules);
        assert_eq!(
            drift_paths(&outcome),
            vec![
                ("$.outbound[0].query".to_owned(), DiffKind::Modified),
                (
                    "$.outbound[0].request.amount".to_owned(),
                    DiffKind::Modified
                ),
            ]
        );
    }

    #[test]
    fn nonce_only_differences_pass_once_ignored() {
        let mut replay = ActiveReplay::new(
            "replay-e".to_owned(),
            session(vec![exchange(
                "POST",
                CHARGES,
                "amount=4200&idempotency_nonce=a1",
            )]),
            IgnoreRules::default(),
        );
        assert_eq!(
            claim(
                &mut replay,
                "POST",
                CHARGES,
                "amount=4200&idempotency_nonce=zz"
            ),
            Some(MatchKind::RouteFallback)
        );
        let rules = IgnoreRules::parse(&["$.idempotency_nonce"]).expect("valid pattern");
        assert!(replay.outcome(&rules).drift.is_empty());
    }

    #[test]
    fn parallel_calls_pair_with_the_closest_recording() {
        let rules = IgnoreRules::parse(&["$.nonce"]).expect("valid pattern");
        let mut replay = ActiveReplay::new(
            "replay-h".to_owned(),
            session(vec![
                exchange("POST", CHARGES, r#"{"amount":100,"nonce":"a"}"#),
                exchange("POST", CHARGES, r#"{"amount":200,"nonce":"b"}"#),
            ]),
            rules.clone(),
        );
        assert_eq!(
            claim(
                &mut replay,
                "POST",
                CHARGES,
                r#"{"amount":200,"nonce":"x"}"#
            ),
            Some(MatchKind::RouteFallback)
        );
        assert_eq!(
            claim(
                &mut replay,
                "POST",
                CHARGES,
                r#"{"amount":100,"nonce":"y"}"#
            ),
            Some(MatchKind::RouteFallback)
        );
        assert!(replay.outcome(&rules).drift.is_empty());
    }

    #[test]
    fn abandoned_trace_buckets_are_removed_and_ids_never_collide() {
        let context = ActiveSessionContext::new();
        let first = context.open(Some("abc"));
        let second = context.open(Some("abc"));
        assert_eq!(first.trace_id(), "abc");
        assert_ne!(second.trace_id(), "abc");
        assert_eq!(context.in_flight(), 2);
        drop(first);
        drop(second);
        assert_eq!(context.in_flight(), 0);
    }

    #[test]
    fn only_one_in_flight_request_absorbs_uncorrelated_calls() {
        let context = ActiveSessionContext::new();
        let only = context.open(None);
        assert_eq!(context.resolve(None).as_deref(), Some(only.trace_id()));
        let other = context.open(None);
        assert_eq!(context.resolve(None), None);
        assert_eq!(context.resolve(Some("unknown")), None);
        assert_eq!(
            context.resolve(Some(other.trace_id())).as_deref(),
            Some(other.trace_id())
        );
    }

    #[tokio::test]
    async fn a_dropped_lease_frees_the_sandbox() {
        let sandbox = ReplaySandbox::new();
        let lease = sandbox
            .engage(
                "replay-f".to_owned(),
                session(Vec::new()),
                IgnoreRules::default(),
            )
            .await;
        assert_eq!(sandbox.active_trace().as_deref(), Some("replay-f"));
        drop(lease);
        assert_eq!(sandbox.active_trace(), None);
        let lease = sandbox
            .engage(
                "replay-g".to_owned(),
                session(Vec::new()),
                IgnoreRules::default(),
            )
            .await;
        assert_eq!(lease.finish(), ReplayOutcome::default());
        assert_eq!(sandbox.active_trace(), None);
    }
}
