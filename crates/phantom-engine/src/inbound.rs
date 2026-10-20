use std::net::SocketAddr;
use std::time::Instant;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{Method, Uri};
use axum::response::Response;
use axum::Router;
use chrono::Utc;
use phantom_core::{HttpMessage, PhantomError, PhantomResult, TrafficSession};
use uuid::Uuid;

use crate::state::{
    is_replay_trace, is_valid_trace, OperatingMode, SharedEngine, CAPTURE_HEADER, REPLAY_OF_HEADER,
    REPLAY_TRACE_PREFIX, SESSION_HEADER, TRACE_HEADER, VIA_HEADER,
};
use crate::wire;

pub fn router(engine: SharedEngine) -> Router {
    Router::new().fallback(capture).with_state(engine)
}

async fn capture(State(engine): State<SharedEngine>, request: Request) -> Response {
    if request.headers().contains_key(VIA_HEADER) {
        return wire::failure(PhantomError::InvalidTarget(
            "this request already went through phantom's outbound proxy; refusing to loop"
                .to_owned(),
        ));
    }
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(address)| *address);
    let replaying = request.headers().contains_key(REPLAY_OF_HEADER);
    let outcome = match (engine.mode(), replaying) {
        (OperatingMode::Record { upstream_url }, false) => {
            record(&engine, &upstream_url, peer, request).await
        }
        (mode, _) => passthrough(&engine, mode.target_url(), peer, request, replaying).await,
    };
    outcome.unwrap_or_else(wire::failure)
}

async fn record(
    engine: &SharedEngine,
    upstream: &str,
    peer: Option<SocketAddr>,
    request: Request,
) -> PhantomResult<Response> {
    let started = Instant::now();
    let (parts, body) = request.into_parts();
    let head = parts.method == Method::HEAD;
    let preferred = wire::header_str(&parts.headers, TRACE_HEADER)
        .filter(|trace_id| is_valid_trace(trace_id) && !is_replay_trace(trace_id));
    let bucket = engine.correlator.open(preferred);
    let body = wire::collect_body(body, engine.config.max_body_bytes).await?;
    let inbound_request = HttpMessage::request(
        parts.method.as_str(),
        path_and_query(&parts.uri),
        wire::capture_headers(&parts.headers),
        body.to_vec(),
    );
    let forwarding = wire::forwarding_headers(bucket.trace_id(), peer, &parts.headers);
    let target = wire::join_url(upstream, &inbound_request.uri);
    let reply = wire::dispatch(
        &engine.http,
        &target,
        &inbound_request,
        forwarding,
        engine.config.max_body_bytes,
    )
    .await?;
    let total_duration_us = wire::elapsed_us(started);
    let trace_id = bucket.trace_id().to_owned();
    let outbound_calls = bucket.finish();

    let session = TrafficSession {
        id: Uuid::now_v7(),
        created_at_ms: Utc::now().timestamp_millis(),
        trace_id,
        inbound_request,
        inbound_response: HttpMessage::response(
            reply.status,
            reply.headers.clone(),
            reply.body.to_vec(),
        ),
        outbound_calls,
        total_duration_us,
    };
    tracing::info!(
        session = %session.id,
        method = %session.inbound_request.method,
        uri = %session.inbound_request.uri,
        status = reply.status,
        outbound = session.outbound_calls.len(),
        duration_us = total_duration_us,
        "session captured"
    );

    let mut response = wire::build_response(reply.status, &reply.headers, reply.body, head);
    wire::set_header(&mut response, TRACE_HEADER, &session.trace_id);
    wire::set_header(&mut response, SESSION_HEADER, &session.id.to_string());
    engine.persist_session(session);
    Ok(response)
}

async fn passthrough(
    engine: &SharedEngine,
    base: &str,
    peer: Option<SocketAddr>,
    request: Request,
    replaying: bool,
) -> PhantomResult<Response> {
    let (parts, body) = request.into_parts();
    let head = parts.method == Method::HEAD;
    let trace_id = wire::header_str(&parts.headers, TRACE_HEADER)
        .filter(|trace_id| is_valid_trace(trace_id) && is_replay_trace(trace_id) == replaying)
        .map_or_else(
            || {
                let fresh = Uuid::new_v4().simple().to_string();
                if replaying {
                    format!("{REPLAY_TRACE_PREFIX}{fresh}")
                } else {
                    fresh
                }
            },
            str::to_owned,
        );
    let body = wire::collect_body(body, engine.config.max_body_bytes).await?;
    let message = HttpMessage::request(
        parts.method.as_str(),
        path_and_query(&parts.uri),
        wire::capture_headers(&parts.headers),
        body.to_vec(),
    );
    let forwarding = wire::forwarding_headers(&trace_id, peer, &parts.headers);
    let target = wire::join_url(base, &message.uri);
    let reply = wire::dispatch(
        &engine.http,
        &target,
        &message,
        forwarding,
        engine.config.max_body_bytes,
    )
    .await?;
    let mut response = wire::build_response(reply.status, &reply.headers, reply.body, head);
    wire::set_header(&mut response, TRACE_HEADER, &trace_id);
    wire::set_header(&mut response, CAPTURE_HEADER, "paused");
    Ok(response)
}

fn path_and_query(uri: &Uri) -> String {
    uri.path_and_query()
        .map_or_else(|| "/".to_owned(), |value| value.as_str().to_owned())
}
