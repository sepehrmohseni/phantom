use std::time::Instant;

use axum::extract::{Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use bytes::Bytes;
use phantom_core::{HttpMessage, OutboundExchange, PhantomError, PhantomResult};
use reqwest::Url;
use serde_json::json;
use uuid::Uuid;

use crate::state::{
    EngineState, OutboundRoute, SharedEngine, ERROR_HEADER, EXCHANGE_HEADER, OUTBOUND_HEADER,
    SANDBOX_HEADER, TARGET_HEADER, TRACE_HEADER, VIA_HEADER,
};
use crate::wire;

pub fn router(engine: SharedEngine) -> Router {
    Router::new().fallback(intercept).with_state(engine)
}

async fn intercept(State(engine): State<SharedEngine>, request: Request) -> Response {
    handle(&engine, request).await.unwrap_or_else(wire::failure)
}

async fn handle(engine: &SharedEngine, request: Request) -> PhantomResult<Response> {
    if request.method() == Method::CONNECT {
        return Err(PhantomError::InvalidTarget(format!(
            "CONNECT tunnels cannot be intercepted; send the call over plain http to the sandbox with the `{TARGET_HEADER}` header"
        )));
    }
    if request.headers().contains_key(VIA_HEADER) {
        return Ok(loop_detected());
    }
    let (parts, body) = request.into_parts();
    let target = resolve_target(&parts.headers, &parts.uri)?;
    let body = wire::collect_body(body, engine.config.max_body_bytes).await?;
    let method = parts.method.as_str();
    let head = parts.method == Method::HEAD;
    let signature = OutboundExchange::compute_signature(method, target.as_str(), &body);
    let trace_id = wire::header_str(&parts.headers, TRACE_HEADER);

    match engine.route_outbound(trace_id) {
        OutboundRoute::Sandbox => Ok(serve_from_sandbox(
            engine,
            method,
            target.as_str(),
            &signature,
            &body,
            head,
        )),
        OutboundRoute::Blocked { reason } => Ok(sandbox_miss(
            method,
            target.as_str(),
            &signature,
            reason,
            "blocked",
        )),
        OutboundRoute::Live { bucket } => {
            forward_live(engine, &parts, &target, body, signature, bucket).await
        }
    }
}

fn loop_detected() -> Response {
    (
        StatusCode::LOOP_DETECTED,
        Json(json!({
            "error": "phantom_loop",
            "message": "this request already went through phantom; pointing the outbound proxy at phantom itself is refused",
        })),
    )
        .into_response()
}

fn serve_from_sandbox(
    engine: &EngineState,
    method: &str,
    uri: &str,
    signature: &str,
    body: &[u8],
    head: bool,
) -> Response {
    let Some(hit) = engine.sandbox.claim(method, uri, signature, body) else {
        tracing::warn!(method, uri, signature, "sandbox miss during replay");
        return sandbox_miss(
            method,
            uri,
            signature,
            "no recorded exchange in the active replay session matches this call",
            "miss",
        );
    };
    let exchange_id = hit.exchange.id.to_string();
    let recorded = hit.exchange.response;
    tracing::debug!(
        method,
        uri,
        kind = hit.kind.header_value(),
        "served sandbox mock"
    );
    let mut response = wire::build_response(
        recorded.status.unwrap_or(200),
        &recorded.headers,
        Bytes::from(recorded.body),
        head,
    );
    wire::set_header(&mut response, SANDBOX_HEADER, hit.kind.header_value());
    wire::set_header(&mut response, EXCHANGE_HEADER, &exchange_id);
    response
}

fn sandbox_miss(method: &str, uri: &str, signature: &str, reason: &str, verdict: &str) -> Response {
    let mut response = (
        StatusCode::BAD_GATEWAY,
        Json(json!({
            "error": "phantom_sandbox_miss",
            "message": reason,
            "method": method,
            "url": uri,
            "signature_hash": signature,
        })),
    )
        .into_response();
    wire::set_header(&mut response, SANDBOX_HEADER, verdict);
    response
}

async fn forward_live(
    engine: &SharedEngine,
    parts: &Parts,
    target: &Url,
    body: Bytes,
    signature: String,
    bucket: Option<String>,
) -> PhantomResult<Response> {
    let id = Uuid::now_v7();
    let head = parts.method == Method::HEAD;
    let request = HttpMessage::request(
        parts.method.as_str(),
        target.as_str(),
        wire::capture_headers(&parts.headers),
        body.to_vec(),
    );
    let mut stamp = HeaderMap::new();
    stamp.insert(VIA_HEADER, HeaderValue::from_static("phantom"));
    let started = Instant::now();
    let dispatched = wire::dispatch(
        &engine.http,
        target.as_str(),
        &request,
        stamp,
        engine.config.max_body_bytes,
    )
    .await;
    let latency_us = wire::elapsed_us(started);

    let (status, headers, reply_body, failure) = match dispatched {
        Ok(reply) => (reply.status, reply.headers, reply.body, None),
        Err(err) if bucket.is_some() => {
            let payload = json!({ "error": err.kind(), "message": err.to_string() }).to_string();
            let headers = vec![
                ("content-type".to_owned(), "application/json".to_owned()),
                (ERROR_HEADER.to_owned(), err.kind().to_owned()),
            ];
            tracing::warn!(uri = %target, error = %err, "outbound call failed while recording; storing the failure");
            (err.status_code(), headers, Bytes::from(payload), Some(err))
        }
        Err(err) => return Err(err),
    };
    let mut response = wire::build_response(status, &headers, reply_body.clone(), head);

    let Some(trace_id) = bucket else {
        tracing::debug!(uri = %target, "uncorrelated outbound call forwarded without capture");
        wire::set_header(&mut response, OUTBOUND_HEADER, "uncorrelated");
        return Ok(response);
    };
    let exchange = OutboundExchange {
        id,
        signature_hash: signature,
        target_host: host_label(target),
        request: HttpMessage {
            headers: wire::redact(request.headers),
            ..request
        },
        response: HttpMessage::response(status, headers, reply_body.to_vec()),
        latency_us,
    };
    let verdict = match (engine.correlator.append(&trace_id, exchange), failure) {
        (true, None) => "recorded",
        (true, Some(_)) => "recorded-failure",
        (false, _) => {
            tracing::debug!(%trace_id, uri = %target, "trace closed before outbound call completed");
            "orphaned"
        }
    };
    wire::set_header(&mut response, OUTBOUND_HEADER, verdict);
    if verdict != "orphaned" {
        wire::set_header(&mut response, EXCHANGE_HEADER, &id.to_string());
    }
    Ok(response)
}

fn resolve_target(headers: &HeaderMap, uri: &Uri) -> PhantomResult<Url> {
    if let Some(raw) = wire::header_str(headers, TARGET_HEADER) {
        let mut target = parse_target(raw)?;
        if target.path() == "/" && uri.path() != "/" {
            target.set_path(uri.path());
        }
        if target.query().is_none() {
            target.set_query(uri.query());
        }
        return Ok(target);
    }
    if uri.scheme().is_some() && uri.authority().is_some() {
        return parse_target(&uri.to_string());
    }
    let embedded = uri.path_and_query().map_or("/", |value| value.as_str());
    if let Some(target) = embedded_target(embedded) {
        return parse_target(&target);
    }
    Err(PhantomError::InvalidTarget(format!(
        "cannot determine the outbound destination; set `{TARGET_HEADER}`, send an absolute-form proxy request, or embed the absolute url in the path"
    )))
}

fn embedded_target(path_and_query: &str) -> Option<String> {
    let trimmed = path_and_query.trim_start_matches('/');
    ["https:", "http:"].into_iter().find_map(|scheme| {
        trimmed
            .strip_prefix(scheme)
            .map(|rest| format!("{scheme}//{}", rest.trim_start_matches('/')))
    })
}

fn parse_target(raw: &str) -> PhantomResult<Url> {
    let url = Url::parse(raw)
        .map_err(|err| PhantomError::InvalidTarget(format!("`{raw}` is not a valid url: {err}")))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(PhantomError::InvalidTarget(format!(
            "`{raw}` must be an absolute http or https url"
        )));
    }
    Ok(url)
}

fn host_label(target: &Url) -> String {
    let host = target.host_str().unwrap_or_default();
    match target.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    }
}
