pub mod correlator;
pub mod inbound;
pub mod outbound;
pub mod replayer;
pub mod state;

use std::future::Future;
use std::io;
use std::time::Duration;

use axum::extract::ConnectInfo;
use axum::Router;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tower::Service;

pub use correlator::{ActiveSessionContext, ReplaySandbox};
pub use state::{EngineConfig, EngineState, OperatingMode, SessionRecorder, SharedEngine};

const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const ACCEPT_BACKOFF: Duration = Duration::from_millis(25);
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn serve<F>(listener: TcpListener, router: Router, shutdown: F) -> io::Result<()>
where
    F: Future<Output = ()> + Send,
{
    let graceful = GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(connection) => connection,
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to accept connection");
                        tokio::time::sleep(ACCEPT_BACKOFF).await;
                        continue;
                    }
                };
                if let Err(err) = stream.set_nodelay(true) {
                    tracing::debug!(%peer, error = %err, "unable to disable nagle");
                }
                let router = router.clone();
                let service = service_fn(move |mut request: Request<Incoming>| {
                    request.extensions_mut().insert(ConnectInfo(peer));
                    router.clone().call(request)
                });
                let mut builder = Builder::new(TokioExecutor::new());
                builder
                    .http1()
                    .timer(TokioTimer::new())
                    .header_read_timeout(HEADER_READ_TIMEOUT);
                builder.http2().timer(TokioTimer::new());
                let connection = builder
                    .serve_connection_with_upgrades(TokioIo::new(stream), service)
                    .into_owned();
                let connection = graceful.watch(connection);
                tokio::spawn(async move {
                    if let Err(err) = connection.await {
                        tracing::debug!(%peer, error = %err, "connection terminated");
                    }
                });
            }
            () = &mut shutdown => break,
        }
    }
    drop(listener);
    tokio::select! {
        () = graceful.shutdown() => {}
        () = tokio::time::sleep(DRAIN_TIMEOUT) => {
            tracing::warn!("drain timeout elapsed with connections still open");
        }
    }
    Ok(())
}

pub mod wire {
    use std::net::SocketAddr;
    use std::time::Instant;

    use axum::body::Body;
    use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::Json;
    use bytes::Bytes;
    use http_body_util::{BodyExt, LengthLimitError, Limited};
    use phantom_core::{HttpMessage, PhantomError, PhantomResult};
    use serde_json::json;

    use crate::state::{ERROR_HEADER, PHANTOM_HEADER_PREFIX, TRACE_HEADER};

    const HOP_BY_HOP: [&str; 9] = [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ];

    const SENSITIVE: [&str; 7] = [
        "authorization",
        "proxy-authorization",
        "cookie",
        "x-api-key",
        "api-key",
        "x-auth-token",
        "x-amz-security-token",
    ];

    pub const REDACTED: &str = "[redacted]";

    #[derive(Debug, Clone)]
    pub struct UpstreamReply {
        pub status: u16,
        pub headers: Vec<(String, String)>,
        pub body: Bytes,
    }

    pub fn capture_headers(headers: &HeaderMap) -> Vec<(String, String)> {
        headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect()
    }

    pub fn redact(headers: Vec<(String, String)>) -> Vec<(String, String)> {
        headers
            .into_iter()
            .map(|(name, value)| {
                if SENSITIVE
                    .iter()
                    .any(|secret| name.eq_ignore_ascii_case(secret))
                {
                    (name, REDACTED.to_owned())
                } else {
                    (name, value)
                }
            })
            .collect()
    }

    pub fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    pub fn set_header(response: &mut Response, name: &'static str, value: &str) {
        if let Ok(value) = HeaderValue::from_str(value) {
            response
                .headers_mut()
                .insert(HeaderName::from_static(name), value);
        }
    }

    pub fn forwarding_headers(
        trace_id: &str,
        peer: Option<SocketAddr>,
        original: &HeaderMap,
    ) -> HeaderMap {
        let mut headers = HeaderMap::new();
        insert(&mut headers, TRACE_HEADER, trace_id);
        if let Some(peer) = peer {
            let chain = match header_str(original, "x-forwarded-for") {
                Some(existing) => format!("{existing}, {}", peer.ip()),
                None => peer.ip().to_string(),
            };
            insert(&mut headers, "x-forwarded-for", &chain);
        }
        if !original.contains_key("x-forwarded-proto") {
            insert(&mut headers, "x-forwarded-proto", "http");
        }
        if !original.contains_key("x-forwarded-host") {
            if let Some(host) = header_str(original, "host") {
                insert(&mut headers, "x-forwarded-host", host);
            }
        }
        headers
    }

    pub fn request_headers(headers: &[(String, String)]) -> HeaderMap {
        let tokens = connection_tokens(headers);
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            let lowered = name.to_ascii_lowercase();
            let excluded = is_hop_by_hop(&lowered, &tokens)
                || matches!(
                    lowered.as_str(),
                    "host" | "content-length" | "accept-encoding"
                )
                || lowered.starts_with(PHANTOM_HEADER_PREFIX);
            if !excluded {
                append(&mut map, name, value);
            }
        }
        map
    }

    pub fn response_headers(headers: &[(String, String)], head: bool) -> HeaderMap {
        let tokens = connection_tokens(headers);
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            let lowered = name.to_ascii_lowercase();
            let length = lowered == "content-length";
            if !is_hop_by_hop(&lowered, &tokens) && (!length || head) {
                append(&mut map, name, value);
            }
        }
        map
    }

    pub fn build_response(
        status: u16,
        headers: &[(String, String)],
        body: Bytes,
        head: bool,
    ) -> Response {
        let mut response = Response::new(Body::from(body));
        *response.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
        *response.headers_mut() = response_headers(headers, head);
        response
    }

    pub async fn collect_body(body: Body, limit: usize) -> PhantomResult<Bytes> {
        match Limited::new(body, limit).collect().await {
            Ok(collected) => Ok(collected.to_bytes()),
            Err(err) if err.downcast_ref::<LengthLimitError>().is_some() => {
                Err(PhantomError::PayloadTooLarge { limit })
            }
            Err(err) => Err(PhantomError::InvalidMessage(format!(
                "failed to read request body: {err}"
            ))),
        }
    }

    pub async fn dispatch(
        client: &reqwest::Client,
        url: &str,
        message: &HttpMessage,
        extra: HeaderMap,
        limit: usize,
    ) -> PhantomResult<UpstreamReply> {
        let method = reqwest::Method::from_bytes(message.method.as_bytes()).map_err(|_| {
            PhantomError::InvalidMessage(format!("unsupported method `{}`", message.method))
        })?;
        let mut headers = request_headers(&message.headers);
        for (name, value) in &extra {
            headers.insert(name.clone(), value.clone());
        }
        let mut request = client.request(method, url).headers(headers);
        if !message.body.is_empty() {
            request = request.body(message.body.clone());
        }
        let mut response = request
            .send()
            .await
            .map_err(|err| upstream_failure(url, &err))?;
        let status = response.status().as_u16();
        let headers = capture_headers(response.headers());
        let oversized = || PhantomError::Upstream {
            target: url.to_owned(),
            reason: format!(
                "response body is larger than the {limit} byte limit, raise --max-body-bytes"
            ),
        };
        let declared = response.content_length().unwrap_or(0);
        if declared > limit as u64 {
            return Err(oversized());
        }
        let mut body = Vec::with_capacity(usize::try_from(declared).unwrap_or(0));
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|err| upstream_failure(url, &err))?
        {
            if body.len() + chunk.len() > limit {
                return Err(oversized());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(UpstreamReply {
            status,
            headers,
            body: Bytes::from(body),
        })
    }

    pub fn join_url(base: &str, path_and_query: &str) -> String {
        if path_and_query.starts_with('/') {
            format!("{base}{path_and_query}")
        } else {
            format!("{base}/{path_and_query}")
        }
    }

    pub fn elapsed_us(started: Instant) -> u64 {
        u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
    }

    pub fn failure(err: PhantomError) -> Response {
        let status =
            StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        if status.is_server_error() {
            tracing::warn!(error = %err, "proxy request failed");
        }
        let mut response = (
            status,
            Json(json!({ "error": err.kind(), "message": err.to_string() })),
        )
            .into_response();
        set_header(&mut response, ERROR_HEADER, err.kind());
        response
    }

    pub fn describe(err: &(dyn std::error::Error + 'static)) -> String {
        let mut parts = vec![err.to_string()];
        let mut source = err.source();
        while let Some(cause) = source {
            let text = cause.to_string();
            if parts
                .last()
                .map_or(true, |previous| !previous.contains(&text))
            {
                parts.push(text);
            }
            source = cause.source();
        }
        parts.join(": ")
    }

    fn upstream_failure(url: &str, err: &reqwest::Error) -> PhantomError {
        PhantomError::Upstream {
            target: url.to_owned(),
            reason: describe(err),
        }
    }

    fn connection_tokens(headers: &[(String, String)]) -> Vec<String> {
        headers
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case("connection"))
            .flat_map(|(_, value)| value.split(','))
            .map(|token| token.trim().to_ascii_lowercase())
            .filter(|token| !token.is_empty())
            .collect()
    }

    fn is_hop_by_hop(lowered: &str, tokens: &[String]) -> bool {
        HOP_BY_HOP.contains(&lowered) || tokens.iter().any(|token| token == lowered)
    }

    fn insert(map: &mut HeaderMap, name: &'static str, value: &str) {
        if let Ok(value) = HeaderValue::from_str(value) {
            map.insert(HeaderName::from_static(name), value);
        }
    }

    fn append(map: &mut HeaderMap, name: &str, value: &str) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_bytes(value.as_bytes()),
        ) {
            map.append(name, value);
        }
    }
}
