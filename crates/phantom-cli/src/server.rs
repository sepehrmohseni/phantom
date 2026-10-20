use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, Request, State};
use axum::http::header::{CONTENT_TYPE, HOST, ORIGIN, UPGRADE};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use phantom_core::models::{ReplayAllRequest, ReplayRequest};
use phantom_core::{
    OperatingMode, PhantomError, ReplayBatchSummary, ReplayReport, TrafficSession, WsEvent,
};
use phantom_engine::state::VIA_HEADER;
use phantom_engine::{replayer, SharedEngine};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::broadcast::error::RecvError;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};
use uuid::Uuid;

use crate::embedded;

const DEFAULT_PAGE: usize = 200;
const MAX_PAGE: usize = 5_000;

#[derive(Debug, Clone, Default)]
pub struct ControlPlaneOptions {
    pub ui_dir: Option<PathBuf>,
    pub cors_origins: Vec<HeaderValue>,
    pub loopback_only: bool,
}

#[derive(Debug, Clone)]
struct RequestGuard {
    loopback_only: bool,
    allowed_origins: Arc<Vec<HeaderValue>>,
}

pub fn router(engine: SharedEngine, options: ControlPlaneOptions) -> Router {
    let api = Router::new()
        .route("/health", get(health))
        .route("/mode", get(current_mode).post(update_mode))
        .route("/sessions", get(list_sessions))
        .route("/sessions/:session_id", get(get_session))
        .route("/reports", get(list_reports))
        .route("/replay/:session_id", post(replay_one))
        .route("/replay-all", post(replay_all))
        .route("/clear", delete(clear_store))
        .route("/ws", get(event_stream))
        .fallback(api_not_found)
        .layer(middleware::from_fn_with_state(
            RequestGuard {
                loopback_only: options.loopback_only,
                allowed_origins: Arc::new(options.cors_origins.clone()),
            },
            guard_requests,
        ))
        .with_state(engine);

    let router = Router::new().nest("/api", api);
    let router = match options.ui_dir {
        Some(directory) => {
            let index = directory.join("index.html");
            router.fallback_service(ServeDir::new(directory).fallback(ServeFile::new(index)))
        }
        None => router.fallback(embedded::serve_asset),
    };
    if options.cors_origins.is_empty() {
        return router;
    }
    router.layer(
        ServiceBuilder::new().layer(
            CorsLayer::new()
                .allow_origin(options.cors_origins)
                .allow_methods([Method::GET, Method::POST, Method::DELETE])
                .allow_headers([CONTENT_TYPE]),
        ),
    )
}

async fn guard_requests(
    State(guard): State<RequestGuard>,
    request: Request,
    next: Next,
) -> Response {
    let headers = request.headers();
    if headers.contains_key(VIA_HEADER) {
        return rejected(
            "proxied_request_rejected",
            "requests relayed through phantom's own proxies cannot reach the control plane",
        );
    }
    if guard.loopback_only {
        let host = headers
            .get(HOST)
            .and_then(|value| value.to_str().ok())
            .or_else(|| {
                request
                    .uri()
                    .authority()
                    .map(|authority| authority.as_str())
            });
        if host.is_some_and(|host| !is_loopback_host(host)) {
            return rejected(
                "host_rejected",
                "the control plane only answers to localhost; start phantom with --ui-bind to expose it",
            );
        }
    }
    let state_changing = !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    );
    let upgrading = headers.contains_key(UPGRADE);
    if (state_changing || upgrading) && !origin_allowed(headers, &guard) {
        return rejected(
            "origin_rejected",
            "cross-origin request refused; add the origin with --cors-origin if this is intended",
        );
    }
    next.run(request).await
}

fn origin_allowed(headers: &HeaderMap, guard: &RequestGuard) -> bool {
    let Some(origin) = headers.get(ORIGIN) else {
        return true;
    };
    if guard
        .allowed_origins
        .iter()
        .any(|allowed| allowed == origin)
    {
        return true;
    }
    let Some(origin_authority) = origin
        .to_str()
        .ok()
        .and_then(|origin| origin.split_once("://"))
        .map(|(_, authority)| authority.trim_end_matches('/'))
    else {
        return false;
    };
    let same_host = headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|host| host.eq_ignore_ascii_case(origin_authority));
    same_host || (guard.loopback_only && is_loopback_host(origin_authority))
}

fn is_loopback_host(authority: &str) -> bool {
    let host = if let Some(rest) = authority.strip_prefix('[') {
        rest.split_once(']').map_or(rest, |(host, _)| host)
    } else {
        authority
            .rsplit_once(':')
            .map_or(authority, |(host, _)| host)
    };
    let host = host.to_ascii_lowercase();
    host == "localhost"
        || host.ends_with(".localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn rejected(kind: &str, message: &str) -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({ "error": kind, "message": message })),
    )
        .into_response()
}

#[derive(Debug)]
pub struct ApiError(PhantomError);

impl From<PhantomError> for ApiError {
    fn from(error: PhantomError) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status =
            StatusCode::from_u16(self.0.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        if status.is_server_error() {
            tracing::error!(error = %self.0, "control plane request failed");
        }
        (
            status,
            Json(json!({ "error": self.0.kind(), "message": self.0.to_string() })),
        )
            .into_response()
    }
}

type ApiResult<T> = Result<Json<T>, ApiError>;

#[derive(Debug, Deserialize)]
struct Page {
    limit: Option<usize>,
}

impl Page {
    fn limit(&self) -> usize {
        self.limit.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE)
    }
}

async fn health(State(engine): State<SharedEngine>) -> ApiResult<Value> {
    let (sessions, reports) = engine
        .with_store(|store| Ok((store.session_count()?, store.report_count()?)))
        .await?;
    Ok(Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "mode": engine.mode(),
        "sessions": sessions,
        "reports": reports,
        "in_flight": engine.correlator.in_flight(),
        "dropped_sessions": engine.dropped_sessions(),
        "ignored_paths": engine.config.ignore_rules.sources(),
    })))
}

async fn current_mode(State(engine): State<SharedEngine>) -> Json<OperatingMode> {
    Json(engine.mode())
}

async fn update_mode(
    State(engine): State<SharedEngine>,
    Json(requested): Json<OperatingMode>,
) -> ApiResult<OperatingMode> {
    Ok(Json(engine.set_mode(requested)?))
}

async fn list_sessions(
    State(engine): State<SharedEngine>,
    Query(page): Query<Page>,
) -> ApiResult<Vec<TrafficSession>> {
    let limit = page.limit();
    Ok(Json(
        engine
            .with_store(move |store| store.list_sessions(limit))
            .await?,
    ))
}

async fn get_session(
    State(engine): State<SharedEngine>,
    Path(session_id): Path<Uuid>,
) -> ApiResult<TrafficSession> {
    engine
        .with_store(move |store| store.get_session(session_id))
        .await?
        .map(Json)
        .ok_or_else(|| PhantomError::SessionNotFound(session_id).into())
}

async fn list_reports(
    State(engine): State<SharedEngine>,
    Query(page): Query<Page>,
) -> ApiResult<Vec<ReplayReport>> {
    let limit = page.limit();
    Ok(Json(
        engine
            .with_store(move |store| store.list_reports(limit))
            .await?,
    ))
}

async fn replay_one(
    State(engine): State<SharedEngine>,
    Path(session_id): Path<Uuid>,
    body: Bytes,
) -> ApiResult<ReplayReport> {
    let request: ReplayRequest = parse_optional_body(&body)?;
    let target = choose_target(&engine, request.target_url)?;
    Ok(Json(
        replayer::replay_session(&engine, session_id, &target).await?,
    ))
}

async fn replay_all(
    State(engine): State<SharedEngine>,
    body: Bytes,
) -> ApiResult<ReplayBatchSummary> {
    let request: ReplayAllRequest = parse_optional_body(&body)?;
    let limit = request.limit.unwrap_or(MAX_PAGE).clamp(1, MAX_PAGE);
    let target = choose_target(&engine, request.target_url)?;
    Ok(Json(replayer::replay_all(&engine, &target, limit).await?))
}

fn parse_optional_body<T: DeserializeOwned + Default>(body: &[u8]) -> Result<T, PhantomError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(T::default());
    }
    serde_json::from_slice(body)
        .map_err(|err| PhantomError::InvalidMessage(format!("request body is not valid: {err}")))
}

async fn clear_store(State(engine): State<SharedEngine>) -> Result<StatusCode, ApiError> {
    engine.with_store(|store| store.delete_all()).await?;
    tracing::info!("session store cleared");
    Ok(StatusCode::NO_CONTENT)
}

async fn api_not_found(uri: Uri) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": "route_not_found", "message": format!("no control plane route for {}", uri.path()) })),
    )
        .into_response()
}

async fn event_stream(ws: WebSocketUpgrade, State(engine): State<SharedEngine>) -> Response {
    ws.on_upgrade(move |socket| pump_events(socket, engine))
}

async fn pump_events(socket: WebSocket, engine: SharedEngine) {
    let mut events = engine.subscribe();
    let mut shutdown = engine.shutdown_signal();
    let greeting = WsEvent::ModeChanged(engine.mode());
    drop(engine);
    let (mut sink, mut inbound) = socket.split();
    if send_event(&mut sink, &greeting).await.is_err() {
        return;
    }
    loop {
        tokio::select! {
            received = events.recv() => match received {
                Ok(event) => {
                    if send_event(&mut sink, &event).await.is_err() {
                        break;
                    }
                }
                Err(RecvError::Lagged(skipped)) => {
                    tracing::debug!(skipped, "websocket subscriber fell behind, skipping events");
                }
                Err(RecvError::Closed) => break,
            },
            frame = inbound.next() => match frame {
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            () = stopping(&mut shutdown) => break,
        }
    }
    let _ = sink.close().await;
}

async fn stopping(signal: &mut tokio::sync::watch::Receiver<bool>) {
    let _ = signal.wait_for(|stopping| *stopping).await;
}

async fn send_event(
    sink: &mut SplitSink<WebSocket, Message>,
    event: &WsEvent,
) -> Result<(), axum::Error> {
    let payload = serde_json::to_string(event).map_err(axum::Error::new)?;
    sink.send(Message::Text(payload)).await
}

fn choose_target(engine: &SharedEngine, requested: Option<String>) -> Result<String, PhantomError> {
    requested
        .map(|target| target.trim().to_owned())
        .filter(|target| !target.is_empty())
        .or_else(|| engine.default_replay_target())
        .ok_or_else(|| {
            PhantomError::InvalidTarget(
                "no replay target given; pass target_url (phantom replay --target URL) or switch to sandbox mode with a staging url. phantom never falls back to the recording upstream".to_owned(),
            )
        })
}
