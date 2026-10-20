use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use phantom_core::{
    IgnoreRules, PhantomError, PhantomResult, PhantomStore, TrafficSession, WsEvent,
};
use reqwest::Url;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub use phantom_core::models::OperatingMode;

use crate::correlator::{ActiveSessionContext, ReplaySandbox};

pub const TRACE_HEADER: &str = "x-phantom-trace-id";
pub const TARGET_HEADER: &str = "x-phantom-target-url";
pub const SESSION_HEADER: &str = "x-phantom-session-id";
pub const SANDBOX_HEADER: &str = "x-phantom-sandbox";
pub const OUTBOUND_HEADER: &str = "x-phantom-outbound";
pub const EXCHANGE_HEADER: &str = "x-phantom-exchange-id";
pub const CAPTURE_HEADER: &str = "x-phantom-capture";
pub const REPLAY_OF_HEADER: &str = "x-phantom-replay-of";
pub const VIA_HEADER: &str = "x-phantom-via";
pub const ERROR_HEADER: &str = "x-phantom-error";
pub const PHANTOM_HEADER_PREFIX: &str = "x-phantom-";
pub const REPLAY_TRACE_PREFIX: &str = "replay-";

const EVENT_BUFFER: usize = 1024;
const MAX_TRACE_LEN: usize = 128;
const RECORDER_BATCH: usize = 512;

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub upstream_url: String,
    pub max_body_bytes: usize,
    pub upstream_timeout: Duration,
    pub connect_timeout: Duration,
    pub capture_queue: usize,
    pub ignore_rules: IgnoreRules,
}

impl EngineConfig {
    pub fn new(upstream_url: &str) -> PhantomResult<Self> {
        Ok(Self {
            upstream_url: normalize_base_url(upstream_url)?,
            max_body_bytes: 16 * 1024 * 1024,
            upstream_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(5),
            capture_queue: 16_384,
            ignore_rules: IgnoreRules::default(),
        })
    }
}

#[derive(Debug)]
pub struct SessionRecorder {
    writer: JoinHandle<()>,
}

impl SessionRecorder {
    pub async fn flush(self) {
        if let Err(err) = self.writer.await {
            tracing::error!(error = %err, "session writer terminated abnormally");
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundRoute {
    Sandbox,
    Live { bucket: Option<String> },
    Blocked { reason: &'static str },
}

pub type SharedEngine = Arc<EngineState>;

pub struct EngineState {
    pub store: Arc<PhantomStore>,
    pub correlator: ActiveSessionContext,
    pub sandbox: ReplaySandbox,
    pub http: reqwest::Client,
    pub config: EngineConfig,
    mode: RwLock<OperatingMode>,
    events: broadcast::Sender<WsEvent>,
    recorder: mpsc::Sender<TrafficSession>,
    dropped_sessions: AtomicU64,
    shutdown: watch::Sender<bool>,
}

impl std::fmt::Debug for EngineState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EngineState")
            .field("mode", &*self.mode.read())
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl EngineState {
    pub fn new(
        store: Arc<PhantomStore>,
        config: EngineConfig,
    ) -> PhantomResult<(SharedEngine, SessionRecorder)> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(config.upstream_timeout)
            .connect_timeout(config.connect_timeout)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_nodelay(true)
            .build()
            .map_err(|err| {
                PhantomError::Internal(format!("http client initialisation failed: {err}"))
            })?;
        let (events, _) = broadcast::channel(EVENT_BUFFER);
        let (recorder, queue) = mpsc::channel(config.capture_queue.max(1));
        let (shutdown, _) = watch::channel(false);
        let writer = tokio::spawn(record_sessions(
            Arc::clone(&store),
            events.clone(),
            queue,
            shutdown.subscribe(),
        ));
        let mode = OperatingMode::Record {
            upstream_url: config.upstream_url.clone(),
        };
        let engine = Arc::new(Self {
            store,
            correlator: ActiveSessionContext::new(),
            sandbox: ReplaySandbox::new(),
            http,
            config,
            mode: RwLock::new(mode),
            events,
            recorder,
            dropped_sessions: AtomicU64::new(0),
            shutdown,
        });
        Ok((engine, SessionRecorder { writer }))
    }

    pub fn mode(&self) -> OperatingMode {
        self.mode.read().clone()
    }

    pub fn set_mode(&self, requested: OperatingMode) -> PhantomResult<OperatingMode> {
        let normalized = match requested {
            OperatingMode::Record { upstream_url } => OperatingMode::Record {
                upstream_url: normalize_base_url(&upstream_url)?,
            },
            OperatingMode::Replay { staging_url } => OperatingMode::Replay {
                staging_url: normalize_base_url(&staging_url)?,
            },
        };
        *self.mode.write() = normalized.clone();
        tracing::info!(
            mode = normalized.label(),
            target = normalized.target_url(),
            "operating mode changed"
        );
        self.publish(WsEvent::ModeChanged(normalized.clone()));
        Ok(normalized)
    }

    pub fn default_replay_target(&self) -> Option<String> {
        match &*self.mode.read() {
            OperatingMode::Replay { staging_url } => Some(staging_url.clone()),
            OperatingMode::Record { .. } => None,
        }
    }

    pub fn begin_shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    pub fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<WsEvent> {
        self.events.subscribe()
    }

    pub fn publish(&self, event: WsEvent) {
        let _ = self.events.send(event);
    }

    pub fn route_outbound(&self, trace_id: Option<&str>) -> OutboundRoute {
        if let Some(trace) = trace_id.filter(|trace| self.correlator.contains(trace)) {
            return OutboundRoute::Live {
                bucket: Some(trace.to_owned()),
            };
        }
        if let Some(active) = self.sandbox.active_trace() {
            match trace_id {
                Some(trace) if trace == active => return OutboundRoute::Sandbox,
                None if self.correlator.in_flight() == 0 => return OutboundRoute::Sandbox,
                None => {
                    return OutboundRoute::Blocked {
                        reason: "a replay and live recording are both in flight and this call carries no x-phantom-trace-id, so phantom cannot tell them apart",
                    }
                }
                Some(_) => {}
            }
        }
        if trace_id.is_some_and(is_replay_trace) {
            return OutboundRoute::Blocked {
                reason: "this call belongs to a replay that has already finished",
            };
        }
        match &*self.mode.read() {
            OperatingMode::Replay { .. } => OutboundRoute::Blocked {
                reason: "phantom is in sandbox mode and this call is not part of an active replay",
            },
            OperatingMode::Record { .. } => OutboundRoute::Live {
                bucket: self.correlator.resolve(trace_id),
            },
        }
    }

    pub async fn with_store<T, F>(&self, operation: F) -> PhantomResult<T>
    where
        F: FnOnce(&PhantomStore) -> PhantomResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || operation(&store))
            .await
            .map_err(|err| PhantomError::Internal(format!("storage task aborted: {err}")))?
    }

    pub fn persist_session(&self, session: TrafficSession) {
        match self.recorder.try_send(session) {
            Ok(()) => {}
            Err(TrySendError::Full(session)) => {
                let dropped = self.dropped_sessions.fetch_add(1, Ordering::Relaxed) + 1;
                if dropped.is_power_of_two() {
                    tracing::warn!(dropped, session = %session.id, "capture queue saturated, shedding sessions to protect live traffic");
                }
            }
            Err(TrySendError::Closed(session)) => {
                tracing::error!(session = %session.id, "session writer is offline, capture discarded");
            }
        }
    }

    pub fn dropped_sessions(&self) -> u64 {
        self.dropped_sessions.load(Ordering::Relaxed)
    }
}

async fn record_sessions(
    store: Arc<PhantomStore>,
    events: broadcast::Sender<WsEvent>,
    mut queue: mpsc::Receiver<TrafficSession>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut batch = Vec::with_capacity(RECORDER_BATCH);
    loop {
        let received = tokio::select! {
            received = queue.recv_many(&mut batch, RECORDER_BATCH) => received,
            _ = shutdown.wait_for(|stopping| *stopping), if !queue.is_closed() => {
                queue.close();
                continue;
            }
        };
        if received == 0 {
            break;
        }
        let pending = std::mem::replace(&mut batch, Vec::with_capacity(RECORDER_BATCH));
        let count = pending.len();
        let store = Arc::clone(&store);
        let written =
            tokio::task::spawn_blocking(move || store.insert_sessions(&pending).map(|()| pending))
                .await;
        match written {
            Ok(Ok(persisted)) => {
                for session in persisted {
                    let _ = events.send(WsEvent::SessionRecorded(session));
                }
            }
            Ok(Err(err)) => {
                tracing::error!(count, error = %err, "failed to persist captured sessions")
            }
            Err(err) => tracing::error!(count, error = %err, "session write task aborted"),
        }
    }
}

pub fn normalize_base_url(raw: &str) -> PhantomResult<String> {
    let trimmed = raw.trim();
    let url = Url::parse(trimmed).map_err(|err| {
        PhantomError::InvalidTarget(format!("`{trimmed}` is not a valid url: {err}"))
    })?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(PhantomError::InvalidTarget(format!(
            "`{trimmed}` must use the http or https scheme"
        )));
    }
    if url.host_str().is_none() {
        return Err(PhantomError::InvalidTarget(format!(
            "`{trimmed}` has no host"
        )));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(PhantomError::InvalidTarget(format!(
            "`{trimmed}` must not carry a query string or fragment"
        )));
    }
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

pub fn is_replay_trace(trace_id: &str) -> bool {
    trace_id.starts_with(REPLAY_TRACE_PREFIX)
}

pub fn is_valid_trace(trace_id: &str) -> bool {
    !trace_id.is_empty()
        && trace_id.len() <= MAX_TRACE_LEN
        && trace_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}
