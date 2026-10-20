use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::Duration;

use chrono::Utc;
use futures::channel::oneshot;
use futures::StreamExt;
use gloo_net::http::{Request, Response};
use gloo_net::websocket::futures::WebSocket;
use gloo_net::websocket::Message;
use leptos::html::Input;
use leptos::*;
use phantom_core::models::{ReplayAllRequest, ReplayRequest};
use phantom_core::{OperatingMode, ReplayBatchSummary, ReplayReport, TrafficSession, WsEvent};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use uuid::Uuid;

pub const SESSION_WINDOW: usize = 500;
pub const REPORT_WINDOW: usize = 2_000;

const NOTICE_TTL: Duration = Duration::from_secs(9);
const WIPE_WINDOW: Duration = Duration::from_secs(4);
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketStatus {
    Connecting,
    Live,
    Offline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InspectorTab {
    Diff,
    Mocks,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    Success,
    Failure,
    Info,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub id: u64,
    pub at: String,
    pub tone: Tone,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError(String);

impl fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Deserialize)]
struct ErrorBody {
    message: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
pub struct Totals {
    pub sessions: u64,
    pub reports: u64,
}

#[derive(Clone, Copy)]
pub struct DashboardState {
    pub sessions: RwSignal<Vec<TrafficSession>>,
    pub reports: RwSignal<Vec<ReplayReport>>,
    pub mode: RwSignal<Option<OperatingMode>>,
    pub socket: RwSignal<SocketStatus>,
    pub selected_session: RwSignal<Option<Uuid>>,
    pub selected_report: RwSignal<Option<Uuid>>,
    pub tab: RwSignal<InspectorTab>,
    pub target_url: RwSignal<String>,
    pub replaying: RwSignal<HashSet<Uuid>>,
    pub batch_running: RwSignal<bool>,
    pub wipe_armed: RwSignal<bool>,
    pub totals: RwSignal<Totals>,
    pub detached: RwSignal<Option<TrafficSession>>,
    pub help_open: RwSignal<bool>,
    pub notice: RwSignal<Option<Notice>>,
    pub verdicts: Memo<HashMap<Uuid, bool>>,
    pub routes: Memo<HashMap<Uuid, String>>,
    pub command_input: NodeRef<Input>,
    pub target_input: NodeRef<Input>,
    notice_sequence: StoredValue<u64>,
    wipe_sequence: StoredValue<u64>,
}

impl DashboardState {
    pub fn new() -> Self {
        let sessions = create_rw_signal(Vec::<TrafficSession>::new());
        let reports = create_rw_signal(Vec::<ReplayReport>::new());
        let verdicts = create_memo(move |_| {
            reports.with(|reports| {
                let mut latest = HashMap::with_capacity(reports.len());
                for report in reports {
                    latest.entry(report.session_id).or_insert(report.passed);
                }
                latest
            })
        });
        let routes = create_memo(move |_| {
            sessions.with(|sessions| {
                sessions
                    .iter()
                    .map(|session| {
                        (
                            session.id,
                            format!(
                                "{} {}",
                                session.inbound_request.method, session.inbound_request.uri
                            ),
                        )
                    })
                    .collect::<HashMap<_, _>>()
            })
        });
        Self {
            sessions,
            reports,
            mode: create_rw_signal(None),
            socket: create_rw_signal(SocketStatus::Connecting),
            selected_session: create_rw_signal(None),
            selected_report: create_rw_signal(None),
            tab: create_rw_signal(InspectorTab::Diff),
            target_url: create_rw_signal(String::new()),
            replaying: create_rw_signal(HashSet::new()),
            batch_running: create_rw_signal(false),
            wipe_armed: create_rw_signal(false),
            totals: create_rw_signal(Totals::default()),
            detached: create_rw_signal(None),
            help_open: create_rw_signal(false),
            notice: create_rw_signal(None),
            verdicts,
            routes,
            command_input: create_node_ref(),
            target_input: create_node_ref(),
            notice_sequence: store_value(0),
            wipe_sequence: store_value(0),
        }
    }

    pub fn bootstrap(self) {
        spawn_local(self.refresh());
        self.connect_stream();
    }

    pub async fn refresh(self) {
        match fetch_mode().await {
            Ok(mode) => self.adopt_mode(mode),
            Err(err) => self.notify(Tone::Failure, format!("mode: {err}")),
        }
        match fetch_totals().await {
            Ok(totals) => self.totals.set(totals),
            Err(err) => self.notify(Tone::Failure, format!("health: {err}")),
        }
        match fetch_sessions(SESSION_WINDOW).await {
            Ok(snapshot) => self.sessions.update(|sessions| {
                reconcile(sessions, snapshot, |session| session.id, SESSION_WINDOW)
            }),
            Err(err) => self.notify(Tone::Failure, format!("sessions: {err}")),
        }
        match fetch_reports(REPORT_WINDOW).await {
            Ok(snapshot) => self
                .reports
                .update(|reports| reconcile(reports, snapshot, |report| report.id, REPORT_WINDOW)),
            Err(err) => self.notify(Tone::Failure, format!("reports: {err}")),
        }
        if self.selected_session.get_untracked().is_none() {
            let newest = self
                .sessions
                .with_untracked(|sessions| sessions.first().map(|session| session.id));
            self.selected_session.set(newest);
        }
    }

    pub fn apply(self, event: WsEvent) {
        match event {
            WsEvent::SessionRecorded(session) => {
                let session_id = session.id;
                let mut inserted = false;
                self.sessions.update(|sessions| {
                    if sessions.iter().all(|existing| existing.id != session.id) {
                        sessions.insert(0, session);
                        sessions.truncate(SESSION_WINDOW);
                        inserted = true;
                    }
                });
                if inserted {
                    self.totals.update(|totals| totals.sessions += 1);
                }
                if self.selected_session.get_untracked().is_none() {
                    self.selected_session.set(Some(session_id));
                }
            }
            WsEvent::ReplayCompleted(report) => {
                let mut inserted = false;
                self.reports.update(|reports| {
                    if reports.iter().all(|existing| existing.id != report.id) {
                        reports.insert(0, report);
                        reports.truncate(REPORT_WINDOW);
                        inserted = true;
                    }
                });
                if inserted {
                    self.totals.update(|totals| totals.reports += 1);
                }
            }
            WsEvent::ModeChanged(mode) => self.adopt_mode(mode),
        }
    }

    pub fn select_session(self, session_id: Uuid) {
        self.selected_session.set(Some(session_id));
        self.selected_report.set(None);
    }

    pub fn select_report(self, session_id: Uuid, report_id: Uuid) {
        self.selected_session.set(Some(session_id));
        self.selected_report.set(Some(report_id));
        self.tab.set(InspectorTab::Diff);
        let loaded = self
            .sessions
            .with_untracked(|sessions| sessions.iter().any(|session| session.id == session_id))
            || self.detached.with_untracked(|detached| {
                detached
                    .as_ref()
                    .is_some_and(|session| session.id == session_id)
            });
        if loaded {
            return;
        }
        spawn_local(async move {
            match fetch_session(session_id).await {
                Ok(session) if self.selected_session.get_untracked() == Some(session_id) => {
                    self.detached.set(Some(session));
                }
                Ok(_) => {}
                Err(err) => self.notify(
                    Tone::Failure,
                    format!("session {}: {err}", short_hex(session_id)),
                ),
            }
        });
    }

    pub fn move_selection(self, step: isize) {
        let current = self.selected_session.get_untracked();
        let next = self.sessions.with_untracked(|sessions| {
            let last = sessions.len().checked_sub(1)?;
            let index = current
                .and_then(|id| sessions.iter().position(|session| session.id == id))
                .map_or(0, |index| index.saturating_add_signed(step).min(last));
            Some(sessions[index].id)
        });
        if let Some(session_id) = next {
            self.select_session(session_id);
        }
    }

    pub fn select_edge(self, last: bool) {
        let edge = self.sessions.with_untracked(|sessions| {
            if last {
                sessions.last().map(|session| session.id)
            } else {
                sessions.first().map(|session| session.id)
            }
        });
        if let Some(session_id) = edge {
            self.select_session(session_id);
        }
    }

    pub fn replay_selected(self) {
        match self.selected_session.get_untracked() {
            Some(session_id) => self.replay(session_id),
            None => self.notify(
                Tone::Failure,
                "nothing selected. j/k to pick a session".to_owned(),
            ),
        }
    }

    pub fn replay(self, session_id: Uuid) {
        if self
            .replaying
            .with_untracked(|active| active.contains(&session_id))
        {
            return;
        }
        self.replaying.update(|active| {
            active.insert(session_id);
        });
        let target_url = self.target_override();
        spawn_local(async move {
            match replay_session(session_id, target_url).await {
                Ok(report) => {
                    let report_id = report.id;
                    let summary = if report.passed {
                        (
                            Tone::Success,
                            format!(
                                "replay {} PASS  status {}  {} mocks  {:.2}ms",
                                short_hex(session_id),
                                report.replay_status,
                                report.mocked_outbound_count,
                                report.replay_latency_us as f64 / 1_000.0
                            ),
                        )
                    } else {
                        (
                            Tone::Failure,
                            format!(
                                "replay {} FAIL  body drift {}  outbound drift {}  status {}->{}",
                                short_hex(session_id),
                                report.body_diffs.len(),
                                report.outbound_diffs.len(),
                                report.original_status,
                                report.replay_status
                            ),
                        )
                    };
                    self.apply(WsEvent::ReplayCompleted(report));
                    let current = self.selected_session.get_untracked();
                    if current.is_none() || current == Some(session_id) {
                        self.select_report(session_id, report_id);
                    }
                    self.notify(summary.0, summary.1);
                }
                Err(err) => self.notify(
                    Tone::Failure,
                    format!("replay {}: {err}", short_hex(session_id)),
                ),
            }
            self.replaying.update(|active| {
                active.remove(&session_id);
            });
        });
    }

    pub fn replay_everything(self) {
        if self.batch_running.get_untracked() {
            return;
        }
        self.batch_running.set(true);
        let target_url = self.target_override();
        spawn_local(async move {
            match replay_all(target_url).await {
                Ok(summary) => {
                    let tone = if summary.failed + summary.errored == 0 {
                        Tone::Success
                    } else {
                        Tone::Failure
                    };
                    self.notify(
                        tone,
                        format!(
                            "replay-all {} -> {} pass / {} fail / {} error of {}",
                            summary.target_url,
                            summary.passed,
                            summary.failed,
                            summary.errored,
                            summary.total
                        ),
                    );
                }
                Err(err) => self.notify(Tone::Failure, format!("replay-all: {err}")),
            }
            self.batch_running.set(false);
        });
    }

    pub fn clear(self) {
        if !self.wipe_armed.get_untracked() {
            self.wipe_sequence.update_value(|sequence| *sequence += 1);
            let armed_at = self.wipe_sequence.get_value();
            self.wipe_armed.set(true);
            self.notify(
                Tone::Failure,
                "wipe every session and report? press X again to confirm".to_owned(),
            );
            set_timeout(
                move || {
                    if self.wipe_sequence.get_value() == armed_at {
                        self.wipe_armed.set(false);
                    }
                },
                WIPE_WINDOW,
            );
            return;
        }
        self.wipe_sequence.update_value(|sequence| *sequence += 1);
        self.wipe_armed.set(false);
        spawn_local(async move {
            match clear_store().await {
                Ok(()) => {
                    self.sessions.set(Vec::new());
                    self.reports.set(Vec::new());
                    self.detached.set(None);
                    self.totals.set(Totals::default());
                    self.selected_session.set(None);
                    self.selected_report.set(None);
                    self.notify(Tone::Info, "store wiped".to_owned());
                }
                Err(err) => self.notify(Tone::Failure, format!("wipe: {err}")),
            }
        });
    }

    pub fn toggle_mode(self) {
        let replay = !self
            .mode
            .with_untracked(|mode| mode.as_ref().is_some_and(OperatingMode::is_replay));
        self.set_mode(replay, None);
    }

    pub fn set_mode(self, replay: bool, explicit: Option<String>) {
        let current = self.mode.get_untracked();
        if explicit.is_none()
            && current
                .as_ref()
                .is_some_and(|mode| mode.is_replay() == replay)
        {
            return;
        }
        let Some(target) = explicit
            .or_else(|| self.target_override())
            .or_else(|| current.map(|mode| mode.target_url().to_owned()))
        else {
            self.notify(
                Tone::Failure,
                "no target url. set one with `target <url>`".to_owned(),
            );
            return;
        };
        let requested = if replay {
            OperatingMode::Replay {
                staging_url: target,
            }
        } else {
            OperatingMode::Record {
                upstream_url: target,
            }
        };
        spawn_local(async move {
            match update_mode(&requested).await {
                Ok(mode) => {
                    let line = format!(
                        "mode {} -> {}",
                        mode.label().to_ascii_lowercase(),
                        mode.target_url()
                    );
                    self.adopt_mode(mode);
                    self.notify(Tone::Info, line);
                }
                Err(err) => self.notify(Tone::Failure, format!("mode: {err}")),
            }
        });
    }

    pub fn execute(self, line: &str) {
        let words: Vec<&str> = line.split_whitespace().collect();
        match words.as_slice() {
            [] => {}
            ["help" | "?" | "man"] => self.help_open.set(true),
            ["replay" | "r"] => self.replay_selected(),
            ["replay" | "r", "all"] | ["replay-all" | "ra"] => self.replay_everything(),
            ["replay" | "r", token] => match self.find_session(token) {
                Ok(session_id) => {
                    self.select_session(session_id);
                    self.replay(session_id);
                }
                Err(reason) => self.notify(Tone::Failure, reason),
            },
            ["mode", "record" | "rec"] => self.set_mode(false, None),
            ["mode", "replay" | "sandbox"] => self.set_mode(true, None),
            ["mode", "record" | "rec", url] => self.set_mode(false, Some((*url).to_owned())),
            ["mode", "replay" | "sandbox", url] => self.set_mode(true, Some((*url).to_owned())),
            ["target", url] => {
                self.target_url.set((*url).to_owned());
                self.notify(Tone::Info, format!("target -> {url}"));
            }
            ["open", token] => match self.find_session(token) {
                Ok(session_id) => self.select_session(session_id),
                Err(reason) => self.notify(Tone::Failure, reason),
            },
            ["diff"] => self.tab.set(InspectorTab::Diff),
            ["mocks"] => self.tab.set(InspectorTab::Mocks),
            ["clear" | "wipe"] => self.clear(),
            ["refresh" | "sync"] => spawn_local(self.refresh()),
            _ => self.notify(
                Tone::Failure,
                format!("unknown command: {}  (try `help`)", line.trim()),
            ),
        }
    }

    pub fn focus_command(self) {
        if let Some(input) = self.command_input.get_untracked() {
            let _ = input.focus();
        }
    }

    pub fn focus_target(self) {
        if let Some(input) = self.target_input.get_untracked() {
            let _ = input.focus();
            input.select();
        }
    }

    pub fn blur_inputs(self) {
        for input in [self.command_input, self.target_input] {
            if let Some(input) = input.get_untracked() {
                let _ = input.blur();
            }
        }
    }

    pub fn notify(self, tone: Tone, text: String) {
        self.notice_sequence.update_value(|sequence| *sequence += 1);
        let id = self.notice_sequence.get_value();
        let at = Utc::now().format("%H:%M:%S").to_string();
        self.notice.set(Some(Notice { id, at, tone, text }));
        set_timeout(
            move || {
                let current = self
                    .notice
                    .with_untracked(|notice| notice.as_ref().map(|notice| notice.id));
                if current == Some(id) {
                    self.notice.set(None);
                }
            },
            NOTICE_TTL,
        );
    }

    fn find_session(self, token: &str) -> Result<Uuid, String> {
        let needle = token
            .trim_start_matches('#')
            .to_ascii_lowercase()
            .replace('-', "");
        if needle.is_empty() {
            return Err("empty session id".to_owned());
        }
        let matches: Vec<Uuid> = self.sessions.with_untracked(|sessions| {
            sessions
                .iter()
                .map(|session| session.id)
                .filter(|id| {
                    let simple = id.simple().to_string();
                    simple.ends_with(&needle) || simple.starts_with(&needle)
                })
                .collect()
        });
        match matches.as_slice() {
            [single] => Ok(*single),
            [] => Err(format!("no session matches `{token}`")),
            many => Err(format!(
                "`{token}` matches {} sessions, type more of the id",
                many.len()
            )),
        }
    }

    fn adopt_mode(self, mode: OperatingMode) {
        let changed = self
            .mode
            .with_untracked(|current| current.as_ref() != Some(&mode));
        let untouched = self
            .target_url
            .with_untracked(|target| target.trim().is_empty());
        if changed || untouched {
            self.target_url.set(mode.target_url().to_owned());
        }
        if changed {
            self.mode.set(Some(mode));
        }
    }

    fn target_override(self) -> Option<String> {
        let target = self.target_url.get_untracked().trim().to_owned();
        (!target.is_empty()).then_some(target)
    }

    fn connect_stream(self) {
        spawn_local(async move {
            let mut backoff = INITIAL_BACKOFF;
            loop {
                self.socket.set(SocketStatus::Connecting);
                let was_live = match stream_url() {
                    Some(url) => self.pump(&url).await,
                    None => false,
                };
                self.socket.set(SocketStatus::Offline);
                backoff = if was_live {
                    INITIAL_BACKOFF
                } else {
                    (backoff * 2).min(MAX_BACKOFF)
                };
                sleep(backoff).await;
            }
        });
    }

    async fn pump(self, url: &str) -> bool {
        let Ok(mut socket) = WebSocket::open(url) else {
            return false;
        };
        let mut live = false;
        while let Some(frame) = socket.next().await {
            match frame {
                Ok(Message::Text(payload)) => {
                    if !live {
                        live = true;
                        self.socket.set(SocketStatus::Live);
                        self.refresh().await;
                    }
                    match serde_json::from_str::<WsEvent>(&payload) {
                        Ok(event) => self.apply(event),
                        Err(err) => logging::warn!("discarding malformed event: {err}"),
                    }
                }
                Ok(Message::Bytes(_)) => {}
                Err(_) => break,
            }
        }
        live
    }
}

impl Default for DashboardState {
    fn default() -> Self {
        Self::new()
    }
}

pub fn short_hex(id: Uuid) -> String {
    let simple = id.simple().to_string();
    simple[simple.len().saturating_sub(7)..].to_owned()
}

pub async fn fetch_sessions(limit: usize) -> Result<Vec<TrafficSession>, ApiError> {
    let response = Request::get(&format!("/api/sessions?limit={limit}"))
        .send()
        .await
        .map_err(transport)?;
    decode(response).await
}

pub async fn fetch_reports(limit: usize) -> Result<Vec<ReplayReport>, ApiError> {
    let response = Request::get(&format!("/api/reports?limit={limit}"))
        .send()
        .await
        .map_err(transport)?;
    decode(response).await
}

pub async fn fetch_session(session_id: Uuid) -> Result<TrafficSession, ApiError> {
    let response = Request::get(&format!("/api/sessions/{session_id}"))
        .send()
        .await
        .map_err(transport)?;
    decode(response).await
}

pub async fn fetch_totals() -> Result<Totals, ApiError> {
    let response = Request::get("/api/health")
        .send()
        .await
        .map_err(transport)?;
    decode(response).await
}

fn reconcile<T>(current: &mut Vec<T>, snapshot: Vec<T>, id: impl Fn(&T) -> Uuid, cap: usize) {
    let newest = snapshot.iter().map(&id).max();
    let mut merged: Vec<T> = current
        .drain(..)
        .filter(|item| newest.is_some_and(|newest| id(item) > newest))
        .collect();
    merged.extend(snapshot);
    merged.sort_by_key(|item| std::cmp::Reverse(id(item)));
    merged.dedup_by_key(|item| id(item));
    merged.truncate(cap);
    *current = merged;
}

pub async fn fetch_mode() -> Result<OperatingMode, ApiError> {
    let response = Request::get("/api/mode").send().await.map_err(transport)?;
    decode(response).await
}

pub async fn update_mode(mode: &OperatingMode) -> Result<OperatingMode, ApiError> {
    let response = Request::post("/api/mode")
        .json(mode)
        .map_err(transport)?
        .send()
        .await
        .map_err(transport)?;
    decode(response).await
}

pub async fn replay_session(
    session_id: Uuid,
    target_url: Option<String>,
) -> Result<ReplayReport, ApiError> {
    let response = Request::post(&format!("/api/replay/{session_id}"))
        .json(&ReplayRequest { target_url })
        .map_err(transport)?
        .send()
        .await
        .map_err(transport)?;
    decode(response).await
}

pub async fn replay_all(target_url: Option<String>) -> Result<ReplayBatchSummary, ApiError> {
    let response = Request::post("/api/replay-all")
        .json(&ReplayAllRequest {
            target_url,
            limit: None,
        })
        .map_err(transport)?
        .send()
        .await
        .map_err(transport)?;
    decode(response).await
}

pub async fn clear_store() -> Result<(), ApiError> {
    let response = Request::delete("/api/clear")
        .send()
        .await
        .map_err(transport)?;
    if response.ok() {
        Ok(())
    } else {
        Err(failure(response).await)
    }
}

async fn decode<T: DeserializeOwned>(response: Response) -> Result<T, ApiError> {
    if !response.ok() {
        return Err(failure(response).await);
    }
    response
        .json::<T>()
        .await
        .map_err(|err| ApiError(format!("unexpected payload: {err}")))
}

async fn failure(response: Response) -> ApiError {
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    match serde_json::from_str::<ErrorBody>(&text) {
        Ok(body) => ApiError(body.message),
        Err(_) if text.trim().is_empty() => ApiError(format!("http {status}")),
        Err(_) => ApiError(text),
    }
}

fn transport(err: gloo_net::Error) -> ApiError {
    ApiError(format!("network: {err}"))
}

fn stream_url() -> Option<String> {
    let location = web_sys::window()?.location();
    let protocol = location.protocol().ok()?;
    let host = location.host().ok()?;
    let scheme = if protocol == "https:" { "wss" } else { "ws" };
    Some(format!("{scheme}://{host}/api/ws"))
}

async fn sleep(duration: Duration) {
    let (wake, woken) = oneshot::channel::<()>();
    set_timeout(
        move || {
            let _ = wake.send(());
        },
        duration,
    );
    let _ = woken.await;
}
