use std::collections::HashSet;
use std::time::Duration;

use chrono::{DateTime, Utc};
use leptos::*;
use phantom_core::{
    DiffKind, HttpMessage, JsonDiffEntry, OperatingMode, OutboundExchange, ReplayReport,
    TrafficSession,
};
use serde_json::Value;
use wasm_bindgen::JsCast;
use web_sys::{ScrollIntoViewOptions, ScrollLogicalPosition};

use crate::api::{short_hex, DashboardState, InspectorTab, SocketStatus, Tone};

const LOG_ROWS: usize = 300;
const SPARK_RUNS: usize = 40;
const GAUGE_CELLS: usize = 26;
const CODE_LINE_LIMIT: usize = 600;
const JSON_PREVIEW_LIMIT: usize = 4_000;
const SPARK_LEVELS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
const GHOST: &str = "   .-.\n  (o o)\n  | O \\\n   \\   \\\n    `~~~'";

const KEYMAP: [(&str, &str); 13] = [
    ("j  k  ↑ ↓", "move through captured sessions"),
    ("g  G", "jump to newest / oldest"),
    ("enter  1", "diff inspector"),
    ("2", "outbound mocks"),
    ("r", "replay the selected session"),
    ("R", "replay everything against the target"),
    ("m", "flip record / sandbox mode"),
    ("t", "edit the target url"),
    (":  /", "command line"),
    ("↑ in prompt", "previous command"),
    ("X  X", "wipe the store (press twice)"),
    ("?", "this screen"),
    ("esc", "close / leave input"),
];

const COMMANDS: [(&str, &str); 8] = [
    (
        "replay [id|all]",
        "id = any prefix or suffix of the session uuid",
    ),
    ("open <id>", "select a session"),
    (
        "mode record [url]",
        "capture again, optionally against a new upstream",
    ),
    (
        "mode replay [url]",
        "sandbox mode, optionally with a new staging url",
    ),
    ("target <url>", "where replays are fired"),
    ("diff | mocks", "switch inspector tab"),
    ("refresh", "resync from the control plane"),
    ("wipe", "same as X"),
];

fn dashboard() -> DashboardState {
    expect_context::<DashboardState>()
}

#[component]
pub fn App() -> impl IntoView {
    let state = DashboardState::new();
    provide_context(state);
    state.bootstrap();
    bind_keys(state);
    view! {
        <div class="flex min-h-screen flex-col bg-ink font-mono text-fg xl:h-screen">
            <StatusBar/>
            <ControlBar/>
            <Gauges/>
            <main class="grid flex-1 grid-cols-1 gap-x-3 gap-y-6 px-3 pb-3 pt-5 xl:min-h-0 xl:grid-cols-12">
                <div class="flex min-w-0 flex-col gap-6 xl:col-span-5 xl:min-h-0">
                    <TrafficPane/>
                    <ReplayLogPane/>
                </div>
                <div class="flex min-w-0 flex-col xl:col-span-7 xl:min-h-0">
                    <InspectorPane/>
                </div>
            </main>
            <CommandDock/>
            <HelpScreen/>
        </div>
    }
}

fn bind_keys(state: DashboardState) {
    let handle = window_event_listener(ev::keydown, move |event| {
        if event.ctrl_key() || event.meta_key() || event.alt_key() {
            return;
        }
        let raw = event.key();
        let key = if raw.chars().count() == 1
            && raw.chars().all(|character| character.is_ascii_alphabetic())
        {
            if event.shift_key() {
                raw.to_ascii_uppercase()
            } else {
                raw.to_ascii_lowercase()
            }
        } else {
            raw
        };
        if event.repeat() && !matches!(key.as_str(), "j" | "k" | "ArrowDown" | "ArrowUp") {
            return;
        }
        if key == "Escape" {
            state.help_open.set(false);
            state.blur_inputs();
            return;
        }
        let focused = event
            .target()
            .and_then(|target| target.dyn_into::<web_sys::Element>().ok())
            .map(|element| element.tag_name())
            .unwrap_or_default();
        if matches!(focused.as_str(), "INPUT" | "TEXTAREA" | "SELECT") {
            return;
        }
        if matches!(focused.as_str(), "BUTTON" | "A" | "SUMMARY")
            && matches!(key.as_str(), "Enter" | " ")
        {
            return;
        }
        match key.as_str() {
            "j" | "ArrowDown" => {
                state.move_selection(1);
                reveal_selected(state);
            }
            "k" | "ArrowUp" => {
                state.move_selection(-1);
                reveal_selected(state);
            }
            "g" | "Home" => {
                state.select_edge(false);
                reveal_selected(state);
            }
            "G" | "End" => {
                state.select_edge(true);
                reveal_selected(state);
            }
            "Enter" | "1" => state.tab.set(InspectorTab::Diff),
            "2" => state.tab.set(InspectorTab::Mocks),
            "r" => state.replay_selected(),
            "R" => state.replay_everything(),
            "m" => state.toggle_mode(),
            "t" => state.focus_target(),
            "X" => state.clear(),
            ":" | "/" => state.focus_command(),
            "?" => state.help_open.update(|open| *open = !*open),
            _ => return,
        }
        event.prevent_default();
    });
    on_cleanup(move || handle.remove());
}

#[component]
fn StatusBar() -> impl IntoView {
    let state = dashboard();
    let clock = create_rw_signal(Utc::now());
    set_interval(move || clock.set(Utc::now()), Duration::from_secs(1));
    let mode = move || {
        state.mode.with(|mode| match mode {
            Some(OperatingMode::Record { upstream_url }) => {
                ("bg-bad", "● rec", format!("upstream {upstream_url}"))
            }
            Some(OperatingMode::Replay { staging_url }) => {
                ("bg-info", "◆ sandbox", format!("staging {staging_url}"))
            }
            None => ("bg-dim", "…", "resolving".to_owned()),
        })
    };
    let socket = move || match state.socket.get() {
        SocketStatus::Live => ("", "ws:live"),
        SocketStatus::Connecting => ("blink", "ws:dial"),
        SocketStatus::Offline => ("bg-bad", "ws:down"),
    };
    let tally = move || {
        let totals = state.totals.get();
        format!("{} sess  {} runs", totals.sessions, totals.reports)
    };
    view! {
        <div class="flex select-none items-stretch overflow-hidden whitespace-nowrap bg-ok text-[12px] font-semibold text-ink">
            <span class="bg-hi px-2">"[phantom]"</span>
            <span class=move || format!("px-2 {}", mode().0)>{move || mode().1}</span>
            <span class="hidden min-w-0 truncate px-2 sm:inline">{move || mode().2}</span>
            <span class="ml-auto hidden px-2 md:inline">{tally}</span>
            <span class=move || format!("px-2 {}", socket().0)>{move || socket().1}</span>
            <span class="bg-hi px-2 tabular-nums">{move || clock.get().format("%H:%M:%S").to_string()}</span>
        </div>
    }
}

#[component]
fn ControlBar() -> impl IntoView {
    let state = dashboard();
    let is_replay = move || {
        state
            .mode
            .with(|mode| mode.as_ref().is_some_and(OperatingMode::is_replay))
    };
    let batch_disabled = move || state.batch_running.get() || state.sessions.with(Vec::is_empty);
    let target_ref = state.target_input;
    view! {
        <div class="flex flex-wrap items-center gap-x-4 gap-y-2 border-b border-rule px-3 py-2">
            <div class="flex items-center border border-rule">
                <button class=move || mode_class(!is_replay(), "bg-bad") on:click=move |_| state.set_mode(false, None)>
                    <span class=move || if is_replay() { "" } else { "blink" }>"●"</span>
                    " REC"
                </button>
                <button class=move || mode_class(is_replay(), "bg-info") on:click=move |_| state.set_mode(true, None)>
                    "◆ SANDBOX"
                </button>
            </div>
            <label class="flex min-w-0 flex-1 items-center gap-2 border-b border-rule focus-within:border-ok sm:max-w-xl">
                <span class="text-ok">"target>"</span>
                <input
                    node_ref=target_ref
                    type="text"
                    spellcheck="false"
                    autocomplete="off"
                    placeholder="http://staging:3000"
                    class="min-w-0 flex-1 bg-transparent py-1 text-hi outline-none"
                    prop:value=move || state.target_url.get()
                    on:input=move |event| state.target_url.set(event_target_value(&event))
                    on:keydown=move |event: web_sys::KeyboardEvent| {
                        if event.key() == "Enter" {
                            state.blur_inputs();
                            state.notify(Tone::Info, format!("target -> {}", state.target_url.get_untracked()));
                        }
                    }
                />
            </label>
            <div class="ml-auto flex items-center gap-1">
                <button class="btn px-1.5 py-0.5" disabled=batch_disabled on:click=move |_| state.replay_everything()>
                    <span class="text-warn">"[R]"</span>
                    {move || if state.batch_running.get() { " running…" } else { " replay-all" }}
                </button>
                <button
                    class=move || if state.wipe_armed.get() { "btn bg-bad px-1.5 py-0.5 text-ink" } else { "btn px-1.5 py-0.5" }
                    on:click=move |_| state.clear()
                >
                    <span class=move || if state.wipe_armed.get() { "text-ink" } else { "text-bad" }>"[X]"</span>
                    {move || if state.wipe_armed.get() { " confirm wipe" } else { " clear-store" }}
                </button>
                <button class="btn px-1.5 py-0.5" on:click=move |_| state.help_open.update(|open| *open = !*open)>
                    <span class="text-info">"[?]"</span>
                    " keys"
                </button>
            </div>
        </div>
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct ReplayStats {
    evaluated: usize,
    passed: usize,
    mean_delta_us: Option<f64>,
}

fn summarize(reports: &[ReplayReport]) -> ReplayStats {
    let mut seen = HashSet::new();
    let latest: Vec<&ReplayReport> = reports
        .iter()
        .filter(|report| seen.insert(report.session_id))
        .collect();
    if latest.is_empty() {
        return ReplayStats::default();
    }
    let passed = latest.iter().filter(|report| report.passed).count();
    let total: f64 = latest
        .iter()
        .map(|report| report.latency_delta_us() as f64)
        .sum();
    ReplayStats {
        evaluated: latest.len(),
        passed,
        mean_delta_us: Some(total / latest.len() as f64),
    }
}

#[component]
fn Gauges() -> impl IntoView {
    let state = dashboard();
    let stats = create_memo(move |_| state.reports.with(|reports| summarize(reports)));
    let captured = move || state.totals.with(|totals| totals.sessions);
    let exchanges = move || {
        state.sessions.with(|sessions| {
            sessions
                .iter()
                .map(|session| session.outbound_calls.len())
                .sum::<usize>()
        })
    };
    let deltas = create_memo(move |_| {
        state.reports.with(|reports| {
            reports
                .iter()
                .take(SPARK_RUNS)
                .map(ReplayReport::latency_delta_us)
                .collect::<Vec<_>>()
        })
    });
    let rate = move || {
        let stats = stats.get();
        if stats.evaluated == 0 {
            ("text-rule", "--.-%".to_owned())
        } else {
            let tenths = stats.passed * 1_000 / stats.evaluated;
            let class = if stats.passed == stats.evaluated {
                "text-ok glow"
            } else {
                "text-bad"
            };
            (class, format!("{}.{}%", tenths / 10, tenths % 10))
        }
    };
    let delta = move || match stats.get().mean_delta_us {
        Some(value) if value < 0.0 => ("text-ok glow", format!("{:+.1}ms", value / 1_000.0)),
        Some(value) if value > 0.0 => ("text-bad", format!("{:+.1}ms", value / 1_000.0)),
        Some(_) => ("text-fg", "±0.0ms".to_owned()),
        None => ("text-rule", "--.-ms".to_owned()),
    };
    view! {
        <div class="grid grid-cols-1 gap-x-3 gap-y-6 px-3 pt-5 sm:grid-cols-3">
            <section class="pane px-3 pb-2 pt-3">
                <div class="pane-head">
                    <span class="text-hi">"captured sessions"</span>
                    <span class="text-dim">"wc -l"</span>
                </div>
                <div class="text-[28px] font-semibold leading-none tabular-nums">{move || odometer(captured())}</div>
                <div class="mt-2 text-[11px] text-dim">{move || format!("{} outbound exchanges in the newest {}", exchanges(), state.sessions.with(Vec::len))}</div>
            </section>
            <section class="pane px-3 pb-2 pt-3">
                <div class="pane-head">
                    <span class="text-hi">"pass rate"</span>
                    <span class="text-dim">{move || format!("{}/{} sessions", stats.get().passed, stats.get().evaluated)}</span>
                </div>
                <div class=move || format!("text-[28px] font-semibold leading-none tabular-nums {}", rate().0)>{move || rate().1}</div>
                <div class="mt-2 overflow-hidden whitespace-nowrap text-[11px]">{move || gauge(stats.get())}</div>
            </section>
            <section class="pane px-3 pb-2 pt-3">
                <div class="pane-head">
                    <span class="text-hi">"avg latency delta"</span>
                    <span class="text-dim">"replay - recorded"</span>
                </div>
                <div class=move || format!("text-[28px] font-semibold leading-none tabular-nums {}", delta().0)>{move || delta().1}</div>
                <div class="mt-2 overflow-hidden whitespace-nowrap text-[11px]">{move || sparkline(deltas.get())}</div>
            </section>
        </div>
    }
}

#[component]
fn TrafficPane() -> impl IntoView {
    let state = dashboard();
    let count = move || state.sessions.with(Vec::len);
    view! {
        <section class="pane flex min-h-[16rem] flex-col xl:min-h-0 xl:flex-[3]">
            <div class="pane-head">
                <span class="text-hi">"1 traffic"</span>
                <span class="text-dim">{move || format!("{} captured, newest first", count())}</span>
            </div>
            <div class="flex items-center gap-2 border-b border-rule px-2 pb-1 pt-3 text-[11px] text-dim">
                <span class="w-[3.75rem] shrink-0">"sid"</span>
                <span class="w-12 shrink-0">"verb"</span>
                <span class="w-8 shrink-0">"st"</span>
                <span class="hidden w-8 shrink-0 sm:inline">"out"</span>
                <span class="hidden w-16 shrink-0 text-right sm:inline">"took"</span>
                <span class="w-10 shrink-0 text-center">"last"</span>
                <span class="min-w-0 flex-1">"path"</span>
            </div>
            <Show
                when=move || { count() > 0 }
                fallback=|| view! {
                    <Ghost
                        headline="nothing captured yet"
                        hint="send requests through the inbound proxy on :8080. sessions show up here as soon as the response is back."
                    />
                }
            >
                <ul class="max-h-[50vh] min-h-0 flex-1 overflow-y-auto xl:max-h-none">
                    <For
                        each=move || state.sessions.get()
                        key=|session| session.id
                        children=move |session| view! { <SessionRow session=session/> }
                    />
                </ul>
            </Show>
        </section>
    }
}

#[component]
fn SessionRow(session: TrafficSession) -> impl IntoView {
    let state = dashboard();
    let session_id = session.id;
    let method = session.inbound_request.method.clone();
    let verb_class = format!("w-12 shrink-0 font-semibold {}", method_color(&method));
    let uri = session.inbound_request.uri.clone();
    let uri_title = uri.clone();
    let status = session.status();
    let mocks = session.outbound_calls.len();
    let took = format!("{:.1}ms", session.total_duration_us as f64 / 1_000.0);
    let sid = short_hex(session_id);
    let selected = move || state.selected_session.get() == Some(session_id);
    let replaying = move || state.replaying.with(|active| active.contains(&session_id));
    let verdict = move || {
        state
            .verdicts
            .with(|verdicts| verdicts.get(&session_id).copied())
    };
    view! {
        <li
            id=format!("session-{}", session_id.simple())
            class=move || if selected() { "row sel flex items-center gap-2 px-2 py-[3px]" } else { "row flex items-center gap-2 px-2 py-[3px]" }
            on:click=move |_| state.select_session(session_id)
        >
            <span class="w-[3.75rem] shrink-0 text-dim">{sid}</span>
            <span class=verb_class>{method}</span>
            <span class=format!("w-8 shrink-0 tabular-nums {}", status_color(status))>{status.to_string()}</span>
            <span class=format!("hidden w-8 shrink-0 tabular-nums sm:inline {}", if mocks == 0 { "text-rule" } else { "text-info" })>
                {format!("{mocks}x")}
            </span>
            <span class="hidden w-16 shrink-0 text-right tabular-nums text-dim sm:inline">{took}</span>
            <span class="w-10 shrink-0 text-center">{move || verdict_mark(verdict())}</span>
            <span class="min-w-0 flex-1 truncate text-hi" title=uri_title>{uri}</span>
            <button
                class="btn shrink-0 px-1 text-ok"
                disabled=replaying
                on:click=move |event| {
                    event.stop_propagation();
                    state.replay(session_id);
                }
            >
                {move || if replaying() { "[ .. ]" } else { "[replay]" }}
            </button>
        </li>
    }
}

#[component]
fn ReplayLogPane() -> impl IntoView {
    let state = dashboard();
    let count = move || state.reports.with(Vec::len);
    view! {
        <section class="pane flex min-h-[11rem] flex-col xl:min-h-0 xl:flex-[2]">
            <div class="pane-head">
                <span class="text-hi">"2 replay log"</span>
                <span class="text-dim">{move || format!("{} runs", count())}</span>
            </div>
            <Show
                when=move || { count() > 0 }
                fallback=|| view! {
                    <div class="px-3 pt-5 text-dim">
                        <div>"$ tail -f replays.log"</div>
                        <div class="mt-1">"empty. select a session and hit " <span class="key">"r"</span></div>
                    </div>
                }
            >
                <ul class="mt-3 max-h-[36vh] min-h-0 flex-1 overflow-y-auto xl:max-h-none">
                    <For
                        each=move || state.reports.with(|reports| reports.iter().take(LOG_ROWS).cloned().collect::<Vec<_>>())
                        key=|report| report.id
                        children=move |report| view! { <ReportLine report=report/> }
                    />
                </ul>
            </Show>
        </section>
    }
}

#[component]
fn ReportLine(report: ReplayReport) -> impl IntoView {
    let state = dashboard();
    let report_id = report.id;
    let session_id = report.session_id;
    let route = move || {
        state
            .routes
            .with(|routes| routes.get(&session_id).cloned())
            .unwrap_or_else(|| format!("{} (evicted)", short_hex(session_id)))
    };
    let selected = move || state.selected_report.get() == Some(report_id);
    let (delta_text, delta_class) = latency_badge(report.latency_delta_us());
    let drift = report.drift_count();
    let statuses = format!("{}>{}", report.original_status, report.replay_status);
    view! {
        <li
            class=move || if selected() { "row sel flex items-center gap-2 px-2 py-[3px]" } else { "row flex items-center gap-2 px-2 py-[3px]" }
            on:click=move |_| state.select_report(session_id, report_id)
        >
            <span class="w-[4.25rem] shrink-0 tabular-nums text-dim">{clock_of(report.executed_at_ms)}</span>
            <span class=if report.passed { "w-9 shrink-0 font-semibold text-ok" } else { "w-9 shrink-0 font-semibold text-bad" }>
                {if report.passed { "PASS" } else { "FAIL" }}
            </span>
            <span class=if report.status_matched { "w-[3.75rem] shrink-0 tabular-nums text-dim" } else { "w-[3.75rem] shrink-0 tabular-nums text-bad" }>
                {statuses}
            </span>
            <span class=format!("hidden w-[7.5rem] shrink-0 tabular-nums sm:inline {delta_class}")>{delta_text}</span>
            <span class=if drift == 0 { "w-9 shrink-0 text-rule" } else { "w-9 shrink-0 text-warn" }>{format!("Δ{drift}")}</span>
            <span class="min-w-0 flex-1 truncate">{route}</span>
        </li>
    }
}

#[component]
fn InspectorPane() -> impl IntoView {
    let state = dashboard();
    let session = create_memo(move |_| {
        let session_id = state.selected_session.get()?;
        state
            .sessions
            .with(|sessions| {
                sessions
                    .iter()
                    .find(|session| session.id == session_id)
                    .cloned()
            })
            .or_else(|| {
                state.detached.with(|detached| {
                    detached
                        .as_ref()
                        .filter(|session| session.id == session_id)
                        .cloned()
                })
            })
    });
    let report = create_memo(move |_| {
        let session_id = state.selected_session.get()?;
        let pinned = state.selected_report.get();
        state.reports.with(|reports| {
            reports
                .iter()
                .find(|report| {
                    report.session_id == session_id
                        && pinned.map_or(true, |pinned| pinned == report.id)
                })
                .cloned()
        })
    });
    let mock_count = move || {
        session.with(|session| {
            session
                .as_ref()
                .map_or(0, |session| session.outbound_calls.len())
        })
    };
    let heading = move || {
        session.with(|session| {
            session
                .as_ref()
                .map(|session| {
                    format!(
                        "{} {}",
                        session.inbound_request.method, session.inbound_request.uri
                    )
                })
                .unwrap_or_default()
        })
    };
    view! {
        <section class="pane flex min-h-[60vh] flex-1 flex-col xl:min-h-0">
            <div class="pane-head">
                <span class="flex items-center gap-2">
                    <span class="text-hi">"3 inspect"</span>
                    <TabLink tab=InspectorTab::Diff label=Signal::derive(|| "1:diff".to_owned())/>
                    <TabLink tab=InspectorTab::Mocks label=Signal::derive(move || format!("2:mocks[{}]", mock_count()))/>
                </span>
                <span class="hidden text-dim md:inline">{heading}</span>
            </div>
            <div class="min-h-0 flex-1 overflow-y-auto px-3 pb-4 pt-5">
                {move || match session.get() {
                    None => view! {
                        <Ghost
                            headline="no session selected"
                            hint="click a row or use j / k. press ? for the full keymap."
                        />
                    }
                    .into_view(),
                    Some(session) => match state.tab.get() {
                        InspectorTab::Diff => view! { <DiffView session=session report=report.get()/> }.into_view(),
                        InspectorTab::Mocks => view! { <MocksView session=session/> }.into_view(),
                    },
                }}
            </div>
        </section>
    }
}

#[component]
fn TabLink(tab: InspectorTab, label: Signal<String>) -> impl IntoView {
    let state = dashboard();
    view! {
        <button
            class=move || if state.tab.get() == tab { "bg-ok px-1 text-ink" } else { "px-1 text-dim hover:text-hi" }
            on:click=move |_| state.tab.set(tab)
        >
            {move || label.get()}
        </button>
    }
}

#[component]
fn DiffView(session: TrafficSession, report: Option<ReplayReport>) -> impl IntoView {
    let state = dashboard();
    let session_id = session.id;
    let verdict = match report {
        Some(report) => view! { <Verdict report=report/> }.into_view(),
        None => view! {
            <Divider label="replay" meta=String::new()/>
            <div class="border border-dashed border-rule px-3 py-2">
                <span class="text-dim">"never replayed. "</span>
                <button class="btn px-1 text-ok" on:click=move |_| state.replay(session_id)>"[r] fire it at the target"</button>
            </div>
        }
        .into_view(),
    };
    view! {
        <div class="flex flex-col">
            <SessionFacts session=session.clone()/>
            {verdict}
            <Divider label="recorded payloads" meta=String::new()/>
            <div class="grid grid-cols-1 gap-3 md:grid-cols-2">
                <CodeView title="inbound.request.body" message=session.inbound_request/>
                <CodeView title="inbound.response.body" message=session.inbound_response/>
            </div>
        </div>
    }
}

#[component]
fn SessionFacts(session: TrafficSession) -> impl IntoView {
    let status = session.status();
    let full_id = session.id.to_string();
    view! {
        <dl class="grid grid-cols-[5.5rem_minmax(0,1fr)] gap-x-3 gap-y-0.5 sm:grid-cols-[5.5rem_minmax(0,1fr)_5rem_minmax(0,1fr)]">
            <dt class="text-dim">"session"</dt>
            <dd class="truncate text-hi" title=full_id.clone()>{full_id}</dd>
            <dt class="text-dim">"trace"</dt>
            <dd class="truncate text-fg" title=session.trace_id.clone()>{session.trace_id.clone()}</dd>
            <dt class="text-dim">"request"</dt>
            <dd class="truncate">
                <span class=method_color(&session.inbound_request.method)>{session.inbound_request.method.clone()}</span>
                " "
                <span class="text-hi">{session.inbound_request.uri.clone()}</span>
            </dd>
            <dt class="text-dim">"status"</dt>
            <dd class=status_color(status)>{status.to_string()}</dd>
            <dt class="text-dim">"captured"</dt>
            <dd class="text-fg">{stamp_of(session.created_at_ms)}</dd>
            <dt class="text-dim">"took"</dt>
            <dd class="text-fg">{format!("{:.2}ms with {} outbound", session.total_duration_us as f64 / 1_000.0, session.outbound_calls.len())}</dd>
        </dl>
    }
}

#[component]
fn Verdict(report: ReplayReport) -> impl IntoView {
    let (delta_text, delta_class) = latency_badge(report.latency_delta_us());
    let drift = report.drift_count();
    let body_count = report.body_diffs.len();
    let outbound_count = report.outbound_diffs.len();
    let verdict_badge = if report.passed {
        view! { <span class="bg-ok px-1.5 font-bold text-ink">"PASS"</span> }
    } else {
        view! { <span class="bg-bad px-1.5 font-bold text-ink">"FAIL"</span> }
    };
    let drift_note = match drift {
        0 => "no drift".to_owned(),
        1 => "1 drift entry".to_owned(),
        count => format!("{count} drift entries"),
    };
    view! {
        <Divider label="replay verdict" meta=format!("run {}", stamp_of(report.executed_at_ms))/>
        <div class="grid grid-cols-[5.5rem_minmax(0,1fr)] gap-x-3 gap-y-0.5">
            <span class="text-dim">"verdict"</span>
            <span>{verdict_badge}<span class="ml-2 text-dim">{drift_note}</span></span>
            <span class="text-dim">"status"</span>
            <span>
                <span class=status_color(report.original_status)>{report.original_status.to_string()}</span>
                <span class="text-dim">" -> "</span>
                <span class=status_color(report.replay_status)>{report.replay_status.to_string()}</span>
                <span class=if report.status_matched { "ml-2 text-ok" } else { "ml-2 text-bad" }>
                    {if report.status_matched { "same" } else { "changed" }}
                </span>
            </span>
            <span class="text-dim">"latency"</span>
            <span>
                <span class="tabular-nums text-fg">
                    {format!("{:.2}ms -> {:.2}ms", report.original_latency_us as f64 / 1_000.0, report.replay_latency_us as f64 / 1_000.0)}
                </span>
                <span class=format!("ml-2 tabular-nums {delta_class}")>{delta_text}</span>
            </span>
            <span class="text-dim">"sandbox"</span>
            <span class="text-info">{format!("{} calls answered from the recording, nothing left the box", report.mocked_outbound_count)}</span>
        </div>
        <Divider label="response body drift" meta=body_count.to_string()/>
        <DiffTable entries=report.body_diffs empty="identical to the recording, structurally"/>
        <Divider label="outbound call drift" meta=outbound_count.to_string()/>
        <DiffTable entries=report.outbound_diffs empty="same calls, same order, each one made exactly once"/>
    }
}

#[component]
fn Divider(label: &'static str, meta: String) -> impl IntoView {
    view! {
        <div class="mb-2 mt-5 flex items-center gap-2 text-[11px]">
            <span class="text-rule">"──"</span>
            <span class="text-hi">{label}</span>
            <span class="h-px flex-1 bg-rule"></span>
            <span class="text-dim">{meta}</span>
            <span class="text-rule">"──"</span>
        </div>
    }
}

#[component]
fn DiffTable(entries: Vec<JsonDiffEntry>, empty: &'static str) -> impl IntoView {
    if entries.is_empty() {
        return view! {
            <div>
                <span class="text-ok">"ok "</span>
                <span class="text-dim">{empty}</span>
            </div>
        }
        .into_view();
    }
    view! {
        <div class="overflow-x-auto">
            <table class="w-full min-w-[640px] table-fixed border-collapse text-left">
                <thead>
                    <tr class="text-[11px] text-dim">
                        <th class="w-[30%] border-b border-rule py-1 pr-3 font-normal">"path"</th>
                        <th class="w-[15%] border-b border-rule py-1 pr-3 font-normal">"kind"</th>
                        <th class="border-b border-rule py-1 pr-3 font-normal">"expected"</th>
                        <th class="border-b border-rule py-1 font-normal">"actual"</th>
                    </tr>
                </thead>
                <tbody>
                    {entries.into_iter().map(|entry| view! { <DiffRow entry=entry/> }).collect_view()}
                </tbody>
            </table>
        </div>
    }
    .into_view()
}

#[component]
fn DiffRow(entry: JsonDiffEntry) -> impl IntoView {
    let (sigil, color) = kind_style(entry.kind);
    view! {
        <tr class="align-top hover:bg-ink3">
            <td class="break-all border-b border-rule/50 py-1 pr-3">
                <span class=color>{sigil}</span>
                " "
                <span class="text-info">{entry.path.clone()}</span>
            </td>
            <td class=format!("border-b border-rule/50 py-1 pr-3 {color}")>{entry.kind.label().to_ascii_lowercase()}</td>
            <td class="border-b border-rule/50 py-1 pr-3">
                <pre class="max-h-48 overflow-auto whitespace-pre-wrap break-all font-mono text-bad/90">{render_json(entry.shown_expected().as_ref())}</pre>
            </td>
            <td class="border-b border-rule/50 py-1">
                <pre class="max-h-48 overflow-auto whitespace-pre-wrap break-all font-mono text-ok/90">{render_json(entry.shown_actual().as_ref())}</pre>
            </td>
        </tr>
    }
}

#[component]
fn CodeView(title: &'static str, message: HttpMessage) -> impl IntoView {
    let show_headers = create_rw_signal(false);
    let is_json = message.json_body().is_some();
    let rendered = message.render_body();
    let bytes = message.body.len();
    let headers = message.headers;
    let header_count = headers.len();
    let total_lines = rendered.lines().count();
    let kind = if bytes == 0 {
        "empty"
    } else if is_json {
        "json"
    } else {
        "raw"
    };
    let body = if bytes == 0 {
        view! { <div class="px-2 text-dim">"(empty body)"</div> }.into_view()
    } else {
        rendered
            .lines()
            .take(CODE_LINE_LIMIT)
            .enumerate()
            .map(|(index, line)| {
                view! {
                    <div class="flex">
                        <span class="w-9 shrink-0 select-none pr-2 text-right text-rule">{index + 1}</span>
                        <span class="min-w-0 flex-1 whitespace-pre-wrap break-all">
                            {if is_json { highlight(line) } else { view! { <span class="text-fg">{line.to_owned()}</span> }.into_view() }}
                        </span>
                    </div>
                }
            })
            .collect_view()
    };
    view! {
        <div class="flex min-w-0 flex-col border border-rule">
            <div class="flex items-center justify-between gap-2 border-b border-rule bg-ink2 px-2 py-0.5 text-[11px]">
                <span class="truncate text-hi">{title}</span>
                <span class="shrink-0 text-dim">{format!("{bytes} B  {kind}")}</span>
            </div>
            <div class="max-h-80 min-h-[5rem] overflow-auto py-1 text-[12px] leading-[1.5]">
                {body}
                {(total_lines > CODE_LINE_LIMIT).then(|| view! {
                    <div class="px-2 text-dim">{format!("-- {} more lines --", total_lines - CODE_LINE_LIMIT)}</div>
                })}
            </div>
            <button
                class="btn border-t border-rule px-2 py-0.5 text-left text-[11px] text-dim"
                on:click=move |_| show_headers.update(|open| *open = !*open)
            >
                {move || format!("{} headers ({header_count})", if show_headers.get() { "[-]" } else { "[+]" })}
            </button>
            <Show when=move || show_headers.get()>
                <div class="max-h-48 overflow-auto border-t border-rule px-2 py-1 text-[11px]">
                    {headers
                        .iter()
                        .map(|(name, value)| view! {
                            <div class="break-all">
                                <span class="text-info">{name.clone()}</span>
                                <span class="text-dim">": "</span>
                                <span class="text-fg">{value.clone()}</span>
                            </div>
                        })
                        .collect_view()}
                </div>
            </Show>
        </div>
    }
}

#[component]
fn MocksView(session: TrafficSession) -> impl IntoView {
    if session.outbound_calls.is_empty() {
        return view! {
            <Ghost
                headline="no outbound calls on this session"
                hint="route SDK traffic through :8081 and forward x-phantom-trace-id from the inbound request."
            />
        }
        .into_view();
    }
    let count = session.outbound_calls.len();
    view! {
        <div class="flex flex-col">
            <div class="mb-3 text-dim">
                {format!("{count} exchanges. on replay they are matched by sha256 signature, then method+path, then repeat.")}
            </div>
            {session
                .outbound_calls
                .into_iter()
                .enumerate()
                .map(|(index, call)| view! { <MockEntry index=index call=call/> })
                .collect_view()}
        </div>
    }
    .into_view()
}

#[component]
fn MockEntry(index: usize, call: OutboundExchange) -> impl IntoView {
    let expanded = create_rw_signal(false);
    let method = call.request.method.clone();
    let verb_class = format!("w-12 shrink-0 font-semibold {}", method_color(&method));
    let host = call.target_host.clone();
    let path = call.request.path().to_owned();
    let status = call.response.status.unwrap_or_default();
    let latency = format!("{:.1}ms", call.latency_us as f64 / 1_000.0);
    let signature = call.signature_hash.clone();
    let short_signature: String = signature.chars().take(12).collect();
    let request = call.request;
    let response = call.response;
    view! {
        <div class="border-b border-rule/60">
            <button class="row flex w-full flex-wrap items-center gap-x-2 px-1 py-1 text-left" on:click=move |_| expanded.update(|open| *open = !*open)>
                <span class="text-dim">{move || if expanded.get() { "[-]" } else { "[+]" }}</span>
                <span class="w-6 shrink-0 text-rule">{format!("{index:02}")}</span>
                <span class=verb_class>{method}</span>
                <span class="shrink-0 text-info">{host}</span>
                <span class="min-w-0 flex-1 truncate text-hi">{path}</span>
                <span class=format!("tabular-nums {}", status_color(status))>{status.to_string()}</span>
                <span class="w-14 text-right tabular-nums text-dim">{latency}</span>
                <span class="text-odd" title=signature>{format!("{short_signature}…")}</span>
            </button>
            <Show when=move || expanded.get()>
                <div class="grid grid-cols-1 gap-3 py-2 md:grid-cols-2">
                    <CodeView title="outbound.request" message=request.clone()/>
                    <CodeView title="recorded.response" message=response.clone()/>
                </div>
            </Show>
        </div>
    }
}

#[component]
fn CommandDock() -> impl IntoView {
    let state = dashboard();
    let draft = create_rw_signal(String::new());
    let history = store_value(Vec::<String>::new());
    let cursor = store_value(0usize);
    let submit = move || {
        let line = draft.get_untracked();
        if !line.trim().is_empty() {
            history.update_value(|entries| entries.push(line.trim().to_owned()));
            cursor.set_value(history.with_value(Vec::len));
            state.execute(&line);
        }
        draft.set(String::new());
    };
    let recall = move |step: isize| {
        let total = history.with_value(Vec::len);
        if total == 0 {
            return;
        }
        let next = cursor.get_value().saturating_add_signed(step).min(total);
        cursor.set_value(next);
        draft.set(history.with_value(|entries| entries.get(next).cloned().unwrap_or_default()));
    };
    let command_ref = state.command_input;
    view! {
        <footer class="border-t border-rule bg-ink2 text-[12px] xl:shrink-0">
            <div class="truncate px-3 pt-1">
                {move || match state.notice.get() {
                    Some(notice) => view! {
                        <span class="text-dim">{format!("[{}] ", notice.at)}</span>
                        <span class=tone_color(notice.tone)>{notice.text}</span>
                    }
                    .into_view(),
                    None => view! {
                        <span class="text-dim">
                            {move || state.mode.with(|mode| format!("-- {} --", mode.as_ref().map_or("CONNECTING", OperatingMode::label)))}
                        </span>
                    }
                    .into_view(),
                }}
            </div>
            <div class="flex items-center gap-2 px-3 pb-1.5">
                <span class="text-ok">"phantom>"</span>
                <input
                    node_ref=command_ref
                    type="text"
                    spellcheck="false"
                    autocomplete="off"
                    placeholder="help"
                    class="min-w-0 flex-1 bg-transparent py-0.5 text-hi outline-none"
                    prop:value=move || draft.get()
                    on:input=move |event| draft.set(event_target_value(&event))
                    on:keydown=move |event: web_sys::KeyboardEvent| match event.key().as_str() {
                        "Enter" => {
                            event.prevent_default();
                            submit();
                        }
                        "ArrowUp" => {
                            event.prevent_default();
                            recall(-1);
                        }
                        "ArrowDown" => {
                            event.prevent_default();
                            recall(1);
                        }
                        _ => {}
                    }
                />
                <span class="hidden items-center gap-1 text-dim lg:flex">
                    <span class="key">"j"</span><span class="key">"k"</span>" move "
                    <span class="key">"r"</span>" replay "
                    <span class="key">"R"</span>" all "
                    <span class="key">"m"</span>" mode "
                    <span class="key">":"</span>" cmd "
                    <span class="key">"?"</span>" keys"
                </span>
            </div>
        </footer>
    }
}

#[component]
fn HelpScreen() -> impl IntoView {
    let state = dashboard();
    view! {
        <Show when=move || state.help_open.get()>
            <div class="fixed inset-0 z-50 flex items-center justify-center bg-ink/85 p-4" on:click=move |_| state.help_open.set(false)>
                <div class="pane w-full max-w-xl px-4 pb-4 pt-5" on:click=|event| event.stop_propagation()>
                    <div class="pane-head">
                        <span class="text-hi">"PHANTOM-CONSOLE(1)"</span>
                        <span class="text-dim">"esc closes"</span>
                    </div>
                    <div class="text-[11px] text-dim">"KEYS"</div>
                    <div class="mt-1 grid grid-cols-[8.5rem_minmax(0,1fr)] gap-y-0.5">
                        {KEYMAP
                            .iter()
                            .map(|(keys, action)| view! {
                                <span class="text-ok">{*keys}</span>
                                <span class="text-fg">{*action}</span>
                            })
                            .collect_view()}
                    </div>
                    <div class="mt-4 text-[11px] text-dim">"COMMANDS  (press : first)"</div>
                    <div class="mt-1 grid grid-cols-[10.5rem_minmax(0,1fr)] gap-y-0.5">
                        {COMMANDS
                            .iter()
                            .map(|(command, meaning)| view! {
                                <span class="text-warn">{*command}</span>
                                <span class="text-fg">{*meaning}</span>
                            })
                            .collect_view()}
                    </div>
                </div>
            </div>
        </Show>
    }
}

#[component]
fn Ghost(headline: &'static str, hint: &'static str) -> impl IntoView {
    view! {
        <div class="flex flex-1 items-center justify-center gap-6 px-4 py-10">
            <pre class="glow font-mono leading-tight text-ok">{GHOST}</pre>
            <div class="max-w-xs">
                <div class="text-hi">{headline}</div>
                <div class="mt-1 text-dim">{hint}</div>
            </div>
        </div>
    }
}

fn reveal_selected(state: DashboardState) {
    let Some(session_id) = state.selected_session.get_untracked() else {
        return;
    };
    let anchor = format!("session-{}", session_id.simple());
    if let Some(element) = document().get_element_by_id(&anchor) {
        let options = ScrollIntoViewOptions::new();
        options.set_block(ScrollLogicalPosition::Nearest);
        element.scroll_into_view_with_scroll_into_view_options(&options);
    }
}

fn odometer(count: u64) -> View {
    let digits = format!("{count:06}");
    let split = digits
        .find(|digit| digit != '0')
        .unwrap_or(digits.len() - 1);
    let (lead, value) = digits.split_at(split);
    let (lead, value) = (lead.to_owned(), value.to_owned());
    view! {
        <span class="text-rule">{lead}</span>
        <span class="glow text-ok">{value}</span>
    }
    .into_view()
}

fn gauge(stats: ReplayStats) -> View {
    if stats.evaluated == 0 {
        return view! { <span class="text-rule">{format!("[{}] no runs yet", "·".repeat(GAUGE_CELLS))}</span> }.into_view();
    }
    let mut filled = stats.passed * GAUGE_CELLS / stats.evaluated;
    if stats.passed < stats.evaluated {
        filled = filled.min(GAUGE_CELLS - 1);
    }
    view! {
        <span class="text-dim">"["</span>
        <span class="text-ok">{"|".repeat(filled)}</span>
        <span class="text-bad">{"|".repeat(GAUGE_CELLS - filled)}</span>
        <span class="text-dim">"]"</span>
    }
    .into_view()
}

fn sparkline(deltas: Vec<i64>) -> View {
    if deltas.is_empty() {
        return view! { <span class="text-rule">"no runs yet"</span> }.into_view();
    }
    let peak = deltas
        .iter()
        .map(|delta| delta.unsigned_abs())
        .max()
        .unwrap_or(1)
        .max(1);
    let bars = deltas
        .iter()
        .rev()
        .map(|delta| {
            let level = ((delta.unsigned_abs() as f64 / peak as f64) * 7.0).round() as usize;
            let color = match delta {
                delta if *delta < 0 => "text-ok",
                delta if *delta > 0 => "text-bad",
                _ => "text-dim",
            };
            view! { <span class=color>{SPARK_LEVELS[level.min(7)].to_string()}</span> }
        })
        .collect_view();
    view! {
        {bars}
        <span class="ml-2 text-dim">{format!("last {} runs", deltas.len())}</span>
    }
    .into_view()
}

fn highlight(line: &str) -> View {
    let trimmed = line.trim_start();
    let indent = line[..line.len() - trimmed.len()].to_owned();
    let (key, value) = match split_json_key(trimmed) {
        Some((key, value)) => (Some(key.to_owned()), value),
        None => (None, trimmed),
    };
    let (core, tail) = match value.strip_suffix(',') {
        Some(core) => (core, ","),
        None => (value, ""),
    };
    view! {
        <span>{indent}</span>
        {key.map(|key| view! {
            <span class="text-info">{key}</span>
            <span class="text-dim">": "</span>
        })}
        <span class=value_color(core)>{core.to_owned()}</span>
        <span class="text-dim">{tail}</span>
    }
    .into_view()
}

fn split_json_key(trimmed: &str) -> Option<(&str, &str)> {
    if !trimmed.starts_with('"') {
        return None;
    }
    let mut escaped = false;
    for (index, character) in trimmed.char_indices().skip(1) {
        match character {
            '\\' if !escaped => escaped = true,
            '"' if !escaped => {
                return trimmed[index + 1..]
                    .strip_prefix(": ")
                    .map(|value| (&trimmed[..=index], value));
            }
            _ => escaped = false,
        }
    }
    None
}

fn value_color(token: &str) -> &'static str {
    match token.chars().next() {
        Some('"') => "text-ok",
        Some('-' | '0'..='9') => "text-warn",
        Some('t' | 'f') => "text-odd",
        Some('n') => "text-dim",
        _ => "text-fg",
    }
}

fn mode_class(active: bool, accent: &str) -> String {
    if active {
        format!("px-2 py-0.5 font-semibold text-ink {accent}")
    } else {
        "px-2 py-0.5 text-dim hover:text-hi".to_owned()
    }
}

fn method_color(method: &str) -> &'static str {
    match method.to_ascii_uppercase().as_str() {
        "GET" => "text-ok",
        "POST" => "text-info",
        "PUT" => "text-warn",
        "PATCH" => "text-odd",
        "DELETE" => "text-bad",
        _ => "text-fg",
    }
}

fn status_color(status: u16) -> &'static str {
    match status {
        200..=299 => "text-ok",
        300..=399 => "text-info",
        400..=499 => "text-warn",
        500..=599 => "text-bad",
        _ => "text-dim",
    }
}

fn verdict_mark(verdict: Option<bool>) -> View {
    match verdict {
        Some(true) => view! { <span class="text-ok">"pass"</span> }.into_view(),
        Some(false) => view! { <span class="text-bad">"FAIL"</span> }.into_view(),
        None => view! { <span class="text-rule">"----"</span> }.into_view(),
    }
}

fn kind_style(kind: DiffKind) -> (&'static str, &'static str) {
    match kind {
        DiffKind::Match => ("=", "text-dim"),
        DiffKind::Added => ("+", "text-ok"),
        DiffKind::Removed => ("-", "text-bad"),
        DiffKind::Modified => ("~", "text-warn"),
        DiffKind::TypeMismatch => ("!", "text-odd"),
    }
}

fn tone_color(tone: Tone) -> &'static str {
    match tone {
        Tone::Success => "text-ok",
        Tone::Failure => "text-bad",
        Tone::Info => "text-info",
    }
}

fn latency_badge(delta_us: i64) -> (String, &'static str) {
    let delta_ms = delta_us as f64 / 1_000.0;
    if delta_us < 0 {
        (format!("{delta_ms:.1}ms faster"), "text-ok")
    } else if delta_us > 0 {
        (format!("+{delta_ms:.1}ms slower"), "text-bad")
    } else {
        ("±0.0ms".to_owned(), "text-dim")
    }
}

fn clock_of(epoch_ms: i64) -> String {
    DateTime::from_timestamp_millis(epoch_ms)
        .map(|moment| moment.format("%H:%M:%S").to_string())
        .unwrap_or_default()
}

fn stamp_of(epoch_ms: i64) -> String {
    DateTime::from_timestamp_millis(epoch_ms)
        .map(|moment| moment.format("%Y-%m-%d %H:%M:%S%.3fZ").to_string())
        .unwrap_or_default()
}

fn render_json(value: Option<&Value>) -> String {
    let Some(value) = value else {
        return "-".to_owned();
    };
    let rendered = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    if rendered.chars().count() <= JSON_PREVIEW_LIMIT {
        return rendered;
    }
    let mut truncated: String = rendered.chars().take(JSON_PREVIEW_LIMIT).collect();
    truncated.push_str("\n…");
    truncated
}
