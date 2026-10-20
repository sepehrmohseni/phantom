mod embedded;
mod server;

use std::io::IsTerminal;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use axum::http::HeaderValue;
use clap::{Args, Parser, Subcommand};
use phantom_core::models::ReplayRequest;
use phantom_core::{
    DiffKind, IgnoreRules, JsonDiffEntry, PhantomStore, ReplayReport, TrafficSession,
};
use phantom_engine::{inbound, outbound, EngineConfig, EngineState};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use crate::server::ControlPlaneOptions;

const FLUSH_TIMEOUT: Duration = Duration::from_secs(10);
const SEARCH_WINDOW: usize = 5_000;

#[derive(Debug, Parser)]
#[command(
    name = "phantom",
    version,
    about = "Record real traffic, replay it against another build, answer third-party calls from the recording.",
    propagate_version = true
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Start the inbound capture proxy, outbound sandbox proxy and control plane")]
    Start(StartArgs),
    #[command(
        about = "Replay captured sessions through a running phantom and exit non-zero on drift"
    )]
    Replay(ReplayArgs),
}

#[derive(Debug, Args)]
struct ReplayArgs {
    #[arg(
        long,
        env = "PHANTOM_API",
        default_value = "http://127.0.0.1:9000",
        help = "Control plane of the running phantom"
    )]
    api: String,

    #[arg(
        long,
        env = "PHANTOM_TARGET",
        help = "Build to fire requests at; defaults to the url of the current mode"
    )]
    target: Option<String>,

    #[arg(
        long = "session",
        value_name = "ID",
        help = "Session uuid, or any prefix or suffix of it; repeatable. Omit to replay everything"
    )]
    sessions: Vec<String>,

    #[arg(
        long,
        default_value_t = 500,
        help = "Most sessions considered when replaying everything"
    )]
    limit: usize,

    #[arg(long, help = "Print one JSON report per line instead of text")]
    json: bool,
}

#[derive(Debug, Args)]
struct StartArgs {
    #[arg(
        long,
        env = "PHANTOM_UPSTREAM",
        default_value = "http://localhost:3000",
        help = "Service that inbound traffic is forwarded to while recording"
    )]
    upstream: String,

    #[arg(
        long,
        env = "PHANTOM_INBOUND_PORT",
        default_value_t = 8080,
        help = "Port of the inbound capture proxy"
    )]
    inbound_port: u16,

    #[arg(
        long,
        env = "PHANTOM_OUTBOUND_PORT",
        default_value_t = 8081,
        help = "Port of the outbound forward and sandbox proxy"
    )]
    outbound_port: u16,

    #[arg(
        long,
        env = "PHANTOM_UI_PORT",
        default_value_t = 9000,
        help = "Port of the control plane API and web console"
    )]
    ui_port: u16,

    #[arg(
        long,
        env = "PHANTOM_DB_PATH",
        default_value = "./phantom.redb",
        help = "Path of the redb session store"
    )]
    db_path: PathBuf,

    #[arg(long, env = "PHANTOM_BIND", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST), help = "Interface the inbound and outbound proxies bind to; use 0.0.0.0 to accept traffic from other hosts")]
    bind: IpAddr,

    #[arg(long, env = "PHANTOM_UI_BIND", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST), help = "Interface the control plane binds to")]
    ui_bind: IpAddr,

    #[arg(
        long = "ignore-field",
        env = "PHANTOM_IGNORE_FIELDS",
        value_delimiter = ',',
        help = "JSONPath excluded from replay diffs, e.g. $.created_at or $.items[*].id"
    )]
    ignore_fields: Vec<String>,

    #[arg(long, env = "PHANTOM_MAX_BODY_BYTES", default_value_t = 16 * 1024 * 1024, help = "Largest request body the proxies will buffer")]
    max_body_bytes: usize,

    #[arg(
        long,
        env = "PHANTOM_UPSTREAM_TIMEOUT_SECS",
        default_value_t = 30,
        help = "Timeout applied to every forwarded request"
    )]
    upstream_timeout_secs: u64,

    #[arg(
        long,
        env = "PHANTOM_CAPTURE_QUEUE",
        default_value_t = 16_384,
        help = "Captured sessions buffered for the writer before new captures are shed"
    )]
    capture_queue: usize,

    #[arg(
        long,
        env = "PHANTOM_UI_DIR",
        help = "Serve the console from this directory instead of the embedded bundle"
    )]
    ui_dir: Option<PathBuf>,

    #[arg(
        long = "cors-origin",
        env = "PHANTOM_CORS_ORIGINS",
        value_delimiter = ',',
        help = "Origin allowed to call the control plane cross-origin"
    )]
    cors_origins: Vec<String>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let outcome = match cli.command {
        Command::Start(args) => {
            init_tracing();
            start(args).await.map(|()| ExitCode::SUCCESS)
        }
        Command::Replay(args) => replay(args).await,
    };
    outcome.unwrap_or_else(|err| {
        eprintln!("phantom: {err:#}");
        ExitCode::from(2)
    })
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

async fn start(args: StartArgs) -> Result<()> {
    let mut config = EngineConfig::new(&args.upstream).context("invalid --upstream url")?;
    config.max_body_bytes = args.max_body_bytes;
    config.upstream_timeout = Duration::from_secs(args.upstream_timeout_secs.max(1));
    config.capture_queue = args.capture_queue.max(1);
    config.ignore_rules =
        IgnoreRules::parse(&args.ignore_fields).context("invalid --ignore-field pattern")?;

    let cors_origins = args
        .cors_origins
        .iter()
        .map(|origin| {
            HeaderValue::from_str(origin.trim())
                .with_context(|| format!("invalid --cors-origin `{origin}`"))
        })
        .collect::<Result<Vec<_>>>()?;
    if let Some(directory) = &args.ui_dir {
        if !directory.join("index.html").is_file() {
            bail!(
                "--ui-dir {} does not contain an index.html",
                directory.display()
            );
        }
    }

    let store = open_store(args.db_path.clone()).await?;
    let (engine, recorder) =
        EngineState::new(store, config).context("failed to initialise the engine")?;

    let inbound_listener = bind(
        SocketAddr::new(args.bind, args.inbound_port),
        "inbound capture proxy",
    )
    .await?;
    let outbound_listener = bind(
        SocketAddr::new(args.bind, args.outbound_port),
        "outbound sandbox proxy",
    )
    .await?;
    let ui_listener = bind(SocketAddr::new(args.ui_bind, args.ui_port), "control plane").await?;

    tracing::info!(version = env!("CARGO_PKG_VERSION"), "phantom engine online");
    tracing::info!(address = %inbound_listener.local_addr()?, upstream = %engine.config.upstream_url, "inbound capture proxy listening");
    tracing::info!(address = %outbound_listener.local_addr()?, "outbound sandbox proxy listening");
    tracing::info!(address = %ui_listener.local_addr()?, embedded_console = embedded::bundle_present(), "control plane listening");
    tracing::info!(path = %args.db_path.display(), "session store opened");
    if !engine.config.ignore_rules.is_empty() {
        tracing::info!(paths = ?engine.config.ignore_rules.sources(), "diff ignore rules active");
    }

    let options = ControlPlaneOptions {
        ui_dir: args.ui_dir,
        cors_origins,
        loopback_only: args.ui_bind.is_loopback(),
    };
    let signal_engine = Arc::clone(&engine);
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("shutdown requested, draining connections");
        signal_engine.begin_shutdown();
    });
    let stop_rx = engine.shutdown_signal();

    tokio::try_join!(
        phantom_engine::serve(
            inbound_listener,
            inbound::router(Arc::clone(&engine)),
            stopped(stop_rx.clone())
        ),
        phantom_engine::serve(
            outbound_listener,
            outbound::router(Arc::clone(&engine)),
            stopped(stop_rx.clone())
        ),
        phantom_engine::serve(
            ui_listener,
            server::router(Arc::clone(&engine), options),
            stopped(stop_rx)
        ),
    )
    .context("listener failure")?;

    engine.begin_shutdown();
    drop(engine);
    if tokio::time::timeout(FLUSH_TIMEOUT, recorder.flush())
        .await
        .is_err()
    {
        tracing::warn!("capture queue did not drain in time; some sessions may be lost");
    }
    tracing::info!("phantom stopped");
    Ok(())
}

async fn open_store(path: PathBuf) -> Result<Arc<PhantomStore>> {
    tokio::task::spawn_blocking(move || {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        PhantomStore::open(&path)
            .with_context(|| format!("failed to open session store at {}", path.display()))
    })
    .await
    .context("store initialisation task aborted")?
}

async fn bind(address: SocketAddr, role: &str) -> Result<TcpListener> {
    TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind the {role} on {address}"))
}

async fn stopped(mut signal: watch::Receiver<bool>) {
    let _ = signal.wait_for(|stopping| *stopping).await;
}

async fn shutdown_signal() {
    let interrupt = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %err, "failed to install the ctrl-c handler");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(err) => {
                tracing::error!(error = %err, "failed to install the sigterm handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
}

#[derive(Debug, Deserialize)]
struct ApiFailure {
    message: String,
}

struct Palette {
    enabled: bool,
}

impl Palette {
    fn detect(json: bool) -> Self {
        Self {
            enabled: !json
                && std::io::stdout().is_terminal()
                && std::env::var_os("NO_COLOR").is_none(),
        }
    }

    fn paint(&self, code: &str, text: &str) -> String {
        if self.enabled {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_owned()
        }
    }
}

async fn replay(args: ReplayArgs) -> Result<ExitCode> {
    let api = args.api.trim_end_matches('/').to_owned();
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(300))
        .build()
        .context("failed to build the http client")?;
    let palette = Palette::detect(args.json);

    let selected = if args.sessions.is_empty() {
        let mut sessions: Vec<TrafficSession> = fetch(
            &client,
            &format!(
                "{api}/api/sessions?limit={}",
                args.limit.clamp(1, SEARCH_WINDOW)
            ),
        )
        .await?;
        sessions.reverse();
        sessions
    } else {
        let mut window: Option<Vec<TrafficSession>> = None;
        let mut chosen = Vec::with_capacity(args.sessions.len());
        for token in &args.sessions {
            if let Ok(id) = Uuid::parse_str(token.trim()) {
                chosen.push(
                    fetch::<TrafficSession>(&client, &format!("{api}/api/sessions/{id}")).await?,
                );
                continue;
            }
            if window.is_none() {
                window = Some(
                    fetch(
                        &client,
                        &format!("{api}/api/sessions?limit={SEARCH_WINDOW}"),
                    )
                    .await?,
                );
            }
            chosen.push(resolve_session(
                window.as_deref().unwrap_or_default(),
                token,
            )?);
        }
        chosen
    };
    if selected.is_empty() {
        bail!("nothing to replay: the store at {api} has no captured sessions");
    }

    let target_label = args
        .target
        .clone()
        .unwrap_or_else(|| "the current mode url".to_owned());
    if !args.json {
        println!(
            "{} replaying {} session(s) against {target_label}\n",
            palette.paint("2", "->"),
            selected.len()
        );
    }

    let (mut passed, mut failed, mut errored) = (0usize, 0usize, 0usize);
    for session in &selected {
        let outcome = client
            .post(format!("{api}/api/replay/{}", session.id))
            .json(&ReplayRequest {
                target_url: args.target.clone(),
            })
            .send()
            .await
            .map_err(anyhow::Error::from);
        let report = match outcome {
            Ok(response) => decode::<ReplayReport>(response).await,
            Err(err) => Err(err),
        };
        match report {
            Ok(report) => {
                if report.passed {
                    passed += 1;
                } else {
                    failed += 1;
                }
                if args.json {
                    println!("{}", serde_json::to_string(&report)?);
                } else {
                    print_report(&palette, session, &report);
                }
            }
            Err(err) => {
                errored += 1;
                if args.json {
                    println!(
                        "{}",
                        serde_json::json!({ "session_id": session.id, "error": format!("{err:#}") })
                    );
                } else {
                    println!(
                        "{}  {}  {}",
                        palette.paint("33;1", "ERR "),
                        short_id(session),
                        route(session)
                    );
                    println!("      {}", palette.paint("33", &format!("{err:#}")));
                }
            }
        }
    }

    if !args.json {
        let summary = format!("{passed} passed, {failed} failed, {errored} errors");
        let tone = if failed + errored == 0 {
            "32;1"
        } else {
            "31;1"
        };
        println!("\n{}", palette.paint(tone, &summary));
    }
    Ok(match (failed, errored) {
        (_, errors) if errors > 0 => ExitCode::from(2),
        (0, _) => ExitCode::SUCCESS,
        _ => ExitCode::from(1),
    })
}

fn print_report(palette: &Palette, session: &TrafficSession, report: &ReplayReport) {
    let verdict = if report.passed {
        palette.paint("32;1", "PASS")
    } else {
        palette.paint("31;1", "FAIL")
    };
    let statuses = format!("{}>{}", report.original_status, report.replay_status);
    let statuses = if report.status_matched {
        statuses
    } else {
        palette.paint("31", &statuses)
    };
    println!(
        "{verdict}  {}  {:<32} {statuses:>7}  {:>8.2}ms  {} mocks",
        short_id(session),
        route(session),
        report.replay_latency_us as f64 / 1_000.0,
        report.mocked_outbound_count
    );
    for entry in report.body_diffs.iter().chain(&report.outbound_diffs) {
        let (sigil, code) = match entry.kind {
            DiffKind::Added => ("+", "32"),
            DiffKind::Removed => ("-", "31"),
            DiffKind::Modified => ("~", "33"),
            DiffKind::TypeMismatch => ("!", "35"),
            DiffKind::Match => ("=", "2"),
        };
        println!(
            "      {}  {:<26} {:<14} {}",
            palette.paint(code, sigil),
            palette.paint("36", &entry.path),
            entry.kind.label().to_ascii_lowercase(),
            describe_change(entry)
        );
    }
}

fn describe_change(entry: &JsonDiffEntry) -> String {
    if let Some(call) = entry
        .shown_actual()
        .as_ref()
        .or(entry.shown_expected().as_ref())
        .and_then(describe_call)
    {
        return call;
    }
    let render = |value: Option<&serde_json::Value>| {
        let text = value.map_or_else(|| "-".to_owned(), serde_json::Value::to_string);
        if text.chars().count() > 64 {
            format!("{}...", text.chars().take(61).collect::<String>())
        } else {
            text
        }
    };
    format!(
        "{} -> {}",
        render(entry.shown_expected().as_ref()),
        render(entry.shown_actual().as_ref())
    )
}

fn describe_call(value: &serde_json::Value) -> Option<String> {
    let method = value.get("method")?.as_str()?;
    let url = value.get("url")?.as_str()?;
    let reason = value.get("reason")?.as_str()?.replace('_', " ");
    Some(
        match value
            .get("duplicate_of")
            .and_then(serde_json::Value::as_u64)
        {
            Some(original) => format!("{reason} of #{original}: {method} {url}"),
            None => format!("{reason}: {method} {url}"),
        },
    )
}

fn resolve_session(sessions: &[TrafficSession], token: &str) -> Result<TrafficSession> {
    let needle = token.trim().to_ascii_lowercase().replace('-', "");
    let matches: Vec<&TrafficSession> = sessions
        .iter()
        .filter(|session| {
            let simple = session.id.simple().to_string();
            !needle.is_empty() && (simple.starts_with(&needle) || simple.ends_with(&needle))
        })
        .collect();
    match matches.as_slice() {
        [single] => Ok((*single).clone()),
        [] => Err(anyhow!("no captured session matches `{token}`")),
        many => Err(anyhow!(
            "`{token}` is ambiguous, it matches {} sessions",
            many.len()
        )),
    }
}

fn short_id(session: &TrafficSession) -> String {
    let simple = session.id.simple().to_string();
    simple[simple.len().saturating_sub(7)..].to_owned()
}

fn route(session: &TrafficSession) -> String {
    format!(
        "{} {}",
        session.inbound_request.method, session.inbound_request.uri
    )
}

async fn fetch<T: DeserializeOwned>(client: &reqwest::Client, url: &str) -> Result<T> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("cannot reach the control plane at {url}"))?;
    decode(response).await
}

async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let status = response.status();
    let body = response
        .bytes()
        .await
        .context("failed to read the control plane response")?;
    if status.is_success() {
        return serde_json::from_slice(&body).context("unexpected control plane payload");
    }
    let message = serde_json::from_slice::<ApiFailure>(&body)
        .map(|failure| failure.message)
        .unwrap_or_else(|_| String::from_utf8_lossy(&body).into_owned());
    Err(anyhow!("{status}: {message}"))
}
