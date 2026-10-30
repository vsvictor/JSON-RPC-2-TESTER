use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Write as _,
    future::Future,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, anyhow};
use eframe::egui::{self, Color32, RichText, TextEdit};
use json_rpc_2_tester::{
    connection::{self, Client, GatewaySettings, RuntimeSettings, Transport},
    ecosystem, requests,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{runtime::Runtime, sync::mpsc, task::JoinHandle};
use tracing::{
    Event as TraceEvent, Level, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

const ACCENT: Color32 = Color32::from_rgb(72, 210, 185);
const QUIC_COLOR: Color32 = Color32::from_rgb(50, 205, 50);
const TLS_COLOR: Color32 = Color32::from_rgb(55, 125, 255);
const TCP_COLOR: Color32 = Color32::from_rgb(95, 205, 245);
const MAX_LOG_ENTRIES: usize = 500;
const MAX_TEXT_BYTES: usize = 32 * 1024;

fn transport_color(kind: &str) -> Color32 {
    match kind {
        "quic" => QUIC_COLOR,
        "tls" => TLS_COLOR,
        "tcp" => TCP_COLOR,
        _ => Color32::LIGHT_RED,
    }
}

fn transport_enabled_mask(transports: &[ecosystem::TransportStatus]) -> [bool; 3] {
    ["quic", "tls", "tcp"].map(|name| {
        transports
            .iter()
            .find(|transport| transport.transport.eq_ignore_ascii_case(name))
            .is_some_and(|transport| transport.enabled)
    })
}

enum Event {
    Connected {
        generation: u64,
        result: Result<(Client, Transport)>,
    },
    MethodsLoaded {
        generation: u64,
        result: Result<Vec<ecosystem::MethodInfo>>,
    },
    TransportsLoaded {
        generation: u64,
        result: Result<Vec<ecosystem::TransportStatus>>,
    },
    ReturnAttempted {
        generation: u64,
        target: String,
        result: Result<()>,
    },
    TransportUpdated {
        generation: u64,
        transport: String,
        enabled: bool,
        result: Result<()>,
    },
    BackendsLoaded {
        generation: u64,
        result: Result<Vec<ecosystem::ManagedBackend>>,
    },
    BackendOperationFinished {
        generation: u64,
        label: String,
        result: Result<()>,
    },
    Finished {
        id: u64,
        label: String,
        elapsed: Duration,
        result: Result<Value>,
    },
}

struct TraceLine(Level, String);

struct Capture {
    tx: mpsc::Sender<TraceLine>,
    debug: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
}

#[derive(Default)]
struct Fields(String);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if self.0.len() < MAX_TEXT_BYTES {
            let _ = write!(&mut self.0, "{}={value:?} ", field.name());
        }
    }
}

impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &TraceEvent<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let level = *event.metadata().level();
        if level > Level::INFO && !self.debug.load(Ordering::Relaxed) {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        let line = TraceLine(
            level,
            clip(format!("{}: {}", event.metadata().target(), fields.0)),
        );
        if self.tx.try_send(line).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn clip(mut text: String) -> String {
    if text.len() > MAX_TEXT_BYTES {
        let mut boundary = MAX_TEXT_BYTES;
        while !text.is_char_boundary(boundary) {
            boundary -= 1;
        }
        text.truncate(boundary);
        text.push_str("\n[display truncated]");
    }
    text
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Tab {
    Request,
    Batch,
    Policy,
    Metrics,
    Log,
    Map,
}

struct LogEntry {
    seconds: f64,
    level: Level,
    message: String,
}

#[derive(Clone, Default)]
struct SettingsDraft {
    host: String,
    port: String,
    server_name: String,
    ca_file: String,
    connect_timeout_ms: String,
    retry_delay_ms: String,
    control_port: String,
    control_timeout_ms: String,
    ping_ms: String,
    missed_pongs: String,
    recovery_attempts: String,
    gateway_up_return_sec: String,
}

fn parse_setting<T: std::str::FromStr>(value: &str, key: &str) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    value
        .trim()
        .parse()
        .map_err(|error| anyhow!("Invalid {key}: {error}"))
}

fn parse_backend_registration(
    id: &str,
    host: &str,
    ports: &str,
    pool_size: &str,
) -> Result<ecosystem::BackendRegistration> {
    let id = id.trim();
    let host = host.trim();
    anyhow::ensure!(!id.is_empty(), "Backend id is required");
    anyhow::ensure!(!host.is_empty(), "Backend host is required");
    anyhow::ensure!(
        !id.contains(['\n', '\r']) && !host.contains(['\n', '\r']),
        "Backend id and host must not contain line breaks"
    );
    let ports: BTreeMap<String, u16> = serde_json::from_str(ports)
        .context("Backend ports must be a JSON object of port numbers")?;
    anyhow::ensure!(!ports.is_empty(), "At least one backend port is required");
    anyhow::ensure!(
        ports
            .iter()
            .all(|(transport, port)| !transport.trim().is_empty() && *port != 0),
        "Backend ports must have non-empty transport names and nonzero port numbers"
    );
    let pool_size = parse_setting(pool_size, "pool size")?;
    anyhow::ensure!(pool_size > 0, "Pool size must be greater than zero");
    Ok(ecosystem::BackendRegistration {
        id: id.to_owned(),
        host: host.to_owned(),
        ports,
        pool_size,
    })
}

impl SettingsDraft {
    fn from_env_file() -> Result<Self> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".env");
        let mut values = BTreeMap::new();
        match dotenvy::from_path_iter(path) {
            Ok(entries) => {
                for entry in entries {
                    let (key, value) = entry.context("Parsing .env settings")?;
                    values.insert(key, value);
                }
            }
            Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("Reading .env settings"),
        }
        let value = |key: &str, default: &str| {
            std::env::var(key)
                .ok()
                .or_else(|| values.get(key).cloned())
                .unwrap_or_else(|| default.to_owned())
        };
        Ok(Self {
            host: value("GATEWAY_HOST", ""),
            port: value("GATEWAY_PORT", ""),
            server_name: value("GATEWAY_TLS_SERVER_NAME", ""),
            ca_file: value("GATEWAY_CA_FILE", ""),
            connect_timeout_ms: value("GATEWAY_CONNECT_TIMEOUT_MS", "10000"),
            retry_delay_ms: value("GATEWAY_RETRY_DELAY_MS", "500"),
            control_port: value("GATEWAY_CONTROL_PORT", ""),
            control_timeout_ms: value("GATEWAY_CONTROL_TIMEOUT_MS", "5000"),
            ping_ms: value("GATEWAY_PING_INTERVAL_MS", "10000"),
            missed_pongs: value("GATEWAY_MISSED_PONGS", "3"),
            recovery_attempts: value("GATEWAY_RECOVERY_ATTEMPTS", "3"),
            gateway_up_return_sec: value("GATEWAY_UP_RETURN_SEC", "30"),
        })
    }

    fn from_settings(settings: &GatewaySettings, policy: &RuntimeSettings) -> Self {
        Self {
            host: settings.host.clone(),
            port: settings.port.to_string(),
            server_name: settings.server_name.clone(),
            ca_file: settings.ca_file.clone(),
            connect_timeout_ms: settings.connect_timeout_ms.to_string(),
            retry_delay_ms: settings.retry_delay_ms.to_string(),
            control_port: settings.control_port.to_string(),
            control_timeout_ms: settings.control_timeout_ms.to_string(),
            ping_ms: policy.ping_ms.to_string(),
            missed_pongs: policy.missed_pongs.to_string(),
            recovery_attempts: policy.recovery_attempts.to_string(),
            gateway_up_return_sec: settings.gateway_up_return_sec.to_string(),
        }
    }

    fn build(&self) -> Result<(GatewaySettings, RuntimeSettings)> {
        for (key, value) in [
            ("GATEWAY_HOST", self.host.as_str()),
            ("GATEWAY_TLS_SERVER_NAME", self.server_name.as_str()),
            ("GATEWAY_CA_FILE", self.ca_file.as_str()),
        ] {
            anyhow::ensure!(
                !value.contains(['\n', '\r']),
                "{key} must not contain a line break"
            );
        }
        let ca_file = self.ca_file.trim().to_owned();
        let ca_pem = std::fs::read_to_string(&ca_file)
            .with_context(|| format!("Reading trusted gateway CA file {ca_file}"))?;
        let policy = RuntimeSettings {
            ping_ms: parse_setting(&self.ping_ms, "GATEWAY_PING_INTERVAL_MS")?,
            missed_pongs: parse_setting(&self.missed_pongs, "GATEWAY_MISSED_PONGS")?,
            recovery_attempts: parse_setting(&self.recovery_attempts, "GATEWAY_RECOVERY_ATTEMPTS")?,
            ..RuntimeSettings::default()
        };
        policy.config()?;
        let settings = GatewaySettings {
            host: self.host.trim().to_owned(),
            port: parse_setting(&self.port, "GATEWAY_PORT")?,
            server_name: self.server_name.trim().to_owned(),
            ca_file,
            ca_pem,
            connect_timeout_ms: parse_setting(
                &self.connect_timeout_ms,
                "GATEWAY_CONNECT_TIMEOUT_MS",
            )?,
            retry_delay_ms: parse_setting(&self.retry_delay_ms, "GATEWAY_RETRY_DELAY_MS")?,
            control_port: parse_setting(&self.control_port, "GATEWAY_CONTROL_PORT")?,
            control_timeout_ms: parse_setting(
                &self.control_timeout_ms,
                "GATEWAY_CONTROL_TIMEOUT_MS",
            )?,
            recovery_attempts: Arc::new(AtomicU32::new(policy.recovery_attempts)),
            transport_policy: Arc::new(std::sync::atomic::AtomicU8::new(0b0000_0111)),
            gateway_up_return_sec: parse_setting(
                &self.gateway_up_return_sec,
                "GATEWAY_UP_RETURN_SEC",
            )?,
        };
        settings.validate()?;
        Ok((settings, policy))
    }

    fn to_env(&self) -> String {
        format!(
            "GATEWAY_HOST={}\nGATEWAY_PORT={}\nGATEWAY_TLS_SERVER_NAME={}\nGATEWAY_CA_FILE={}\nGATEWAY_CONNECT_TIMEOUT_MS={}\nGATEWAY_RETRY_DELAY_MS={}\nGATEWAY_CONTROL_PORT={}\nGATEWAY_CONTROL_TIMEOUT_MS={}\nGATEWAY_PING_INTERVAL_MS={}\nGATEWAY_MISSED_PONGS={}\nGATEWAY_RECOVERY_ATTEMPTS={}\nGATEWAY_UP_RETURN_SEC={}\n",
            self.host.trim(),
            self.port.trim(),
            self.server_name.trim(),
            self.ca_file.trim(),
            self.connect_timeout_ms.trim(),
            self.retry_delay_ms.trim(),
            self.control_port.trim(),
            self.control_timeout_ms.trim(),
            self.ping_ms.trim(),
            self.missed_pongs.trim(),
            self.recovery_attempts.trim(),
            self.gateway_up_return_sec.trim()
        )
    }
}

pub struct DesktopApp {
    map: crate::ecosystem_map::EcosystemMap,
    runtime: Option<Runtime>,
    tx: mpsc::Sender<Event>,
    rx: mpsc::Receiver<Event>,
    trace_rx: mpsc::Receiver<TraceLine>,
    debug: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
    connection: Option<GatewaySettings>,
    policy: RuntimeSettings,
    client: Option<(Client, Transport)>,
    connection_error: bool,
    control: Option<JoinHandle<()>>,
    method_discovery: Option<JoinHandle<()>>,
    transports: Vec<ecosystem::TransportStatus>,
    transports_initialized: bool,
    transports_loading: bool,
    transport_operation: Option<String>,
    return_in_progress: bool,
    last_transport_poll: Instant,
    connection_generation: u64,
    backends_open: bool,
    backends: Vec<ecosystem::ManagedBackend>,
    backends_loading: bool,
    backend_operation: Option<String>,
    backends_error: Option<String>,
    backends_last_poll: Instant,
    backend_id_draft: String,
    backend_host_draft: String,
    backend_ports_draft: String,
    backend_pool_size_draft: String,
    settings_open: bool,
    settings_draft: Option<SettingsDraft>,
    settings_error: Option<String>,
    tasks: BTreeMap<u64, (String, JoinHandle<()>)>,
    next_id: u64,
    started: Instant,
    logs: VecDeque<LogEntry>,
    tab: Tab,
    method: String,
    methods: Vec<ecosystem::MethodInfo>,
    methods_loading: bool,
    methods_refresh_pending: bool,
    methods_error: Option<String>,
    params: String,
    parallel_count: usize,
    batch: String,
    response: String,
    response_error: bool,
    status: String,
    log_filter: String,
    only_errors: bool,
}

impl DesktopApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Result<Self> {
        cc.egui_ctx.set_visuals(egui::Visuals::dark());
        let mut style = (*cc.egui_ctx.style()).clone();
        style.visuals.selection.bg_fill = ACCENT.gamma_multiply(0.4);
        style.spacing.item_spacing = egui::vec2(10.0, 8.0);
        cc.egui_ctx.set_style(style);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let (tx, rx) = mpsc::channel(128);
        let (trace_tx, trace_rx) = mpsc::channel(512);
        let debug = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicUsize::new(0));
        tracing_subscriber::registry()
            .with(Capture {
                tx: trace_tx,
                debug: debug.clone(),
                dropped: dropped.clone(),
            })
            .try_init()
            .context("Installing tracing subscriber")?;
        let (connection, policy, configuration_error) = match GatewaySettings::from_env() {
            Ok(settings) => match RuntimeSettings::from_env() {
                Ok(policy) => {
                    settings.set_recovery_attempts(policy.recovery_attempts);
                    (Some(settings), policy, None)
                }
                Err(error) => (None, RuntimeSettings::default(), Some(format!("{error:#}"))),
            },
            Err(error) => (None, RuntimeSettings::default(), Some(format!("{error:#}"))),
        };
        let map = connection.as_ref().map_or_else(
            crate::ecosystem_map::EcosystemMap::default,
            |connection| {
                crate::ecosystem_map::EcosystemMap::configured_with_transport(
                    connection.host.clone(),
                    connection.control_port,
                    connection.control_timeout_ms,
                    ecosystem::CONTROL_PLANE_TRANSPORT,
                    connection.server_name.clone(),
                    connection.ca_pem.clone(),
                )
            },
        );
        let mut app = Self {
            map,
            runtime: Some(runtime),
            tx,
            rx,
            trace_rx,
            debug,
            dropped,
            connection,
            policy,
            client: None,
            connection_error: configuration_error.is_some(),
            control: None,
            method_discovery: None,
            transports: Vec::new(),
            transports_initialized: false,
            transports_loading: false,
            transport_operation: None,
            return_in_progress: false,
            last_transport_poll: Instant::now(),
            connection_generation: 0,
            backends_open: false,
            backends: Vec::new(),
            backends_loading: false,
            backend_operation: None,
            backends_error: None,
            backends_last_poll: Instant::now(),
            backend_id_draft: String::new(),
            backend_host_draft: String::new(),
            backend_ports_draft: r#"{"tcp":18081}"#.into(),
            backend_pool_size_draft: "4".into(),
            settings_open: false,
            settings_draft: None,
            settings_error: None,
            tasks: BTreeMap::new(),
            next_id: 1,
            started: Instant::now(),
            logs: VecDeque::new(),
            tab: Tab::Request,
            method: String::new(),
            methods: Vec::new(),
            methods_loading: false,
            methods_refresh_pending: false,
            methods_error: None,
            params: "{\n  \"message\": \"Hello, desktop!\"\n}".into(),
            parallel_count: 8,
            batch: requests::DEFAULT_BATCH.into(),
            response: configuration_error
                .as_ref()
                .map_or_else(|| "Connecting to gateway from .env…".into(), |error| {
                    format!(
                        "Gateway configuration is unavailable: {error}\n\nCheck .env values for the gateway, CA file, GATEWAY_PING_INTERVAL_MS, GATEWAY_MISSED_PONGS, and GATEWAY_RECOVERY_ATTEMPTS, then restart the app."
                    )
                }),
            response_error: configuration_error.is_some(),
            status: if configuration_error.is_some() {
                "Gateway configuration required".into()
            } else {
                "Ready".into()
            },
            log_filter: String::new(),
            only_errors: false,
        };
        if configuration_error.is_none() {
            app.start_connection();
        }
        Ok(app)
    }

    fn rt(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .expect("runtime exists until app shutdown")
    }

    fn log(&mut self, level: Level, message: impl Into<String>) {
        if self.logs.len() == MAX_LOG_ENTRIES {
            self.logs.pop_front();
        }
        self.logs.push_back(LogEntry {
            seconds: self.started.elapsed().as_secs_f64(),
            level,
            message: clip(message.into()),
        });
    }

    fn error(&mut self, error: impl std::fmt::Display) {
        self.response = clip(error.to_string());
        self.response_error = true;
        self.status = "Operation failed; see result / event log".into();
        self.log(Level::ERROR, self.response.clone());
    }

    fn drain(&mut self) {
        while let Ok(event) = self.rx.try_recv() {
            match event {
                Event::MethodsLoaded { generation, result } => {
                    if generation != self.connection_generation {
                        continue;
                    }
                    self.method_discovery = None;
                    self.methods_loading = false;
                    match result {
                        Ok(methods) => {
                            let selected = methods
                                .iter()
                                .any(|method| method.name == self.method)
                                .then(|| self.method.clone())
                                .or_else(|| methods.first().map(|method| method.name.clone()))
                                .unwrap_or_default();
                            if selected != self.method
                                && let Some(method) =
                                    methods.iter().find(|method| method.name == selected)
                            {
                                self.params = method.example_params.clone();
                            }
                            self.method = selected;
                            self.methods = methods;
                            self.methods_error = None;
                            self.log(
                                Level::INFO,
                                format!("Loaded {} implemented RPC methods", self.methods.len()),
                            );
                        }
                        Err(error) => {
                            let message = format!("{error:#}");
                            self.methods_error = Some(message.clone());
                            self.log(
                                Level::ERROR,
                                format!("Loading RPC methods failed: {message}"),
                            );
                        }
                    }
                    if std::mem::take(&mut self.methods_refresh_pending) {
                        self.start_method_discovery();
                    }
                }
                Event::TransportsLoaded { generation, result } => {
                    if generation != self.connection_generation {
                        continue;
                    }
                    self.transports_loading = false;
                    self.last_transport_poll = Instant::now();
                    match result {
                        Ok(transports) => {
                            let enabled = transport_enabled_mask(&transports);
                            let was_initialized = self.transports_initialized;
                            let state_changed = !was_initialized
                                || transports.len() != self.transports.len()
                                || transports.iter().any(|current| {
                                    !self.transports.iter().any(|previous| {
                                        previous.transport.eq_ignore_ascii_case(&current.transport)
                                            && previous.enabled == current.enabled
                                    })
                                });
                            let active_kind =
                                self.client.as_ref().map(|(_, transport)| transport.kind());
                            let active_was_disabled = active_kind.is_some_and(|kind| {
                                transports.iter().any(|profile| {
                                    profile.transport.eq_ignore_ascii_case(kind) && !profile.enabled
                                })
                            });
                            let connecting_with_disabled_profile =
                                !was_initialized && self.control.is_some() && enabled != [true; 3];
                            let retry_after_initial_disabled_profiles = !was_initialized
                                && self.client.is_none()
                                && self.control.is_none()
                                && enabled != [true; 3];
                            if let Some(settings) = &self.connection {
                                settings.set_transport_policy(enabled, false);
                            }
                            self.transports_initialized = true;
                            self.transports = transports;
                            if state_changed {
                                self.log(Level::INFO, "Gateway transport profile state updated");
                            }
                            if connecting_with_disabled_profile
                                || retry_after_initial_disabled_profiles
                            {
                                self.log(
                                    Level::WARN,
                                    "Transport availability changed; reconnecting QUIC → TLS → TCP",
                                );
                                self.start_connection_order(false);
                            } else if active_was_disabled && !self.return_in_progress {
                                self.log(
                                    Level::WARN,
                                    "Active transport was disabled; reconnecting with available profiles",
                                );
                                self.start_return_attempt(enabled, "fallback");
                            } else if was_initialized && !self.return_in_progress {
                                match active_kind {
                                    Some("tcp") if enabled[0] || enabled[1] => {
                                        let candidates = [enabled[0], enabled[1], false];
                                        let target = if enabled[0] { "quic" } else { "tls" };
                                        self.start_return_attempt(candidates, target);
                                    }
                                    Some("tls") if enabled[0] => {
                                        self.start_return_attempt([true, false, false], "quic");
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Err(error) => {
                            self.log(
                                Level::ERROR,
                                format!("Loading gateway transport profiles failed: {error:#}"),
                            );
                        }
                    }
                }
                Event::ReturnAttempted {
                    generation,
                    target,
                    result,
                } => {
                    if generation != self.connection_generation {
                        continue;
                    }
                    self.return_in_progress = false;
                    self.last_transport_poll = Instant::now();
                    if let Some(settings) = &self.connection {
                        settings
                            .set_transport_policy(transport_enabled_mask(&self.transports), false);
                    }
                    match result {
                        Ok(()) => {
                            self.log(Level::INFO, "Gateway transport preference updated");
                            self.start_transport_discovery();
                        }
                        Err(error) => {
                            self.log(
                                Level::WARN,
                                format!(
                                    "Transport return attempt ({target}) failed; keeping the active session: {error:#}"
                                ),
                            );
                        }
                    }
                }
                Event::TransportUpdated {
                    generation,
                    transport,
                    enabled,
                    result,
                } => {
                    if generation != self.connection_generation {
                        continue;
                    }
                    self.transport_operation = None;
                    match result {
                        Ok(()) => {
                            self.log(
                                Level::INFO,
                                format!(
                                    "Gateway transport {transport} {}",
                                    if enabled { "enabled" } else { "disabled" }
                                ),
                            );
                            if !enabled {
                                self.start_transport_discovery();
                            }
                        }
                        Err(error) => {
                            self.log(
                                Level::ERROR,
                                format!("Updating {transport} transport failed: {error:#}"),
                            );
                            self.start_transport_discovery();
                        }
                    }
                }
                Event::BackendsLoaded { generation, result } => {
                    if generation != self.connection_generation {
                        continue;
                    }
                    self.backends_loading = false;
                    match result {
                        Ok(backends) => {
                            self.backends = backends;
                            self.backends_error = None;
                        }
                        Err(error) => {
                            self.backends_error = Some(format!("{error:#}"));
                        }
                    }
                }
                Event::BackendOperationFinished {
                    generation,
                    label,
                    result,
                } => {
                    if generation != self.connection_generation {
                        continue;
                    }
                    self.backend_operation = None;
                    match result {
                        Ok(()) => {
                            self.log(Level::INFO, format!("Backend operation completed: {label}"));
                            if label == "Add backend" {
                                self.backend_id_draft.clear();
                                self.backend_host_draft.clear();
                            }
                            self.start_backends_discovery();
                            if self.methods_loading {
                                self.methods_refresh_pending = true;
                            } else {
                                self.start_method_discovery();
                            }
                        }
                        Err(error) => {
                            let message = format!("{error:#}");
                            self.backends_error = Some(message.clone());
                            self.log(
                                Level::ERROR,
                                format!("Backend operation {label} failed: {message}"),
                            );
                        }
                    }
                }
                Event::Connected { generation, result } => {
                    if generation != self.connection_generation {
                        if let Ok((client, _)) = result {
                            self.rt().spawn(async move {
                                let _ = client.close().await;
                            });
                        }
                        continue;
                    }
                    self.control = None;
                    match result {
                        Ok(pair) => {
                            self.connection_error = false;
                            let session = format!(
                                "{} / {}",
                                pair.1.kind(),
                                pair.1.peer().unwrap_or_else(|| "peer not reported".into())
                            );
                            self.client = Some(pair);
                            self.status = "Session open".into();
                            self.log(Level::INFO, format!("Session opened: {session}"));
                            self.start_method_discovery();
                            self.start_transport_discovery();
                        }
                        Err(error) => {
                            self.connection_error = true;
                            self.error(format!("{error:#}"));
                        }
                    }
                }
                Event::Finished {
                    id,
                    label,
                    elapsed,
                    result,
                } => {
                    // Aborted jobs cannot replace a newer result or a new session's output.
                    if self.tasks.remove(&id).is_none() {
                        continue;
                    }
                    match result {
                        Ok(value) => match serde_json::to_string_pretty(&value) {
                            Ok(text) => {
                                self.response = clip(text);
                                self.response_error = false;
                                self.status = format!(
                                    "#{id} {label}: {:.1} ms",
                                    elapsed.as_secs_f64() * 1000.0
                                );
                                self.log(Level::INFO, self.status.clone());
                            }
                            Err(error) => self.error(error),
                        },
                        Err(error) => self.error(format!("#{id} {label}: {error:#}")),
                    }
                }
            }
        }
        for _ in 0..512 {
            match self.trace_rx.try_recv() {
                Ok(TraceLine(level, text)) => self.log(level, text),
                Err(_) => break,
            }
        }
        let dropped = self.dropped.swap(0, Ordering::Relaxed);
        if dropped > 0 {
            self.log(
                Level::WARN,
                format!("{dropped} trace events dropped because the UI log queue was full"),
            );
        }
    }

    fn start_connection(&mut self) {
        self.start_connection_order(false);
    }

    fn start_connection_order(&mut self, prefer_reverse: bool) {
        if let Err(error) = self.policy.config() {
            self.error(error);
            return;
        }
        let Some(settings) = self.connection.clone() else {
            return;
        };
        if !self.transports_initialized {
            settings.set_transport_policy([true; 3], prefer_reverse);
        }
        self.connection_generation = self.connection_generation.wrapping_add(1);
        let generation = self.connection_generation;
        if let Some(task) = self.control.take() {
            task.abort();
        }
        if let Some(task) = self.method_discovery.take() {
            task.abort();
        }
        self.methods.clear();
        self.method.clear();
        self.methods_error = None;
        self.methods_loading = false;
        self.methods_refresh_pending = false;
        self.transports_loading = false;
        self.transport_operation = None;
        self.return_in_progress = false;
        self.backends_loading = false;
        self.backend_operation = None;
        self.cancel_all();
        self.connection_error = false;
        let previous_client = self.client.take().map(|(client, _)| client);
        let policy = self.policy.clone();
        let tx = self.tx.clone();
        self.status = "Connecting...".into();
        self.control = Some(self.rt().spawn(async move {
            if let Some(client) = previous_client {
                let _ = tokio::time::timeout(Duration::from_secs(3), client.close()).await;
            }
            let result = connection::connect_gateway(settings, policy).await;
            if let Err(error) = tx.send(Event::Connected { generation, result }).await
                && let Event::Connected {
                    result: Ok((client, _)),
                    ..
                } = error.0
            {
                let _ = client.close().await;
            }
        }));
    }

    fn start_method_discovery(&mut self) {
        if self.methods_loading {
            return;
        }
        let Some(connection) = &self.connection else {
            self.methods_error = Some("Gateway configuration is unavailable".into());
            return;
        };
        let host = connection.host.clone();
        let port = connection.control_port;
        let timeout = Duration::from_millis(connection.control_timeout_ms);
        let transport = ecosystem::CONTROL_PLANE_TRANSPORT.to_owned();
        let server_name = connection.server_name.clone();
        let ca_pem = connection.ca_pem.clone();
        let tx = self.tx.clone();
        let generation = self.connection_generation;
        self.methods_loading = true;
        self.methods_error = None;
        self.method_discovery = Some(self.rt().spawn(async move {
            let result = ecosystem::fetch_methods_with_transport(
                &host,
                port,
                timeout,
                &transport,
                &server_name,
                &ca_pem,
            )
            .await;
            let _ = tx.send(Event::MethodsLoaded { generation, result }).await;
        }));
    }

    fn start_transport_discovery(&mut self) {
        if self.transports_loading {
            return;
        }
        let Some(connection) = &self.connection else {
            return;
        };
        let host = connection.host.clone();
        let port = connection.control_port;
        let timeout = Duration::from_millis(connection.control_timeout_ms);
        let transport = ecosystem::CONTROL_PLANE_TRANSPORT.to_owned();
        let server_name = connection.server_name.clone();
        let ca_pem = connection.ca_pem.clone();
        let tx = self.tx.clone();
        let generation = self.connection_generation;
        self.last_transport_poll = Instant::now();
        self.transports_loading = true;
        self.rt().spawn(async move {
            let result = ecosystem::fetch_transports_with_transport(
                &host,
                port,
                timeout,
                &transport,
                &server_name,
                &ca_pem,
            )
            .await;
            let _ = tx
                .send(Event::TransportsLoaded { generation, result })
                .await;
        });
    }

    fn start_backends_discovery(&mut self) {
        if self.backends_loading || self.backend_operation.is_some() {
            return;
        }
        let Some(connection) = &self.connection else {
            self.backends_error = Some("Gateway configuration is unavailable".into());
            return;
        };
        let host = connection.host.clone();
        let port = connection.control_port;
        let timeout = Duration::from_millis(connection.control_timeout_ms);
        let transport = ecosystem::CONTROL_PLANE_TRANSPORT.to_owned();
        let server_name = connection.server_name.clone();
        let ca_pem = connection.ca_pem.clone();
        let tx = self.tx.clone();
        let generation = self.connection_generation;
        self.backends_loading = true;
        self.backends_last_poll = Instant::now();
        self.rt().spawn(async move {
            let result = ecosystem::fetch_backends_with_transport(
                &host,
                port,
                timeout,
                ecosystem::ControlTransportOptions {
                    kind: &transport,
                    server_name: &server_name,
                    ca_pem: &ca_pem,
                },
            )
            .await;
            let _ = tx.send(Event::BackendsLoaded { generation, result }).await;
        });
    }

    fn start_backend_operation(
        &mut self,
        label: String,
        future: impl Future<Output = Result<()>> + Send + 'static,
    ) {
        if self.backend_operation.is_some() {
            return;
        }
        let tx = self.tx.clone();
        let generation = self.connection_generation;
        self.backend_operation = Some(label.clone());
        self.backends_error = None;
        self.rt().spawn(async move {
            let result = future.await;
            let _ = tx
                .send(Event::BackendOperationFinished {
                    generation,
                    label,
                    result,
                })
                .await;
        });
    }

    fn add_backend(&mut self) {
        let backend = match parse_backend_registration(
            &self.backend_id_draft,
            &self.backend_host_draft,
            &self.backend_ports_draft,
            &self.backend_pool_size_draft,
        ) {
            Ok(backend) => backend,
            Err(error) => {
                self.backends_error = Some(format!("{error:#}"));
                return;
            }
        };
        let Some(connection) = &self.connection else {
            self.backends_error = Some("Gateway configuration is unavailable".into());
            return;
        };
        let host = connection.host.clone();
        let port = connection.control_port;
        let timeout = Duration::from_millis(connection.control_timeout_ms);
        let transport = ecosystem::CONTROL_PLANE_TRANSPORT.to_owned();
        let server_name = connection.server_name.clone();
        let ca_pem = connection.ca_pem.clone();
        self.start_backend_operation("Add backend".into(), async move {
            ecosystem::add_backend_with_transport(
                &host,
                port,
                timeout,
                ecosystem::ControlTransportOptions {
                    kind: &transport,
                    server_name: &server_name,
                    ca_pem: &ca_pem,
                },
                &backend,
            )
            .await
        });
    }

    fn set_backend_enabled(&mut self, backend_id: String, enabled: bool) {
        let Some(connection) = &self.connection else {
            self.backends_error = Some("Gateway configuration is unavailable".into());
            return;
        };
        let host = connection.host.clone();
        let port = connection.control_port;
        let timeout = Duration::from_millis(connection.control_timeout_ms);
        let transport = ecosystem::CONTROL_PLANE_TRANSPORT.to_owned();
        let server_name = connection.server_name.clone();
        let ca_pem = connection.ca_pem.clone();
        let label = if enabled {
            format!("Enable backend {backend_id}")
        } else {
            format!("Disable backend {backend_id}")
        };
        self.start_backend_operation(label, async move {
            ecosystem::set_backend_enabled_with_transport(
                &host,
                port,
                timeout,
                ecosystem::ControlTransportOptions {
                    kind: &transport,
                    server_name: &server_name,
                    ca_pem: &ca_pem,
                },
                &backend_id,
                enabled,
            )
            .await
        });
    }

    fn start_return_attempt(&mut self, candidates: [bool; 3], target: &str) {
        if self.return_in_progress {
            return;
        }
        let Some(settings) = &self.connection else {
            return;
        };
        let Some((_, transport)) = &self.client else {
            return;
        };
        settings.set_transport_policy(candidates, false);
        let transport = transport.clone();
        let generation = self.connection_generation;
        let target = target.to_owned();
        let tx = self.tx.clone();
        self.return_in_progress = true;
        self.log(
            Level::INFO,
            format!("Trying preferred transport {target} while keeping the active session"),
        );
        self.rt().spawn(async move {
            let result = transport
                .reconnect()
                .await
                .map_err(|error| anyhow!("{error}"));
            let _ = tx
                .send(Event::ReturnAttempted {
                    generation,
                    target,
                    result,
                })
                .await;
        });
    }

    fn set_transport_enabled(&mut self, transport: String, enabled: bool) {
        if self.transport_operation.is_some() || self.return_in_progress {
            return;
        }
        let Some(connection) = &self.connection else {
            return;
        };
        let host = connection.host.clone();
        let port = connection.control_port;
        let timeout = Duration::from_millis(connection.control_timeout_ms);
        let connection_transport = ecosystem::CONTROL_PLANE_TRANSPORT.to_owned();
        let server_name = connection.server_name.clone();
        let ca_pem = connection.ca_pem.clone();
        let tx = self.tx.clone();
        let generation = self.connection_generation;
        self.transport_operation = Some(transport.clone());
        self.rt().spawn(async move {
            let result = ecosystem::set_transport_enabled_with_transport(
                &host,
                port,
                timeout,
                ecosystem::ControlTransportOptions {
                    kind: &connection_transport,
                    server_name: &server_name,
                    ca_pem: &ca_pem,
                },
                &transport,
                enabled,
            )
            .await;
            let _ = tx
                .send(Event::TransportUpdated {
                    generation,
                    transport,
                    enabled,
                    result,
                })
                .await;
        });
    }

    fn open_settings(&mut self) {
        self.settings_draft = Some(match &self.connection {
            Some(settings) => SettingsDraft::from_settings(settings, &self.policy),
            None => match SettingsDraft::from_env_file() {
                Ok(draft) => draft,
                Err(error) => {
                    self.settings_error = Some(format!("{error:#}"));
                    SettingsDraft::default()
                }
            },
        });
        if self.connection.is_some() {
            self.settings_error = None;
        }
        self.settings_open = true;
    }

    fn save_settings(&mut self) {
        let Some(draft) = self.settings_draft.clone() else {
            return;
        };
        let result: Result<()> = (|| {
            let (settings, policy) = draft.build()?;
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".env");
            let temporary_path = path.with_extension("env.tmp");
            std::fs::write(&temporary_path, draft.to_env())
                .context("Writing temporary application settings")?;
            if let Err(error) = std::fs::rename(&temporary_path, &path) {
                let _ = std::fs::remove_file(&temporary_path);
                return Err(error).context("Saving application settings to .env");
            }
            settings.set_recovery_attempts(policy.recovery_attempts);
            self.connection = Some(settings.clone());
            self.policy = policy;
            self.map = crate::ecosystem_map::EcosystemMap::configured_with_transport(
                settings.host.clone(),
                settings.control_port,
                settings.control_timeout_ms,
                ecosystem::CONTROL_PLANE_TRANSPORT,
                settings.server_name.clone(),
                settings.ca_pem.clone(),
            );
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.settings_error = None;
                self.settings_open = false;
                self.transports.clear();
                self.transports_initialized = false;
                self.transports_loading = false;
                self.start_connection();
                self.status = "Settings saved; reconnecting to gateway".into();
            }
            Err(error) => self.settings_error = Some(format!("{error:#}")),
        }
    }

    fn cancel_all(&mut self) {
        let tasks = std::mem::take(&mut self.tasks);
        for (id, (label, task)) in tasks {
            task.abort();
            self.log(
                Level::WARN,
                format!(
                    "#{id} {label}: local task cancelled (server side effects are not rolled back)"
                ),
            );
        }
    }

    fn run(&mut self, label: &str, future: impl Future<Output = Result<Value>> + Send + 'static) {
        if self.tasks.len() >= 64 {
            self.error("At most 64 UI operations may run at once");
            return;
        }
        let id = self.next_id;
        self.next_id += 1;
        let tx = self.tx.clone();
        let label = label.to_owned();
        let task_label = label.clone();
        let task = self.rt().spawn(async move {
            let started = Instant::now();
            let result = future.await;
            let _ = tx
                .send(Event::Finished {
                    id,
                    label: task_label,
                    elapsed: started.elapsed(),
                    result,
                })
                .await;
        });
        self.tasks.insert(id, (label.clone(), task));
        self.log(Level::INFO, format!("#{id} {label}: started"));
    }

    fn send_request(&mut self, notification: bool, parallel: bool) {
        if !self.methods.iter().any(|method| method.name == self.method) {
            self.error("Select a method reported by the Control Plane before sending a request");
            return;
        }
        let input = requests::method(&self.method)
            .and_then(|name| Ok((name, requests::params(&self.params)?)));
        let (name, params) = match input {
            Ok(input) => input,
            Err(error) => {
                self.error(error);
                return;
            }
        };
        let Some((client, _)) = &self.client else {
            self.error("No open session");
            return;
        };
        let client = client.clone();
        let count = self.parallel_count;
        let label = if notification {
            "Notification"
        } else if parallel {
            "Parallel calls"
        } else {
            "Call"
        };
        self.run(label, async move {
            if notification {
                client.notify(name, params).await?;
                Ok(json!({"notification": "sent", "note": "No JSON-RPC response is expected"}))
            } else if parallel {
                requests::parallel(client, name, params, count).await
            } else {
                client
                    .request_value(name, params)
                    .await
                    .map_err(|error| anyhow!("{}", requests::error_json(&error)))
            }
        });
    }

    fn sidebar(&mut self, ui: &mut egui::Ui) {
        ui.heading(RichText::new("CONNECTION").color(ACCENT));
        ui.label("Gateway RPC session shared by every tab");
        ui.separator();
        if ui
            .add_enabled(
                self.connection.is_some() && !self.return_in_progress,
                egui::Button::new("Reconnect"),
            )
            .clicked()
        {
            self.start_connection();
        }
        ui.horizontal(|ui| {
            if let Some((_, transport)) = &self.client {
                let kind = transport.kind();
                ui.colored_label(transport_color(kind), "●");
                ui.colored_label(
                    transport_color(kind),
                    format!("Connected via {}", kind.to_uppercase()),
                );
            } else if self.control.is_some() {
                ui.colored_label(Color32::YELLOW, "●");
                ui.spinner();
                ui.label("Connecting…");
            } else {
                ui.colored_label(Color32::LIGHT_RED, "●");
                ui.colored_label(
                    Color32::LIGHT_RED,
                    if self.connection_error {
                        "Connection error"
                    } else {
                        "Not connected"
                    },
                );
            }
        });
        ui.separator();
        ui.horizontal(|ui| {
            ui.heading(RichText::new("GATEWAY TRANSPORTS").color(ACCENT));
            if ui
                .add_enabled(
                    !self.transports_loading
                        && self.transport_operation.is_none()
                        && !self.return_in_progress,
                    egui::Button::new("Refresh"),
                )
                .clicked()
            {
                self.start_transport_discovery();
            }
            if self.transports_loading {
                ui.spinner();
            }
        });
        let return_interval = self
            .connection
            .as_ref()
            .map_or(30, |settings| settings.gateway_up_return_sec);
        ui.small(format!(
            "Transport status is monitored every {return_interval} seconds."
        ));
        ui.small("Disabling closes active sessions; re-enabling does not restore them.");
        if self.transports.is_empty() && !self.transports_loading {
            ui.label("Transport state is unavailable.");
        }
        let mut transport_statuses = self.transports.clone();
        transport_statuses.sort_by_key(|status| match status.transport.as_str() {
            "quic" => 0,
            "tls" => 1,
            "tcp" => 2,
            _ => 3,
        });
        let mut transport_change = None;
        ui.add_enabled_ui(
            !self.transports_loading
                && self.transport_operation.is_none()
                && !self.return_in_progress,
            |ui| {
                for status in transport_statuses {
                    let mut enabled = status.enabled;
                    if ui
                        .checkbox(&mut enabled, status.transport.to_uppercase())
                        .changed()
                    {
                        transport_change = Some((status.transport, enabled));
                    }
                }
            },
        );
        if let Some((transport, enabled)) = transport_change {
            self.set_transport_enabled(transport, enabled);
        }
        if let Some(connection) = &self.connection {
            ui.label(format!("Gateway: {}", connection.host));
            ui.label("QUIC → TLS → TCP (3 attempts each)");
            if !connection.ca_file.is_empty() {
                ui.label(format!("CA file: {}", connection.ca_file));
            }
        } else {
            ui.colored_label(
                Color32::LIGHT_RED,
                "Create and configure .env, then restart the app.",
            );
        }
        ui.small("Connection settings are loaded from .env at startup.");
    }

    fn request_tab(&mut self, ui: &mut egui::Ui) {
        ui.heading("Request composer");
        let mut refresh_methods = false;
        let previous_method = self.method.clone();
        ui.horizontal(|ui| {
            ui.label("Method");
            egui::ComboBox::from_id_salt("request-method")
                .selected_text(if self.method.is_empty() {
                    "No implemented methods available"
                } else {
                    &self.method
                })
                .show_ui(ui, |ui| {
                    for method in &self.methods {
                        ui.selectable_value(&mut self.method, method.name.clone(), &method.name);
                    }
                });
            if ui
                .add_enabled(!self.methods_loading, egui::Button::new("Refresh methods"))
                .clicked()
            {
                refresh_methods = true;
            }
            if self.methods_loading {
                ui.spinner();
            }
        });
        if self.method != previous_method
            && let Some(method) = self
                .methods
                .iter()
                .find(|method| method.name == self.method)
        {
            self.params = method.example_params.clone();
        }
        if refresh_methods {
            self.start_method_discovery();
        }
        if let Some(error) = &self.methods_error {
            ui.colored_label(
                Color32::LIGHT_RED,
                format!("Could not load methods: {error}"),
            );
        } else if !self.methods_loading && self.methods.is_empty() {
            ui.label("The Control Plane reported no implemented methods.");
        }
        if let Some(warning) = self
            .methods
            .iter()
            .find(|method| method.name == self.method)
            .and_then(|method| method.metadata_warning.as_deref())
        {
            ui.colored_label(Color32::YELLOW, warning);
        }
        if let Some(schema) = self
            .methods
            .iter()
            .find(|method| method.name == self.method)
            .and_then(|method| method.params_schema.as_ref())
        {
            egui::CollapsingHeader::new("Parameter schema from Control Plane").show(ui, |ui| {
                ui.monospace(schema.to_string());
            });
        }
        ui.label("Parameters: JSON object or array; empty = omit params");
        ui.add(
            TextEdit::multiline(&mut self.params)
                .code_editor()
                .desired_rows(7)
                .desired_width(f32::INFINITY),
        );
        let can_send =
            self.client.is_some() && self.methods.iter().any(|method| method.name == self.method);
        ui.add_enabled_ui(can_send, |ui| {
            ui.horizontal_wrapped(|ui| {
                if ui.button("Call").clicked() {
                    self.send_request(false, false);
                }
                if ui.button("Notify (no response)").clicked() {
                    self.send_request(true, false);
                }
                ui.add(egui::DragValue::new(&mut self.parallel_count).range(1..=64));
                if ui.button("Parallel calls").clicked() {
                    self.send_request(false, true);
                }
                if self.method == "greet" && ui.button("Typed greet").clicked() {
                    #[derive(Deserialize, serde::Serialize)]
                    struct GreetingParams {
                        name: String,
                    }
                    let params = match serde_json::from_str::<GreetingParams>(&self.params) {
                        Ok(params) => params,
                        Err(error) => {
                            self.error(format!("Invalid typed greet example params: {error}"));
                            return;
                        }
                    };
                    let client = self.client.as_ref().expect("enabled with client").0.clone();
                    self.run("Typed request<P, R>", async move {
                        #[derive(Deserialize, serde::Serialize)]
                        struct Greeting {
                            message: String,
                            source: String,
                        }
                        let result: Greeting = client.request("greet", params).await?;
                        Ok(serde_json::to_value(result)?)
                    });
                }
                if self.method == "info"
                    && self
                        .methods
                        .iter()
                        .find(|method| method.name == "info")
                        .is_some_and(|method| method.example_params.is_empty())
                    && ui.button("Typed info (no params)").clicked()
                {
                    let client = self.client.as_ref().expect("enabled with client").0.clone();
                    self.run("request_without_params", async move {
                        Ok(client.request_without_params::<Value>("info").await?)
                    });
                }
            });
        });
        ui.small("Timeout: configure Policy. Cancellation drops the local future; it cannot undo server-side effects.");
    }

    fn batch_tab(&mut self, ui: &mut egui::Ui) {
        ui.heading("Mixed batch");
        ui.label("Each entry: method, optional structured params, optional notification: true. IDs are generated by the library.");
        ui.add(
            TextEdit::multiline(&mut self.batch)
                .code_editor()
                .desired_rows(13)
                .desired_width(f32::INFINITY),
        );
        ui.horizontal(|ui| {
            if ui.add_enabled(self.client.is_some(), egui::Button::new("Send batch")).clicked() {
                match requests::batch(&self.batch) {
                    Ok(batch) => {
                        let client = self.client.as_ref().expect("enabled with client").0.clone();
                        self.run("Batch", async move { requests::execute_batch(&client, batch).await });
                    }
                    Err(error) => self.error(error),
                }
            }
            if ui.button("Reset example").clicked() { self.batch = requests::DEFAULT_BATCH.into(); }
            if ui.button("Notifications only").clicked() { self.batch = r#"[{"method":"echo","params":{"event":"one"},"notification":true},{"method":"echo","notification":true}]"#.into(); }
        });
        ui.small("Results preserve input call order, not response arrival order. Notifications are omitted. Per-item remote errors remain visible.");
    }

    fn policy_tab(&mut self, ui: &mut egui::Ui) {
        ui.heading("Runtime policy");
        ui.label("Used at connect time. Apply updates the open session without reconnecting.");
        egui::Grid::new("policy").num_columns(2).show(ui, |ui| {
            for (label, value) in [
                ("Request timeout (ms)", &mut self.policy.timeout_ms),
                ("Ping interval (ms)", &mut self.policy.ping_ms),
                ("Reconnect cooldown (ms)", &mut self.policy.cooldown_ms),
                ("Maximum backoff (ms)", &mut self.policy.max_backoff_ms),
                ("Reconnect jitter (ms)", &mut self.policy.jitter_ms),
                (
                    "Recovery budget (ms), 0 = unlimited",
                    &mut self.policy.max_elapsed_ms,
                ),
            ] {
                ui.label(label);
                ui.add(egui::DragValue::new(value));
                ui.end_row();
            }
            ui.label("Missed pongs before recovery");
            ui.add(egui::DragValue::new(&mut self.policy.missed_pongs));
            ui.end_row();
            ui.label("Failed restore attempts before reconnect");
            ui.add(egui::DragValue::new(&mut self.policy.recovery_attempts));
            ui.end_row();
            ui.label("Outbound queue capacity");
            ui.add_enabled(
                self.client.is_none(),
                egui::DragValue::new(&mut self.policy.queue_capacity),
            );
            ui.end_row();
            ui.label("Maximum in-flight requests");
            ui.add(egui::DragValue::new(&mut self.policy.max_in_flight));
            ui.end_row();
        });
        ui.checkbox(
            &mut self.policy.restore_first,
            "Try restoring before reconnecting",
        );
        ui.small("Queue capacity is fixed at connect time. The in-flight limit can only grow while connected. Disconnect to reduce it.");
        if ui
            .add_enabled(
                self.client.is_some() && self.tasks.is_empty(),
                egui::Button::new("Apply policy"),
            )
            .clicked()
        {
            match self.policy.config() {
                Ok(_) => {
                    let client = self.client.as_ref().expect("enabled with client").0.clone();
                    let policy = self.policy.clone();
                    let connection = self.connection.clone();
                    self.run("Apply policy", async move {
                        policy.apply(&client).await?;
                        if let Some(connection) = connection {
                            connection.set_recovery_attempts(policy.recovery_attempts);
                        }
                        Ok(json!({"policy": "applied"}))
                    });
                }
                Err(error) => self.error(error),
            }
        }
        ui.separator();
        ui.heading("Transport controls");
        ui.add_enabled_ui(self.client.is_some(), |ui| {
            ui.horizontal_wrapped(|ui| {
                for (label, operation) in [("Ping / pong", 0), ("Restore", 1), ("Reconnect", 2)] {
                    if ui.button(label).clicked() {
                        let transport =
                            self.client.as_ref().expect("enabled with client").1.clone();
                        self.run(label, async move {
                            tokio::time::timeout(Duration::from_secs(10), async {
                                match operation {
                                    0 => transport.transport_ping().await,
                                    1 => transport.restore().await,
                                    _ => transport.reconnect().await,
                                }
                            })
                            .await
                            .context("Transport control timed out")??;
                            Ok(json!({"transport_control": label, "status": "ok"}))
                        });
                    }
                }
            });
        });
        ui.small("Periodic pings start automatically after connection. After the missed-pong threshold, the client retries restore before reconnecting with gateway fallback. RPC requests are not replayed.");
    }

    fn metrics_tab(&self, ui: &mut egui::Ui) {
        ui.heading("Live client metrics");
        if let Some((client, _)) = &self.client {
            let metrics = client.metrics();
            egui::Grid::new("metrics").striped(true).show(ui, |ui| {
                for (label, value) in [
                    ("Active requests", metrics.active_requests.to_string()),
                    (
                        "Outbound queue depth",
                        metrics.outbound_queue_depth.to_string(),
                    ),
                    ("RPC calls", metrics.rpc_calls_total.to_string()),
                    ("RPC errors", metrics.rpc_errors_total.to_string()),
                    ("Timeouts", metrics.request_timeouts_total.to_string()),
                    (
                        "Cancelled requests",
                        metrics.cancelled_requests_total.to_string(),
                    ),
                    ("Recovery attempts", metrics.reconnect_attempts.to_string()),
                    ("Missed pong streak", metrics.missed_pong_streak.to_string()),
                ] {
                    ui.label(label);
                    ui.monospace(value);
                    ui.end_row();
                }
            });
            ui.separator();
            ui.label("Applied library config (not unsaved form values)");
            ui.monospace(format!("{:#?}", client.config()));
        } else {
            ui.label("Connect to inspect the real library counters.");
        }
        ui.small("Session open is not a reachability guarantee. Observe missed pongs and recovery events after a network failure.");
    }

    fn log_tab(&mut self, ui: &mut egui::Ui) {
        ui.heading("Client event log");
        let mut debug = self.debug.load(Ordering::Relaxed);
        if ui
            .checkbox(
                &mut debug,
                "Include DEBUG / TRACE payloads (may contain secrets)",
            )
            .changed()
        {
            self.debug.store(debug, Ordering::Relaxed);
        }
        ui.small("In-memory only; last 500 events. Copy/export may expose parameters, responses, credentials or personal data.");
        ui.horizontal(|ui| {
            ui.label("Filter");
            ui.text_edit_singleline(&mut self.log_filter);
            ui.checkbox(&mut self.only_errors, "WARN / ERROR");
            if ui.button("Clear").clicked() {
                self.logs.clear();
            }
            if ui.button("Copy visible").clicked() {
                ui.ctx().copy_text(self.visible_logs());
            }
        });
        egui::ScrollArea::vertical()
            .id_salt("events")
            .max_height(430.0)
            .stick_to_bottom(true)
            .show(ui, |ui| {
                for entry in &self.logs {
                    if self.matches_log(entry) {
                        let color = if entry.level <= Level::WARN {
                            Color32::LIGHT_RED
                        } else {
                            Color32::LIGHT_GRAY
                        };
                        ui.label(
                            RichText::new(format!(
                                "{:8.2}s {:5} {}",
                                entry.seconds, entry.level, entry.message
                            ))
                            .monospace()
                            .color(color),
                        );
                    }
                }
            });
    }

    fn matches_log(&self, entry: &LogEntry) -> bool {
        (!self.only_errors || entry.level <= Level::WARN)
            && (self.log_filter.is_empty()
                || entry
                    .message
                    .to_lowercase()
                    .contains(&self.log_filter.to_lowercase()))
    }

    fn visible_logs(&self) -> String {
        self.logs
            .iter()
            .filter(|entry| self.matches_log(entry))
            .map(|entry| format!("{:.2}s {} {}", entry.seconds, entry.level, entry.message))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn show_backends_window(&mut self, ctx: &egui::Context) {
        if !self.backends_open {
            return;
        }
        let mut refresh = false;
        let mut add = false;
        let mut toggle = None;
        egui::Window::new("Backends")
            .open(&mut self.backends_open)
            .default_width(760.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.heading("Gateway backends");
                    if self.backends_loading {
                        ui.spinner();
                    }
                    if ui
                        .add_enabled(
                            self.connection.is_some()
                                && !self.backends_loading
                                && self.backend_operation.is_none(),
                            egui::Button::new("Refresh"),
                        )
                        .clicked()
                    {
                        refresh = true;
                    }
                });
                ui.small("Registration does not launch a backend process. Disabling one stops new routing and lets in-flight requests finish; it does not stop the process.");
                ui.separator();

                ui.heading("Register backend");
                egui::Grid::new("backend-registration-grid")
                    .num_columns(2)
                    .spacing([12.0, 8.0])
                    .show(ui, |ui| {
                        ui.label("ID");
                        ui.text_edit_singleline(&mut self.backend_id_draft);
                        ui.end_row();
                        ui.label("Host / IP");
                        ui.text_edit_singleline(&mut self.backend_host_draft);
                        ui.end_row();
                        ui.label("Ports (JSON)");
                        ui.text_edit_singleline(&mut self.backend_ports_draft);
                        ui.end_row();
                        ui.label("Pool size");
                        ui.text_edit_singleline(&mut self.backend_pool_size_draft);
                        ui.end_row();
                    });
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(
                            self.connection.is_some() && self.backend_operation.is_none(),
                            egui::Button::new("Add backend"),
                        )
                        .clicked()
                    {
                        add = true;
                    }
                });
                ui.small(r#"Ports must be a JSON object, for example {"tcp":18081}."#);
                ui.separator();

                ui.heading(format!("Registered backends ({})", self.backends.len()));
                if self.backends.is_empty() && !self.backends_loading {
                    ui.label("No backends reported by the Control Plane.");
                }
                egui::ScrollArea::vertical()
                    .max_height(360.0)
                    .show(ui, |ui| {
                        for backend in &self.backends {
                            ui.group(|ui| {
                                ui.horizontal(|ui| {
                                    ui.strong(&backend.id);
                                    ui.label(&backend.host);
                                    ui.label(
                                        RichText::new(if backend.enabled {
                                            "enabled"
                                        } else {
                                            "disabled"
                                        })
                                        .color(if backend.enabled {
                                            ACCENT
                                        } else {
                                            Color32::LIGHT_RED
                                        }),
                                    );
                                    if ui
                                        .add_enabled(
                                            self.connection.is_some()
                                                && self.backend_operation.is_none(),
                                            egui::Button::new(if backend.enabled {
                                                "Disable"
                                            } else {
                                                "Enable"
                                            }),
                                        )
                                        .clicked()
                                    {
                                        toggle = Some((backend.id.clone(), !backend.enabled));
                                    }
                                });
                                let ports = backend
                                    .ports
                                    .iter()
                                    .map(|(transport, port)| format!("{transport}: {port}"))
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                ui.label(format!("Ports: {ports}"));
                                if let Some(pool_size) = backend.pool_size {
                                    ui.label(format!("Pool size: {pool_size}"));
                                }
                                ui.label(format!("Active requests: {}", backend.active_requests));
                                if let Some(pools) = &backend.pools {
                                    ui.label(format!("Pools: {pools}"));
                                } else {
                                    ui.label("Pool state: not reported");
                                }
                            });
                        }
                    });
                if let Some(operation) = &self.backend_operation {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label(operation);
                    });
                }
                if let Some(error) = &self.backends_error {
                    ui.colored_label(Color32::LIGHT_RED, error);
                }
            });
        if refresh {
            self.start_backends_discovery();
        }
        if add {
            self.add_backend();
        }
        if let Some((id, enabled)) = toggle {
            self.set_backend_enabled(id, enabled);
        }
    }
}

impl eframe::App for DesktopApp {
    fn update(&mut self, ctx: &egui::Context, _: &mut eframe::Frame) {
        self.drain();
        let transport_interval = self
            .connection
            .as_ref()
            .map(|settings| Duration::from_secs(settings.gateway_up_return_sec))
            .unwrap_or(Duration::from_secs(30));
        if self.transports_initialized
            && self.transport_operation.is_none()
            && !self.return_in_progress
            && !self.transports_loading
            && self.last_transport_poll.elapsed() >= transport_interval
        {
            self.start_transport_discovery();
        }
        if self.backends_open
            && !self.backends_loading
            && self.backend_operation.is_none()
            && self.backends_last_poll.elapsed() >= Duration::from_secs(5)
        {
            self.start_backends_discovery();
        }
        if self.client.is_some()
            && let Some(result) = self.map.poll(
                self.runtime
                    .as_ref()
                    .expect("runtime exists until shutdown"),
            )
        {
            match result {
                Ok(message) => self.log(Level::INFO, message),
                Err(error) => self.error(error),
            }
        }
        ctx.request_repaint_after(Duration::from_millis(150));
        let mut open_settings = false;
        let mut open_backends = false;
        egui::TopBottomPanel::top("header").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading(RichText::new("JSON-RPC-2").color(ACCENT));
                ui.label("DESKTOP CLIENT LAB");
                ui.separator();
                if let Some((_, transport)) = &self.client {
                    ui.label(format!(
                        "{} / {}",
                        transport.kind(),
                        transport
                            .peer()
                            .unwrap_or_else(|| "peer not reported".into())
                    ));
                } else if self.control.is_some() {
                    ui.label("Connecting...");
                } else if self.connection.is_none() {
                    ui.label("Gateway configuration required");
                } else {
                    ui.label("No open session");
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Backends").clicked() {
                        open_backends = true;
                    }
                    ui.menu_button("Settings", |ui| {
                        if ui.button("Application settings…").clicked() {
                            open_settings = true;
                            ui.close();
                        }
                    });
                });
            });
        });
        if open_settings {
            self.open_settings();
        }
        if open_backends {
            self.backends_open = true;
            self.start_backends_discovery();
        }
        self.show_backends_window(ctx);
        egui::TopBottomPanel::bottom("status").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.label(&self.status);
                if !self.tasks.is_empty() {
                    ui.spinner();
                    ui.label(format!("{} active UI operations", self.tasks.len()));
                }
            });
        });
        egui::SidePanel::left("connection")
            .default_width(320.0)
            .min_width(280.0)
            .max_width(450.0)
            .show(ctx, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| self.sidebar(ui));
            });
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.horizontal_wrapped(|ui| {
                for (tab, label) in [
                    (Tab::Request, "Request"),
                    (Tab::Batch, "Batch"),
                    (Tab::Policy, "Policy & liveness"),
                    (Tab::Metrics, "Metrics"),
                    (Tab::Log, "Event log"),
                    (Tab::Map, "Мапа"),
                ] {
                    ui.selectable_value(&mut self.tab, tab, label);
                }
            });
            ui.separator();
            egui::ScrollArea::vertical()
                .id_salt("content")
                .show(ui, |ui| {
                    match self.tab {
                        Tab::Request => self.request_tab(ui),
                        Tab::Batch => self.batch_tab(ui),
                        Tab::Policy => self.policy_tab(ui),
                        Tab::Metrics => self.metrics_tab(ui),
                        Tab::Log => self.log_tab(ui),
                        Tab::Map => self.map.show(ui),
                    }
                    if !self.tasks.is_empty() {
                        ui.separator();
                        ui.horizontal(|ui| {
                            ui.label("Running operations");
                            if ui.button("Cancel all").clicked() {
                                self.cancel_all();
                            }
                        });
                        let mut cancel = None;
                        for (id, (label, _)) in &self.tasks {
                            ui.horizontal(|ui| {
                                ui.label(format!("#{id} {label}"));
                                if ui.small_button("Cancel").clicked() {
                                    cancel = Some(*id);
                                }
                            });
                        }
                        if let Some(id) = cancel
                            && let Some((label, task)) = self.tasks.remove(&id)
                        {
                            task.abort();
                            self.log(
                                Level::WARN,
                                format!(
                                    "#{id} {label}: cancelled locally; server work may continue"
                                ),
                            );
                        }
                    }
                    ui.separator();
                    ui.horizontal(|ui| {
                        ui.heading(if self.response_error {
                            "Error"
                        } else {
                            "Result"
                        });
                        if ui.button("Copy JSON / error").clicked() {
                            ctx.copy_text(self.response.clone());
                        }
                    });
                    if self.response_error {
                        ui.colored_label(Color32::LIGHT_RED, "Operation failed");
                    }
                    ui.add(
                        TextEdit::multiline(&mut self.response)
                            .code_editor()
                            .interactive(false)
                            .desired_rows(12)
                            .desired_width(f32::INFINITY),
                    );
                });
        });
        if self.settings_open {
            let mut save_settings = false;
            let mut close_settings = false;
            egui::Window::new("Application settings (.env)")
                .open(&mut self.settings_open)
                .resizable(true)
                .show(ctx, |ui| {
                    ui.label("Changes are validated and saved to .env before reconnecting.");
                    egui::Grid::new("settings-grid")
                        .num_columns(2)
                        .spacing([12.0, 8.0])
                        .show(ui, |ui| {
                            let draft = self
                                .settings_draft
                                .get_or_insert_with(SettingsDraft::default);
                            ui.label("Gateway host");
                            ui.text_edit_singleline(&mut draft.host);
                            ui.end_row();
                            ui.label("Gateway port");
                            ui.text_edit_singleline(&mut draft.port);
                            ui.end_row();
                            ui.label("TLS server name");
                            ui.text_edit_singleline(&mut draft.server_name);
                            ui.end_row();
                            ui.label("CA file");
                            ui.text_edit_singleline(&mut draft.ca_file);
                            ui.end_row();
                            ui.label("Connect timeout (ms)");
                            ui.text_edit_singleline(&mut draft.connect_timeout_ms);
                            ui.end_row();
                            ui.label("Retry delay (ms)");
                            ui.text_edit_singleline(&mut draft.retry_delay_ms);
                            ui.end_row();
                            ui.label("Control Plane port");
                            ui.text_edit_singleline(&mut draft.control_port);
                            ui.end_row();
                            ui.label("Control Plane timeout (ms)");
                            ui.text_edit_singleline(&mut draft.control_timeout_ms);
                            ui.end_row();
                            ui.label("Ping interval (ms)");
                            ui.text_edit_singleline(&mut draft.ping_ms);
                            ui.end_row();
                            ui.label("Missed pongs");
                            ui.text_edit_singleline(&mut draft.missed_pongs);
                            ui.end_row();
                            ui.label("Recovery attempts");
                            ui.text_edit_singleline(&mut draft.recovery_attempts);
                            ui.end_row();
                            ui.label("GATEWAY_UP_RETURN_SEC (s)");
                            ui.text_edit_singleline(&mut draft.gateway_up_return_sec);
                            ui.end_row();
                        });
                    if let Some(error) = &self.settings_error {
                        ui.colored_label(Color32::LIGHT_RED, error);
                    }
                    ui.horizontal(|ui| {
                        if ui.button("Save and reconnect").clicked() {
                            save_settings = true;
                        }
                        if ui.button("Cancel").clicked() {
                            close_settings = true;
                        }
                    });
                });
            if save_settings {
                self.save_settings();
            } else if close_settings || !self.settings_open {
                self.settings_open = false;
                self.settings_draft = None;
                self.settings_error = None;
            }
        }
    }
}

impl Drop for DesktopApp {
    fn drop(&mut self) {
        for (_, (_, task)) in std::mem::take(&mut self.tasks) {
            task.abort();
        }
        if let Some(task) = self.control.take() {
            task.abort();
        }
        if let Some(task) = self.method_discovery.take() {
            task.abort();
        }
        if let Some(runtime) = self.runtime.take() {
            runtime.block_on(async {
                if let Some((client, _)) = self.client.take() {
                    match tokio::time::timeout(Duration::from_secs(3), client.close()).await {
                        Ok(Ok(())) => {}
                        result => eprintln!("Client shutdown failed: {result:?}"),
                    }
                }
            });
            runtime.shutdown_timeout(Duration::from_secs(2));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_backend_registration_form() {
        let registration =
            parse_backend_registration(" backend-133 ", " 192.168.1.133 ", r#"{"tcp":18081}"#, "4")
                .unwrap();
        assert_eq!(registration.id, "backend-133");
        assert_eq!(registration.host, "192.168.1.133");
        assert_eq!(registration.ports["tcp"], 18081);
        assert_eq!(registration.pool_size, 4);

        assert!(parse_backend_registration("", "192.168.1.133", r#"{"tcp":18081}"#, "4").is_err());
        assert!(parse_backend_registration("backend", "", r#"{"tcp":18081}"#, "4").is_err());
        assert!(parse_backend_registration("backend", "host", r#"{"tcp":0}"#, "4").is_err());
        assert!(parse_backend_registration("backend", "host", r#"{"tcp":18081}"#, "0").is_err());
    }
}
