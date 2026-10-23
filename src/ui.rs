use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Write as _,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, anyhow};
use eframe::egui::{self, Color32, RichText, TextEdit};
use json_rpc_2::TcpCongestionControl;
use json_rpc_2_tester::{
    connection::{self, Client, ConnectionSettings, RuntimeSettings, TRANSPORTS, Transport},
    demo, requests,
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
const MAX_LOG_ENTRIES: usize = 500;
const MAX_TEXT_BYTES: usize = 32 * 1024;

enum Event {
    Connected(Result<(Client, Transport)>),
    Disconnected(Result<()>),
    DemoStarted(Result<demo::DemoServer>),
    DemoStopped(Result<()>),
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
}

struct LogEntry {
    seconds: f64,
    level: Level,
    message: String,
}

pub struct DesktopApp {
    runtime: Option<Runtime>,
    tx: mpsc::Sender<Event>,
    rx: mpsc::Receiver<Event>,
    trace_rx: mpsc::Receiver<TraceLine>,
    debug: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
    connection: ConnectionSettings,
    policy: RuntimeSettings,
    client: Option<(Client, Transport)>,
    session: String,
    control: Option<JoinHandle<()>>,
    server: Option<demo::DemoServer>,
    server_control: Option<JoinHandle<()>>,
    listeners: Vec<(String, u16)>,
    tasks: BTreeMap<u64, (String, JoinHandle<()>)>,
    next_id: u64,
    started: Instant,
    logs: VecDeque<LogEntry>,
    tab: Tab,
    method: String,
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
        Ok(Self {
            runtime: Some(runtime),
            tx,
            rx,
            trace_rx,
            debug,
            dropped,
            connection: Default::default(),
            policy: Default::default(),
            client: None,
            session: String::new(),
            control: None,
            server: None,
            server_control: None,
            listeners: Vec::new(),
            tasks: BTreeMap::new(),
            next_id: 1,
            started: Instant::now(),
            logs: VecDeque::new(),
            tab: Tab::Request,
            method: "echo".into(),
            params: "{\n  \"message\": \"Hello, desktop!\"\n}".into(),
            parallel_count: 8,
            batch: requests::DEFAULT_BATCH.into(),
            response: "Start the local demo server, select its endpoint, then connect.".into(),
            response_error: false,
            status: "Ready".into(),
            log_filter: String::new(),
            only_errors: false,
        })
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
                Event::Connected(result) => {
                    self.control = None;
                    match result {
                        Ok(pair) => {
                            self.session = format!(
                                "{} / {}",
                                self.connection.choice.id,
                                pair.1.peer().unwrap_or_else(|| "peer not reported".into())
                            );
                            self.client = Some(pair);
                            self.status = "Session open".into();
                            self.log(Level::INFO, format!("Session opened: {}", self.session));
                        }
                        Err(error) => self.error(format!("{error:#}")),
                    }
                }
                Event::Disconnected(result) => {
                    self.control = None;
                    self.session.clear();
                    match result {
                        Ok(()) => {
                            self.status = "Disconnected".into();
                            self.log(Level::INFO, "Session closed");
                        }
                        Err(error) => self.error(error),
                    }
                }
                Event::DemoStarted(result) => {
                    self.server_control = None;
                    match result {
                        Ok(server) => {
                            self.listeners = server
                                .running
                                .listeners()
                                .map(|(kind, addr)| (kind.id().to_owned(), addr.port()))
                                .collect();
                            self.server = Some(server);
                            self.status =
                                "Local demo server running; choose an endpoint below".into();
                            self.log(Level::INFO, format!("Demo listeners: {:?}", self.listeners));
                            if self.client.is_none() && self.control.is_none() {
                                self.use_demo("tcp");
                            }
                        }
                        Err(error) => self.error(error),
                    }
                }
                Event::DemoStopped(result) => {
                    self.server_control = None;
                    self.listeners.clear();
                    match result {
                        Ok(()) => self.log(Level::INFO, "Local demo server stopped"),
                        Err(error) => self.error(error),
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

    fn use_demo(&mut self, id: &str) {
        if let Some((_, port)) = self.listeners.iter().find(|(name, _)| name == id)
            && let Some(choice) = TRANSPORTS.iter().find(|choice| choice.id == id)
        {
            self.connection.choice = *choice;
            self.connection.host = "127.0.0.1".into();
            self.connection.port = *port;
            if choice.tls()
                && let Some(server) = &self.server
            {
                self.connection.ca_file.clear();
                self.connection.ca_pem = server.ca_pem.clone();
                self.connection.server_name = "localhost".into();
            }
        }
    }

    fn start_connection(&mut self) {
        if let Err(error) = self
            .connection
            .validate()
            .and_then(|_| self.policy.config().map(|_| ()))
        {
            self.error(error);
            return;
        }
        let settings = self.connection.clone();
        let policy = self.policy.clone();
        let tx = self.tx.clone();
        self.status = "Connecting...".into();
        self.control = Some(self.rt().spawn(async move {
            let result = connection::connect(settings, policy).await;
            if let Err(error) = tx.send(Event::Connected(result)).await
                && let Event::Connected(Ok((client, _))) = error.0
            {
                let _ = client.close().await;
            }
        }));
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

    fn disconnect(&mut self) {
        self.cancel_all();
        if let Some((client, _)) = self.client.take() {
            let tx = self.tx.clone();
            self.status = "Closing session...".into();
            self.control = Some(self.rt().spawn(async move {
                let result = tokio::time::timeout(Duration::from_secs(5), client.close())
                    .await
                    .context("Closing session timed out")
                    .and_then(|r| r.map_err(Into::into));
                let _ = tx.send(Event::Disconnected(result)).await;
            }));
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
        ui.label("Direct Rust client / no FFI");
        ui.separator();
        ui.add_enabled_ui(self.client.is_none() && self.control.is_none(), |ui| {
            let previous = self.connection.choice;
            egui::ComboBox::from_label("Transport")
                .selected_text(self.connection.choice.id)
                .show_ui(ui, |ui| {
                    for choice in TRANSPORTS {
                        let label = if choice.unavailable().is_some() { format!("{} (unavailable)", choice.id) } else { choice.id.into() };
                        ui.selectable_value(&mut self.connection.choice, *choice, label);
                    }
                });
            if previous != self.connection.choice {
                self.connection.port = self.connection.choice.port;
                if self.connection.choice.broker() {
                    let mqtt = self.connection.choice.id.ends_with("mqtt");
                    self.connection.broker_url = if mqtt { "mqtt://127.0.0.1:1883" } else { "amqp://127.0.0.1:5672/%2f" }.into();
                    self.connection.broker_route = if mqtt { "json-rpc-2/requests" } else { "json-rpc-2.requests" }.into();
                }
                let id = self.connection.choice.id;
                self.use_demo(id);
            }
            if let Some(reason) = self.connection.choice.unavailable() {
                ui.colored_label(Color32::YELLOW, reason);
            }
            if self.connection.choice.broker() {
                field(ui, "Broker URL (may include credentials)", &mut self.connection.broker_url);
                field(ui, "Request topic / queue", &mut self.connection.broker_route);
                number(ui, "Broker operation timeout (ms)", &mut self.connection.broker_operation_ms);
                number(ui, "Broker response timeout (ms)", &mut self.connection.broker_response_ms);
                ui.small("MQTT/AMQP require an external broker AND an RPC consumer (gateway-iot). WS variants use a private envelope.");
            } else {
                field(ui, "Host / IP", &mut self.connection.host);
                number(ui, "Port", &mut self.connection.port);
            }
            number(ui, "Connect timeout (ms)", &mut self.connection.connect_timeout_ms);
            if matches!(self.connection.choice.id, "ws-http1" | "ws-http3" | "ws-tls") {
                field(ui, "WebSocket path", &mut self.connection.ws_path);
            }
            if self.connection.choice.tls() || self.connection.choice.id.ends_with("mqtt") {
                ui.collapsing("Certificate trust", |ui| {
                    if self.connection.choice.tls() {
                        field(ui, "TLS server name (verified)", &mut self.connection.server_name);
                    }
                    field(ui, "CA PEM file (takes priority over text)", &mut self.connection.ca_file);
                    ui.label("Or paste public CA PEM");
                    ui.add(TextEdit::multiline(&mut self.connection.ca_pem).desired_rows(5).desired_width(f32::INFINITY));
                    ui.small("No insecure TLS bypass. AMQP uses system roots; custom CA applies to MQTT and direct TLS transports.");
                });
            }
            if matches!(self.connection.choice.id, "srt" | "ws-srt") {
                ui.label("SRT passphrase (10..=79 bytes)");
                ui.add(TextEdit::singleline(&mut self.connection.srt_passphrase).password(true));
            }
            if matches!(self.connection.choice.id, "udplite" | "ws-udplite") {
                number(ui, "Send checksum coverage", &mut self.connection.send_coverage);
                number(ui, "Receive checksum coverage", &mut self.connection.receive_coverage);
                ui.small("0 = full checksum, otherwise >= 8");
            }
            if matches!(self.connection.choice.id, "tcp" | "tls" | "http1" | "ws-tcp" | "ws-http1" | "ws-tls") {
                ui.collapsing("TCP socket tuning", |ui| {
                    ui.checkbox(&mut self.connection.tcp.no_delay, "TCP_NODELAY");
                    ui.checkbox(&mut self.connection.tcp.fast_open, "TCP Fast Open");
                    let mut bbr = self.connection.tcp.congestion_control == TcpCongestionControl::Bbr;
                    ui.checkbox(&mut bbr, "BBR congestion control (requires OS support)");
                    self.connection.tcp.congestion_control = if bbr { TcpCongestionControl::Bbr } else { TcpCongestionControl::SystemDefault };
                });
            }
        });
        ui.add_space(8.0);
        if self.control.is_some() {
            ui.spinner();
        } else if self.client.is_some() {
            if ui.button("Disconnect").clicked() {
                self.disconnect();
            }
        } else if ui
            .add_enabled(
                self.connection.choice.unavailable().is_none(),
                egui::Button::new("Connect").fill(ACCENT.gamma_multiply(0.3)),
            )
            .clicked()
        {
            self.start_connection();
        }
        ui.separator();
        ui.heading("LOCAL SANDBOX");
        if self.server_control.is_some() {
            ui.spinner();
        } else if self.server.is_none() {
            if ui.button("Start demo server").clicked() {
                let tx = self.tx.clone();
                self.server_control = Some(self.rt().spawn(async move {
                    let result = demo::start().await;
                    if let Err(error) = tx.send(Event::DemoStarted(result)).await
                        && let Event::DemoStarted(Ok(server)) = error.0
                    {
                        let _ = server.close().await;
                    }
                }));
            }
        } else {
            ui.label("Running on 127.0.0.1 / dynamic ports");
            let mut selected = None;
            ui.add_enabled_ui(self.client.is_none() && self.control.is_none(), |ui| {
                egui::ComboBox::from_label("Use demo endpoint")
                    .selected_text("Select transport")
                    .show_ui(ui, |ui| {
                        for (id, port) in &self.listeners {
                            if ui
                                .selectable_label(false, format!("{id} : {port}"))
                                .clicked()
                            {
                                selected = Some(id.clone());
                            }
                        }
                    });
            });
            if let Some(id) = selected {
                self.use_demo(&id);
            }
            if ui
                .add_enabled(
                    self.client.is_none() && self.control.is_none(),
                    egui::Button::new("Stop demo server"),
                )
                .clicked()
                && let Some(server) = self.server.take()
            {
                let tx = self.tx.clone();
                self.server_control = Some(self.rt().spawn(async move {
                    let result = server.close().await;
                    let _ = tx.send(Event::DemoStopped(result)).await;
                }));
            }
        }
        ui.small("16 local transports, including verified TLS and QUIC with an ephemeral localhost certificate. Brokers and kernel-specific transports use an external server.");
    }

    fn request_tab(&mut self, ui: &mut egui::Ui) {
        ui.heading("Request composer");
        ui.horizontal_wrapped(|ui| {
            for (label, name, params) in [
                ("Echo", "echo", r#"{"message": "Hello, desktop!"}"#),
                ("Sum", "sum", "[1, 2, 3, 4]"),
                ("Named params", "greet", r#"{"name": "Desktop"}"#),
                ("No params", "info", ""),
                ("Remote error", "fail", "{}"),
                ("Unknown method", "missing.method", ""),
                ("Slow / cancel", "slow", r#"{"ms": 10000}"#),
            ] {
                if ui.button(label).clicked() {
                    self.method = name.into();
                    self.params = params.into();
                }
            }
        });
        field(ui, "Method", &mut self.method);
        ui.label("Parameters: JSON object or array; empty = omit params");
        ui.add(
            TextEdit::multiline(&mut self.params)
                .code_editor()
                .desired_rows(7)
                .desired_width(f32::INFINITY),
        );
        ui.add_enabled_ui(self.client.is_some(), |ui| {
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
                if ui.button("Typed greet").clicked() {
                    let client = self.client.as_ref().expect("enabled with client").0.clone();
                    self.run("Typed request<P, R>", async move {
                        #[derive(Deserialize, serde::Serialize)]
                        struct Greeting {
                            message: String,
                            source: String,
                        }
                        let result: Greeting = client
                            .request("greet", json!({"name": "Rust / serde"}))
                            .await?;
                        Ok(serde_json::to_value(result)?)
                    });
                }
                if ui.button("Typed info (no params)").clicked() {
                    let client = self.client.as_ref().expect("enabled with client").0.clone();
                    self.run("request_without_params", async move {
                        Ok(client.request_without_params::<Value>("info").await?)
                    });
                }
            });
        });
        ui.small("Demo methods require the local sandbox. Timeout: set Policy < slow.ms. Cancellation drops the local future; it cannot undo server side effects.");
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
                ("Ping interval (ms), 0 = off", &mut self.policy.ping_ms),
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
                    self.run("Apply policy", async move {
                        policy.apply(&client).await?;
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
        ui.small("Automatic recovery requires periodic pings. It does not replay RPC requests. Reconnect metrics count library-managed recovery attempts, not these manual controls.");
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
}

impl eframe::App for DesktopApp {
    fn update(&mut self, ctx: &egui::Context, _: &mut eframe::Frame) {
        self.drain();
        ctx.request_repaint_after(Duration::from_millis(150));
        egui::TopBottomPanel::top("header").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading(RichText::new("JSON-RPC-2").color(ACCENT));
                ui.label("DESKTOP CLIENT LAB");
                ui.separator();
                ui.label(if self.client.is_some() {
                    &self.session
                } else {
                    "No open session"
                });
            });
        });
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
        if let Some(task) = self.server_control.take() {
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
                if let Some(server) = self.server.take() {
                    match tokio::time::timeout(Duration::from_secs(3), server.close()).await {
                        Ok(Ok(())) => {}
                        result => eprintln!("Demo shutdown failed: {result:?}"),
                    }
                }
            });
            runtime.shutdown_timeout(Duration::from_secs(2));
        }
    }
}

fn field(ui: &mut egui::Ui, label: &str, value: &mut String) {
    ui.label(label);
    ui.add(TextEdit::singleline(value).desired_width(f32::INFINITY));
}

fn number<N: egui::emath::Numeric>(ui: &mut egui::Ui, label: &str, value: &mut N) {
    ui.horizontal(|ui| {
        ui.label(label);
        ui.add(egui::DragValue::new(value));
    });
}
