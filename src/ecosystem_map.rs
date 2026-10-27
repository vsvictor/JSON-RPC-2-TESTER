use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    time::{Duration, Instant},
};

use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, Sense, Stroke, Vec2};
use json_rpc_2_tester::ecosystem::{Circuit, Components, ControlSession};
use tokio::{runtime::Runtime, task::JoinHandle};

const CLOSED: Color32 = Color32::from_rgb(72, 210, 185);
const OPEN: Color32 = Color32::from_rgb(245, 105, 105);
const HALF_OPEN: Color32 = Color32::from_rgb(245, 192, 90);

pub struct EcosystemMap {
    host: String,
    port: u16,
    timeout_ms: u64,
    transport: String,
    server_name: String,
    ca_pem: String,
    pending: Option<JoinHandle<anyhow::Result<(ControlSession, Components)>>>,
    session: Option<ControlSession>,
    monitoring: bool,
    interval_seconds: u64,
    completed: Option<Instant>,
    inventory: Option<Components>,
    fetched_from: String,
    updated: Option<Instant>,
    error: Option<String>,
    zoom: f32,
}

impl Default for EcosystemMap {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 8500,
            timeout_ms: 5000,
            transport: "tcp".into(),
            server_name: String::new(),
            ca_pem: String::new(),
            pending: None,
            session: None,
            monitoring: false,
            interval_seconds: 5,
            completed: None,
            inventory: None,
            fetched_from: String::new(),
            updated: None,
            error: None,
            zoom: 1.0,
        }
    }
}

impl EcosystemMap {
    #[cfg(test)]
    pub fn configured(host: String, port: u16, timeout_ms: u64) -> Self {
        Self::configured_with_transport(host, port, timeout_ms, "tcp", String::new(), String::new())
    }

    pub fn configured_with_transport(
        host: String,
        port: u16,
        timeout_ms: u64,
        transport: &str,
        server_name: String,
        ca_pem: String,
    ) -> Self {
        let fetched_from = format!("{host}:{port}");
        let mut map = Self::default();
        map.host = host;
        map.port = port;
        map.timeout_ms = timeout_ms;
        map.transport = transport.into();
        map.server_name = server_name;
        map.ca_pem = ca_pem;
        map.monitoring = true;
        map.fetched_from = fetched_from;
        map
    }

    #[cfg(test)]
    pub fn set_active_transport(&mut self, transport: &str) {
        if self.transport.eq_ignore_ascii_case(transport) {
            return;
        }
        self.transport = transport.to_owned();
        if let Some(task) = self.pending.take() {
            task.abort();
        }
        self.session = None;
        self.completed = None;
    }

    fn start_request(&mut self, runtime: &Runtime) {
        let host = self.host.clone();
        let port = self.port;
        let timeout = Duration::from_millis(self.timeout_ms);
        let transport = self.transport.clone();
        let server_name = self.server_name.clone();
        let ca_pem = self.ca_pem.clone();
        let session = self.session.take();
        self.pending = Some(runtime.spawn(async move {
            anyhow::ensure!(!timeout.is_zero(), "Control Plane timeout must be positive");
            tokio::time::timeout(timeout, async {
                let mut session = match session {
                    Some(session) if session.transport_kind() == transport => session,
                    Some(_) => {
                        ControlSession::connect_with_transport(
                            &host,
                            port,
                            &transport,
                            &server_name,
                            &ca_pem,
                        )
                        .await?
                    }
                    None => {
                        ControlSession::connect_with_transport(
                            &host,
                            port,
                            &transport,
                            &server_name,
                            &ca_pem,
                        )
                        .await?
                    }
                };
                let inventory = session.request().await?;
                Ok((session, inventory))
            })
            .await
            .map_err(|_| anyhow::anyhow!("Control Plane request timed out"))?
        }));
    }

    fn stop(&mut self) {
        self.monitoring = false;
        if let Some(task) = self.pending.take() {
            task.abort();
        }
        self.session = None;
        self.completed = None;
    }

    fn refresh_due(&self, now: Instant) -> bool {
        self.monitoring
            && self.pending.is_none()
            && self.completed.is_none_or(|completed| {
                now.duration_since(completed) >= Duration::from_secs(self.interval_seconds)
            })
    }

    pub fn poll(&mut self, runtime: &Runtime) -> Option<Result<String, String>> {
        if self.refresh_due(Instant::now()) {
            self.start_request(runtime);
        }
        if !self.pending.as_ref().is_some_and(|task| task.is_finished()) {
            return None;
        }
        let task = self.pending.take().expect("finished request exists");
        self.completed = Some(Instant::now());
        match runtime.block_on(task) {
            Ok(Ok((session, inventory))) => {
                self.session = Some(session);
                let message = format!(
                    "list_components: {} backends from {}",
                    inventory.backends.len(),
                    self.fetched_from
                );
                self.inventory = Some(inventory);
                self.updated = Some(Instant::now());
                self.error = None;
                Some(Ok(message))
            }
            result => {
                let error = match result {
                    Ok(Err(error)) => format!("{error:#}"),
                    Err(error) => format!("Component discovery task failed: {error}"),
                    Ok(Ok(_)) => unreachable!(),
                };
                self.error = Some(error.clone());
                Some(Err(error))
            }
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        ui.heading("Мапа екосистеми");
        ui.label(format!(
            "Control Plane / {}: {}:{} — {{\"action\":\"list_components\"}}",
            self.transport.to_uppercase(),
            self.host,
            self.port
        ));
        ui.horizontal_wrapped(|ui| {
            ui.label("Оновлювати кожні (с)");
            ui.add(egui::DragValue::new(&mut self.interval_seconds).range(1..=3600));
            if self.pending.is_some() {
                ui.spinner();
            }
        });
        if self.monitoring {
            if self.pending.is_some() {
                ui.label("Оновлення мапи…");
            } else if let Some(completed) = self.completed {
                let remaining =
                    Duration::from_secs(self.interval_seconds).saturating_sub(completed.elapsed());
                ui.label(format!(
                    "Автооновлення увімкнено / наступний запит через {:.0} с",
                    remaining.as_secs_f64().ceil()
                ));
            }
        } else if self.inventory.is_some() {
            ui.colored_label(
                HALF_OPEN,
                "Відключено — показано останній знімок, автооновлення зупинено.",
            );
        }
        ui.small("Control Plane не має автентифікації. Використовуйте приватну мережу або захищений тунель; не відкривайте порт публічно.");
        if let Some(error) = &self.error {
            ui.colored_label(OPEN, error);
            if self.monitoring {
                ui.colored_label(HALF_OPEN, "Зв’язок втрачено: дані можуть бути застарілими. Повторне підключення під час наступного оновлення.");
            }
        }
        if let Some(updated) = self.updated {
            ui.label(format!(
                "Джерело: {} / знімок отримано {:.0} с тому",
                self.fetched_from,
                updated.elapsed().as_secs_f64()
            ));
        }
        ui.horizontal_wrapped(|ui| {
            ui.colored_label(CLOSED, "● closed");
            ui.colored_label(OPEN, "● open");
            ui.colored_label(HALF_OPEN, "● half_open");
            ui.label("Масштаб");
            ui.add(egui::Slider::new(&mut self.zoom, 0.5..=1.5).show_value(false));
        });
        ui.small("Це локальний неатомарний знімок, НЕ health check. Наявний connection або closed circuit не доводять доступність backend-процесу. Стрілки показують конфігурацію, не живий трафік.");
        let Some(inventory) = &self.inventory else {
            if self.pending.is_none() && self.error.is_none() {
                ui.label("Вкажіть Control Plane gateway-а та натисніть «Підключитися». RPC-підключення зліва для цього не потрібне.");
            }
            return;
        };
        if inventory.balancer.backend_count != inventory.backends.len() {
            ui.colored_label(
                HALF_OPEN,
                "backend_count відрізняється від кількості записів у знімку.",
            );
        }
        draw_inventory(ui, inventory, self.zoom);
    }
}

impl Drop for EcosystemMap {
    fn drop(&mut self) {
        self.stop();
    }
}

fn gateway_ingress_lines(
    gateway: &json_rpc_2_tester::ecosystem::Gateway,
    color: Color32,
) -> Vec<(String, Color32)> {
    let mut by_port: BTreeMap<u16, Vec<&str>> = BTreeMap::new();
    let mut unknown_port = Vec::new();
    for listener in &gateway.listeners {
        let port = listener
            .address
            .parse::<SocketAddr>()
            .map(|address| address.port())
            .ok()
            .or_else(|| {
                listener
                    .address
                    .rsplit_once(':')
                    .and_then(|(_, port)| port.parse::<u16>().ok())
            });
        if let Some(port) = port {
            let protocols = by_port.entry(port).or_default();
            if !protocols.contains(&listener.transport.as_str()) {
                protocols.push(&listener.transport);
            }
        } else {
            unknown_port.push(format!("{} / {}", listener.transport, listener.address));
        }
    }

    let mut lines = Vec::new();
    for (port, protocols) in by_port {
        lines.push((format!("Ingress port: {port}"), color));
        lines.push((format!("Protocols: {}", protocols.join(", ")), color));
    }
    if !unknown_port.is_empty() {
        let mut seen = HashSet::new();
        unknown_port.retain(|protocol| seen.insert(protocol.clone()));
        lines.push((format!("Ingress: {}", unknown_port.join(", ")), color));
    }
    lines
}

fn circuit_color(circuit: &Circuit) -> Color32 {
    match circuit {
        Circuit::Closed { .. } => CLOSED,
        Circuit::Open { .. } => OPEN,
        Circuit::HalfOpen { .. } => HALF_OPEN,
    }
}

struct Card {
    title: String,
    lines: Vec<(String, Color32)>,
}

fn draw_inventory(ui: &mut egui::Ui, inventory: &Components, zoom: f32) {
    let text_color = Color32::LIGHT_GRAY;
    let gateway = &inventory.gateway;
    let mut gateway_lines = vec![
        (
            format!("version {} / PID {}", gateway.version, gateway.pid),
            text_color,
        ),
        (format!("uptime: {} s", gateway.uptime_seconds), text_color),
        (format!("Control: {}", gateway.control_address), text_color),
        (format!("Metrics: {}", gateway.metrics_address), text_color),
        ("Ingress listeners:".into(), CLOSED),
    ];
    gateway_lines.extend(gateway_ingress_lines(gateway, text_color));
    let gateway_card = Card {
        title: "Gateway".into(),
        lines: gateway_lines,
    };
    let balancer = &inventory.balancer;
    let balancer_card = Card {
        title: "Balancer".into(),
        lines: vec![
            (format!("version {}", balancer.version), text_color),
            (format!("execution: {}", balancer.execution), text_color),
            (
                if balancer.execution == "in_process" {
                    "У процесі Gateway (не окремий сервіс)"
                } else {
                    "Режим виконання повідомлено gateway-ом"
                }
                .into(),
                text_color,
            ),
            (
                format!("configured backends: {}", balancer.backend_count),
                text_color,
            ),
        ],
    };
    let backends: Vec<Card> = inventory
        .backends
        .iter()
        .map(|backend| {
            let mut lines = vec![(backend.host.clone(), text_color)];
            for transport in &backend.transports {
                let color = circuit_color(&transport.circuit);
                lines.push((
                    format!(
                        "{} : {} / {}",
                        transport.transport,
                        transport.port,
                        transport.circuit.label()
                    ),
                    color,
                ));
                lines.push((
                    format!(
                        "pool {} / idle {} / empty {} / busy {}",
                        transport.pool_size,
                        transport.connected_idle_slots,
                        transport.empty_idle_slots,
                        transport.busy_slots
                    ),
                    text_color,
                ));
            }
            Card {
                title: backend.id.clone(),
                lines,
            }
        })
        .collect();
    let card_height = |card: &Card| 52.0 + card.lines.len() as f32 * 21.0;
    let backend_total: f32 = backends.iter().map(|card| card_height(card) + 24.0).sum();
    let height = backend_total.max(card_height(&gateway_card)).max(280.0) + 48.0;
    let width = 1480.0;
    egui::ScrollArea::both()
        .id_salt("ecosystem_canvas")
        .max_height(650.0)
        .show(ui, |ui| {
            let (canvas, _) =
                ui.allocate_exact_size(Vec2::new(width * zoom, height * zoom), Sense::hover());
            let rect = |x: f32, y: f32, w: f32, h: f32| {
                Rect::from_min_size(canvas.min + Vec2::new(x, y) * zoom, Vec2::new(w, h) * zoom)
            };
            let gateway_rect = rect(16.0, 24.0, 400.0, card_height(&gateway_card));
            let balancer_rect = rect(476.0, 24.0, 400.0, card_height(&balancer_card));
            let mut backend_rects = Vec::new();
            let mut y = 24.0;
            for card in &backends {
                backend_rects.push(rect(936.0, y, 528.0, card_height(card)));
                y += card_height(card) + 24.0;
            }
            let painter = ui.painter();
            let connect = |from: Pos2, to: Pos2, color: Color32| {
                let mid = (from.x + to.x) / 2.0;
                let points = vec![
                    from,
                    Pos2::new(mid, from.y),
                    Pos2::new(mid, to.y),
                    to - Vec2::new(8.0 * zoom, 0.0),
                ];
                painter.add(egui::Shape::line(points, Stroke::new(1.5 * zoom, color)));
                painter.arrow(
                    to - Vec2::new(12.0 * zoom, 0.0),
                    Vec2::new(12.0 * zoom, 0.0),
                    Stroke::new(1.5 * zoom, color),
                );
            };
            connect(
                gateway_rect.right_center(),
                balancer_rect.left_center(),
                CLOSED,
            );
            for backend_rect in &backend_rects {
                connect(
                    balancer_rect.right_center(),
                    backend_rect.left_center(),
                    Color32::GRAY,
                );
            }
            draw_card(ui, gateway_rect, &gateway_card, zoom, "gateway");
            draw_card(ui, balancer_rect, &balancer_card, zoom, "balancer");
            for (index, (card, rect)) in backends.iter().zip(backend_rects).enumerate() {
                draw_card(ui, rect, card, zoom, ("backend", index));
            }
            if backends.is_empty() {
                ui.painter().text(
                    canvas.min + Vec2::new(950.0, 40.0) * zoom,
                    Align2::LEFT_TOP,
                    "Немає налаштованих backend-ів",
                    FontId::proportional(16.0 * zoom),
                    text_color,
                );
            }
        });
}

fn draw_card(ui: &mut egui::Ui, rect: Rect, card: &Card, zoom: f32, id: impl std::hash::Hash) {
    let painter = ui.painter();
    painter.rect_filled(rect, 8.0, Color32::from_rgb(30, 39, 49));
    painter.rect_stroke(
        rect,
        8.0,
        Stroke::new(zoom, Color32::from_rgb(70, 94, 110)),
        egui::StrokeKind::Inside,
    );
    // Clip long addresses/IDs on the graph; the tooltip retains the full details.
    let painter = painter.with_clip_rect(rect.shrink(8.0 * zoom));
    painter.text(
        rect.min + Vec2::new(14.0, 12.0) * zoom,
        Align2::LEFT_TOP,
        &card.title,
        FontId::proportional(18.0 * zoom),
        CLOSED,
    );
    for (index, (line, color)) in card.lines.iter().enumerate() {
        painter.text(
            rect.min + Vec2::new(14.0, 42.0 + index as f32 * 21.0) * zoom,
            Align2::LEFT_TOP,
            line,
            FontId::monospace(12.0 * zoom),
            *color,
        );
    }
    ui.interact(rect, ui.id().with(id), Sense::hover())
        .on_hover_ui(|ui| {
            ui.strong(&card.title);
            for (line, color) in &card.lines {
                ui.colored_label(*color, line);
            }
        });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::TcpListener,
    };

    #[test]
    fn refresh_schedule_obeys_interval_and_stop() {
        let now = Instant::now();
        let mut map = EcosystemMap::default();
        assert!(!map.refresh_due(now));
        map.monitoring = true;
        assert!(map.refresh_due(now));
        map.completed = Some(now);
        assert!(!map.refresh_due(now + Duration::from_secs(4)));
        assert!(map.refresh_due(now + Duration::from_secs(5)));
        map.interval_seconds = 10;
        assert!(!map.refresh_due(now + Duration::from_secs(5)));
        assert!(map.refresh_due(now + Duration::from_secs(10)));
        map.stop();
        assert!(!map.refresh_due(now + Duration::from_secs(100)));
    }

    #[test]
    fn configured_map_uses_gateway_control_endpoint_and_starts_polling() {
        let map = EcosystemMap::configured("gateway.internal".into(), 18500, 2500);

        assert_eq!(map.host, "gateway.internal");
        assert_eq!(map.port, 18500);
        assert_eq!(map.timeout_ms, 2500);
        assert_eq!(map.transport, "tcp");
        assert_eq!(map.fetched_from, "gateway.internal:18500");
        assert!(map.refresh_due(Instant::now()));
    }

    #[test]
    fn changing_active_transport_discards_the_old_control_session() {
        let mut map = EcosystemMap::configured_with_transport(
            "gateway.internal".into(),
            18500,
            2500,
            "quic",
            "gateway.internal".into(),
            "test CA".into(),
        );
        map.completed = Some(Instant::now());
        map.set_active_transport("tls");

        assert_eq!(map.transport, "tls");
        assert!(map.completed.is_none());
        assert!(map.session.is_none());
        assert_eq!(map.server_name, "gateway.internal");
        assert_eq!(map.ca_pem, "test CA");
    }

    #[test]
    fn gateway_ingress_lists_shared_port_once_with_supported_protocols() {
        let gateway = json_rpc_2_tester::ecosystem::Gateway {
            version: "1".into(),
            pid: 1,
            uptime_seconds: 1,
            listeners: vec![
                json_rpc_2_tester::ecosystem::Listener {
                    transport: "tcp".into(),
                    address: "192.168.1.132:18080".into(),
                },
                json_rpc_2_tester::ecosystem::Listener {
                    transport: "tls".into(),
                    address: "192.168.1.132:18080".into(),
                },
                json_rpc_2_tester::ecosystem::Listener {
                    transport: "quic".into(),
                    address: "192.168.1.132:18080".into(),
                },
                json_rpc_2_tester::ecosystem::Listener {
                    transport: "tls".into(),
                    address: "192.168.1.132:18080".into(),
                },
            ],
            control_address: "127.0.0.1:8500".into(),
            metrics_address: "127.0.0.1:9090".into(),
        };

        let lines = gateway_ingress_lines(&gateway, CLOSED);
        assert_eq!(
            lines
                .iter()
                .map(|(line, _)| line.as_str())
                .collect::<Vec<_>>(),
            ["Ingress port: 18080", "Protocols: tcp, tls, quic"]
        );
    }

    fn wait_for_result(map: &mut EcosystemMap, runtime: &Runtime) -> Result<String, String> {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(result) = map.poll(runtime) {
                return result;
            }
            assert!(Instant::now() < deadline, "discovery did not finish");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)] // Drop prevents struct-update syntax.
    fn background_polling_keeps_inventory_on_error_and_reconnects() {
        let runtime = Runtime::new().unwrap();
        let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = runtime.spawn(async move {
            for pid in [1, 2] {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut command = String::new();
                reader.read_line(&mut command).await.unwrap();
                assert_eq!(
                    command.as_bytes(),
                    json_rpc_2_tester::ecosystem::LIST_COMPONENTS
                );
                let value = serde_json::json!({"success":true,"components":{
                    "gateway":{"version":"1","pid":pid,"uptime_seconds":1,"listeners":[],
                        "control_address":"127.0.0.1:8500","metrics_address":"127.0.0.1:9090"},
                    "balancer":{"version":"1","execution":"in_process","backend_count":0},
                    "backends":[]
                }});
                reader
                    .get_mut()
                    .write_all(format!("{value}\n").as_bytes())
                    .await
                    .unwrap();
                // Close the first connection to exercise the reconnect path.
            }
        });
        let mut map = EcosystemMap::default();
        map.port = port;
        map.monitoring = true;
        assert!(wait_for_result(&mut map, &runtime).is_ok());
        assert_eq!(map.inventory.as_ref().unwrap().gateway.pid, 1);
        // No show() call: polling must continue when another UI tab is open.
        assert!(map.poll(&runtime).is_none());
        assert!(map.pending.is_none());
        map.completed = Some(Instant::now() - Duration::from_secs(5));
        assert!(wait_for_result(&mut map, &runtime).is_err());
        assert!(map.error.is_some());
        assert_eq!(map.inventory.as_ref().unwrap().gateway.pid, 1);
        assert!(map.session.is_none());
        map.completed = Some(Instant::now() - Duration::from_secs(5));
        assert!(wait_for_result(&mut map, &runtime).is_ok());
        assert_eq!(map.inventory.as_ref().unwrap().gateway.pid, 2);
        assert!(map.error.is_none());
        map.stop();
        assert!(map.session.is_none());
        assert!(map.poll(&runtime).is_none());
        runtime.block_on(server).unwrap();
    }
}
