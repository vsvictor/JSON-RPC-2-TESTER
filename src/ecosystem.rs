use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use json_rpc_2::{
    QUIC_ALPN, RpcClientTransport,
    client::{ClientTlsConfig, transports},
};
use serde::Deserialize;
use tokio::net;

pub const LIST_COMPONENTS: &[u8] = b"{\"action\":\"list_components\"}\n";
pub const LIST_METHODS: &[u8] = b"{\"action\":\"list_methods\"}\n";
pub const LIST_TRANSPORTS: &[u8] = b"{\"action\":\"list_transports\"}\n";
pub const CONTROL_PLANE_TRANSPORT: &str = "tcp";
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportStatus {
    pub transport: String,
    pub enabled: bool,
}

pub struct ControlTransportOptions<'a> {
    pub kind: &'a str,
    pub server_name: &'a str,
    pub ca_pem: &'a str,
}

#[derive(Deserialize)]
struct ActionResponse {
    success: bool,
    error: Option<String>,
}

pub fn parse_transport_statuses(bytes: &[u8]) -> Result<Vec<TransportStatus>> {
    let response: serde_json::Value =
        serde_json::from_slice(bytes).context("Invalid list_transports response")?;
    let success = response
        .get("success")
        .and_then(serde_json::Value::as_bool)
        .context("list_transports response is missing success")?;
    if !success {
        let error = response
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unspecified error");
        bail!("Control Plane rejected list_transports: {error}");
    }

    let transports = response
        .get("transports")
        .context("Successful list_transports response is missing transports")?;
    let statuses = if let Some(items) = transports.as_array() {
        items
            .iter()
            .map(|item| {
                let name = item
                    .get("transport")
                    .or_else(|| item.get("name"))
                    .and_then(serde_json::Value::as_str)
                    .context("Transport entry is missing transport name")?;
                let enabled = item
                    .get("enabled")
                    .and_then(serde_json::Value::as_bool)
                    .context("Transport entry is missing enabled state")?;
                Ok(TransportStatus {
                    transport: name.to_owned(),
                    enabled,
                })
            })
            .collect::<Result<Vec<_>>>()?
    } else if let Some(items) = transports.as_object() {
        items
            .iter()
            .map(|(name, enabled)| {
                let enabled = enabled
                    .get("enabled")
                    .unwrap_or(enabled)
                    .as_bool()
                    .with_context(|| format!("Transport {name} has a non-boolean enabled state"))?;
                Ok(TransportStatus {
                    transport: name.clone(),
                    enabled,
                })
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        bail!("list_transports transports must be an array or object");
    };

    let mut names = std::collections::HashSet::new();
    ensure!(
        statuses.iter().all(|status| {
            !status.transport.trim().is_empty() && names.insert(status.transport.clone())
        }),
        "list_transports response contains an empty or duplicate transport"
    );
    Ok(statuses)
}

pub fn parse_action_response(bytes: &[u8], action: &str) -> Result<()> {
    let response: ActionResponse =
        serde_json::from_slice(bytes).with_context(|| format!("Invalid {action} response"))?;
    if !response.success {
        bail!(
            "Control Plane rejected {action}: {}",
            response.error.as_deref().unwrap_or("unspecified error")
        );
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct Components {
    pub gateway: Gateway,
    pub balancer: Balancer,
    pub backends: Vec<Backend>,
}

#[derive(Debug, Deserialize)]
pub struct Gateway {
    pub version: String,
    pub pid: u32,
    pub uptime_seconds: u64,
    pub listeners: Vec<Listener>,
    pub control_address: String,
    pub metrics_address: String,
}

#[derive(Debug, Deserialize)]
pub struct Listener {
    pub transport: String,
    pub address: String,
}

#[derive(Debug, Deserialize)]
pub struct Balancer {
    pub version: String,
    pub execution: String,
    pub backend_count: usize,
}

#[derive(Debug, Deserialize)]
pub struct Backend {
    pub id: String,
    pub host: String,
    pub transports: Vec<BackendTransport>,
}

#[derive(Debug, Deserialize)]
pub struct BackendTransport {
    pub transport: String,
    pub port: u16,
    pub pool_size: usize,
    pub connected_idle_slots: usize,
    pub empty_idle_slots: usize,
    pub busy_slots: usize,
    pub circuit: Circuit,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Circuit {
    Closed { consecutive_failures: usize },
    Open { retry_after_ms: u64 },
    HalfOpen { probe_in_flight: bool },
}

impl Circuit {
    pub fn label(&self) -> String {
        match self {
            Self::Closed {
                consecutive_failures,
            } => format!("closed / failures: {consecutive_failures}"),
            Self::Open { retry_after_ms } => format!("open / retry: {retry_after_ms} ms"),
            Self::HalfOpen { probe_in_flight } => format!("half_open / probe: {probe_in_flight}"),
        }
    }
}

#[derive(Deserialize)]
struct ControlResponse {
    success: bool,
    error: Option<String>,
    components: Option<Components>,
}

pub fn parse_response(bytes: &[u8]) -> Result<Components> {
    let response: ControlResponse =
        serde_json::from_slice(bytes).context("Invalid list_components response")?;
    if !response.success {
        bail!(
            "Control Plane rejected list_components: {}",
            response.error.as_deref().unwrap_or("unspecified error")
        );
    }
    response
        .components
        .context("Successful response is missing components")
}

#[derive(Deserialize)]
struct MethodsResponse {
    success: bool,
    error: Option<String>,
    methods: Option<Vec<String>>,
    method_details: Option<Vec<MethodDetailResponse>>,
}

#[derive(Deserialize)]
struct MethodDetailResponse {
    name: String,
    params_schema: Option<serde_json::Value>,
    example_request: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct MethodInfo {
    pub name: String,
    pub params_schema: Option<serde_json::Value>,
    pub example_params: String,
}

pub fn parse_methods_response(bytes: &[u8]) -> Result<Vec<MethodInfo>> {
    let response: MethodsResponse =
        serde_json::from_slice(bytes).context("Invalid list_methods response")?;
    if !response.success {
        bail!(
            "Control Plane rejected list_methods: {}",
            response.error.as_deref().unwrap_or("unspecified error")
        );
    }
    let methods = response
        .methods
        .context("Successful list_methods response is missing methods")?;
    ensure!(
        methods.iter().all(|method| !method.trim().is_empty()),
        "list_methods response contains an empty method name"
    );
    let mut unique_methods = std::collections::HashSet::new();
    ensure!(
        methods
            .iter()
            .all(|method| unique_methods.insert(method.clone())),
        "list_methods response contains duplicate method names"
    );

    let details = response
        .method_details
        .context("Successful list_methods response is missing method_details")?;
    let mut details_by_name = std::collections::HashMap::new();
    for detail in details {
        ensure!(
            unique_methods.contains(&detail.name),
            "Method details contain unlisted method {}",
            detail.name
        );
        ensure!(
            !details_by_name.contains_key(&detail.name),
            "Method details contain duplicate method {}",
            detail.name
        );
        let request = detail
            .example_request
            .as_object()
            .context("Method example_request must be a JSON object")?;
        ensure!(
            request.get("jsonrpc").and_then(serde_json::Value::as_str) == Some("2.0"),
            "Method {} example_request must specify JSON-RPC 2.0",
            detail.name
        );
        ensure!(
            request.get("method").and_then(serde_json::Value::as_str) == Some(detail.name.as_str()),
            "Method {} example_request has a mismatched method name",
            detail.name
        );
        if let Some(params) = request.get("params") {
            ensure!(
                params.is_object() || params.is_array(),
                "Method {} example params must be an object or array",
                detail.name
            );
        }
        let example_params = request
            .get("params")
            .map(serde_json::to_string_pretty)
            .transpose()
            .context("Serializing method example params")?
            .unwrap_or_default();
        details_by_name.insert(
            detail.name.clone(),
            MethodInfo {
                name: detail.name,
                params_schema: detail.params_schema,
                example_params,
            },
        );
    }
    ensure!(
        details_by_name.len() == methods.len(),
        "Method details are missing one or more listed methods"
    );
    methods
        .into_iter()
        .map(|name| {
            details_by_name
                .remove(&name)
                .with_context(|| format!("Missing details for listed method {name}"))
        })
        .collect()
}

pub struct ControlSession {
    transport: Arc<dyn RpcClientTransport>,
}

impl ControlSession {
    pub async fn connect(host: &str, port: u16) -> Result<Self> {
        Self::connect_with_transport(host, port, "tcp", "", "").await
    }

    pub async fn connect_with_transport(
        host: &str,
        port: u16,
        transport: &str,
        server_name: &str,
        ca_pem: &str,
    ) -> Result<Self> {
        ensure!(
            !host.trim().is_empty() && port != 0,
            "Control Plane host and nonzero port are required"
        );
        ensure!(
            matches!(transport, "quic" | "tls" | "tcp"),
            "Unsupported Control Plane transport: {transport}"
        );
        let host = host.trim().trim_start_matches('[').trim_end_matches(']');
        let address = net::lookup_host((host, port))
            .await
            .context("Resolving Control Plane address")?
            .next()
            .context("DNS returned no Control Plane addresses")?;
        let transport: Arc<dyn RpcClientTransport> = match transport {
            "tcp" => Arc::new(
                transports::tcp::TcpClientTransport::connect(address)
                    .await
                    .context("Connecting to Control Plane over TCP")?,
            ),
            "tls" => Arc::new(
                transports::tls::TlsClientTransport::connect(address, server_name, ca_pem)
                    .await
                    .context("Connecting to Control Plane over TLS")?,
            ),
            "quic" => {
                let local: SocketAddr = if address.is_ipv6() {
                    "[::]:0"
                } else {
                    "0.0.0.0:0"
                }
                .parse()
                .context("Creating local QUIC address")?;
                let mut tls = ClientTlsConfig::default();
                tls.set_ca_certificate_pem(ca_pem)
                    .map_err(anyhow::Error::from)
                    .context("Configuring Control Plane QUIC trust")?;
                let endpoint = tls
                    .endpoint(local, QUIC_ALPN)
                    .map_err(anyhow::Error::from)
                    .context("Creating Control Plane QUIC endpoint")?;
                Arc::new(
                    transports::quic::QuicClientTransport::connect(endpoint, address, server_name)
                        .await
                        .map_err(anyhow::Error::from)
                        .context("Connecting to Control Plane over QUIC")?,
                )
            }
            _ => unreachable!("transport was validated above"),
        };
        Ok(Self { transport })
    }

    pub fn transport_kind(&self) -> &'static str {
        self.transport.kind()
    }

    /// Discard this session after any error or cancelled request: a partial reply
    /// must never be mistaken for the next inventory.
    pub async fn request(&mut self) -> Result<Components> {
        let bytes = self
            .request_bytes(LIST_COMPONENTS, "list_components")
            .await?;
        parse_response(&bytes)
    }

    pub async fn list_methods(&mut self) -> Result<Vec<MethodInfo>> {
        let bytes = self.request_bytes(LIST_METHODS, "list_methods").await?;
        parse_methods_response(&bytes)
    }

    pub async fn list_transports(&mut self) -> Result<Vec<TransportStatus>> {
        let bytes = self
            .request_bytes(LIST_TRANSPORTS, "list_transports")
            .await?;
        parse_transport_statuses(&bytes)
    }

    pub async fn set_transport_enabled(&mut self, transport: &str, enabled: bool) -> Result<()> {
        ensure!(
            !transport.trim().is_empty() && !transport.contains(['\n', '\r', '"']),
            "Transport name is invalid"
        );
        let command = serde_json::json!({
            "action": "set_transport_enabled",
            "transport": transport,
            "enabled": enabled
        });
        let mut bytes = serde_json::to_vec(&command).context("Encoding set_transport_enabled")?;
        bytes.push(b'\n');
        let response = self.request_bytes(&bytes, "set_transport_enabled").await?;
        parse_action_response(&response, "set_transport_enabled")
    }

    async fn request_bytes(&mut self, command: &[u8], action: &str) -> Result<Vec<u8>> {
        let message = command.strip_suffix(b"\n").unwrap_or(command);
        let response = self
            .transport
            .send_with_expectation(Bytes::copy_from_slice(message), true)
            .await
            .map_err(anyhow::Error::from)
            .with_context(|| format!("Exchanging {action} with Control Plane"))?
            .with_context(|| format!("Control Plane returned no {action} response"))?;
        let bytes = response.to_vec();
        ensure!(
            bytes.len() <= MAX_RESPONSE_BYTES,
            "{action} response exceeds the 4 MiB limit"
        );
        Ok(bytes)
    }
}

pub async fn fetch(host: &str, port: u16, timeout: Duration) -> Result<Components> {
    ensure!(!timeout.is_zero(), "Control Plane timeout must be positive");
    tokio::time::timeout(timeout, async {
        ControlSession::connect(host, port).await?.request().await
    })
    .await
    .context("Control Plane request timed out")?
}

pub async fn fetch_methods(host: &str, port: u16, timeout: Duration) -> Result<Vec<MethodInfo>> {
    fetch_methods_with_transport(host, port, timeout, "tcp", "", "").await
}

pub async fn fetch_methods_with_transport(
    host: &str,
    port: u16,
    timeout: Duration,
    transport: &str,
    server_name: &str,
    ca_pem: &str,
) -> Result<Vec<MethodInfo>> {
    ensure!(!timeout.is_zero(), "Control Plane timeout must be positive");
    tokio::time::timeout(timeout, async {
        let mut session =
            ControlSession::connect_with_transport(host, port, transport, server_name, ca_pem)
                .await?;
        session.list_methods().await
    })
    .await
    .context("Control Plane list_methods request timed out")?
}

pub async fn fetch_transports(
    host: &str,
    port: u16,
    timeout: Duration,
) -> Result<Vec<TransportStatus>> {
    fetch_transports_with_transport(host, port, timeout, "tcp", "", "").await
}

pub async fn fetch_transports_with_transport(
    host: &str,
    port: u16,
    timeout: Duration,
    transport: &str,
    server_name: &str,
    ca_pem: &str,
) -> Result<Vec<TransportStatus>> {
    ensure!(!timeout.is_zero(), "Control Plane timeout must be positive");
    tokio::time::timeout(timeout, async {
        let mut session =
            ControlSession::connect_with_transport(host, port, transport, server_name, ca_pem)
                .await?;
        session.list_transports().await
    })
    .await
    .context("Control Plane list_transports request timed out")?
}

pub async fn set_transport_enabled(
    host: &str,
    port: u16,
    timeout: Duration,
    transport: &str,
    enabled: bool,
) -> Result<()> {
    set_transport_enabled_with_transport(
        host,
        port,
        timeout,
        ControlTransportOptions {
            kind: "tcp",
            server_name: "",
            ca_pem: "",
        },
        transport,
        enabled,
    )
    .await
}

pub async fn set_transport_enabled_with_transport(
    host: &str,
    port: u16,
    timeout: Duration,
    connection_transport: ControlTransportOptions<'_>,
    transport: &str,
    enabled: bool,
) -> Result<()> {
    ensure!(!timeout.is_zero(), "Control Plane timeout must be positive");
    tokio::time::timeout(timeout, async {
        let mut session = ControlSession::connect_with_transport(
            host,
            port,
            connection_transport.kind,
            connection_transport.server_name,
            connection_transport.ca_pem,
        )
        .await?;
        session.set_transport_enabled(transport, enabled).await
    })
    .await
    .context("Control Plane set_transport_enabled request timed out")?
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    fn inventory() -> serde_json::Value {
        json!({"success": true, "components": {
            "gateway": {"version": "0.1", "pid": 42, "uptime_seconds": 10,
                "listeners": [{"transport": "tcp", "address": "127.0.0.1:8080"}],
                "control_address": "127.0.0.1:8500", "metrics_address": "127.0.0.1:9090"},
            "balancer": {"version": "0.1", "execution": "in_process", "backend_count": 1},
            "backends": [{"id": "a", "host": "localhost", "transports": [
                {"transport": "tcp", "port": 8081, "pool_size": 4,
                 "connected_idle_slots": 2, "empty_idle_slots": 1, "busy_slots": 1,
                 "circuit": {"state": "closed", "consecutive_failures": 0}},
                {"transport": "udp", "port": 8081, "pool_size": 4,
                 "connected_idle_slots": 0, "empty_idle_slots": 4, "busy_slots": 0,
                 "circuit": {"state": "open", "retry_after_ms": 100}},
                {"transport": "tls", "port": 8443, "pool_size": 4,
                 "connected_idle_slots": 1, "empty_idle_slots": 2, "busy_slots": 1,
                 "circuit": {"state": "half_open", "probe_in_flight": true}}
            ]}]
        }})
    }

    #[test]
    fn decodes_all_circuit_states_and_empty_inventory() {
        let data = parse_response(&serde_json::to_vec(&inventory()).unwrap()).unwrap();
        assert_eq!(data.backends[0].transports.len(), 3);
        assert!(matches!(
            data.backends[0].transports[2].circuit,
            Circuit::HalfOpen {
                probe_in_flight: true
            }
        ));
        let mut empty = inventory();
        empty["components"]["backends"] = json!([]);
        empty["components"]["balancer"]["backend_count"] = json!(0);
        assert!(
            parse_response(&serde_json::to_vec(&empty).unwrap())
                .unwrap()
                .backends
                .is_empty()
        );
        for invalid in [
            r#"{"success":false,"error":"Denied"}"#,
            r#"{"success":true}"#,
            "{}",
        ] {
            assert!(parse_response(invalid.as_bytes()).is_err());
        }
    }

    #[test]
    fn parses_transport_statuses_from_list_and_map_responses() {
        let listed = parse_transport_statuses(
            br#"{"success":true,"transports":[{"transport":"quic","enabled":true},{"name":"tls","enabled":false}]}"#,
        )
        .unwrap();
        assert_eq!(
            listed,
            [
                TransportStatus {
                    transport: "quic".into(),
                    enabled: true
                },
                TransportStatus {
                    transport: "tls".into(),
                    enabled: false
                }
            ]
        );
        let mapped =
            parse_transport_statuses(br#"{"success":true,"transports":{"tcp":true}}"#).unwrap();
        assert_eq!(
            mapped,
            [TransportStatus {
                transport: "tcp".into(),
                enabled: true
            }]
        );
        assert!(parse_transport_statuses(br#"{"success":false,"error":"denied"}"#).is_err());
        assert!(
            parse_transport_statuses(
                br#"{"success":true,"transports":[{"transport":"quic","enabled":true},{"transport":"quic","enabled":false}]}"#
            )
            .is_err()
        );
    }

    #[test]
    fn parses_method_examples_and_preserves_catalog_order() {
        let catalog = json!({
            "success": true,
            "methods": ["startShift", "echo"],
            "method_details": [
                {
                    "name": "echo",
                    "params_schema": {"type": ["object", "array"]},
                    "example_request": {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "echo",
                        "params": {"message": "example"}
                    }
                },
                {
                    "name": "startShift",
                    "params_schema": {
                        "type": "object",
                        "required": ["accessToken", "deviceTime", "sendTime", "vehicleId"]
                    },
                    "example_request": {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "startShift",
                        "params": {
                            "accessToken": "mock-access-token",
                            "deviceTime": "2026-01-01T12:00:00Z",
                            "sendTime": "2026-01-01T12:00:01Z",
                            "vehicleId": "mock-vehicle-id",
                            "lat": 50.45,
                            "lng": 30.52
                        }
                    }
                }
            ]
        });
        let methods = parse_methods_response(&serde_json::to_vec(&catalog).unwrap()).unwrap();
        assert_eq!(
            methods
                .iter()
                .map(|method| method.name.as_str())
                .collect::<Vec<_>>(),
            ["startShift", "echo"]
        );
        let params: serde_json::Value = serde_json::from_str(&methods[0].example_params).unwrap();
        assert_eq!(params["deviceTime"], "2026-01-01T12:00:00Z");
        assert_eq!(params["sendTime"], "2026-01-01T12:00:01Z");
        assert_eq!(params["vehicleId"], "mock-vehicle-id");
        assert_eq!(params["lat"], 50.45);
        assert_eq!(params["lng"], 30.52);
        assert_eq!(
            methods[0].params_schema.as_ref().unwrap()["required"],
            json!(["accessToken", "deviceTime", "sendTime", "vehicleId"])
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&methods[1].example_params).unwrap()["message"],
            "example"
        );
    }

    #[test]
    fn rejects_incomplete_or_mismatched_method_catalogs() {
        assert!(parse_methods_response(br#"{"success":true}"#).is_err());
        assert!(parse_methods_response(br#"{"success":false,"error":"denied"}"#).is_err());
        assert!(parse_methods_response(br#"{"success":true,"methods":[""]}"#).is_err());
        assert!(
            parse_methods_response(br#"{"success":true,"methods":["echo"],"method_details":[]}"#)
                .is_err()
        );
        assert!(parse_methods_response(
            br#"{"success":true,"methods":["echo"],"method_details":[{"name":"echo","example_request":{"jsonrpc":"2.0","method":"wrong","params":{}}}]}"#
        )
        .is_err());
    }

    #[tokio::test]
    async fn sends_exact_control_command_and_reads_fragmented_response() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut command = Vec::new();
            reader.read_until(b'\n', &mut command).await.unwrap();
            assert_eq!(command, LIST_COMPONENTS);
            let mut response = serde_json::to_vec(&inventory()).unwrap();
            response.push(b'\n');
            for part in response.chunks(13) {
                reader.get_mut().write_all(part).await.unwrap();
            }
        });
        let data = fetch("127.0.0.1", port, Duration::from_secs(2)).await?;
        assert_eq!(data.gateway.pid, 42);
        assert_eq!(data.backends[0].id, "a");
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn sends_exact_list_methods_command_and_reads_method_names() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut command = Vec::new();
            reader.read_until(b'\n', &mut command).await.unwrap();
            assert_eq!(command, LIST_METHODS);
            let response = json!({
                "success": true,
                "methods": ["sum", "echo"],
                "method_details": [
                    {
                        "name": "sum",
                        "params_schema": {"type": "array"},
                        "example_request": {
                            "jsonrpc": "2.0", "id": 1, "method": "sum",
                            "params": {"numbers": [1, 2, 3]}
                        }
                    },
                    {
                        "name": "echo",
                        "params_schema": {"type": "object"},
                        "example_request": {
                            "jsonrpc": "2.0", "id": 1, "method": "echo",
                            "params": {"message": "example"}
                        }
                    }
                ]
            });
            reader
                .get_mut()
                .write_all(&serde_json::to_vec(&response).unwrap())
                .await
                .unwrap();
            reader.get_mut().write_all(b"\n").await.unwrap();
        });
        let methods = fetch_methods("127.0.0.1", port, Duration::from_secs(2)).await?;
        assert_eq!(
            methods
                .iter()
                .map(|method| method.name.as_str())
                .collect::<Vec<_>>(),
            ["sum", "echo"]
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&methods[0].example_params)?["numbers"],
            json!([1, 2, 3])
        );
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn sends_exact_transport_control_commands() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut command = Vec::new();
            reader.read_until(b'\n', &mut command).await.unwrap();
            let mut expected =
                br#"{"action":"set_transport_enabled","enabled":false,"transport":"quic"}"#
                    .to_vec();
            expected.push(b'\n');
            assert_eq!(command, expected);
            reader
                .get_mut()
                .write_all(br#"{"success":true}"#)
                .await
                .unwrap();
            reader.get_mut().write_all(b"\n").await.unwrap();
        });
        set_transport_enabled("127.0.0.1", port, Duration::from_secs(2), "quic", false).await?;
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn sends_exact_list_transports_command() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut command = Vec::new();
            reader.read_until(b'\n', &mut command).await.unwrap();
            assert_eq!(command, LIST_TRANSPORTS);
            reader
                .get_mut()
                .write_all(
                    br#"{"success":true,"transports":[{"transport":"quic","enabled":true}]}"#,
                )
                .await
                .unwrap();
            reader.get_mut().write_all(b"\n").await.unwrap();
        });
        let transports = fetch_transports("127.0.0.1", port, Duration::from_secs(2)).await?;
        assert_eq!(
            transports,
            [TransportStatus {
                transport: "quic".into(),
                enabled: true
            }]
        );
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn reuses_one_tcp_connection_for_changing_snapshots() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            for uptime in [10, 20, 30] {
                let mut command = Vec::new();
                reader.read_until(b'\n', &mut command).await.unwrap();
                assert_eq!(command, LIST_COMPONENTS);
                let mut value = inventory();
                value["components"]["gateway"]["uptime_seconds"] = json!(uptime);
                let mut response = serde_json::to_vec(&value).unwrap();
                response.push(b'\n');
                reader.get_mut().write_all(&response).await.unwrap();
            }
        });
        let mut session = ControlSession::connect("127.0.0.1", port).await?;
        for uptime in [10, 20, 30] {
            assert_eq!(session.request().await?.gateway.uptime_seconds, uptime);
        }
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn rejects_partial_oversized_and_stalled_responses() -> Result<()> {
        for mode in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let port = listener.local_addr()?.port();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut command = vec![0; LIST_COMPONENTS.len()];
                stream.read_exact(&mut command).await.unwrap();
                match mode {
                    0 => {
                        stream.write_all(b"{\"success\":true}").await.unwrap();
                    }
                    1 => {
                        let _ = stream.write_all(&vec![b' '; MAX_RESPONSE_BYTES + 1]).await;
                    }
                    _ => tokio::time::sleep(Duration::from_secs(2)).await,
                }
            });
            assert!(
                fetch("127.0.0.1", port, Duration::from_millis(300))
                    .await
                    .is_err()
            );
            server.abort();
        }
        Ok(())
    }
}
