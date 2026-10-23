use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use json_rpc_2::{
    AmqpConfig, CancellationToken, ClientCloseFuture, ClientPingFuture, ClientRecoverFuture,
    ClientTransportFuture, H3_ALPN, MqttConfig, QUIC_ALPN, RpcClientTransport, TcpTuningConfig,
    TransportError, TransportMessage, WS_QUIC_ALPN,
    client::{
        ClientLimits, ClientLivenessConfig, ClientTlsConfig, ReconnectPolicy, RpcClient,
        RpcClientConfig, transports::*,
    },
};

pub type Client = RpcClient<Arc<dyn RpcClientTransport>>;
pub type Transport = Arc<dyn RpcClientTransport>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransportChoice {
    pub id: &'static str,
    pub port: u16,
}

pub const TRANSPORTS: &[TransportChoice] = &[
    TransportChoice {
        id: "tcp",
        port: 40400,
    },
    TransportChoice {
        id: "tls",
        port: 40424,
    },
    TransportChoice {
        id: "udp",
        port: 40401,
    },
    TransportChoice {
        id: "rtp",
        port: 40402,
    },
    TransportChoice {
        id: "rudp",
        port: 40403,
    },
    TransportChoice {
        id: "srt",
        port: 40404,
    },
    TransportChoice {
        id: "udplite",
        port: 40416,
    },
    TransportChoice {
        id: "dccp",
        port: 40418,
    },
    TransportChoice {
        id: "sctp",
        port: 40420,
    },
    TransportChoice {
        id: "quic",
        port: 40405,
    },
    TransportChoice {
        id: "http1",
        port: 40406,
    },
    TransportChoice {
        id: "http3",
        port: 40407,
    },
    TransportChoice {
        id: "mqtt",
        port: 1883,
    },
    TransportChoice {
        id: "coap",
        port: 5683,
    },
    TransportChoice {
        id: "amqp",
        port: 5672,
    },
    TransportChoice {
        id: "ws-tcp",
        port: 40408,
    },
    TransportChoice {
        id: "ws-udp",
        port: 40409,
    },
    TransportChoice {
        id: "ws-rtp",
        port: 40410,
    },
    TransportChoice {
        id: "ws-rudp",
        port: 40411,
    },
    TransportChoice {
        id: "ws-srt",
        port: 40412,
    },
    TransportChoice {
        id: "ws-udplite",
        port: 40417,
    },
    TransportChoice {
        id: "ws-dccp",
        port: 40419,
    },
    TransportChoice {
        id: "ws-sctp",
        port: 40421,
    },
    TransportChoice {
        id: "ws-quic",
        port: 40413,
    },
    TransportChoice {
        id: "ws-http1",
        port: 40414,
    },
    TransportChoice {
        id: "ws-http3",
        port: 40415,
    },
    TransportChoice {
        id: "ws-tls",
        port: 40425,
    },
    TransportChoice {
        id: "ws-mqtt",
        port: 1883,
    },
    TransportChoice {
        id: "ws-coap",
        port: 5683,
    },
    TransportChoice {
        id: "ws-amqp",
        port: 5672,
    },
];

impl TransportChoice {
    pub fn unavailable(self) -> Option<&'static str> {
        match self.id {
            "srt" | "ws-srt" if !cfg!(feature = "native-srt") => {
                Some("Rebuild with --features native-srt and install encrypted libsrt >= 1.5")
            }
            "udplite" | "ws-udplite" | "dccp" | "ws-dccp" | "sctp" | "ws-sctp"
                if !cfg!(target_os = "linux") =>
            {
                Some("This desktop adapter requires Linux and kernel protocol support")
            }
            _ => None,
        }
    }

    pub fn tls(self) -> bool {
        matches!(
            self.id,
            "tls" | "quic" | "http3" | "ws-quic" | "ws-http3" | "ws-tls"
        )
    }

    pub fn broker(self) -> bool {
        matches!(self.id, "mqtt" | "ws-mqtt" | "amqp" | "ws-amqp")
    }
}

#[derive(Clone)]
pub struct ConnectionSettings {
    pub choice: TransportChoice,
    pub host: String,
    pub port: u16,
    pub server_name: String,
    pub ca_pem: String,
    pub ca_file: String,
    pub ws_path: String,
    pub srt_passphrase: String,
    pub send_coverage: u16,
    pub receive_coverage: u16,
    pub broker_url: String,
    pub broker_route: String,
    pub broker_operation_ms: u64,
    pub broker_response_ms: u64,
    pub tcp: TcpTuningConfig,
    pub connect_timeout_ms: u64,
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        Self {
            choice: TRANSPORTS[0],
            host: "127.0.0.1".into(),
            port: 40400,
            server_name: "localhost".into(),
            ca_pem: String::new(),
            ca_file: String::new(),
            ws_path: "/ws".into(),
            srt_passphrase: String::new(),
            send_coverage: 8,
            receive_coverage: 8,
            broker_url: "mqtt://127.0.0.1:1883".into(),
            broker_route: "json-rpc-2/requests".into(),
            broker_operation_ms: 10_000,
            broker_response_ms: 30_000,
            tcp: TcpTuningConfig::default(),
            connect_timeout_ms: 10_000,
        }
    }
}

#[derive(Clone)]
pub struct RuntimeSettings {
    pub timeout_ms: u64,
    pub ping_ms: u64,
    pub missed_pongs: u32,
    pub queue_capacity: usize,
    pub max_in_flight: usize,
    pub restore_first: bool,
    pub cooldown_ms: u64,
    pub max_backoff_ms: u64,
    pub jitter_ms: u64,
    pub max_elapsed_ms: u64,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            timeout_ms: 5000,
            ping_ms: 0,
            missed_pongs: 3,
            queue_capacity: 256,
            max_in_flight: 64,
            restore_first: true,
            cooldown_ms: 250,
            max_backoff_ms: 5000,
            jitter_ms: 100,
            max_elapsed_ms: 30_000,
        }
    }
}

impl RuntimeSettings {
    pub fn config(&self) -> Result<RpcClientConfig> {
        ensure!(self.timeout_ms > 0, "Request timeout must be positive");
        ensure!(
            self.missed_pongs > 0,
            "Missed pong threshold must be positive"
        );
        ensure!(
            self.queue_capacity > 0 && self.max_in_flight > 0,
            "Queue and in-flight limits must be positive"
        );
        ensure!(
            self.queue_capacity <= tokio::sync::Semaphore::MAX_PERMITS
                && self.max_in_flight <= tokio::sync::Semaphore::MAX_PERMITS,
            "Queue and in-flight limits exceed the Tokio semaphore capacity"
        );
        ensure!(
            self.max_backoff_ms == 0 || self.max_backoff_ms >= self.cooldown_ms,
            "Maximum backoff must be 0 or >= cooldown"
        );
        Ok(RpcClientConfig {
            request_timeout: Duration::from_millis(self.timeout_ms),
            liveness: ClientLivenessConfig {
                ping_frequency: (self.ping_ms != 0).then(|| Duration::from_millis(self.ping_ms)),
                max_missed_pongs: self.missed_pongs,
            },
            limits: ClientLimits {
                outbound_queue_capacity: self.queue_capacity,
                max_in_flight_requests: self.max_in_flight,
            },
            reconnect: ReconnectPolicy {
                restore_before_reconnect: self.restore_first,
                reconnect_cooldown: Duration::from_millis(self.cooldown_ms),
                max_reconnect_backoff: Duration::from_millis(self.max_backoff_ms),
                reconnect_jitter: Duration::from_millis(self.jitter_ms),
                max_reconnect_elapsed: (self.max_elapsed_ms != 0)
                    .then(|| Duration::from_millis(self.max_elapsed_ms)),
            },
        })
    }

    pub async fn apply(&self, client: &Client) -> Result<()> {
        let config = self.config()?;
        let current = client.config();
        ensure!(
            config.limits.outbound_queue_capacity == current.limits.outbound_queue_capacity,
            "Changing queue capacity requires disconnecting first"
        );
        ensure!(
            config.limits.max_in_flight_requests >= current.limits.max_in_flight_requests,
            "Decreasing the in-flight limit requires disconnecting first"
        );
        client.set_config(config).await?;
        Ok(())
    }
}

impl ConnectionSettings {
    pub fn validate(&self) -> Result<()> {
        if let Some(reason) = self.choice.unavailable() {
            bail!("{reason}");
        }
        ensure!(TRANSPORTS.contains(&self.choice), "Unknown transport");
        ensure!(
            self.connect_timeout_ms > 0,
            "Connect timeout must be positive"
        );
        if self.choice.broker() {
            ensure!(
                !self.broker_route.trim().is_empty(),
                "Broker route is required"
            );
            let schemes: &[&str] = if self.choice.id.ends_with("mqtt") {
                &["mqtt://", "mqtts://"]
            } else {
                &["amqp://", "amqps://"]
            };
            ensure!(
                schemes.iter().any(|s| self.broker_url.starts_with(s)),
                "Invalid broker URL scheme"
            );
            ensure!(
                self.broker_operation_ms > 0 && self.broker_response_ms > 0,
                "Broker timeouts must be positive"
            );
        } else {
            ensure!(
                !self.host.trim().is_empty() && self.port != 0,
                "Host and nonzero port are required"
            );
        }
        if self.choice.tls() {
            ensure!(
                !self.server_name.trim().is_empty(),
                "TLS server name is required"
            );
            ensure!(
                !self.ca_pem.trim().is_empty() || !self.ca_file.trim().is_empty(),
                "A trusted CA PEM or PEM file is required"
            );
        }
        if matches!(self.choice.id, "srt" | "ws-srt") {
            ensure!(
                (10..=79).contains(&self.srt_passphrase.len()),
                "SRT passphrase must contain 10..=79 bytes"
            );
        }
        if matches!(self.choice.id, "udplite" | "ws-udplite") {
            ensure!(
                [self.send_coverage, self.receive_coverage]
                    .iter()
                    .all(|v| *v == 0 || *v >= 8),
                "UDP-Lite coverage must be 0 (full) or >= 8"
            );
        }
        ensure!(
            self.ws_path.starts_with('/') && !self.ws_path.contains(['\r', '\n']),
            "WebSocket path must start with / and contain no line breaks"
        );
        Ok(())
    }
}

pub async fn connect(
    settings: ConnectionSettings,
    runtime: RuntimeSettings,
) -> Result<(Client, Transport)> {
    settings.validate()?;
    let config = runtime.config()?;
    let timeout = Duration::from_millis(settings.connect_timeout_ms);
    let mut transport = tokio::time::timeout(timeout, create_transport(&settings))
        .await
        .context("Connection timed out")??;
    if matches!(settings.choice.id, "ws-quic" | "ws-http3") {
        transport = Arc::new(EndpointSession {
            inner: tokio::sync::RwLock::new(transport),
            settings,
        });
    }
    let client = Client::new_with_config(transport.clone(), config.clone())?;
    // Construction stores liveness policy; set_config starts the periodic task.
    if let Err(error) = client.set_config(config).await {
        let close_result = client.close().await;
        return Err(anyhow::anyhow!(
            "Applying client policy failed: {error}; cleanup: {close_result:?}"
        ));
    }
    Ok((client, transport))
}

// These upstream WebSocket carriers wait for endpoint-wide idleness when closing.
// Give every replacement its own endpoint so closing the old session cannot wait
// for (or time out) the new session. Forward cancellation and framing unchanged.
struct EndpointSession {
    inner: tokio::sync::RwLock<Transport>,
    settings: ConnectionSettings,
}

impl RpcClientTransport for EndpointSession {
    fn send<'a>(&'a self, message: Bytes) -> ClientTransportFuture<'a> {
        Box::pin(async move { self.inner.read().await.send(message).await })
    }

    fn send_message<'a>(
        &'a self,
        message: TransportMessage,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            self.inner
                .read()
                .await
                .send_message(message, expects_response)
                .await
        })
    }

    fn send_with_expectation<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            self.inner
                .read()
                .await
                .send_with_expectation(message, expects_response)
                .await
        })
    }

    fn send_cancelable<'a>(
        &'a self,
        message: Bytes,
        expects_response: bool,
        cancel: CancellationToken,
    ) -> ClientTransportFuture<'a> {
        Box::pin(async move {
            self.inner
                .read()
                .await
                .send_cancelable(message, expects_response, cancel)
                .await
        })
    }

    fn kind(&self) -> &'static str {
        self.settings.choice.id
    }

    fn peer(&self) -> Option<String> {
        Some(format!("{}:{}", self.settings.host, self.settings.port))
    }

    fn transport_ping<'a>(&'a self) -> ClientPingFuture<'a> {
        Box::pin(async move { self.inner.read().await.transport_ping().await })
    }

    fn restore<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move { self.inner.read().await.restore().await })
    }

    fn reconnect<'a>(&'a self) -> ClientRecoverFuture<'a> {
        Box::pin(async move {
            let replacement = tokio::time::timeout(
                Duration::from_millis(self.settings.connect_timeout_ms),
                create_transport(&self.settings),
            )
            .await
            .map_err(|error| TransportError::new(error.to_string()))?
            .map_err(|error| TransportError::new(format!("{error:#}")))?;
            let previous = std::mem::replace(&mut *self.inner.write().await, replacement);
            previous.close().await?;
            tracing::info!(
                transport = self.kind(),
                "WebSocket session reconnected with a new QUIC endpoint"
            );
            Ok(())
        })
    }

    fn close<'a>(&'a self) -> ClientCloseFuture<'a> {
        Box::pin(async move { self.inner.read().await.close().await })
    }
}

async fn create_transport(s: &ConnectionSettings) -> Result<Transport> {
    let uses_ca = s.choice.tls() || s.choice.id.ends_with("mqtt");
    let ca = if !uses_ca {
        String::new()
    } else if !s.ca_file.trim().is_empty() {
        tokio::fs::read_to_string(&s.ca_file)
            .await
            .context("Reading CA PEM file")?
    } else {
        s.ca_pem.clone()
    };
    if s.choice.broker() {
        let operation_timeout = Duration::from_millis(s.broker_operation_ms);
        let response_timeout = Duration::from_millis(s.broker_response_ms);
        return Ok(match s.choice.id {
            "mqtt" | "ws-mqtt" => {
                let config = MqttConfig {
                    broker_url: s.broker_url.clone(),
                    request_topic: s.broker_route.clone(),
                    operation_timeout,
                    response_timeout,
                    ca_certificate: (!ca.trim().is_empty()).then(|| ca.into_bytes()),
                    ..Default::default()
                };
                if s.choice.id == "mqtt" {
                    Arc::new(mqtt::MqttClientTransport::connect_with_config(config).await?)
                } else {
                    Arc::new(
                        websocket::mqtt::MqttWebSocketClientTransport::connect_with_config(config)
                            .await?,
                    )
                }
            }
            "amqp" | "ws-amqp" => {
                let config = AmqpConfig {
                    broker_url: s.broker_url.clone(),
                    request_queue: s.broker_route.clone(),
                    operation_timeout,
                    response_timeout,
                    ..Default::default()
                };
                if s.choice.id == "amqp" {
                    Arc::new(amqp::AmqpClientTransport::connect_with_config(config).await?)
                } else {
                    Arc::new(
                        websocket::amqp::AmqpWebSocketClientTransport::connect_with_config(config)
                            .await?,
                    )
                }
            }
            _ => unreachable!("broker choices checked above"),
        });
    }
    let host = s.host.trim().trim_start_matches('[').trim_end_matches(']');
    let address = tokio::net::lookup_host((host, s.port))
        .await?
        .next()
        .context("DNS returned no addresses")?;
    let tuning = s.tcp.clone();
    let ws_url = format!("ws://{address}{}", s.ws_path);
    let transport: Transport = match s.choice.id {
        "tcp" => Arc::new(tcp::TcpClientTransport::connect_with_tuning(address, tuning).await?),
        "tls" => Arc::new(
            tls::TlsClientTransport::connect_with_tuning(address, &s.server_name, &ca, tuning)
                .await?,
        ),
        "http1" => {
            Arc::new(http1::Http1ClientTransport::connect_with_tuning(address, tuning).await?)
        }
        "udp" => Arc::new(udp::UdpClientTransport::connect(address).await?),
        "rtp" => Arc::new(rtp::RtpClientTransport::connect(address).await?),
        "rudp" => Arc::new(rudp::RudpClientTransport::connect(address).await?),
        "coap" => Arc::new(coap::CoapClientTransport::connect(address).await?),
        "ws-tcp" => Arc::new(
            websocket::tcp::TcpWebSocketClientTransport::connect_with_tuning(
                &format!("ws://{address}"),
                tuning,
            )
            .await?,
        ),
        "ws-http1" => Arc::new(
            websocket::http1::Http1WebSocketClientTransport::connect_with_tuning(&ws_url, tuning)
                .await?,
        ),
        "ws-tls" => {
            let authority = if s.server_name.contains(':') {
                format!("[{}]", s.server_name)
            } else {
                s.server_name.clone()
            };
            Arc::new(
                websocket::http1::Http1WebSocketClientTransport::connect_with_ca_pem_to_addr(
                    &format!("wss://{authority}:{}{}", s.port, s.ws_path),
                    address,
                    &ca,
                    tuning,
                )
                .await?,
            )
        }
        "ws-udp" => Arc::new(websocket::udp::UdpWebSocketClientTransport::connect(address).await?),
        "ws-rtp" => Arc::new(websocket::rtp::RtpWebSocketClientTransport::connect(address).await?),
        "ws-rudp" => {
            Arc::new(websocket::rudp::RudpWebSocketClientTransport::connect(address).await?)
        }
        "ws-coap" => {
            Arc::new(websocket::coap::CoapWebSocketClientTransport::connect(address).await?)
        }
        "quic" | "http3" | "ws-quic" | "ws-http3" => {
            let alpn = match s.choice.id {
                "quic" => QUIC_ALPN,
                "ws-quic" => WS_QUIC_ALPN,
                _ => H3_ALPN,
            };
            let local: SocketAddr = if address.is_ipv6() {
                "[::]:0"
            } else {
                "0.0.0.0:0"
            }
            .parse()?;
            let mut tls = ClientTlsConfig::default();
            tls.set_ca_certificate_pem(&ca)?;
            let endpoint = tls.endpoint(local, alpn)?;
            match s.choice.id {
                "quic" => Arc::new(
                    quic::QuicClientTransport::connect(endpoint, address, &s.server_name).await?,
                ),
                "http3" => Arc::new(
                    http3::Http3ClientTransport::connect(endpoint, address, &s.server_name).await?,
                ),
                "ws-quic" => Arc::new(
                    websocket::quic::QuicWebSocketClientTransport::connect(
                        endpoint,
                        address,
                        &s.server_name,
                    )
                    .await?,
                ),
                _ => Arc::new(
                    websocket::http3::Http3WebSocketClientTransport::connect(
                        endpoint,
                        address,
                        &s.server_name,
                        &s.ws_path,
                    )
                    .await?,
                ),
            }
        }
        #[cfg(target_os = "linux")]
        "udplite" | "ws-udplite" => {
            let coverage = json_rpc_2::UdpLiteConfig {
                send_checksum_coverage: s.send_coverage,
                recv_checksum_coverage: s.receive_coverage,
            };
            if s.choice.id == "udplite" {
                Arc::new(
                    udplite::UdpLiteClientTransport::connect_with_configs(
                        address,
                        Default::default(),
                        coverage,
                    )
                    .await?,
                )
            } else {
                Arc::new(
                    websocket::udplite::UdpLiteWebSocketClientTransport::connect_with_configs(
                        address,
                        Default::default(),
                        coverage,
                    )
                    .await?,
                )
            }
        }
        #[cfg(target_os = "linux")]
        "dccp" => Arc::new(dccp::DccpClientTransport::connect(address).await?),
        #[cfg(target_os = "linux")]
        "sctp" => Arc::new(sctp::SctpClientTransport::connect(address).await?),
        #[cfg(target_os = "linux")]
        "ws-dccp" => {
            Arc::new(websocket::dccp::DccpWebSocketClientTransport::connect(address).await?)
        }
        #[cfg(target_os = "linux")]
        "ws-sctp" => {
            Arc::new(websocket::sctp::SctpWebSocketClientTransport::connect(address).await?)
        }
        #[cfg(feature = "native-srt")]
        "srt" => {
            Arc::new(srt::SrtClientTransport::connect(address, s.srt_passphrase.clone()).await?)
        }
        #[cfg(feature = "native-srt")]
        "ws-srt" => Arc::new(
            websocket::srt::SrtWebSocketClientTransport::connect(address, s.srt_passphrase.clone())
                .await?,
        ),
        other => bail!("Transport {other} is not available in this build"),
    };
    Ok(transport)
}
