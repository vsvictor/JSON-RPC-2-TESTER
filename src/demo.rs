use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use json_rpc_2::{MultiTransportServer, RpcError, RpcServer, RunningServer, TransportKind};
use serde_json::{Value, json};

pub struct DemoServer {
    pub running: RunningServer,
    pub ca_pem: String,
}

impl DemoServer {
    pub async fn close(self) -> Result<()> {
        self.running.close().await?;
        Ok(())
    }
}

pub async fn start() -> Result<DemoServer> {
    let rpc = Arc::new(RpcServer::new());
    rpc.register("echo", |params| async move { Ok(params) })?;
    rpc.register("sum", |params| async move {
        let values = params
            .as_array()
            .ok_or_else(|| RpcError::new(-32602, "Expected an array of numbers"))?;
        let mut sum = 0.0;
        for value in values {
            let number = value
                .as_f64()
                .ok_or_else(|| RpcError::new(-32602, "Expected numbers"))?;
            sum += number;
        }
        if !sum.is_finite() {
            return Err(RpcError::new(-32602, "Sum is not finite"));
        }
        Ok(json!(sum))
    })?;
    rpc.register("greet", |params| async move {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| RpcError::new(-32602, "Expected {\"name\": string}"))?;
        Ok(json!({"message": format!("Hello, {name}!"), "source": "Rust desktop demo"}))
    })?;
    rpc.register("slow", |params| async move {
        let ms = params
            .get("ms")
            .and_then(Value::as_u64)
            .filter(|ms| *ms <= 60_000)
            .ok_or_else(|| RpcError::new(-32602, "Expected {\"ms\": 0..60000}"))?;
        tokio::time::sleep(Duration::from_millis(ms)).await;
        Ok(json!({"slept_ms": ms}))
    })?;
    rpc.register("fail", |_| async {
        let mut error = RpcError::new(-32001, "Intentional demonstration error");
        error.data = Some(json!({"reason": "requested by the showcase"}));
        Err(error)
    })?;
    rpc.register("info", |_| async {
        Ok(json!({"application": "JSON-RPC-2 Desktop", "methods": ["echo", "sum", "greet", "slow", "fail", "info"]}))
    })?;
    let mut server = MultiTransportServer::new(rpc);
    server.set_bind([IpAddr::V4(Ipv4Addr::LOCALHOST)]);
    let identity =
        rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])?;
    let ca_pem = identity.cert.pem();
    let key = identity
        .key_pair
        .serialize_der()
        .try_into()
        .map_err(anyhow::Error::msg)?;
    server.set_tls(vec![identity.cert.der().clone()], key);
    // Ephemeral ports avoid clashing with an existing upstream example server.
    for kind in [
        TransportKind::Tcp,
        TransportKind::Udp,
        TransportKind::Rtp,
        TransportKind::Rudp,
        TransportKind::Http1,
        TransportKind::WebSocketTcp,
        TransportKind::WebSocketUdp,
        TransportKind::WebSocketRtp,
        TransportKind::WebSocketRudp,
        TransportKind::WebSocketHttp1,
        TransportKind::Tls,
        TransportKind::Quic,
        TransportKind::Http3,
        TransportKind::WebSocketTls,
        TransportKind::WebSocketQuic,
        TransportKind::WebSocketHttp3,
    ] {
        server.set_port(kind, 0);
    }
    let running = server.start().await.context("Starting local demo server")?;
    Ok(DemoServer { running, ca_pem })
}
