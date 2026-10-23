use std::time::Duration;

use anyhow::Context;
use json_rpc_2::client::RpcClientError;
use json_rpc_2_tester::{
    connection::{ConnectionSettings, RuntimeSettings, TRANSPORTS, connect},
    demo, requests,
};
use serde::Deserialize;
use serde_json::{Value, json};

#[tokio::test]
async fn real_rpc_roundtrips_on_every_sandbox_transport() -> anyhow::Result<()> {
    let server = demo::start().await?;
    let endpoints: Vec<_> = server.running.listeners().collect();
    assert_eq!(endpoints.len(), 16);
    for (kind, address) in endpoints {
        eprintln!("Exercising {}", kind.id());
        let choice = *TRANSPORTS
            .iter()
            .find(|choice| choice.id == kind.id())
            .unwrap();
        let settings = ConnectionSettings {
            choice,
            port: address.port(),
            ca_pem: server.ca_pem.clone(),
            ..Default::default()
        };
        let (client, transport) = connect(settings, RuntimeSettings::default())
            .await
            .context("connect")?;
        client.transport_ping().await.context("initial ping")?;
        assert_eq!(
            client
                .request_value("echo", Some(json!({"transport": kind.id()})))
                .await?,
            json!({"transport": kind.id()})
        );
        let sum: f64 = client.request("sum", [1, 2, 3]).await?;
        assert_eq!(sum, 6.0);

        #[derive(Deserialize)]
        struct Greeting {
            message: String,
            source: String,
        }
        let greeting: Greeting = client.request("greet", json!({"name": "test"})).await?;
        assert_eq!(greeting.message, "Hello, test!");
        assert_eq!(greeting.source, "Rust desktop demo");
        let info: Value = client.request_without_params("info").await?;
        assert_eq!(info["application"], "JSON-RPC-2 Desktop");
        client
            .notify("echo", Some(json!({"notification": true})))
            .await?;
        let responses =
            requests::execute_batch(&client, requests::batch(requests::DEFAULT_BATCH)?).await?;
        assert_eq!(responses.as_array().unwrap().len(), 3);
        assert_eq!(responses[0]["result"]["message"], "Hello, desktop!");
        assert_eq!(responses[1]["result"], 6.0);
        assert_eq!(responses[2]["error"]["code"], -32601);
        let notifications = requests::batch(r#"[{"method":"echo","notification":true}]"#)?;
        assert_eq!(
            requests::execute_batch(&client, notifications).await?,
            json!([])
        );
        let results =
            requests::parallel(client.clone(), "sum".into(), Some(json!([2, 3])), 4).await?;
        for (index, result) in results.as_array().unwrap().iter().enumerate() {
            assert_eq!(result["index"], index);
            assert_eq!(result["result"], 5.0);
        }
        match client.request_value("fail", None).await.unwrap_err() {
            RpcClientError::Remote(error) => {
                assert_eq!(error.code, -32001);
                assert!(error.data.is_some());
            }
            other => panic!("unexpected error on {}: {other}", kind.id()),
        }
        transport.restore().await.context("restore")?;
        transport.reconnect().await.context("reconnect")?;
        client
            .transport_ping()
            .await
            .context("ping after reconnect")?;
        assert_eq!(
            client
                .request_value("echo", Some(json!({"reconnected": true})))
                .await?,
            json!({"reconnected": true})
        );
        client.close().await.context("close")?;
        assert!(client.request_value("echo", None).await.is_err());
    }
    server.close().await?;
    Ok(())
}

#[tokio::test]
async fn timeout_cancellation_liveness_and_live_policy() -> anyhow::Result<()> {
    let server = demo::start().await?;
    let (_, address) = server
        .running
        .listeners()
        .find(|(kind, _)| kind.id() == "tcp")
        .unwrap();
    let settings = ConnectionSettings {
        port: address.port(),
        ..Default::default()
    };
    let (client, _) = connect(settings, RuntimeSettings::default()).await?;

    client.set_request_timeout_millis(30)?;
    assert!(matches!(
        client.request_value("slow", Some(json!({"ms": 300}))).await,
        Err(RpcClientError::Timeout { .. })
    ));
    assert_eq!(client.metrics().request_timeouts_total, 1);
    client.set_request_timeout_millis(5000)?;
    let cloned = client.clone();
    let task =
        tokio::spawn(async move { cloned.request_value("slow", Some(json!({"ms": 300}))).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.metrics().active_requests == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.metrics().cancelled_requests_total == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    assert_eq!(client.metrics().active_requests, 0);

    let policy = RuntimeSettings {
        ping_ms: 20,
        timeout_ms: 2000,
        max_in_flight: 128,
        ..Default::default()
    };
    let config = policy.config()?;
    policy.apply(&client).await?;
    assert_eq!(client.config(), config);
    let invalid_policy = RuntimeSettings {
        queue_capacity: 16,
        ..policy.clone()
    };
    assert!(invalid_policy.apply(&client).await.is_err());
    assert_eq!(client.config(), config);
    let invalid_policy = RuntimeSettings {
        max_in_flight: 1,
        ..policy
    };
    assert!(invalid_policy.apply(&client).await.is_err());
    assert_eq!(client.config(), config);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(client.metrics().missed_pong_streak, 0);
    assert_eq!(
        client.request_value("echo", Some(json!([42]))).await?,
        json!([42])
    );
    client.set_ping_frequency(0).await?;
    client.close().await?;
    server.close().await?;
    Ok(())
}

#[test]
fn rejects_invalid_configuration_before_network_io() {
    assert!(
        RuntimeSettings {
            timeout_ms: 0,
            ..Default::default()
        }
        .config()
        .is_err()
    );
    assert!(
        RuntimeSettings {
            queue_capacity: 0,
            ..Default::default()
        }
        .config()
        .is_err()
    );
    assert!(
        RuntimeSettings {
            max_in_flight: 0,
            ..Default::default()
        }
        .config()
        .is_err()
    );
    assert!(
        RuntimeSettings {
            missed_pongs: 0,
            ..Default::default()
        }
        .config()
        .is_err()
    );
    assert!(
        RuntimeSettings {
            cooldown_ms: 500,
            max_backoff_ms: 100,
            ..Default::default()
        }
        .config()
        .is_err()
    );
    assert!(
        ConnectionSettings {
            port: 0,
            ..Default::default()
        }
        .validate()
        .is_err()
    );
    let tls = *TRANSPORTS.iter().find(|choice| choice.id == "tls").unwrap();
    assert!(
        ConnectionSettings {
            choice: tls,
            ..Default::default()
        }
        .validate()
        .is_err()
    );
    for choice in TRANSPORTS {
        if choice.unavailable().is_some() {
            assert!(
                ConnectionSettings {
                    choice: *choice,
                    ..Default::default()
                }
                .validate()
                .is_err()
            );
        }
    }
    let ids: std::collections::HashSet<_> = TRANSPORTS.iter().map(|choice| choice.id).collect();
    assert_eq!(ids.len(), 30);
}

#[tokio::test]
async fn tls_rejects_wrong_identity_and_untrusted_certificates() -> anyhow::Result<()> {
    let server = demo::start().await?;
    for (kind, address) in server.running.listeners().filter(|(kind, _)| {
        matches!(
            kind.id(),
            "tls" | "quic" | "http3" | "ws-tls" | "ws-quic" | "ws-http3"
        )
    }) {
        let settings = ConnectionSettings {
            choice: *TRANSPORTS
                .iter()
                .find(|choice| choice.id == kind.id())
                .unwrap(),
            port: address.port(),
            ca_pem: server.ca_pem.clone(),
            server_name: "wrong.example".into(),
            connect_timeout_ms: 1500,
            ..Default::default()
        };
        assert!(
            connect(settings.clone(), RuntimeSettings::default())
                .await
                .is_err(),
            "{}",
            kind.id()
        );
        let other = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        let settings = ConnectionSettings {
            server_name: "localhost".into(),
            ca_pem: other.cert.pem(),
            ..settings
        };
        assert!(
            connect(settings, RuntimeSettings::default()).await.is_err(),
            "{}",
            kind.id()
        );
    }
    server.close().await?;
    Ok(())
}
