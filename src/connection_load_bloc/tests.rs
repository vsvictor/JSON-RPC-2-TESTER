use super::*;
use json_rpc_2_tester::demo;
use std::sync::atomic::{AtomicU8, AtomicU32};

fn settings(port: u16, ca_pem: String) -> GatewaySettings {
    GatewaySettings {
        host: "127.0.0.1".into(),
        port,
        control_host: "127.0.0.1".into(),
        server_name: "localhost".into(),
        ca_file: String::new(),
        ca_pem,
        connect_timeout_ms: 5000,
        retry_delay_ms: 0,
        control_port: 8500,
        control_timeout_ms: 1000,
        http_service_port: 8508,
        recovery_attempts: Arc::new(AtomicU32::new(1)),
        transport_policy: Arc::new(AtomicU8::new(7)),
        gateway_up_return_sec: 30,
    }
}

async fn wait_until(state: &Arc<Mutex<State>>, predicate: impl Fn(&State) -> bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if predicate(&state.lock().unwrap()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("load-test state did not reach expected condition");
}

#[tokio::test]
async fn adds_concurrent_connections_pings_and_closes_on_stop() {
    let server = demo::start().await.unwrap();
    for kind in ["tcp", "tls", "quic"] {
        let port = server
            .running
            .listeners()
            .find(|(transport, _)| transport.id() == kind)
            .unwrap()
            .1
            .port();
        let state = Arc::new(Mutex::new(State {
            phase: Phase::Running,
            started: Some(Instant::now()),
            ..State::default()
        }));
        let (stop, receiver) = watch::channel(false);
        let task = tokio::spawn(run(
            state.clone(),
            settings(port, server.ca_pem.clone()),
            kind.into(),
            receiver,
            LoadSettings {
                step_size: 10,
                step_interval_seconds: 2,
                ping_interval_seconds: 5,
            },
        ));
        wait_until(&state, |state| {
            state.steps >= 2 && state.connected >= 20 && state.ping_ok >= 20
        })
        .await;
        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(8), task)
            .await
            .unwrap()
            .unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.phase, Phase::Stopped, "{kind}");
        assert_eq!(state.connecting, 0, "{kind}");
        assert_eq!(state.connected, 0, "{kind}");
        assert!(state.closed >= 20, "{kind}");
        assert_eq!(state.failed_connections, 0, "{kind}");
        assert_eq!(state.ping_failed, 0, "{kind}");
        assert_eq!(state.close_failed, 0, "{kind}");
    }
    server.close().await.unwrap();
}

#[tokio::test]
async fn reports_failed_connections_without_counting_them_as_active() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let state = Arc::new(Mutex::new(State {
        phase: Phase::Running,
        started: Some(Instant::now()),
        ..State::default()
    }));
    let (stop, receiver) = watch::channel(false);
    let task = tokio::spawn(run(
        state.clone(),
        settings(port, "unused".into()),
        "tcp".into(),
        receiver,
        LoadSettings {
            step_size: 100,
            step_interval_seconds: 5,
            ping_interval_seconds: 4,
        },
    ));
    wait_until(&state, |state| state.failed_connections == 100).await;
    stop.send(true).unwrap();
    task.await.unwrap();
    let state = state.lock().unwrap();
    assert_eq!(state.attempted, 100);
    assert_eq!(state.connected, 0);
    assert_eq!(state.connecting, 0);
    assert!(!state.errors.is_empty());
    assert!(
        state
            .errors
            .iter()
            .all(|error| error.starts_with("Сторона невідома / з'єднання:"))
    );
}

#[tokio::test]
async fn reports_ping_failure_after_peer_disconnects() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            drop(stream);
        }
    });
    let state = Arc::new(Mutex::new(State {
        phase: Phase::Running,
        started: Some(Instant::now()),
        ..State::default()
    }));
    let (stop, receiver) = watch::channel(false);
    let task = tokio::spawn(run(
        state.clone(),
        settings(port, "unused".into()),
        "tcp".into(),
        receiver,
        LoadSettings {
            step_size: 20,
            step_interval_seconds: 5,
            ping_interval_seconds: 4,
        },
    ));
    wait_until(&state, |state| state.ping_failed > 0).await;
    stop.send(true).unwrap();
    task.await.unwrap();
    server.abort();
    let state = state.lock().unwrap();
    assert_eq!(state.connected, 0);
    assert!(
        state
            .errors
            .iter()
            .any(|error| error.starts_with("Сторона невідома / ping:"))
    );
}

#[test]
fn connection_errors_indicate_observed_side_without_guessing() {
    let local = LoadConnectError::Client(anyhow::anyhow!("invalid settings"));
    let timeout = LoadConnectError::Timeout { timeout_ms: 1000 };
    let transport = LoadConnectError::Transport(anyhow::anyhow!("connection reset"));
    let local = connection_error_message(&local);
    assert!(local.starts_with("Клієнт /"));
    assert!(local.contains("invalid settings"));
    let timeout = connection_error_message(&timeout);
    assert!(timeout.starts_with("Клієнт /"));
    assert!(timeout.contains("1000 мс"));
    assert!(timeout.contains("причина затримки невідома"));
    let transport = connection_error_message(&transport);
    assert!(transport.starts_with("Сторона невідома /"));
    assert!(transport.contains("connection reset"));
}

#[tokio::test]
async fn unsupported_transport_reports_client_error() {
    let error =
        match connect_gateway_test_transport(&settings(1234, String::new()), "invalid").await {
            Ok(_) => panic!("unsupported transport unexpectedly succeeded"),
            Err(error) => error,
        };
    assert!(matches!(error, LoadConnectError::Client(_)));
    assert!(connection_error_message(&error).starts_with("Клієнт /"));
}

#[tokio::test]
async fn stalled_tls_handshake_reports_local_timeout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        drop(stream);
    });
    let ca = rcgen::generate_simple_self_signed(vec!["localhost".into()])
        .unwrap()
        .cert
        .pem();
    let mut settings = settings(port, ca);
    settings.connect_timeout_ms = 100;
    let error = match connect_gateway_test_transport(&settings, "tls").await {
        Ok(_) => panic!("stalled TLS handshake unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        LoadConnectError::Timeout { timeout_ms: 100 }
    ));
    assert!(connection_error_message(&error).contains("причина затримки невідома"));
    server.abort();
}

#[tokio::test]
async fn ping_runs_at_the_configured_interval() {
    let server = demo::start().await.unwrap();
    let port = server
        .running
        .listeners()
        .find(|(kind, _)| kind.id() == "tcp")
        .unwrap()
        .1
        .port();
    let state = Arc::new(Mutex::new(State {
        phase: Phase::Running,
        connecting: 1,
        ..State::default()
    }));
    let (stop, receiver) = watch::channel(false);
    let task = tokio::spawn(connection_task(
        state.clone(),
        settings(port, server.ca_pem.clone()),
        "tcp".into(),
        receiver,
        Duration::from_secs(2),
    ));
    wait_until(&state, |state| state.ping_ok == 1).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(state.lock().unwrap().ping_ok, 1);
    wait_until(&state, |state| state.ping_ok >= 2).await;
    stop.send(true).unwrap();
    task.await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn production_defaults_add_one_thousand_connections_every_five_seconds() {
    let load = LoadSettings::default();
    assert_eq!(load.step_size, 1000);
    assert_eq!(load.step_interval_seconds, 5);
    assert_eq!(load.ping_interval_seconds, 4);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let state = Arc::new(Mutex::new(State {
        phase: Phase::Running,
        started: Some(Instant::now()),
        ..State::default()
    }));
    let (stop, receiver) = watch::channel(false);
    let task = tokio::spawn(run(
        state.clone(),
        settings(port, "unused".into()),
        "tcp".into(),
        receiver,
        load,
    ));
    wait_until(&state, |state| state.attempted == 1000).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(state.lock().unwrap().attempted, 1000);
    wait_until(&state, |state| state.attempted == 2000).await;
    stop.send(true).unwrap();
    task.await.unwrap();
}

#[test]
fn validates_settings_and_locks_them_while_running() {
    let mut bloc = ConnectionLoadBloc::default();
    let valid = LoadSettings {
        step_size: 7,
        step_interval_seconds: 2,
        ping_interval_seconds: 3,
    };
    for invalid in [
        LoadSettings {
            step_size: 0,
            ..valid
        },
        LoadSettings {
            step_interval_seconds: 0,
            ..valid
        },
        LoadSettings {
            ping_interval_seconds: 0,
            ..valid
        },
    ] {
        assert!(bloc.set_settings(invalid).is_err());
    }
    bloc.set_settings(valid).unwrap();
    assert_eq!(bloc.settings(), valid);
    bloc.state().phase = Phase::Running;
    assert!(bloc.set_settings(LoadSettings::default()).is_err());
    assert_eq!(bloc.settings(), valid);
    bloc.state().phase = Phase::Stopping;
    assert!(bloc.set_settings(LoadSettings::default()).is_err());
    bloc.state().phase = Phase::Stopped;
    bloc.set_settings(LoadSettings::default()).unwrap();
}

#[tokio::test]
async fn custom_step_size_and_interval_are_used() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let state = Arc::new(Mutex::new(State::default()));
    let (stop, receiver) = watch::channel(false);
    let load = LoadSettings {
        step_size: 7,
        step_interval_seconds: 2,
        ping_interval_seconds: 3,
    };
    let task = tokio::spawn(run(
        state.clone(),
        settings(port, "unused".into()),
        "tcp".into(),
        receiver,
        load,
    ));
    wait_until(&state, |state| state.attempted == 7).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(state.lock().unwrap().attempted, 7);
    wait_until(&state, |state| state.attempted == 14).await;
    stop.send(true).unwrap();
    task.await.unwrap();
}
