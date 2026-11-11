use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicU32},
};

use json_rpc_2_tester::connection::GatewaySettings;

fn gateway_settings(control_host: &str) -> GatewaySettings {
    GatewaySettings {
        host: "gateway.example".into(),
        port: 40400,
        control_host: control_host.into(),
        server_name: "gateway.example".into(),
        ca_file: "/tmp/gateway-ca.pem".into(),
        ca_pem: "test CA".into(),
        connect_timeout_ms: 10_000,
        retry_delay_ms: 500,
        control_port: 8500,
        control_timeout_ms: 5000,
        http_service_port: 8508,
        recovery_attempts: Arc::new(AtomicU32::new(3)),
        transport_policy: Arc::new(AtomicU8::new(0b0000_0111)),
        gateway_up_return_sec: 30,
    }
}

#[test]
fn accepts_private_control_plane_ipv4_addresses() {
    for address in ["192.168.1.132", "10.0.0.8", "172.20.1.5", "127.0.0.1"] {
        assert!(
            gateway_settings(address).validate().is_ok(),
            "expected {address} to be accepted"
        );
    }
}

#[test]
fn rejects_non_private_control_plane_addresses() {
    for address in ["", "gateway.example", "8.8.8.8"] {
        assert!(
            gateway_settings(address).validate().is_err(),
            "expected {address:?} to be rejected"
        );
    }
}
