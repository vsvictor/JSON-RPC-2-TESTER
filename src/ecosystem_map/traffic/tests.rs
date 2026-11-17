use super::*;
use json_rpc_2_tester::ecosystem::parse_response;
use serde_json::json;

fn inventory(gateway: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "success": true,
        "components": {
            "gateway": gateway,
            "balancer": {"version": "1", "execution": "in_process", "backend_count": 0},
            "backends": []
        }
    }))
    .unwrap()
}

fn gateway() -> serde_json::Value {
    json!({
        "version": "1",
        "pid": 1,
        "uptime_seconds": 1,
        "listeners": [],
        "control_address": "127.0.0.1:8500",
        "metrics_address": "127.0.0.1:9090"
    })
}

#[test]
fn displays_traffic_counters_and_missing_values() {
    let mut value = gateway();
    value["inbound_traffic_bytes"] = json!(0);
    value["outbound_traffic_bytes"] = json!(u64::MAX);
    let components = parse_response(&inventory(value)).unwrap();
    let lines = gateway_traffic_lines(&components.gateway, CLOSED);
    assert_eq!(lines[0].0, "Вхідний трафік: 0 B");
    assert_eq!(lines[1].0, format!("Вихідний трафік: {} B", u64::MAX));

    let components = parse_response(&inventory(gateway())).unwrap();
    let lines = gateway_traffic_lines(&components.gateway, CLOSED);
    assert_eq!(lines[0].0, "Вхідний трафік: недоступно");
    assert_eq!(lines[1].0, "Вихідний трафік: недоступно");
}

#[test]
fn rejects_invalid_traffic_counters() {
    for invalid in [json!(-1), json!("100"), json!(1.5)] {
        let mut value = gateway();
        value["inbound_traffic_bytes"] = invalid;
        assert!(parse_response(&inventory(value)).is_err());
    }
}
