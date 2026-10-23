use anyhow::{Context, Result, ensure};
use json_rpc_2::client::{BatchRequest, RpcClientError};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};

use crate::connection::Client;

pub fn params(text: &str) -> Result<Option<Value>> {
    if text.trim().is_empty() {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(text).context("Invalid parameters JSON")?;
    ensure!(
        value.is_object() || value.is_array(),
        "Parameters must be an object or array; leave empty to omit params"
    );
    Ok(Some(value))
}

pub fn method(text: &str) -> Result<String> {
    ensure!(!text.trim().is_empty(), "Method name is required");
    Ok(text.trim().into())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchItem {
    method: String,
    #[serde(default, deserialize_with = "structured_params")]
    params: Option<Value>,
    #[serde(default)]
    notification: bool,
}

fn structured_params<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Value>, D::Error> {
    let value = Value::deserialize(deserializer)?;
    if value.is_object() || value.is_array() {
        Ok(Some(value))
    } else {
        Err(serde::de::Error::custom(
            "params must be an object or array",
        ))
    }
}

pub fn batch(text: &str) -> Result<Vec<BatchRequest>> {
    let items: Vec<BatchItem> = serde_json::from_str(text).context("Invalid batch JSON")?;
    ensure!(
        !items.is_empty() && items.len() <= 256,
        "Batch must contain 1..=256 entries"
    );
    items
        .into_iter()
        .map(|item| {
            let method = method(&item.method)?;
            if let Some(params) = &item.params {
                ensure!(
                    params.is_array() || params.is_object(),
                    "Batch parameters must be objects or arrays"
                );
            }
            Ok(if item.notification {
                BatchRequest::notification(method, item.params)
            } else {
                BatchRequest::call(method, item.params)
            })
        })
        .collect()
}

pub fn error_json(error: &RpcClientError) -> Value {
    match error {
        RpcClientError::Remote(remote) => json!({
            "kind": "remote", "code": remote.code, "message": remote.message, "data": remote.data
        }),
        other => json!({"kind": "client", "message": other.to_string()}),
    }
}

pub async fn execute_batch(client: &Client, requests: Vec<BatchRequest>) -> Result<Value> {
    let responses = client.batch(requests).await?;
    Ok(Value::Array(
        responses
            .into_iter()
            .map(|response| match response.result {
                Ok(value) => json!({"id": response.id, "result": value}),
                Err(error) => json!({"id": response.id, "error": error}),
            })
            .collect(),
    ))
}

pub async fn parallel(
    client: Client,
    name: String,
    params: Option<Value>,
    count: usize,
) -> Result<Value> {
    ensure!(
        (1..=64).contains(&count),
        "Parallel request count must be 1..=64"
    );
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..count {
        let client = client.clone();
        let name = name.clone();
        let params = params.clone();
        tasks.spawn(async move {
            let result = client.request_value(name, params).await;
            (index, result)
        });
    }
    let mut results = vec![Value::Null; count];
    while let Some(result) = tasks.join_next().await {
        let (index, result) = result.context("Parallel request task failed")?;
        results[index] = match result {
            Ok(value) => json!({"index": index, "result": value}),
            Err(error) => json!({"index": index, "error": error_json(&error)}),
        };
    }
    Ok(Value::Array(results))
}

pub const DEFAULT_BATCH: &str = r#"[
  {"method": "echo", "params": {"message": "Hello, desktop!"}},
  {"method": "sum", "params": [1, 2, 3]},
  {"method": "echo", "params": {"event": "notification"}, "notification": true},
  {"method": "missing.method"}
]"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parameters_are_structured_or_absent() {
        assert_eq!(params(" ").unwrap(), None);
        assert!(params("{}").is_ok());
        assert!(params("[]").is_ok());
        for text in ["null", "1", "\"x\"", "{broken"] {
            assert!(params(text).is_err(), "{text}");
        }
    }

    #[test]
    fn batches_are_validated() {
        assert_eq!(batch(DEFAULT_BATCH).unwrap().len(), 4);
        for text in [
            "[]",
            "{}",
            r#"[{"method":""}]"#,
            r#"[{"method":"x","params":2}]"#,
            r#"[{"method":"x","params":null}]"#,
            r#"[{"method":"x","id":1}]"#,
        ] {
            assert!(batch(text).is_err(), "{text}");
        }
    }
}
