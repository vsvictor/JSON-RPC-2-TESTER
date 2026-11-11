use std::{collections::BTreeMap, net::Ipv4Addr, time::Duration};

use anyhow::{Context, Result, ensure};
use reqwest::{Method, Url};
use serde_json::{Value, json};

const MAX_BODY_BYTES: usize = 32 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

pub fn endpoint_url(host: &str, port: u16, endpoint: &str) -> Result<Url> {
    let ip: Ipv4Addr = host
        .trim()
        .parse()
        .context("HTTP service host must be an IPv4 address")?;
    ensure!(
        ip.is_private() || ip.is_loopback(),
        "HTTP service host must be a private or loopback IPv4 address"
    );
    ensure!(port != 0, "HTTP service port must be nonzero");

    let endpoint = endpoint.trim();
    ensure!(!endpoint.is_empty(), "HTTP endpoint is required");
    ensure!(
        !endpoint.contains(['\r', '\n']) && !endpoint.starts_with("//"),
        "HTTP endpoint must be a relative path"
    );
    ensure!(
        !endpoint.contains("://"),
        "HTTP endpoint must not contain a URL scheme"
    );

    let base = Url::parse(&format!("http://{ip}:{port}/"))?;
    Ok(base.join(endpoint.trim_start_matches('/'))?)
}

pub async fn send_request(
    host: &str,
    port: u16,
    method: &str,
    endpoint: &str,
    body: &str,
) -> Result<Value> {
    ensure!(
        body.len() <= MAX_BODY_BYTES,
        "HTTP request body exceeds {MAX_BODY_BYTES} bytes"
    );
    let method = Method::from_bytes(method.as_bytes()).context("Invalid HTTP method")?;
    ensure!(
        matches!(
            method,
            Method::GET | Method::POST | Method::PUT | Method::DELETE
        ),
        "HTTP method must be GET, POST, PUT, or DELETE"
    );
    let url = endpoint_url(host, port, endpoint)?;
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("Creating HTTP service client")?;
    let mut request = client.request(method.clone(), url.clone());
    if !body.is_empty() {
        request = request
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_owned());
    }

    let mut response = request
        .send()
        .await
        .with_context(|| format!("Sending {method} request to {url}"))?;
    let status = response.status();
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let content_length = response.content_length().unwrap_or_default();
    ensure!(
        content_length <= MAX_BODY_BYTES as u64,
        "HTTP response body exceeds {MAX_BODY_BYTES} bytes"
    );
    let mut response_body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("Reading HTTP service response")?
    {
        ensure!(
            response_body.len() + chunk.len() <= MAX_BODY_BYTES,
            "HTTP response body exceeds {MAX_BODY_BYTES} bytes"
        );
        response_body.extend_from_slice(&chunk);
    }

    Ok(json!({
        "url": url.as_str(),
        "method": method.as_str(),
        "status": status.as_u16(),
        "reason": status.canonical_reason(),
        "headers": headers,
        "body": String::from_utf8_lossy(&response_body),
    }))
}

#[cfg(test)]
#[path = "http_service/tests.rs"]
mod tests;
