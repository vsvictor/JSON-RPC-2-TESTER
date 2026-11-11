use super::*;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};

#[test]
fn builds_relative_endpoint_urls_from_the_configured_private_service_address() {
    let url = endpoint_url("192.168.1.132", 8508, "/api/items?active=true").unwrap();
    assert_eq!(
        url.as_str(),
        "http://192.168.1.132:8508/api/items?active=true"
    );
    assert!(endpoint_url("8.8.8.8", 8508, "/").is_err());
    assert!(endpoint_url("192.168.1.132", 8508, "https://example.com/").is_err());
    assert!(endpoint_url("192.168.1.132", 8508, "//example.com/").is_err());
}

#[tokio::test]
async fn sends_each_supported_http_method_and_optional_json_body() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let server = tokio::spawn(async move {
        for expected_method in ["GET", "POST", "PUT", "DELETE"] {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut request_line = String::new();
            reader.read_line(&mut request_line).await.unwrap();
            assert_eq!(
                request_line,
                format!("{expected_method} /api/items/1 HTTP/1.1\r\n")
            );

            let mut content_length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    content_length = value.trim().parse::<usize>().unwrap();
                }
            }
            let mut body = vec![0; content_length];
            reader.read_exact(&mut body).await.unwrap();
            if matches!(expected_method, "POST" | "PUT") {
                assert_eq!(body, br#"{"enabled":true}"#);
            } else {
                assert!(body.is_empty());
            }

            reader
                .get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                .await
                .unwrap();
        }
    });

    for method in ["GET", "POST", "PUT", "DELETE"] {
        let body = if matches!(method, "POST" | "PUT") {
            r#"{"enabled":true}"#
        } else {
            ""
        };
        let response = send_request("127.0.0.1", port, method, "/api/items/1", body).await?;
        assert_eq!(response["status"], 200);
        assert_eq!(response["body"], "{}");
    }
    server.await?;
    Ok(())
}
