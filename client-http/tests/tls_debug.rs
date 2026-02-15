//! Minimal TLS debug test.
#![cfg(feature = "tls")]

use std::sync::LazyLock;
use std::time::Duration;
use crucible_http_client::{HttpClient, HttpClientConfig};

static CLIENT: LazyLock<HttpClient> = LazyLock::new(|| {
    HttpClient::connect(HttpClientConfig {
        servers: vec!["www.google.com:443".to_string()],
        workers: 1,
        connections_per_server: 1,
        connect_timeout_ms: 10_000,
        tls: true,
        tls_server_name: Some("www.google.com".to_string()),
        default_authority: Some("www.google.com".to_string()),
        default_scheme: "https".to_string(),
        ..Default::default()
    })
    .expect("connect failed")
});

async fn wait_ready() -> bool {
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        match CLIENT.get("/generate_204").await {
            Ok(resp) if resp.status() == 204 => return true,
            _ => {}
        }
    }
    false
}

#[tokio::test]
async fn debug_tls_get_with_body() {
    if !wait_ready().await {
        eprintln!("SKIPPED: could not connect");
        return;
    }

    // Now test a path that returns a body
    for i in 0..5 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        match CLIENT.get("/robots.txt").await {
            Ok(resp) => {
                println!("robots attempt {i}: status={} body_len={}", resp.status(), resp.body().len());
                let body_str = String::from_utf8_lossy(resp.body());
                println!("body: {}", &body_str[..body_str.len().min(200)]);
                assert!(resp.body().len() > 0, "expected non-empty body");
                return;
            }
            Err(e) => {
                println!("robots attempt {i}: error={e}");
            }
        }
    }
    panic!("never got robots.txt body");
}
