//! Integration test hitting public HTTP/2 servers over TLS.
//!
//! Requires the "tls" feature. Skipped when network is unavailable.

#![cfg(feature = "tls")]

use std::sync::LazyLock;
use std::time::Duration;

use crucible_http_client::{HttpClient, HttpClientConfig};

struct TlsTestEnv {
    client: HttpClient,
}

static ENV: LazyLock<TlsTestEnv> = LazyLock::new(|| {
    let client = HttpClient::connect(HttpClientConfig {
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
    .expect("HttpClient::connect failed");

    TlsTestEnv { client }
});

/// Wait for the TLS + HTTP/2 connection to become ready.
async fn wait_ready(client: &HttpClient) -> bool {
    for _ in 0..50 {
        match client.get("/").await {
            Ok(resp) if resp.status() > 0 => return true,
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

#[tokio::test]
async fn test_tls_get_google() {
    let client = &ENV.client;
    if !wait_ready(client).await {
        eprintln!("SKIPPED: could not connect to www.google.com:443 (network unavailable?)");
        return;
    }

    let resp = client.get("/").await.unwrap();
    // Google returns 200 or 301/302 redirect
    assert!(
        resp.status() == 200 || resp.status() == 301 || resp.status() == 302,
        "unexpected status: {}",
        resp.status()
    );
    // Should have some body content
    assert!(!resp.body().is_empty(), "response body should not be empty");
    println!(
        "GET https://www.google.com/ → {} ({} bytes)",
        resp.status(),
        resp.body().len()
    );
}

#[tokio::test]
async fn test_tls_get_google_generate_204() {
    let client = &ENV.client;
    if !wait_ready(client).await {
        eprintln!("SKIPPED: could not connect to www.google.com:443");
        return;
    }

    let resp = client.get("/generate_204").await.unwrap();
    assert_eq!(resp.status(), 204, "expected 204 No Content");
    println!(
        "GET https://www.google.com/generate_204 → {}",
        resp.status()
    );
}

#[tokio::test]
async fn test_tls_concurrent_requests() {
    let client = &ENV.client;
    if !wait_ready(client).await {
        eprintln!("SKIPPED: could not connect");
        return;
    }

    // Multiple concurrent requests over a single HTTP/2 connection
    let mut handles = Vec::new();
    for _ in 0..3 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let resp = c.get("/generate_204").await.unwrap();
            assert_eq!(resp.status(), 204);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
    println!("3 concurrent requests to /generate_204 all returned 204");
}

#[tokio::test]
async fn test_tls_latency_recorded() {
    let client = &ENV.client;
    if !wait_ready(client).await {
        eprintln!("SKIPPED: could not connect");
        return;
    }

    let _ = client.get("/generate_204").await.unwrap();

    let lat = client.latency();
    assert!(
        lat.request().load().is_some(),
        "request latency should be recorded"
    );
    assert!(lat.get().load().is_some(), "GET latency should be recorded");
    if let Some(snap) = lat.request().load()
        && let Ok(Some(pcts)) = snap.percentiles(&[50.0, 99.0])
    {
        let p50 = pcts.first().map(|(_, b)| b.end() as f64).unwrap_or(0.0);
        let p99 = pcts.get(1).map(|(_, b)| b.end() as f64).unwrap_or(0.0);
        println!("Latency: p50={:.1}ms p99={:.1}ms", p50 / 1e6, p99 / 1e6);
    }
}
