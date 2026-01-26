//! HTTP admin server for health checks and metrics exposition.
//!
//! Provides the following endpoints:
//! - `GET /health` - Liveness probe (always returns 200 OK)
//! - `GET /ready` - Readiness probe (returns 200 OK when server is ready)
//! - `GET /metrics` - Prometheus-formatted metrics

use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Handle returned by `AdminServer::start()` for shutdown coordination.
pub struct AdminHandle {
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    join_handle: std::thread::JoinHandle<()>,
}

impl AdminHandle {
    /// Signal the admin server to shut down and wait for it to finish.
    pub fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.join_handle.join();
    }
}

/// Configuration for the admin server.
pub struct AdminConfig {
    /// Address to bind the admin server to.
    pub address: SocketAddr,
    /// Shared shutdown flag to check if server is shutting down.
    pub shutdown: Arc<AtomicBool>,
}

/// Start the admin server in a dedicated thread.
///
/// The admin server runs in its own thread with a single-threaded Tokio runtime
/// to avoid interfering with the main cache server I/O loops.
///
/// # Arguments
///
/// * `config` - Admin server configuration
///
/// # Returns
///
/// An `AdminHandle` that can be used to shut down the server.
pub fn start(config: AdminConfig) -> std::io::Result<AdminHandle> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let address = config.address;
    let shutdown = config.shutdown;

    let join_handle = std::thread::Builder::new()
        .name("admin".to_string())
        .spawn(move || {
            // Create a single-threaded Tokio runtime for the admin server
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create admin runtime");

            rt.block_on(async move {
                run_admin_server(address, shutdown, shutdown_rx).await;
            });
        })?;

    Ok(AdminHandle {
        shutdown_tx,
        join_handle,
    })
}

async fn run_admin_server(
    address: SocketAddr,
    shutdown: Arc<AtomicBool>,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    // Build the router
    let app = Router::new()
        .route("/health", get(health_handler))
        .route(
            "/ready",
            get({
                let shutdown = shutdown.clone();
                move || ready_handler(shutdown.clone())
            }),
        )
        .route("/metrics", get(metrics_handler));

    // Bind and serve
    let listener = match tokio::net::TcpListener::bind(address).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(error = %e, address = %address, "Failed to bind admin server");
            return;
        }
    };

    tracing::info!(address = %address, "Admin server listening");

    // Serve with graceful shutdown
    let server = axum::serve(listener, app);

    tokio::select! {
        result = server => {
            if let Err(e) = result {
                tracing::error!(error = %e, "Admin server error");
            }
        }
        _ = shutdown_rx => {
            tracing::debug!("Admin server received shutdown signal");
        }
    }

    tracing::debug!("Admin server stopped");
}

/// Health check handler (liveness probe).
///
/// Always returns 200 OK to indicate the process is alive.
async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

/// Readiness check handler.
///
/// Returns 200 OK if the server is ready to accept requests,
/// 503 Service Unavailable if shutting down.
async fn ready_handler(shutdown: Arc<AtomicBool>) -> impl IntoResponse {
    if shutdown.load(Ordering::Relaxed) {
        (StatusCode::SERVICE_UNAVAILABLE, "Shutting down")
    } else {
        (StatusCode::OK, "OK")
    }
}

/// Metrics handler (Prometheus format).
///
/// Exposes all registered metrics in Prometheus text format.
async fn metrics_handler() -> impl IntoResponse {
    let output = generate_prometheus_output();
    (
        StatusCode::OK,
        [("Content-Type", "text/plain; version=0.0.4; charset=utf-8")],
        output,
    )
}

/// Generate Prometheus-formatted metrics output.
fn generate_prometheus_output() -> String {
    let mut output = String::with_capacity(4096);

    // Iterate over all registered metrics
    for metric in metriken::metrics().iter() {
        let name = metric.name();

        // Skip metrics with no name
        if name.is_empty() {
            continue;
        }

        let value = match metric.value() {
            Some(v) => v,
            None => continue,
        };

        // Sanitize metric name for Prometheus (replace invalid chars with _)
        let prom_name: String = name
            .chars()
            .map(|c: char| {
                if c.is_ascii_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();

        // Handle different metric types
        match value {
            metriken::Value::Counter(v) => {
                output.push_str(&format!("# TYPE {} counter\n", prom_name));
                output.push_str(&format!("{} {}\n", prom_name, v));
            }
            metriken::Value::Gauge(v) => {
                output.push_str(&format!("# TYPE {} gauge\n", prom_name));
                output.push_str(&format!("{} {}\n", prom_name, v));
            }
            metriken::Value::Other(any) => {
                // Try to downcast to AtomicHistogram
                if let Some(histogram) = any.downcast_ref::<metriken::AtomicHistogram>()
                    && let Some(snapshot) = histogram.load()
                {
                    output.push_str(&format!("# TYPE {} histogram\n", prom_name));

                    // Output percentiles as summary-style metrics
                    let percentiles = [50.0, 90.0, 95.0, 99.0, 99.9, 99.99];
                    if let Ok(Some(results)) = snapshot.percentiles(&percentiles) {
                        for (pct, bucket) in results {
                            let quantile = pct / 100.0;
                            output.push_str(&format!(
                                "{}{{quantile=\"{}\"}} {}\n",
                                prom_name,
                                quantile,
                                bucket.end()
                            ));
                        }
                    }

                    // Output count and sum
                    let mut count = 0u64;
                    let mut sum = 0u64;
                    for bucket in snapshot.into_iter() {
                        let bucket_count = bucket.count();
                        count += bucket_count;
                        // Use midpoint of bucket for sum approximation
                        let midpoint = (bucket.start() + bucket.end()) / 2;
                        sum += bucket_count * midpoint;
                    }
                    output.push_str(&format!("{}_count {}\n", prom_name, count));
                    output.push_str(&format!("{}_sum {}\n", prom_name, sum));
                }
            }
            // Handle any future Value variants
            _ => {}
        }

        // Add empty line between metrics for readability
        output.push('\n');
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prometheus_name_sanitization() {
        let name = "foo.bar-baz";
        let sanitized: String = name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        assert_eq!(sanitized, "foo_bar_baz");
    }
}
