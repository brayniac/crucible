mod command;
mod config;
mod error;
mod handle;
mod latency;
mod request;
mod response;
mod worker;

pub use config::HttpClientConfig;
pub use error::HttpError;
pub use latency::HttpLatency;
pub use request::{Method, Request};
pub use response::HttpResponse;

use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::thread;

use bytes::Bytes;
use tokio::sync::oneshot;

use command::HttpCommand;
use handle::WorkerHandle;
use worker::{HttpHandler, HttpWorkerConfig};

/// Async HTTP/2 client backed by krio io_uring workers.
///
/// Clone-able, Send + Sync. All clones share the same worker pool.
#[derive(Clone)]
pub struct HttpClient {
    inner: Arc<HttpClientInner>,
}

struct HttpClientInner {
    workers: Box<[WorkerHandle]>,
    round_robin: AtomicU64,
    shutdown: krio::ShutdownHandle,
    latency: Arc<HttpLatency>,
    default_authority: Bytes,
    default_scheme: Bytes,
    _threads: Vec<thread::JoinHandle<Result<(), krio::Error>>>,
}

impl HttpClient {
    /// Connect to one or more HTTP/2 servers. Spawns krio worker threads.
    ///
    /// This is synchronous — it spawns krio background threads and returns
    /// immediately. Connections are established asynchronously by the workers.
    pub fn connect(config: HttpClientConfig) -> Result<Self, HttpError> {
        let num_workers = if config.workers == 0 {
            1
        } else {
            config.workers
        };

        // Resolve server addresses
        let mut server_addrs = Vec::with_capacity(config.servers.len());
        for server in &config.servers {
            let addr = server
                .to_socket_addrs()
                .map_err(HttpError::Io)?
                .next()
                .ok_or_else(|| {
                    HttpError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("cannot resolve address: {server}"),
                    ))
                })?;
            server_addrs.push(addr);
        }

        // Default authority: use the first server address if not specified
        let default_authority = config
            .default_authority
            .unwrap_or_else(|| config.servers[0].clone());
        let default_scheme = if config.tls && config.default_scheme == "http" {
            "https".to_string()
        } else {
            config.default_scheme.clone()
        };

        // Resolve TLS server name: explicit config > host portion of first server
        let tls_server_name = if config.tls {
            Some(config.tls_server_name.unwrap_or_else(|| {
                // Strip port from server address to get hostname
                let server = &config.servers[0];
                server
                    .rsplit_once(':')
                    .map(|(host, _)| host.to_string())
                    .unwrap_or_else(|| server.clone())
            }))
        } else {
            None
        };

        let latency = Arc::new(HttpLatency::new());

        // Create per-worker channels and atomics
        let mut worker_txs = Vec::with_capacity(num_workers);
        let mut worker_configs = Vec::with_capacity(num_workers);
        let mut pending_atomics = Vec::with_capacity(num_workers);

        for _ in 0..num_workers {
            let (tx, rx) = crossbeam_channel::unbounded::<HttpCommand>();
            let pending = Arc::new(AtomicU32::new(0));
            worker_txs.push(tx);
            pending_atomics.push(pending.clone());
            worker_configs.push(HttpWorkerConfig {
                cmd_rx: rx,
                pending,
                servers: server_addrs.clone(),
                connections_per_server: config.connections_per_server,
                connect_timeout_ms: config.connect_timeout_ms,
                latency: latency.clone(),
                tls_server_name: tls_server_name.clone(),
            });
        }

        // Set up config channel for workers
        let (config_tx, config_rx) = crossbeam_channel::unbounded::<HttpWorkerConfig>();
        worker::init_config_channel(config_rx);

        // Send configs for each worker
        for wc in worker_configs {
            config_tx.send(wc).expect("config channel closed");
        }
        drop(config_tx);

        // Build krio config
        let mut krio_config = krio::Config::default();
        krio_config.worker.threads = num_workers;
        krio_config.worker.pin_to_core = false;
        krio_config.tcp_nodelay = config.tcp_nodelay;
        krio_config.tick_timeout_us = 10_000; // 10ms tick for reconnects

        // Configure TLS if enabled
        #[cfg(feature = "tls")]
        if config.tls {
            let root_store =
                rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let mut tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();
            tls_config.alpn_protocols = vec![b"h2".to_vec()];

            krio_config.tls_client = Some(krio::TlsClientConfig {
                client_config: Arc::new(tls_config),
            });
        }

        #[cfg(not(feature = "tls"))]
        if config.tls {
            return Err(HttpError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "TLS requested but 'tls' feature not enabled",
            )));
        }

        // Launch without bind (client-only mode)
        let (shutdown, threads) = krio::KrioBuilder::new(krio_config).launch::<HttpHandler>()?;

        // Build WorkerHandle for each worker
        let eventfds = shutdown.worker_eventfds();
        let mut handles = Vec::with_capacity(num_workers);
        for i in 0..num_workers {
            handles.push(WorkerHandle::new(
                worker_txs.remove(0),
                eventfds[i],
                pending_atomics[i].clone(),
            ));
        }

        Ok(HttpClient {
            inner: Arc::new(HttpClientInner {
                workers: handles.into_boxed_slice(),
                round_robin: AtomicU64::new(0),
                shutdown,
                latency,
                default_authority: Bytes::from(default_authority),
                default_scheme: Bytes::from(default_scheme),
                _threads: threads,
            }),
        })
    }

    /// Select a worker (round-robin).
    fn pick_worker(&self) -> &WorkerHandle {
        let idx = self.inner.round_robin.fetch_add(1, Ordering::Relaxed) as usize;
        &self.inner.workers[idx % self.inner.workers.len()]
    }

    /// Send an arbitrary HTTP/2 request.
    pub async fn request(&self, req: Request) -> Result<HttpResponse, HttpError> {
        let (tx, rx) = oneshot::channel();
        let cmd = HttpCommand::Request {
            request: req,
            authority: self.inner.default_authority.clone(),
            scheme: self.inner.default_scheme.clone(),
            tx,
        };
        self.pick_worker().send(cmd)?;
        rx.await.map_err(|_| HttpError::RequestCancelled)?
    }

    /// GET request.
    pub async fn get(&self, path: &str) -> Result<HttpResponse, HttpError> {
        self.request(Request::get(path)).await
    }

    /// POST request with body.
    pub async fn post(
        &self,
        path: &str,
        body: impl Into<Bytes>,
    ) -> Result<HttpResponse, HttpError> {
        self.request(Request::post(path, body)).await
    }

    /// PUT request with body.
    pub async fn put(&self, path: &str, body: impl Into<Bytes>) -> Result<HttpResponse, HttpError> {
        self.request(Request::put(path, body)).await
    }

    /// DELETE request.
    pub async fn delete(&self, path: &str) -> Result<HttpResponse, HttpError> {
        self.request(Request::delete(path)).await
    }

    /// Access wire latency histograms (nanoseconds).
    pub fn latency(&self) -> &HttpLatency {
        &self.inner.latency
    }

    /// Shutdown all workers and wait for threads to exit.
    pub fn shutdown(self) {
        self.inner.shutdown.shutdown();
    }
}
