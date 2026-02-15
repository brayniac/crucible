mod command;
mod config;
mod error;
mod handle;
mod latency;
mod response;
mod worker;

pub use config::GrpcClientConfig;
pub use error::GrpcError;
pub use grpc::{Code, Metadata, Status};
pub use latency::GrpcLatency;
pub use response::GrpcResponse;

use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::thread;

use bytes::Bytes;
use tokio::sync::oneshot;

use command::GrpcCommand;
use handle::WorkerHandle;
use worker::{GrpcHandler, GrpcWorkerConfig};

/// Async gRPC client backed by kompio io_uring workers.
///
/// Clone-able, Send + Sync. All clones share the same worker pool.
#[derive(Clone)]
pub struct GrpcClient {
    inner: Arc<GrpcClientInner>,
}

struct GrpcClientInner {
    workers: Box<[WorkerHandle]>,
    round_robin: AtomicU64,
    shutdown: kompio::ShutdownHandle,
    latency: Arc<GrpcLatency>,
    _threads: Vec<thread::JoinHandle<Result<(), kompio::Error>>>,
}

impl GrpcClient {
    /// Connect to one or more gRPC servers. Spawns kompio worker threads.
    ///
    /// This is synchronous — it spawns kompio background threads and returns
    /// immediately. Connections are established asynchronously by the workers.
    pub fn connect(config: GrpcClientConfig) -> Result<Self, GrpcError> {
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
                .map_err(GrpcError::Io)?
                .next()
                .ok_or_else(|| {
                    GrpcError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("cannot resolve address: {server}"),
                    ))
                })?;
            server_addrs.push(addr);
        }

        // Default authority: use the first server address if not specified
        let authority = config
            .default_authority
            .unwrap_or_else(|| config.servers[0].clone());

        // Resolve TLS server name: explicit config > host portion of first server
        let tls_server_name = if config.tls {
            Some(config.tls_server_name.unwrap_or_else(|| {
                let server = &config.servers[0];
                server
                    .rsplit_once(':')
                    .map(|(host, _)| host.to_string())
                    .unwrap_or_else(|| server.clone())
            }))
        } else {
            None
        };

        let latency = Arc::new(GrpcLatency::new());

        // Create per-worker channels and atomics
        let mut worker_txs = Vec::with_capacity(num_workers);
        let mut worker_configs = Vec::with_capacity(num_workers);
        let mut pending_atomics = Vec::with_capacity(num_workers);

        for _ in 0..num_workers {
            let (tx, rx) = crossbeam_channel::unbounded::<GrpcCommand>();
            let pending = Arc::new(AtomicU32::new(0));
            worker_txs.push(tx);
            pending_atomics.push(pending.clone());
            worker_configs.push(GrpcWorkerConfig {
                cmd_rx: rx,
                pending,
                servers: server_addrs.clone(),
                connections_per_server: config.connections_per_server,
                connect_timeout_ms: config.connect_timeout_ms,
                latency: latency.clone(),
                authority: authority.clone(),
                tls_server_name: tls_server_name.clone(),
            });
        }

        // Set up config channel for workers
        let (config_tx, config_rx) = crossbeam_channel::unbounded::<GrpcWorkerConfig>();
        worker::init_config_channel(config_rx);

        // Send configs for each worker
        for wc in worker_configs {
            config_tx.send(wc).expect("config channel closed");
        }
        drop(config_tx);

        // Build kompio config
        let mut kompio_config = kompio::Config::default();
        kompio_config.worker.threads = num_workers;
        kompio_config.worker.pin_to_core = false;
        kompio_config.tcp_nodelay = config.tcp_nodelay;
        kompio_config.tick_timeout_us = 10_000; // 10ms tick for reconnects

        // Configure TLS if enabled
        #[cfg(feature = "tls")]
        if config.tls {
            let root_store =
                rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let mut tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();
            tls_config.alpn_protocols = vec![b"h2".to_vec()];

            kompio_config.tls_client = Some(kompio::TlsClientConfig {
                client_config: Arc::new(tls_config),
            });
        }

        #[cfg(not(feature = "tls"))]
        if config.tls {
            return Err(GrpcError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "TLS requested but 'tls' feature not enabled",
            )));
        }

        // Launch without bind (client-only mode)
        let (shutdown, threads) =
            kompio::KompioBuilder::new(kompio_config).launch::<GrpcHandler>()?;

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

        Ok(GrpcClient {
            inner: Arc::new(GrpcClientInner {
                workers: handles.into_boxed_slice(),
                round_robin: AtomicU64::new(0),
                shutdown,
                latency,
                _threads: threads,
            }),
        })
    }

    /// Select a worker (round-robin).
    fn pick_worker(&self) -> &WorkerHandle {
        let idx = self.inner.round_robin.fetch_add(1, Ordering::Relaxed) as usize;
        &self.inner.workers[idx % self.inner.workers.len()]
    }

    /// Send a unary gRPC call with metadata.
    pub async fn unary(
        &self,
        path: &str,
        metadata: Metadata,
        body: impl Into<Bytes>,
    ) -> Result<GrpcResponse, GrpcError> {
        let (tx, rx) = oneshot::channel();
        let cmd = GrpcCommand::Unary {
            path: path.to_string(),
            metadata,
            body: body.into(),
            tx,
        };
        self.pick_worker().send(cmd)?;
        rx.await.map_err(|_| GrpcError::RequestCancelled)?
    }

    /// Convenience: unary call with no metadata.
    pub async fn call(
        &self,
        path: &str,
        body: impl Into<Bytes>,
    ) -> Result<GrpcResponse, GrpcError> {
        self.unary(path, Metadata::new(), body).await
    }

    /// Access wire latency histograms (nanoseconds).
    pub fn latency(&self) -> &GrpcLatency {
        &self.inner.latency
    }

    /// Shutdown all workers and wait for threads to exit.
    pub fn shutdown(self) {
        self.inner.shutdown.shutdown();
    }
}
