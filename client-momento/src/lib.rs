mod command;
mod config;
mod error;
mod handle;
mod latency;
mod worker;

pub use config::MomentoClientConfig;
pub use error::MomentoError;
pub use latency::MomentoLatency;
pub use protocol_momento::{CacheValue, Credential, WireFormat};

use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use tokio::sync::oneshot;

use command::MomentoCommand;
use handle::WorkerHandle;
use worker::{MomentoHandler, MomentoWorkerConfig};

/// Async Momento cache client backed by kompio io_uring workers.
///
/// Clone-able, Send + Sync. All clones share the same worker pool.
#[derive(Clone)]
pub struct MomentoClient {
    inner: Arc<MomentoClientInner>,
}

struct MomentoClientInner {
    workers: Box<[WorkerHandle]>,
    round_robin: AtomicU64,
    shutdown: kompio::ShutdownHandle,
    latency: Arc<MomentoLatency>,
    default_ttl: Duration,
    _threads: Vec<thread::JoinHandle<Result<(), kompio::Error>>>,
}

impl MomentoClient {
    /// Connect to Momento. Spawns kompio worker threads.
    ///
    /// This is synchronous — it spawns kompio background threads and returns
    /// immediately. Connections are established asynchronously by the workers.
    pub fn connect(config: MomentoClientConfig) -> Result<Self, MomentoError> {
        let num_workers = if config.workers == 0 {
            1
        } else {
            config.workers
        };

        // Resolve server addresses
        let server_addrs = if config.servers.is_empty() {
            let host = config.credential.host();
            let port = config.credential.port();
            let addr_str = format!("{}:{}", host, port);
            let addrs: Vec<_> = addr_str
                .to_socket_addrs()
                .map_err(MomentoError::Io)?
                .collect();
            if addrs.is_empty() {
                return Err(MomentoError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("cannot resolve address: {addr_str}"),
                )));
            }
            addrs
        } else {
            config.servers.clone()
        };

        // Resolve TLS server name based on wire format
        let tls_server_name: Option<String> = None;

        let latency = Arc::new(MomentoLatency::new());

        // Create per-worker channels and atomics
        let mut worker_txs = Vec::with_capacity(num_workers);
        let mut worker_configs = Vec::with_capacity(num_workers);
        let mut pending_atomics = Vec::with_capacity(num_workers);

        for _ in 0..num_workers {
            let (tx, rx) = crossbeam_channel::unbounded::<MomentoCommand>();
            let pending = Arc::new(std::sync::atomic::AtomicU32::new(0));
            worker_txs.push(tx);
            pending_atomics.push(pending.clone());
            worker_configs.push(MomentoWorkerConfig {
                cmd_rx: rx,
                pending,
                servers: server_addrs.clone(),
                connections_per_server: config.connections_per_server,
                connect_timeout_ms: config.connect_timeout_ms,
                latency: latency.clone(),
                credential: config.credential.clone(),
                cache_name: config.cache_name.clone(),
                tls_server_name: tls_server_name.clone(),
            });
        }

        // Set up config channel for workers
        let (config_tx, config_rx) = crossbeam_channel::unbounded::<MomentoWorkerConfig>();
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
        if tls_server_name.is_some() {
            let root_store =
                rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let mut tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            if config.credential.wire_format() == WireFormat::Grpc {
                tls_config.alpn_protocols = vec![b"h2".to_vec()];
            }

            kompio_config.tls_client = Some(kompio::TlsClientConfig {
                client_config: Arc::new(tls_config),
            });
        }

        // Launch without bind (client-only mode)
        let (shutdown, threads) =
            kompio::KompioBuilder::new(kompio_config).launch::<MomentoHandler>()?;

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

        Ok(MomentoClient {
            inner: Arc::new(MomentoClientInner {
                workers: handles.into_boxed_slice(),
                round_robin: AtomicU64::new(0),
                shutdown,
                latency,
                default_ttl: config.default_ttl,
                _threads: threads,
            }),
        })
    }

    /// Select a worker (round-robin).
    fn pick_worker(&self) -> &WorkerHandle {
        let idx = self.inner.round_robin.fetch_add(1, Ordering::Relaxed) as usize;
        &self.inner.workers[idx % self.inner.workers.len()]
    }

    /// Get a value from the cache.
    pub async fn get(&self, key: &[u8]) -> Result<CacheValue, MomentoError> {
        let (tx, rx) = oneshot::channel();
        let cmd = MomentoCommand::Get {
            key: key.to_vec(),
            tx,
        };
        self.pick_worker().send(cmd)?;
        rx.await.map_err(|_| MomentoError::RequestCancelled)?
    }

    /// Set a value in the cache with the default TTL.
    pub async fn set(&self, key: &[u8], value: &[u8]) -> Result<(), MomentoError> {
        self.set_with_ttl(key, value, self.inner.default_ttl).await
    }

    /// Set a value in the cache with explicit TTL.
    pub async fn set_with_ttl(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> Result<(), MomentoError> {
        let (tx, rx) = oneshot::channel();
        let cmd = MomentoCommand::Set {
            key: key.to_vec(),
            value: value.to_vec(),
            ttl,
            tx,
        };
        self.pick_worker().send(cmd)?;
        rx.await.map_err(|_| MomentoError::RequestCancelled)?
    }

    /// Delete a key from the cache.
    pub async fn delete(&self, key: &[u8]) -> Result<(), MomentoError> {
        let (tx, rx) = oneshot::channel();
        let cmd = MomentoCommand::Delete {
            key: key.to_vec(),
            tx,
        };
        self.pick_worker().send(cmd)?;
        rx.await.map_err(|_| MomentoError::RequestCancelled)?
    }

    /// Access wire latency histograms (nanoseconds).
    pub fn latency(&self) -> &MomentoLatency {
        &self.inner.latency
    }

    /// Shutdown all workers and wait for threads to exit.
    pub fn shutdown(self) {
        self.inner.shutdown.shutdown();
    }
}
