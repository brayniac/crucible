//! Async Redis client backed by kompio io_uring workers.
//!
//! Exposes a familiar tokio `async fn` API while routing all I/O through
//! kompio workers for io_uring performance.
//!
//! # Example
//!
//! ```no_run
//! use crucible_client::{Client, ClientConfig};
//!
//! # async fn example() -> Result<(), crucible_client::ClientError> {
//! let client = Client::connect(ClientConfig {
//!     servers: vec!["127.0.0.1:6379".to_string()],
//!     workers: 1,
//!     connections_per_server: 1,
//!     ..Default::default()
//! })?;
//!
//! client.set(b"hello", &b"world"[..]).await?;
//! let val = client.get(b"hello").await?;
//! assert_eq!(val.as_deref(), Some(b"world".as_ref()));
//!
//! client.del(b"hello").await?;
//! # Ok(())
//! # }
//! ```

mod command;
mod config;
mod error;
mod handle;
mod latency;
mod router;
mod worker;

pub use config::ClientConfig;
pub use error::ClientError;
pub use latency::ClientLatency;

use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::thread;

use bytes::Bytes;
use tokio::sync::oneshot;

use command::Command;
use handle::WorkerHandle;
use worker::{ClientHandler, ClientWorkerConfig};

/// Async Redis client backed by kompio io_uring workers.
///
/// Clone-able, Send + Sync. All clones share the same worker pool.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    workers: Box<[WorkerHandle]>,
    round_robin: AtomicU64,
    shutdown: kompio::ShutdownHandle,
    latency: Arc<ClientLatency>,
    _threads: Vec<thread::JoinHandle<Result<(), kompio::Error>>>,
}

impl Client {
    /// Connect to one or more Redis servers. Spawns kompio worker threads.
    ///
    /// This is synchronous — it spawns kompio background threads and returns
    /// immediately. Connections are established asynchronously by the workers.
    pub fn connect(config: ClientConfig) -> Result<Self, ClientError> {
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
                .map_err(ClientError::Io)?
                .next()
                .ok_or_else(|| {
                    ClientError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("cannot resolve address: {server}"),
                    ))
                })?;
            server_addrs.push(addr);
        }

        let latency = Arc::new(ClientLatency::new());

        // Create per-worker channels and atomics
        let mut worker_txs = Vec::with_capacity(num_workers);
        let mut worker_configs = Vec::with_capacity(num_workers);
        let mut pending_atomics = Vec::with_capacity(num_workers);

        for _ in 0..num_workers {
            let (tx, rx) = crossbeam_channel::unbounded::<Command>();
            let pending = Arc::new(AtomicU32::new(0));
            worker_txs.push(tx);
            pending_atomics.push(pending.clone());
            worker_configs.push(ClientWorkerConfig {
                cmd_rx: rx,
                pending,
                servers: server_addrs.clone(),
                connections_per_server: config.connections_per_server,
                connect_timeout_ms: config.connect_timeout_ms,
                latency: latency.clone(),
            });
        }

        // Set up config channel for workers
        let (config_tx, config_rx) = crossbeam_channel::unbounded::<ClientWorkerConfig>();
        worker::init_config_channel(config_rx);

        // Send configs for each worker
        for wc in worker_configs {
            config_tx.send(wc).expect("config channel closed");
        }
        drop(config_tx);

        // Build kompio config
        let mut kompio_config = kompio::Config::default();
        kompio_config.worker.threads = num_workers;
        kompio_config.worker.pin_to_core = false; // Don't pin client threads by default
        kompio_config.tcp_nodelay = config.tcp_nodelay;
        kompio_config.tick_timeout_us = 10_000; // 10ms tick for reconnects

        // Launch without bind (client-only mode)
        let (shutdown, threads) =
            kompio::KompioBuilder::new(kompio_config).launch::<ClientHandler>()?;

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

        Ok(Client {
            inner: Arc::new(ClientInner {
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

    /// Get a value by key.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, ClientError> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Get {
            key: Bytes::copy_from_slice(key),
            tx,
        };
        self.pick_worker().send(cmd)?;
        rx.await.map_err(|_| ClientError::RequestCancelled)?
    }

    /// Set a key-value pair.
    ///
    /// Accepts any type that converts to `Bytes` — pass `Bytes` directly for
    /// zero-copy, or `&[u8]` / `Vec<u8>` for convenience (copies once).
    pub async fn set(&self, key: &[u8], value: impl Into<Bytes>) -> Result<(), ClientError> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Set {
            key: Bytes::copy_from_slice(key),
            value: value.into(),
            ttl_secs: None,
            tx,
        };
        self.pick_worker().send(cmd)?;
        rx.await.map_err(|_| ClientError::RequestCancelled)?
    }

    /// Set a key-value pair with a TTL in seconds.
    ///
    /// Accepts any type that converts to `Bytes` — pass `Bytes` directly for
    /// zero-copy, or `&[u8]` / `Vec<u8>` for convenience (copies once).
    pub async fn set_ex(
        &self,
        key: &[u8],
        value: impl Into<Bytes>,
        ttl_secs: u64,
    ) -> Result<(), ClientError> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Set {
            key: Bytes::copy_from_slice(key),
            value: value.into(),
            ttl_secs: Some(ttl_secs),
            tx,
        };
        self.pick_worker().send(cmd)?;
        rx.await.map_err(|_| ClientError::RequestCancelled)?
    }

    /// Delete a key. Returns the number of keys deleted.
    pub async fn del(&self, key: &[u8]) -> Result<u64, ClientError> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Del {
            key: Bytes::copy_from_slice(key),
            tx,
        };
        self.pick_worker().send(cmd)?;
        rx.await.map_err(|_| ClientError::RequestCancelled)?
    }

    /// Ping the server.
    pub async fn ping(&self) -> Result<(), ClientError> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Ping { tx };
        self.pick_worker().send(cmd)?;
        rx.await.map_err(|_| ClientError::RequestCancelled)?
    }

    /// Access wire latency histograms (nanoseconds).
    pub fn latency(&self) -> &ClientLatency {
        &self.inner.latency
    }

    /// Shutdown all workers and wait for threads to exit.
    pub fn shutdown(self) {
        self.inner.shutdown.shutdown();
    }
}
