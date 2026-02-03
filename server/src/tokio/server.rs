//! Tokio runtime server loop.

use crate::affinity::set_cpu_affinity;
use crate::config::Config;
use crate::connection::Connection;
use crate::metrics::{CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE};
use cache_core::Cache;
use io_driver::RecvBuf;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

/// Read buffer size for socket reads.
const READ_BUF_SIZE: usize = 64 * 1024;

/// A simple RecvBuf that owns a contiguous buffer.
struct SimpleRecvBuf {
    data: Vec<u8>,
    offset: usize,
}

impl SimpleRecvBuf {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            offset: 0,
        }
    }

    /// Get mutable spare capacity for direct reads.
    /// Compacts buffer first if needed to ensure adequate space.
    #[inline]
    fn spare_capacity_mut(&mut self) -> &mut [u8] {
        // Compact if we've consumed more than half
        if self.offset > self.data.len() / 2 && self.offset > 0 {
            self.data.drain(..self.offset);
            self.offset = 0;
        }

        // Ensure we have space for a read
        self.data.reserve(READ_BUF_SIZE);

        // Safety: We're returning uninitialized memory as a mutable slice.
        // The caller (try_read) will write to it and we'll call advance() with
        // the number of bytes written.
        let len = self.data.len();
        let cap = self.data.capacity();
        unsafe { std::slice::from_raw_parts_mut(self.data.as_mut_ptr().add(len), cap - len) }
    }

    /// Advance the buffer length after a successful read.
    /// # Safety
    /// Caller must ensure `n` bytes were written to spare capacity.
    #[inline]
    unsafe fn advance(&mut self, n: usize) {
        // Safety: Caller guarantees n bytes were written to spare capacity
        unsafe { self.data.set_len(self.data.len() + n) };
    }
}

/// Shrink threshold for recv buffer (64KB).
const SHRINK_THRESHOLD: usize = 64 * 1024;

/// Default recv buffer capacity (16KB).
const DEFAULT_RECV_CAPACITY: usize = 16 * 1024;

impl RecvBuf for SimpleRecvBuf {
    fn as_slice(&self) -> &[u8] {
        &self.data[self.offset..]
    }

    fn len(&self) -> usize {
        self.data.len() - self.offset
    }

    fn consume(&mut self, n: usize) {
        self.offset += n;
        // Compact when all data consumed
        if self.offset >= self.data.len() {
            self.data.clear();
            self.offset = 0;
            // Shrink if buffer grew too large
            if self.data.capacity() > SHRINK_THRESHOLD {
                self.data.shrink_to(DEFAULT_RECV_CAPACITY);
            }
        }
    }

    fn capacity(&self) -> usize {
        self.data.capacity()
    }

    fn shrink_if_oversized(&mut self) {
        if self.data.capacity() <= SHRINK_THRESHOLD {
            return;
        }

        let remaining = self.data.len() - self.offset;
        if remaining == 0 {
            // No data - just shrink
            self.data = Vec::with_capacity(DEFAULT_RECV_CAPACITY);
            self.offset = 0;
        } else {
            // Compact remaining data into a new smaller buffer
            let target_capacity = remaining.max(DEFAULT_RECV_CAPACITY);
            let mut new_buf = Vec::with_capacity(target_capacity);
            new_buf.extend_from_slice(&self.data[self.offset..]);
            self.data = new_buf;
            self.offset = 0;
        }
    }
}

/// Run the tokio runtime server.
///
/// # Arguments
///
/// * `config` - Server configuration
/// * `cache` - Cache implementation
/// * `shutdown` - Shared shutdown flag (set to true when shutdown is requested)
/// * `drain_timeout` - Maximum time to wait for connections to drain during shutdown
pub fn run<C: Cache + 'static>(
    config: &Config,
    cache: C,
    shutdown: Arc<AtomicBool>,
    drain_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let cache = Arc::new(cache);
    let num_workers = config.threads();
    let cpu_affinity = config.cpu_affinity();

    // Extract listener addresses
    let listeners: Vec<SocketAddr> = config.listener.iter().map(|l| l.address).collect();

    // Build tokio runtime with optional CPU pinning
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(num_workers).enable_all();

    // Set up CPU affinity if configured
    if let Some(cpus) = cpu_affinity {
        let worker_index = Arc::new(AtomicUsize::new(0));
        let cpus = Arc::new(cpus);

        builder.on_thread_start({
            let worker_index = worker_index.clone();
            let cpus = cpus.clone();
            move || {
                let idx = worker_index.fetch_add(1, Ordering::SeqCst);
                let cpu = cpus[idx % cpus.len()];
                let _ = set_cpu_affinity(cpu);
            }
        });
    }

    let runtime = builder.build()?;

    let max_value_size = config.cache.max_value_size;
    // Allow flush if ANY listener allows it (since tokio runtime doesn't track per-listener)
    let allow_flush = config.listener.iter().any(|l| l.allow_flush);
    runtime.block_on(async move {
        run_async(
            listeners,
            cache,
            max_value_size,
            allow_flush,
            shutdown,
            drain_timeout,
        )
        .await
    })
}

async fn run_async<C: Cache + 'static>(
    addresses: Vec<SocketAddr>,
    cache: Arc<C>,
    max_value_size: usize,
    allow_flush: bool,
    shutdown: Arc<AtomicBool>,
    drain_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    // Bind all listeners
    let mut listeners = Vec::with_capacity(addresses.len());
    for addr in &addresses {
        let listener = TcpListener::bind(addr).await?;
        listeners.push(listener);
    }

    // Create a channel to notify accept loops of shutdown
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    // Spawn accept loops for each listener
    let mut handles = Vec::with_capacity(listeners.len());
    for listener in listeners {
        let cache = cache.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        let handle = tokio::spawn(async move {
            accept_loop(
                listener,
                cache,
                max_value_size,
                allow_flush,
                shutdown_rx,
            )
            .await
        });
        handles.push(handle);
    }

    // Wait for shutdown signal
    while !shutdown.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    info!("Shutdown signal received, draining connections...");

    // Signal accept loops to stop accepting new connections
    let _ = shutdown_tx.send(());

    // Wait for connections to drain with timeout
    let drain_start = std::time::Instant::now();
    while drain_start.elapsed() < drain_timeout {
        let active = CONNECTIONS_ACTIVE.value();
        if active == 0 {
            break;
        }
        debug!(
            active_connections = active,
            "Waiting for connections to drain"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let active_conns = CONNECTIONS_ACTIVE.value();
    if active_conns > 0 {
        warn!(
            active_connections = active_conns,
            "Drain timeout reached, {} connections still active", active_conns
        );
    }

    // Wait for accept loops to finish
    for handle in handles {
        let _ = handle.await;
    }

    info!("Server shutdown complete");

    Ok(())
}

async fn accept_loop<C: Cache + 'static>(
    listener: TcpListener,
    cache: Arc<C>,
    max_value_size: usize,
    allow_flush: bool,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _addr)) => {
                        CONNECTIONS_ACCEPTED.increment();
                        CONNECTIONS_ACTIVE.increment();

                        let cache = cache.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(
                                stream,
                                cache,
                                max_value_size,
                                allow_flush,
                            )
                            .await
                                && !is_connection_reset(&e)
                            {
                                debug!(error = %e, "Connection error");
                            }
                            CONNECTIONS_ACTIVE.decrement();
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "Accept error");
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                debug!("Accept loop received shutdown signal");
                break;
            }
        }
    }
}

async fn handle_connection<C: Cache>(
    mut stream: TcpStream,
    cache: Arc<C>,
    max_value_size: usize,
    allow_flush: bool,
) -> std::io::Result<()> {
    let mut conn = Connection::with_options(max_value_size, allow_flush);
    let mut recv_buf = SimpleRecvBuf::with_capacity(READ_BUF_SIZE);

    loop {
        // Wait for the socket to be readable
        stream.readable().await?;

        // Try to read directly into recv_buf's spare capacity (avoids extra copy)
        match stream.try_read(recv_buf.spare_capacity_mut()) {
            Ok(0) => {
                // Connection closed
                return Ok(());
            }
            Ok(n) => {
                // Safety: try_read wrote n bytes to spare capacity
                unsafe { recv_buf.advance(n) };

                // Process one command at a time with zero-copy responses
                loop {
                    if recv_buf.is_empty() || !conn.should_read() {
                        break;
                    }

                    let initial_len = recv_buf.len();
                    if let Some(resp) = conn.process_one_zero_copy(&mut recv_buf, &*cache) {
                        // Send zero-copy response using write_vectored
                        let slices = resp.as_io_slices();
                        // Note: write_vectored may do a partial write, but for simplicity
                        // we rely on the response being small enough to fit in one syscall
                        let _n = stream.write_vectored(&slices).await?;
                    }

                    // Check if we made progress (consumed any data).
                    // If not, command was incomplete - wait for more data.
                    if recv_buf.len() == initial_len {
                        break;
                    }

                    // Send any pending write_buf data (non-zero-copy responses)
                    if conn.has_pending_write() {
                        let data = conn.pending_write_data();
                        stream.write_all(data).await?;
                        conn.advance_write(data.len());
                    }

                    if conn.should_close() {
                        return Ok(());
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No data available, continue waiting
                continue;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
}

fn is_connection_reset(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::UnexpectedEof
    )
}
