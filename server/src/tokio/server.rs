//! Tokio runtime server loop.

use crate::affinity::set_cpu_affinity;
use crate::config::Config;
use crate::connection::Connection;
use crate::metrics::{CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE};
use cache_core::Cache;
use io_driver::RecvBuf;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

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

    /// Append data to the buffer
    fn append(&mut self, new_data: &[u8]) {
        // Compact if we've consumed more than half
        if self.offset > self.data.len() / 2 && self.offset > 0 {
            self.data.drain(..self.offset);
            self.offset = 0;
        }
        self.data.extend_from_slice(new_data);
    }
}

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
        }
    }
}

/// Run the tokio runtime server.
pub fn run<C: Cache + 'static>(
    config: &Config,
    cache: C,
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

    runtime.block_on(async move { run_async(listeners, cache).await })
}

async fn run_async<C: Cache + 'static>(
    addresses: Vec<SocketAddr>,
    cache: Arc<C>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Bind all listeners
    let mut listeners = Vec::with_capacity(addresses.len());
    for addr in &addresses {
        let listener = TcpListener::bind(addr).await?;
        listeners.push(listener);
    }

    // Spawn accept loops for each listener
    let mut handles = Vec::with_capacity(listeners.len());
    for listener in listeners {
        let cache = cache.clone();
        let handle = tokio::spawn(async move { accept_loop(listener, cache).await });
        handles.push(handle);
    }

    // Wait for all accept loops (they run forever unless error)
    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("Accept loop error: {}", e);
        }
    }

    Ok(())
}

async fn accept_loop<C: Cache + 'static>(listener: TcpListener, cache: Arc<C>) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                CONNECTIONS_ACCEPTED.increment();
                CONNECTIONS_ACTIVE.increment();

                let cache = cache.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, cache).await
                        && !is_connection_reset(&e)
                    {
                        eprintln!("Connection error: {}", e);
                    }
                    CONNECTIONS_ACTIVE.decrement();
                });
            }
            Err(e) => {
                eprintln!("Accept error: {}", e);
            }
        }
    }
}

async fn handle_connection<C: Cache>(mut stream: TcpStream, cache: Arc<C>) -> std::io::Result<()> {
    let mut conn = Connection::new();
    let mut recv_buf = SimpleRecvBuf::with_capacity(64 * 1024);
    let mut temp_buf = vec![0u8; 64 * 1024];

    loop {
        // Wait for the socket to be readable
        stream.readable().await?;

        // Try to read data
        match stream.try_read(&mut temp_buf) {
            Ok(0) => {
                // Connection closed
                return Ok(());
            }
            Ok(n) => {
                // Append to our RecvBuf
                recv_buf.append(&temp_buf[..n]);

                // Process using the new API
                conn.process_from(&mut recv_buf, &*cache);

                if conn.should_close() {
                    // Flush any pending writes before closing
                    if conn.has_pending_write() {
                        let _ = stream.write_all(conn.pending_write_data()).await;
                    }
                    return Ok(());
                }

                // Write response if we have data
                if conn.has_pending_write() {
                    let data = conn.pending_write_data();
                    stream.write_all(data).await?;
                    conn.advance_write(data.len());
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
