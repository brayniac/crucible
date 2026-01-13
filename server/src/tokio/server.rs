//! Tokio runtime server loop.

use crate::affinity::set_cpu_affinity;
use crate::config::{Config, ZeroCopyMode};
use crate::connection::Connection;
use crate::metrics::{CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE};
use cache_core::Cache;
use io_driver::RecvBuf;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

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

    let max_value_size = config.cache.max_value_size;
    let zero_copy_mode = config.zero_copy;
    runtime
        .block_on(async move { run_async(listeners, cache, max_value_size, zero_copy_mode).await })
}

async fn run_async<C: Cache + 'static>(
    addresses: Vec<SocketAddr>,
    cache: Arc<C>,
    max_value_size: usize,
    zero_copy_mode: ZeroCopyMode,
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
        let handle = tokio::spawn(async move {
            accept_loop(listener, cache, max_value_size, zero_copy_mode).await
        });
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

async fn accept_loop<C: Cache + 'static>(
    listener: TcpListener,
    cache: Arc<C>,
    max_value_size: usize,
    zero_copy_mode: ZeroCopyMode,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                CONNECTIONS_ACCEPTED.increment();
                CONNECTIONS_ACTIVE.increment();

                let cache = cache.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        handle_connection(stream, cache, max_value_size, zero_copy_mode).await
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

async fn handle_connection<C: Cache>(
    mut stream: TcpStream,
    cache: Arc<C>,
    max_value_size: usize,
    zero_copy_mode: ZeroCopyMode,
) -> std::io::Result<()> {
    let mut conn = Connection::new(max_value_size);
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

                // Zero-copy path: process one command at a time
                if zero_copy_mode != ZeroCopyMode::Disabled {
                    while !recv_buf.is_empty() && conn.should_read() {
                        if let Some(resp) =
                            conn.process_one_zero_copy(&mut recv_buf, &*cache, zero_copy_mode)
                        {
                            // Send zero-copy response using write_vectored
                            let slices = resp.as_io_slices();
                            // Note: write_vectored may do a partial write, but for simplicity
                            // we rely on the response being small enough to fit in one syscall
                            let _n = stream.write_vectored(&slices).await?;
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
                } else {
                    // Non-zero-copy path: process all commands at once
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
