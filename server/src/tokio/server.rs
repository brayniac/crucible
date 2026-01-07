//! Tokio runtime server loop.

use crate::affinity::set_cpu_affinity;
use crate::config::Config;
use crate::connection::Connection;
use crate::metrics::{CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE};
use cache_core::Cache;
use io_driver::BufferPool;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

/// Per-connection buffer pool settings.
/// For tokio, we use a small pool per connection since we don't have
/// io_uring's ring-provided buffers.
const CONN_CHUNK_SIZE: usize = 32 * 1024; // 32KB chunks
const CONN_CHUNK_COUNT: usize = 8; // 8 chunks = 256KB per connection
const ASSEMBLY_SIZE: usize = 256 * 1024; // 256KB assembly buffer

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
    let mut pool = BufferPool::new(CONN_CHUNK_SIZE, CONN_CHUNK_COUNT);
    let mut assembly = vec![0u8; ASSEMBLY_SIZE];

    loop {
        // Wait for the socket to be readable
        stream.readable().await?;

        // Check out a buffer for receiving
        let buf_id = match pool.checkout() {
            Some(id) => id,
            None => {
                // Pool exhausted - process pending data to free buffers
                conn.process(&*cache, &pool, &mut assembly);
                for id in conn.drain_consumed_buffers() {
                    pool.checkin(id);
                }
                // Try again
                match pool.checkout() {
                    Some(id) => id,
                    None => {
                        // Still no buffers - connection has too much pending data
                        return Err(std::io::Error::other("buffer pool exhausted"));
                    }
                }
            }
        };

        // Try to read data into the checked-out buffer
        let recv_buf = pool.get_mut(buf_id);
        match stream.try_read(recv_buf) {
            Ok(0) => {
                // Connection closed - return buffer and cleanup
                pool.checkin(buf_id);
                for id in conn.clear_recv_buffers() {
                    pool.checkin(id);
                }
                return Ok(());
            }
            Ok(n) => {
                // Push buffer to connection's recv chain
                conn.push_recv_buffer(buf_id, n);
                conn.process(&*cache, &pool, &mut assembly);

                // Return consumed buffers to pool
                for id in conn.drain_consumed_buffers() {
                    pool.checkin(id);
                }

                if conn.should_close() {
                    // Flush any pending writes before closing
                    if conn.has_pending_write() {
                        let _ = stream.write_all(conn.pending_write_data()).await;
                    }
                    for id in conn.clear_recv_buffers() {
                        pool.checkin(id);
                    }
                    return Ok(());
                }

                // Write response if we have data
                if conn.has_pending_write() {
                    stream.write_all(conn.pending_write_data()).await?;
                    conn.advance_write(conn.pending_write_data().len());
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No data available - return buffer to pool and continue waiting
                pool.checkin(buf_id);
                continue;
            }
            Err(e) => {
                // Error - cleanup and return
                pool.checkin(buf_id);
                for id in conn.clear_recv_buffers() {
                    pool.checkin(id);
                }
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
