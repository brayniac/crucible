//! Native runtime server loop.

use crate::affinity::set_cpu_affinity;
use crate::config::Config;
use crate::connection::Connection;
use crate::metrics::{CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE};
use cache_core::Cache;
use io_driver::{CompletionKind, ConnId, Driver, IoDriver};
use slab::Slab;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for each worker thread.
#[derive(Clone)]
struct WorkerConfig {
    listeners: Vec<SocketAddr>,
    backlog: u32,
    read_buffer_size: usize,
    buffer_size: usize,
    buffer_count: u16,
    sq_depth: u32,
    sqpoll: bool,
}

/// Run the native runtime server.
pub fn run<C: Cache + 'static>(
    config: &Config,
    cache: C,
) -> Result<(), Box<dyn std::error::Error>> {
    let cache = Arc::new(cache);
    let num_workers = config.threads();
    let cpu_affinity = config.cpu_affinity();

    // Extract listener addresses
    let listeners: Vec<SocketAddr> = config.listener.iter().map(|l| l.address).collect();

    let worker_config = WorkerConfig {
        listeners,
        backlog: 4096,
        read_buffer_size: 64 * 1024,
        buffer_size: config.uring.buffer_size,
        buffer_count: config.uring.buffer_count,
        sq_depth: config.uring.sq_depth,
        sqpoll: config.uring.sqpoll,
    };

    // Spawn workers
    let mut handles = Vec::with_capacity(num_workers);
    for worker_id in 0..num_workers {
        let cpu_id = cpu_affinity
            .as_ref()
            .map(|cpus| cpus[worker_id % cpus.len()]);
        let cache = cache.clone();
        let worker_config = worker_config.clone();

        let handle = std::thread::Builder::new()
            .name(format!("worker-{}", worker_id))
            .spawn(move || {
                if let Some(cpu) = cpu_id {
                    let _ = set_cpu_affinity(cpu);
                }

                if let Err(e) = run_worker(worker_id, worker_config, cache) {
                    eprintln!("Worker {} error: {}", worker_id, e);
                }
            })
            .expect("failed to spawn worker thread");

        handles.push(handle);
    }

    // Wait for all workers
    for handle in handles {
        let _ = handle.join();
    }

    Ok(())
}

fn run_worker<C: Cache>(_worker_id: usize, config: WorkerConfig, cache: Arc<C>) -> io::Result<()> {
    let mut driver = Driver::builder()
        .buffer_size(config.buffer_size)
        .buffer_count(config.buffer_count.next_power_of_two())
        .sq_depth(config.sq_depth)
        .sqpoll(config.sqpoll)
        .build()?;

    // Start listening on all configured addresses
    for addr in &config.listeners {
        driver.listen(*addr, config.backlog)?;
    }

    let mut connections: Slab<Connection> = Slab::with_capacity(4096);
    let mut recv_buf = vec![0u8; config.read_buffer_size];

    loop {
        driver.poll(Some(Duration::from_millis(100)))?;

        for completion in driver.drain_completions() {
            match completion.kind {
                CompletionKind::Accept { conn_id, .. } => {
                    CONNECTIONS_ACCEPTED.increment();
                    CONNECTIONS_ACTIVE.increment();

                    let entry = connections.vacant_entry();
                    debug_assert_eq!(entry.key(), conn_id.as_usize());
                    entry.insert(Connection::new(config.read_buffer_size));
                }

                CompletionKind::Recv { conn_id } => {
                    let token = conn_id.as_usize();
                    if !connections.contains(token) {
                        continue;
                    }

                    loop {
                        match driver.recv(conn_id, &mut recv_buf) {
                            Ok(0) => {
                                close_connection(&mut driver, &mut connections, conn_id);
                                break;
                            }
                            Ok(n) => {
                                let conn = &mut connections[token];
                                conn.append_recv_data(&recv_buf[..n]);
                                conn.process(&*cache);

                                if conn.should_close() {
                                    close_connection(&mut driver, &mut connections, conn_id);
                                    break;
                                }

                                if conn.has_pending_write() {
                                    let data = conn.pending_write_data();
                                    match driver.send(conn_id, data) {
                                        Ok(n) => conn.advance_write(n),
                                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                                        Err(_) => {
                                            close_connection(
                                                &mut driver,
                                                &mut connections,
                                                conn_id,
                                            );
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(_) => {
                                close_connection(&mut driver, &mut connections, conn_id);
                                break;
                            }
                        }
                    }
                }

                CompletionKind::SendReady { conn_id } => {
                    let token = conn_id.as_usize();
                    if !connections.contains(token) {
                        continue;
                    }

                    let conn = &mut connections[token];
                    if conn.has_pending_write() {
                        let data = conn.pending_write_data();
                        match driver.send(conn_id, data) {
                            Ok(n) => conn.advance_write(n),
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                            Err(_) => {
                                close_connection(&mut driver, &mut connections, conn_id);
                            }
                        }
                    }
                }

                CompletionKind::Closed { conn_id } => {
                    close_connection(&mut driver, &mut connections, conn_id);
                }

                CompletionKind::Error { conn_id, .. } => {
                    close_connection(&mut driver, &mut connections, conn_id);
                }

                CompletionKind::ListenerError { error, .. } => {
                    eprintln!("Listener error: {}", error);
                }
            }
        }
    }
}

fn close_connection(
    driver: &mut Box<dyn IoDriver>,
    connections: &mut Slab<Connection>,
    conn_id: ConnId,
) {
    let token = conn_id.as_usize();
    if connections.try_remove(token).is_some() {
        let _ = driver.close(conn_id);
        CONNECTIONS_ACTIVE.decrement();
    }
}
